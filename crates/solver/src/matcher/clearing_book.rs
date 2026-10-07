use std::collections::{btree_map, BTreeMap, HashMap};

use miden_protocol::note::NoteId;
use tokio::sync::mpsc;

use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingError, MatchOrder, Order, OrderKey, OrderSide, PairBatch,
};
use crate::matching::types::{BestLevel, SwapBookSnapshot};
use crate::types::{BookOrder, BookUpdate, TokenId};

/// Live ingestion and startup hydration reject zero amounts before admission.
/// Parse once here and maintain the exact price/FIFO index incrementally.
/// Both directions use requested/offered: lower is always better.
#[derive(Default)]
pub(crate) struct ClearingBook {
    orders: HashMap<NoteId, Order>,
    pairs: HashMap<(TokenId, TokenId), BTreeMap<OrderKey, NoteId>>,
}

impl ClearingBook {
    /// Synchronous handoff: no matching can run between parent removal and
    /// remainder activation. Input comes from committed, ordered DB updates.
    pub(super) fn apply(&mut self, update: BookUpdate) {
        for id in update.removed {
            self.remove(id);
        }
        for order in &update.active {
            self.insert_or_skip(order);
        }
    }

    /// Apply the updates queued at the start of the tick. New arrivals wait for
    /// the next receive, so a busy producer cannot postpone matching forever.
    pub(super) fn apply_pending(&mut self, updates: &mut mpsc::Receiver<BookUpdate>) {
        for _ in 0..updates.len() {
            let Ok(update) = updates.try_recv() else {
                break;
            };
            self.apply(update);
        }
    }

    /// Index `order`, or leave it out of the live book if it cannot be
    /// indexed: a malformed note, or a stale parent whose remainder already
    /// holds its FIFO slot. One bad order must not stop matching for every
    /// other one; it stays Active in the database and returns on next boot.
    pub(super) fn insert_or_skip(&mut self, order: &BookOrder) {
        if let Err(error) = self.insert(order) {
            self.remove(order.id());
            tracing::warn!(note_id = %order.id(), %error, "order left out of the live book");
        }
    }

    fn remove_from_index(&mut self, pair: (TokenId, TokenId), key: OrderKey, id: NoteId) {
        if let Some(index) = self.pairs.get_mut(&pair) {
            // A remainder can inherit this exact key. A stale parent event
            // must never remove the child's entry.
            if let btree_map::Entry::Occupied(entry) = index.entry(key) {
                if *entry.get() == id {
                    entry.remove();
                }
            }
        }
    }

    pub fn insert(&mut self, order: &BookOrder) -> Result<(), ClearingError> {
        let id = order.id();
        self.orders.insert(id, Order::from_book_order(order)?);
        self.add_to_index(id)
    }

    fn add_to_index(&mut self, id: NoteId) -> Result<(), ClearingError> {
        let (pair, key) = self
            .orders
            .get(&id)
            .ok_or(ClearingError::InternalInvariant("missing order to index"))?
            .index_key();
        let index = self.pairs.entry(pair).or_default();
        match index.entry(key) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(id);
            }
            btree_map::Entry::Occupied(entry) => {
                if *entry.get() != id {
                    return Err(ClearingError::DuplicateBookPriority);
                }
            }
        }
        self.orders
            .get_mut(&id)
            .ok_or(ClearingError::InternalInvariant("missing indexed order"))?
            .activate();
        Ok(())
    }

    pub fn deactivate(&mut self, id: NoteId) {
        if let Some(order) = self.orders.get_mut(&id) {
            let (pair, key) = order.index_key();
            // The order record remains for recovery, but the active index must
            // no longer expose it to matching or RFQ.
            order.deactivate();
            self.remove_from_index(pair, key, id);
        }
    }

    /// A routed note may have been removed by ingestion before its lease expires.
    pub(crate) fn reactivate(&mut self, id: NoteId) -> Result<(), ClearingError> {
        if self.orders.contains_key(&id) {
            self.add_to_index(id)?;
        }
        Ok(())
    }

    pub(crate) fn note(&self, id: NoteId) -> Option<&miden_protocol::note::Note> {
        self.orders.get(&id).map(Order::note)
    }

    /// Use the same active price/FIFO index for RFQ; do not rebuild or sort a book.
    pub(crate) fn routing_orders(
        &self,
        quotes: &crate::router::QuotesSnapshot,
    ) -> HashMap<crate::router::Pair, Vec<crate::matching::types::Order>> {
        quotes
            .keys()
            .filter_map(|pair| {
                let index = self.pairs.get(pair)?;
                let orders = index
                    .values()
                    .filter_map(|id| self.orders.get(id))
                    .map(|order| {
                        let offered = order.offered_asset();
                        let requested = order.requested_asset();
                        crate::matching::types::Order {
                            id: order.id(),
                            offered_token: offered.faucet_id(),
                            requested_token: requested.faucet_id(),
                            offered: offered.amount().as_u64(),
                            requested: requested.amount().as_u64(),
                            requested_remaining: requested.amount().as_u64(),
                        }
                    })
                    .collect();
                Some((*pair, orders))
            })
            .collect()
    }

    pub fn remove(&mut self, id: NoteId) {
        if let Some(order) = self.orders.remove(&id) {
            let (pair, key) = order.index_key();
            self.remove_from_index(pair, key, id);
        }
    }

    fn admit<'a>(
        &'a self,
        pair: (TokenId, TokenId),
        side: OrderSide,
        price: BatchPrice,
        fee_ppm: u32,
        limit: usize,
    ) -> Result<Vec<MatchOrder<'a>>, ClearingError> {
        let mut selected = Vec::new();
        if let Some(index) = self.pairs.get(&pair) {
            for id in index.values().take(limit) {
                let order = self
                    .orders
                    .get(id)
                    .ok_or(ClearingError::InternalInvariant("missing indexed order"))?;
                match order.prepare_if_eligible(side, price, fee_ppm)? {
                    Some(prepared) => selected.push(prepared),
                    None => {
                        // Eligibility is monotone in this exact price ordering.
                        break;
                    }
                }
            }
        }
        Ok(selected)
    }

    pub(super) fn build_pair_batch<'a>(
        &'a self,
        base: TokenId,
        quote: TokenId,
        price: BatchPrice,
        config: &ClearingConfig,
    ) -> Result<PairBatch<'a>, ClearingError> {
        let sell_orders = self.admit(
            (base, quote),
            OrderSide::SellBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
        )?;
        let buy_orders = self.admit(
            (quote, base),
            OrderSide::BuyBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
        )?;
        Ok(PairBatch::new(price, sell_orders, buy_orders))
    }

    pub(super) fn best_levels_snapshot(&self) -> SwapBookSnapshot {
        self.pairs
            .iter()
            .filter_map(|(&pair, index)| {
                let rate = index.first_key_value()?.0.rate;
                let volume = index
                    .iter()
                    .take_while(|(key, _)| key.rate == rate)
                    .filter_map(|(_, id)| self.orders.get(id))
                    .fold(0u64, |sum, order| {
                        sum.saturating_add(order.offered_asset().amount().as_u64())
                    });
                Some((pair, BestLevel { rate, volume }))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::matcher::run_matcher;
    use crate::matcher::matcher::{run_worker, ClearingRuntime};
    use crate::matcher::MatcherError;
    use crate::types::ExecutionBatch;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;

    fn fixture(
        buy: bool,
        offered: u64,
        requested: u64,
        seq: u64,
        rng: &mut RandomCoin,
    ) -> BookOrder {
        let a = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let b = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let (offered_token, requested_token) = if buy { (b, a) } else { (a, b) };
        let note: Note = PswapNote::builder()
            .sender(creator)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested_token, requested).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .serial_number(rng.draw_word())
            .note_type(NoteType::Public)
            .offered_asset(FungibleAsset::new(offered_token, offered).unwrap())
            .build()
            .unwrap()
            .into();
        BookOrder {
            priority_seq: seq,
            arrival_unix: 1,
            note: Arc::new(note),
        }
    }

    fn admit<'a>(book: &'a ClearingBook, buy: bool, limit: usize) -> Vec<MatchOrder<'a>> {
        let a = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let b = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        book.admit(
            if buy { (b, a) } else { (a, b) },
            if buy {
                OrderSide::BuyBase
            } else {
                OrderSide::SellBase
            },
            BatchPrice::from_ratio(2, 1).unwrap(),
            0,
            limit,
        )
        .unwrap()
    }

    fn routing_fixture(
        order: &BookOrder,
    ) -> (
        crate::router::Routing,
        mpsc::Receiver<crate::router::RouteBatch>,
    ) {
        let pair = Order::from_book_order(order).unwrap().index_key().0;
        let quotes = [(
            pair,
            vec![crate::router::Quote {
                dex: 1,
                pair,
                supply: 100,
                demand: 10,
                expires_at: u64::MAX,
            }],
        )]
        .into_iter()
        .collect();
        let (_, quotes_rx) = watch::channel(Arc::new(quotes));
        let (route_tx, route_rx) = mpsc::channel(1);
        (
            crate::router::Routing::new(quotes_rx, route_tx, 10),
            route_rx,
        )
    }

    #[test]
    fn rfq_uses_active_book_and_preserves_note_and_fifo_after_timeout() {
        use miden_protocol::crypto::utils::Serializable;
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        book.insert(&order).unwrap();
        let (mut routing, mut route_rx) = routing_fixture(&order);
        routing.dispatch(&mut book, 100).unwrap();
        let handover = route_rx.try_recv().unwrap();
        assert_eq!(handover.items.len(), 1);
        assert_eq!(handover.items[0].note_id, order.id());
        assert_eq!(handover.items[0].fill, 18);
        assert_eq!(handover.items[0].note_bytes, order.note.to_bytes());
        assert!(book.best_levels_snapshot().is_empty());
        routing.dispatch(&mut book, 101).unwrap();
        assert!(
            route_rx.try_recv().is_err(),
            "RFQ must not dispatch a reserved note twice"
        );
        routing.release_expired(&mut book, 109).unwrap();
        assert!(book.best_levels_snapshot().is_empty());
        routing.release_expired(&mut book, 110).unwrap();
        assert_eq!(admit(&book, false, 100)[0].order().priority_sequence(), 1);
        assert_eq!(book.orders[&order.id()].note().id(), order.id());
    }

    #[test]
    fn rfq_does_not_reactivate_consumed_notes_or_route_executor_reservations() {
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        book.insert(&order).unwrap();
        let (mut routing, mut route_rx) = routing_fixture(&order);
        book.deactivate(order.id());
        routing.dispatch(&mut book, 100).unwrap();
        assert!(
            route_rx.try_recv().is_err(),
            "executor-reserved notes are not RFQ candidates"
        );
        book.reactivate(order.id()).unwrap();
        routing.dispatch(&mut book, 101).unwrap();
        route_rx.try_recv().unwrap();
        book.remove(order.id());
        routing.release_expired(&mut book, 111).unwrap();
        assert!(book.orders.is_empty());
        assert!(book.best_levels_snapshot().is_empty());
    }

    #[test]
    fn rfq_backpressure_or_closed_channel_leaves_orders_active() {
        for closed in [false, true] {
            let mut rng = RandomCoin::new(Word::default());
            let order = fixture(false, 10, 18, 1, &mut rng);
            let mut book = ClearingBook::default();
            book.insert(&order).unwrap();
            let pair = Order::from_book_order(&order).unwrap().index_key().0;
            let quotes = [(
                pair,
                vec![crate::router::Quote {
                    dex: 1,
                    pair,
                    supply: 100,
                    demand: 10,
                    expires_at: u64::MAX,
                }],
            )]
            .into_iter()
            .collect();
            let (_, quotes_rx) = watch::channel(Arc::new(quotes));
            let (route_tx, route_rx) = mpsc::channel(1);
            route_tx
                .try_send(crate::router::RouteBatch { items: Vec::new() })
                .unwrap();
            if closed {
                drop(route_rx);
            }
            let mut routing = crate::router::Routing::new(quotes_rx, route_tx, 10);
            let result = routing.dispatch(&mut book, 100);
            assert_eq!(result.is_err(), closed);
            assert_eq!(admit(&book, false, 100).len(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_routes_only_after_executor_capacity_returns() {
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let (routing, mut route_rx) = routing_fixture(&order);
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx.send(vec![order.clone()]).is_ok());
        // No price snapshot: direct clearing cannot match, but RFQ still can.
        let (_, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig::default(),
            routing: Some(routing),
        };
        let (_book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        exec_tx
            .try_send(ExecutionBatch {
                filled_notes: Vec::new(),
                group_ends: Vec::new(),
            })
            .unwrap();
        let (snapshot_tx, mut snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            snapshot_tx,
            runtime,
            cancel.clone(),
        ));
        // The first tick cannot clear, so it must not bypass internal
        // matching by routing an order to an external DEX either.
        snapshot_rx.changed().await.unwrap();
        assert!(route_rx.try_recv().is_err());
        assert!(exec_rx.try_recv().unwrap().filled_notes.is_empty());

        tokio::time::advance(Duration::from_secs(1)).await;
        let handover = tokio::time::timeout(Duration::from_secs(2), route_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handover.items[0].note_id, order.id());
        assert!(exec_rx.try_recv().is_err());
        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn full_executor_queue_does_not_route_an_internal_cross_to_a_dex() {
        let mut rng = RandomCoin::new(Word::default());
        let seller = fixture(false, 11, 18, 1, &mut rng);
        let buyer = fixture(true, 22, 10, 2, &mut rng);
        let pair = Order::from_book_order(&seller).unwrap().index_key().0;
        let (routing, mut route_rx) = routing_fixture(&seller);
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx
            .send(vec![seller.clone(), buyer.clone()])
            .is_ok());
        let (_, prices_rx) = watch::channel(Arc::new(crate::price::PriceSnapshot::for_tests(
            &[(pair.0, pair.1, "2", std::time::Instant::now())],
            &[],
            Duration::from_secs(3_600),
        )));
        let runtime = ClearingRuntime {
            bootstrap,
            prices: prices_rx,
            config: ClearingConfig::default(),
            routing: Some(routing),
        };
        let (_book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        exec_tx
            .try_send(ExecutionBatch {
                filled_notes: Vec::new(),
                group_ends: Vec::new(),
            })
            .unwrap();
        let (snapshot_tx, mut snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            snapshot_tx,
            runtime,
            cancel.clone(),
        ));

        snapshot_rx.changed().await.unwrap();
        assert!(route_rx.try_recv().is_err());
        assert_eq!(
            snapshot_rx.borrow().len(),
            2,
            "orders stay live on a skipped tick"
        );
        assert!(exec_rx.try_recv().unwrap().filled_notes.is_empty());

        tokio::time::advance(Duration::from_secs(1)).await;
        let execution = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(execution.filled_notes.len(), 2);
        assert!(
            route_rx.try_recv().is_err(),
            "internal matches must not route"
        );
        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn worker_rejects_an_invalid_config_before_bootstrap() {
        let invalid = [
            ClearingConfig {
                protocol_fee_ppm: crate::clearing::PPM_DENOMINATOR,
                ..ClearingConfig::default()
            },
            ClearingConfig {
                max_orders_per_side: 0,
                ..ClearingConfig::default()
            },
        ];
        for config in invalid {
            let (_bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
            let (_, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
            let runtime = ClearingRuntime {
                bootstrap,
                prices,
                config,
                routing: None,
            };
            let (_book_tx, book_rx) = mpsc::channel(1);
            let (exec_tx, _exec_rx) = mpsc::channel(1);
            let (snapshot_tx, _) = watch::channel(Arc::new(SwapBookSnapshot::new()));
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                run_worker(
                    book_rx,
                    exec_tx,
                    Duration::from_secs(1),
                    snapshot_tx,
                    runtime,
                ),
            )
            .await
            .expect("an invalid config must fail before waiting for bootstrap");
            assert!(result.is_err());
        }
    }

    #[test]
    fn admits_best_hundred_eligible_on_each_side_without_rejecting_larger_book() {
        for buy in [false, true] {
            let mut rng = RandomCoin::new(Word::default());
            let mut book = ClearingBook::default();
            // Reverse insertion must not reverse durable FIFO. All 130 orders
            // have the same eligible rate; another 20 are ineligible.
            for seq in (1..=150).rev() {
                let (o, r) = match (buy, seq <= 130) {
                    (false, true) => (10, 18),
                    (false, false) => (10, 21),
                    (true, true) => (22, 10),
                    (true, false) => (19, 10),
                };
                book.insert(&fixture(buy, o, r, seq, &mut rng)).unwrap();
            }
            let admitted = admit(&book, buy, 100);
            assert_eq!(
                admitted
                    .iter()
                    .map(|order| order.order().priority_sequence())
                    .collect::<Vec<_>>(),
                (1..=100).collect::<Vec<_>>()
            );
            assert_eq!(book.orders.len(), 150);
            assert_eq!(admit(&book, buy, 200).len(), 130);
        }
    }

    #[test]
    fn pair_batch_keeps_eligible_seller_then_buyer() {
        let mut rng = RandomCoin::new(Word::default());
        let mut book = ClearingBook::default();
        let seller = fixture(false, 10, 18, 2, &mut rng);
        let ineligible_seller = fixture(false, 10, 21, 1, &mut rng);
        let buyer = fixture(true, 22, 10, 4, &mut rng);
        let ineligible_buyer = fixture(true, 19, 10, 3, &mut rng);
        for order in [&buyer, &ineligible_seller, &seller, &ineligible_buyer] {
            book.insert(order).unwrap();
        }
        let (base, quote) = (
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(),
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap(),
        );
        let batch = book
            .build_pair_batch(
                base,
                quote,
                BatchPrice::from_ratio(2, 1).unwrap(),
                &ClearingConfig::default(),
            )
            .unwrap();
        assert_eq!(
            batch
                .orders()
                .map(|order| order.order().id())
                .collect::<Vec<_>>(),
            vec![seller.id(), buyer.id()]
        );
    }

    #[test]
    fn price_precedes_fifo_and_refeed_keeps_original_priority() {
        let mut rng = RandomCoin::new(Word::default());
        let mut book = ClearingBook::default();
        let worse = fixture(false, 10, 19, 1, &mut rng);
        let better = fixture(false, 10, 18, 3, &mut rng);
        let earlier = fixture(false, 20, 36, 2, &mut rng);
        for order in [&worse, &better, &earlier] {
            book.insert(order).unwrap();
        }
        let expected = vec![earlier.id(), better.id(), worse.id()];
        assert_eq!(
            admit(&book, false, 100)
                .iter()
                .map(|order| order.order().id())
                .collect::<Vec<_>>(),
            expected
        );
        book.deactivate(earlier.id());
        assert_eq!(book.orders.len(), 3);
        assert!(!admit(&book, false, 100)
            .iter()
            .any(|order| order.order().id() == earlier.id()));
        book.insert(&earlier).unwrap();
        book.insert(&earlier).unwrap();
        assert_eq!(book.orders.len(), 3);
        assert_eq!(
            admit(&book, false, 100)
                .iter()
                .map(|order| order.order().id())
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn confirmed_child_survives_removal_of_inactive_parent() {
        let mut rng = RandomCoin::new(Word::default());
        let mut book = ClearingBook::default();
        let parent = fixture(false, 20, 36, 2, &mut rng);
        let child = fixture(false, 10, 18, 2, &mut rng);
        book.insert(&parent).unwrap();
        book.deactivate(parent.id());
        // A child may already be indexed when a stale parent event arrives.
        // Both share the inherited price/FIFO key, but not the note ID.
        book.insert(&child).unwrap();
        book.deactivate(parent.id());
        book.deactivate(parent.id());
        assert_eq!(admit(&book, false, 100)[0].order().id(), child.id());
        book.apply(BookUpdate {
            removed: vec![parent.id()],
            active: vec![child.clone()],
        });
        assert_eq!(admit(&book, false, 100)[0].order().id(), child.id());
    }

    #[tokio::test]
    async fn clearer_propagates_closed_update_channel() {
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx.send(Vec::new()).is_ok());
        let (_price_tx, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig::default(),
            routing: None,
        };
        let (book_tx, book_rx) = mpsc::channel(1);
        drop(book_tx);
        let (exec_tx, _exec_rx) = mpsc::channel(1);
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let error = run_worker(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            snapshot_tx,
            runtime,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, MatcherError::IngestStopped), "{error:?}");
    }

    #[test]
    fn removing_last_order_keeps_empty_pair_index_for_reuse() {
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        book.insert(&order).unwrap();
        assert_eq!(book.best_levels_snapshot().len(), 1);
        book.remove(order.id());
        book.remove(order.id());
        assert!(book.orders.is_empty());
        let pair = Order::from_book_order(&order).unwrap().index_key().0;
        assert!(book.pairs[&pair].is_empty());
        assert!(book.best_levels_snapshot().is_empty());
        book.insert(&order).unwrap();
        assert_eq!(book.pairs.len(), 1);
        assert_eq!(admit(&book, false, 100)[0].order().id(), order.id());
    }

    #[tokio::test(start_paused = true)]
    async fn clearer_rejects_invalid_config_before_waiting_for_bootstrap() {
        let (_bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        let (_price_tx, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig {
                protocol_fee_ppm: crate::clearing::PPM_DENOMINATOR,
                ..ClearingConfig::default()
            },
            routing: None,
        };
        let (_book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, _exec_rx) = mpsc::channel(1);
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_worker(
                book_rx,
                exec_tx,
                Duration::from_secs(1),
                snapshot_tx,
                runtime,
            ),
        )
        .await
        .expect("invalid config must not wait for bootstrap");
        assert!(matches!(
            result,
            Err(MatcherError::Config(ClearingError::InvalidConfig))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn clearer_waits_for_bootstrap_then_uses_only_live_events() {
        use crate::clearing::ClearingConfig;
        use crate::price::PriceSnapshot;
        let mut rng = RandomCoin::new(Word::default());
        let seller = fixture(false, 11, 18, 1, &mut rng);
        let buyer = fixture(true, 22, 10, 2, &mut rng);
        let (base, quote) = Order::from_book_order(&seller).unwrap().index_key().0;
        let (_price_tx, prices) = watch::channel(Arc::new(PriceSnapshot::for_tests(
            &[(base, quote, "2", std::time::Instant::now())],
            &[],
            Duration::from_secs(3_600),
        )));
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig::default(),
            routing: None,
        };
        let (book_tx, book_rx) = mpsc::channel(4);
        let (exec_tx, mut exec_rx) = mpsc::channel(4);
        let queued_batches = exec_tx.clone();
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_millis(10),
            snapshot_tx,
            runtime,
            cancel.clone(),
        ));
        book_tx.send(buyer.clone().into()).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(exec_rx.try_recv().is_err());
        assert!(bootstrap_tx.send(vec![seller.clone()]).is_ok());
        let first = tokio::time::timeout(Duration::from_secs(1), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.filled_notes.len(), 2);
        tokio::time::advance(Duration::from_millis(30)).await;
        assert!(exec_rx.try_recv().is_err(), "pending parents matched twice");

        // A definite executor failure returns the original notes with their
        // original priority; the book admits them again without a DB read.
        book_tx.send(seller.into()).await.unwrap();
        book_tx.send(buyer.into()).await.unwrap();
        let retried = tokio::time::timeout(Duration::from_secs(1), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.filled_notes.len(), 2);
        assert!(retried
            .filled_notes
            .iter()
            .all(|note| note.priority_seq <= 2));

        // A new pair of notes arrives solely over the ingestion channel.
        let later_seller = fixture(false, 11, 18, 3, &mut rng);
        let later_buyer = fixture(true, 22, 10, 4, &mut rng);
        book_tx.send(later_seller.into()).await.unwrap();
        book_tx.send(later_buyer.into()).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.filled_notes.len(), 2);
        assert!(second
            .filled_notes
            .iter()
            .all(|note| note.priority_seq >= 3));

        // Shutdown must interrupt a blocked dispatch at the worker boundary,
        // without cancellation checks in the tick or clearing functions.
        for _ in 0..4 {
            queued_batches
                .try_send(ExecutionBatch {
                    filled_notes: Vec::new(),
                    group_ends: Vec::new(),
                })
                .unwrap();
        }
        for order in [
            fixture(false, 11, 18, 5, &mut rng),
            fixture(true, 22, 10, 6, &mut rng),
        ] {
            book_tx.send(order.into()).await.unwrap();
        }
        tokio::time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(queued_batches.capacity(), 0);
        assert!(!task.is_finished());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("matcher must stop even when the executor queue is full")
            .unwrap()
            .unwrap();
    }

    #[test]
    fn apply_skips_an_order_whose_fifo_slot_is_taken_and_keeps_matching() {
        let mut rng = RandomCoin::new(Word::default());
        // Same pair, price and priority: a remainder holds the slot a stale
        // parent activation would also claim.
        let holder = fixture(false, 11, 18, 5, &mut rng);
        let stale = fixture(false, 11, 18, 5, &mut rng);
        let other = fixture(true, 22, 10, 6, &mut rng);
        assert_ne!(holder.id(), stale.id());

        let mut book = ClearingBook::default();
        book.apply(BookUpdate {
            removed: Vec::new(),
            active: vec![holder.clone(), stale.clone(), other.clone()],
        });

        assert!(book.orders.contains_key(&holder.id()));
        assert!(book.orders.contains_key(&other.id()));
        assert!(!book.orders.contains_key(&stale.id()));
        let indexed: usize = book.pairs.values().map(BTreeMap::len).sum();
        assert_eq!(indexed, 2, "the slot still belongs to the holder");
    }
}

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use miden_protocol::asset::AssetId;
use miden_protocol::note::NoteId;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::matcher::{internal_clear, ClearingRuntime};
use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingError, MatchOrder, Order, OrderKey, OrderSide, PairBatch,
};
use crate::matching::types::{BestLevel, SwapBookSnapshot};
use crate::types::{now_millis, BookUpdate, ExecutionBatch, IngestOrder, TokenId};

/// Sent once, after ingestion reconciles persisted notes against the chain.
pub struct ClearingBootstrap {
    pub orders: Vec<IngestOrder>,
    pub decimals: HashMap<TokenId, u8>,
}

/// Live ingestion and startup hydration reject zero amounts before admission.
/// Parse once here and maintain the exact price/FIFO index incrementally.
/// Both directions use requested/offered: lower is always better.
#[derive(Default)]
pub(super) struct ClearingBook {
    orders: HashMap<NoteId, Order>,
    pairs: HashMap<(TokenId, TokenId), BTreeMap<OrderKey, NoteId>>,
}

impl ClearingBook {
    /// Synchronous handoff: no matching can run between parent removal and
    /// remainder activation. Input comes from committed, ordered DB updates.
    fn apply(&mut self, update: BookUpdate) -> Result<()> {
        for id in update.removed {
            self.remove(id);
        }
        for order in update.active {
            self.insert(&order)?;
        }
        Ok(())
    }

    /// Apply the updates queued at the start of the tick. New arrivals wait for
    /// the next receive, so a busy producer cannot postpone matching forever.
    fn apply_pending(&mut self, updates: &mut mpsc::Receiver<BookUpdate>) -> Result<()> {
        for _ in 0..updates.len() {
            let Ok(update) = updates.try_recv() else {
                break;
            };
            self.apply(update)?;
        }
        Ok(())
    }

    fn remove_from_index(&mut self, pair: (TokenId, TokenId), key: OrderKey) {
        if let Some(index) = self.pairs.get_mut(&pair) {
            index.remove(&key);
        }
    }

    pub fn insert(&mut self, order: &IngestOrder) -> Result<()> {
        if let Some(existing) = self.orders.get_mut(&order.note_id) {
            if !existing.is_active() {
                let (pair, key) = existing.index_key();
                let index = self.pairs.entry(pair).or_default();
                ensure!(!index.contains_key(&key), "duplicate price/FIFO priority");
                index.insert(key, order.note_id);
                existing.activate();
            }
            return Ok(());
        }
        let parsed = Order::from_ingest_order(order)?;
        let (pair, key) = parsed.index_key();
        let index = self.pairs.entry(pair).or_default();
        ensure!(!index.contains_key(&key), "duplicate price/FIFO priority");
        index.insert(key, order.note_id);
        self.orders.insert(order.note_id, parsed);
        Ok(())
    }

    pub fn deactivate(&mut self, id: NoteId) {
        if let Some(order) = self.orders.get_mut(&id) {
            if !order.is_active() {
                return;
            }
            let (pair, key) = order.index_key();
            order.deactivate();
            self.remove_from_index(pair, key);
        }
    }

    pub fn remove(&mut self, id: NoteId) {
        if let Some(order) = self.orders.remove(&id) {
            if order.is_active() {
                let (pair, key) = order.index_key();
                self.remove_from_index(pair, key);
            }
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
        selected: &HashSet<NoteId>,
    ) -> Result<PairBatch<'a>, ClearingError> {
        let base_asset = AssetId::new_fungible(base);
        let quote_asset = AssetId::new_fungible(quote);
        let mut sell_orders = self.admit(
            (base, quote),
            OrderSide::SellBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
        )?;
        let mut buy_orders = self.admit(
            (quote, base),
            OrderSide::BuyBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
        )?;
        sell_orders.retain(|order| !selected.contains(&order.order().id()));
        buy_orders.retain(|order| !selected.contains(&order.order().id()));
        PairBatch::new(base_asset, quote_asset, price, sell_orders, buy_orders)
    }

    pub(super) fn snapshot(&self) -> SwapBookSnapshot {
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

pub(super) async fn run_clearer(
    mut book_rx: mpsc::Receiver<BookUpdate>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    mut runtime: ClearingRuntime,
    cancel: CancellationToken,
) -> Result<()> {
    // Configuration is frozen for this worker; validate before admitting orders.
    runtime.config.validate()?;
    let bootstrap = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = &mut runtime.bootstrap => result?,
    };
    let mut book = ClearingBook::default();
    for order in bootstrap.orders {
        if let Err(error) = book.insert(&order) {
            tracing::warn!(note = %order.note_id, %error, "persisted order rejected");
        }
    }
    let mut interval = tokio::time::interval(match_interval);
    loop {
        // Update the book immediately; run matching only on the batch timer.
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            update = book_rx.recv() => {
                let Some(update) = update else { return Ok(()) };
                book.apply(update)?;
            }
            _ = interval.tick() => {
                book.apply_pending(&mut book_rx)?;
                let stopped = tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    stopped = internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now_millis()) => stopped,
                };
                if stopped {
                    return Ok(());
                }
                // Latest order-book levels for the price API's swap-ETA estimates.
                snapshot_tx.send_replace(Arc::new(book.snapshot()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::{
        rand::{FeltRng, RandomCoin},
        utils::Serializable,
    };
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    fn fixture(
        buy: bool,
        offered: u64,
        requested: u64,
        seq: u64,
        rng: &mut RandomCoin,
    ) -> IngestOrder {
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
        IngestOrder {
            note_id: note.id(),
            priority_seq: seq,
            offered_token,
            requested_token,
            offered_amount: offered,
            requested_amount: requested,
            min_fill_step: 1,
            raw_note_data: note.to_bytes().into(),
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
                &HashSet::new(),
            )
            .unwrap();
        assert_eq!(
            batch
                .orders()
                .map(|order| order.order().id())
                .collect::<Vec<_>>(),
            vec![seller.note_id, buyer.note_id]
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
        let expected = vec![earlier.note_id, better.note_id, worse.note_id];
        assert_eq!(
            admit(&book, false, 100)
                .iter()
                .map(|order| order.order().id())
                .collect::<Vec<_>>(),
            expected
        );
        book.deactivate(earlier.note_id);
        assert!(!book.orders[&earlier.note_id].is_active());
        assert_eq!(book.orders.len(), 3);
        assert!(!admit(&book, false, 100)
            .iter()
            .any(|order| order.order().id() == earlier.note_id));
        book.insert(&earlier).unwrap();
        book.insert(&earlier).unwrap();
        assert!(book.orders[&earlier.note_id].is_active());
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
        book.deactivate(parent.note_id);
        book.apply(BookUpdate {
            removed: vec![parent.note_id],
            active: vec![child.clone()],
        })
        .unwrap();
        assert_eq!(admit(&book, false, 100)[0].order().id(), child.note_id);
    }

    #[test]
    fn removing_last_order_keeps_empty_pair_index_for_reuse() {
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        book.insert(&order).unwrap();
        assert_eq!(book.snapshot().len(), 1);
        book.remove(order.note_id);
        book.remove(order.note_id);
        assert!(book.orders.is_empty());
        let pair = (order.offered_token, order.requested_token);
        assert!(book.pairs[&pair].is_empty());
        assert!(book.snapshot().is_empty());
        book.insert(&order).unwrap();
        assert_eq!(book.pairs.len(), 1);
        assert_eq!(admit(&book, false, 100)[0].order().id(), order.note_id);
    }

    #[tokio::test(start_paused = true)]
    async fn clearer_rejects_invalid_config_before_waiting_for_bootstrap() {
        let (_bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        let (_price_tx, prices) = watch::channel(crate::price::PreciseSnapshot::new());
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            pairs: Vec::new(),
            config: ClearingConfig {
                protocol_fee_ppm: crate::clearing::PPM_DENOMINATOR,
                ..ClearingConfig::default()
            },
            max_price_age_ms: 10_000,
            max_source_age_ms: 10_000,
            max_source_skew_ms: 100,
        };
        let (_book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, _exec_rx) = mpsc::channel(1);
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_clearer(
                book_rx,
                exec_tx,
                Duration::from_secs(1),
                snapshot_tx,
                runtime,
                CancellationToken::new(),
            ),
        )
        .await
        .expect("invalid config must not wait for bootstrap");
        assert!(matches!(
            result.unwrap_err().downcast_ref::<ClearingError>(),
            Some(ClearingError::InvalidConfig)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn clearer_waits_for_bootstrap_then_uses_only_live_events() {
        use crate::clearing::{ClearingConfig, ReferencePrice};
        use crate::price::{PreciseSnapshot, PriceData};
        let mut rng = RandomCoin::new(Word::default());
        let seller = fixture(false, 11, 18, 1, &mut rng);
        let buyer = fixture(true, 22, 10, 2, &mut rng);
        let base = seller.offered_token;
        let quote = seller.requested_token;
        let mut prices = PreciseSnapshot::new();
        for (token, value) in [(base, "2"), (quote, "1")] {
            prices.insert(
                token,
                PriceData {
                    usd: value.parse().unwrap(),
                    exact_reference: Some(ReferencePrice::from_decimal(value).unwrap()),
                    source_updated_at_unix_ms: Some(now_millis()),
                    observed_at_unix_ms: 0,
                },
            );
        }
        let observed = now_millis();
        for price in prices.values_mut() {
            price.observed_at_unix_ms = observed;
        }
        let (_price_tx, prices) = watch::channel(prices);
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            pairs: vec![(base, quote)],
            config: ClearingConfig::default(),
            max_price_age_ms: 10_000,
            max_source_age_ms: 10_000,
            max_source_skew_ms: 100,
        };
        let (book_tx, book_rx) = mpsc::channel(4);
        let (exec_tx, mut exec_rx) = mpsc::channel(4);
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_clearer(
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
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![seller.clone()],
                decimals: [(base, 0), (quote, 0)].into_iter().collect(),
            })
            .is_ok());
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
        cancel.cancel();
        task.await.unwrap().unwrap();
    }
}

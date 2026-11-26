use std::collections::{btree_map, BTreeMap, HashMap, HashSet};

use miden_protocol::note::NoteId;
use tokio::sync::mpsc;

use super::maker_index::MakerIndex;
use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingError, MatchOrder, Order, OrderKey, OrderSide, PairBatch,
};
use crate::maker::{CutoffScope, MakerId, MakerUpdate};
use crate::swap_eta::DepthChange;
use crate::types::{BookOrder, BookUpdate, TokenId};

mod bounded_price;

pub(super) use bounded_price::BoundedPrice;

/// Sent once, after ingestion reconciles persisted notes against the chain.
pub struct ClearingBootstrap {
    pub orders: Vec<BookOrder>,
    /// Every maker cancel-all barrier, applied to later arrivals.
    pub cutoffs: Vec<(MakerId, CutoffScope, u64)>,
}

/// Live ingestion and startup hydration reject zero amounts before admission.
/// Parse once here and maintain the exact price/FIFO index incrementally.
/// Both directions use requested/offered: lower is always better.
#[derive(Default)]
pub(crate) struct ClearingBook {
    orders: HashMap<NoteId, Order>,
    pairs: HashMap<(TokenId, TokenId), BTreeMap<OrderKey, NoteId>>,
    makers: MakerIndex,
    /// Where index changes go for the price API's depth mirror. Sending never
    /// blocks; a closed receiver is ignored.
    depth_tx: Option<mpsc::UnboundedSender<DepthChange>>,
    /// When each order was first matched (Unix ms), kept while the matcher
    /// keeps choosing it: in flight, or back and chosen again. Forgotten when
    /// the order leaves the book, or when it is back and a clearing tick does
    /// not choose it (for whatever reason: price, window, no counterparty).
    /// In memory only: a restart starts it again.
    matched_since: HashMap<NoteId, u64>,
    /// Orders kept but off the matchable index: in a settlement, or routed.
    /// Small, so counting live maker orders costs the makers and these, not
    /// the whole book.
    inactive: HashSet<NoteId>,
}

impl ClearingBook {
    pub(super) fn with_depth(depth_tx: mpsc::UnboundedSender<DepthChange>) -> Self {
        Self {
            depth_tx: Some(depth_tx),
            ..Self::default()
        }
    }

    fn send_depth(&self, change: DepthChange) {
        if let Some(depth_tx) = &self.depth_tx {
            let _ = depth_tx.send(change);
        }
    }

    /// Expiry gates new selection only. Already selected proofs/settlements
    /// continue, and give-backs/remainders are checked again on their next tick.
    pub(super) fn remove_expired(&mut self, now_ms: u64, buffer_ms: u64) {
        let expired = self.makers.expired(now_ms, buffer_ms);
        for id in expired {
            if self.orders.get(&id).is_some_and(|order| order.is_active()) {
                self.remove(id);
            }
        }
    }

    /// Synchronous handoff: no matching can run between parent removal and
    /// remainder activation. Input comes from committed, ordered DB updates.
    pub(super) fn apply(&mut self, update: BookUpdate) {
        for maker_update in update.maker_updates {
            self.apply_maker_update(maker_update);
        }
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
        if !self.makers.admit(order) {
            // A maker cancel bars it; the database stores it Stopped.
            self.remove(order.id());
            tracing::debug!(note_id = %order.id(), "cancelled maker order left out of the book");
            return;
        }
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
                    self.send_depth(DepthChange::Removed {
                        pair,
                        rate: key.rate,
                        volume: key.rate.offered,
                    });
                }
            }
        }
    }

    pub(super) fn insert(&mut self, order: &BookOrder) -> Result<(), ClearingError> {
        let id = order.id();
        self.orders.insert(id, Order::from_book_order(order)?);
        self.add_to_index(id)
    }

    fn add_to_index(&mut self, id: NoteId) -> Result<(), ClearingError> {
        let order = self
            .orders
            .get(&id)
            .ok_or(ClearingError::InternalInvariant("missing order to index"))?;
        let (pair, key) = order.index_key();
        let index = self.pairs.entry(pair).or_default();
        match index.entry(key) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(id);
                self.send_depth(DepthChange::Added {
                    pair,
                    rate: key.rate,
                    volume: key.rate.offered,
                });
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
        self.inactive.remove(&id);
        Ok(())
    }

    pub fn deactivate(&mut self, id: NoteId) {
        if let Some(order) = self.orders.get_mut(&id) {
            let (pair, key) = order.index_key();
            // The order record remains for recovery, but the active index must
            // no longer expose it to matching or RFQ.
            order.deactivate();
            self.inactive.insert(id);
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
                // Maker orders are never routed (ADR 0003).
                let orders = index
                    .values()
                    .filter_map(|id| self.orders.get(id))
                    .filter(|order| order.can_route_to_rfq())
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

    /// Apply a committed maker update and remove any orders it cancels.
    pub(super) fn apply_maker_update(&mut self, update: MakerUpdate) {
        let cancelled = self.makers.apply(update);
        if !cancelled.is_empty() {
            tracing::debug!(
                cancelled = cancelled.len(),
                "maker cancel dropped book entries"
            );
        }
        for id in cancelled {
            self.remove(id);
        }
    }

    pub(super) fn raise_maker_cutoff(
        &mut self,
        maker_id: MakerId,
        scope: CutoffScope,
        cutoff: u64,
    ) {
        self.makers.raise_cutoff(maker_id, scope, cutoff);
    }

    pub fn remove(&mut self, id: NoteId) {
        self.makers.forget(id);
        self.matched_since.remove(&id);
        self.inactive.remove(&id);
        if let Some(order) = self.orders.remove(&id) {
            let (pair, key) = order.index_key();
            self.remove_from_index(pair, key, id);
        }
    }

    /// After a clearing tick that sent `sent`: start the clock of each order
    /// matched for the first time, and stop it for any order back in the
    /// book that the tick did not choose.
    pub(super) fn track_matched(&mut self, sent: &[NoteId], now_ms: u64) {
        let sent: HashSet<NoteId> = sent.iter().copied().collect();
        let orders = &self.orders;
        self.matched_since.retain(|id, _| {
            orders
                .get(id)
                .is_some_and(|order| !order.is_active() || sent.contains(id))
        });
        for id in sent {
            self.matched_since.entry(id).or_insert(now_ms);
        }
    }

    /// When the order matched longest ago and still not settled was first
    /// matched (Unix ms).
    pub(super) fn oldest_matched_since(&self) -> Option<u64> {
        self.matched_since.values().copied().min()
    }

    /// Each maker's orders the matcher can choose now: its entries less those
    /// off the index. Costs the makers plus the inactive orders, not the book.
    pub(super) fn live_orders_per_maker(&self) -> BTreeMap<MakerId, u64> {
        let mut live: BTreeMap<MakerId, u64> = self.makers.entry_counts().collect();
        for id in &self.inactive {
            if let Some(count) = self
                .makers
                .tag(id)
                .and_then(|tag| live.get_mut(&tag.maker_id))
            {
                *count = count.saturating_sub(1);
            }
        }
        live
    }

    /// Matchable orders on a pair outside `configured`, which must list both
    /// token orders of each pair: users' orders this solver never clears.
    pub(super) fn orders_outside(&self, configured: &HashSet<(TokenId, TokenId)>) -> u64 {
        self.pairs
            .iter()
            .filter(|(pair, _)| !configured.contains(pair))
            .map(|(_, index)| index.len() as u64)
            .sum()
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
            for id in index.values() {
                if selected.len() == limit {
                    break;
                }
                let order = self
                    .orders
                    .get(id)
                    .ok_or(ClearingError::InternalInvariant("missing indexed order"))?;
                let Some(prepared) = order.prepare_if_eligible(side, price, fee_ppm)? else {
                    // Eligibility is monotone in this exact price ordering.
                    break;
                };
                selected.push(prepared);
            }
        }
        Ok(selected)
    }

    /// The pair's batch at `price`: the eligible orders of each side, best
    /// first, up to the window. A maker's own crossing orders are cancelled
    /// at submit (ADR 0009), so admission does not look at makers.
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

    /// Every non-empty directed pair's levels, best first, read straight from
    /// the index: the reference the depth mirror must match.
    #[cfg(test)]
    pub(super) fn levels(&self) -> crate::matching::types::SwapBookSnapshot {
        use crate::matching::types::BookLevel;

        self.pairs
            .iter()
            .filter(|(_, index)| !index.is_empty())
            .map(|(&pair, index)| {
                let mut levels: Vec<BookLevel> = Vec::new();
                for (key, id) in index {
                    let Some(order) = self.orders.get(id) else {
                        continue;
                    };
                    let volume = order.offered_asset().amount().as_u64();
                    match levels.last_mut() {
                        Some(level) if level.rate == key.rate => {
                            level.volume = level.volume.saturating_add(volume);
                        }
                        _ => levels.push(BookLevel {
                            rate: key.rate,
                            volume,
                        }),
                    }
                }
                (pair, levels)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::matcher::run_matcher;
    use crate::matcher::matcher::{run_worker, ClearingRuntime};
    use crate::matcher::test_ticks::{after_first_tick, after_next_tick};
    use crate::matcher::MatcherError;
    use crate::swap_eta::DepthBook;
    use crate::types::{now_millis, ExecutionBatch};
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

    pub(super) fn fixture(
        buy: bool,
        offered: u64,
        requested: u64,
        seq: u64,
        rng: &mut RandomCoin,
    ) -> BookOrder {
        fixture_of_type(buy, offered, requested, seq, rng, NoteType::Public)
    }

    fn fixture_of_type(
        buy: bool,
        offered: u64,
        requested: u64,
        seq: u64,
        rng: &mut RandomCoin,
        note_type: NoteType,
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
            .note_type(note_type)
            .offered_asset(FungibleAsset::new(offered_token, offered).unwrap())
            .build()
            .unwrap()
            .into();
        BookOrder {
            priority_seq: seq,
            arrival_unix: 1,
            note: Arc::new(note),
            maker: None,
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

    /// Drain `changes` into `depth`, then compare it with the index itself.
    fn assert_mirrors(
        book: &ClearingBook,
        depth: &mut DepthBook,
        changes: &mut mpsc::UnboundedReceiver<DepthChange>,
    ) {
        depth.drain(changes);
        assert_eq!(depth.snapshot(), book.levels());
    }

    #[test]
    fn depth_mirror_matches_the_index_through_every_change() {
        let mut rng = RandomCoin::new(Word::default());
        let (depth_tx, mut changes) = mpsc::unbounded_channel();
        let mut book = ClearingBook::with_depth(depth_tx);
        let mut depth = DepthBook::default();
        let first = fixture(false, 10, 18, 1, &mut rng);
        let same_rate = fixture(false, 20, 36, 2, &mut rng);
        let worse = fixture(false, 5, 11, 3, &mut rng);
        let buyer = fixture(true, 30, 10, 4, &mut rng);
        for order in [&first, &same_rate, &worse, &buyer] {
            book.insert(order).unwrap();
        }
        assert_mirrors(&book, &mut depth, &mut changes);
        assert_eq!(depth.snapshot().len(), 2);

        book.deactivate(first.id());
        assert_mirrors(&book, &mut depth, &mut changes);
        book.reactivate(first.id()).unwrap();
        // Indexing an already indexed order changes nothing.
        book.insert(&worse).unwrap();
        assert_mirrors(&book, &mut depth, &mut changes);
        book.remove(same_rate.id());
        book.remove(same_rate.id());
        book.deactivate(same_rate.id());
        assert_mirrors(&book, &mut depth, &mut changes);
        for order in [&first, &worse, &buyer] {
            book.remove(order.id());
        }
        assert_mirrors(&book, &mut depth, &mut changes);
        assert!(depth.snapshot().is_empty());
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
        assert!(book.levels().is_empty());
        routing.dispatch(&mut book, 101).unwrap();
        assert!(
            route_rx.try_recv().is_err(),
            "RFQ must not dispatch a reserved note twice"
        );
        routing.release_expired(&mut book, 109).unwrap();
        assert!(book.levels().is_empty());
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
        assert!(book.levels().is_empty());
    }

    /// Live = matchable now: off the index (sent to the executor) is not
    /// live; a give-back is again.
    #[test]
    fn live_orders_per_maker_and_orders_outside_the_plan() {
        let mut rng = RandomCoin::new(Word::default());
        let mut book = ClearingBook::default();
        let a1 = tagged(fixture(false, 10, 18, 1, &mut rng), 7, 1);
        let a2 = tagged(fixture(false, 10, 19, 2, &mut rng), 7, 2);
        let b1 = tagged(fixture(true, 30, 10, 3, &mut rng), 8, 1);
        let user = fixture(true, 30, 11, 4, &mut rng);
        for order in [&a1, &a2, &b1, &user] {
            book.insert_or_skip(order);
        }
        let live = |book: &ClearingBook| book.live_orders_per_maker();
        assert_eq!(live(&book), BTreeMap::from([(7, 2), (8, 1)]));

        book.deactivate(a1.id());
        book.track_matched(&[a1.id()], 1);
        assert_eq!(live(&book), BTreeMap::from([(7, 1), (8, 1)]));
        // Off the index without being matched (routed).
        book.deactivate(a2.id());
        assert_eq!(live(&book), BTreeMap::from([(7, 0), (8, 1)]));
        book.reactivate(a1.id()).unwrap();
        book.reactivate(a2.id()).unwrap();
        assert_eq!(live(&book), BTreeMap::from([(7, 2), (8, 1)]));
        book.remove(b1.id());
        assert_eq!(live(&book), BTreeMap::from([(7, 2)]));
        book.deactivate(a1.id());
        book.remove(a1.id());
        assert_eq!(live(&book), BTreeMap::from([(7, 1)]));
        assert!(book.inactive.is_empty());

        let a: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let b: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        assert_eq!(book.orders_outside(&HashSet::from([(a, b), (b, a)])), 0);
        // Not configured: every matchable order on it counts, both sides.
        assert_eq!(book.orders_outside(&HashSet::new()), 2);
        book.deactivate(user.id());
        assert_eq!(book.orders_outside(&HashSet::new()), 1);
    }

    pub(super) fn tagged(mut order: BookOrder, maker_id: MakerId, root_seq: u64) -> BookOrder {
        order.maker = Some(crate::maker::MakerTag {
            maker_id,
            root_seq,
            expires_at_unix_ms: None,
        });
        order
    }

    #[test]
    fn rfq_never_routes_a_maker_order() {
        let mut rng = RandomCoin::new(Word::default());
        let quote = tagged(fixture(false, 10, 18, 1, &mut rng), 7, 1);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&quote);
        let (mut routing, mut route_rx) = routing_fixture(&quote);
        routing.dispatch(&mut book, 100).unwrap();
        assert!(
            route_rx.try_recv().is_err(),
            "a maker order is never routed"
        );
        assert_eq!(
            admit(&book, false, 100).len(),
            1,
            "it still clears internally"
        );
    }

    #[test]
    fn a_maker_cutoff_drops_entries_and_bars_later_updates() {
        let mut rng = RandomCoin::new(Word::default());
        let old = tagged(fixture(false, 10, 18, 1, &mut rng), 7, 1);
        let new = tagged(fixture(false, 10, 18, 2, &mut rng), 7, 9);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&old);
        book.insert_or_skip(&new);
        book.apply_maker_update(MakerUpdate::CutoffRaised {
            maker_id: 7,
            scope: CutoffScope::all(),
            cutoff: 5,
        });
        assert!(!book.orders.contains_key(&old.id()));
        assert!(book.orders.contains_key(&new.id()));
        // A stale activation is still refused by the cutoff.
        book.apply(old.clone().into());
        assert!(!book.orders.contains_key(&old.id()));
        assert_eq!(admit(&book, false, 100).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_cancel_binds_the_next_tick() {
        let mut rng = RandomCoin::new(Word::default());
        let seller = tagged(fixture(false, 11, 18, 1, &mut rng), 7, 1);
        let buyer = fixture(true, 22, 10, 2, &mut rng);
        let pair = Order::from_book_order(&seller).unwrap().index_key().0;
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![seller.clone(), buyer.clone()],
                cutoffs: Vec::new(),
            })
            .is_ok());
        let (_, prices_rx) = watch::channel(Arc::new(crate::price::PriceSnapshot::for_tests(
            &[(pair.0, pair.1, "2", std::time::Instant::now())],
            &[],
            Duration::from_secs(3_600),
        )));
        // The cancel committed while the book was loading.
        let (book_tx, book_rx) = mpsc::channel(1);
        book_tx
            .try_send(BookUpdate {
                removed: Vec::new(),
                active: Vec::new(),
                maker_updates: vec![MakerUpdate::CutoffRaised {
                    maker_id: 7,
                    scope: CutoffScope::all(),
                    cutoff: 2,
                }],
            })
            .unwrap();
        let runtime = ClearingRuntime {
            bootstrap,
            prices: prices_rx,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (depth_tx, mut depth_rx) = mpsc::unbounded_channel();
        let mut depth = DepthBook::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            depth_tx,
            runtime,
            cancel.clone(),
        ));
        after_first_tick().await;
        depth.drain(&mut depth_rx);
        assert!(
            exec_rx.try_recv().is_err(),
            "the cancelled quote never reached clearing"
        );
        assert_eq!(depth.snapshot().len(), 1, "only the buyer's side is left");
        // The ordered book stream remains open for subsequent updates.
        after_next_tick().await;
        depth.drain(&mut depth_rx);
        assert_eq!(depth.snapshot().len(), 1);
        assert!(!task.is_finished());
        cancel.cancel();
        task.await.unwrap().unwrap();
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
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![order.clone()],
                cutoffs: Vec::new(),
            })
            .is_ok());
        // No price snapshot: direct clearing cannot match, but RFQ still can.
        let (_, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 0,
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
        let (depth_tx, mut depth_rx) = mpsc::unbounded_channel();
        let mut depth = DepthBook::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            depth_tx,
            runtime,
            cancel.clone(),
        ));
        // The first tick cannot clear, so it must not bypass internal
        // matching by routing an order to an external DEX either.
        after_first_tick().await;
        depth.drain(&mut depth_rx);
        assert_eq!(depth.snapshot().len(), 1);
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
            .send(ClearingBootstrap {
                orders: vec![seller.clone(), buyer.clone()],
                cutoffs: Vec::new(),
            })
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
            maker_settlement_buffer_ms: 0,
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
        let (depth_tx, mut depth_rx) = mpsc::unbounded_channel();
        let mut depth = DepthBook::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            depth_tx,
            runtime,
            cancel.clone(),
        ));

        after_first_tick().await;
        depth.drain(&mut depth_rx);
        assert!(route_rx.try_recv().is_err());
        assert_eq!(
            depth.snapshot().len(),
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
                maker_settlement_buffer_ms: 0,
                routing: None,
            };
            let (_book_tx, book_rx) = mpsc::channel(1);
            let (exec_tx, _exec_rx) = mpsc::channel(1);
            let (depth_tx, _) = mpsc::unbounded_channel();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                run_worker(book_rx, exec_tx, Duration::from_secs(1), depth_tx, runtime),
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
            maker_updates: Vec::new(),
        });
        assert_eq!(admit(&book, false, 100)[0].order().id(), child.id());
    }

    #[tokio::test]
    async fn clearer_propagates_closed_update_channel() {
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: Vec::new(),
                cutoffs: Vec::new(),
            })
            .is_ok());
        let (_price_tx, prices) = watch::channel(Arc::new(crate::price::PriceSnapshot::default()));
        let runtime = ClearingRuntime {
            bootstrap,
            prices,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (book_tx, book_rx) = mpsc::channel(1);
        drop(book_tx);
        let (exec_tx, _exec_rx) = mpsc::channel(1);
        let (depth_tx, _depth_rx) = mpsc::unbounded_channel();
        let error = run_worker(book_rx, exec_tx, Duration::from_secs(1), depth_tx, runtime)
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
        assert_eq!(book.levels().len(), 1);
        book.remove(order.id());
        book.remove(order.id());
        assert!(book.orders.is_empty());
        let pair = Order::from_book_order(&order).unwrap().index_key().0;
        assert!(book.pairs[&pair].is_empty());
        assert!(book.levels().is_empty());
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
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (_book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, _exec_rx) = mpsc::channel(1);
        let (depth_tx, _depth_rx) = mpsc::unbounded_channel();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_worker(book_rx, exec_tx, Duration::from_secs(1), depth_tx, runtime),
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
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (book_tx, book_rx) = mpsc::channel(4);
        let (exec_tx, mut exec_rx) = mpsc::channel(4);
        let queued_batches = exec_tx.clone();
        let (depth_tx, _depth_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_millis(10),
            depth_tx,
            runtime,
            cancel.clone(),
        ));
        book_tx.send(buyer.clone().into()).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(exec_rx.try_recv().is_err());
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![seller.clone()],
                cutoffs: Vec::new(),
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

        let (depth_tx, mut changes) = mpsc::unbounded_channel();
        let mut book = ClearingBook::with_depth(depth_tx);
        let mut depth = DepthBook::default();
        book.apply(BookUpdate {
            removed: Vec::new(),
            active: vec![holder.clone(), stale.clone(), other.clone()],
            maker_updates: Vec::new(),
        });

        assert!(book.orders.contains_key(&holder.id()));
        assert!(book.orders.contains_key(&other.id()));
        assert!(!book.orders.contains_key(&stale.id()));
        let indexed: usize = book.pairs.values().map(BTreeMap::len).sum();
        assert_eq!(indexed, 2, "the slot still belongs to the holder");
        // The skipped order never reached the depth mirror, so dropping it
        // must not remove the holder's volume there either.
        assert_mirrors(&book, &mut depth, &mut changes);
        book.remove(stale.id());
        assert_mirrors(&book, &mut depth, &mut changes);
        book.remove(holder.id());
        assert_mirrors(&book, &mut depth, &mut changes);
    }
    #[test]
    fn expiry_removes_active_orders_but_preserves_selected_work_and_checks_givebacks() {
        let mut rng = RandomCoin::new(Word::default());
        let mut quote = tagged(fixture(false, 10, 18, 1, &mut rng), 7, 1);
        quote.maker.as_mut().unwrap().expires_at_unix_ms = Some(1000);
        let (depth_tx, mut changes) = mpsc::unbounded_channel();
        let mut book = ClearingBook::with_depth(depth_tx);
        let mut depth = DepthBook::default();
        book.insert_or_skip(&quote);
        book.remove_expired(899, 100);
        assert!(book.orders.contains_key(&quote.id()));
        book.deactivate(quote.id());
        book.remove_expired(900, 100);
        assert!(
            book.orders.contains_key(&quote.id()),
            "selected work continues"
        );
        book.apply(BookUpdate::from(quote.clone()));
        book.remove_expired(900, 100);
        assert!(
            !book.orders.contains_key(&quote.id()),
            "expired giveback cannot be selected again"
        );
        assert!(book.levels().is_empty());
        assert_mirrors(&book, &mut depth, &mut changes);
    }
    #[tokio::test(start_paused = true)]
    async fn an_expired_maker_order_is_not_selected_after_hydration() {
        let mut rng = RandomCoin::new(Word::default());
        let mut seller = tagged(fixture(false, 11, 18, 1, &mut rng), 7, 1);
        seller.maker.as_mut().unwrap().expires_at_unix_ms = Some(now_millis() + 100);
        let buyer = fixture(true, 22, 10, 2, &mut rng);
        let pair = Order::from_book_order(&seller).unwrap().index_key().0;
        let (bootstrap_tx, bootstrap) = tokio::sync::oneshot::channel();
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![seller.clone(), buyer.clone()],
                cutoffs: Vec::new(),
            })
            .is_ok());
        let (_, prices_rx) = watch::channel(Arc::new(crate::price::PriceSnapshot::for_tests(
            &[(pair.0, pair.1, "2", std::time::Instant::now())],
            &[],
            Duration::from_secs(3_600),
        )));
        let (_book_tx, book_rx) = mpsc::channel(1);
        let runtime = ClearingRuntime {
            bootstrap,
            prices: prices_rx,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 100,
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (depth_tx, mut depth_rx) = mpsc::unbounded_channel();
        let mut depth = DepthBook::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_matcher(
            book_rx,
            exec_tx,
            Duration::from_secs(1),
            depth_tx,
            runtime,
            cancel.clone(),
        ));
        after_first_tick().await;
        depth.drain(&mut depth_rx);
        assert!(
            exec_rx.try_recv().is_err(),
            "the expired quote never reached clearing"
        );
        assert_eq!(depth.snapshot().len(), 1, "only the buyer's side is left");
        // The ordered book stream remains open for subsequent updates.
        after_next_tick().await;
        depth.drain(&mut depth_rx);
        assert_eq!(depth.snapshot().len(), 1);
        assert!(!task.is_finished());
        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    /// An order eligible at price 2: a buy of 10 base for 22 quote, or a
    /// sell of 10 base for 18 quote.
    fn eligible(buy: bool, seq: u64, rng: &mut RandomCoin) -> BookOrder {
        if buy {
            fixture(true, 22, 10, seq, rng)
        } else {
            fixture(false, 10, 18, seq, rng)
        }
    }

    #[test]
    fn the_matched_clock_runs_until_the_order_settles_or_is_passed_over() {
        use crate::matcher::matcher::internal_clear;
        use crate::price::PriceSnapshot;
        let mut rng = RandomCoin::new(Word::default());
        let seller = eligible(false, 1, &mut rng);
        let buyer = eligible(true, 2, &mut rng);
        let (base, quote) = Order::from_book_order(&seller).unwrap().index_key().0;
        let quote_at_two = [(base, quote, "2", std::time::Instant::now())];
        let runtime = |priced: bool| ClearingRuntime {
            bootstrap: tokio::sync::oneshot::channel().1,
            prices: watch::channel(Arc::new(PriceSnapshot::for_tests(
                if priced { &quote_at_two[..] } else { &[] },
                &[],
                Duration::from_secs(3_600),
            )))
            .1,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (priced, unpriced) = (runtime(true), runtime(false));
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&seller);
        book.insert_or_skip(&buyer);
        let back = |book: &mut ClearingBook, orders: Vec<BookOrder>| {
            book.apply(BookUpdate {
                removed: Vec::new(),
                active: orders,
                maker_updates: Vec::new(),
            })
        };
        assert_eq!(book.oldest_matched_since(), None);

        // Matched: the clock starts.
        internal_clear(&mut book, &priced, &exec_tx).unwrap();
        exec_rx.try_recv().expect("the two orders cross");
        let first = book.oldest_matched_since().expect("the clock started");

        // Released and matched again: the clock keeps its start.
        back(&mut book, vec![seller.clone(), buyer.clone()]);
        internal_clear(&mut book, &priced, &exec_tx).unwrap();
        exec_rx.try_recv().expect("matched again");
        assert_eq!(book.oldest_matched_since(), Some(first));

        // Back again, and a tick without a price for the pair does not
        // choose them: the clock stops (the price has its own alert).
        back(&mut book, vec![seller.clone(), buyer.clone()]);
        internal_clear(&mut book, &unpriced, &exec_tx).unwrap();
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(book.oldest_matched_since(), None);

        // Matched again, back, and the buyer is gone: a priced tick passes
        // the seller over, and its clock stops.
        internal_clear(&mut book, &priced, &exec_tx).unwrap();
        exec_rx.try_recv().expect("matched again");
        assert!(book.oldest_matched_since().unwrap() >= first);
        book.remove(buyer.id());
        back(&mut book, vec![seller.clone()]);
        internal_clear(&mut book, &priced, &exec_tx).unwrap();
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(book.oldest_matched_since(), None);

        // Matched and settled: the order leaves the book, and its clock too.
        book.insert_or_skip(&buyer);
        internal_clear(&mut book, &priced, &exec_tx).unwrap();
        exec_rx.try_recv().expect("the two orders cross");
        assert!(book.oldest_matched_since().is_some());
        book.apply(BookUpdate {
            removed: vec![seller.id(), buyer.id()],
            active: Vec::new(),
            maker_updates: Vec::new(),
        });
        assert_eq!(book.oldest_matched_since(), None);
    }
}

#[cfg(test)]
mod metrics_cost;

use std::collections::{btree_map, BTreeMap, HashMap};

use miden_protocol::note::NoteId;
use tokio::sync::mpsc;

use super::maker_index::MakerIndex;
use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingError, MatchOrder, Order, OrderKey, OrderSide, PairBatch,
};
use crate::maker::{CutoffScope, MakerId, MakerUpdate};
use crate::swap_eta::DepthChange;
use crate::types::{BookOrder, BookUpdate, ExecutionBatch, TokenId};

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
        if let (Some(tag), Some(stored)) = (
            self.makers.tag(&order.id()),
            self.orders.get_mut(&order.id()),
        ) {
            stored.set_maker(tag);
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
        let attribution = match &update {
            MakerUpdate::OrdersAttributed { order_ids, tag, .. } => Some((order_ids.clone(), *tag)),
            _ => None,
        };
        let cancelled = self.makers.apply(update);
        if let Some((order_ids, tag)) = attribution {
            for id in order_ids {
                if let Some(order) = self.orders.get_mut(&id) {
                    order.set_maker(tag);
                }
            }
        }
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
        mut self_trade: Option<&mut SelfTrade>,
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
                // A sit-out takes no slot: the next order in line moves up.
                if let Some(self_trade) = self_trade.as_deref_mut() {
                    if !self_trade.keeps(self, order, side)? {
                        continue;
                    }
                }
                selected.push(prepared);
            }
        }
        Ok(selected)
    }

    /// The pair's batch, and the sit-outs of every crossed maker (ADR 0005).
    pub(super) fn build_pair_batch<'a>(
        &'a self,
        base: TokenId,
        quote: TokenId,
        price: BatchPrice,
        config: &ClearingConfig,
    ) -> Result<(PairBatch<'a>, Vec<MakerDecision>), ClearingError> {
        let mut self_trade = config.self_trade_prevention.then(|| SelfTrade {
            base,
            quote,
            price,
            fee_ppm: config.protocol_fee_ppm,
            decided: Vec::new(),
        });
        let sell_orders = self.admit(
            (base, quote),
            OrderSide::SellBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
            self_trade.as_mut(),
        )?;
        let buy_orders = self.admit(
            (quote, base),
            OrderSide::BuyBase,
            price,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
            self_trade.as_mut(),
        )?;
        let decisions = self_trade
            .map(SelfTrade::into_decisions)
            .unwrap_or_default();
        Ok((PairBatch::new(price, sell_orders, buy_orders), decisions))
    }

    /// Give each crossed maker's sit-outs to its first filled order in the
    /// pair, to be cancelled if that settlement is reserved. A crossed maker
    /// has only kept orders in the batch, so any fill of its is a kept one.
    pub(super) fn attach_self_trade_cancels(
        &self,
        decisions: Vec<MakerDecision>,
        execution: &mut ExecutionBatch,
    ) {
        for decision in decisions {
            let first_fill = execution.filled_notes.iter_mut().find(|filled| {
                self.orders
                    .get(&filled.note_id)
                    .and_then(Order::maker_tag)
                    .is_some_and(|tag| tag.maker_id == decision.maker_id)
            });
            if let Some(filled) = first_fill {
                filled.cancel_on_reserve = decision
                    .sit_out
                    .iter()
                    .filter_map(|id| self.orders.get(id))
                    .map(Order::to_book_order)
                    .collect();
            }
        }
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

/// A crossed maker's orders on a pair that must not trade this round
/// (ADR 0005); cancelled if a kept order of the maker fills.
pub(super) struct MakerDecision {
    pub(super) maker_id: MakerId,
    pub(super) sit_out: Vec<NoteId>,
}

/// A crossed maker's side this round: its orders on `winner` newer than
/// `cutoff` trade, every other eligible order sits out.
struct Crossed {
    winner: OrderSide,
    cutoff: u64,
    sit_out: Vec<NoteId>,
}

/// Self-trade prevention for one pair and round (ADR 0005). Each maker the
/// walk meets is decided once, from all its eligible orders on the pair.
struct SelfTrade {
    base: TokenId,
    quote: TokenId,
    price: BatchPrice,
    fee_ppm: u32,
    /// A pair has a handful of makers: a linear scan, no hashing.
    decided: Vec<(MakerId, Option<Crossed>)>,
}

impl SelfTrade {
    /// Whether `order`, eligible on `side`, may enter matching.
    fn keeps(
        &mut self,
        book: &ClearingBook,
        order: &Order,
        side: OrderSide,
    ) -> Result<bool, ClearingError> {
        let Some(tag) = order.maker_tag() else {
            return Ok(true);
        };
        let at = match self
            .decided
            .iter()
            .position(|(maker_id, _)| *maker_id == tag.maker_id)
        {
            Some(at) => at,
            None => {
                let crossed = self.decide(book, tag.maker_id)?;
                self.decided.push((tag.maker_id, crossed));
                self.decided.len() - 1
            }
        };
        Ok(match &self.decided[at].1 {
            None => true,
            Some(crossed) => side == crossed.winner && tag.root_seq > crossed.cutoff,
        })
    }

    /// The side of the maker's newest eligible order wins, from just after
    /// the newest order on the other side: the same orders a replay in
    /// submission order would leave. `None` when its eligible orders on the
    /// pair are all on one side.
    fn decide(
        &self,
        book: &ClearingBook,
        maker_id: MakerId,
    ) -> Result<Option<Crossed>, ClearingError> {
        let mut eligible = Vec::new();
        let (mut newest_sell, mut newest_buy) = (None, None);
        for id in book.makers.orders_on_pair(maker_id, self.base, self.quote) {
            // Inactive orders are in flight, not in the book.
            let Some(order) = book.orders.get(&id).filter(|order| order.is_active()) else {
                continue;
            };
            let Some(tag) = order.maker_tag() else {
                continue;
            };
            let tokens = (
                order.offered_asset().faucet_id(),
                order.requested_asset().faucet_id(),
            );
            let side = if tokens == (self.base, self.quote) {
                OrderSide::SellBase
            } else if tokens == (self.quote, self.base) {
                OrderSide::BuyBase
            } else {
                continue;
            };
            if !order.is_eligible(side, self.price, self.fee_ppm)? {
                continue;
            }
            let newest = match side {
                OrderSide::SellBase => &mut newest_sell,
                OrderSide::BuyBase => &mut newest_buy,
            };
            *newest = (*newest).max(Some(tag.root_seq));
            eligible.push((id, side, tag.root_seq));
        }
        let (Some(newest_sell), Some(newest_buy)) = (newest_sell, newest_buy) else {
            return Ok(None);
        };
        let (winner, cutoff) = if newest_buy > newest_sell {
            (OrderSide::BuyBase, newest_sell)
        } else {
            (OrderSide::SellBase, newest_buy)
        };
        let sit_out = eligible
            .into_iter()
            .filter(|&(_, side, seq)| side != winner || seq <= cutoff)
            .map(|(id, _, _)| id)
            .collect();
        Ok(Some(Crossed {
            winner,
            cutoff,
            sit_out,
        }))
    }

    fn into_decisions(self) -> Vec<MakerDecision> {
        self.decided
            .into_iter()
            .filter_map(|(maker_id, crossed)| {
                Some(MakerDecision {
                    maker_id,
                    sit_out: crossed?.sit_out,
                })
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
            None,
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
        let public = fixture(false, 10, 18, 2, &mut rng);
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

        // A public order becomes a maker order when its maker's submit
        // arrives after ingest: from then on it is not routed either.
        book.insert_or_skip(&public);
        book.apply_maker_update(MakerUpdate::OrdersAttributed {
            order_ids: vec![public.id()],
            tag: crate::maker::MakerTag {
                maker_id: 7,
                root_seq: 2,
                expires_at_unix_ms: None,
            },
            cancelled: false,
        });
        routing.dispatch(&mut book, 101).unwrap();
        assert!(route_rx.try_recv().is_err());
    }

    #[test]
    fn late_attribution_preserves_an_inactive_orders_lifecycle() {
        let mut rng = RandomCoin::new(Word::default());
        let public = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&public);
        book.deactivate(public.id());
        book.apply(BookUpdate {
            removed: Vec::new(),
            active: Vec::new(),
            maker_updates: vec![MakerUpdate::OrdersAttributed {
                order_ids: vec![public.id()],
                tag: crate::maker::MakerTag {
                    maker_id: 7,
                    root_seq: 1,
                    expires_at_unix_ms: None,
                },
                cancelled: false,
            }],
        });
        let stored = &book.orders[&public.id()];
        assert!(
            !stored.is_active(),
            "metadata must not release an in-flight order"
        );
        assert!(!stored.can_route_to_rfq());
        assert!(book.levels().is_empty());
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
        let (batch, decisions) = book
            .build_pair_batch(
                base,
                quote,
                BatchPrice::from_ratio(2, 1).unwrap(),
                &ClearingConfig::default(),
            )
            .unwrap();
        assert!(decisions.is_empty());
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

    /// One self-trade round at price 2: the batch, then each crossed maker's
    /// sit-outs, as the `seq` labels of `orders`, sorted.
    fn stp_round(
        orders: &[(u64, &BookOrder)],
        config: &ClearingConfig,
    ) -> (Vec<u64>, Vec<(MakerId, Vec<u64>)>) {
        let mut book = ClearingBook::default();
        for (_, order) in orders {
            book.insert_or_skip(order);
        }
        stp_round_on(&book, orders, config)
    }

    fn stp_round_on(
        book: &ClearingBook,
        orders: &[(u64, &BookOrder)],
        config: &ClearingConfig,
    ) -> (Vec<u64>, Vec<(MakerId, Vec<u64>)>) {
        let labels: HashMap<NoteId, u64> = orders
            .iter()
            .map(|(label, order)| (order.id(), *label))
            .collect();
        let label = |id: &NoteId| labels[id];
        let (batch, decisions) = book
            .build_pair_batch(
                ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(),
                ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap(),
                BatchPrice::from_ratio(2, 1).unwrap(),
                config,
            )
            .unwrap();
        let mut into: Vec<_> = batch
            .orders()
            .map(|order| label(&order.order().id()))
            .collect();
        into.sort_unstable();
        let mut decisions: Vec<_> = decisions
            .into_iter()
            .map(|decision| {
                let mut sit_out: Vec<_> = decision.sit_out.iter().map(label).collect();
                sit_out.sort_unstable();
                (decision.maker_id, sit_out)
            })
            .collect();
        decisions.sort_unstable();
        (into, decisions)
    }

    #[test]
    fn self_trade_prevention_follows_the_adr_examples() {
        /// Maker 7's orders as (buy, root_seq), then what enters matching
        /// and what sits out.
        type Case = (&'static [(bool, u64)], &'static [u64], &'static [u64]);
        let cases: &[Case] = &[
            (&[(true, 1), (true, 2)], &[1, 2], &[]),
            (&[(true, 1), (false, 2)], &[2], &[1]),
            (&[(false, 1), (true, 2)], &[2], &[1]),
            (&[(true, 1), (false, 2), (true, 3)], &[3], &[1, 2]),
            (&[(true, 1), (true, 2), (false, 3)], &[3], &[1, 2]),
            (&[(true, 1), (false, 2), (false, 3)], &[2, 3], &[1]),
            (
                &[(true, 1), (false, 2), (true, 3), (false, 4)],
                &[4],
                &[1, 2, 3],
            ),
        ];
        let mut rng = RandomCoin::new(Word::default());
        for (sides, into, sit_out) in cases {
            // Book priority runs against the submission order, so the walk
            // meets the maker's oldest orders last.
            let orders: Vec<_> = sides
                .iter()
                .map(|&(buy, seq)| (seq, tagged(eligible(buy, 10 - seq, &mut rng), 7, seq)))
                .collect();
            let orders: Vec<_> = orders.iter().map(|(seq, order)| (*seq, order)).collect();
            let expected = if sit_out.is_empty() {
                Vec::new()
            } else {
                vec![(7, sit_out.to_vec())]
            };
            assert_eq!(
                stp_round(&orders, &ClearingConfig::default()),
                (into.to_vec(), expected),
                "{sides:?}"
            );
        }
    }

    #[test]
    fn self_trade_decision_equals_a_submission_order_replay() {
        // Every side sequence of up to 8 maker orders against the literal
        // replay: each submit on the other side cancels the maker's older
        // live orders, and what is left trades.
        let mut rng = RandomCoin::new(Word::default());
        let sides: Vec<[BookOrder; 2]> = (1..=8)
            .map(|seq| {
                [
                    eligible(false, seq, &mut rng),
                    eligible(true, seq, &mut rng),
                ]
            })
            .collect();
        for len in 1..=8usize {
            for mask in 0..(1u32 << len) {
                let buys: Vec<bool> = (0..len).map(|at| mask >> at & 1 == 1).collect();
                let mut live: Vec<u64> = Vec::new();
                for (at, &buy) in buys.iter().enumerate() {
                    if at > 0 && buys[at - 1] != buy {
                        live.clear();
                    }
                    live.push(at as u64 + 1);
                }
                let sit_out: Vec<u64> =
                    (1..=len as u64).filter(|seq| !live.contains(seq)).collect();
                let orders: Vec<_> = buys
                    .iter()
                    .enumerate()
                    .map(|(at, &buy)| {
                        let seq = at as u64 + 1;
                        (seq, tagged(sides[at][usize::from(buy)].clone(), 7, seq))
                    })
                    .collect();
                let orders: Vec<_> = orders.iter().map(|(seq, order)| (*seq, order)).collect();
                let expected = if sit_out.is_empty() {
                    Vec::new()
                } else {
                    vec![(7, sit_out)]
                };
                assert_eq!(
                    stp_round(&orders, &ClearingConfig::default()),
                    (live, expected),
                    "buys {buys:?}"
                );
            }
        }
    }

    #[test]
    fn self_trade_prevention_leaves_other_owners_and_one_sided_makers_alone() {
        let mut rng = RandomCoin::new(Word::default());
        let m7_sell = tagged(eligible(false, 1, &mut rng), 7, 1);
        let m8_buy = tagged(eligible(true, 2, &mut rng), 8, 2);
        // The same creator account as every fixture, but no maker ID.
        let public_buy = eligible(true, 3, &mut rng);
        let public_sell = eligible(false, 4, &mut rng);
        // Maker 9's buy is eligible, its newer sell is not: no conflict.
        let m9_buy = tagged(eligible(true, 5, &mut rng), 9, 1);
        let m9_ineligible_sell = tagged(fixture(false, 10, 21, 6, &mut rng), 9, 2);
        let orders = [
            (1, &m7_sell),
            (2, &m8_buy),
            (3, &public_buy),
            (4, &public_sell),
            (5, &m9_buy),
            (6, &m9_ineligible_sell),
        ];
        assert_eq!(
            stp_round(&orders, &ClearingConfig::default()),
            (vec![1, 2, 3, 4, 5], Vec::new())
        );
    }

    #[test]
    fn self_trade_prevention_can_be_switched_off() {
        let mut rng = RandomCoin::new(Word::default());
        let buy = tagged(eligible(true, 1, &mut rng), 7, 1);
        let sell = tagged(eligible(false, 2, &mut rng), 7, 2);
        let off = ClearingConfig {
            self_trade_prevention: false,
            ..ClearingConfig::default()
        };
        assert_eq!(
            stp_round(&[(1, &buy), (2, &sell)], &off),
            (vec![1, 2], Vec::new())
        );
    }

    #[test]
    fn an_order_in_flight_does_not_cross_its_maker() {
        let mut rng = RandomCoin::new(Word::default());
        let buy = tagged(eligible(true, 1, &mut rng), 7, 1);
        let sell = tagged(eligible(false, 2, &mut rng), 7, 2);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&buy);
        book.insert_or_skip(&sell);
        book.deactivate(buy.id());
        assert_eq!(
            stp_round_on(&book, &[(1, &buy), (2, &sell)], &ClearingConfig::default()),
            (vec![2], Vec::new())
        );
    }

    #[test]
    fn sit_outs_take_no_slot_and_a_newest_order_outside_the_window_still_decides() {
        // Buys best first: user 1, maker B1, user 2, maker B3; maker S2 sells.
        // A buy window of two takes both users; B3, the maker's newest order,
        // is beyond it, yet B1 and S2 still sit out.
        let mut rng = RandomCoin::new(Word::default());
        let user_1 = fixture(true, 30, 10, 1, &mut rng);
        let b1 = tagged(fixture(true, 26, 10, 2, &mut rng), 7, 1);
        let user_2 = fixture(true, 24, 10, 3, &mut rng);
        let b3 = tagged(fixture(true, 22, 10, 4, &mut rng), 7, 3);
        let s2 = tagged(eligible(false, 5, &mut rng), 7, 2);
        let orders = [(10, &user_1), (1, &b1), (20, &user_2), (3, &b3), (2, &s2)];
        let window_of_two = ClearingConfig {
            max_orders_per_side: 2,
            ..ClearingConfig::default()
        };
        assert_eq!(
            stp_round(&orders, &window_of_two),
            (vec![10, 20], vec![(7, vec![1, 2])])
        );
        // Without the rule the window would take the stale B1 and S2.
        let off = ClearingConfig {
            self_trade_prevention: false,
            ..window_of_two
        };
        assert_eq!(stp_round(&orders, &off), (vec![1, 2, 10], Vec::new()));
    }

    #[test]
    fn a_kept_fill_carries_the_sit_outs_until_the_executor_answers() {
        use crate::matcher::matcher::internal_clear;
        use crate::price::PriceSnapshot;
        let mut rng = RandomCoin::new(Word::default());
        let b1 = tagged(eligible(true, 1, &mut rng), 7, 1);
        let s2 = tagged(eligible(false, 2, &mut rng), 7, 2);
        let user = eligible(true, 3, &mut rng);
        let (base, quote) = Order::from_book_order(&s2).unwrap().index_key().0;
        let runtime = ClearingRuntime {
            bootstrap: tokio::sync::oneshot::channel().1,
            prices: watch::channel(Arc::new(PriceSnapshot::for_tests(
                &[(base, quote, "2", std::time::Instant::now())],
                &[],
                Duration::from_secs(3_600),
            )))
            .1,
            config: ClearingConfig::default(),
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let mut book = ClearingBook::default();
        book.insert_or_skip(&b1);
        book.insert_or_skip(&s2);

        // The kept S2 finds no buyer: nothing is sent and B1 stays live.
        internal_clear(&mut book, &runtime, &exec_tx).unwrap();
        assert!(exec_rx.try_recv().is_err());
        assert!(book.orders[&b1.id()].is_active());

        book.insert_or_skip(&user);
        let round = |book: &mut ClearingBook, exec_rx: &mut mpsc::Receiver<ExecutionBatch>| {
            internal_clear(book, &runtime, &exec_tx).unwrap();
            exec_rx.try_recv().expect("S2 fills the user's buy")
        };
        let batch = round(&mut book, &mut exec_rx);
        let filled: std::collections::HashSet<_> =
            batch.filled_notes.iter().map(|f| f.note_id).collect();
        assert_eq!(filled, [s2.id(), user.id()].into(), "B1 is not an input");
        let carrier = batch
            .filled_notes
            .iter()
            .find(|filled| filled.note_id == s2.id())
            .unwrap();
        assert_eq!(
            carrier
                .cancel_on_reserve
                .iter()
                .map(BookOrder::id)
                .collect::<Vec<_>>(),
            vec![b1.id()]
        );
        assert_eq!(batch.self_trade_cancels(), vec![b1.id()]);
        // Held like the inputs, so no later round selects it.
        assert!(!book.orders[&b1.id()].is_active());
        assert!(book.levels().is_empty());

        // A give-back returns it with the inputs; `write_book` reloads each
        // one from `live_orders`, with its maker tag.
        let stored = [&b1, &s2, &user];
        let returned: Vec<_> = batch
            .book_orders()
            .iter()
            .map(|order| {
                (*stored
                    .iter()
                    .find(|stored| stored.id() == order.id())
                    .unwrap())
                .clone()
            })
            .collect();
        assert_eq!(returned.len(), 3);
        book.apply(BookUpdate {
            removed: Vec::new(),
            active: returned,
            maker_updates: Vec::new(),
        });
        assert!(book.orders[&b1.id()].is_active());

        // A confirmation or discard after the reservation removes it.
        let batch = round(&mut book, &mut exec_rx);
        book.apply(BookUpdate {
            removed: batch
                .filled_notes
                .iter()
                .map(|filled| filled.note_id)
                .chain(batch.self_trade_cancels())
                .collect(),
            active: Vec::new(),
            maker_updates: Vec::new(),
        });
        assert!(book.orders.is_empty());
        assert!(book.levels().is_empty());
    }

    /// ADR 0005 through the database: maker 7 submits B1, S1, S2 in that
    /// order, and a user buys 20. B1 and S2 are eligible at price 2; S1 is
    /// run both ways. The book is loaded from `live_orders`, one round
    /// clears, and that round's reservation commits.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn b1_s1_s2_through_a_clearing_round_and_its_reservation() -> anyhow::Result<()> {
        use crate::db::maker_db::{create_maker_tx, execute_command_tx, read_events_tx};
        use crate::db::postgres_db::{
            insert_orders_batch_tx, load_live_orders_tx, reserve_settlement_tx, test_consumer,
        };
        use crate::db::postgres_models::{NewOrderRow, SettlementAttemptRow, SettlementInputRow};
        use crate::db::postgres_schema::{live_orders, maker_stops, orders};
        use crate::db::postgres_test::TestSchema;
        use crate::db::DbError;
        use crate::gateway::proto;
        use crate::maker::{CommandHeader, MakerCommand};
        use crate::matcher::matcher::internal_clear;
        use crate::price::PriceSnapshot;
        use diesel::prelude::*;
        use miden_protocol::crypto::utils::{Deserializable, Serializable};
        use prost::Message as _;

        for s1_eligible in [false, true] {
            let mut schema = TestSchema::migrated()?;
            let conn = &mut schema.conn;
            let mut rng = RandomCoin::new(Word::from([u32::from(s1_eligible), 0, 0, 0]));
            let b1 = fixture(true, 22, 10, 1, &mut rng);
            let s1 = fixture(false, 10, if s1_eligible { 18 } else { 21 }, 2, &mut rng);
            let s2 = fixture(false, 10, 18, 3, &mut rng);
            let user = fixture(true, 44, 20, 4, &mut rng);
            let named = [("B1", &b1), ("S1", &s1), ("S2", &s2), ("user", &user)];
            let name = |id: &NoteId| -> &str {
                named
                    .iter()
                    .find(|(_, order)| order.id() == *id)
                    .map_or("?", |(name, _)| name)
            };

            // Maker 7 submits B1, S1, S2 with sequences 1, 2, 3; then ingest.
            let maker = create_maker_tx(conn, "m7")?.unwrap();
            for (seq, order) in [(1, &b1), (2, &s1), (3, &s2)] {
                let header = CommandHeader::new(maker, format!("s{seq}"), seq)?;
                let submit = MakerCommand::submit(order.note.as_ref().clone())?;
                conn.transaction::<_, DbError, _>(|conn| {
                    execute_command_tx(conn, &header, &submit)
                })?;
            }
            let rows: Vec<_> = named
                .iter()
                .map(|(_, order)| NewOrderRow::ingested(&order.note, 1))
                .collect::<Result<_, _>>()?;
            conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &rows, 1))?;

            // Startup hydration: the book holds what `live_orders` returns.
            let mut book = ClearingBook::default();
            for order in load_live_orders_tx(conn)? {
                book.insert_or_skip(&order);
            }
            let (base, quote) = Order::from_book_order(&s2)?.index_key().0;
            let price = BatchPrice::from_ratio(2, 1)?;
            let (into, decisions) =
                book.build_pair_batch(base, quote, price, &ClearingConfig::default())?;
            let mut into: Vec<_> = into.orders().map(|o| name(&o.order().id())).collect();
            into.sort_unstable();
            let sat_out: Vec<_> = decisions
                .iter()
                .flat_map(|decision| decision.sit_out.iter().map(name))
                .collect();

            let runtime = ClearingRuntime {
                bootstrap: tokio::sync::oneshot::channel().1,
                prices: watch::channel(Arc::new(PriceSnapshot::for_tests(
                    &[(base, quote, "2", std::time::Instant::now())],
                    &[],
                    Duration::from_secs(3_600),
                )))
                .1,
                config: ClearingConfig::default(),
                maker_settlement_buffer_ms: 0,
                routing: None,
            };
            let (exec_tx, mut exec_rx) = mpsc::channel(1);
            internal_clear(&mut book, &runtime, &exec_tx)?;
            let batch = exec_rx.try_recv()?;
            let mut filled: Vec<_> = batch
                .filled_notes
                .iter()
                .map(|f| format!("{} {}", name(&f.note_id), f.requested_filled))
                .collect();
            filled.sort_unstable();
            let cancels: Vec<_> = batch.self_trade_cancels();

            // The executor's reservation: the inputs, then the cancels, in
            // one commit.
            let attempt = SettlementAttemptRow {
                tx_id: vec![1; 32],
                tx_result: vec![1],
                status: "prepared".into(),
            };
            let inputs = batch
                .filled_notes
                .iter()
                .map(|filled| {
                    let pswap = miden_standards::note::PswapNote::try_from(filled.note.as_ref())?;
                    let requested = pswap.storage().requested_faucet_id();
                    let (_, remainder) = pswap.execute(
                        test_consumer(),
                        None,
                        Some(FungibleAsset::new(requested, filled.requested_filled)?),
                    )?;
                    let remainder = remainder.map(Note::from);
                    Ok(SettlementInputRow {
                        tx_id: attempt.tx_id.clone(),
                        parent_note_id: filled.note_id.to_bytes().to_vec(),
                        child_note_id: remainder.as_ref().map(|n| n.id().to_bytes().to_vec()),
                        child_note_data: remainder.as_ref().map(Serializable::to_bytes),
                        fill_amount: Some(i64::try_from(filled.requested_filled)?),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let cancel_ids: Vec<_> = cancels.iter().map(|id| id.to_bytes().to_vec()).collect();
            conn.transaction::<_, DbError, _>(|conn| {
                reserve_settlement_tx(conn, &attempt, &inputs, None, &cancel_ids)
            })?;

            let live: Vec<Vec<u8>> = live_orders::table.select(live_orders::note_id).load(conn)?;
            let stops: i64 = maker_stops::table.count().get_result(conn)?;
            let mut after = Vec::new();
            for (label, order) in &named {
                let id = order.id().to_bytes().to_vec();
                let status: String = orders::table.find(&id).select(orders::status).first(conn)?;
                after.push(format!(
                    "{label} {status}{}",
                    if live.contains(&id) { ", live" } else { "" }
                ));
            }
            let events: Vec<String> = read_events_tx(conn, maker, 0, 100)?
                .into_iter()
                .map(|event| {
                    match proto::EventBody::decode(event.payload.as_slice())
                        .unwrap()
                        .kind
                        .unwrap()
                    {
                        proto::event_body::Kind::OrderStatus(status) => format!(
                            "OrderStatus {:?} {} ({})",
                            proto::OrderState::try_from(status.state).unwrap(),
                            name(&NoteId::read_from_bytes(&status.note_id).unwrap()),
                            status.reason
                        ),
                        proto::event_body::Kind::SettlementPending(pending) => format!(
                            "SettlementPending {}",
                            pending.fill.as_ref().map_or("?".into(), |fill| name(
                                &NoteId::read_from_bytes(&fill.note_id).unwrap()
                            )
                            .to_string())
                        ),
                        other => format!("{other:?}"),
                    }
                })
                .collect();

            println!("\n=== B1 -> S1 -> S2, S1 eligible: {s1_eligible} ===");
            println!("into matching : {into:?}");
            println!("sit out       : {sat_out:?}");
            println!("filled        : {filled:?}");
            println!(
                "cancel if reserved: {:?}",
                cancels.iter().map(name).collect::<Vec<_>>()
            );
            println!("after reservation : {after:?}");
            println!("maker stop rows   : {stops}");
            println!("maker 7 events    : {events:?}");

            let expected_into = if s1_eligible {
                vec!["S1", "S2", "user"]
            } else {
                vec!["S2", "user"]
            };
            assert_eq!(into, expected_into);
            assert_eq!(sat_out, ["B1"]);
            assert_eq!(cancels, [b1.id()]);
            assert!(
                !live.contains(&b1.id().to_bytes().to_vec()),
                "B1 is stopped"
            );
            assert_eq!(live.contains(&s1.id().to_bytes().to_vec()), !s1_eligible);
            assert_eq!(stops, 1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod stp_scale;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use miden_protocol::account::AccountId;
use miden_protocol::asset::AssetId;
use miden_protocol::note::NoteId;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::matcher::{internal_clear, ClearingRuntime};
use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingError, MatchOrder, Order, OrderSide, PairBatch,
};
use crate::matching::types::{BestLevel, RateKey, SwapBookSnapshot};
use crate::types::{now_millis, now_unix, BookUpdate, ExecutionBatch, IngestOrder, TokenId};

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
    inactive: HashSet<NoteId>,
    pairs: HashMap<(TokenId, TokenId), BTreeMap<(RateKey, u64), NoteId>>,
    pub arrivals: HashMap<NoteId, u64>,
}

impl ClearingBook {
    /// Synchronous handoff: no matching can run between parent removal and
    /// remainder activation. Input comes from committed, ordered DB updates.
    fn apply(&mut self, update: BookUpdate, solver_id: AccountId) -> Result<()> {
        for id in update.removed {
            self.remove(id);
        }
        for order in update.active {
            self.insert(&order, solver_id)?;
        }
        Ok(())
    }

    fn index_key(order: &Order) -> ((TokenId, TokenId), (RateKey, u64)) {
        let offered = order.offered_asset();
        let requested = order.requested_asset();
        (
            (offered.faucet_id(), requested.faucet_id()),
            (
                RateKey::new(requested.amount().as_u64(), offered.amount().as_u64()),
                order.priority_sequence(),
            ),
        )
    }

    fn remove_from_index(&mut self, pair: (TokenId, TokenId), key: (RateKey, u64)) {
        if let Some(index) = self.pairs.get_mut(&pair) {
            index.remove(&key);
            if index.is_empty() {
                self.pairs.remove(&pair);
            }
        }
    }

    pub fn insert(&mut self, order: &IngestOrder, solver_id: AccountId) -> Result<()> {
        if let Some(existing) = self.orders.get(&order.note_id) {
            if self.inactive.contains(&order.note_id) {
                let (pair, key) = Self::index_key(existing);
                let index = self.pairs.entry(pair).or_default();
                ensure!(!index.contains_key(&key), "duplicate price/FIFO priority");
                index.insert(key, order.note_id);
                self.inactive.remove(&order.note_id);
            }
            return Ok(());
        }
        let parsed = Order::from_ingest_order(order)?;
        ensure!(
            parsed.pswap_note().storage().creator_account_id() != solver_id,
            "solver-created order"
        );
        let (pair, key) = Self::index_key(&parsed);
        let index = self.pairs.entry(pair).or_default();
        ensure!(!index.contains_key(&key), "duplicate price/FIFO priority");
        index.insert(key, order.note_id);
        self.orders.insert(order.note_id, parsed);
        self.arrivals.insert(order.note_id, now_unix());
        Ok(())
    }

    pub fn deactivate(&mut self, id: NoteId) {
        if self.inactive.contains(&id) {
            return;
        }
        if let Some(order) = self.orders.get(&id) {
            let (pair, key) = Self::index_key(order);
            self.remove_from_index(pair, key);
            self.inactive.insert(id);
        }
    }

    pub fn remove(&mut self, id: NoteId) {
        if let Some(order) = self.orders.remove(&id) {
            if !self.inactive.remove(&id) {
                let (pair, key) = Self::index_key(&order);
                self.remove_from_index(pair, key);
            }
            self.arrivals.remove(&id);
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
        config.validate()?;
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

    fn snapshot(&self) -> SwapBookSnapshot {
        self.pairs
            .iter()
            .filter_map(|(&pair, index)| {
                let (&(rate, _), _) = index.first_key_value()?;
                let volume = index
                    .iter()
                    .take_while(|((other, _), _)| *other == rate)
                    .filter_map(|(_, id)| self.orders.get(id))
                    .fold(0u64, |sum, order| {
                        sum.saturating_add(order.offered_asset().amount().as_u64())
                    });
                Some((pair, BestLevel { rate, volume }))
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_clearer(
    mut book_rx: mpsc::Receiver<BookUpdate>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    mut runtime: ClearingRuntime,
    cancel: CancellationToken,
) {
    let bootstrap = tokio::select! {
        _ = cancel.cancelled() => return,
        result = &mut runtime.bootstrap => match result {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                tracing::error!(%error, "clearing startup reconciliation failed");
                cancel.cancel();
                return;
            }
        }
    };
    let mut book = ClearingBook::default();
    for order in bootstrap.orders {
        if let Err(error) = book.insert(&order, runtime.solver_id) {
            tracing::warn!(note = %order.note_id, %error, "persisted order rejected");
        }
    }
    let mut interval = tokio::time::interval(match_interval);
    loop {
        // Apply outcome messages as soon as they arrive. Only batch matching
        // waits for the timer; parents must leave the book before their
        // inherited-priority remainder is inserted.
        let update = tokio::select! {
            _ = cancel.cancelled() => return,
            result = book_rx.recv() => match result {
                Some(update) => Some(update),
                None => return,
            },
            _ = interval.tick() => None,
        };
        let match_now = update.is_none();
        if let Some(update) = update {
            if let Err(error) = book.apply(update, runtime.solver_id) {
                tracing::error!(%error, "book update failed; requiring recovery");
                cancel.cancel();
                return;
            }
        }
        // Drain only the already-queued updates: continuous ingestion must not
        // keep this loop running forever and starve matching or shutdown.
        for _ in 0..book_rx.len() {
            let Ok(update) = book_rx.try_recv() else {
                break;
            };
            if let Err(error) = book.apply(update, runtime.solver_id) {
                tracing::error!(%error, "book update failed; requiring recovery");
                cancel.cancel();
                return;
            }
        }
        if match_now {
            let stopped = tokio::select! {
                _ = cancel.cancelled() => return,
                stopped = internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now_millis()) => stopped,
            };
            if stopped {
                return;
            }
            snapshot_tx.send_replace(Arc::new(book.snapshot()));
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
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
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
            raw_note_data: note.to_bytes(),
        }
    }

    fn solver() -> AccountId {
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap()
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
                book.insert(&fixture(buy, o, r, seq, &mut rng), solver())
                    .unwrap();
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
            book.insert(order, solver()).unwrap();
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
            book.insert(order, solver()).unwrap();
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
        assert!(book.inactive.contains(&earlier.note_id));
        assert_eq!(book.orders.len(), 3);
        assert!(!admit(&book, false, 100)
            .iter()
            .any(|order| order.order().id() == earlier.note_id));
        book.insert(&earlier, solver()).unwrap();
        book.insert(&earlier, solver()).unwrap();
        assert!(!book.inactive.contains(&earlier.note_id));
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
        book.insert(&parent, solver()).unwrap();
        book.deactivate(parent.note_id);
        book.apply(
            BookUpdate {
                removed: vec![parent.note_id],
                active: vec![child.clone()],
            },
            solver(),
        )
        .unwrap();
        assert_eq!(admit(&book, false, 100)[0].order().id(), child.note_id);
    }

    #[test]
    fn removing_last_order_clears_index_and_solver_creator_is_rejected() {
        let mut rng = RandomCoin::new(Word::default());
        let order = fixture(false, 10, 18, 1, &mut rng);
        let mut book = ClearingBook::default();
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        assert!(book.insert(&order, creator).is_err());
        book.insert(&order, solver()).unwrap();
        assert_eq!(book.snapshot().len(), 1);
        book.remove(order.note_id);
        book.remove(order.note_id);
        assert!(book.orders.is_empty() && book.pairs.is_empty() && book.arrivals.is_empty());
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
            solver_id: solver(),
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
        task.await.unwrap();
    }
}

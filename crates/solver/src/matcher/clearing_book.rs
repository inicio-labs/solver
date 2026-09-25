use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use miden_protocol::account::AccountId;
use miden_protocol::asset::AssetId;
use miden_protocol::note::NoteId;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::matcher::{internal_clear, ClearingRuntime};
use crate::clearing::{self, AdmittedPswap, ClearingError, ExactPrice};
use crate::matching::types::{BestLevel, RateKey, SwapBookSnapshot};
use crate::types::{now_millis, now_unix, ExecutionBatch, IngestOrder, TokenId};

/// Sent once, after ingestion reconciles persisted notes against the chain.
pub struct ClearingBootstrap {
    pub orders: Vec<IngestOrder>,
    pub decimals: HashMap<TokenId, u8>,
}

/// Parse once on admission; maintain the exact price/FIFO index incrementally.
/// Both directions use requested/offered: lower is always better.
#[derive(Default)]
pub(super) struct ClearingBook {
    orders: HashMap<NoteId, AdmittedPswap>,
    pairs: HashMap<(TokenId, TokenId), BTreeMap<(RateKey, u64), NoteId>>,
    pub arrivals: HashMap<NoteId, u64>,
}

impl ClearingBook {
    pub fn insert(&mut self, order: &IngestOrder, solver_id: AccountId) -> Result<()> {
        if self.orders.contains_key(&order.note_id) {
            return Ok(());
        }
        let parsed = AdmittedPswap::from_ingest_order(order)?;
        ensure!(
            parsed.note.storage().creator_account_id() != solver_id,
            "solver-created order"
        );
        ensure!(
            order.offered_amount > 0 && order.requested_amount > 0,
            "zero order amount"
        );
        let pair = (order.offered_token, order.requested_token);
        let key = (
            RateKey::new(order.requested_amount, order.offered_amount),
            order.priority_seq,
        );
        let index = self.pairs.entry(pair).or_default();
        ensure!(!index.contains_key(&key), "duplicate price/FIFO priority");
        index.insert(key, order.note_id);
        self.orders.insert(order.note_id, parsed);
        self.arrivals.insert(order.note_id, now_unix());
        Ok(())
    }

    pub fn remove(&mut self, id: NoteId) {
        if let Some(order) = self.orders.remove(&id) {
            let offered = order.note.offered_asset();
            let requested = order.note.storage().min_requested_asset();
            let pair = (offered.faucet_id(), requested.faucet_id());
            if let Some(index) = self.pairs.get_mut(&pair) {
                index.remove(&(
                    RateKey::new(requested.amount().as_u64(), offered.amount().as_u64()),
                    order.priority_seq,
                ));
                if index.is_empty() {
                    self.pairs.remove(&pair);
                }
            }
            self.arrivals.remove(&id);
        }
    }

    pub fn admit(
        &self,
        pair: (TokenId, TokenId),
        base: AssetId,
        price: ExactPrice,
        fee_ppm: u32,
        limit: usize,
    ) -> Result<Vec<AdmittedPswap>, ClearingError> {
        let mut selected = Vec::new();
        if let Some(index) = self.pairs.get(&pair) {
            for id in index.values().take(limit) {
                let order = self
                    .orders
                    .get(id)
                    .ok_or(ClearingError::InternalInvariant("missing indexed order"))?;
                if !clearing::order_is_eligible(order, base, price, fee_ppm)? {
                    // Eligibility is monotone in this exact price ordering.
                    break;
                }
                selected.push(order.clone());
            }
        }
        Ok(selected)
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
                        sum.saturating_add(order.note.offered_asset().amount().as_u64())
                    });
                Some((pair, BestLevel { rate, volume }))
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_clearer(
    mut order_rx: mpsc::Receiver<IngestOrder>,
    mut consumed_rx: mpsc::Receiver<NoteId>,
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
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {
                // Apply queued additions before removals so a consume observed
                // in the same tick wins over the corresponding ingestion event.
                while let Ok(order) = order_rx.try_recv() {
                    if let Err(error) = book.insert(&order, runtime.solver_id) {
                        tracing::warn!(note = %order.note_id, %error, "live order rejected");
                    }
                }
                while let Ok(id) = consumed_rx.try_recv() {
                    book.remove(id);
                }
                let stopped = tokio::select! {
                    _ = cancel.cancelled() => return,
                    stopped = internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now_millis()) => stopped,
                };
                if stopped { return; }
                snapshot_tx.send_replace(Arc::new(book.snapshot()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clearing::Wide;
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

    fn admit(book: &ClearingBook, buy: bool, limit: usize) -> Vec<AdmittedPswap> {
        let a = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let b = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        book.admit(
            if buy { (b, a) } else { (a, b) },
            AssetId::new_fungible(a),
            ExactPrice {
                quote_units: Wide::from(2),
                base_units: Wide::from(1),
            },
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
                admitted.iter().map(|o| o.priority_seq).collect::<Vec<_>>(),
                (1..=100).collect::<Vec<_>>()
            );
            assert_eq!(book.orders.len(), 150);
            assert_eq!(admit(&book, buy, 200).len(), 130);
        }
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
                .map(|o| o.note_id)
                .collect::<Vec<_>>(),
            expected
        );
        book.remove(earlier.note_id);
        book.insert(&earlier, solver()).unwrap();
        book.insert(&earlier, solver()).unwrap();
        assert_eq!(book.orders.len(), 3);
        assert_eq!(
            admit(&book, false, 100)
                .iter()
                .map(|o| o.note_id)
                .collect::<Vec<_>>(),
            expected
        );
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
        let (order_tx, order_rx) = mpsc::channel(4);
        let (_consumed_tx, consumed_rx) = mpsc::channel(4);
        let (exec_tx, mut exec_rx) = mpsc::channel(4);
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_clearer(
            order_rx,
            consumed_rx,
            exec_tx,
            Duration::from_millis(10),
            snapshot_tx,
            runtime,
            cancel.clone(),
        ));
        order_tx.send(buyer).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(exec_rx.try_recv().is_err());
        assert!(bootstrap_tx
            .send(ClearingBootstrap {
                orders: vec![seller],
                decimals: [(base, 0), (quote, 0)].into_iter().collect(),
            })
            .is_ok());
        let first = tokio::time::timeout(Duration::from_secs(1), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.filled_notes.len(), 2);
        // A new pair of notes arrives solely over the ingestion channel.
        order_tx
            .send(fixture(false, 11, 18, 3, &mut rng))
            .await
            .unwrap();
        order_tx
            .send(fixture(true, 22, 10, 4, &mut rng))
            .await
            .unwrap();
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

use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::clearing_book::{ClearingBook, ClearingBootstrap};
use crate::clearing::{
    self, ClearingConfig, ClearingError, ClearingOutcome, PairMatcher, ReferencePrice, SkipReason,
};
use crate::matching::types::SwapBookSnapshot;
use crate::price::PreciseSnapshot;
use crate::types::*;

static SKIPPED_EXECUTOR_FULL_TICKS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn skipped_executor_full_ticks() -> u64 {
    SKIPPED_EXECUTOR_FULL_TICKS.load(Ordering::Relaxed)
}

/// Worker inputs for pair clearing and optional RFQ routing. Missing or stale
/// oracle prices pause clearing, but do not disable fixed-limit RFQ selection.
pub struct ClearingRuntime {
    pub bootstrap: oneshot::Receiver<ClearingBootstrap>,
    pub prices: watch::Receiver<PreciseSnapshot>,
    pub pairs: Vec<(TokenId, TokenId)>,
    pub config: ClearingConfig,
    pub max_price_age_ms: u64,
    pub max_source_age_ms: u64,
    pub max_source_skew_ms: u64,
    pub routing: Option<crate::router::Routing>,
}

impl ClearingRuntime {
    /// An order belongs to one unordered pair. Reject duplicate markets once,
    /// so clearing needs no per-tick set to prevent double selection.
    fn validate(&self) -> Result<(), ClearingError> {
        self.config.validate()?;
        let mut pairs = HashSet::with_capacity(self.pairs.len());
        for &(base, quote) in &self.pairs {
            if base == quote {
                return Err(ClearingError::IdenticalPairAssets);
            }
            let pair = if base < quote {
                (base, quote)
            } else {
                (quote, base)
            };
            if !pairs.insert(pair) {
                return Err(ClearingError::DuplicatePair);
            }
        }
        Ok(())
    }
}

/// Apply lifecycle updates immediately; match the active book on timer ticks.
/// Startup reconciliation supplies the initial book before matching begins.
pub async fn run_matcher(
    book_rx: mpsc::Receiver<BookUpdate>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    runtime: ClearingRuntime,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // One cancellation boundary covers bootstrap and matching. Executor
    // backpressure cannot block this worker from receiving book updates.
    tokio::select! {
        _ = cancel.cancelled() => Ok(()),
        result = run_worker(book_rx, exec_tx, match_interval, swap_snapshot_tx, runtime) => result,
    }
}

pub(super) async fn run_worker(
    mut book_rx: mpsc::Receiver<BookUpdate>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    mut runtime: ClearingRuntime,
) -> anyhow::Result<()> {
    // Configuration is frozen for this worker; validate before admitting orders.
    runtime.validate()?;
    let bootstrap = (&mut runtime.bootstrap).await?;
    let mut book = ClearingBook::default();
    for order in bootstrap.orders {
        book.insert(&order)?;
    }
    let mut interval = tokio::time::interval(match_interval);
    loop {
        // Update the book immediately; run matching only on the batch timer.
        tokio::select! {
            update = book_rx.recv() => {
                let update = update.context("ingestion stopped: book update channel closed")?;
                book.apply(update)?;
            }
            _ = interval.tick() => {
                book.apply_pending(&mut book_rx)?;
                let now = now_millis();
                if let Some(routing) = runtime.routing.as_mut() {
                    routing.release_expired(&mut book, now)?;
                }
                let clearing = internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now)?;
                if clearing == ClearingTickOutcome::Completed {
                    if let Some(routing) = runtime.routing.as_mut() {
                        // External dispatch assumes internal clearing already
                        // removed its matches. When executor capacity is full,
                        // skip both paths and leave all orders active.
                        routing.dispatch(&mut book, now_millis())?;
                    }
                }
                // Latest order-book levels for the price API's swap-ETA estimates.
                snapshot_tx.send_replace(Arc::new(book.best_levels_snapshot()));
            }
        }
    }
}

fn fresh_reference_prices(
    prices: &PreciseSnapshot,
    base: TokenId,
    quote: TokenId,
    now_ms: u64,
    max_observation_age_ms: u64,
    max_source_age_ms: u64,
    max_source_skew_ms: u64,
) -> Option<(ReferencePrice, ReferencePrice)> {
    let base_price = prices.get(&base)?;
    let quote_price = prices.get(&quote)?;
    let observed = base_price.observed_at_unix_ms;
    if observed == 0
        || observed != quote_price.observed_at_unix_ms
        || now_ms.saturating_sub(observed) > max_observation_age_ms
    {
        return None;
    }
    let base_source = base_price.source_updated_at_unix_ms?;
    let quote_source = quote_price.source_updated_at_unix_ms?;
    if base_source == 0
        || quote_source == 0
        || base_source > now_ms
        || quote_source > now_ms
        || now_ms - base_source > max_source_age_ms
        || now_ms - quote_source > max_source_age_ms
        || base_source.abs_diff(quote_source) > max_source_skew_ms
    {
        return None;
    }
    Some((base_price.exact_reference?, quote_price.exact_reference?))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClearingTickOutcome {
    Completed,
    SkippedExecutorFull,
}

/// Solve all pairs from the live book using one frozen price snapshot.
pub(super) fn internal_clear(
    book: &mut ClearingBook,
    decimals: &HashMap<TokenId, u8>,
    runtime: &ClearingRuntime,
    exec_tx: &mpsc::Sender<ExecutionBatch>,
    now_ms: u64,
) -> anyhow::Result<ClearingTickOutcome> {
    // Do not wait on a full executor queue: the matcher must remain able to
    // receive lifecycle updates. A skipped tick changes no order state and
    // clearing retries against the current book at the next tick.
    let permit = match exec_tx.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            SKIPPED_EXECUTOR_FULL_TICKS.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("executor queue full; skipping clearing tick");
            return Ok(ClearingTickOutcome::SkippedExecutorFull);
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            anyhow::bail!("executor stopped: execution batch receiver closed");
        }
    };
    let prices = runtime.prices.borrow().clone();

    // Each independently solvent pair stays indivisible when the executor
    // splits the combined tick into protocol-sized transactions.
    let mut combined = ExecutionBatch {
        filled_notes: Vec::new(),
        group_ends: Vec::new(),
    };
    let mut included_pairs = 0usize;
    for &(base, quote) in &runtime.pairs {
        let Some((base_price, quote_price)) = fresh_reference_prices(
            &prices,
            base,
            quote,
            now_ms,
            runtime.max_price_age_ms,
            runtime.max_source_age_ms,
            runtime.max_source_skew_ms,
        ) else {
            continue;
        };
        let (Some(&base_decimals), Some(&quote_decimals)) =
            (decimals.get(&base), decimals.get(&quote))
        else {
            continue;
        };
        let batch = clearing::BatchPrice::from_reference_prices(
            base_price,
            quote_price,
            base_decimals,
            quote_decimals,
        )
        .and_then(|price| book.build_pair_batch(base, quote, price, &runtime.config));
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                tracing::error!(%base, %quote, %error, "clearing admission failed");
                continue;
            }
        };
        let plan = match PairMatcher::new(&batch, &runtime.config).clear() {
            Ok(ClearingOutcome::Accepted(plan)) => plan,
            Ok(ClearingOutcome::Skipped(reason)) => {
                match reason {
                    SkipReason::ResourceLimit => {
                        tracing::warn!(%base, %quote, "pair clearing resource limit exceeded");
                    }
                    SkipReason::Insolvent {
                        base_shortfall,
                        quote_shortfall,
                        ..
                    } => {
                        tracing::warn!(%base, %quote, %base_shortfall, %quote_shortfall, "pair candidate was insolvent; nothing submitted");
                    }
                    other => tracing::debug!(%base, %quote, ?other, "pair did not clear"),
                }
                continue;
            }
            Err(error) => {
                tracing::error!(%base, %quote, %error, "pair clearing failed");
                continue;
            }
        };
        let execution = match plan.to_execution_batch(&batch) {
            Ok(execution) => execution,
            Err(error) => {
                tracing::error!(%base, %quote, %error, "clearing execution batch construction failed");
                continue;
            }
        };
        tracing::info!(
            %base,
            %quote,
            orders = execution.filled_notes.len(),
            fee_base = %plan.accruals.realized_protocol_fee.base,
            fee_quote = %plan.accruals.realized_protocol_fee.quote,
            surplus_base = %plan.accruals.rounding_surplus.base,
            surplus_quote = %plan.accruals.rounding_surplus.quote,
            "clearing pair included in combined batch"
        );
        combined.filled_notes.extend(execution.filled_notes);
        combined.group_ends.push(combined.filled_notes.len());
        included_pairs += 1;
    }
    if combined.filled_notes.is_empty() {
        return Ok(ClearingTickOutcome::Completed);
    }
    // Deactivation and send are synchronous after acquiring capacity, so a
    // cancelled task cannot leave half of the handoff applied.
    // Keep the parents in memory but off the matchable index. A definite
    // failure reactivates them; confirmation removes them permanently.
    for filled in &combined.filled_notes {
        book.deactivate(filled.note_id);
    }
    tracing::info!(
        pairs = included_pairs,
        orders = combined.filled_notes.len(),
        "combined clearing batch sent to executor"
    );
    permit.send(combined);
    Ok(ClearingTickOutcome::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::price::PriceData;
    use miden_protocol::account::AccountId;
    use miden_protocol::crypto::utils::Serializable;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    };

    fn imiden() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
    }
    fn iusdt() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
    }
    fn ieth() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2.try_into().unwrap()
    }

    #[test]
    fn clearing_requires_one_fresh_exact_price_snapshot() {
        let exact = ReferencePrice::from_decimal("2.01").unwrap();
        let mut prices = PreciseSnapshot::new();
        prices.insert(
            imiden(),
            PriceData {
                usd: 2.01,
                exact_reference: Some(exact),
                source_updated_at_unix_ms: Some(1_000),
                observed_at_unix_ms: 1_000,
            },
        );
        prices.insert(
            iusdt(),
            PriceData {
                usd: 1.0,
                exact_reference: Some(ReferencePrice::from_decimal("1").unwrap()),
                source_updated_at_unix_ms: Some(1_000),
                observed_at_unix_ms: 1_000,
            },
        );
        assert_eq!(
            fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 0),
            Some((exact, ReferencePrice::from_decimal("1").unwrap()))
        );
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_501, 500, 500, 0).is_none());
        prices.get_mut(&iusdt()).unwrap().observed_at_unix_ms = 1_001;
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 0).is_none());
        prices.get_mut(&iusdt()).unwrap().observed_at_unix_ms = 1_000;
        prices.get_mut(&iusdt()).unwrap().source_updated_at_unix_ms = Some(1_001);
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 0).is_none());
        prices.get_mut(&iusdt()).unwrap().source_updated_at_unix_ms = None;
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 0).is_none());
        prices.get_mut(&iusdt()).unwrap().source_updated_at_unix_ms = Some(1_000);
        prices.get_mut(&iusdt()).unwrap().exact_reference = None;
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 0).is_none());
        prices.get_mut(&iusdt()).unwrap().exact_reference =
            Some(ReferencePrice::from_decimal("1").unwrap());
        prices.get_mut(&imiden()).unwrap().source_updated_at_unix_ms = Some(999);
        assert!(fresh_reference_prices(&prices, imiden(), iusdt(), 1_500, 500, 500, 1).is_none());
    }

    #[tokio::test(start_paused = true)]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn exact_price_runtime_combines_two_pairs_in_one_execution_batch() {
        use crate::db::postgres_models::NewOrderRow;
        use crate::db::postgres_test::TestDb;
        use miden_protocol::asset::{AssetAmount, FungibleAsset};
        use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
        use miden_protocol::note::{Note, NoteType};
        use miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;
        use miden_protocol::Word;
        use miden_standards::note::{PswapNote, PswapNoteStorage};

        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let solver_id =
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE).unwrap();
        let mut rng = RandomCoin::new(Word::default());
        let creator =
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
                .try_into()
                .unwrap();
        let mut make_note = |offered, requested| -> Note {
            let storage = PswapNoteStorage::builder()
                .min_requested_asset(requested)
                .min_fill_step(AssetAmount::new(1).unwrap())
                .creator_account_id(creator)
                .build();
            PswapNote::builder()
                .sender(solver_id)
                .storage(storage)
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(offered)
                .build()
                .unwrap()
                .into()
        };
        let notes = [
            make_note(
                FungibleAsset::new(imiden(), 11).unwrap(),
                FungibleAsset::new(iusdt(), 18).unwrap(),
            ),
            make_note(
                FungibleAsset::new(iusdt(), 22).unwrap(),
                FungibleAsset::new(imiden(), 10).unwrap(),
            ),
            make_note(
                FungibleAsset::new(ieth(), 11).unwrap(),
                FungibleAsset::new(iusdt(), 18).unwrap(),
            ),
            make_note(
                FungibleAsset::new(iusdt(), 22).unwrap(),
                FungibleAsset::new(ieth(), 10).unwrap(),
            ),
        ];
        pool.write(move |conn| {
            for token in [imiden(), iusdt(), ieth()] {
                db::postgres_db::register_token_tx(conn, &token.to_bytes(), None)?;
                db::postgres_db::set_token_metadata_tx(conn, &token.to_bytes(), Some(0), None)?;
            }
            let (note_rows, order_rows): (Vec<_>, Vec<_>) = notes
                .iter()
                .map(|note| NewOrderRow::ingested(note, 1).unwrap())
                .unzip();
            db::postgres_db::insert_notes_batch_tx(conn, &note_rows, &order_rows, 1)?;
            Ok(())
        })
        .await
        .unwrap();
        let persisted = pool
            .read(db::postgres_db::load_active_orders_with_notes_tx)
            .await
            .unwrap();
        let mut book = ClearingBook::default();
        for order in &persisted {
            book.insert(order).unwrap();
        }
        let decimals = [(imiden(), 0), (iusdt(), 0), (ieth(), 0)]
            .into_iter()
            .collect();
        let mut prices = PreciseSnapshot::new();
        for (token, decimal) in [(imiden(), "2"), (iusdt(), "1"), (ieth(), "2")] {
            prices.insert(
                token,
                PriceData {
                    usd: decimal.parse().unwrap(),
                    exact_reference: Some(ReferencePrice::from_decimal(decimal).unwrap()),
                    source_updated_at_unix_ms: Some(1_000),
                    observed_at_unix_ms: 1_000,
                },
            );
        }
        let (_prices_tx, prices_rx) = watch::channel(prices);
        let runtime = ClearingRuntime {
            bootstrap: oneshot::channel().1,
            prices: prices_rx,
            pairs: vec![(imiden(), iusdt()), (ieth(), iusdt())],
            config: ClearingConfig::default(),
            max_price_age_ms: 1_000,
            max_source_age_ms: 1_000,
            max_source_skew_ms: 0,
            routing: None,
        };
        runtime.validate().unwrap();
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let error = internal_clear(&mut book, &decimals, &runtime, &closed_tx, 1_500).unwrap_err();
        assert!(error.to_string().contains("executor stopped"));
        assert_eq!(
            book.best_levels_snapshot().len(),
            4,
            "failed dispatch must leave orders live"
        );
        exec_tx
            .try_send(ExecutionBatch {
                filled_notes: Vec::new(),
                group_ends: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500).unwrap(),
            ClearingTickOutcome::SkippedExecutorFull
        );
        assert_eq!(
            book.best_levels_snapshot().len(),
            4,
            "a full executor queue must leave orders active for a later tick"
        );
        assert!(exec_rx.try_recv().unwrap().filled_notes.is_empty());

        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500).unwrap();
        let execution = exec_rx.try_recv().unwrap();
        assert_eq!(execution.filled_notes.len(), 4);
        assert_eq!(execution.group_ends, vec![2, 4]);
        for filled in &execution.filled_notes {
            let source = persisted
                .iter()
                .find(|order| order.id() == filled.note_id)
                .unwrap();
            assert!(Arc::ptr_eq(&filled.note, &source.note));
        }
        assert!(exec_rx.try_recv().is_err());
        assert!(book.best_levels_snapshot().is_empty());
        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500).unwrap();
        assert!(
            exec_rx.try_recv().is_err(),
            "pending orders must not be dispatched twice"
        );
        for order in &persisted {
            book.insert(order).unwrap();
        }
        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500).unwrap();
        let retried = exec_rx.try_recv().unwrap();
        for (first, next) in execution.filled_notes.iter().zip(&retried.filled_notes) {
            assert_eq!(first.note_id, next.note_id);
            assert_eq!(first.arrival_unix, next.arrival_unix);
            assert!(Arc::ptr_eq(&first.note, &next.note));
        }

        // A full executor queue must not stop the worker from receiving a
        // committed book update. Once capacity returns, the next tick clears
        // only against the updated book.
        let mut fresh_prices = runtime.prices.borrow().clone();
        let observed_at = now_millis();
        for data in fresh_prices.values_mut() {
            data.observed_at_unix_ms = observed_at;
            data.source_updated_at_unix_ms = Some(observed_at);
        }
        let (_fresh_prices_tx, fresh_prices_rx) = watch::channel(fresh_prices);
        let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
        let (book_tx, book_rx) = mpsc::channel(1);
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        exec_tx
            .try_send(ExecutionBatch {
                filled_notes: Vec::new(),
                group_ends: Vec::new(),
            })
            .unwrap();
        let (snapshot_tx, mut snapshot_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
        let worker_runtime = ClearingRuntime {
            bootstrap: bootstrap_rx,
            prices: fresh_prices_rx,
            pairs: runtime.pairs.clone(),
            config: runtime.config,
            max_price_age_ms: 30_000,
            max_source_age_ms: 30_000,
            max_source_skew_ms: 0,
            routing: None,
        };
        let removed_id = persisted[0].id();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let worker = tokio::task::spawn_local(run_worker(
                    book_rx,
                    exec_tx,
                    Duration::from_secs(1),
                    snapshot_tx,
                    worker_runtime,
                ));
                assert!(bootstrap_tx
                    .send(ClearingBootstrap {
                        orders: persisted,
                        decimals,
                    })
                    .is_ok());
                snapshot_rx.changed().await.unwrap();
                assert_eq!(snapshot_rx.borrow().len(), 4);

                book_tx
                    .send(BookUpdate {
                        removed: vec![removed_id],
                        active: Vec::new(),
                    })
                    .await
                    .unwrap();
                assert_eq!(book_tx.capacity(), 0, "book-update channel must be full");
                tokio::time::advance(Duration::from_secs(1)).await;
                snapshot_rx.changed().await.unwrap();
                assert_eq!(snapshot_rx.borrow().len(), 3);
                assert!(exec_rx.try_recv().unwrap().filled_notes.is_empty());

                tokio::time::advance(Duration::from_secs(1)).await;
                snapshot_rx.changed().await.unwrap();
                assert_eq!(exec_rx.try_recv().unwrap().filled_notes.len(), 2);
                worker.abort();
                assert!(worker.await.unwrap_err().is_cancelled());
            })
            .await;
    }
}

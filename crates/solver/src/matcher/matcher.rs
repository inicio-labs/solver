use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::clearing_book::{ClearingBook, ClearingBootstrap};
use super::error::MatcherError;
use crate::clearing::{
    self, ClearingConfig, ClearingError, ClearingOutcome, PairMatcher, ReferencePrice, SkipReason,
};
use crate::maker::MakerFact;
use crate::matching::types::SwapBookSnapshot;
use crate::price::PreciseSnapshot;
use crate::types::*;

static SKIPPED_EXECUTOR_FULL_TICKS: AtomicU64 = AtomicU64::new(0);
static MAKER_LANE_BACKLOG: AtomicU64 = AtomicU64::new(0);

pub(crate) fn skipped_executor_full_ticks() -> u64 {
    SKIPPED_EXECUTOR_FULL_TICKS.load(Ordering::Relaxed)
}

/// Facts waiting on the maker control lane at the last tick. The lane is
/// unbounded so the maker intake never waits for the matcher; this shows
/// whether the matcher keeps up.
pub(crate) fn maker_lane_backlog() -> u64 {
    MAKER_LANE_BACKLOG.load(Ordering::Relaxed)
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
    /// The maker control lane (ADR 0003): cutoffs, stops and lineage
    /// attributions the maker intake committed. `None`, or a closed lane,
    /// when this process runs no maker intake.
    pub maker_facts: Option<mpsc::UnboundedReceiver<MakerFact>>,
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
) -> Result<(), MatcherError> {
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
) -> Result<(), MatcherError> {
    // Configuration is frozen for this worker; validate before admitting orders.
    runtime.validate()?;
    let bootstrap = (&mut runtime.bootstrap).await?;
    let mut book = ClearingBook::default();
    for (maker_id, scope, cutoff) in &bootstrap.cutoffs {
        book.raise_maker_cutoff(*maker_id, scope.clone(), *cutoff);
    }
    for order in &bootstrap.orders {
        book.insert_or_skip(order);
    }
    let mut facts = runtime.maker_facts.take();
    let mut interval = tokio::time::interval(match_interval);
    loop {
        // Update the book immediately; run matching only on the batch timer.
        // Maker facts come first so a cancel never waits behind book
        // updates; the timer precedes book updates so a busy producer cannot
        // postpone matching (the tick applies queued updates itself).
        tokio::select! {
            biased;
            fact = next_fact(&mut facts), if facts.is_some() => match fact {
                Some(fact) => book.apply_maker_fact(fact, now_millis()),
                // No maker intake in this process: stop polling the lane.
                None => facts = None,
            },
            _ = interval.tick() => {
                let now = now_millis();
                if let Some(facts) = facts.as_mut() {
                    MAKER_LANE_BACKLOG.store(facts.len() as u64, Ordering::Relaxed);
                    book.apply_pending_maker_facts(facts, now);
                }
                book.apply_pending(&mut book_rx);
                if let Some(routing) = runtime.routing.as_mut() {
                    routing.release_expired(&mut book, now).map_err(MatcherError::Routing)?;
                }
                // Internal clearing has first claim on the book. While the
                // executor queue is full (busy, or verifying it can settle),
                // skip the whole tick: routing would otherwise send external
                // fillers orders that should cross internally next tick.
                if executor_accepting(&exec_tx)? {
                    internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now)?;
                    if let Some(routing) = runtime.routing.as_mut() {
                        routing.dispatch(&mut book, now_millis()).map_err(MatcherError::Routing)?;
                    }
                }
                // Latest order-book levels for the price API's swap-ETA estimates.
                snapshot_tx.send_replace(Arc::new(book.best_levels_snapshot()));
            }
            update = book_rx.recv() => {
                book.apply(update.ok_or(MatcherError::IngestStopped)?);
            }
        }
    }
}

async fn next_fact(facts: &mut Option<mpsc::UnboundedReceiver<MakerFact>>) -> Option<MakerFact> {
    match facts {
        Some(facts) => facts.recv().await,
        None => None,
    }
}

/// Whether the executor can take a batch this tick. `false` while its queue
/// is full (busy, or in verification mode); the tick is skipped and changes
/// no order state. The matcher is the queue's only sender, so a `true` here
/// cannot turn into a full queue before `internal_clear` sends.
pub(super) fn executor_accepting(
    exec_tx: &mpsc::Sender<ExecutionBatch>,
) -> Result<bool, MatcherError> {
    if exec_tx.is_closed() {
        return Err(MatcherError::ExecutorStopped);
    }
    let accepting = exec_tx.capacity() > 0;
    if !accepting {
        SKIPPED_EXECUTOR_FULL_TICKS.fetch_add(1, Ordering::Relaxed);
        tracing::debug!("executor queue full; skipping clearing tick");
    }
    Ok(accepting)
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

/// Solve all pairs from the live book using one frozen price snapshot and
/// send the combined batch to the executor. A pair that fails to clear is
/// logged and skipped; an empty batch sends nothing.
pub(super) fn internal_clear(
    book: &mut ClearingBook,
    decimals: &HashMap<TokenId, u8>,
    runtime: &ClearingRuntime,
    exec_tx: &mpsc::Sender<ExecutionBatch>,
    now_ms: u64,
) -> Result<(), MatcherError> {
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
        return Ok(());
    }
    let sent: Vec<_> = combined
        .filled_notes
        .iter()
        .map(|filled| filled.note_id)
        .collect();
    match exec_tx.try_send(combined) {
        Ok(()) => {}
        // Unreachable while the matcher is the only sender; if it ever
        // happens the orders simply stay active for the next tick.
        Err(TrySendError::Full(_)) => {
            tracing::warn!("executor queue filled unexpectedly; batch dropped");
            return Ok(());
        }
        Err(TrySendError::Closed(_)) => return Err(MatcherError::ExecutorStopped),
    }
    // Keep the parents in memory but off the matchable index. A definite
    // failure reactivates them; confirmation removes them permanently.
    for note_id in &sent {
        book.deactivate(*note_id);
    }
    tracing::info!(
        pairs = included_pairs,
        orders = sent.len(),
        "combined clearing batch sent to executor"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::price::PriceData;
    use miden_protocol::account::AccountId;
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
                db::postgres_db::register_token_tx(conn, token, None)?;
                db::postgres_db::set_token_metadata_tx(conn, token, Some(0), None)?;
            }
            let order_rows: Vec<_> = notes
                .iter()
                .map(|note| NewOrderRow::ingested(note, 1).unwrap())
                .collect();
            db::postgres_db::insert_orders_batch_tx(conn, &order_rows, 1)?;
            Ok(())
        })
        .await
        .unwrap();
        let persisted = pool
            .read(db::postgres_db::load_live_orders_tx)
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
            maker_facts: None,
        };
        runtime.validate().unwrap();
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let clear = |book: &mut ClearingBook, exec_tx: &mpsc::Sender<ExecutionBatch>| {
            if executor_accepting(exec_tx).unwrap() {
                internal_clear(book, &decimals, &runtime, exec_tx, 1_500).unwrap();
            }
        };
        assert!(matches!(
            executor_accepting(&closed_tx),
            Err(MatcherError::ExecutorStopped)
        ));
        assert_eq!(
            book.best_levels_snapshot().len(),
            4,
            "a stopped executor must leave orders live"
        );
        exec_tx
            .try_send(ExecutionBatch {
                filled_notes: Vec::new(),
                group_ends: Vec::new(),
            })
            .unwrap();
        assert!(!executor_accepting(&exec_tx).unwrap());
        assert_eq!(
            book.best_levels_snapshot().len(),
            4,
            "a full executor queue must leave orders active for a later tick"
        );
        assert!(exec_rx.try_recv().unwrap().filled_notes.is_empty());

        clear(&mut book, &exec_tx);
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
        clear(&mut book, &exec_tx);
        assert!(
            exec_rx.try_recv().is_err(),
            "pending orders must not be dispatched twice"
        );
        for order in &persisted {
            book.insert(order).unwrap();
        }
        clear(&mut book, &exec_tx);
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
            maker_facts: None,
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
                        cutoffs: Vec::new(),
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

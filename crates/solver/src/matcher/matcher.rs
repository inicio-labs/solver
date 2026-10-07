use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use strum::{EnumCount, IntoEnumIterator};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::clearing_book::ClearingBook;
use super::error::MatcherError;
use crate::clearing::{ClearingConfig, ClearingOutcome, PairMatcher, SkipReason};
use crate::matching::types::SwapBookSnapshot;
use crate::price::{PriceSnapshot, PriceUnavailable};
use crate::types::*;

static SKIPPED_EXECUTOR_FULL_TICKS: AtomicU64 = AtomicU64::new(0);
static PRICE_SKIPS: [AtomicU64; PriceUnavailable::COUNT] =
    [const { AtomicU64::new(0) }; PriceUnavailable::COUNT];

pub(crate) fn skipped_executor_full_ticks() -> u64 {
    SKIPPED_EXECUTOR_FULL_TICKS.load(Ordering::Relaxed)
}

/// Pairs skipped for want of a usable price, per reason.
pub(crate) fn price_skips() -> impl Iterator<Item = (&'static str, u64)> {
    PriceUnavailable::iter().map(|reason| {
        (
            reason.into(),
            PRICE_SKIPS[reason as usize].load(Ordering::Relaxed),
        )
    })
}

/// Worker inputs for pair clearing and optional RFQ routing. Internal clearing
/// visits every clearing pair (all confirmed at startup) and clears those with
/// a fresh price; the others pause alone and are counted. RFQ selection uses
/// fixed note limits and needs no price.
pub struct ClearingRuntime {
    /// The active orders, sent once after ingestion reconciles persisted notes
    /// against the chain.
    pub bootstrap: oneshot::Receiver<Vec<BookOrder>>,
    pub prices: watch::Receiver<Arc<PriceSnapshot>>,
    pub config: ClearingConfig,
    pub routing: Option<crate::router::Routing>,
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
    runtime.config.validate()?;
    let bootstrap = (&mut runtime.bootstrap).await?;
    let mut book = ClearingBook::default();
    for order in &bootstrap {
        book.insert_or_skip(order);
    }
    let mut interval = tokio::time::interval(match_interval);
    loop {
        // Update the book immediately; run matching only on the batch timer.
        tokio::select! {
            update = book_rx.recv() => {
                book.apply(update.ok_or(MatcherError::IngestStopped)?);
            }
            _ = interval.tick() => {
                book.apply_pending(&mut book_rx);
                let now = now_millis();
                if let Some(routing) = runtime.routing.as_mut() {
                    routing.release_expired(&mut book, now).map_err(MatcherError::Routing)?;
                }
                // Internal clearing has first claim on the book. While the
                // executor queue is full (busy, or verifying it can settle),
                // skip the whole tick: routing would otherwise send external
                // fillers orders that should cross internally next tick.
                if executor_accepting(&exec_tx)? {
                    internal_clear(&mut book, &runtime, &exec_tx)?;
                    if let Some(routing) = runtime.routing.as_mut() {
                        routing.dispatch(&mut book, now_millis()).map_err(MatcherError::Routing)?;
                    }
                }
                // Latest order-book levels for the price API's swap-ETA estimates.
                snapshot_tx.send_replace(Arc::new(book.best_levels_snapshot()));
            }
        }
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

/// Solve all pairs from the live book using one frozen price snapshot and
/// send the combined batch to the executor. A pair without a usable price or
/// that fails to clear is logged and skipped; an empty batch sends nothing.
pub(super) fn internal_clear(
    book: &mut ClearingBook,
    runtime: &ClearingRuntime,
    exec_tx: &mpsc::Sender<ExecutionBatch>,
) -> Result<(), MatcherError> {
    // One snapshot and one clock reading for the whole tick: a later quote
    // cannot reprice fills selected here or an in-flight settlement.
    let prices = runtime.prices.borrow().clone();
    let now = Instant::now();

    // Each independently solvent pair stays indivisible when the executor
    // splits the combined tick into protocol-sized transactions.
    let mut combined = ExecutionBatch {
        filled_notes: Vec::new(),
        group_ends: Vec::new(),
    };
    let mut included_pairs = 0usize;
    // Each clearing pair is one unordered market (checked when the plan was
    // built), so no order can be selected twice in a tick.
    for (base, quote) in prices.markets().clearing_pairs() {
        let price = match prices.pair_price(base, quote, now) {
            Ok(price) => price,
            Err(reason) => {
                PRICE_SKIPS[reason as usize].fetch_add(1, Ordering::Relaxed);
                tracing::debug!(%base, %quote, %reason, "no usable Binance price; pair skipped");
                continue;
            }
        };
        // The price is already in base units: the market plan knows both
        // tokens' decimals.
        let batch = match book.build_pair_batch(base, quote, price, &runtime.config) {
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
    use std::collections::HashMap;

    use super::*;
    use crate::db;
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
                db::postgres_db::register_token_tx(conn, token)?;
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
            .read(db::postgres_db::load_active_orders_tx)
            .await
            .unwrap();
        let mut book = ClearingBook::default();
        for order in &persisted {
            book.insert(order).unwrap();
        }
        // Both pairs at 2 quote per base, each quote received at its time.
        let ttl = Duration::from_secs(30);
        let prices = |miden_at: Instant, eth_at: Instant| {
            Arc::new(PriceSnapshot::for_tests(
                &[
                    (imiden(), iusdt(), "2", miden_at),
                    (ieth(), iusdt(), "2", eth_at),
                ],
                &[],
                ttl,
            ))
        };
        // A quote received a full TTL ago is stale.
        let stale = Instant::now().checked_sub(ttl).unwrap();
        let (prices_tx, prices_rx) = watch::channel(prices(stale, stale));
        let runtime = ClearingRuntime {
            bootstrap: oneshot::channel().1,
            prices: prices_rx,
            config: ClearingConfig::default(),
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let clear = |book: &mut ClearingBook, exec_tx: &mpsc::Sender<ExecutionBatch>| {
            if executor_accepting(exec_tx).unwrap() {
                internal_clear(book, &runtime, exec_tx).unwrap();
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

        let stale_skips = PRICE_SKIPS[PriceUnavailable::Stale as usize].load(Ordering::Relaxed);
        clear(&mut book, &exec_tx);
        assert!(exec_rx.try_recv().is_err(), "a stale price must not clear");
        assert_eq!(book.best_levels_snapshot().len(), 4);
        assert!(
            PRICE_SKIPS[PriceUnavailable::Stale as usize].load(Ordering::Relaxed)
                >= stale_skips + 2
        );

        // Only the pair with a fresh quote clears; the other keeps its orders.
        prices_tx.send_replace(prices(Instant::now(), stale));
        clear(&mut book, &exec_tx);
        let miden = exec_rx.try_recv().unwrap();
        assert_eq!(miden.group_ends, vec![2]);
        assert_eq!(book.best_levels_snapshot().len(), 2);
        prices_tx.send_replace(prices(Instant::now(), Instant::now()));
        clear(&mut book, &exec_tx);
        let eth = exec_rx.try_recv().unwrap();
        assert_eq!(eth.group_ends, vec![2]);
        let sent: HashMap<_, _> = miden
            .filled_notes
            .iter()
            .chain(&eth.filled_notes)
            .map(|filled| (filled.note_id, filled))
            .collect();
        assert_eq!(sent.len(), 4);
        for filled in sent.values() {
            let source = persisted
                .iter()
                .find(|order| order.id() == filled.note_id)
                .unwrap();
            assert!(Arc::ptr_eq(&filled.note, &source.note));
        }
        assert!(book.best_levels_snapshot().is_empty());
        clear(&mut book, &exec_tx);
        assert!(
            exec_rx.try_recv().is_err(),
            "pending orders must not be dispatched twice"
        );
        // Reactivated orders clear again, both pairs in one batch.
        for order in &persisted {
            book.insert(order).unwrap();
        }
        clear(&mut book, &exec_tx);
        let retried = exec_rx.try_recv().unwrap();
        assert_eq!(retried.group_ends, vec![2, 4]);
        for next in &retried.filled_notes {
            let first = sent[&next.note_id];
            assert_eq!(first.arrival_unix, next.arrival_unix);
            assert!(Arc::ptr_eq(&first.note, &next.note));
        }

        // A full executor queue must not stop the worker from receiving a
        // committed book update. Once capacity returns, the next tick clears
        // only against the updated book.
        let (_fresh_prices_tx, fresh_prices_rx) =
            watch::channel(prices(Instant::now(), Instant::now()));
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
            config: runtime.config,
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
                assert!(bootstrap_tx.send(persisted).is_ok());
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
    /// A pair listed the other way round on Binance, with unequal token
    /// decimals, clears at the pair price: USDC (6 decimals) / ETH (18
    /// decimals), approved on `ETHUSDC`, at 2500 USDC per ETH, i.e. 0.0004 ETH
    /// per USDC. Both orders are eligible only within 0.1% of that price, so
    /// any mispricing leaves them in the book. Swapping the two decimals at
    /// the conversion would misprice by 10^24 and clear nothing, so the second
    /// half checks that too.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_reversed_market_with_unequal_decimals_clears_at_the_exact_price() {
        use crate::db::postgres_models::NewOrderRow;
        use crate::db::postgres_test::TestDb;
        use miden_protocol::asset::{AssetAmount, FungibleAsset};
        use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
        use miden_protocol::note::{Note, NoteType};
        use miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;
        use miden_protocol::Word;
        use miden_standards::note::{PswapNote, PswapNoteStorage};

        let (usdc, eth) = (iusdt(), ieth());
        const USDC_DECIMALS: u8 = 6;
        const ETH_DECIMALS: u8 = 18;
        // 2.5 USDC is worth 0.001 ETH at the market; the orders' limits sit
        // 0.1% on either side of it.
        const USDC_2_5: u64 = 2_500_000;
        const MILLI_ETH: u64 = 1_000_000_000_000_000;
        const MILLI_ETH_MINUS: u64 = 999_000_000_000_000;
        const MILLI_ETH_PLUS: u64 = 1_001_000_000_000_000;

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
        // Seller of USDC: 2.5 USDC for at least 0.999 milli-ETH (asks 0.1%
        // below the market). Buyer: 1.001 milli-ETH for at least 2.5 USDC
        // (pays up to 0.1% above the market).
        let notes = [
            make_note(
                FungibleAsset::new(usdc, USDC_2_5).unwrap(),
                FungibleAsset::new(eth, MILLI_ETH_MINUS).unwrap(),
            ),
            make_note(
                FungibleAsset::new(eth, MILLI_ETH_PLUS).unwrap(),
                FungibleAsset::new(usdc, USDC_2_5).unwrap(),
            ),
        ];
        let seller_id = notes[0].id();
        let buyer_id = notes[1].id();
        pool.write(move |conn| {
            for token in [usdc, eth] {
                db::postgres_db::register_token_tx(conn, token)?;
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
            .read(db::postgres_db::load_active_orders_tx)
            .await
            .unwrap();
        let book = || {
            let mut book = ClearingBook::default();
            for order in &persisted {
                book.insert(order).unwrap();
            }
            book
        };
        // The market plan carries the tokens' decimals.
        let runtime = |decimals: &[(TokenId, u8)]| ClearingRuntime {
            bootstrap: oneshot::channel().1,
            prices: watch::channel(Arc::new(PriceSnapshot::for_tests_reversed(
                &[(usdc, eth, "0.0004", Instant::now())],
                decimals,
                Duration::from_secs(30),
            )))
            .1,
            config: ClearingConfig::default(),
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);

        // Right decimals: both orders cross within 0.1% of the market, so each
        // receives at least what it asked and at most the market's value.
        let right_decimals = runtime(&[(usdc, USDC_DECIMALS), (eth, ETH_DECIMALS)]);
        let mut right = book();
        internal_clear(&mut right, &right_decimals, &exec_tx).unwrap();
        let batch = exec_rx.try_recv().expect("the pair clears");
        assert_eq!(batch.group_ends, vec![2]);
        let filled: HashMap<_, _> = batch
            .filled_notes
            .iter()
            .map(|filled| (filled.note_id, filled.requested_filled))
            .collect();
        assert!(
            (MILLI_ETH_MINUS..=MILLI_ETH).contains(&filled[&seller_id]),
            "seller received {} ETH units",
            filled[&seller_id]
        );
        assert!(
            (USDC_2_5 - 2_500..=USDC_2_5 + 2_500).contains(&filled[&buyer_id]),
            "buyer received {} USDC units",
            filled[&buyer_id]
        );

        // Swapped decimals: the price is wrong by 10^24 and nothing is eligible.
        let swapped = runtime(&[(usdc, ETH_DECIMALS), (eth, USDC_DECIMALS)]);
        let mut wrong = book();
        internal_clear(&mut wrong, &swapped, &exec_tx).unwrap();
        assert!(
            exec_rx.try_recv().is_err(),
            "swapped decimals must not produce a batch"
        );
    }
}

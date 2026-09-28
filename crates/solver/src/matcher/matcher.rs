use anyhow::{ensure, Context, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::clearing_book::{ClearingBook, ClearingBootstrap};
use crate::clearing::{
    self, ClearingConfig, ClearingOutcome, PairMatcher, ReferencePrice, SkipReason,
};
use crate::matching::types::SwapBookSnapshot;
use crate::price::PreciseSnapshot;
use crate::types::*;

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
    fn validate(&self) -> Result<()> {
        self.config.validate()?;
        let mut pairs = HashSet::with_capacity(self.pairs.len());
        for &(base, quote) in &self.pairs {
            ensure!(
                base != quote,
                "a clearing pair must contain different assets"
            );
            let pair = if base < quote {
                (base, quote)
            } else {
                (quote, base)
            };
            ensure!(
                pairs.insert(pair),
                "duplicate clearing pair: {base}/{quote}"
            );
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
) -> Result<()> {
    // One cancellation boundary covers bootstrap, matching, and a blocked send.
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
) -> Result<()> {
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
                internal_clear(&mut book, &bootstrap.decimals, &runtime, &exec_tx, now).await?;
                if let Some(routing) = runtime.routing.as_mut() {
                    // Executor backpressure may have delayed this tick. Check RFQ
                    // expiry against the handover time, not the old tick timestamp.
                    routing.dispatch(&mut book, now_millis())?;
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

/// Solve all pairs from the live book using one frozen price snapshot.
pub(super) async fn internal_clear(
    book: &mut ClearingBook,
    decimals: &HashMap<TokenId, u8>,
    runtime: &ClearingRuntime,
    exec_tx: &mpsc::Sender<ExecutionBatch>,
    now_ms: u64,
) -> Result<()> {
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
    // Reserve before changing the book. Once reserved, deactivation and send
    // are synchronous: cancellation cannot leave half of the handoff applied.
    let permit = exec_tx
        .reserve()
        .await
        .context("executor stopped: execution batch receiver closed")?;
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
    Ok(())
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

    #[tokio::test]
    async fn exact_price_runtime_combines_two_pairs_in_one_execution_batch() {
        use crate::db::models::{NoteRow, OrderRow};
        use crate::db::{init_db, insert_notes_batch, register_token, set_token_metadata};
        use miden_protocol::asset::{AssetAmount, FungibleAsset};
        use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
        use miden_protocol::note::{Note, NoteType};
        use miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;
        use miden_protocol::Word;
        use miden_standards::note::{PswapNote, PswapNoteStorage};

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let pool = init_db(tmp.path().to_str().unwrap(), 2).unwrap();
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
        {
            let mut conn = pool.write_conn().unwrap();
            for token in [imiden(), iusdt(), ieth()] {
                register_token(&mut conn, &token.to_bytes(), None).unwrap();
                set_token_metadata(&mut conn, &token.to_bytes(), Some(0), None).unwrap();
            }
            let note_rows: Vec<_> = notes
                .iter()
                .map(|note| NoteRow {
                    note_id: note.id().to_bytes().to_vec(),
                    account_id: solver_id.to_bytes().to_vec(),
                    raw_data: note.to_bytes(),
                })
                .collect();
            let order_rows: Vec<_> = notes
                .iter()
                .map(|note| {
                    let order = crate::types::Order::from_note(note).unwrap();
                    OrderRow {
                        note_id: note.id().to_bytes().to_vec(),
                        account_id: solver_id.to_bytes().to_vec(),
                        requested_asset: order.requested_faucet_id.to_bytes().to_vec(),
                        requested_amount: order.requested_amount as i64,
                        offered_asset: order.offered_faucet_id.to_bytes().to_vec(),
                        offered_amount: order.offered_amount as i64,
                        timestamp: 1,
                        status: OrderStatus::Active.as_str().to_owned(),
                        priority_seq: 0,
                    }
                })
                .collect();
            insert_notes_batch(&mut conn, &note_rows, &order_rows, 1).unwrap();
        }
        let persisted = db::load_active_orders_with_notes(&mut pool.read_conn().unwrap()).unwrap();
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
        let error = internal_clear(&mut book, &decimals, &runtime, &closed_tx, 1_500)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("executor stopped"));
        assert_eq!(
            book.best_levels_snapshot().len(),
            4,
            "failed dispatch must leave orders live"
        );
        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500)
            .await
            .unwrap();
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
        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500)
            .await
            .unwrap();
        assert!(
            exec_rx.try_recv().is_err(),
            "pending orders must not be dispatched twice"
        );
        for order in &persisted {
            book.insert(order).unwrap();
        }
        internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500)
            .await
            .unwrap();
        let retried = exec_rx.try_recv().unwrap();
        for (first, next) in execution.filled_notes.iter().zip(&retried.filled_notes) {
            assert_eq!(first.note_id, next.note_id);
            assert_eq!(first.arrival_unix, next.arrival_unix);
            assert!(Arc::ptr_eq(&first.note, &next.note));
        }
    }
}

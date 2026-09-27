use miden_protocol::account::AccountId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::clearing::{
    self, ClearingConfig, ClearingOutcome, PairMatcher, ReferencePrice, SkipReason,
};
use crate::db::{self, DbPool};
use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::matching::types::{Order, SwapBookSnapshot};
use crate::price::{PreciseSnapshot, PriceSnapshot, WatchPriceFeed};
use crate::router::{select_notes, Pair, QuotesSnapshot, RouteBatch, RoutedNote};
// `now_unix` / `UnixSecs` come from here (deduped — was a local copy).
use super::clearing_book::{run_clearer, ClearingBook, ClearingBootstrap};
use crate::types::*;

/// Hooks that enable the external-liquidity pass in the matcher tick. When the
/// router is disabled these are absent and the matcher behaves exactly as before.
/// All fields are `Send`; the router itself runs on its own OS thread.
pub struct RouterHooks {
    /// Latest standing quotes from connected DEXes (filtered by freshness here).
    pub quotes_rx: watch::Receiver<Arc<QuotesSnapshot>>,
    /// Selected notes pushed to the router for delivery (`try_send`, never blocks).
    pub route_tx: mpsc::Sender<RouteBatch>,
    /// How long a handed-over note waits for on-chain consume before reactivating.
    pub inflight_ttl_ms: u64,
}

/// Optional replacement for legacy internal matching. Prices come from the
/// existing price service's single published snapshot, never from its rounded
/// cents side-channel. Missing, stale, or inexact prices leave notes untouched.
pub struct ClearingRuntime {
    pub bootstrap: oneshot::Receiver<ClearingBootstrap>,
    pub prices: watch::Receiver<PreciseSnapshot>,
    pub pairs: Vec<(TokenId, TokenId)>,
    pub config: ClearingConfig,
    pub max_price_age_ms: u64,
    pub max_source_age_ms: u64,
    pub max_source_skew_ms: u64,
    pub solver_id: AccountId,
}

/// The matcher owns a persistent OrderBook and runs matching on a timer.
///
/// On startup, the book is hydrated from the DB (`load_active_orders_with_notes`)
/// so orders persisted by ingest but never delivered through the channel
/// (e.g. crash between DB write and channel send) are still considered.
/// DB is the source of truth; in-memory state is rebuildable from it.
///
/// Each tick: apply ordered book updates →
/// reactivate timed-out parked notes → run internal matching (→ executor) →
/// run the external pass (→ router), if enabled. The external pass and
/// reactivation run on EVERY tick (no early `continue`), since the
/// zero-internal-match tick is exactly when external routing matters most.
/// It also stamps each order's arrival and publishes a top-of-book snapshot
/// every tick for the swap-eta API.
#[allow(clippy::too_many_arguments)]
pub async fn run_matcher(
    pool: DbPool,
    mut book_rx: mpsc::Receiver<BookUpdate>,
    price_rx: watch::Receiver<PriceSnapshot>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    triangular_enabled: bool,
    // Publishes the top-of-book snapshot each tick for the swap-eta API. Read
    // lock-free off-thread, so wallet ETA traffic never touches the live book.
    swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    mut router: Option<RouterHooks>,
    clearing: Option<ClearingRuntime>,
    cancel: CancellationToken,
) {
    if let Some(runtime) = clearing {
        run_clearer(
            book_rx,
            exec_tx,
            match_interval,
            swap_snapshot_tx,
            runtime,
            cancel,
        )
        .await;
        return;
    }
    let feed = WatchPriceFeed::from_watch(&price_rx);
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book).with_triangular_enabled(triangular_enabled);

    // Map from OrderId → raw note data for building FilledNotes / handovers.
    let mut raw_notes: HashMap<OrderId, Vec<u8>> = HashMap::new();
    let mut priority_seqs: HashMap<OrderId, u64> = HashMap::new();
    // Per-order arrival time, carried onto FilledNote for the swap-eta window.
    let mut arrivals: HashMap<OrderId, UnixSecs> = HashMap::new();

    // Monotonic-clamped wall clock (SystemTime can step backwards).
    let mut last_now: u64 = 0;

    // Hydrate the in-memory book from DB.
    match pool.read_conn() {
        Ok(mut conn) => match db::load_active_orders_with_notes(&mut conn) {
            Ok(loaded) => {
                let n = loaded.len();
                for order in loaded {
                    engine.book.add_user_order_with_min_fill(
                        order.note_id,
                        order.offered_token,
                        order.requested_token,
                        order.offered_amount,
                        order.requested_amount,
                        order.min_fill_step,
                    );
                    arrivals.insert(order.note_id, now_unix());
                    priority_seqs.insert(order.note_id, order.priority_seq);
                    raw_notes.insert(order.note_id, order.raw_note_data);
                }
                if n > 0 {
                    tracing::info!(count = n, "hydrated active orders from DB");
                }
            }
            Err(e) => tracing::error!(error = %e, "matcher hydration query failed"),
        },
        Err(e) => tracing::error!(error = %e, "matcher hydration: read_conn failed"),
    }

    let mut interval = tokio::time::interval(match_interval);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("matcher cancelled, shutting down");
                return;
            }
            _ = interval.tick() => {}
        }

        let now = {
            last_now = last_now.max(now_millis());
            last_now
        };

        // Each committed update removes parents and activates remainders together.
        for _ in 0..book_rx.len() {
            let Ok(update) = book_rx.try_recv() else {
                break;
            };
            for note_id in update.removed {
                engine.book.remove_order(note_id);
                raw_notes.remove(&note_id);
                priority_seqs.remove(&note_id);
                arrivals.remove(&note_id);
            }
            for order in update.active {
                engine.book.add_user_order_with_min_fill(
                    order.note_id,
                    order.offered_token,
                    order.requested_token,
                    order.offered_amount,
                    order.requested_amount,
                    order.min_fill_step,
                );
                arrivals.entry(order.note_id).or_insert_with(now_unix);
                priority_seqs.insert(order.note_id, order.priority_seq);
                raw_notes.insert(order.note_id, order.raw_note_data);
            }
        }

        // 1. Reactivate parked notes whose DEX no-showed past the in-flight TTL.
        if let Some(r) = router.as_ref() {
            for (id, dex) in engine
                .book
                .reactivate_parked_older_than(r.inflight_ttl_ms, now)
            {
                tracing::debug!(note = %id, dex, "parked note timed out; reactivated");
            }
        }

        // 2. Internal matching (→ executor).
        let stop = internal_match(
            &mut engine,
            &mut raw_notes,
            &mut priority_seqs,
            &mut arrivals,
            &price_rx,
            &exec_tx,
        )
        .await;
        if stop {
            return; // executor channel closed
        }

        // 3. External matching: hand residual notes to DEXes whose quotes clear them.
        if let Some(r) = router.as_ref() {
            if external_pass(&mut engine, &raw_notes, r, now) {
                router = None; // router channel closed — stop routing
            }
        }
        // Publish the post-tick top-of-book for the swap-eta API — every tick,
        // including empty ones, so it never goes stale. Latest-wins, non-blocking;
        // reflects the residual (after matching + routing) a new order would cross.
        swap_snapshot_tx.send_replace(Arc::new(engine.book.snapshot_best_levels()));
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
) -> bool {
    let prices = runtime.prices.borrow().clone();

    // Each independently solvent pair stays indivisible when the executor
    // splits the combined tick into protocol-sized transactions.
    let mut combined = ExecutionBatch {
        filled_notes: Vec::new(),
        group_ends: Vec::new(),
    };
    let mut selected = HashSet::new();
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
        .and_then(|price| book.build_pair_batch(base, quote, price, &runtime.config, &selected));
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
        let execution = match plan.to_execution_batch(&batch, &book.arrivals) {
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
        for filled in &execution.filled_notes {
            selected.insert(filled.note_id);
        }
        combined.filled_notes.extend(execution.filled_notes);
        combined.group_ends.push(combined.filled_notes.len());
        included_pairs += 1;
    }
    if combined.filled_notes.is_empty() {
        return false;
    }
    let filled_ids: Vec<_> = combined
        .filled_notes
        .iter()
        .map(|note| note.note_id)
        .collect();
    tracing::info!(
        pairs = included_pairs,
        orders = filled_ids.len(),
        "combined clearing batch sent to executor"
    );
    if exec_tx.send(combined).await.is_err() {
        tracing::error!("executor channel closed during clearing");
        return true;
    }
    // Keep the parents in memory but off the matchable index. A definite
    // failure reactivates them; confirmation removes them permanently.
    for note_id in filled_ids {
        book.deactivate(note_id);
    }
    false
}

/// Internal matching pass — unchanged from the non-routing path: refresh the price
/// feed, run the engine, and settle any filled notes to the executor. A no-op when
/// the book is empty or nothing crosses. Returns `true` if the executor channel
/// closes or an authoritative input is missing (the matcher should stop).
async fn internal_match(
    engine: &mut MatchingEngine<WatchPriceFeed>,
    raw_notes: &mut HashMap<OrderId, Vec<u8>>,
    priority_seqs: &mut HashMap<OrderId, u64>,
    arrivals: &mut HashMap<OrderId, u64>,
    price_rx: &watch::Receiver<PriceSnapshot>,
    exec_tx: &mpsc::Sender<ExecutionBatch>,
) -> bool {
    if engine.book.orders.is_empty() {
        return false;
    }
    engine.book.feed = WatchPriceFeed::from_watch(price_rx);
    let batch = engine.run();
    if batch.filled_orders.is_empty() {
        return false;
    }
    tracing::info!(orders = batch.filled_orders.len(), "matcher produced batch");
    let mut filled_notes = Vec::new();
    for &order_id in &batch.filled_orders {
        let Some(raw_note_data) = raw_notes.get(&order_id).cloned() else {
            tracing::error!(note = %order_id, "matched note lacks raw data; stopping matcher");
            return true;
        };
        let Some(&priority_seq) = priority_seqs.get(&order_id) else {
            tracing::error!(note = %order_id, "matched note lacks persisted FIFO sequence; stopping matcher");
            return true;
        };
        let requested_filled = engine
            .book
            .orders
            .get(&order_id)
            .map(|o| o.requested_filled())
            .unwrap_or(0);
        filled_notes.push(FilledNote {
            note_id: order_id,
            priority_seq,
            requested_filled,
            raw_note_data,
            arrival_unix: arrivals.get(&order_id).copied().unwrap_or_else(now_unix),
        });
    }
    if exec_tx
        .send(ExecutionBatch {
            filled_notes,
            group_ends: Vec::new(),
        })
        .await
        .is_err()
    {
        tracing::warn!("executor channel closed, matcher shutting down");
        return true;
    }
    // The executor owns the pending batch. Keep its parents off the
    // matchable index until a definite failure re-feeds them.
    for &order_id in &batch.filled_orders {
        engine.book.remove_order(order_id);
        raw_notes.remove(&order_id);
        priority_seqs.remove(&order_id);
        arrivals.remove(&order_id);
    }
    engine.book.protocol_balances.clear();
    false
}

/// Select residual notes against the cached quotes, park each pick, and
/// `try_send` a handover. Never `.await`s. Returns `true` if the router channel
/// has **closed** — the caller should then stop the external pass.
fn external_pass(
    engine: &mut MatchingEngine<WatchPriceFeed>,
    raw_notes: &HashMap<OrderId, Vec<u8>>,
    r: &RouterHooks,
    now: u64,
) -> bool {
    let quotes = r.quotes_rx.borrow().clone(); // Arc<QuotesSnapshot>

    let items = route_external(&mut engine.book, raw_notes, &quotes, now);
    if items.is_empty() {
        return false;
    }

    let n = items.len();
    match r.route_tx.try_send(RouteBatch { items }) {
        Ok(()) => {
            tracing::info!(count = n, "routed unmatched notes to DEXes");
            false
        }
        Err(e) => {
            // Not delivered → unpark so the notes stay eligible. A closed channel
            // means the router thread is gone: also tell the caller to stop.
            let closed = matches!(e, mpsc::error::TrySendError::Closed(_));
            for item in &e.into_inner().items {
                engine.book.unpark(item.note_id);
            }
            if closed {
                tracing::error!(count = n, "router channel closed; disabling external pass");
            } else {
                tracing::warn!(count = n, "handover not sent (channel full); unparked");
            }
            closed
        }
    }
}

/// Pure core of the external pass: select residual notes against the cached
/// quotes, **park** each pick (removing it from the matching index), and return
/// the handover items. `book` is mutated only via `park`.
fn route_external<F: crate::matching::price_feed::PriceFeed>(
    book: &mut OrderBook<F>,
    raw_notes: &HashMap<OrderId, Vec<u8>>,
    quotes: &QuotesSnapshot,
    now: u64,
) -> Vec<RoutedNote> {
    if quotes.is_empty() {
        return Vec::new();
    }
    // Candidates per quoted pair, straight from the book index so they arrive
    // rate-ordered (parked notes aren't in the index). Route only WHOLE notes: a
    // partially-filled note is left for internal matching — v1 hands whole notes.
    let mut notes_by_pair: HashMap<Pair, Vec<Order>> = HashMap::new();
    for pair in quotes.keys() {
        let notes: Vec<Order> = book
            .notes_for_pair(pair.0, pair.1)
            .into_iter()
            .filter(|o| o.requested_remaining == o.requested)
            .collect();
        if !notes.is_empty() {
            notes_by_pair.insert(*pair, notes);
        }
    }
    if notes_by_pair.is_empty() {
        return Vec::new();
    }

    let picks = select_notes(&notes_by_pair, quotes, now);

    let mut items = Vec::with_capacity(picks.len());
    for pick in &picks {
        // Skip (don't park) a note whose raw bytes we don't have — parking it
        // without a handover would strand it until the TTL for nothing.
        let Some(bytes) = raw_notes.get(&pick.note_id) else {
            tracing::warn!(note = %pick.note_id, "no raw note data; skipping external route");
            continue;
        };
        book.park(pick.note_id, pick.dex, now);
        items.push(RoutedNote {
            dex: pick.dex,
            note_id: pick.note_id,
            fill: pick.fill,
            pair: pick.pair,
            note_bytes: bytes.clone(),
        });
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::order_book::OrderBook;
    use crate::matching::types::DexId;
    use crate::price::PriceData;
    use crate::price::WatchPriceFeed;
    use crate::router::Quote;
    use miden_protocol::crypto::utils::Serializable;
    use miden_protocol::note::NoteId;
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
    fn nid(seed: u64) -> NoteId {
        NoteId::try_from_hex(&format!("0x{seed:064x}")).unwrap()
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
            book.insert(order, solver_id).unwrap();
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
            solver_id,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        assert!(internal_clear(&mut book, &decimals, &runtime, &closed_tx, 1_500).await);
        assert_eq!(
            book.arrivals.len(),
            4,
            "failed dispatch must leave orders live"
        );
        assert!(!internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500,).await);
        let execution = exec_rx.try_recv().unwrap();
        assert_eq!(execution.filled_notes.len(), 4);
        assert_eq!(execution.group_ends, vec![2, 4]);
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(book.arrivals.len(), 4);
        assert!(!internal_clear(&mut book, &decimals, &runtime, &exec_tx, 1_500).await);
        assert!(
            exec_rx.try_recv().is_err(),
            "pending orders must not be dispatched twice"
        );
    }
    /// A quote for the IMIDEN/IUSDT pair at base-unit rate 1/50 (requested-base per
    /// offered-base). Any note whose rate is at or below this is willing.
    fn quote_at_mid(dex: DexId, supply: Amount, expires_at: u64) -> Quote {
        // rate supply/demand = 1/50; `supply` is the capacity.
        Quote {
            dex,
            pair: (imiden(), iusdt()),
            supply,
            demand: supply.saturating_mul(50),
            expires_at,
        }
    }
    // Group quotes into the published snapshot shape (by pair, rate-sorted like the router).
    fn snap(quotes: Vec<Quote>) -> QuotesSnapshot {
        let mut by_pair: QuotesSnapshot = HashMap::new();
        for q in quotes {
            by_pair.entry(q.pair).or_default().push(q);
        }
        for list in by_pair.values_mut() {
            list.sort_by(|a, b| {
                (b.supply as u128 * a.demand as u128).cmp(&(a.supply as u128 * b.demand as u128))
            });
        }
        by_pair
    }
    // Offer `offered` IMIDEN for `requested` IUSDT.
    fn book_with_order(
        id: NoteId,
        offered: Amount,
        requested: Amount,
    ) -> (OrderBook<WatchPriceFeed>, HashMap<OrderId, Vec<u8>>) {
        let mut book = OrderBook::new(WatchPriceFeed::new());
        book.add_user_order(id, imiden(), iusdt(), offered, requested);
        let mut raw = HashMap::new();
        raw.insert(id, vec![0xAA, 0xBB, 0xCC]);
        (book, raw)
    }

    /// The user's scenario: an unmatched order + a clearing DEX quote ⇒ the note
    /// is parked (orderbook change) and a handover is emitted to that DEX.
    #[test]
    fn unmatched_order_with_clearing_quote_is_parked_and_handed_over() {
        let id = nid(1);
        // Offer 1.1 IMIDEN for 2 IUSDT — the DEX (quoting 1/50) is willing → exported.
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        assert_eq!(book.active_order_count(), 1);
        let items = route_external(
            &mut book,
            &raw,
            &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]),
            1_000,
        );

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].dex, 7);
        assert_eq!(items[0].note_id, id);
        assert_eq!(items[0].fill, 2_000_000);
        assert_eq!(items[0].note_bytes, vec![0xAA, 0xBB, 0xCC]);
        // The note is parked, invisible to internal matching.
        assert!(book.is_parked(id));
        assert_eq!(book.active_order_count(), 0);
        assert!(book.best_order(imiden(), iusdt()).is_none());
    }

    #[test]
    fn unwilling_order_retained_not_exported() {
        let id = nid(2);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        // The DEX accepts only 1/100 — below the note's rate → unwilling.
        let q = Quote {
            dex: 7,
            pair: (imiden(), iusdt()),
            supply: 10_000_000,
            demand: 1_000_000_000,
            expires_at: u64::MAX,
        };
        let items = route_external(&mut book, &raw, &snap(vec![q]), 1_000);
        assert!(items.is_empty());
        assert!(
            !book.is_parked(id),
            "an unroutable order stays matchable internally"
        );
        assert_eq!(book.active_order_count(), 1);
    }

    #[test]
    fn partially_filled_note_not_routed() {
        let id = nid(8);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        book.orders.get_mut(&id).unwrap().fill(1_000_000); // partial internal fill
        let items = route_external(
            &mut book,
            &raw,
            &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]),
            1_000,
        );
        assert!(items.is_empty(), "v1 routes whole notes only");
        assert!(!book.is_parked(id));
    }

    #[test]
    fn missing_raw_bytes_skips_without_parking() {
        let id = nid(9);
        let (mut book, _raw) = book_with_order(id, 110_000_000, 2_000_000);
        // Candidate in the book, but its bytes are absent → skipped, not parked.
        let items = route_external(
            &mut book,
            &HashMap::new(),
            &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]),
            1_000,
        );
        assert!(items.is_empty());
        assert!(!book.is_parked(id), "no raw bytes → not parked");
    }

    #[test]
    fn stale_quote_no_handover() {
        let id = nid(3);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        // expires_at == now ⇒ stale (strict >).
        let items = route_external(
            &mut book,
            &raw,
            &snap(vec![quote_at_mid(7, 10_000_000, 1_000)]),
            1_000,
        );
        assert!(items.is_empty());
        assert!(!book.is_parked(id));
    }

    /// End-to-end through the real `run_matcher` tick loop: an unmatched order +
    /// a DEX quote ⇒ a handover is emitted and the note is NOT sent to the
    /// executor. Covers hydration, the channel drains, the internal-match path
    /// (no counterparty), and the external pass with a real DB decimals load.
    #[tokio::test]
    async fn run_matcher_routes_unmatched_order_to_dex() {
        use crate::db::{init_db, register_token, set_token_metadata};
        use miden_protocol::crypto::utils::Serializable;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let pool = init_db(tmp.path().to_str().unwrap(), 2).unwrap();
        {
            let mut conn = pool.write_conn().unwrap();
            register_token(&mut conn, &imiden().to_bytes(), None).unwrap();
            register_token(&mut conn, &iusdt().to_bytes(), None).unwrap();
            set_token_metadata(&mut conn, &imiden().to_bytes(), Some(8), None).unwrap();
            set_token_metadata(&mut conn, &iusdt().to_bytes(), Some(6), None).unwrap();
        }

        let (book_tx, book_rx) = mpsc::channel(16);
        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(HashMap::new()));
        let (route_tx, mut route_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();
        quotes_tx
            .send(Arc::new(snap(vec![quote_at_mid(
                1,
                1_000_000_000,
                u64::MAX,
            )])))
            .unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 60_000,
        };
        let id = nid(777);
        let order = IngestOrder {
            note_id: id,
            priority_seq: 1,
            offered_token: imiden(),
            requested_token: iusdt(),
            offered_amount: 110_000_000, // 1.1 IMIDEN = $2.20
            requested_amount: 2_000_000, // 2 IUSDT = $2.00 → +10% generous
            min_fill_step: 0,
            raw_note_data: vec![1, 2, 3, 4],
        };
        book_tx.send(order.into()).await.unwrap();

        // Matcher is `spawn_local` in production (mirror that here).
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    book_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
                    None,
                    cancel.clone(),
                ));

                let handover = tokio::time::timeout(Duration::from_secs(2), route_rx.recv())
                    .await
                    .expect("handover within timeout")
                    .expect("handover present");
                assert_eq!(handover.items.len(), 1);
                assert_eq!(handover.items[0].note_id, id);
                assert_eq!(handover.items[0].dex, 1);
                assert_eq!(handover.items[0].note_bytes, vec![1, 2, 3, 4]);
                // No internal counterparty → nothing handed to the executor.
                assert!(exec_rx.try_recv().is_err());

                cancel.cancel();
                let _ = task.await;
            })
            .await;
    }

    /// FULL LOOP through the public SDK and the real router thread: a DEX
    /// (`LpClient`) connects and posts a filler-centric **RFQ quote**; an
    /// unmatched **order** sits in the matcher; the real `run_matcher` external
    /// pass selects it, and the **handover travels all the way back to the SDK**
    /// as an `LpEvent::Handover` carrying the decoded `Note`. This is the
    /// end-to-end the hand-injected `integration_lp_sdk` test does NOT cover:
    /// SDK quote → router → matcher select → router → SDK handover, nothing
    /// mocked in between. Feeds a **real serialized note** (the SDK decodes it on
    /// the way back — fake bytes would be dropped at `Note::read_from_bytes`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sdk_quote_and_order_route_through_real_matcher_back_to_sdk() {
        use crate::db::{init_db, register_token, set_token_metadata};
        use crate::router::{spawn_router_thread, RouterConfig};
        use miden_protocol::asset::FungibleAsset;
        use miden_protocol::crypto::utils::Serializable;
        use miden_protocol::note::Note;
        use miden_protocol::Word;
        use pswap_lp_sdk::{Handover, LpClient, LpEvent};

        // DB with both tokens priced + decimalled (export gates need both).
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let pool = init_db(tmp.path().to_str().unwrap(), 2).unwrap();
        {
            let mut conn = pool.write_conn().unwrap();
            register_token(&mut conn, &imiden().to_bytes(), None).unwrap();
            register_token(&mut conn, &iusdt().to_bytes(), None).unwrap();
            set_token_metadata(&mut conn, &imiden().to_bytes(), Some(8), None).unwrap();
            set_token_metadata(&mut conn, &iusdt().to_bytes(), Some(6), None).unwrap();
        }

        let (book_tx, book_rx) = mpsc::channel(16);
        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let (exec_tx, _exec_rx) = mpsc::channel(16);
        // The router owns quotes_tx (publishes DEX quotes) + route_rx (delivers
        // handovers); the matcher owns quotes_rx + route_tx. Real wiring.
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(HashMap::new()));
        let (route_tx, route_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();

        // Real router thread on an ephemeral port.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["dex-tok".into()],
        };
        let (router_thread, ready) =
            spawn_router_thread(cfg, quotes_tx, route_rx, cancel.clone()).unwrap();
        ready.await.unwrap().expect("router bound");

        // A REAL serialized note; its id is the OrderId (as ingest computes it).
        let note = Note::mock_noop(Word::from([0x00C0_FFEEu32, 1, 2, 3]));
        let note_bytes = note.to_bytes();
        let id = note.id();

        // An unmatched order: offer 1.1 IMIDEN for 2 IUSDT — the DEX quote below
        // crosses the note's rate, so the note is willing and gets routed.
        let order = IngestOrder {
            note_id: id,
            priority_seq: 1,
            offered_token: imiden(),
            requested_token: iusdt(),
            offered_amount: 110_000_000,
            requested_amount: 2_000_000,
            min_fill_step: 0,
            raw_note_data: note_bytes.clone(),
        };
        book_tx.send(order.into()).await.unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 60_000,
        };
        let url = format!("ws://127.0.0.1:{port}/v1/rfq");

        // Matcher is current-thread + LocalSet in production — mirror that.
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    book_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
                    None,
                    cancel.clone(),
                ));

                // The DEX connects and posts a filler-centric quote: it GIVES iusdt
                // and WANTS imiden (to fill a note that offers imiden / wants iusdt),
                // at a rate that crosses the note (2e6 iusdt : 1e8 imiden).
                let mut client = LpClient::connect(&url, "dex-tok").await.expect("connect");
                assert!(matches!(client.next_event().await, Some(LpEvent::AuthOk)));
                client
                    .quote(
                        FungibleAsset::new(iusdt(), 2_000_000_000).unwrap(),
                        FungibleAsset::new(imiden(), 100_000_000_000).unwrap(),
                        None,
                    )
                    .unwrap();

                // The matcher selects the order; the handover returns to the SDK.
                let handover = loop {
                    let ev = tokio::time::timeout(Duration::from_secs(5), client.next_event())
                        .await
                        .expect("handover within timeout")
                        .expect("event present");
                    match ev {
                        LpEvent::Handover(h) => break h,
                        LpEvent::Disconnected { reason } => panic!("disconnected: {reason}"),
                        _ => continue, // ignore any Ask/Error/reconnect noise
                    }
                };
                let Handover {
                    note: got,
                    fill_amount,
                } = handover;
                assert_eq!(fill_amount, 2_000_000, "full requested amount");
                assert_eq!(got.id(), id, "the exact note we fed, decoded round-trip");

                drop(client); // let the router's graceful shutdown complete
                cancel.cancel();
                let _ = task.await;
            })
            .await;

        tokio::task::spawn_blocking(move || router_thread.join().unwrap())
            .await
            .unwrap();
    }

    /// Backpressure rollback: when the handover channel is **full**, the external
    /// pass must not stall — and the dropped batch is **rolled back** (notes
    /// unparked), so a note the DEX never received is not
    /// penalized. It stays immediately eligible: once the channel drains, a retry
    /// re-routes it to that **same** DEX. Exercises the `try_send` Full branch and
    /// the rollback in `external_pass`.
    #[test]
    fn full_handover_channel_rolls_back_so_dropped_note_stays_eligible() {
        use crate::db::{init_db, register_token, set_token_metadata};
        use miden_protocol::crypto::utils::Serializable;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let pool = init_db(tmp.path().to_str().unwrap(), 2).unwrap();
        {
            let mut conn = pool.write_conn().unwrap();
            register_token(&mut conn, &imiden().to_bytes(), None).unwrap();
            register_token(&mut conn, &iusdt().to_bytes(), None).unwrap();
            set_token_metadata(&mut conn, &imiden().to_bytes(), Some(8), None).unwrap();
            set_token_metadata(&mut conn, &iusdt().to_bytes(), Some(6), None).unwrap();
        }

        let (_qtx, quotes_rx) = watch::channel(Arc::new(snap(vec![quote_at_mid(
            1,
            1_000_000_000,
            u64::MAX,
        )])));

        // Capacity-1 handover channel, pre-filled → the first try_send is Full.
        let (route_tx, mut route_rx) = mpsc::channel::<RouteBatch>(1);
        route_tx.try_send(RouteBatch { items: vec![] }).unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 60_000,
        };

        let id = nid(99);
        let (book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let mut engine = MatchingEngine::new(book).with_triangular_enabled(false);

        // (1) Full channel → the handover is dropped and rolled back: unparked,
        //     back in internal matching. No hang, no penalty.
        external_pass(&mut engine, &raw, &hooks, 1_000);
        assert!(
            !engine.book.is_parked(id),
            "dropped handover rolled back — note not left parked"
        );
        assert_eq!(
            engine.book.active_order_count(),
            1,
            "note is eligible again"
        );

        // (2) Drain the channel, retry → the note re-routes to the SAME DEX and is
        //     delivered. The drop cost it nothing.
        let _ = route_rx.try_recv(); // free the slot
        external_pass(&mut engine, &raw, &hooks, 2_000);
        assert!(
            engine.book.is_parked(id),
            "after the drop the note re-routes to the same DEX"
        );
        let delivered = route_rx.try_recv().expect("handover delivered on retry");
        assert_eq!(delivered.items.len(), 1);
        assert_eq!(delivered.items[0].note_id, id);
    }

    #[test]
    fn closed_route_channel_reported_and_unparked() {
        let id = nid(100);
        let (_qtx, quotes_rx) = watch::channel(Arc::new(snap(vec![quote_at_mid(
            1,
            1_000_000_000,
            u64::MAX,
        )])));
        let (route_tx, route_rx) = mpsc::channel::<RouteBatch>(1);
        drop(route_rx); // receiver gone → channel closed
        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 60_000,
        };
        let (book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let mut engine = MatchingEngine::new(book).with_triangular_enabled(false);
        assert!(
            external_pass(&mut engine, &raw, &hooks, 1_000),
            "closed channel is reported"
        );
        assert!(
            !engine.book.is_parked(id),
            "note unparked on closed channel"
        );
    }

    fn harness_db() -> (tempfile::NamedTempFile, DbPool) {
        use crate::db::{init_db, register_token, set_token_metadata};
        use miden_protocol::crypto::utils::Serializable;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let pool = init_db(tmp.path().to_str().unwrap(), 2).unwrap();
        {
            let mut conn = pool.write_conn().unwrap();
            register_token(&mut conn, &imiden().to_bytes(), None).unwrap();
            register_token(&mut conn, &iusdt().to_bytes(), None).unwrap();
            set_token_metadata(&mut conn, &imiden().to_bytes(), Some(8), None).unwrap();
            set_token_metadata(&mut conn, &iusdt().to_bytes(), Some(6), None).unwrap();
        }
        (tmp, pool)
    }

    /// Router disabled: two crossing orders match internally and produce an
    /// ExecutionBatch (covers the internal-settle path + the no-router branches).
    #[tokio::test]
    async fn run_matcher_internal_match_emits_exec_batch() {
        let (_tmp, pool) = harness_db();
        let (book_tx, book_rx) = mpsc::channel(16);
        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        // Direct matching gates on the price feed being present for both tokens.
        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();

        // Maker offers 1 IMIDEN for 2 IUSDT; taker offers 2.1 IUSDT for 1 IMIDEN
        // → crossing with surplus, so direct matching fills both.
        let maker = IngestOrder {
            note_id: nid(1),
            priority_seq: 1,
            offered_token: imiden(),
            requested_token: iusdt(),
            offered_amount: 100_000_000,
            requested_amount: 200_000_000,
            min_fill_step: 0,
            raw_note_data: vec![1],
        };
        let taker = IngestOrder {
            note_id: nid(2),
            priority_seq: 2,
            offered_token: iusdt(),
            requested_token: imiden(),
            offered_amount: 210_000_000,
            requested_amount: 100_000_000,
            min_fill_step: 0,
            raw_note_data: vec![2],
        };
        book_tx.send(maker.into()).await.unwrap();
        book_tx.send(taker.into()).await.unwrap();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    book_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    None, // router disabled
                    None,
                    cancel.clone(),
                ));
                let batch = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
                    .await
                    .expect("exec batch within timeout")
                    .expect("batch present");
                assert!(!batch.filled_notes.is_empty(), "internal match settled");
                cancel.cancel();
                let _ = task.await;
            })
            .await;
    }

    /// A handed-over note that the DEX doesn't consume is reactivated after the
    /// in-flight TTL and re-offered to the same DEX (a second handover), then
    /// consumed on-chain.
    #[tokio::test]
    async fn run_matcher_reactivates_and_consumes_parked_note() {
        let (_tmp, pool) = harness_db();
        let (book_tx, book_rx) = mpsc::channel(16);
        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let (exec_tx, _exec_rx) = mpsc::channel(16);
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(HashMap::new()));
        let (route_tx, mut route_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();
        quotes_tx
            .send(Arc::new(snap(vec![quote_at_mid(
                1,
                1_000_000_000,
                u64::MAX,
            )])))
            .unwrap();

        let id = nid(50);
        let order = IngestOrder {
            note_id: id,
            priority_seq: 1,
            offered_token: imiden(),
            requested_token: iusdt(),
            offered_amount: 110_000_000,
            requested_amount: 2_000_000,
            min_fill_step: 0,
            raw_note_data: vec![9],
        };
        book_tx.send(order.into()).await.unwrap();

        // Tiny in-flight TTL so the parked note reactivates quickly.
        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 1,
        };
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    book_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
                    None,
                    cancel.clone(),
                ));
                // First handover (note parked).
                let h = tokio::time::timeout(Duration::from_secs(2), route_rx.recv())
                    .await
                    .expect("first handover")
                    .unwrap();
                assert_eq!(h.items[0].note_id, id);
                // After the TTL the note reactivates and is handed to the DEX again.
                let second = tokio::time::timeout(Duration::from_secs(2), route_rx.recv())
                    .await
                    .expect("second handover after reactivation")
                    .unwrap();
                assert_eq!(second.items[0].note_id, id);
                // Now the order is consumed on-chain → release path runs.
                book_tx
                    .send(BookUpdate {
                        removed: vec![id],
                        active: Vec::new(),
                    })
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(40)).await;
                cancel.cancel();
                let _ = task.await;
            })
            .await;
    }

    #[test]
    fn empty_book_or_no_quotes_is_noop() {
        // No quotes.
        let id = nid(6);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let items = route_external(&mut book, &raw, &snap(vec![]), 1_000);
        assert!(items.is_empty());
        assert!(!book.is_parked(id));
        // Empty book.
        let mut empty = OrderBook::new(WatchPriceFeed::new());
        let items2 = route_external(
            &mut empty,
            &HashMap::new(),
            &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]),
            1_000,
        );
        assert!(items2.is_empty());
    }
}

use miden_protocol::note::NoteId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::db::{self, DbPool};
use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::matching::types::{Order, SwapBookSnapshot};
use crate::price::{PriceSnapshot, WatchPriceFeed};
use crate::router::{select_notes, Pair, QuotesSnapshot, RouteBatch, RoutedNote};
// `now_unix` / `UnixSecs` come from here (deduped — was a local copy).
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

/// The matcher owns a persistent OrderBook and runs matching on a timer.
///
/// On startup, the book is hydrated from the DB (`load_active_orders_with_notes`)
/// so orders persisted by ingest but never delivered through the channel
/// (e.g. crash between DB write and channel send) are still considered.
/// DB is the source of truth; in-memory state is rebuildable from it.
///
/// Each tick: drain consumed (the one removal path) → drain new orders →
/// reactivate timed-out parked notes → run internal matching (→ executor) →
/// run the external pass (→ router), if enabled. The external pass and
/// reactivation run on EVERY tick (no early `continue`), since the
/// zero-internal-match tick is exactly when external routing matters most.
/// It also stamps each order's arrival and publishes a top-of-book snapshot
/// every tick for the swap-eta API.
#[allow(clippy::too_many_arguments)]
pub async fn run_matcher(
    pool: DbPool,
    mut order_rx: mpsc::Receiver<IngestOrder>,
    mut consumed_rx: mpsc::Receiver<NoteId>,
    price_rx: watch::Receiver<PriceSnapshot>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    triangular_enabled: bool,
    // Publishes the top-of-book snapshot each tick for the swap-eta API. Read
    // lock-free off-thread, so wallet ETA traffic never touches the live book.
    swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    mut router: Option<RouterHooks>,
    cancel: CancellationToken,
) {
    let feed = WatchPriceFeed::from_watch(&price_rx);
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book).with_triangular_enabled(triangular_enabled);

    // Map from OrderId → raw note data for building FilledNotes / handovers.
    let mut raw_notes: HashMap<OrderId, Vec<u8>> = HashMap::new();
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
                    engine.book.add_user_order(
                        order.note_id,
                        order.offered_token,
                        order.requested_token,
                        order.offered_amount,
                        order.requested_amount,
                    );
                    arrivals.insert(order.note_id, now_unix());
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

        // Sync the book with inbound events: drop consumed notes, add new orders.
        while let Ok(note_id) = consumed_rx.try_recv() {
            engine.book.remove_order(note_id);
            raw_notes.remove(&note_id);
            arrivals.remove(&note_id);
        }
        while let Ok(order) = order_rx.try_recv() {
            engine.book.add_user_order(
                order.note_id,
                order.offered_token,
                order.requested_token,
                order.offered_amount,
                order.requested_amount,
            );
            arrivals.entry(order.note_id).or_insert_with(now_unix);
            raw_notes.insert(order.note_id, order.raw_note_data);
        }

        // 1. Reactivate parked notes whose DEX no-showed past the in-flight TTL.
        if let Some(r) = router.as_ref() {
            for (id, dex) in engine.book.reactivate_parked_older_than(r.inflight_ttl_ms, now) {
                tracing::debug!(note = %id, dex, "parked note timed out; reactivated");
            }
        }

        // 2. Internal matching (→ executor).
        if internal_match(&mut engine, &mut raw_notes, &mut arrivals, &price_rx, &exec_tx).await {
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

/// Internal matching pass — unchanged from the non-routing path: refresh the price
/// feed, run the engine, and settle any filled notes to the executor. A no-op when
/// the book is empty or nothing crosses. Returns `true` if the executor channel has
/// closed (the matcher should stop).
async fn internal_match(
    engine: &mut MatchingEngine<WatchPriceFeed>,
    raw_notes: &mut HashMap<OrderId, Vec<u8>>,
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
        let Some(raw_note_data) = raw_notes.get(&order_id).cloned() else { continue };
        let requested_filled = engine
            .book
            .orders
            .get(&order_id)
            .map(|o| o.requested_filled())
            .unwrap_or(0);
        filled_notes.push(FilledNote {
            note_id: order_id,
            requested_filled,
            raw_note_data,
            arrival_unix: arrivals.get(&order_id).copied().unwrap_or_else(now_unix),
        });
    }
    if exec_tx.send(ExecutionBatch { filled_notes }).await.is_err() {
        tracing::warn!("executor channel closed, matcher shutting down");
        return true;
    }
    for &order_id in &batch.filled_orders {
        engine.book.remove_order(order_id);
        raw_notes.remove(&order_id);
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
    use crate::matching::types::DexId;
    use crate::matching::order_book::OrderBook;
    use crate::price::WatchPriceFeed;
    use crate::router::Quote;
    use miden_protocol::note::NoteId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    fn imiden() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
    }
    fn iusdt() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
    }
    fn nid(seed: u64) -> NoteId {
        NoteId::try_from_hex(&format!("0x{seed:064x}")).unwrap()
    }
    /// A quote for the IMIDEN/IUSDT pair at base-unit rate 1/50 (requested-base per
    /// offered-base). Any note whose rate is at or below this is willing.
    fn quote_at_mid(dex: DexId, supply: Amount, expires_at: u64) -> Quote {
        // rate supply/demand = 1/50; `supply` is the capacity.
        Quote { dex, pair: (imiden(), iusdt()), supply, demand: supply.saturating_mul(50), expires_at }
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
        let items = route_external(&mut book, &raw, &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]), 1_000);

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
        let q = Quote { dex: 7, pair: (imiden(), iusdt()), supply: 10_000_000, demand: 1_000_000_000, expires_at: u64::MAX };
        let items = route_external(&mut book, &raw, &snap(vec![q]), 1_000);
        assert!(items.is_empty());
        assert!(!book.is_parked(id), "an unroutable order stays matchable internally");
        assert_eq!(book.active_order_count(), 1);
    }

    #[test]
    fn partially_filled_note_not_routed() {
        let id = nid(8);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        book.orders.get_mut(&id).unwrap().fill(1_000_000); // partial internal fill
        let items = route_external(&mut book, &raw, &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]), 1_000);
        assert!(items.is_empty(), "v1 routes whole notes only");
        assert!(!book.is_parked(id));
    }

    #[test]
    fn missing_raw_bytes_skips_without_parking() {
        let id = nid(9);
        let (mut book, _raw) = book_with_order(id, 110_000_000, 2_000_000);
        // Candidate in the book, but its bytes are absent → skipped, not parked.
        let items = route_external(&mut book, &HashMap::new(), &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]), 1_000);
        assert!(items.is_empty());
        assert!(!book.is_parked(id), "no raw bytes → not parked");
    }

    #[test]
    fn stale_quote_no_handover() {
        let id = nid(3);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        // expires_at == now ⇒ stale (strict >).
        let items = route_external(&mut book, &raw, &snap(vec![quote_at_mid(7, 10_000_000, 1_000)]), 1_000);
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

        let (order_tx, order_rx) = mpsc::channel(16);
        let (_consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(16);
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
            .send(Arc::new(snap(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)])))
            .unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            route_tx,
            inflight_ttl_ms: 60_000,
        };
        let id = nid(777);
        order_tx
            .send(IngestOrder {
                note_id: id,
                offered_token: imiden(),
                requested_token: iusdt(),
                offered_amount: 110_000_000, // 1.1 IMIDEN = $2.20
                requested_amount: 2_000_000, // 2 IUSDT = $2.00 → +10% generous
                raw_note_data: vec![1, 2, 3, 4],
            })
            .await
            .unwrap();

        // Matcher is `spawn_local` in production (mirror that here).
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    order_rx,
                    consumed_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
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

        let (order_tx, order_rx) = mpsc::channel(16);
        let (_consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(16);
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
        order_tx
            .send(IngestOrder {
                note_id: id,
                offered_token: imiden(),
                requested_token: iusdt(),
                offered_amount: 110_000_000,
                requested_amount: 2_000_000,
                raw_note_data: note_bytes.clone(),
            })
            .await
            .unwrap();

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
                    order_rx,
                    consumed_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
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
                let Handover { note: got, fill_amount } = handover;
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

        let (_qtx, quotes_rx) =
            watch::channel(Arc::new(snap(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)])));

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
        assert!(!engine.book.is_parked(id), "dropped handover rolled back — note not left parked");
        assert_eq!(engine.book.active_order_count(), 1, "note is eligible again");

        // (2) Drain the channel, retry → the note re-routes to the SAME DEX and is
        //     delivered. The drop cost it nothing.
        let _ = route_rx.try_recv(); // free the slot
        external_pass(&mut engine, &raw, &hooks, 2_000);
        assert!(engine.book.is_parked(id), "after the drop the note re-routes to the same DEX");
        let delivered = route_rx.try_recv().expect("handover delivered on retry");
        assert_eq!(delivered.items.len(), 1);
        assert_eq!(delivered.items[0].note_id, id);
    }

    #[test]
    fn closed_route_channel_reported_and_unparked() {
        let id = nid(100);
        let (_qtx, quotes_rx) = watch::channel(Arc::new(snap(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)])));
        let (route_tx, route_rx) = mpsc::channel::<RouteBatch>(1);
        drop(route_rx); // receiver gone → channel closed
        let hooks = RouterHooks { quotes_rx, route_tx, inflight_ttl_ms: 60_000 };
        let (book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let mut engine = MatchingEngine::new(book).with_triangular_enabled(false);
        assert!(external_pass(&mut engine, &raw, &hooks, 1_000), "closed channel is reported");
        assert!(!engine.book.is_parked(id), "note unparked on closed channel");
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
        let (order_tx, order_rx) = mpsc::channel(16);
        let (_consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(16);
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
        order_tx
            .send(IngestOrder {
                note_id: nid(1),
                offered_token: imiden(),
                requested_token: iusdt(),
                offered_amount: 100_000_000,
                requested_amount: 200_000_000,
                raw_note_data: vec![1],
            })
            .await
            .unwrap();
        order_tx
            .send(IngestOrder {
                note_id: nid(2),
                offered_token: iusdt(),
                requested_token: imiden(),
                offered_amount: 210_000_000,
                requested_amount: 100_000_000,
                raw_note_data: vec![2],
            })
            .await
            .unwrap();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let task = tokio::task::spawn_local(run_matcher(
                    pool,
                    order_rx,
                    consumed_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    None, // router disabled
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
        let (order_tx, order_rx) = mpsc::channel(16);
        let (consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(16);
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
            .send(Arc::new(snap(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)])))
            .unwrap();

        let id = nid(50);
        order_tx
            .send(IngestOrder {
                note_id: id,
                offered_token: imiden(),
                requested_token: iusdt(),
                offered_amount: 110_000_000,
                requested_amount: 2_000_000,
                raw_note_data: vec![9],
            })
            .await
            .unwrap();

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
                    order_rx,
                    consumed_rx,
                    price_rx,
                    exec_tx,
                    Duration::from_millis(10),
                    false,
                    watch::channel(Arc::new(SwapBookSnapshot::new())).0,
                    Some(hooks),
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
                consumed_tx.send(id).await.unwrap();
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
        let items2 = route_external(&mut empty, &HashMap::new(), &snap(vec![quote_at_mid(7, 10_000_000, u64::MAX)]), 1_000);
        assert!(items2.is_empty());
    }
}

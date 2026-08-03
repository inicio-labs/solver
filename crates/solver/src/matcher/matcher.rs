use miden_protocol::note::NoteId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::db::{self, DbPool};
use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::matching::types::DexId;
use crate::price::{PriceSnapshot, WatchPriceFeed};
use crate::router::{select_notes, Handover, HandoverPick, NoteView, Pair, QuotesSnapshot};
use crate::types::*;

/// Hooks that enable the external-liquidity pass inside the matcher tick. When
/// `None` (router disabled), the matcher behaves exactly as before. All fields
/// are `Send` channels/scalars — the router itself runs on its own OS thread.
pub struct RouterHooks {
    /// Latest standing quotes from connected DEXes (filtered by freshness here).
    pub quotes_rx: watch::Receiver<Arc<QuotesSnapshot>>,
    /// Selected notes pushed to the router for delivery (`try_send`, never blocks).
    pub handover_tx: mpsc::Sender<Handover>,
    /// How long a handed-over note waits for on-chain consume before reactivating.
    pub inflight_ttl_ms: u64,
    pub min_edge_bps: u64,
    pub max_dev_bps: u64,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
#[allow(clippy::too_many_arguments)]
pub async fn run_matcher(
    pool: DbPool,
    mut order_rx: mpsc::Receiver<IngestOrder>,
    mut consumed_rx: mpsc::Receiver<NoteId>,
    price_rx: watch::Receiver<PriceSnapshot>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    match_interval: Duration,
    triangular_enabled: bool,
    router: Option<RouterHooks>,
    cancel: CancellationToken,
) {
    let feed = WatchPriceFeed::from_watch(&price_rx);
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book).with_triangular_enabled(triangular_enabled);

    // Map from OrderId → raw note data for building FilledNotes / handovers.
    let mut raw_notes: HashMap<OrderId, Vec<u8>> = HashMap::new();

    // External-routing bookkeeping (only used when `router` is Some).
    // Per-(dex,pair) reserved quote quantity, and per-note reservation record so
    // we can release the exact amount on consume/reactivation.
    let mut reserved: HashMap<(DexId, Pair), Amount> = HashMap::new();
    let mut reservations: HashMap<OrderId, (DexId, Pair, Amount)> = HashMap::new();
    // Notes that no-showed at a DEX → don't immediately re-offer to that DEX.
    let mut no_reoffer: HashMap<OrderId, HashSet<DexId>> = HashMap::new();
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

        // 0. Drain consumed-note notifications FIRST (the ONE removal path).
        //    Release any reservation, drop no-reoffer state, remove from book.
        while let Ok(note_id) = consumed_rx.try_recv() {
            release_reservation(&mut reserved, &mut reservations, note_id);
            no_reoffer.remove(&note_id);
            engine.book.remove_order(note_id);
            raw_notes.remove(&note_id);
        }

        // 1. Drain all pending new orders from the channel.
        while let Ok(order) = order_rx.try_recv() {
            engine.book.add_user_order(
                order.note_id,
                order.offered_token,
                order.requested_token,
                order.offered_amount,
                order.requested_amount,
            );
            raw_notes.insert(order.note_id, order.raw_note_data);
        }

        // 2. Reactivate parked notes whose DEX no-showed past the TTL
        //    (unconditional, before any early-out).
        if let Some(r) = router.as_ref() {
            for (id, dex) in engine.book.reactivate_parked_older_than(r.inflight_ttl_ms, now) {
                release_reservation(&mut reserved, &mut reservations, id);
                no_reoffer.entry(id).or_default().insert(dex);
                tracing::debug!(note = %id, dex, "parked note timed out; reactivated");
            }
        }

        // 3. Refresh price feed.
        engine.book.feed = WatchPriceFeed::from_watch(&price_rx);

        // 4. Internal matching (only when matchable liquidity exists).
        if engine.book.active_order_count() > 0 {
            let batch = engine.run();
            if !batch.filled_orders.is_empty() {
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
                    filled_notes.push(FilledNote { note_id: order_id, requested_filled, raw_note_data });
                }
                if exec_tx.send(ExecutionBatch { filled_notes }).await.is_err() {
                    tracing::warn!("executor channel closed, matcher shutting down");
                    return;
                }
                for &order_id in &batch.filled_orders {
                    engine.book.remove_order(order_id);
                    raw_notes.remove(&order_id);
                }
                engine.book.protocol_balances.clear();
            }
        }

        // 5. External pass: hand residual notes to DEXes whose quotes they clear.
        if let Some(r) = router.as_ref() {
            external_pass(
                &mut engine,
                &raw_notes,
                &pool,
                &price_rx,
                &mut reserved,
                &mut reservations,
                &no_reoffer,
                r,
                now,
            );
        }
    }
}

/// Release a note's reservation (if any) back to its `(dex,pair)` budget.
fn release_reservation(
    reserved: &mut HashMap<(DexId, Pair), Amount>,
    reservations: &mut HashMap<OrderId, (DexId, Pair, Amount)>,
    note_id: OrderId,
) {
    if let Some((dex, pair, amt)) = reservations.remove(&note_id) {
        if let Some(c) = reserved.get_mut(&(dex, pair)) {
            *c = c.saturating_sub(amt);
        }
    }
}

/// Select residual notes against the cached quotes, park each pick (remove from
/// the matching index), reserve its quantity, and `try_send` a handover. Pure
/// reads except the book parks and the reservation ledger. Never `.await`s.
#[allow(clippy::too_many_arguments)]
fn external_pass(
    engine: &mut MatchingEngine<WatchPriceFeed>,
    raw_notes: &HashMap<OrderId, Vec<u8>>,
    pool: &DbPool,
    price_rx: &watch::Receiver<PriceSnapshot>,
    reserved: &mut HashMap<(DexId, Pair), Amount>,
    reservations: &mut HashMap<OrderId, (DexId, Pair, Amount)>,
    no_reoffer: &HashMap<OrderId, HashSet<DexId>>,
    r: &RouterHooks,
    now: u64,
) {
    // Decimals (immutable per token; tiny table — refresh each tick).
    let decimals = db::load_token_decimals(pool).unwrap_or_default();
    let price_snap: PriceSnapshot = price_rx.borrow().clone();
    let price_fn = |t: TokenId| price_snap.get(&t).copied();
    let quotes = r.quotes_rx.borrow().clone(); // Arc<Vec<Quote>>

    let items = route_external(
        &mut engine.book,
        raw_notes,
        &decimals,
        &price_fn,
        &quotes[..],
        reserved,
        reservations,
        no_reoffer,
        r.min_edge_bps,
        r.max_dev_bps,
        now,
    );

    if !items.is_empty() {
        let n = items.len();
        match r.handover_tx.try_send(Handover { items }) {
            Ok(()) => tracing::info!(count = n, "routed unmatched notes to DEXes"),
            Err(e) => {
                // The DEX never received these, so roll the batch back: unpark each
                // note and release its reservation. A dropped handover is then a
                // no-op (the notes stay eligible next tick, to the same DEX) rather
                // than a no-re-offer penalty earned on TTL reactivation for a note
                // the DEX never saw.
                let reason = e.to_string();
                let dropped = e.into_inner();
                for item in &dropped.items {
                    engine.book.unpark(item.note_id);
                    release_reservation(reserved, reservations, item.note_id);
                }
                tracing::warn!(
                    count = n,
                    reason = %reason,
                    "handover not sent; rolled back park so notes stay eligible"
                );
            }
        }
    }
}

/// Pure core of the external pass (no DB / channel I/O): select residual notes
/// against the cached quotes, **park** each pick (removing it from the matching
/// index), reserve its quantity, and return the handover items. `book` is
/// mutated only via `park`. Directly unit-tested — this is the proof that an
/// unmatched order + a clearing DEX quote ⇒ the note is parked and handed over.
#[allow(clippy::too_many_arguments)]
fn route_external<F: crate::matching::price_feed::PriceFeed>(
    book: &mut OrderBook<F>,
    raw_notes: &HashMap<OrderId, Vec<u8>>,
    decimals: &crate::router::Decimals,
    price_cents: &impl Fn(TokenId) -> Option<u64>,
    quotes: &[crate::router::Quote],
    reserved: &mut HashMap<(DexId, Pair), Amount>,
    reservations: &mut HashMap<OrderId, (DexId, Pair, Amount)>,
    no_reoffer: &HashMap<OrderId, HashSet<DexId>>,
    min_edge_bps: u64,
    max_dev_bps: u64,
    now: u64,
) -> Vec<HandoverPick> {
    // Candidates: active, non-parked residual notes.
    let candidates: Vec<NoteView> = book
        .orders
        .values()
        .filter(|o| o.is_active() && !book.is_parked(o.id))
        .map(|o| NoteView {
            id: o.id,
            offered_token: o.offered_token,
            offered: o.offered,
            requested_token: o.requested_token,
            requested: o.requested,
        })
        .collect();
    if candidates.is_empty() || quotes.is_empty() {
        return Vec::new();
    }

    let blocked: HashSet<(OrderId, DexId)> = no_reoffer
        .iter()
        .flat_map(|(id, dexes)| dexes.iter().map(move |d| (*id, *d)))
        .collect();

    let picks = select_notes(
        &candidates,
        quotes,
        now,
        price_cents,
        decimals,
        reserved,
        &blocked,
        min_edge_bps,
        max_dev_bps,
    );

    let mut items = Vec::with_capacity(picks.len());
    for pick in &picks {
        book.park(pick.note_id, pick.dex, now);
        *reserved.entry((pick.dex, pick.pair)).or_default() += pick.fill;
        reservations.insert(pick.note_id, (pick.dex, pick.pair, pick.fill));
        if let Some(bytes) = raw_notes.get(&pick.note_id) {
            items.push(HandoverPick {
                dex: pick.dex,
                note_id: pick.note_id,
                fill: pick.fill,
                note_bytes: bytes.clone(),
            });
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::order_book::OrderBook;
    use crate::price::WatchPriceFeed;
    use crate::router::{Decimals, Quote};
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
    // Asymmetric on purpose (exercises the 10^decimals normalisation):
    // IMIDEN 8-dec, IUSDT 6-dec. So 1 IMIDEN = 1e8, 1 IUSDT = 1e6.
    fn decimals_pair() -> Decimals {
        let mut d = Decimals::new();
        d.insert(imiden(), 8);
        d.insert(iusdt(), 6);
        d
    }
    // IMIDEN = $2.00 (200c), IUSDT = $1.00 (100c). Mid (IUSDT per IMIDEN) = 2.
    fn oracle(t: TokenId) -> Option<u64> {
        if t == imiden() {
            Some(200)
        } else if t == iusdt() {
            Some(100)
        } else {
            None
        }
    }
    fn quote_at_mid(dex: DexId, qty: Amount, expires_at: u64) -> Quote {
        Quote {
            dex,
            pair: (imiden(), iusdt()),
            // Base-unit rate (note orientation) at the oracle mid: IMIDEN $2 / 8dec
            // vs IUSDT $1 / 6dec ⇒ 1e8 imiden-base ↔ 2e6 iusdt-base = 1/50.
            price_num: 1,
            price_den: 50,
            quantity: qty,
            expires_at,
        }
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
        // Offer 1.1 IMIDEN ($2.20) for 2 IUSDT ($2.00): +10% generous → exports at 50bps.
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let pair = (imiden(), iusdt());
        let quotes = vec![quote_at_mid(7, 10_000_000, u64::MAX)];
        let mut reserved = HashMap::new();
        let mut reservations = HashMap::new();

        assert_eq!(book.active_order_count(), 1);
        let items = route_external(
            &mut book,
            &raw,
            &decimals_pair(),
            &oracle,
            &quotes,
            &mut reserved,
            &mut reservations,
            &HashMap::new(),
            100,
            200,
            1_000,
        );

        // Handover emitted: the note → DEX 7, with its bytes.
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].dex, 7);
        assert_eq!(items[0].note_id, id);
        assert_eq!(items[0].fill, 2_000_000);
        assert_eq!(items[0].note_bytes, vec![0xAA, 0xBB, 0xCC]);
        // Orderbook change: the note is parked, invisible to internal matching.
        assert!(book.is_parked(id));
        assert_eq!(book.active_order_count(), 0);
        assert!(book.best_order(imiden(), iusdt()).is_none());
        // Reservation recorded against the quote budget.
        assert_eq!(reserved.get(&(7u64, pair)).copied(), Some(2_000_000));
        assert!(reservations.contains_key(&id));
    }

    #[test]
    fn stingy_order_retained_not_exported() {
        let id = nid(2);
        // Offer 1.005 IMIDEN ($2.01) for 2 IUSDT ($2.00): +0.5% < 50bps → retained.
        let (mut book, raw) = book_with_order(id, 100_500_000, 2_000_000);
        let items = route_external(
            &mut book, &raw, &decimals_pair(), &oracle, &[quote_at_mid(7, 10_000_000, u64::MAX)],
            &mut HashMap::new(), &mut HashMap::new(), &HashMap::new(), 100, 200, 1_000,
        );
        assert!(items.is_empty());
        assert!(!book.is_parked(id), "retained order stays matchable internally");
        assert_eq!(book.active_order_count(), 1);
    }

    #[test]
    fn stale_quote_no_handover() {
        let id = nid(3);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        // expires_at == now ⇒ stale (strict >).
        let items = route_external(
            &mut book, &raw, &decimals_pair(), &oracle, &[quote_at_mid(7, 10_000_000, 1_000)],
            &mut HashMap::new(), &mut HashMap::new(), &HashMap::new(), 100, 200, 1_000,
        );
        assert!(items.is_empty());
        assert!(!book.is_parked(id));
    }

    #[test]
    fn blocked_note_not_reoffered_to_same_dex_but_ok_to_other() {
        let id = nid(4);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let mut no_reoffer: HashMap<OrderId, HashSet<DexId>> = HashMap::new();
        no_reoffer.entry(id).or_default().insert(7u64);
        // DEX 7 is blocked for this note.
        let items = route_external(
            &mut book, &raw, &decimals_pair(), &oracle, &[quote_at_mid(7, 10_000_000, u64::MAX)],
            &mut HashMap::new(), &mut HashMap::new(), &no_reoffer, 100, 200, 1_000,
        );
        assert!(items.is_empty(), "not re-offered to the DEX it no-showed at");
        assert!(!book.is_parked(id));
        // A different DEX can still take it.
        let items2 = route_external(
            &mut book, &raw, &decimals_pair(), &oracle, &[quote_at_mid(9, 10_000_000, u64::MAX)],
            &mut HashMap::new(), &mut HashMap::new(), &no_reoffer, 100, 200, 1_000,
        );
        assert_eq!(items2.len(), 1);
        assert_eq!(items2[0].dex, 9);
        assert!(book.is_parked(id));
    }

    #[test]
    fn unpriced_token_not_exported() {
        let id = nid(5);
        let (mut book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let no_price = |_t: TokenId| None;
        let items = route_external(
            &mut book, &raw, &decimals_pair(), &no_price, &[quote_at_mid(7, 10_000_000, u64::MAX)],
            &mut HashMap::new(), &mut HashMap::new(), &HashMap::new(), 100, 200, 1_000,
        );
        assert!(items.is_empty(), "data gate (unpriced) excludes the note");
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
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(Vec::new()));
        let (handover_tx, mut handover_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();
        quotes_tx
            .send(Arc::new(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)]))
            .unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            handover_tx,
            inflight_ttl_ms: 60_000,
            min_edge_bps: 100,
            max_dev_bps: 200,
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
                    Some(hooks),
                    cancel.clone(),
                ));

                let handover = tokio::time::timeout(Duration::from_secs(2), handover_rx.recv())
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
        // The router owns quotes_tx (publishes DEX quotes) + handover_rx (delivers
        // handovers); the matcher owns quotes_rx + handover_tx. Real wiring.
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(Vec::new()));
        let (handover_tx, handover_rx) = mpsc::channel(16);
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
            spawn_router_thread(cfg, quotes_tx, handover_rx, cancel.clone()).unwrap();
        ready.await.unwrap().expect("router bound");

        // A REAL serialized note; its id is the OrderId (as ingest computes it).
        let note = Note::mock_noop(Word::from([0x00C0_FFEEu32, 1, 2, 3]));
        let note_bytes = note.to_bytes();
        let id = note.id();

        // An unmatched order: offer 1.1 IMIDEN ($2.20) for 2 IUSDT ($2.00) — +10%
        // generous, so it clears a mid quote at 100 bps edge / 200 bps dev.
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
            handover_tx,
            inflight_ttl_ms: 60_000,
            min_edge_bps: 100,
            max_dev_bps: 200,
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
                    Some(hooks),
                    cancel.clone(),
                ));

                // The DEX connects and posts a filler-centric quote: it GIVES iusdt
                // and WANTS imiden (to fill a note that offers imiden / wants iusdt),
                // priced at the oracle mid (2e6 iusdt : 1e8 imiden).
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
    /// unparked, reservations released), so a note the DEX never received is not
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

        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();

        let (_qtx, quotes_rx) =
            watch::channel(Arc::new(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)]));

        // Capacity-1 handover channel, pre-filled → the first try_send is Full.
        let (handover_tx, mut handover_rx) = mpsc::channel::<Handover>(1);
        handover_tx.try_send(Handover { items: vec![] }).unwrap();

        let hooks = RouterHooks {
            quotes_rx,
            handover_tx,
            inflight_ttl_ms: 60_000,
            min_edge_bps: 100,
            max_dev_bps: 200,
        };

        let id = nid(99);
        let (book, raw) = book_with_order(id, 110_000_000, 2_000_000);
        let mut engine = MatchingEngine::new(book).with_triangular_enabled(false);
        let mut reserved = HashMap::new();
        let mut reservations = HashMap::new();
        let no_reoffer = HashMap::new();

        // (1) Full channel → the handover is dropped and ROLLED BACK: not parked,
        //     back in internal matching, reservation released. No hang, no penalty.
        external_pass(
            &mut engine, &raw, &pool, &price_rx,
            &mut reserved, &mut reservations, &no_reoffer, &hooks, 1_000,
        );
        assert!(!engine.book.is_parked(id), "dropped handover rolled back — note not left parked");
        assert_eq!(engine.book.active_order_count(), 1, "note is eligible again");
        assert!(!reservations.contains_key(&id), "reservation released on rollback");

        // (2) Drain the channel, retry → the note re-routes to the SAME DEX and is
        //     delivered. The drop cost it nothing.
        let _ = handover_rx.try_recv(); // free the slot
        external_pass(
            &mut engine, &raw, &pool, &price_rx,
            &mut reserved, &mut reservations, &no_reoffer, &hooks, 2_000,
        );
        assert!(engine.book.is_parked(id), "after the drop the note re-routes to the same DEX");
        assert!(reservations.contains_key(&id), "reservation recorded on the successful retry");
        let delivered = handover_rx.try_recv().expect("handover delivered on retry");
        assert_eq!(delivered.items.len(), 1);
        assert_eq!(delivered.items[0].note_id, id);
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
    /// in-flight TTL (covers the reactivation + reservation-release path), and is
    /// not re-offered to the same DEX (so no second handover).
    #[tokio::test]
    async fn run_matcher_reactivates_and_consumes_parked_note() {
        let (_tmp, pool) = harness_db();
        let (order_tx, order_rx) = mpsc::channel(16);
        let (consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(16);
        let (price_tx, price_rx) = watch::channel(PriceSnapshot::new());
        let (exec_tx, _exec_rx) = mpsc::channel(16);
        let (quotes_tx, quotes_rx) = watch::channel(Arc::new(Vec::new()));
        let (handover_tx, mut handover_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();

        let mut prices = PriceSnapshot::new();
        prices.insert(imiden(), 200);
        prices.insert(iusdt(), 100);
        price_tx.send(prices).unwrap();
        quotes_tx
            .send(Arc::new(vec![quote_at_mid(1, 1_000_000_000, u64::MAX)]))
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
            handover_tx,
            inflight_ttl_ms: 1,
            min_edge_bps: 100,
            max_dev_bps: 200,
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
                    Some(hooks),
                    cancel.clone(),
                ));
                // First handover (note parked).
                let h = tokio::time::timeout(Duration::from_secs(2), handover_rx.recv())
                    .await
                    .expect("first handover")
                    .unwrap();
                assert_eq!(h.items[0].note_id, id);
                // After the TTL the note reactivates but is blocked for DEX 1, so we
                // should NOT get a second handover for a while.
                let second = tokio::time::timeout(Duration::from_millis(200), handover_rx.recv()).await;
                assert!(second.is_err(), "reactivated note not re-offered to same DEX");
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
        let items = route_external(
            &mut book, &raw, &decimals_pair(), &oracle, &[],
            &mut HashMap::new(), &mut HashMap::new(), &HashMap::new(), 100, 200, 1_000,
        );
        assert!(items.is_empty());
        assert!(!book.is_parked(id));
        // Empty book.
        let mut empty = OrderBook::new(WatchPriceFeed::new());
        let items2 = route_external(
            &mut empty, &HashMap::new(), &decimals_pair(), &oracle, &[quote_at_mid(7, 10_000_000, u64::MAX)],
            &mut HashMap::new(), &mut HashMap::new(), &HashMap::new(), 100, 200, 1_000,
        );
        assert!(items2.is_empty());
    }
}

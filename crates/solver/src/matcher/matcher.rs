use miden_protocol::note::NoteId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::db::{self, DbPool};
use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::matching::types::SwapBookSnapshot;
use crate::price::{PriceSnapshot, WatchPriceFeed};
use crate::types::*;

/// Unix secs, saturating. Used to stamp order arrival for the swap-eta window.
fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// The matcher owns a persistent OrderBook and runs matching on a timer.
///
/// On startup, the book is hydrated from the DB (`load_active_orders_with_notes`)
/// so orders persisted by ingest but never delivered through the channel
/// (e.g. crash between DB write and channel send) are still considered.
/// DB is the source of truth; in-memory state is rebuildable from it.
///
/// Flow each tick:
/// 1. Drain new orders from the order channel → add to OrderBook
/// 2. Snapshot latest prices from the watch channel → update feed
/// 3. Run matching engine (direct + triangular)
/// 4. For each filled order: build FilledNote with fill amount + raw note data
/// 5. Send ExecutionBatch downstream
/// 6. Remove matched orders from OrderBook
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
    cancel: CancellationToken,
) {
    let feed = WatchPriceFeed::from_watch(&price_rx);
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book).with_triangular_enabled(triangular_enabled);

    // Map from OrderId → raw note data for building FilledNotes
    let mut raw_notes: HashMap<OrderId, Vec<u8>> = HashMap::new();
    // Arrival time (unix secs) per order — stamped when the matcher first sees
    // the order. Carried onto FilledNote so the executor can record the
    // settlement duration for the swap-eta window. In-memory only (no DB).
    let mut arrivals: HashMap<OrderId, u64> = HashMap::new();

    // Hydrate the in-memory book from DB. Closes the "ingest wrote DB but
    // channel send failed" gap — those orders are loaded here on next start.
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

        // 0. Drain consumed-note notifications FIRST so zombies aren't
        //    considered for fresh matches this tick.
        while let Ok(note_id) = consumed_rx.try_recv() {
            // OrderId is NoteId in our model — no conversion needed.
            engine.book.remove_order(note_id);
            raw_notes.remove(&note_id);
            arrivals.remove(&note_id);
        }

        // 1. Drain all pending orders from the channel
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

        // Publish the top-of-book snapshot for the swap-eta API — every tick,
        // including empty ones, so the snapshot never goes stale. Latest-wins,
        // non-blocking; reflects post-drain state (this tick's matches show up
        // next tick, a sub-second lag that's immaterial for an ETA).
        swap_snapshot_tx.send_replace(Arc::new(engine.book.snapshot_best_levels()));

        if engine.book.orders.is_empty() {
            continue;
        }

        // 2. Update price feed with latest snapshot
        engine.book.feed = WatchPriceFeed::from_watch(&price_rx);

        // 3. Run matching
        let batch = engine.run();

        if batch.filled_orders.is_empty() {
            continue;
        }
        tracing::info!(orders = batch.filled_orders.len(), "matcher produced batch");

        // 4. Build FilledNotes
        let mut filled_notes = Vec::new();

        for &order_id in &batch.filled_orders {
            let raw_note_data = match raw_notes.get(&order_id) {
                Some(data) => data.clone(),
                None => continue,
            };

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

        // send only errors if executor has crashed (receiver dropped)
        if exec_tx.send(ExecutionBatch { filled_notes }).await.is_err() {
            tracing::warn!("executor channel closed, matcher shutting down");
            return;
        }

        // 6. Remove all matched orders from the book (indices + HashMap)
        for &order_id in &batch.filled_orders {
            engine.book.remove_order(order_id);
            raw_notes.remove(&order_id);
            arrivals.remove(&order_id);
        }

        engine.book.protocol_balances.clear();
    }
}

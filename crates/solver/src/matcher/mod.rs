use miden_protocol::crypto::utils::Serializable;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use crate::db::{self, DbPool};
use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::price::{PriceSnapshot, WatchPriceFeed};
use crate::types::*;

/// The matcher owns a persistent OrderBook and runs matching on a timer.
///
/// Flow each tick:
/// 1. Drain new orders from the order channel → add to OrderBook
/// 2. Snapshot latest prices from the watch channel → update feed
/// 3. Run matching engine (direct + triangular)
/// 4. For each filled order: build FilledNote with fill amount + raw note data
/// 5. Send ExecutionBatch downstream
/// 6. Remove fully filled orders from OrderBook, update DB status
pub async fn run_matcher(
    mut order_rx: mpsc::Receiver<IngestOrder>,
    price_rx: watch::Receiver<PriceSnapshot>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    pool: DbPool,
    match_interval: Duration,
) {
    let feed = WatchPriceFeed::from_watch(&price_rx);
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book);

    // Map from OrderId → raw note data for building FilledNotes
    let mut raw_notes: HashMap<OrderId, Vec<u8>> = HashMap::new();

    let mut interval = tokio::time::interval(match_interval);

    loop {
        interval.tick().await;

        // 1. Drain all pending orders from the channel
        while let Ok(order) = order_rx.try_recv() {
            let added = engine.book.add_user_order(
                order.note_id,
                order.offered_token,
                order.requested_token,
                order.offered_amount,
                order.requested_amount,
            );

            if added {
                raw_notes.insert(order.note_id, order.raw_note_data);
            }
        }

        if engine.book.active_order_count() == 0 {
            continue;
        }

        // 2. Update price feed with latest snapshot
        engine.book.feed = WatchPriceFeed::from_watch(&price_rx);

        // 3. Run matching
        let batch = engine.run();

        if batch.filled_orders.is_empty() {
            continue;
        }

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
            });
        }

        // 5. Send ExecutionBatch downstream
        if let Err(e) = exec_tx.send(ExecutionBatch { filled_notes }).await {
            eprintln!("[matcher] execution channel send failed: {e}");
            continue;
        }

        // 6. Update DB status and clean up filled orders
        let filled_ids: Vec<OrderId> = batch.filled_orders.iter().copied().collect();

        if let Ok(mut conn) = pool.get() {
            for &order_id in &filled_ids {
                let id_bytes = order_id.to_bytes().to_vec();
                let _ = db::update_order_status(&mut conn, &id_bytes, OrderStatus::InFlight);
            }
        }

        for &order_id in &filled_ids {
            if engine
                .book
                .orders
                .get(&order_id)
                .map_or(true, |o| o.is_completely_filled())
            {
                engine.book.orders.remove(&order_id);
                raw_notes.remove(&order_id);
            }
        }

        engine.book.protocol_balances.clear();
    }
}

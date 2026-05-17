use anyhow::Result;
use async_trait::async_trait;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::db::models::{NoteRow, OrderRow};
use crate::db::{self, DbPool};
use crate::types::Order as PipelineOrder;
use crate::types::{IngestOrder, OrderStatus, TokenId};

/// Result of a sync_state call — newly received notes plus IDs of notes
/// whose nullifier was just observed on-chain. The matcher uses the
/// latter to surgically drop zombie orders from its in-memory book.
pub struct SyncResult {
    pub block_num: u64,
    pub new_notes: Vec<Note>,
    pub consumed_notes: Vec<NoteId>,
}

/// Trait abstracting the Miden Node RPC client.
///
/// No `Send` bound: the production adapter wraps `Client<FilesystemKeyStore>`
/// which has `Arc<dyn Trait>` fields without `Send + Sync` bounds upstream,
/// making the whole `Client` `!Send`. All tasks that own a `MidenClient`
/// run on a single-threaded `LocalSet`, so cross-thread migration is
/// forbidden by construction.
#[async_trait(?Send)]
pub trait MidenClient {
    /// Register note tags for a trading pair (both directions).
    /// Must be called before sync_state to receive notes for this pair.
    async fn subscribe_pair(&mut self, offered: TokenId, requested: TokenId) -> Result<()>;

    /// Sync client state with the Miden Node.
    /// Returns the new block number, newly received notes, and IDs of notes
    /// whose nullifiers just appeared on-chain.
    async fn sync_state(&mut self) -> Result<SyncResult>;

    /// Given a slice of notes the solver currently believes are matchable,
    /// return the subset whose nullifiers are already on-chain. Used by the
    /// executor after a non-RPC submit error to identify which input notes
    /// are zombies vs. which are still legitimately active.
    async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>>;
}

/// Run the note ingestion loop.
///
/// Each tick: sync → fetch new notes by ID → filter PSWAP → atomic DB insert → send to channel.
/// On a successful tick the `last_sync_unix_seconds` atomic is bumped so the
/// observability `/readyz` endpoint can detect a stalled ingest.
pub async fn run_ingest(
    client: Arc<Mutex<dyn MidenClient>>,
    pool: DbPool,
    order_tx: mpsc::Sender<IngestOrder>,
    consumed_tx: mpsc::Sender<NoteId>,
    interval: Duration,
    cancel: CancellationToken,
    last_sync_unix_seconds: Arc<AtomicI64>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            res = ingest_once(&client, &pool, &order_tx, &consumed_tx) => {
                match res {
                    Ok(()) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        last_sync_unix_seconds.store(now, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "ingest iteration failed");
                    }
                }
            }
        }
        // Sleep at end so the first tick fires immediately, not after `interval`.
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    tracing::info!("ingest cancelled, shutting down");
}

/// Single iteration of the ingest loop.
#[tracing::instrument(skip_all)]
async fn ingest_once(
    client: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    order_tx: &mpsc::Sender<IngestOrder>,
    consumed_tx: &mpsc::Sender<NoteId>,
) -> Result<()> {
    let SyncResult { block_num, new_notes, consumed_notes } = {
        let mut c = client.lock().await;
        c.sync_state().await?
    };

    // Handle consumed notes BEFORE the new-notes path. DB write happens
    // first so a crash between write and channel send leaves the DB
    // authoritative; matcher's next-boot hydration sees terminal state.
    if !consumed_notes.is_empty() {
        let consumed_bytes: Vec<Vec<u8>> = consumed_notes
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect();
        {
            let mut conn = pool.write_conn()?;
            let updated =
                db::mark_orders_onchain_nullified(&mut conn, &consumed_bytes)?;
            if updated > 0 {
                tracing::warn!(
                    count = updated,
                    "ingest marked orders OnchainNullified after sync"
                );
            }
        }
        for note_id in consumed_notes {
            // Best-effort send. Matcher channel-closed means the matcher
            // shut down; we keep ingesting (other tasks may still be live)
            // but the in-memory book stops being maintained.
            let _ = consumed_tx.send(note_id).await;
        }
    }

    // 3. Filter PSWAP notes and parse into DB records + channel messages
    let mut db_notes = Vec::new();
    let mut db_orders = Vec::new();
    let mut ingest_orders = Vec::new();

    for note in &new_notes {
        if note.recipient().script().root() != PswapNote::script_root() {
            continue;
        }

        let order = match PipelineOrder::from_note(note) {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(note_id = %note.id(), error = %e, "skipping unparseable PSWAP note");
                continue;
            }
        };


        let note_id_bytes = note.id().to_bytes().to_vec();

        let mut raw_data = Vec::new();
        note.write_into(&mut raw_data);

        db_notes.push(NoteRow {
            note_id: note_id_bytes.clone(),
            account_id: order.creator_id.to_bytes().to_vec(),
            raw_data: raw_data.clone(),
        });

        db_orders.push(OrderRow {
            note_id: note_id_bytes,
            account_id: order.creator_id.to_bytes().to_vec(),
            requested_asset: order.requested_faucet_id.to_bytes().to_vec(),
            requested_amount: order.requested_amount as i64,
            offered_asset: order.offered_faucet_id.to_bytes().to_vec(),
            offered_amount: order.offered_amount as i64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            status: OrderStatus::Active.as_str().to_string(),
        });

        ingest_orders.push(IngestOrder {
            note_id: note.id(),
            offered_token: order.offered_faucet_id,
            requested_token: order.requested_faucet_id,
            offered_amount: order.offered_amount,
            requested_amount: order.requested_amount,
            raw_note_data: raw_data,
        });
    }

    if db_notes.is_empty() {
        return Ok(());
    }

    // 4. Atomic DB insert (notes + orders + block number). The returned set
    //    is the orders that were *actually* inserted this call — i.e. seen
    //    for the first time. Duplicates (already-known note_ids) are excluded
    //    by the `orders` primary key. This is the durable, bounded dedup that
    //    replaces the old in-memory `seen_notes` HashSet: a note enters the
    //    matcher channel exactly once, at first commit, even across restarts.
    let inserted = {
        let mut conn = pool.write_conn()?;
        db::insert_notes_batch(&mut conn, &db_notes, &db_orders, block_num)?
    };

    // 5. Forward only first-seen orders to the matcher. Backpressure via the
    //    bounded channel; an error means the matcher has shut down.
    let mut forwarded = 0usize;
    for order in ingest_orders {
        if inserted.contains(order.note_id.to_bytes().as_slice()) {
            order_tx
                .send(order)
                .await
                .map_err(|_| anyhow::anyhow!("matcher channel closed"))?;
            forwarded += 1;
        }
    }
    tracing::info!(
        persisted = inserted.len(),
        forwarded,
        block = block_num,
        "ingested PSWAP orders"
    );

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Mock MidenClient for testing.
    pub struct MockMidenClient {
        notes: Vec<Note>,
        block: u64,
        /// Pre-staged consumed-note IDs returned by the next sync_state call.
        /// Drained on each sync so a single push delivers once.
        pending_consumed: Vec<NoteId>,
        /// Set of note IDs that should be reported as consumed by
        /// `check_consumed_notes`. Persistent — represents on-chain state.
        consumed_set: HashSet<NoteId>,
    }

    impl MockMidenClient {
        pub fn new() -> Self {
            Self {
                notes: Vec::new(),
                block: 0,
                pending_consumed: Vec::new(),
                consumed_set: HashSet::new(),
            }
        }

        pub fn add_notes(&mut self, notes: Vec<Note>, block: u64) {
            self.notes.extend(notes);
            self.block = block;
        }

        /// Stage NoteIds to be returned by the next `sync_state` call as
        /// `consumed_notes`. Also marks them as consumed for any later
        /// `check_consumed_notes` calls.
        pub fn add_consumed(&mut self, note_ids: Vec<NoteId>) {
            for id in &note_ids {
                self.consumed_set.insert(*id);
            }
            self.pending_consumed.extend(note_ids);
        }

        /// Mark IDs as consumed for `check_consumed_notes` without
        /// surfacing them via `sync_state`. Useful for tests of the
        /// executor's classify path where the discovery path is bypassed.
        pub fn mark_consumed_silent(&mut self, note_ids: Vec<NoteId>) {
            for id in note_ids {
                self.consumed_set.insert(id);
            }
        }
    }

    #[async_trait(?Send)]
    impl MidenClient for MockMidenClient {
        async fn subscribe_pair(&mut self, _offered: TokenId, _requested: TokenId) -> Result<()> {
            Ok(())
        }

        async fn sync_state(&mut self) -> Result<SyncResult> {
            let consumed = std::mem::take(&mut self.pending_consumed);
            Ok(SyncResult {
                block_num: self.block,
                new_notes: self.notes.clone(),
                consumed_notes: consumed,
            })
        }

        async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>> {
            Ok(notes
                .iter()
                .map(|n| n.id())
                .filter(|id| self.consumed_set.contains(id))
                .collect())
        }
    }
}

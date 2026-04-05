use anyhow::Result;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

use crate::db::models::{NoteRow, OrderRow};
use crate::db::{self, DbPool};
use crate::order::Order as PipelineOrder;
use crate::types::{OrderId, OrderStatus, TokenId};

/// Result of a sync_state call — contains the block number and IDs of newly received notes.
pub struct SyncResult {
    pub block_num: u64,
    pub new_note_ids: Vec<OrderId>,
}

/// Trait abstracting the Miden Node RPC client.
/// Object-safe (uses boxed futures) so it can be used as `dyn MidenClient`.
pub trait MidenClient: Send {
    /// Register note tags for a trading pair (both directions).
    /// Must be called before sync_state to receive notes for this pair.
    fn subscribe_pair(
        &mut self,
        base: TokenId,
        quote: TokenId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;

    /// Sync client state with the Miden Node.
    /// Returns the new block number and IDs of newly received notes.
    fn sync_state(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SyncResult>> + Send + '_>>;

    /// Fetch full notes by their IDs from the local store.
    fn get_notes_by_ids(
        &self,
        ids: &[OrderId],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Note>>> + Send + '_>>;
}

/// Run the note ingestion loop.
///
/// Each tick: sync → fetch new notes by ID → filter PSWAP → atomic DB insert → send to channel.
pub async fn run_ingest(
    client: Arc<Mutex<dyn MidenClient + Send>>,
    pool: DbPool,
    order_tx: mpsc::Sender<OrderRow>,
    interval: Duration,
) {
    loop {
        if let Err(e) = ingest_once(&client, &pool, &order_tx).await {
            eprintln!("[ingest] error: {e}");
        }
        tokio::time::sleep(interval).await;
    }
}

/// Single iteration of the ingest loop.
async fn ingest_once(
    client: &Arc<Mutex<dyn MidenClient + Send>>,
    pool: &DbPool,
    order_tx: &mpsc::Sender<OrderRow>,
) -> Result<()> {
    // 1. Sync state — get block number and newly received note IDs
    let sync_data = {
        let mut c = client.lock().await;
        c.sync_state().await?
    };

    if sync_data.new_note_ids.is_empty() {
        return Ok(());
    }

    // 2. Fetch full notes by IDs
    let notes = {
        let c = client.lock().await;
        c.get_notes_by_ids(&sync_data.new_note_ids).await?
    };

    // 3. Filter PSWAP notes and parse into DB records
    let mut new_notes = Vec::new();
    let mut new_orders = Vec::new();

    for note in &notes {
        // Only process PSWAP notes
        if note.recipient().script().root() != PswapNote::script_root() {
            continue;
        }

        let order = match PipelineOrder::from_note(note) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("[ingest] skipping unparseable note: {e}");
                continue;
            }
        };

        let note_id = note.id().to_bytes().to_vec();

        let mut raw_data = Vec::new();
        note.write_into(&mut raw_data);

        new_notes.push(NoteRow {
            note_id: note_id.clone(),
            account_id: order.creator_id.to_bytes().to_vec(),
            raw_data,
        });

        new_orders.push(OrderRow {
            note_id,
            account_id: order.creator_id.to_bytes().to_vec(),
            requested_asset: order.requested_faucet_id.to_bytes().to_vec(),
            requested_amount: order.requested_amount as i64,
            offered_asset: order.offered_faucet_id.to_bytes().to_vec(),
            offered_amount: order.offered_amount as i64,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            status: OrderStatus::Active.as_str().to_string(),
        });
    }

    if new_notes.is_empty() {
        return Ok(());
    }

    // 4. Atomic DB insert (notes + orders + block number)
    let mut conn = pool.get()?;
    db::insert_notes_batch(&mut conn, &new_notes, &new_orders, sync_data.block_num)?;

    // 5. Send new orders to channel
    for order in new_orders {
        if let Err(e) = order_tx.send(order).await {
            eprintln!("[ingest] channel send failed: {e}");
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Mock MidenClient for testing.
    pub struct MockMidenClient {
        notes: Vec<Note>,
        block: u64,
    }

    impl MockMidenClient {
        pub fn new() -> Self {
            Self {
                notes: Vec::new(),
                block: 0,
            }
        }

        pub fn add_notes(&mut self, notes: Vec<Note>, block: u64) {
            self.notes.extend(notes);
            self.block = block;
        }
    }

    impl MidenClient for MockMidenClient {
        fn subscribe_pair(
            &mut self,
            _base: TokenId,
            _quote: TokenId,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn sync_state(
            &mut self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SyncResult>> + Send + '_>>
        {
            let ids: Vec<OrderId> = self.notes.iter().map(|n| n.id()).collect();
            let block = self.block;
            Box::pin(async move {
                Ok(SyncResult {
                    block_num: block,
                    new_note_ids: ids,
                })
            })
        }

        fn get_notes_by_ids(
            &self,
            ids: &[OrderId],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Note>>> + Send + '_>>
        {
            let notes: Vec<Note> = self
                .notes
                .iter()
                .filter(|n| ids.contains(&n.id()))
                .cloned()
                .collect();
            Box::pin(async move { Ok(notes) })
        }
    }
}

use anyhow::Result;
use async_trait::async_trait;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};

use crate::db::models::{NoteRow, OrderRow};
use crate::db::{self, DbPool};
use crate::types::Order as PipelineOrder;
use crate::types::{IngestOrder, OrderStatus, TokenId};

/// Result of a sync_state call — contains the block number and newly received notes.
pub struct SyncResult {
    pub block_num: u64,
    pub new_notes: Vec<Note>,
}

/// Trait abstracting the Miden Node RPC client.
#[async_trait]
pub trait MidenClient: Send {
    /// Register note tags for a trading pair (both directions).
    /// Must be called before sync_state to receive notes for this pair.
    async fn subscribe_pair(&mut self, offered: TokenId, requested: TokenId) -> Result<()>;

    /// Sync client state with the Miden Node.
    /// Returns the new block number and full note data for newly received notes.
    async fn sync_state(&mut self) -> Result<SyncResult>;
}

/// Run the note ingestion loop.
///
/// Each tick: sync → fetch new notes by ID → filter PSWAP → atomic DB insert → send to channel.
pub async fn run_ingest(
    client: Arc<Mutex<dyn MidenClient + Send>>,
    pool: DbPool,
    order_tx: mpsc::Sender<IngestOrder>,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = ingest_once(&client, &pool, &order_tx).await {
            eprintln!("[ingest] error: {e}");
        }
    }
}

/// Single iteration of the ingest loop.
async fn ingest_once(
    client: &Arc<Mutex<dyn MidenClient + Send>>,
    pool: &DbPool,
    order_tx: &mpsc::Sender<IngestOrder>,
) -> Result<()> {
    let SyncResult { block_num, new_notes } = {
        let mut c = client.lock().await;
        c.sync_state().await?
    };

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
                eprintln!("[ingest] skipping unparseable note: {e}");
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

    // 4. Atomic DB insert (notes + orders + block number)
    let mut conn = pool.write_conn()?;
    db::insert_notes_batch(&mut conn, &db_notes, &db_orders, block_num)?;

    // send blocks when full (backpressure), errors only if matcher has crashed
    for order in ingest_orders {
        order_tx.send(order).await
            .map_err(|_| anyhow::anyhow!("matcher channel closed"))?;
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

    #[async_trait]
    impl MidenClient for MockMidenClient {
        async fn subscribe_pair(&mut self, _offered: TokenId, _requested: TokenId) -> Result<()> {
            Ok(())
        }

        async fn sync_state(&mut self) -> Result<SyncResult> {
            Ok(SyncResult {
                block_num: self.block,
                new_notes: self.notes.clone(),
            })
        }
    }
}

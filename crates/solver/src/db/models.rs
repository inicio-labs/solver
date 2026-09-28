use crate::db::schema::*;
use crate::types::{BookOrder, OrderId, OrderStatus};
use anyhow::Result;
use diesel::prelude::*;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;
use thiserror::Error;

#[derive(Debug, Error)]
enum StoredOrderError {
    #[error("stored order and note IDs differ")]
    NoteIdMismatch,
    #[error("stored order terms differ from note")]
    TermsMismatch,
}

#[derive(Queryable, Selectable, Insertable, Debug)]
#[diesel(table_name = sync_state)]
pub struct SyncState {
    pub id: i32,
    pub last_fetched_block: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = notes)]
pub struct NoteRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub raw_data: Vec<u8>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct OrderRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub requested_asset: Vec<u8>,
    pub requested_amount: i64,
    pub offered_asset: Vec<u8>,
    pub offered_amount: i64,
    pub timestamp: i64,
    pub status: String,
    pub priority_seq: i64,
}

impl OrderRow {
    pub fn order_status(&self) -> Option<OrderStatus> {
        OrderStatus::parse(&self.status)
    }

    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.status = status.as_str().to_string();
        self
    }

    pub fn into_book_order(self, raw_note_data: Vec<u8>) -> Result<BookOrder> {
        let note = Note::read_from(&mut SliceReader::new(&raw_note_data))?;
        let parsed = crate::types::Order::from_note(&note)?;
        let note_id = OrderId::read_from(&mut SliceReader::new(&self.note_id))?;
        if note.id() != note_id {
            return Err(StoredOrderError::NoteIdMismatch.into());
        }
        // Validate persisted metadata at the DB boundary, not on every book insert.
        let terms_match = parsed.offered_faucet_id.to_bytes() == self.offered_asset
            && parsed.requested_faucet_id.to_bytes() == self.requested_asset
            && parsed.offered_amount == u64::try_from(self.offered_amount)?
            && parsed.requested_amount == u64::try_from(self.requested_amount)?;
        if !terms_match {
            return Err(StoredOrderError::TermsMismatch.into());
        }
        Ok(BookOrder {
            priority_seq: u64::try_from(self.priority_seq)?,
            note: std::sync::Arc::new(note),
        })
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_attempts)]
pub struct SettlementAttemptRow {
    pub tx_id: Vec<u8>,
    pub tx_result: Vec<u8>,
    pub status: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_inputs)]
pub struct SettlementInputRow {
    pub tx_id: Vec<u8>,
    pub parent_note_id: Vec<u8>,
    pub payback_note_id: Vec<u8>,
    pub child_note_id: Option<Vec<u8>>,
    pub child_note_data: Option<Vec<u8>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = generated_notes)]
pub struct GeneratedNoteRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub source_note_a: Vec<u8>,
    pub source_note_b: Vec<u8>,
    pub data: Vec<u8>,
    pub created_at: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = registered_tokens)]
pub struct RegisteredTokenRow {
    pub token_id: Vec<u8>,
    pub created_at: i64,
    pub external_symbol: Option<String>,
    /// On-chain token decimals, fetched from the faucet once at registration
    /// (NULL until known). `Integer` (i32) since a `u8` fits trivially.
    pub decimals: Option<i32>,
    /// On-chain token symbol/ticker (e.g. "USDC"), fetched with `decimals`.
    pub ticker: Option<String>,
}

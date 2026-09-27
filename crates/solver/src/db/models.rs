use crate::db::schema::*;
use crate::types::{IngestOrder, OrderId, OrderStatus, TokenId};
use anyhow::{anyhow, Result};
use diesel::prelude::*;
use miden_protocol::crypto::utils::{Deserializable, SliceReader};
use miden_protocol::note::Note;

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
        OrderStatus::from_str(&self.status)
    }

    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.status = status.as_str().to_string();
        self
    }

    pub fn into_ingest(self, raw_note_data: Vec<u8>) -> Result<IngestOrder> {
        let note = Note::read_from(&mut SliceReader::new(&raw_note_data))?;
        let parsed = crate::types::Order::from_note(&note)?;
        let note_id = OrderId::read_from(&mut SliceReader::new(&self.note_id))?;
        if note.id() != note_id {
            return Err(anyhow!("stored order and note IDs differ"));
        }
        Ok(IngestOrder {
            note_id,
            priority_seq: u64::try_from(self.priority_seq)?,
            offered_token: TokenId::read_from(&mut SliceReader::new(&self.offered_asset))?,
            requested_token: TokenId::read_from(&mut SliceReader::new(&self.requested_asset))?,
            offered_amount: u64::try_from(self.offered_amount)?,
            requested_amount: u64::try_from(self.requested_amount)?,
            min_fill_step: parsed.min_fill_step,
            raw_note_data,
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

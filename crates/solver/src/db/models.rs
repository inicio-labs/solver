use diesel::prelude::*;
use crate::db::schema::*;
use crate::types::OrderStatus;

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
}

impl OrderRow {
    pub fn order_status(&self) -> Option<OrderStatus> {
        OrderStatus::from_str(&self.status)
    }

    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.status = status.as_str().to_string();
        self
    }
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

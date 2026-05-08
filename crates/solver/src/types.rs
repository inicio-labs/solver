use anyhow::{anyhow, Result};
use miden_protocol::account::AccountId;
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;

/// Faucet ID identifying a token.
pub type TokenId = AccountId;

/// Note ID identifying an order.
pub type OrderId = NoteId;

/// Token amount (u64 to match Miden's native asset amounts).
pub type Amount = u64;

/// Order lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Active,
    InFlight,
    Executed,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Active => "active",
            OrderStatus::InFlight => "in_flight",
            OrderStatus::Executed => "executed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(OrderStatus::Active),
            "in_flight" => Some(OrderStatus::InFlight),
            "executed" => Some(OrderStatus::Executed),
            _ => None,
        }
    }
}

/// An order extracted from a PSWAP note.
#[derive(Debug, Clone)]
pub struct Order {
    pub note: Note,
    pub offered_faucet_id: AccountId,
    pub offered_amount: u64,
    pub requested_faucet_id: AccountId,
    pub requested_amount: u64,
    pub creator_id: AccountId,
}

impl Order {
    pub fn from_note(note: &Note) -> Result<Self> {
        let pswap = PswapNote::try_from(note)
            .map_err(|e| anyhow!("Failed to parse PSWAP note: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_faucet_id = offered_asset.faucet_id();
        let offered_amount = offered_asset.amount();

        let requested_asset = pswap.storage().requested_asset();
        let requested_faucet_id = requested_asset.faucet_id();
        let requested_amount = requested_asset.amount();

        let creator_id = pswap.storage().creator_account_id();

        if offered_amount == 0 || requested_amount == 0 {
            return Err(anyhow!("order has zero amount (offered={offered_amount}, requested={requested_amount})"));
        }

        Ok(Order {
            note: note.clone(),
            offered_faucet_id,
            offered_amount,
            requested_faucet_id,
            requested_amount,
            creator_id,
        })
    }
}

/// An order with its raw note data, flowing from ingest → matcher.
#[derive(Debug, Clone)]
pub struct IngestOrder {
    pub note_id: OrderId,
    pub offered_token: TokenId,
    pub requested_token: TokenId,
    pub offered_amount: Amount,
    pub requested_amount: Amount,
    pub raw_note_data: Vec<u8>,
}

/// A filled note with its fill amount, flowing from matcher → executor.
#[derive(Debug, Clone)]
pub struct FilledNote {
    pub note_id: OrderId,
    pub requested_filled: Amount,
    pub raw_note_data: Vec<u8>,
}

/// A batch of matched orders to be executed together.
#[derive(Debug, Clone)]
pub struct ExecutionBatch {
    pub filled_notes: Vec<FilledNote>,
}

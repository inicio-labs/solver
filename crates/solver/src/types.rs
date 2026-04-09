use miden_protocol::account::AccountId;
use miden_protocol::note::NoteId;

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

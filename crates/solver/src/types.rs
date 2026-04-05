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

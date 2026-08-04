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

/// Milliseconds since the Unix epoch (0 if the clock is before it). std has no
/// single call for this — `SystemTime` + `duration_since(UNIX_EPOCH)` is idiomatic.
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Order lifecycle status.
///
/// `Settling` is the interval between submitting an on-chain settlement
/// transaction and confirming its outcome. Crash recovery resets any
/// `Settling` rows back to `Active` at boot.
///
/// `OnchainNullified` is terminal: ingest or executor observed that the
/// note's nullifier is on-chain (consumed by another party, or by a
/// previous solver attempt whose DB bookkeeping we lost). No further
/// processing — the matcher's hydration query already filters
/// `status = 'active'`, so terminal rows are excluded automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Active,
    Settling,
    Executed,
    OnchainNullified,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Active => "active",
            OrderStatus::Settling => "settling",
            OrderStatus::Executed => "executed",
            OrderStatus::OnchainNullified => "onchain_nullified",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(OrderStatus::Active),
            "settling" => Some(OrderStatus::Settling),
            "executed" => Some(OrderStatus::Executed),
            "onchain_nullified" => Some(OrderStatus::OnchainNullified),
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
        let offered_amount: u64 = offered_asset.amount().into();

        let requested_asset = pswap.storage().requested_asset();
        let requested_faucet_id = requested_asset.faucet_id();
        let requested_amount: u64 = requested_asset.amount().into();

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

use anyhow::{anyhow, Result};
use miden_protocol::account::AccountId;
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use std::sync::Arc;
use thiserror::Error;

/// Faucet ID identifying a token.
pub type TokenId = AccountId;

/// Note ID identifying an order.
pub type OrderId = NoteId;

/// Token amount (u64 to match Miden's native asset amounts).
pub type Amount = u64;

/// Invalid state detected while preparing, storing, or recovering a settlement.
#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("settlement input belongs to a different transaction")]
    InputTransactionMismatch,
    #[error("settlement input order is not active")]
    InputOrderNotActive,
    #[error("settlement child ID does not match its note")]
    ChildIdMismatch,
    #[error("expected payback {0} is absent from the executed outputs")]
    MissingPayback(NoteId),
    #[error("expected remainder {0} is absent from the executed outputs")]
    MissingRemainder(NoteId),
    #[error("recorded transaction ID does not match its transaction result")]
    RecordedTransactionIdMismatch,
    #[error("invalid execution group boundary {end} after {previous} for {total} notes")]
    InvalidExecutionGroupBoundary {
        previous: usize,
        end: usize,
        total: usize,
    },
    #[error("execution group has {size} inputs; maximum is {maximum}")]
    ExecutionGroupTooLarge { size: usize, maximum: usize },
    #[error("execution groups cover {covered} of {total} notes")]
    IncompleteExecutionGroups { covered: usize, total: usize },
}

/// Milliseconds since the Unix epoch (0 if the clock is before it). std has no
/// single call for this — `SystemTime` + `duration_since(UNIX_EPOCH)` is idiomatic.
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Seconds since the Unix epoch. A named alias (not a bare `u64`) so signatures
/// carrying a wall-clock instant read as *time* at the call site.
pub type UnixSecs = u64;

/// Wall-clock now, in [`UnixSecs`] (saturating). The seconds analogue of
/// [`now_millis`]; stamps order arrival for the swap-eta window.
pub fn now_unix() -> UnixSecs {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Order lifecycle status.
///
/// `Settling` is the interval between submitting an on-chain settlement
/// transaction and confirming its outcome. Recovery keeps unresolved
/// transactions reserved until their outcome is known.
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

    pub fn parse(s: &str) -> Option<Self> {
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
    /// Per-fill floor from the note (`0` = none): the script rejects a
    /// consumption whose total fill is below `min(min_fill_step, requested_amount)`.
    pub min_fill_step: u64,
    pub creator_id: AccountId,
}

impl Order {
    pub fn from_note(note: &Note) -> Result<Self> {
        let pswap =
            PswapNote::try_from(note).map_err(|e| anyhow!("Failed to parse PSWAP note: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_faucet_id = offered_asset.faucet_id();
        let offered_amount: u64 = offered_asset.amount().into();

        let requested_asset = pswap.storage().min_requested_asset();
        let requested_faucet_id = requested_asset.faucet_id();
        let requested_amount: u64 = requested_asset.amount().into();

        let creator_id = pswap.storage().creator_account_id();
        let min_fill_step = pswap.storage().min_fill_step().as_u64();

        if offered_amount == 0 || requested_amount == 0 {
            return Err(anyhow!(
                "order has zero amount (offered={offered_amount}, requested={requested_amount})"
            ));
        }

        Ok(Order {
            note: note.clone(),
            offered_faucet_id,
            offered_amount,
            requested_faucet_id,
            requested_amount,
            min_fill_step,
            creator_id,
        })
    }
}

/// A shared note and its durable FIFO priority, flowing into the matcher.
#[derive(Debug, Clone)]
pub struct BookOrder {
    /// Durable ingestion FIFO sequence, independent of restart order.
    pub priority_seq: u64,
    /// First durable observation time. Re-feeds and remainders preserve it.
    pub arrival_unix: UnixSecs,
    pub note: Arc<Note>,
}

impl BookOrder {
    pub fn id(&self) -> OrderId {
        self.note.id()
    }
}

/// One committed change to the book. Apply removals and activations without
/// yielding, so a parent-to-remainder handoff cannot be matched halfway through.
#[derive(Debug, Default)]
pub struct BookUpdate {
    pub removed: Vec<OrderId>,
    pub active: Vec<BookOrder>,
}

impl BookUpdate {
    /// Avoid waking the matcher for a transaction that changed no book entries.
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.active.is_empty()
    }
}

impl From<BookOrder> for BookUpdate {
    fn from(order: BookOrder) -> Self {
        Self {
            removed: Vec::new(),
            active: vec![order],
        }
    }
}

/// A filled note with its fill amount, flowing from matcher → executor.
#[derive(Debug, Clone)]
pub struct FilledNote {
    pub note_id: OrderId,
    pub priority_seq: u64,
    pub requested_filled: Amount,
    pub note: Arc<Note>,
    /// First durable observation time, carried through re-feeds and remainders
    /// so the executor can record `settled - arrival` consistently.
    pub arrival_unix: UnixSecs,
}

impl FilledNote {
    /// Re-activate the original note without re-parsing its terms.
    pub fn to_book_order(&self) -> BookOrder {
        BookOrder {
            priority_seq: self.priority_seq,
            arrival_unix: self.arrival_unix,
            note: self.note.clone(),
        }
    }
}

/// A batch of matched orders to be executed together.
#[derive(Debug, Clone)]
pub struct ExecutionBatch {
    pub filled_notes: Vec<FilledNote>,
    /// Exclusive ends of independently solvent groups. The executor may split
    /// only between these boundaries, never between counterparties in a group.
    /// Empty means the whole batch is indivisible (legacy matching).
    pub group_ends: Vec<usize>,
}

impl ExecutionBatch {
    pub fn book_orders(&self) -> Vec<BookOrder> {
        self.filled_notes
            .iter()
            .map(FilledNote::to_book_order)
            .collect()
    }
}

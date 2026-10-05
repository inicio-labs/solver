//! Market-maker domain types shared by the maker store, the matcher and the
//! ordered matcher update stream between them (ADR 0003).

use miden_protocol::crypto::hash::blake::Blake3_256;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use thiserror::Error;
use tokio::sync::watch;

use crate::types::{OrderError, OrderId, OrderKeys, TokenId};

/// Database identity of an onboarded maker.
pub type MakerId = i64;

/// Creator account plus root serial number: the same for every note of one
/// order across partial fills (see [`crate::types::OrderKeys`]).
pub type LineageId = Vec<u8>;

/// The maker that claimed an order's lineage, and the sequence of the submit
/// that claimed it. Every remainder of the order carries the same tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MakerTag {
    pub maker_id: MakerId,
    pub root_seq: u64,
    /// MM deadline, in Unix milliseconds; applies to the whole lineage.
    pub expires_at_unix_ms: Option<u64>,
}

/// Both faucet IDs, smaller bytes first: the same key for either direction.
pub fn market_key(a: TokenId, b: TokenId) -> Vec<u8> {
    let (a, b) = (a.to_bytes(), b.to_bytes());
    if a <= b {
        [a, b].concat()
    } else {
        [b, a].concat()
    }
}

/// Offered faucet ID, then requested faucet ID.
pub fn direction_key(offered: TokenId, requested: TokenId) -> Vec<u8> {
    [offered.to_bytes(), requested.to_bytes()].concat()
}

/// Canonical offered-token bytes followed by requested-token bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderPair(pub Vec<u8>);

impl OrderPair {
    pub fn new(offered: TokenId, requested: TokenId) -> Self {
        Self(direction_key(offered, requested))
    }
}

/// Which of a maker's orders a cancel-all stops, in the byte form stored in
/// `maker_cutoffs`: an empty market or direction matches every one. A
/// direction scope also stores its market, so the two never disagree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CutoffScope {
    pub market: Vec<u8>,
    pub direction: Vec<u8>,
}

impl CutoffScope {
    pub fn all() -> Self {
        Self {
            market: Vec::new(),
            direction: Vec::new(),
        }
    }

    pub fn market(a: TokenId, b: TokenId) -> Self {
        Self {
            market: market_key(a, b),
            direction: Vec::new(),
        }
    }

    pub fn direction(offered: TokenId, requested: TokenId) -> Self {
        Self {
            market: market_key(offered, requested),
            direction: direction_key(offered, requested),
        }
    }

    /// Whether an order with these keys (see [`OrderKeys`]) is in this scope.
    pub fn covers(&self, market: &[u8], direction: &[u8]) -> bool {
        (self.market.is_empty() || self.market == market)
            && (self.direction.is_empty() || self.direction == direction)
    }

    /// Match an order's offered/requested tokens without allocating keys.
    pub fn covers_pair(&self, offered: TokenId, requested: TokenId) -> bool {
        let offered = offered.to_bytes();
        let requested = requested.to_bytes();
        let (first, second) = if offered <= requested {
            (&offered[..], &requested[..])
        } else {
            (&requested[..], &offered[..])
        };
        let equals_pair = |key: &[u8], a: &[u8], b: &[u8]| {
            key.iter()
                .copied()
                .eq(a.iter().copied().chain(b.iter().copied()))
        };
        (self.market.is_empty() || equals_pair(&self.market, first, second))
            && (self.direction.is_empty() || equals_pair(&self.direction, &offered, &requested))
    }
}

/// A committed maker change, sent with book changes in commit order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MakerUpdate {
    /// Orders of `maker_id` in `scope` whose root sequence is below `cutoff`
    /// are cancelled. `cutoff` is the effective, already-maximised barrier.
    CutoffRaised {
        maker_id: MakerId,
        scope: CutoffScope,
        cutoff: u64,
    },
    /// A targeted cancel: every note of this lineage claimed by `maker_id`.
    LineageCancelled {
        maker_id: MakerId,
        lineage_id: LineageId,
    },
    /// A submit claimed existing orders; update their metadata in place.
    OrdersAttributed {
        order_ids: Vec<OrderId>,
        tag: MakerTag,
        /// A targeted cancellation committed before these orders were claimed.
        cancelled: bool,
    },
}

/// Wakes maker event streams after a commit that appended events. Only a
/// hint: streams read durable rows, and also look on every keep-alive.
#[derive(Clone)]
pub struct EventWake {
    generation_tx: watch::Sender<u64>,
}

impl Default for EventWake {
    fn default() -> Self {
        Self {
            generation_tx: watch::channel(0).0,
        }
    }
}

static EVENT_WAKE: LazyLock<EventWake> = LazyLock::new(EventWake::default);
static EVENTS_APPENDED: AtomicBool = AtomicBool::new(false);

impl EventWake {
    /// The process's wake, notified by the core writer.
    pub fn global() -> &'static EventWake {
        &EVENT_WAKE
    }

    pub fn notify(&self) {
        self.generation_tx
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation_tx.subscribe()
    }

    /// Recorded by every event append, inside its transaction.
    pub(crate) fn mark_appended() {
        EVENTS_APPENDED.store(true, Ordering::Release);
    }

    /// Called after every core-writer commit: wakes the streams if the
    /// committed transaction appended events. A rolled-back append leaves
    /// the mark for the next commit, which only wakes streams once more.
    pub(crate) fn notify_if_appended() {
        if EVENTS_APPENDED.swap(false, Ordering::AcqRel) {
            Self::global().notify();
        }
    }
}

/// Maker commands carry a request ID and a maker-assigned sequence, unique
/// per maker across every command kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandHeader {
    pub maker_id: MakerId,
    pub request_id: String,
    pub seq: u64,
}

/// Longest accepted request ID, in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 128;

/// A command header the store can write without a constraint violation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InvalidCommand {
    #[error("request ID must be 1 to {MAX_REQUEST_ID_LEN} bytes")]
    RequestId,
    #[error("sequence must be between 1 and {}", i64::MAX)]
    Sequence,
}

impl CommandHeader {
    /// Validated here, before the intake batches it: a command that reached
    /// the store must not be able to abort the batch's transaction.
    pub fn new(maker_id: MakerId, request_id: String, seq: u64) -> Result<Self, InvalidCommand> {
        if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_LEN {
            return Err(InvalidCommand::RequestId);
        }
        if seq == 0 || i64::try_from(seq).is_err() {
            return Err(InvalidCommand::Sequence);
        }
        Ok(Self {
            maker_id,
            request_id,
            seq,
        })
    }
}

/// A validated maker command.
#[derive(Debug, Clone)]
pub enum MakerCommand {
    /// A PSWAP note, already parsed; `keys` were computed from it.
    Submit {
        note: Box<Note>,
        keys: OrderKeys,
        expires_at_unix_ms: Option<u64>,
    },
    CancelAll {
        scope: CutoffScope,
    },
    /// Targeted cancel, by lineage ID. It may name a lineage not yet submitted.
    CancelOrder {
        lineage_id: LineageId,
    },
}

impl MakerCommand {
    pub fn submit(note: Note) -> Result<Self, OrderError> {
        crate::types::Order::from_note(&note)?;
        let keys = OrderKeys::from_note(&note)?;
        Ok(Self::Submit {
            note: Box::new(note),
            keys,
            expires_at_unix_ms: None,
        })
    }

    /// Stored command kind.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Submit { .. } => "submit",
            Self::CancelAll { .. } => "cancel_all",
            Self::CancelOrder { .. } => "cancel_order",
        }
    }

    /// Canonical bytes compared when a request ID is retried.
    pub fn payload(&self) -> Vec<u8> {
        match self {
            Self::Submit {
                note,
                expires_at_unix_ms,
                ..
            } => {
                // Keep no-expiry payloads compatible with previously stored retries.
                let mut payload = note.to_bytes();
                if let Some(expiry) = expires_at_unix_ms {
                    payload.extend_from_slice(&expiry.to_be_bytes());
                }
                payload
            }
            Self::CancelAll { scope } => {
                // A market key is empty or two faucet IDs long.
                let mut payload = vec![scope.market.len() as u8];
                payload.extend_from_slice(&scope.market);
                payload.extend_from_slice(&scope.direction);
                payload
            }
            Self::CancelOrder { lineage_id } => lineage_id.clone(),
        }
    }
}

/// A new API key for a maker: shown once, stored only as its hash.
pub fn new_api_key() -> String {
    let mut secret = [0_u8; 32];
    rand::rng().fill_bytes(&mut secret);
    format!("mmk_{}", hex::encode(secret))
}

/// What `api_keys.key_hash` stores for `key`.
pub fn api_key_hash(key: &str) -> Vec<u8> {
    Blake3_256::hash(key.as_bytes()).as_bytes().to_vec()
}

/// The stored reply of a committed maker command, returned again for an
/// exact retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum CommandResult {
    /// The note and its lineage claim are durable. Not yet Live.
    Accepted,
    /// Another submit already claimed this note's lineage.
    AlreadyRegistered,
    /// Cancel-all applied. `cutoff` is the effective barrier (the maximum of
    /// this and any earlier cancel in the scope); `settling` counts orders
    /// already reserved by a settlement, which may still fill.
    Applied { cutoff: u64, settling: u64 },
    /// Targeted cancel applied; `settling` as for [`CommandResult::Applied`].
    Stopped { settling: u64 },
}

/// What a maker command returns to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandReply {
    /// Committed by this call.
    Committed(CommandResult),
    /// An exact retry: the reply stored when the command first committed.
    Replayed(CommandResult),
    /// The request ID or sequence was already used for a different command.
    Conflict,
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    #[test]
    fn scopes_cover_their_market_and_direction_only() {
        let x = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap();
        let y = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap();
        let covers = |scope: CutoffScope, offered, requested| {
            let stored = scope.covers(
                &market_key(offered, requested),
                &direction_key(offered, requested),
            );
            assert_eq!(scope.covers_pair(offered, requested), stored);
            stored
        };
        assert!(covers(CutoffScope::all(), x, y));
        assert!(covers(CutoffScope::market(y, x), x, y));
        assert!(covers(CutoffScope::market(x, y), y, x));
        assert!(covers(CutoffScope::direction(x, y), x, y));
        assert!(!covers(CutoffScope::direction(x, y), y, x));
        assert!(!covers(CutoffScope::market(x, x), x, y));
    }

    #[test]
    fn headers_that_would_violate_a_constraint_are_rejected() {
        assert!(CommandHeader::new(1, "r".into(), 1).is_ok());
        assert_eq!(
            CommandHeader::new(1, String::new(), 1),
            Err(InvalidCommand::RequestId)
        );
        assert_eq!(
            CommandHeader::new(1, "r".repeat(MAX_REQUEST_ID_LEN + 1), 1),
            Err(InvalidCommand::RequestId)
        );
        assert_eq!(
            CommandHeader::new(1, "r".into(), 0),
            Err(InvalidCommand::Sequence)
        );
        assert_eq!(
            CommandHeader::new(1, "r".into(), u64::MAX),
            Err(InvalidCommand::Sequence)
        );
    }

    #[test]
    fn api_keys_are_random_and_hash_stably() {
        let (a, b) = (new_api_key(), new_api_key());
        assert_ne!(a, b);
        assert!(a.starts_with("mmk_") && a.len() == 4 + 64);
        assert_eq!(api_key_hash(&a), api_key_hash(&a));
        assert_ne!(api_key_hash(&a), api_key_hash(&b));
    }

    #[test]
    fn stored_results_round_trip() {
        for result in [
            CommandResult::Accepted,
            CommandResult::AlreadyRegistered,
            CommandResult::Applied {
                cutoff: 100,
                settling: 2,
            },
            CommandResult::Stopped { settling: 0 },
        ] {
            let text = serde_json::to_string(&result).unwrap();
            assert_eq!(
                serde_json::from_str::<CommandResult>(&text).unwrap(),
                result
            );
        }
    }
}

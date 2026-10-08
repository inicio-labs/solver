use miden_protocol::account::AccountId;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::errors::NoteError;
use miden_protocol::note::{Note, NoteId};
use miden_protocol::Felt;
use miden_standards::note::PswapNote;
use std::sync::Arc;
use thiserror::Error;

use crate::maker::{direction_key, market_key, LineageId, MakerTag};

/// Faucet ID identifying a token.
pub type TokenId = AccountId;

/// Note ID identifying an order.
pub type OrderId = NoteId;

/// Token amount (u64 to match Miden's native asset amounts).
pub type Amount = u64;

/// Invalid state detected while preparing, storing, or recovering a settlement.
#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("settlement has no inputs")]
    NoInputs,
    #[error("settlement lists the same parent order twice")]
    DuplicateParent,
    #[error("settlement input order is missing")]
    MissingInputOrder,
    #[error("settlement child is not a valid remainder of its parent")]
    InvalidRemainder,
    #[error("settlement input order is not live")]
    InputOrderNotActive,
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
/// processing — the matcher hydrates from the `live_orders` view, which
/// admits only `status = 'active'`, so terminal rows are excluded.
/// Stored as its snake_case name (`as_str`, derived by `strum`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum OrderStatus {
    Active,
    Settling,
    Executed,
    OnchainNullified,
    /// A maker order a cancel stopped, recorded when a write that happens
    /// anyway (insertion, release) finds it below a cutoff. Liveness is read
    /// through `live_orders`, never from this column alone.
    Stopped,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// A note that cannot be traded as a PSWAP order.
#[derive(Debug, Error)]
pub enum OrderError {
    #[error("note is not a PSWAP order")]
    NotPswap(#[from] NoteError),
    #[error("order has zero amount (offered={offered}, requested={requested})")]
    ZeroAmount { offered: u64, requested: u64 },
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
    pub fn from_note(note: &Note) -> Result<Self, OrderError> {
        let pswap = PswapNote::try_from(note)?;

        let offered_asset = pswap.offered_asset();
        let offered_faucet_id = offered_asset.faucet_id();
        let offered_amount: u64 = offered_asset.amount().into();

        let requested_asset = pswap.storage().min_requested_asset();
        let requested_faucet_id = requested_asset.faucet_id();
        let requested_amount: u64 = requested_asset.amount().into();

        let creator_id = pswap.storage().creator_account_id();
        let min_fill_step = pswap.storage().min_fill_step().as_u64();

        if offered_amount == 0 || requested_amount == 0 {
            return Err(OrderError::ZeroAmount {
                offered: offered_amount,
                requested: requested_amount,
            });
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

/// Keys computed from a PSWAP note that identify its order across fills and
/// place it in a market, for maker attribution and cancellation scopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKeys {
    /// Creator account plus the root note's full serial number. The PSWAP
    /// script builds each remainder with serial `[s0, s1, s2, s3 + 1]` and
    /// depth + 1, so `(s0, s1, s2, s3 - depth)` is the same for every note of
    /// one order. Stored as exact bytes, not a hash.
    pub lineage_id: LineageId,
    /// 0 for the original note, one more per partial fill.
    pub depth: u32,
    /// Both faucet IDs in canonical order: the same for either direction.
    pub market: Vec<u8>,
    /// Offered faucet ID, then requested faucet ID.
    pub direction: Vec<u8>,
}

impl OrderKeys {
    pub fn from_note(note: &Note) -> Result<Self, OrderError> {
        let pswap = PswapNote::try_from(note)?;
        Ok(Self::from_pswap(&pswap))
    }

    pub fn from_pswap(pswap: &PswapNote) -> Self {
        let depth = pswap.parent_depth();
        let lineage_id = Self::lineage_id_from_pswap(pswap);
        let offered = pswap.offered_asset().faucet_id();
        let requested = pswap.storage().requested_faucet_id();
        Self {
            lineage_id,
            depth,
            market: market_key(offered, requested),
            direction: direction_key(offered, requested),
        }
    }

    pub fn lineage_id_from_pswap(pswap: &PswapNote) -> LineageId {
        let depth = pswap.parent_depth();
        let serial = pswap.serial_number();
        let root_s3 = serial[3] - Felt::from(depth);
        let mut lineage_id = pswap.storage().creator_account_id().to_bytes();
        for element in [serial[0], serial[1], serial[2], root_s3] {
            lineage_id.extend_from_slice(&element.as_canonical_u64().to_be_bytes());
        }
        lineage_id
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
    /// Set when a maker claimed the order's lineage. Filled in from the
    /// database by every book-changing transaction (`write_book`) and by
    /// startup hydration; `None` elsewhere.
    pub maker: Option<MakerTag>,
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
    /// Maker control changes committed in the same ordered publication stream.
    pub maker_updates: Vec<crate::maker::MakerUpdate>,
}

impl BookUpdate {
    /// Avoid waking the matcher for a transaction that changed no book entries.
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.active.is_empty() && self.maker_updates.is_empty()
    }
}

impl From<BookOrder> for BookUpdate {
    fn from(order: BookOrder) -> Self {
        Self {
            removed: Vec::new(),
            active: vec![order],
            maker_updates: Vec::new(),
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
            maker: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNoteAttachment, PswapNoteStorage};

    fn order(serial: Word, offered: AccountId, requested: AccountId) -> PswapNote {
        let storage = PswapNoteStorage::builder()
            .min_requested_asset(FungibleAsset::new(requested, 100).unwrap())
            .min_fill_step(AssetAmount::new(1).unwrap())
            .creator_account_id(
                ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
                    .try_into()
                    .unwrap(),
            )
            .build();
        PswapNote::builder()
            .sender(
                ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
                    .try_into()
                    .unwrap(),
            )
            .storage(storage)
            .serial_number(serial)
            .note_type(NoteType::Private)
            .offered_asset(FungibleAsset::new(offered, 50).unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn remainders_keep_the_lineage_and_count_depth() {
        let (x, y) = (
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
        );
        let root = order(Word::from([7_u32, 8, 9, 10]), x, y);
        let consumer = ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
            .try_into()
            .unwrap();
        let first = root
            .remainder_note(
                consumer,
                &PswapNoteAttachment::new(AssetAmount::new(40).unwrap(), root.order_id(), 1),
                AssetAmount::new(30).unwrap(),
                AssetAmount::new(60).unwrap(),
            )
            .unwrap();
        let second = PswapNote::try_from(&first)
            .unwrap()
            .remainder_note(
                consumer,
                &PswapNoteAttachment::new(AssetAmount::new(20).unwrap(), root.order_id(), 2),
                AssetAmount::new(15).unwrap(),
                AssetAmount::new(30).unwrap(),
            )
            .unwrap();

        let keys =
            [Note::from(root), first, second].map(|note| OrderKeys::from_note(&note).unwrap());
        assert_eq!(
            keys.iter().map(|k| k.depth).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(keys.iter().all(|k| k.lineage_id == keys[0].lineage_id));
        assert!(keys.iter().all(|k| k.direction == keys[0].direction));
    }

    #[test]
    fn different_serials_are_different_lineages_and_pairs_share_a_market() {
        let (x, y) = (
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
        );
        let keys = |serial: [u32; 4], offered, requested| {
            OrderKeys::from_note(&order(Word::from(serial), offered, requested).into()).unwrap()
        };
        // Differing only in the last element (a counter) still differs: the
        // lineage uses the whole root serial.
        assert_ne!(
            keys([0, 0, 0, 1], x, y).lineage_id,
            keys([0, 0, 0, 2], x, y).lineage_id
        );
        let (buy, sell) = (keys([1, 2, 3, 4], x, y), keys([5, 6, 7, 8], y, x));
        assert_eq!(buy.market, sell.market);
        assert_ne!(buy.direction, sell.direction);
    }
}

use miden_protocol::asset::{AssetAmount, AssetId, FungibleAsset};
use miden_protocol::crypto::utils::{Deserializable, SliceReader};
use miden_protocol::errors::{AssetError, NoteError};
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use ruint::aliases::U256;
use thiserror::Error;

use crate::types::IngestOrder;

pub type Wide = U256;
pub type PrioritySeq = u64;

pub const PPM_DENOMINATOR: u32 = 1_000_000;
pub const DEFAULT_MAX_ORDERS_PER_SIDE: usize = 100;
pub const DEFAULT_MAX_INTERVALS_PER_ROW: usize = 1_000;
pub const DEFAULT_MAX_TOTAL_INTERVALS_PER_SIDE: usize = 50_000;

/// Quote asset base units per one base asset base unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPrice {
    pub quote_units: Wide,
    pub base_units: Wide,
}

/// An exact price for one whole token in the same reference currency for both
/// assets, supplied by Polaris from one frozen snapshot. A decimal feed string
/// can be parsed without passing through `f64`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReferencePrice {
    pub numerator: Wide,
    pub denominator: Wide,
}

#[derive(Clone, Debug)]
pub struct AdmittedPswap {
    pub note_id: NoteId,
    pub note: PswapNote,
    pub priority_seq: PrioritySeq,
}

impl AdmittedPswap {
    pub fn from_note(note: &Note, priority_seq: PrioritySeq) -> Result<Self, ClearingError> {
        if priority_seq == 0 {
            return Err(ClearingError::InvalidOrder {
                note_id: note.id(),
                reason: InvalidOrderReason::MissingPriority,
            });
        }
        let parsed = PswapNote::try_from(note).map_err(|source| ClearingError::InvalidPswap {
            note_id: note.id(),
            source,
        })?;
        Ok(Self {
            note_id: note.id(),
            note: parsed,
            priority_seq,
        })
    }

    /// Admit the exact note bytes and durable FIFO sequence supplied by the
    /// ingestion pipeline. Reject stale or mismatched cached order fields.
    pub fn from_ingest_order(order: &IngestOrder) -> Result<Self, ClearingError> {
        let note = Note::read_from(&mut SliceReader::new(&order.raw_note_data)).map_err(|_| {
            ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::MalformedRawNote,
            }
        })?;
        if note.id() != order.note_id {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::InconsistentIngestOrder,
            });
        }
        let admitted = Self::from_note(&note, order.priority_seq)?;
        let offered = admitted.note.offered_asset();
        let requested = admitted.note.storage().min_requested_asset();
        if offered.faucet_id() != order.offered_token
            || offered.amount().as_u64() != order.offered_amount
            || requested.faucet_id() != order.requested_token
            || requested.amount().as_u64() != order.requested_amount
            || admitted.note.storage().min_fill_step().as_u64() != order.min_fill_step
        {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::InconsistentIngestOrder,
            });
        }
        Ok(admitted)
    }
}

#[derive(Clone, Debug)]
pub struct PairBatch {
    pub base: AssetId,
    pub quote: AssetId,
    pub clearing_price: ExactPrice,
    pub orders: Vec<AdmittedPswap>,
}

impl PairBatch {
    /// Construct a pair batch from Polaris's frozen prices for the two whole
    /// tokens and the on-chain faucet decimals. Both prices must use the same
    /// reference currency and snapshot; Polaris enforces freshness and skew.
    pub fn from_reference_prices(
        base: AssetId,
        quote: AssetId,
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
        orders: Vec<AdmittedPswap>,
    ) -> Result<Self, ClearingError> {
        Ok(Self {
            base,
            quote,
            clearing_price: ExactPrice::from_reference_prices(
                base_price,
                quote_price,
                base_decimals,
                quote_decimals,
            )?,
            orders,
        })
    }

    /// Boundary used by the solver pipeline: verify the serialized notes and
    /// persisted priorities before deriving the exact price from Polaris data.
    pub fn from_ingest_orders(
        base: AssetId,
        quote: AssetId,
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
        orders: &[IngestOrder],
    ) -> Result<Self, ClearingError> {
        let orders = orders
            .iter()
            .map(AdmittedPswap::from_ingest_order)
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_reference_prices(
            base,
            quote,
            base_price,
            quote_price,
            base_decimals,
            quote_decimals,
            orders,
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ClearingConfig {
    pub protocol_fee_ppm: u32,
    pub max_orders_per_side: usize,
    pub max_intervals_per_row: usize,
    pub max_total_intervals: usize,
}

impl Default for ClearingConfig {
    fn default() -> Self {
        Self {
            protocol_fee_ppm: 0,
            max_orders_per_side: DEFAULT_MAX_ORDERS_PER_SIDE,
            max_intervals_per_row: DEFAULT_MAX_INTERVALS_PER_ROW,
            max_total_intervals: DEFAULT_MAX_TOTAL_INTERVALS_PER_SIDE,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ClearingOutcome {
    Accepted(SettlementPlan),
    Skipped(SkipReason),
}

#[derive(Clone, Debug)]
pub enum SkipReason {
    NoEligibleCross,
    NoPositiveCross,
    ResourceLimit(ResourceLimitKind),
    Insolvent {
        base_shortfall: Wide,
        quote_shortfall: Wide,
        candidate: CandidatePlan,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceLimitKind {
    OrdersPerSide,
    IntervalsPerRow,
    TotalIntervals,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidOrderReason {
    ZeroAmount,
    UnsupportedAssetDirection,
    MissingPriority,
    MalformedRawNote,
    InconsistentIngestOrder,
}

#[derive(Debug, Error)]
pub enum ClearingError {
    #[error("invalid clearing configuration")]
    InvalidConfig,
    #[error("invalid clearing price")]
    InvalidPrice,
    #[error("invalid oracle price or token decimals")]
    InvalidOraclePrice,
    #[error("invalid order {note_id}: {reason:?}")]
    InvalidOrder {
        note_id: NoteId,
        reason: InvalidOrderReason,
    },
    #[error("invalid PSWAP note {note_id}: {source}")]
    InvalidPswap {
        note_id: NoteId,
        #[source]
        source: NoteError,
    },
    #[error("duplicate note ID {0}")]
    DuplicateNoteId(NoteId),
    #[error("duplicate FIFO sequence {0} within one side of the book")]
    DuplicatePriority(PrioritySeq),
    #[error("envelope resource limit: {0:?}")]
    ResourceLimit(ResourceLimitKind),
    #[error(transparent)]
    Asset(#[from] AssetError),
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
    #[error("internal invariant failed: {0}")]
    InternalInvariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Side {
    SellBase,
    BuyBase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Ratio {
    pub num: Wide,
    pub den: Wide,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReachableInterval {
    pub lo: Wide,
    pub hi: Wide,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedOrder {
    pub input_index: usize,
    pub side: Side,
    pub offered: FungibleAsset,
    pub requested: FungibleAsset,
    pub min_fill: AssetAmount,
    pub scale: Ratio,
    pub domain: ReachableInterval,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderExecution {
    pub input_index: usize,
    pub scaled: Wide,
    pub payment: FungibleAsset,
    pub release: FungibleAsset,
    /// Wide because the gross price improvement need not itself fit in a note.
    pub nominal_protocol_fee: Wide,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PairAmounts {
    pub base: Wide,
    pub quote: Wide,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolverAccruals {
    pub realized_protocol_fee: PairAmounts,
    pub rounding_surplus: PairAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePlan {
    pub clearing_price: ExactPrice,
    pub buyer_target_a: Wide,
    pub seller_target_b: Wide,
    pub executions: Vec<OrderExecution>,
    pub released: PairAmounts,
    pub paid: PairAmounts,
    pub nominal_protocol_fee: PairAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettlementPlan {
    pub candidate: CandidatePlan,
    pub accruals: SolverAccruals,
}

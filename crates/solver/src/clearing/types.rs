use miden_protocol::asset::FungibleAsset;
use miden_protocol::errors::{AssetError, NoteError};
use miden_protocol::note::NoteId;
use ruint::aliases::U256;
use thiserror::Error;

pub const PPM_DENOMINATOR: u32 = 1_000_000;
pub const DEFAULT_MAX_ORDERS_PER_SIDE: usize = 100;
pub const DEFAULT_MAX_INTERVALS_PER_ROW: usize = 1_000;
pub const DEFAULT_MAX_TOTAL_INTERVALS_PER_SIDE: usize = 50_000;

/// Quote-asset base units per one base-asset base unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPrice {
    pub(crate) quote_units: U256,
    pub(crate) base_units: U256,
}

/// An exact whole-token price in a common reference currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReferencePrice {
    pub(crate) numerator: U256,
    pub(crate) denominator: U256,
}

#[derive(Clone, Copy, Debug)]
pub struct ClearingConfig {
    pub protocol_fee_ppm: u32,
    pub max_orders_per_side: usize,
    pub max_intervals_per_row: usize,
    pub max_total_intervals: usize,
}

impl ClearingConfig {
    pub(crate) fn validate(&self) -> Result<(), ClearingError> {
        if self.protocol_fee_ppm >= PPM_DENOMINATOR
            || self.max_orders_per_side == 0
            || self.max_intervals_per_row == 0
            || self.max_total_intervals == 0
        {
            return Err(ClearingError::InvalidConfig);
        }
        Ok(())
    }
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
    Accepted(Box<SettlementPlan>),
    Skipped(SkipReason),
}

#[derive(Clone, Debug)]
pub enum SkipReason {
    NoEligibleCross,
    NoPositiveCross,
    ResourceLimit(ResourceLimitKind),
    Insolvent {
        base_shortfall: U256,
        quote_shortfall: U256,
        candidate: Box<CandidatePlan>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceLimitKind {
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
    DuplicatePriority(u64),
    #[error("envelope resource limit: {0:?}")]
    ResourceLimit(ResourceLimitKind),
    #[error(transparent)]
    Asset(#[from] AssetError),
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
    #[error("internal invariant failed: {0}")]
    InternalInvariant(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderExecution {
    pub order_id: NoteId,
    pub comparison_fill: U256,
    pub payment: FungibleAsset,
    pub release: FungibleAsset,
    /// Fee target before it is capped by the settlement's realized residual.
    pub protocol_fee_target: U256,
}

/// Batch totals may exceed the per-asset limit, so they cannot be AssetAmount.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PairAmounts {
    pub base: U256,
    pub quote: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolverAccruals {
    pub realized_protocol_fee: PairAmounts,
    pub rounding_surplus: PairAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePlan {
    pub clearing_price: ExactPrice,
    pub buyer_base_target: U256,
    pub seller_quote_target: U256,
    pub executions: Vec<OrderExecution>,
    pub released: PairAmounts,
    pub paid: PairAmounts,
    pub protocol_fee_target: PairAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettlementPlan {
    pub candidate: CandidatePlan,
    pub accruals: SolverAccruals,
}

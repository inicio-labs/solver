use miden_protocol::asset::FungibleAsset;
use miden_protocol::errors::{AssetError, NoteError};
use miden_protocol::note::NoteId;
use ruint::aliases::U256;
use thiserror::Error;

/// Quote-asset base units per one base-asset base unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchPrice {
    pub(crate) quote_units: U256,
    pub(crate) base_units: U256,
}

/// An exact whole-token price in a common reference currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReferencePrice {
    pub(crate) numerator: U256,
    pub(crate) denominator: U256,
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
    ResourceLimit,
    Insolvent {
        base_shortfall: u128,
        quote_shortfall: u128,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidOrderReason {
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
    #[error("envelope interval limit exceeded")]
    ResourceLimit,
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
    pub base: u128,
    pub quote: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolverAccruals {
    pub realized_protocol_fee: PairAmounts,
    pub rounding_surplus: PairAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePlan {
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

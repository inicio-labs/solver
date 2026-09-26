//! Exact, bounded PSWAP pair clearing at one frozen external price.

mod envelope;
mod matching;
mod math;
mod order;
mod settlement;
mod types;

pub use matching::{PairBatch, PairMatcher};
pub use order::Order;
pub use types::{
    CandidatePlan, ClearingConfig, ClearingError, ClearingOutcome, ExactPrice, InvalidOrderReason,
    OrderExecution, PairAmounts, ReferencePrice, ResourceLimitKind, SettlementPlan, SkipReason,
    SolverAccruals, PPM_DENOMINATOR,
};

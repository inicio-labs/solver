//! Exact, bounded PSWAP pair clearing at one frozen external price.

mod config;
mod envelope;
mod matching;
mod math;
mod order;
mod settlement;
mod types;

pub use config::{ClearingConfig, PPM_DENOMINATOR};
pub use matching::{PairBatch, PairMatcher};
pub(crate) use math::{checked_mul, mul_div_ceil, mul_div_floor, ppm_floor};
pub use order::Order;
pub(crate) use order::{eligible_units, MatchOrder, OrderKey, OrderSide};
pub use types::{
    BatchPrice, CandidatePlan, ClearingError, ClearingOutcome, InvalidOrderReason, OrderExecution,
    PairAmounts, SettlementPlan, SkipReason, SolverAccruals,
};

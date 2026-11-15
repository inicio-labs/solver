//! Executor module. Implementation in [`executor`], errors in [`error`];
//! this file only wires the submodules and re-exports their public surface
//! so callers keep using `crate::executor::{...}`.

mod error;
mod executor;
pub(crate) mod metrics;
pub use error::{BatchError, ExecResult, ExecutorError};
pub use executor::*;

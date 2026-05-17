//! Executor module. Implementation in [`executor`]; this file only wires the
//! submodule and re-exports its public surface so callers keep using
//! `crate::executor::{...}`.

mod executor;
pub use executor::*;

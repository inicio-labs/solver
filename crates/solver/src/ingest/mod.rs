//! Note ingestion module. Implementation in [`ingest`]; this file only
//! wires the submodule and re-exports its public surface so callers keep
//! using `crate::ingest::{...}`.

mod ingest;
pub use ingest::*;

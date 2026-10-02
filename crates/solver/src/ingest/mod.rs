//! Note ingestion module. Implementation in [`ingest`], errors in [`error`];
//! this file only wires the submodules and re-exports their public surface
//! so callers keep using `crate::ingest::{...}`.

mod error;
mod ingest;
pub use error::{ChainError, ChainResult, IngestError};
pub use ingest::*;

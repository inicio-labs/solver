//! Note ingestion module. Implementation in [`ingest`], errors in [`error`];
//! this file only wires the submodules and re-exports their public surface
//! so callers keep using `crate::ingest::{...}`.

mod error;
mod ingest;
pub(crate) use error::client_error_detail;
pub use error::{ChainError, ChainResult, IngestError};
pub use ingest::*;

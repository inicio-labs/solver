//! Errors of the maker gateway.

use std::sync::Arc;

use thiserror::Error;

use crate::db::DbError;

/// Why a maker command got no durable reply. Every case is safe to retry
/// with the same request ID: an exact retry returns the stored reply.
#[derive(Debug, Clone, Error)]
pub enum IntakeError {
    #[error("maker intake queue is full")]
    Busy,
    #[error("maker intake stopped")]
    Stopped,
    #[error("maker command outcome is unknown; retry with the same request ID after recovery")]
    OutcomeUnknown(#[source] Arc<DbError>),
}

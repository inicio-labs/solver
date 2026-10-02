//! Matcher errors.

use thiserror::Error;
use tokio::sync::oneshot;

use crate::clearing::ClearingError;

/// Why the matcher stopped. Every variant requires a whole-solver restart.
#[derive(Debug, Error)]
pub enum MatcherError {
    #[error("invalid clearing configuration")]
    Config(#[from] ClearingError),
    #[error("startup reconciliation stopped before sending the initial book")]
    BootstrapLost(#[from] oneshot::error::RecvError),
    #[error("ingestion stopped: book update channel closed")]
    IngestStopped,
    #[error("executor stopped: execution batch receiver closed")]
    ExecutorStopped,
    #[error("RFQ routing failed")]
    Routing(#[source] anyhow::Error),
}

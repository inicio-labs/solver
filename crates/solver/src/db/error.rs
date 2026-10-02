//! Errors of the PostgreSQL application database layer.
//!
//! [`DbError::is_fatal`] separates the failures that end this solver process
//! (it can no longer prove it is the only writer) from ordinary errors that a
//! caller handles by retrying or re-feeding.

use std::num::TryFromIntError;
use std::time::Duration;

use miden_protocol::utils::serde::DeserializationError;
use thiserror::Error;

use crate::types::{OrderError, SettlementError};

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug, Error)]
pub enum DbError {
    // ---- connections and the pool ----
    #[error("cannot connect to PostgreSQL")]
    Connect(#[from] diesel::ConnectionError),
    #[error("PostgreSQL query failed")]
    Query(#[from] diesel::result::Error),
    #[error("PostgreSQL read pool checkout failed")]
    ReadPool(#[from] diesel::r2d2::PoolError),
    #[error("PostgreSQL read pool stayed busy for {0:?}")]
    ReadPoolBusy(Duration),
    #[error("PostgreSQL read pool is closed")]
    ReadPoolClosed,
    #[error("PostgreSQL read_pool_size must be at least one")]
    InvalidReadPoolSize,
    #[error("PostgreSQL {operation} did not finish within {deadline:?}")]
    Deadline {
        operation: &'static str,
        deadline: Duration,
    },
    #[error("PostgreSQL worker stopped")]
    WorkerStopped(#[from] tokio::task::JoinError),
    #[error("matcher stopped: book update receiver closed")]
    MatcherStopped,

    // ---- startup and ownership ----
    #[error(
        "PostgreSQL reader targets {reader} but the writer targets {writer}; \
         both URLs must reach the same database and schema"
    )]
    ReaderTargetMismatch { writer: String, reader: String },
    #[error("another solver already owns this PostgreSQL application database")]
    AlreadyOwned,
    #[error("PostgreSQL writer is no longer safe; restart the whole solver")]
    WriterUnsafe,
    #[error("PostgreSQL writer stayed busy for {0:?}; restart the whole solver")]
    WriterBusy(Duration),
    #[error("PostgreSQL writer lost its ownership lock")]
    OwnershipLost,
    #[error("another solver holds the ownership lock after the writer session was lost")]
    LockTakenAfterDisconnect,
    #[error(
        "another solver owned the database while the writer was disconnected \
         (owner epoch {ours} -> {current})"
    )]
    OwnerEpochMoved { ours: i64, current: i64 },
    #[error("PostgreSQL writer did not reconnect within {window:?}")]
    ReconnectTimedOut {
        window: Duration,
        #[source]
        last: Box<DbError>,
    },
    #[error("cannot determine whether transaction {xid} committed (status {status:?})")]
    CommitOutcomeUnknown { xid: String, status: Option<String> },
    #[error("PostgreSQL writer session was lost before commit; reconnected, nothing was written")]
    LostBeforeCommit(#[source] Box<DbError>),
    #[error(
        "PostgreSQL writer session was lost during commit; reconnected, \
         the commit did not take effect"
    )]
    CommitDidNotApply(#[source] Box<DbError>),

    // ---- migrations ----
    #[error("PostgreSQL migration history is unreadable; run migrate-db first")]
    MissingMigrationHistory(#[source] diesel::result::Error),
    #[error(
        "PostgreSQL schema mismatch: missing migrations {missing:?}, unsupported \
         applied migrations {unsupported:?}; run migrate-db or use a compatible solver binary"
    )]
    SchemaMismatch {
        missing: Vec<String>,
        unsupported: Vec<String>,
    },
    #[error("PostgreSQL migration failed: {0}")]
    Migration(String),

    // ---- stored and incoming data ----
    #[error("PostgreSQL sync cursor row is missing")]
    MissingSyncCursor,
    #[error("sync height {0} exceeds PostgreSQL BIGINT")]
    BlockOutOfRange(u64),
    #[error("token ID is not a canonical serialized account ID")]
    InvalidTokenId,
    #[error("token decimals {0} do not fit a u8")]
    InvalidDecimals(i32),
    #[error("invalid settlement status transition {from} -> {to}")]
    InvalidTransition { from: String, to: &'static str },
    #[error("settlement attempt missing during {0} transition")]
    MissingAttempt(&'static str),
    #[error("corrupt stored data: {0}")]
    Corrupt(&'static str),
    #[error("stored bytes do not decode")]
    Decode(#[from] DeserializationError),
    #[error("integer out of range")]
    OutOfRange(#[from] TryFromIntError),
    #[error(transparent)]
    InvalidOrder(#[from] OrderError),
    #[error(transparent)]
    Settlement(#[from] SettlementError),
}

impl DbError {
    /// Whether this error means the solver must stop: it can no longer prove
    /// it is the only writer, or a write's outcome is unknown.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::WriterUnsafe
                | Self::WriterBusy(_)
                | Self::OwnershipLost
                | Self::LockTakenAfterDisconnect
                | Self::OwnerEpochMoved { .. }
                | Self::ReconnectTimedOut { .. }
                | Self::CommitOutcomeUnknown { .. }
                | Self::WorkerStopped(_)
        ) || matches!(self, Self::Deadline { operation, .. } if *operation == "write")
    }
}

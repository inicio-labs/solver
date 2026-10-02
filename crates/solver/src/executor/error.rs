//! Executor errors. Most are handled per batch or settlement (hand back,
//! pause into verification mode, retry next tick); the pipeline stops only
//! on a fatal database error (`DbError::is_fatal`).

use miden_client::transaction::TransactionRequestError;
use miden_client::ClientError;
use miden_protocol::crypto::utils::DeserializationError;
use miden_protocol::errors::{AssetError, NoteError};
use miden_protocol::note::NoteId;
use thiserror::Error;

use crate::db::DbError;
use crate::ingest::ChainError;
use crate::types::{SettlementError, TokenId};

pub type ExecResult<T> = Result<T, ExecutorError>;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Chain(#[from] ChainError),
    /// Boxed: `ClientError` alone would make every executor `Result` large.
    #[error(transparent)]
    Client(Box<ClientError>),
    /// This batch cannot be built as matched; its orders go back.
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error(transparent)]
    Settlement(#[from] SettlementError),
    #[error("stored settlement does not decode")]
    Decode(#[from] DeserializationError),
    #[error(
        "solver fee-asset balance {have} is below the {need} one settlement may cost \
         (fee faucet {faucet}); fund the solver account"
    )]
    InsufficientFee {
        have: u64,
        need: u64,
        faucet: TokenId,
    },
    #[error("submit cancelled during backoff")]
    Cancelled,
}

/// Why a matched batch cannot be turned into a settlement transaction.
#[derive(Debug, Error)]
pub enum BatchError {
    #[error("invalid PSWAP note in batch")]
    Note(#[from] NoteError),
    #[error("invalid asset in batch")]
    Asset(#[from] AssetError),
    #[error("cannot build the transaction request")]
    Request(#[from] TransactionRequestError),
    #[error("zero fill for note {0}")]
    ZeroFill(NoteId),
    #[error("insolvent batch: token {token} has a deficit of {deficit}")]
    Insolvent { token: TokenId, deficit: u128 },
    #[error("surplus of token {token} does not fit a u64: {surplus}")]
    SurplusOverflow { token: TokenId, surplus: i128 },
}

impl ExecutorError {
    /// Whether the solver must stop (see `DbError::is_fatal`).
    pub(super) fn is_fatal(&self) -> bool {
        matches!(self, Self::Db(error) if error.is_fatal())
    }
}

/// `From<Source> for Outer` through an inner error enum, so `?` lifts a
/// source error straight into the right nested variant.
macro_rules! from_nested {
    ($outer:ident :: $variant:ident ( $inner:ident ) <= $($source:ty),+ $(,)?) => {
        $(
            impl From<$source> for $outer {
                fn from(error: $source) -> Self {
                    Self::$variant($inner::from(error))
                }
            }
        )+
    };
}

from_nested!(
    ExecutorError::Batch(BatchError) <= NoteError,
    AssetError,
    TransactionRequestError
);

impl From<ClientError> for ExecutorError {
    fn from(error: ClientError) -> Self {
        Self::Client(Box::new(error))
    }
}

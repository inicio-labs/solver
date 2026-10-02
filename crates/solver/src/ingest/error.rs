//! Ingest errors.

use miden_client::rpc::RpcError;
use miden_client::store::NoteRecordError;
use miden_client::ClientError;
use miden_protocol::errors::AssetError;
use miden_protocol::note::NoteId;
use thiserror::Error;

use crate::db::DbError;

pub type ChainResult<T> = Result<T, ChainError>;

/// Errors from the Miden node or the local Miden client store.
#[derive(Debug, Error)]
pub enum ChainError {
    /// Boxed: `ClientError` alone would make every chain `Result` large.
    #[error(transparent)]
    Client(Box<ClientError>),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("invalid asset for a note tag")]
    Asset(#[from] AssetError),
    #[error("stored client note does not convert to a note")]
    NoteRecord(#[from] NoteRecordError),
    #[error("synced note {0} is missing from the client store")]
    MissingSyncedNote(NoteId),
    #[cfg(test)]
    #[error("test chain failure: {0}")]
    Test(&'static str),
}

impl ChainError {
    /// A node RPC failure: the same request may succeed on a later tick.
    pub fn is_rpc(&self) -> bool {
        match self {
            Self::Rpc(_) => true,
            Self::Client(error) => matches!(**error, ClientError::RpcError(_)),
            _ => false,
        }
    }
}

impl From<ClientError> for ChainError {
    fn from(error: ClientError) -> Self {
        Self::Client(Box::new(error))
    }
}

/// Errors of startup recovery: reading the client, or writing PostgreSQL.
#[derive(Debug, Error)]
pub enum IngestError {
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Db(#[from] DbError),
}

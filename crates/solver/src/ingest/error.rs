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

/// An RPC error for logs, with what the node said. Its `Display` names the
/// endpoint and the gRPC status, and the node's message when it is typed;
/// otherwise the message is only in its source (the gRPC status).
pub(crate) fn rpc_error_detail(rpc: &RpcError) -> String {
    match rpc {
        RpcError::RequestError {
            endpoint_error: None,
            source: Some(source),
            ..
        } => format!("{rpc}: {source}"),
        _ => rpc.to_string(),
    }
}

/// A client error for logs: `ClientError` displays only "RPC error" for a
/// failed RPC.
pub(crate) fn client_error_detail(error: &ClientError) -> String {
    match error {
        ClientError::RpcError(rpc) => rpc_error_detail(rpc),
        other => other.to_string(),
    }
}

impl ChainError {
    /// For logs: the inner RPC error and what the node said, where `Display`
    /// would print only "RPC error".
    pub(crate) fn detail(&self) -> String {
        match self {
            Self::Client(error) => client_error_detail(error),
            Self::Rpc(rpc) => rpc_error_detail(rpc),
            other => other.to_string(),
        }
    }

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

#[cfg(test)]
mod tests {
    use miden_client::rpc::{AddTransactionError, EndpointError, GrpcError, RpcEndpoint, RpcError};
    use miden_client::ClientError;

    use super::ChainError;

    /// `ClientError` alone displays only "RPC error"; the detail carries the
    /// endpoint and what the node said.
    #[test]
    fn the_detail_carries_what_the_node_said() {
        let rpc = RpcError::RequestError {
            endpoint: RpcEndpoint::GetNotesById,
            error_kind: GrpcError::InvalidArgument,
            endpoint_error: None,
            source: Some(Box::new(std::io::Error::other("note decoding failed"))),
        };
        let client = ChainError::Client(Box::new(ClientError::RpcError(rpc)));
        assert_eq!(client.to_string(), "RPC error");
        let detail = client.detail();
        assert!(detail.ends_with(": note decoding failed"), "{detail}");

        let typed = ChainError::Rpc(RpcError::RequestError {
            endpoint: RpcEndpoint::SubmitProvenTx,
            error_kind: GrpcError::InvalidArgument,
            endpoint_error: Some(EndpointError::AddTransaction(
                AddTransactionError::StateConflict {
                    message: "nullifiers already exist".into(),
                },
            )),
            source: None,
        });
        assert!(typed.detail().contains("nullifiers already exist"));
    }
}

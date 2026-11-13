//! Settlement counters for `/metrics`. They change nothing; they show how
//! often the node refuses or drops our settlements, the baseline for letting
//! maker orders trade before their note is confirmed.

use std::sync::atomic::{AtomicU64, Ordering};

use miden_client::rpc::{AddTransactionError, EndpointError, RpcEndpoint, RpcError};
use miden_client::ClientError;
use strum::{EnumCount, IntoEnumIterator};

use super::refusal::ConflictKind;

static REFUSALS: [[AtomicU64; RefusalAttempt::COUNT]; RefusalCode::COUNT] =
    [const { [const { AtomicU64::new(0) }; RefusalAttempt::COUNT] }; RefusalCode::COUNT];
/// Indexed by whether the settlement spent an unconfirmed maker order.
static DROPPED: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static BOUNCED: [AtomicU64; ReleaseCause::COUNT] =
    [const { AtomicU64::new(0) }; ReleaseCause::COUNT];
static CONFLICTS: [AtomicU64; ConflictKind::COUNT] =
    [const { AtomicU64::new(0) }; ConflictKind::COUNT];

/// Why a submission of ours was refused, by the node's error code.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumCount, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum RefusalCode {
    /// Code 1: expired before the node took it.
    Expired,
    /// Code 2: conflicts with the node's state (notes spent or unknown,
    /// duplicate outputs, account state).
    StateConflict,
    /// Code 3: the mempool is full.
    Capacity,
    /// Codes 4 to 6: the fee checks.
    Fee,
    /// Code 8: failed against committed state, e.g. a note already spent.
    AuthFailed,
    /// Code 0, 7 or a code this client does not know.
    OtherCode,
    /// The node refused the submission without a typed reason: a bad proof,
    /// a validator rejection, a stale encryption key.
    NoDetails,
    /// Failed on our side, or on another call before the submission was
    /// sent (e.g. fetching the node's encryption key).
    Local,
}

/// Whether the refused submission was the first one of that transaction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumCount, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum RefusalAttempt {
    First,
    /// A resubmission after an unknown outcome or a restart. The first copy
    /// may already be at the node, so this refusal may refuse a duplicate.
    Resubmission,
}

/// Why a failed settlement released its inputs.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumCount, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ReleaseCause {
    /// The node refused it at submission.
    Refused,
    /// The node accepted it (the client recorded the submission), but it never
    /// reached a block.
    Dropped,
    /// It was never recorded as accepted, and its validity window passed
    /// unseen. This includes a submission the node accepted while the client
    /// failed to record it, so `Dropped` is a lower bound.
    Expired,
}

/// The node's reason for refusing a submission.
pub(crate) fn refusal_code(error: &ClientError) -> RefusalCode {
    let ClientError::RpcError(RpcError::RequestError {
        endpoint,
        endpoint_error,
        ..
    }) = error
    else {
        return RefusalCode::Local;
    };
    if !matches!(
        endpoint,
        RpcEndpoint::SubmitProvenTx | RpcEndpoint::SubmitProvenBatch
    ) {
        return RefusalCode::Local;
    }
    match endpoint_error {
        Some(EndpointError::AddTransaction(error)) => match error {
            AddTransactionError::Expired => RefusalCode::Expired,
            AddTransactionError::StateConflict { .. } => RefusalCode::StateConflict,
            AddTransactionError::CapacityExceeded => RefusalCode::Capacity,
            AddTransactionError::MissingFee
            | AddTransactionError::ConsumesInflightFeeNotes
            | AddTransactionError::InvalidFeeAsset => RefusalCode::Fee,
            AddTransactionError::AuthenticationFailed { .. } => RefusalCode::AuthFailed,
            AddTransactionError::Internal
            | AddTransactionError::BatchIdMismatch
            | AddTransactionError::Unknown { .. } => RefusalCode::OtherCode,
        },
        _ => RefusalCode::NoDetails,
    }
}

/// A refusal as text for logs, with what the node said. `ClientError`
/// displays only "RPC error"; the inner error names the endpoint and the gRPC
/// status, and carries the node's message when it is typed, otherwise its
/// source (the gRPC status) does.
pub(crate) fn refusal_detail(error: &ClientError) -> String {
    match error {
        ClientError::RpcError(rpc) => match rpc {
            RpcError::RequestError {
                endpoint_error: None,
                source: Some(source),
                ..
            } => format!("{rpc}: {source}"),
            _ => rpc.to_string(),
        },
        other => other.to_string(),
    }
}

pub(crate) fn record_refusal(code: RefusalCode, attempt: RefusalAttempt) {
    REFUSALS[code as usize][attempt as usize].fetch_add(1, Ordering::Relaxed);
}

/// A release that committed and returned `inputs` parents to the book,
/// before the maker liveness check. Called once per committed release, so a
/// retried release counts once. `unconfirmed`: the settlement spent a maker
/// order that went live at accept and whose note was not seen yet.
pub(crate) fn record_release(cause: ReleaseCause, inputs: usize, unconfirmed: bool) {
    BOUNCED[cause as usize].fetch_add(inputs as u64, Ordering::Relaxed);
    if cause == ReleaseCause::Dropped {
        DROPPED[usize::from(unconfirmed)].fetch_add(1, Ordering::Relaxed);
    }
}

/// A first refusal that was a state conflict (code 2), by what it was about.
pub(crate) fn record_conflict(kind: ConflictKind) {
    CONFLICTS[kind as usize].fetch_add(1, Ordering::Relaxed);
}

/// Refused submissions, per node error code and attempt.
pub(crate) fn refusals() -> impl Iterator<Item = (&'static str, &'static str, u64)> {
    RefusalCode::iter().flat_map(|code| {
        RefusalAttempt::iter().map(move |attempt| {
            (
                code.into(),
                attempt.into(),
                REFUSALS[code as usize][attempt as usize].load(Ordering::Relaxed),
            )
        })
    })
}

/// Settlements the node accepted that never landed, by whether they spent
/// an unconfirmed maker order (`"true"`/`"false"`).
pub(crate) fn dropped() -> impl Iterator<Item = (&'static str, u64)> {
    [("false", 0), ("true", 1)]
        .into_iter()
        .map(|(label, index)| (label, DROPPED[index].load(Ordering::Relaxed)))
}

/// First refusals that were state conflicts, per kind.
pub(crate) fn conflicts() -> impl Iterator<Item = (&'static str, u64)> {
    ConflictKind::iter().map(|kind| {
        (
            kind.into(),
            CONFLICTS[kind as usize].load(Ordering::Relaxed),
        )
    })
}

/// Inputs released by failed settlements, per cause.
pub(crate) fn bounced() -> impl Iterator<Item = (&'static str, u64)> {
    ReleaseCause::iter().map(|cause| {
        (
            cause.into(),
            BOUNCED[cause as usize].load(Ordering::Relaxed),
        )
    })
}

#[cfg(test)]
mod tests {
    use miden_client::rpc::GrpcError;

    use super::*;

    fn refused_at(endpoint: RpcEndpoint, error: Option<AddTransactionError>) -> ClientError {
        ClientError::RpcError(RpcError::RequestError {
            endpoint,
            error_kind: GrpcError::InvalidArgument,
            endpoint_error: error.map(EndpointError::AddTransaction),
            source: None,
        })
    }

    fn refused(error: Option<AddTransactionError>) -> ClientError {
        refused_at(RpcEndpoint::SubmitProvenTx, error)
    }

    #[test]
    fn refusals_are_classified_by_the_node_code() {
        let unknown = "transaction conflicts with current mempool state\ncaused by: \
                       unauthenticated input notes are unknown: [NoteId(Word([1, 2, 3, 4]))]";
        let cases = [
            (
                refused(Some(AddTransactionError::StateConflict {
                    message: unknown.into(),
                })),
                RefusalCode::StateConflict,
            ),
            (
                refused(Some(AddTransactionError::AuthenticationFailed {
                    message: "nullifiers already exist".into(),
                })),
                RefusalCode::AuthFailed,
            ),
            (
                refused(Some(AddTransactionError::Expired)),
                RefusalCode::Expired,
            ),
            (
                refused(Some(AddTransactionError::CapacityExceeded)),
                RefusalCode::Capacity,
            ),
            (
                refused(Some(AddTransactionError::MissingFee)),
                RefusalCode::Fee,
            ),
            (
                refused(Some(AddTransactionError::ConsumesInflightFeeNotes)),
                RefusalCode::Fee,
            ),
            (
                refused(Some(AddTransactionError::InvalidFeeAsset)),
                RefusalCode::Fee,
            ),
            (
                refused(Some(AddTransactionError::Internal)),
                RefusalCode::OtherCode,
            ),
            (
                refused(Some(AddTransactionError::BatchIdMismatch)),
                RefusalCode::OtherCode,
            ),
            (
                refused(Some(AddTransactionError::Unknown {
                    code: 42,
                    message: "new".into(),
                })),
                RefusalCode::OtherCode,
            ),
            (refused(None), RefusalCode::NoDetails),
            (
                refused_at(RpcEndpoint::SubmitProvenBatch, None),
                RefusalCode::NoDetails,
            ),
            // A failed call before the submission was sent is ours, not the
            // node's answer to it.
            (
                refused_at(RpcEndpoint::GetTransactionEncryptionKey, None),
                RefusalCode::Local,
            ),
            (
                ClientError::RpcError(RpcError::InvalidNodeEndpoint("x".into())),
                RefusalCode::Local,
            ),
            (
                ClientError::RecencyConditionError("stale"),
                RefusalCode::Local,
            ),
        ];
        for (error, code) in cases {
            assert_eq!(refusal_code(&error), code, "{error:?}");
        }
    }

    #[test]
    fn refusal_detail_carries_the_node_text() {
        let typed = refused(Some(AddTransactionError::StateConflict {
            message: "unauthenticated input notes are unknown: [NoteId(..)]".into(),
        }));
        let detail = refusal_detail(&typed);
        assert!(
            detail.contains("unauthenticated input notes are unknown"),
            "{detail}"
        );
        assert!(detail.contains("submit_proven_transaction"), "{detail}");

        // Untyped: the node's text is only in the source status.
        let untyped = ClientError::RpcError(RpcError::RequestError {
            endpoint: RpcEndpoint::SubmitProvenTx,
            error_kind: GrpcError::InvalidArgument,
            endpoint_error: None,
            source: Some(Box::new(std::io::Error::other(
                "Invalid proof for transaction",
            ))),
        });
        assert!(refusal_detail(&untyped).ends_with(": Invalid proof for transaction"));
    }
}

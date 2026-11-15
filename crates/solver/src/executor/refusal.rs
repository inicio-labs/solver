//! What the node said when it refused a settlement (ADR 0008). The answer
//! decides where the settlement's unconfirmed maker orders go: back to the
//! book, or out of it until their note is in a block.
//!
//! The node reports a state conflict (code 2) as text. These markers match
//! node 0.17.0 to 0.18.2 (`block-producer/src/errors.rs`); a node that words
//! them differently makes every unconfirmed order wait, which is safe, and
//! shows up as `unreadable` or `unknown_notes_unmatched` in `/metrics`.

use miden_client::rpc::{AddTransactionError, EndpointError, RpcError};
use miden_client::ClientError;
use miden_protocol::note::Note;

use super::metrics::{refusal_code, RefusalCode};
use crate::db::postgres_db::NodeAnswer;

const UNKNOWN_NOTES: &str = "unauthenticated input notes are unknown";
const ACCOUNT_MISMATCH: &str = "does not match the current commitment";
const NULLIFIERS_EXIST: &str = "nullifiers already exist";
const OUTPUTS_EXIST: &str = "output notes already exist";

/// What a state conflict (code 2) was about, for `/metrics`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumCount, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ConflictKind {
    /// Unknown unauthenticated input notes, naming at least one of ours.
    UnknownNotes,
    /// Unknown input notes, but no note ID in the text is one of ours: the
    /// node may have changed how it prints them.
    UnknownNotesUnmatched,
    /// A note already spent by a transaction waiting in the mempool.
    MempoolNullifiers,
    /// Our account's state differs from the node's. The node checks this
    /// after the notes, so every note was known.
    AccountMismatch,
    /// An output note that already exists.
    DuplicateOutputs,
    /// None of the above.
    Unreadable,
}

/// The node's answer to a refused submission of a settlement spending
/// `parents`, and what kind of state conflict it was, if one.
pub(crate) fn node_answer(
    error: &ClientError,
    parents: &[Note],
) -> (NodeAnswer, Option<ConflictKind>) {
    if refusal_code(error) == RefusalCode::Local {
        // The submission was never sent.
        return (NodeAnswer::NotSent, None);
    }
    let ClientError::RpcError(RpcError::RequestError {
        endpoint_error:
            Some(EndpointError::AddTransaction(AddTransactionError::StateConflict { message })),
        ..
    }) = error
    else {
        // Another code, or no details: the node may have stopped before it
        // checked the notes.
        return (NodeAnswer::Unclear, None);
    };
    if message.contains(UNKNOWN_NOTES) {
        // The node prints note IDs with their Debug form.
        let named: std::collections::HashSet<_> = parents
            .iter()
            .map(Note::id)
            .filter(|id| message.contains(&format!("{id:?}")))
            .collect();
        let kind = if named.is_empty() {
            ConflictKind::UnknownNotesUnmatched
        } else {
            ConflictKind::UnknownNotes
        };
        return (NodeAnswer::UnknownNotes(named), Some(kind));
    }
    if message.contains(ACCOUNT_MISMATCH) {
        return (
            NodeAnswer::AccountMismatch,
            Some(ConflictKind::AccountMismatch),
        );
    }
    let kind = if message.contains(NULLIFIERS_EXIST) {
        ConflictKind::MempoolNullifiers
    } else if message.contains(OUTPUTS_EXIST) {
        ConflictKind::DuplicateOutputs
    } else {
        ConflictKind::Unreadable
    };
    (NodeAnswer::Unclear, Some(kind))
}

#[cfg(test)]
mod tests {
    use miden_client::rpc::{GrpcError, RpcEndpoint};

    use super::*;
    use crate::gateway::intake::tests::pswap_note;

    fn conflict(message: String) -> ClientError {
        ClientError::RpcError(RpcError::RequestError {
            endpoint: RpcEndpoint::SubmitProvenTx,
            error_kind: GrpcError::InvalidArgument,
            endpoint_error: Some(EndpointError::AddTransaction(
                AddTransactionError::StateConflict { message },
            )),
            source: None,
        })
    }

    /// The node's report of a state conflict: its top error, then the cause,
    /// as node 0.17.6 and 0.18.0 word them (`block-producer/src/errors.rs`),
    /// written out here rather than taken from the parser's constants.
    fn report(cause: &str) -> String {
        format!("transaction conflicts with current mempool state\ncaused by: {cause}")
    }

    #[test]
    fn unknown_notes_name_the_missing_parents() {
        let (a, b, c) = (pswap_note(1), pswap_note(2), pswap_note(3));
        let parents = [a.clone(), b.clone(), c.clone()];
        let text = report(&format!(
            "unauthenticated input notes are unknown: [{:?}, {:?}]",
            a.id(),
            c.id()
        ));
        assert!(text.contains("NoteId("), "Debug form changed: {text}");
        let (answer, kind) = node_answer(&conflict(text), &parents);
        assert_eq!(
            answer,
            NodeAnswer::UnknownNotes([a.id(), c.id()].into_iter().collect())
        );
        assert_eq!(kind, Some(ConflictKind::UnknownNotes));

        // IDs printed another way match nothing.
        let (answer, kind) = node_answer(
            &conflict(report(&format!(
                "unauthenticated input notes are unknown: [0x{}]",
                a.id().to_hex()
            ))),
            &parents,
        );
        assert_eq!(answer, NodeAnswer::UnknownNotes(Default::default()));
        assert_eq!(kind, Some(ConflictKind::UnknownNotesUnmatched));
    }

    #[test]
    fn other_conflicts_and_codes_are_classified() {
        let parents = [pswap_note(1)];
        let cases = [
            (
                report("initial account commitment 0x1 does not match the current commitment 0x2 for account 0x3"),
                NodeAnswer::AccountMismatch,
                Some(ConflictKind::AccountMismatch),
            ),
            (
                report("nullifiers already exist: [0x1]"),
                NodeAnswer::Unclear,
                Some(ConflictKind::MempoolNullifiers),
            ),
            (
                report("output notes already exist: [NoteId(..)]"),
                NodeAnswer::Unclear,
                Some(ConflictKind::DuplicateOutputs),
            ),
            (
                "something new".to_owned(),
                NodeAnswer::Unclear,
                Some(ConflictKind::Unreadable),
            ),
        ];
        for (text, answer, kind) in cases {
            assert_eq!(
                node_answer(&conflict(text.clone()), &parents),
                (answer, kind),
                "{text}"
            );
        }

        let refused = |error: Option<AddTransactionError>, endpoint| {
            ClientError::RpcError(RpcError::RequestError {
                endpoint,
                error_kind: GrpcError::InvalidArgument,
                endpoint_error: error.map(EndpointError::AddTransaction),
                source: None,
            })
        };
        for error in [
            Some(AddTransactionError::Expired),
            Some(AddTransactionError::CapacityExceeded),
            Some(AddTransactionError::ConsumesInflightFeeNotes),
            Some(AddTransactionError::AuthenticationFailed {
                message: "nullifiers already exist".into(),
            }),
            Some(AddTransactionError::Unknown {
                code: 42,
                message: "new".into(),
            }),
            None,
        ] {
            let error = refused(error, RpcEndpoint::SubmitProvenTx);
            assert_eq!(
                node_answer(&error, &parents),
                (NodeAnswer::Unclear, None),
                "{error:?}"
            );
        }
        // A call that failed before the submission was sent.
        assert_eq!(
            node_answer(
                &refused(None, RpcEndpoint::GetTransactionEncryptionKey),
                &parents
            ),
            (NodeAnswer::NotSent, None)
        );
    }

    /// Refusals a local node 0.18.0 sent the client the solver uses, captured
    /// by `tests/local_node_018.rs`. Re-capture after every node upgrade.
    #[test]
    fn refusals_captured_from_node_0_18_parse() {
        use miden_protocol::utils::serde::Deserializable;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/node-0.18-refusals.json"))
                .unwrap();
        let note = |key: &str, field: &str| {
            let bytes = hex::decode(fixture[key][field].as_str().unwrap()).unwrap();
            Note::read_from_bytes(&bytes).unwrap()
        };
        let message = |key: &str| fixture[key]["message"].as_str().unwrap().to_owned();
        let other = pswap_note(1);

        let unknown = note("unknown_notes", "unknown_note");
        let (answer, kind) = node_answer(
            &conflict(message("unknown_notes")),
            &[other.clone(), unknown.clone()],
        );
        assert_eq!(
            answer,
            NodeAnswer::UnknownNotes([unknown.id()].into_iter().collect())
        );
        assert_eq!(kind, Some(ConflictKind::UnknownNotes));

        // A note a waiting transaction already spends is refused by nullifier
        // (issue #69): every unconfirmed parent waits.
        let spent = note("mempool_nullifiers", "spent_note");
        let (answer, kind) = node_answer(&conflict(message("mempool_nullifiers")), &[spent, other]);
        assert_eq!(answer, NodeAnswer::Unclear);
        assert_eq!(kind, Some(ConflictKind::MempoolNullifiers));
    }
}

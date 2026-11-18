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
static SETTLEMENTS_CONFIRMED: AtomicU64 = AtomicU64::new(0);
static SETTLEMENTS_RELEASED: [AtomicU64; ReleaseCause::COUNT] =
    [const { AtomicU64::new(0) }; ReleaseCause::COUNT];
/// The fee balance and base fee at the last headroom check, and when (Unix
/// seconds; 0 until the first check).
static FEE_BALANCE: AtomicU64 = AtomicU64::new(0);
static FEE_BASE: AtomicU64 = AtomicU64::new(0);
static FEE_CHECKED_AT: AtomicU64 = AtomicU64::new(0);
static CONFLICTS: [AtomicU64; ConflictKind::COUNT] =
    [const { AtomicU64::new(0) }; ConflictKind::COUNT];
/// Indexed by stage, then by whether a node call failed (1) or not (0).
static FAILED_BEFORE_SUBMIT: [[AtomicU64; 2]; FailedStage::COUNT] =
    [const { [const { AtomicU64::new(0) }; 2] }; FailedStage::COUNT];
/// When the oldest work the executor holds was taken, in Unix milliseconds
/// (0: none): settlements waiting for their outcome, and batches held while
/// it verifies that it can settle. Published by the executor loop.
static OLDEST_HELD_MS: AtomicU64 = AtomicU64::new(0);
/// When the executor took the batch it is working on now (0: none).
static IN_HAND_MS: AtomicU64 = AtomicU64::new(0);

/// Where a settlement failed before it was submitted.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumCount, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum FailedStage {
    /// Its inputs or its request could not be put together.
    Build,
    /// Running the transaction failed.
    Execute,
    /// Proving it failed.
    Prove,
}

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

pub(crate) fn record_refusal(code: RefusalCode, attempt: RefusalAttempt) {
    REFUSALS[code as usize][attempt as usize].fetch_add(1, Ordering::Relaxed);
}

/// A release that committed and returned `inputs` parents to the book,
/// before the maker liveness check. Called once per committed release, so a
/// retried release counts once. `unconfirmed`: the settlement spent a maker
/// order that went live at accept and whose note was not seen yet.
pub(crate) fn record_release(cause: ReleaseCause, inputs: usize, unconfirmed: bool) {
    BOUNCED[cause as usize].fetch_add(inputs as u64, Ordering::Relaxed);
    SETTLEMENTS_RELEASED[cause as usize].fetch_add(1, Ordering::Relaxed);
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

/// A settlement that failed before it was submitted. `rpc`: a node call
/// failed, and the orders go straight back; otherwise the same composition
/// would fail the same way again.
pub(crate) fn record_failed_before_submit(stage: FailedStage, rpc: bool) {
    FAILED_BEFORE_SUBMIT[stage as usize][usize::from(rpc)].fetch_add(1, Ordering::Relaxed);
}

/// Settlements that failed before submission, per stage and cause (`rpc` or
/// `other`).
pub(crate) fn failed_before_submit() -> impl Iterator<Item = (&'static str, &'static str, u64)> {
    FailedStage::iter().flat_map(|stage| {
        [("other", 0), ("rpc", 1)]
            .into_iter()
            .map(move |(cause, index)| {
                (
                    stage.into(),
                    cause,
                    FAILED_BEFORE_SUBMIT[stage as usize][index].load(Ordering::Relaxed),
                )
            })
    })
}

/// When the oldest work the executor holds was taken (Unix ms), or `None`.
pub(crate) fn publish_oldest_held(since_ms: Option<u64>) {
    OLDEST_HELD_MS.store(since_ms.unwrap_or(0), Ordering::Relaxed);
}

/// The batch the executor is working on now, from when it took it, or `None`.
pub(crate) fn set_in_hand(since_ms: Option<u64>) {
    IN_HAND_MS.store(since_ms.unwrap_or(0), Ordering::Relaxed);
}

/// Seconds since the executor took the oldest work it has not finished, aged
/// at `now_ms`; 0 when it holds none.
pub(crate) fn oldest_unfinished_age_secs(now_ms: u64) -> u64 {
    unfinished_age_secs(
        OLDEST_HELD_MS.load(Ordering::Relaxed),
        IN_HAND_MS.load(Ordering::Relaxed),
        now_ms,
    )
}

fn unfinished_age_secs(held_ms: u64, in_hand_ms: u64, now_ms: u64) -> u64 {
    [held_ms, in_hand_ms]
        .into_iter()
        .filter(|since| *since != 0)
        .min()
        .map_or(0, |since| now_ms.saturating_sub(since) / 1000)
}

/// A settlement committed on chain and was confirmed in the database.
pub(crate) fn record_confirmed() {
    SETTLEMENTS_CONFIRMED.fetch_add(1, Ordering::Relaxed);
}

/// What the last fee headroom check saw.
pub(crate) fn record_fee(balance: u64, base_fee: u64, now_unix: u64) {
    FEE_BALANCE.store(balance, Ordering::Relaxed);
    FEE_BASE.store(base_fee, Ordering::Relaxed);
    FEE_CHECKED_AT.store(now_unix, Ordering::Relaxed);
}

pub(crate) fn settlements_confirmed() -> u64 {
    SETTLEMENTS_CONFIRMED.load(Ordering::Relaxed)
}

/// Settlements given back, one per settlement, by cause.
pub(crate) fn settlements_released() -> impl Iterator<Item = (&'static str, u64)> {
    ReleaseCause::iter().map(|cause| {
        (
            cause.into(),
            SETTLEMENTS_RELEASED[cause as usize].load(Ordering::Relaxed),
        )
    })
}

/// `(balance, base fee, checked at)`, or `None` before the first check. A
/// chain that charges no fee never records one: the check stops early.
pub(crate) fn fee() -> Option<(u64, u64, u64)> {
    let at = FEE_CHECKED_AT.load(Ordering::Relaxed);
    (at != 0).then(|| {
        (
            FEE_BALANCE.load(Ordering::Relaxed),
            FEE_BASE.load(Ordering::Relaxed),
            at,
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
    fn unfinished_age_counts_the_oldest_held_or_in_hand_work() {
        assert_eq!(unfinished_age_secs(0, 0, 50_000), 0);
        assert_eq!(unfinished_age_secs(20_000, 0, 50_000), 30);
        assert_eq!(unfinished_age_secs(0, 41_000, 50_000), 9);
        assert_eq!(unfinished_age_secs(20_000, 41_000, 50_000), 30);
        assert_eq!(unfinished_age_secs(41_000, 20_000, 50_000), 30);
        // A clock that went back never shows a negative age.
        assert_eq!(unfinished_age_secs(60_000, 0, 50_000), 0);
    }

    #[test]
    fn failures_before_submit_name_every_stage_and_cause() {
        let rows: Vec<_> = failed_before_submit().collect();
        assert_eq!(rows.len(), FailedStage::COUNT * 2);
        let count = |stage: &str, cause: &str| {
            failed_before_submit()
                .find(|(s, c, _)| *s == stage && *c == cause)
                .unwrap()
                .2
        };
        let before = count("prove", "rpc");
        record_failed_before_submit(FailedStage::Prove, true);
        assert_eq!(count("prove", "rpc"), before + 1);
    }

    /// The counters are process-wide. Only this test records a drop of a
    /// settlement with an unconfirmed maker input, and it returns no inputs,
    /// so it moves nothing the executor's release test compares (the
    /// `false` label and the inputs returned by drops).
    #[test]
    fn a_drop_is_labelled_by_whether_it_spent_an_unconfirmed_order() {
        let count = |label| dropped().find(|(l, _)| *l == label).unwrap().1;
        let before = count("true");
        record_release(ReleaseCause::Dropped, 0, true);
        record_release(ReleaseCause::Refused, 0, true);
        assert_eq!(count("true"), before + 1);
    }
}

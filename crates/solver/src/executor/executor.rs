use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use consume_script::ConsumeAssetScript;
use miden_client::store::TransactionFilter;
use miden_client::transaction::{
    DiscardCause, NoteArgs, TransactionRequest, TransactionRequestBuilder, TransactionRequestError,
    TransactionResult, TransactionStatus,
};
use miden_client::ClientError;
use miden_client::{keystore::FilesystemKeyStore, Client};
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    block::BlockNumber,
    crypto::utils::{Deserializable, DeserializationError, Serializable, SliceReader},
    errors::{AssetError, NoteError},
    note::{Note, NoteId, NoteRecipient},
    transaction::{InputNote, TransactionId},
};
use miden_standards::note::{NoteConsumptionStatus, P2idNote, P2ideNote, PswapNote};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::db::postgres_models::{SettlementAttemptRow, SettlementInputRow, SettlementStatus};
use crate::db::{self, DbError, DbPool};
use crate::ingest::{ChainError, MidenClient, MidenClientAdapter};
use crate::swap_eta::SettlementStats;
use crate::types::{BookOrder, BookUpdate, ExecutionBatch, SettlementError, TokenId};

// ── Backoff knobs ──────────────────────────────────────────────────────────

/// Maximum number of retries after an indeterminate submission. The first
/// submit is "attempt 0," so the helper makes up to `MAX_SUBMISSION_RETRIES + 1`
/// total submit calls before giving up.
const MAX_SUBMISSION_RETRIES: u32 = 5;

/// Initial backoff before retrying an indeterminate submission.
const INITIAL_SUBMISSION_BACKOFF: Duration = Duration::from_millis(500);

/// Maximum per-attempt backoff sleep. Caps `INITIAL_SUBMISSION_BACKOFF * 2^n`.
const MAX_SUBMISSION_BACKOFF: Duration = Duration::from_secs(30);

/// Upper bound on one transaction's fee, in multiples of the verification base
/// fee: `fee = base × (⌊log2 cycles⌋ + 1)` ≤ 30 × base at the 2^29 cycle cap,
/// and the auth procedure may reserve up to 2×.
const FEE_HEADROOM_MULTIPLIER: u64 = 64;

/// While verifying, how often the executor re-checks whether it can settle.
/// The matcher holds clearing for this long at minimum, so a persistent
/// condition is not rebuilt and rejected on every matcher tick.
const VERIFY_INTERVAL: Duration = Duration::from_secs(5);

/// Most incoming notes claimed into the solver's vault in one transaction.
const MAX_CLAIM_NOTES: usize = 16;

/// A PSWAP creates a payback and at most one remainder; reserve one output
/// for authentication's transaction fee. Never split a solvent match group.
const MAX_SETTLEMENT_INPUTS: usize = {
    let output_bound = (miden_protocol::MAX_OUTPUT_NOTES_PER_TX - 1) / 2;
    if miden_protocol::MAX_INPUT_NOTES_PER_TX < output_bound {
        miden_protocol::MAX_INPUT_NOTES_PER_TX
    } else {
        output_bound
    }
};

/// After a failed claim, how long the sync loop waits before trying again.
const CLAIM_RETRY_DELAY: Duration = Duration::from_secs(60);
const SETTLEMENT_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Miden's standard transaction expiry. It bounds how long an indeterminate
/// settlement can reserve its inputs. The custom surplus script applies the
/// same delta itself because the request builder cannot combine an expiration
/// delta with a custom script.
const SETTLEMENT_EXPIRATION_BLOCKS: u16 = 20;

// ── Errors ─────────────────────────────────────────────────────────────────

/// Executor errors. Most are handled per batch or settlement (re-feed, pause,
/// retry next tick); the pipeline stops only on a fatal database error
/// (`DbError::is_fatal`) or cancellation.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Chain(#[from] ChainError),
    /// Boxed: `ClientError` alone would make every executor `Result` large.
    #[error(transparent)]
    Client(Box<ClientError>),
    #[error(transparent)]
    Settlement(#[from] SettlementError),
    #[error("stored settlement does not decode")]
    Decode(#[from] DeserializationError),
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

impl ExecutorError {
    /// Whether the solver must stop (see `DbError::is_fatal`).
    fn is_fatal(&self) -> bool {
        matches!(self, Self::Db(error) if error.is_fatal())
    }
}

impl From<ClientError> for ExecutorError {
    fn from(error: ClientError) -> Self {
        Self::Client(Box::new(error))
    }
}

type ExecResult<T> = Result<T, ExecutorError>;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Outcome of `submit_with_rpc_backoff`. The executor's main path branches on
/// this to decide the post-submit state-machine transition.
enum SubmitOutcome {
    /// Accepted into the mempool; confirmation is handled separately.
    Success(PendingSettlement),
    /// Submission may have landed, or its local store update failed. Keep
    /// inputs reserved until chain reconciliation establishes the outcome.
    Pending(PendingSettlement, String),
    /// Execution, proving, or submission failed with a definite outcome.
    /// Classify per-note via the nullifier check, mark consumed orders
    /// OnchainNullified, re-feed the rest to the matcher.
    TxError(ClientError, Option<PendingSettlement>),
    /// `build_tx_request` failed deterministically for this batch
    /// composition. The submit never landed, so every order is still valid:
    /// revert to Active and re-feed so the live matcher reconsiders them
    /// (it dropped them on emit and never re-reads the DB mid-run).
    BuildFailed(ExecutorError),
    /// A fatal database failure; stop the pipeline and hydrate from durable
    /// state on a whole-solver restart.
    Critical(DbError),
    /// Cancellation during backoff leaves the attempted transaction reserved.
    Cancelled,
}

enum BatchSubmission {
    /// Submitted; confirmation is reconciled later.
    Accepted(PendingSettlement),
    /// Maybe submitted; inputs stay reserved until reconciliation decides.
    Uncertain(PendingSettlement),
    /// A submitted transaction failed and its parents were already released.
    Released,
    /// Nothing was reserved and this batch cannot run as built: hand its
    /// orders back to the matcher.
    GiveBack(Vec<BookOrder>),
    /// The executor cannot settle right now (no fee headroom, RPC or database
    /// unavailable). Nothing was reserved; the orders are still Active. Retry
    /// this transaction after verification mode passes.
    Paused(ExecutorError),
}

/// Keep each input and its predicted outputs together, in batch order.
struct PreparedInput {
    note: Arc<Note>,
    args: NoteArgs,
    payback_id: NoteId,
    remainder: Option<Note>,
}

/// Inputs pre-computed once for a batch. Retries reuse the *same proven
/// transaction*, so its ID and predicted children cannot change.
struct BatchComponents {
    inputs: Vec<PreparedInput>,
    expected_output_recipients: Vec<NoteRecipient>,
    surplus_assets: Vec<Asset>,
}

/// Everything the live executor needs to reconcile one durable settlement.
/// The database is read once at startup to rebuild this state after a crash;
/// normal reconciliation then works from this in-memory registry.
struct PendingSettlement {
    tx_id: TransactionId,
    attempt: SettlementAttemptRow,
    parent_notes: Vec<Note>,
    /// Remainders this transaction creates when it commits.
    child_notes: Vec<Note>,
    retry_at: Option<tokio::time::Instant>,
}

impl PendingSettlement {
    fn prepared(attempt: SettlementAttemptRow, components: &BatchComponents) -> ExecResult<Self> {
        let tx_id = TransactionId::read_from(&mut SliceReader::new(&attempt.tx_id))?;
        Ok(Self {
            tx_id,
            attempt,
            parent_notes: components.notes(),
            child_notes: components
                .inputs
                .iter()
                .filter_map(|input| input.remainder.clone())
                .collect(),
            retry_at: None,
        })
    }

    async fn load(pool: &DbPool) -> ExecResult<HashMap<TransactionId, Self>> {
        let attempts = pool
            .read(db::postgres_db::load_unresolved_attempts_tx)
            .await?;
        let mut pending = HashMap::with_capacity(attempts.len());

        for recovered in attempts {
            let attempt = recovered.attempt;
            attempt.settlement_status()?;
            let tx_id = TransactionId::read_from(&mut SliceReader::new(&attempt.tx_id))?;
            let result = TransactionResult::read_from(&mut SliceReader::new(&attempt.tx_result))?;
            if result.id() != tx_id {
                return Err(SettlementError::RecordedTransactionIdMismatch.into());
            }
            let child_notes = recovered.children;
            let parent_notes = recovered
                .parents
                .into_iter()
                .map(|parent| parent.note.as_ref().clone())
                .collect();
            pending.insert(
                tx_id,
                Self {
                    tx_id,
                    attempt,
                    parent_notes,
                    child_notes,
                    retry_at: None,
                },
            );
        }

        Ok(pending)
    }

    fn id(&self) -> TransactionId {
        self.tx_id
    }

    fn id_bytes(&self) -> &[u8] {
        &self.attempt.tx_id
    }

    fn mark_rejected(&mut self) {
        self.attempt.status = SettlementStatus::Rejected.as_str().to_string();
    }

    fn mark_uncertain(&mut self) {
        self.attempt.status = SettlementStatus::Uncertain.as_str().to_string();
    }
}

impl BatchComponents {
    /// Parse each input once and predict its outputs. Exact per-token balances
    /// must be solvent before the transaction can be built.
    fn prepare(batch: &ExecutionBatch, solver_id: AccountId) -> ExecResult<Self> {
        let mut inputs = Vec::with_capacity(batch.filled_notes.len());
        let mut expected_output_recipients = Vec::new();

        // Net flow per token: positive = surplus staying with solver, negative = insolvent.
        // i128 is lossless for any u64 sum encountered here — no wrap risk on `as i128`.
        let mut flow: HashMap<TokenId, i128> = HashMap::new();

        for filled in &batch.filled_notes {
            let note = Arc::clone(&filled.note);

            let pswap = PswapNote::try_from(note.as_ref())?;

            let offered_asset = pswap.offered_asset();
            let offered_token = offered_asset.faucet_id();
            let requested_token = pswap.storage().requested_faucet_id();

            *flow.entry(offered_token).or_default() += u64::from(offered_asset.amount()) as i128;

            let fill_asset = FungibleAsset::new(requested_token, filled.requested_filled)?;

            // Both-zero args make the script fall back to a vault-funded full fill.
            if filled.requested_filled == 0 {
                return Err(ExecutorError::ZeroFill(filled.note_id));
            }
            let note_args = PswapNote::create_args(0, filled.requested_filled)?;

            let (p2id, remainder) = pswap.execute(solver_id, None, Some(fill_asset))?;
            let payback_id = p2id.id();

            *flow.entry(requested_token).or_default() -= note_asset_amount(&p2id) as i128;
            // Payback + remainder settle to the order CREATOR, not the solver. Declare them
            // as expected OUTPUT RECIPIENTS only — NEVER as expected future notes. This mirrors
            // `miden-client::build_pswap_consume`, which deliberately does the same and warns
            // that registering them as future notes "would leave stale, un-consumable notes in
            // the consumer's store": the future-note record is built from `NoteDetails` (which
            // carries NO attachments), and a PSWAP note's id commits to its attachments — so it
            // would land attachment-stripped, and the kernel later rejects the re-consume with
            // `InputNoteNotInBlock`.
            expected_output_recipients.push(p2id.recipient().clone());

            let remainder = remainder.map(Note::from);
            if let Some(rem_note) = &remainder {
                *flow.entry(offered_token).or_default() -= note_asset_amount(rem_note) as i128;
                expected_output_recipients.push(rem_note.recipient().clone());
            }
            inputs.push(PreparedInput {
                note,
                args: note_args,
                payback_id,
                remainder,
            });
        }

        // Negative flow means we owe more than we have — batch is insolvent.
        let mut surplus_assets: Vec<Asset> = Vec::new();
        for (token, net) in &flow {
            if *net < 0 {
                return Err(ExecutorError::Insolvent {
                    token: *token,
                    deficit: net.unsigned_abs(),
                });
            }
            if *net > 0 {
                let amount = u64::try_from(*net).map_err(|_| ExecutorError::SurplusOverflow {
                    token: *token,
                    surplus: *net,
                })?;
                surplus_assets.push(FungibleAsset::new(*token, amount)?.into());
            }
        }

        Ok(Self {
            inputs,
            expected_output_recipients,
            surplus_assets,
        })
    }

    fn request(&self) -> ExecResult<TransactionRequest> {
        // Every batch note is consumed unauthenticated, from the solver's own copy.
        // `input_notes` would substitute the executor store's copy whenever it holds an
        // inclusion proof, and that copy can have its attachments stripped (see
        // crates/solver/pswap-attachment-corruption-report.md); a PSWAP note id commits
        // to its attachments, so the batch would fail with `InputNoteNotInBlock`.
        let inputs = self.inputs.iter().map(|input| {
            (
                InputNote::unauthenticated(input.note.as_ref().clone()),
                Some(input.args),
            )
        });
        let mut builder = TransactionRequestBuilder::new()
            .explicit_input_notes(inputs)
            .expected_output_recipients(self.expected_output_recipients.clone());

        if !self.surplus_assets.is_empty() {
            let data = ConsumeAssetScript::prepare(&self.surplus_assets);
            builder = builder
                .custom_script(ConsumeAssetScript::tx_script())
                .script_arg(data.commitment_arg)
                .extend_advice_map([data.advice_map_entry]);
        } else {
            builder = builder.expiration_delta(SETTLEMENT_EXPIRATION_BLOCKS);
        }

        Ok(builder.build()?)
    }

    fn settlement_inputs(&self, result: &TransactionResult) -> ExecResult<Vec<SettlementInputRow>> {
        let tx_id = result.id().to_bytes();
        let output_ids: HashSet<_> = result
            .created_notes()
            .iter()
            .map(|note| note.id())
            .collect();
        self.inputs
            .iter()
            .map(|input| {
                let remainder = input.remainder.as_ref();
                let payback_id = input.payback_id;
                if !output_ids.contains(&payback_id) {
                    return Err(SettlementError::MissingPayback(payback_id).into());
                }
                if let Some(note) = remainder {
                    if !output_ids.contains(&note.id()) {
                        return Err(SettlementError::MissingRemainder(note.id()).into());
                    }
                }
                Ok(SettlementInputRow {
                    tx_id: tx_id.clone(),
                    parent_note_id: input.note.id().to_bytes().to_vec(),
                    child_note_id: remainder.map(|note| note.id().to_bytes().to_vec()),
                    child_note_data: remainder.map(Serializable::to_bytes),
                })
            })
            .collect()
    }

    /// Only failure paths need a separate note slice for the client API.
    fn notes(&self) -> Vec<Note> {
        self.inputs
            .iter()
            .map(|input| input.note.as_ref().clone())
            .collect()
    }
}

/// Execute and prove once, record the fixed ID and children, then submit that
/// same transaction with bounded backoff. Never re-execute an unknown outcome.
async fn submit_with_rpc_backoff(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    solver_id: AccountId,
    components: &BatchComponents,
    pool: &DbPool,
    cancel: &CancellationToken,
) -> SubmitOutcome {
    let request = match components.request() {
        Ok(request) => request,
        Err(error) => return SubmitOutcome::BuildFailed(error),
    };
    let result = match client
        .lock()
        .await
        .execute_transaction(solver_id, request)
        .await
    {
        Ok(result) => result,
        Err(error) => return SubmitOutcome::TxError(error, None),
    };
    let inputs = match components.settlement_inputs(&result) {
        Ok(inputs) => inputs,
        Err(error) => return SubmitOutcome::BuildFailed(error),
    };
    let proven = match client.lock().await.prove_transaction(&result).await {
        Ok(proven) => proven,
        Err(error) => return SubmitOutcome::TxError(error, None),
    };
    let attempt = SettlementAttemptRow::prepared(&result);
    let pending = match PendingSettlement::prepared(attempt, components) {
        Ok(pending) => pending,
        Err(error) => return SubmitOutcome::BuildFailed(error),
    };
    let durable_attempt = pending.attempt.clone();
    let persisted = pool
        .write(move |conn| db::postgres_db::prepare_settlement_tx(conn, &durable_attempt, &inputs))
        .await;
    if let Err(error) = persisted {
        if error.is_fatal() {
            return SubmitOutcome::Critical(error);
        }
        // The transaction rolled back, so nothing is reserved and nothing was
        // submitted. A parent consumed or reclaimed during proving is the
        // common cause; the matcher simply reconsiders whatever is still Active.
        return SubmitOutcome::BuildFailed(error.into());
    }

    let mut backoff = INITIAL_SUBMISSION_BACKOFF;
    let mut unknown_seen = false;
    for attempt in 0..=MAX_SUBMISSION_RETRIES {
        let submit_res = {
            let mut c = client.lock().await;
            c.submit_proven_transaction(proven.clone(), &result).await
        };
        match submit_res {
            // Acceptance is not recorded: reconciliation reads the outcome
            // from our transaction ID, and resubmitting it is harmless.
            Ok(height) => {
                return match client.lock().await.apply_transaction(&result, height).await {
                    Ok(()) => SubmitOutcome::Success(pending),
                    Err(error) => SubmitOutcome::Pending(
                        pending,
                        format!("transaction accepted but local store update failed: {error}"),
                    ),
                };
            }
            Err(e) if submission_outcome_is_unknown(&e) => {
                unknown_seen = true;
                if attempt == MAX_SUBMISSION_RETRIES {
                    return SubmitOutcome::Pending(
                        pending,
                        format!("submission outcome unknown for {}: {e}", result.id()),
                    );
                }
                tracing::warn!(attempt, error = %e, "submission outcome unknown; retrying same transaction");
            }
            Err(e) if unknown_seen => {
                return SubmitOutcome::Pending(
                    pending,
                    format!(
                        "earlier submission of {} may have landed; later rejection: {e}",
                        result.id()
                    ),
                );
            }
            Err(e) => return SubmitOutcome::TxError(e, Some(pending)),
        }
        tokio::select! {
            _ = cancel.cancelled() => return SubmitOutcome::Cancelled,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_SUBMISSION_BACKOFF);
    }
    unreachable!("loop exits via return inside the matched arms")
}

fn submission_outcome_is_unknown(error: &ClientError) -> bool {
    matches!(error, ClientError::SubmissionOutcomeUnknown { .. })
}

/// Verification-mode bookkeeping, kept apart from the I/O so it is testable.
/// While `active`, the executor stops reading its queue; held transactions
/// run, in their original order, once verification passes.
#[derive(Default)]
struct Verification {
    active: bool,
    /// Transactions to run again once verification passes.
    retry: VecDeque<ExecutionBatch>,
    /// Orders owed back to the matcher whose return did not commit yet.
    returning: Vec<BookOrder>,
}

impl Verification {
    /// Enter verification mode; `retry` runs, in order, once it passes.
    fn pause(&mut self, retry: impl IntoIterator<Item = ExecutionBatch>) {
        self.retry.extend(retry);
        self.active = true;
    }

    fn resume(&mut self) {
        self.active = false;
    }

    /// Held transactions to run before reading new batches; none while paused.
    fn take_retries(&mut self) -> Vec<ExecutionBatch> {
        if self.active {
            Vec::new()
        } else {
            self.retry.drain(..).collect()
        }
    }
}

/// Give orders that were never reserved back to the matcher, minus any whose
/// note is already consumed on chain (those are marked OnchainNullified). The
/// only give-back path. Leaves `held` intact on error, so verification
/// retries the return once RPC and DB are back.
async fn release_held(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    held: &mut Vec<BookOrder>,
) -> ExecResult<()> {
    if held.is_empty() {
        return Ok(());
    }
    let notes: Vec<Note> = held
        .iter()
        .map(|order| order.note.as_ref().clone())
        .collect();
    let mut consumed = HashSet::new();
    for chunk in notes.chunks(miden_protocol::MAX_INPUT_NOTES_PER_TX) {
        consumed.extend(
            miden_adapter
                .lock()
                .await
                .check_consumed_notes(chunk)
                .await?,
        );
    }
    let orders: Vec<_> = held
        .iter()
        .filter(|order| !consumed.contains(&order.id()))
        .cloned()
        .collect();
    let returned = orders.len();
    let dropped = consumed.len();
    pool.write_book(book_tx, move |conn| {
        let consumed_bytes: Vec<_> = consumed.iter().map(|id| id.to_bytes().to_vec()).collect();
        db::postgres_db::mark_orders_onchain_nullified_tx(conn, &consumed_bytes)?;
        let mut update = db::postgres_db::active_book_update_tx(conn, orders)?;
        update.removed.extend(consumed);
        Ok(update)
    })
    .await?;
    tracing::info!(returned, dropped, "held orders returned to the matcher");
    held.clear();
    Ok(())
}

/// Verification mode's readiness probe. Every check must pass before the
/// executor accepts batches again: node RPC answers, fee headroom covers one
/// settlement, the database writer commits, and any orders still owed to the
/// matcher are returned.
async fn verify_ready(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    held: &mut Vec<BookOrder>,
) -> ExecResult<()> {
    check_fee_headroom(client, miden_adapter, solver_id).await?;
    pool.write(|_| Ok(())).await?;
    release_held(miden_adapter, pool, book_tx, held).await
}

// ── Main loop ──────────────────────────────────────────────────────────────

/// Submit batches and reconcile their on-chain outcomes. Local execution and
/// proof precede a durable Active → Settling reservation. Mempool acceptance
/// does not retire the parent: confirmation moves it to Executed and activates
/// any remainder. Unknown outcomes stay reserved; definite failures undergo
/// nullifier classification before parents can return to the book.
#[allow(clippy::too_many_arguments)]
pub async fn run_executor(
    client: Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: DbPool,
    exec_rx: mpsc::Receiver<ExecutionBatch>,
    book_tx: mpsc::Sender<BookUpdate>,
    // In-memory swap-eta settlement-time window; republished on each success.
    stats_tx: watch::Sender<Arc<SettlementStats>>,
    cancel: CancellationToken,
) {
    let result = executor_loop(
        &client,
        &miden_adapter,
        solver_id,
        &pool,
        exec_rx,
        &book_tx,
        &stats_tx,
        &cancel,
    )
    .await;
    match result {
        Ok(()) | Err(ExecutorError::Cancelled) => {}
        // Only fatal database failures and broken durable state reach here;
        // everything else is handled per batch or by verification mode.
        Err(error) => {
            tracing::error!(%error, "executor failed; stopping pipeline");
            cancel.cancel();
        }
    }
    tracing::info!("executor shutting down");
}

#[allow(clippy::too_many_arguments)]
async fn executor_loop(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    mut exec_rx: mpsc::Receiver<ExecutionBatch>,
    book_tx: &mpsc::Sender<BookUpdate>,
    stats_tx: &watch::Sender<Arc<SettlementStats>>,
    cancel: &CancellationToken,
) -> ExecResult<()> {
    // Owned here (executor thread) and published over `stats_tx`. Ephemeral —
    // no DB persistence; rebuilds after a restart.
    let mut stats = SettlementStats::new();
    let mut pending = PendingSettlement::load(pool).await?;
    let mut reconcile_tick = tokio::time::interval(SETTLEMENT_RECONCILE_INTERVAL);
    reconcile_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut verify_tick = tokio::time::interval(VERIFY_INTERVAL);
    verify_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Verification mode. The executor stops reading `exec_rx`; once its one
    // queue slot is full the matcher skips ticks and keeps orders live.
    let mut verification = Verification::default();
    loop {
        let held = verification.take_retries();
        let transactions = if !held.is_empty() {
            held
        } else {
            // Cancellation is only checked BETWEEN batches. Once execute_batch
            // starts, only the backoff sleep is cancel-aware — the on-chain
            // submit runs to completion before a result is observed.
            let mut batch = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = reconcile_tick.tick() => {
                    // RPC failures are handled per settlement inside; an error
                    // here means durable state or the matcher is unavailable.
                    reconcile_settlements(client, miden_adapter, solver_id, pool, book_tx, &mut pending).await?;
                    continue;
                }
                _ = verify_tick.tick(), if verification.active => {
                    match verify_ready(client, miden_adapter, solver_id, pool, book_tx, &mut verification.returning).await {
                        Ok(()) => {
                            verification.resume();
                            tracing::info!(retrying = verification.retry.len(), "executor verified ready; accepting batches");
                        }
                        Err(error) if error.is_fatal() => return Err(error),
                        Err(error) => tracing::warn!(%error, "executor not ready; still verifying"),
                    }
                    continue;
                }
                batch = exec_rx.recv(), if !verification.active => match batch {
                    Some(batch) => batch,
                    None => return Ok(()), // channel closed → upstream is gone
                },
            };
            if batch.filled_notes.is_empty() {
                continue;
            }
            match split_batch(&mut batch) {
                Ok(transactions) => transactions,
                Err(error) => {
                    // A malformed batch fails the same way on every retry.
                    tracing::error!(%error, "cannot split execution batch safely; returning orders");
                    verification.returning.extend(batch.book_orders());
                    give_back(
                        miden_adapter,
                        pool,
                        book_tx,
                        &mut verification,
                        &mut verify_tick,
                    )
                    .await?;
                    continue;
                }
            }
        };

        let mut transactions = transactions.into_iter();
        while let Some(batch) = transactions.next() {
            if cancel.is_cancelled() {
                // Unsubmitted groups remain Active in the DB for boot recovery.
                break;
            }
            let notes = batch.filled_notes.len();
            match execute_batch(
                client,
                miden_adapter,
                solver_id,
                pool,
                &batch,
                book_tx,
                cancel,
            )
            .await?
            {
                BatchSubmission::Accepted(settlement) => {
                    tracing::info!(notes, "batch accepted for settlement");
                    record_settlement(&batch, &mut stats, stats_tx);
                    pending.insert(settlement.id(), settlement);
                }
                BatchSubmission::Uncertain(settlement) => {
                    tracing::warn!(notes, "batch submission outcome uncertain");
                    pending.insert(settlement.id(), settlement);
                }
                BatchSubmission::Released => {}
                BatchSubmission::GiveBack(orders) => {
                    verification.returning.extend(orders);
                    if !give_back(
                        miden_adapter,
                        pool,
                        book_tx,
                        &mut verification,
                        &mut verify_tick,
                    )
                    .await?
                    {
                        verification.pause(transactions.by_ref());
                        break;
                    }
                }
                BatchSubmission::Paused(reason) => {
                    tracing::warn!(%reason, "executor entering verification mode");
                    verification.pause(std::iter::once(batch).chain(transactions.by_ref()));
                    verify_tick.reset();
                    break;
                }
            }
        }
    }
}

/// Return `verification.returning` to the matcher now. `Ok(false)`: the
/// return did not commit; the orders stay owed and the executor enters
/// verification mode, which retries the return.
async fn give_back(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    verification: &mut Verification,
    verify_tick: &mut tokio::time::Interval,
) -> ExecResult<bool> {
    match release_held(miden_adapter, pool, book_tx, &mut verification.returning).await {
        Ok(()) => Ok(true),
        Err(error) if error.is_fatal() => Err(error),
        Err(error) => {
            tracing::warn!(%error, "orders could not be returned; executor entering verification mode");
            verification.pause(std::iter::empty());
            verify_tick.reset();
            Ok(false)
        }
    }
}

/// The executor client syncs independently. Once its local transaction record
/// says Committed, atomically hand the original FIFO slot to the child note.
async fn reconcile_settlements(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    pending: &mut HashMap<TransactionId, PendingSettlement>,
) -> ExecResult<()> {
    let tx_ids = pending.keys().copied().collect::<Vec<_>>();
    for key in tx_ids {
        let Some(mut settlement) = pending.remove(&key) else {
            continue;
        };
        let resolved = match reconcile_settlement(
            client,
            miden_adapter,
            solver_id,
            pool,
            book_tx,
            &mut settlement,
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(error) if error.is_fatal() => return Err(error),
            // One failed lifecycle write leaves this settlement exactly as
            // durable state describes it; try again on the next tick.
            Err(error) => {
                tracing::warn!(tx_id = %key, %error, "settlement reconciliation failed; retrying next tick");
                false
            }
        };
        if !resolved {
            pending.insert(key, settlement);
        }
    }
    Ok(())
}

/// What the chain says about our settlement transaction. Decided only from
/// our transaction ID — never from payback or remainder note IDs, which
/// another filler of the same parent and amount reproduces exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxOutcome {
    /// The node committed our transaction.
    Committed,
    /// It can never commit: rejected by the node, or its whole validity
    /// window has passed without it appearing.
    NeverCommits,
    /// Not decidable yet (still valid, not seen, or an RPC failed).
    /// `resubmittable`: the executor client has no record of submitting it,
    /// so the executor may have stopped before the node received it.
    Unknown { resubmittable: bool },
}

/// Look up our transaction: the executor client's record first, then the node
/// for the solver account's transactions over the attempt's validity window.
async fn transaction_outcome(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    settlement: &PendingSettlement,
) -> ExecResult<TxOutcome> {
    let tx_id = settlement.id();
    if settlement.attempt.settlement_status()? == SettlementStatus::Rejected {
        // Rejected by the node at submission: it never entered the chain.
        return Ok(TxOutcome::NeverCommits);
    }
    let record = match client
        .lock()
        .await
        .get_transactions(TransactionFilter::Ids(vec![tx_id]))
        .await
    {
        Ok(records) => records.into_iter().next(),
        Err(ClientError::RpcError(error)) => {
            tracing::warn!(%tx_id, %error, "transaction lookup failed; retrying next tick");
            return Ok(TxOutcome::Unknown {
                resubmittable: false,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let resubmittable = record.is_none();
    match record.map(|record| record.status) {
        Some(TransactionStatus::Committed { .. }) => return Ok(TxOutcome::Committed),
        // A definite discard means the node never accepted this transaction.
        Some(TransactionStatus::Discarded(reason))
            if !matches!(
                reason,
                DiscardCause::Stale | DiscardCause::DiscardedInitialState
            ) =>
        {
            tracing::warn!(%tx_id, %reason, "settlement transaction discarded");
            return Ok(TxOutcome::NeverCommits);
        }
        // Pending, an ambiguous discard, or no local record (the executor may
        // have stopped between submission and recording): ask the node.
        _ => {}
    }

    let result =
        TransactionResult::read_from(&mut SliceReader::new(&settlement.attempt.tx_result))?;
    let executed = result.executed_transaction();
    let reference = executed.block_header().block_num();
    let expiration = executed.expiration_block_num();
    // Read the synced height before the lookup, so a negative answer covers
    // every block up to it.
    let synced = client.lock().await.get_sync_height().await?;
    let to = if synced < expiration {
        synced
    } else {
        expiration
    };
    let committed = match miden_adapter
        .lock()
        .await
        .transaction_committed(solver_id, tx_id, reference, to)
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            tracing::warn!(%tx_id, %error, "node transaction lookup failed; retrying next tick");
            return Ok(TxOutcome::Unknown { resubmittable });
        }
    };
    Ok(node_outcome(committed, synced, expiration, resubmittable))
}

/// The node's answer for our transaction, searched up to `synced`. A
/// transaction is valid in its expiration block; once the chain is past it
/// and the node never committed it, it never will.
fn node_outcome(
    committed: bool,
    synced: BlockNumber,
    expiration: BlockNumber,
    resubmittable: bool,
) -> TxOutcome {
    if committed {
        TxOutcome::Committed
    } else if synced > expiration {
        TxOutcome::NeverCommits
    } else {
        TxOutcome::Unknown { resubmittable }
    }
}

/// Returns `true` only after a terminal database and matcher transition.
async fn reconcile_settlement(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    settlement: &mut PendingSettlement,
) -> ExecResult<bool> {
    match transaction_outcome(client, miden_adapter, solver_id, settlement).await? {
        TxOutcome::Committed => {
            activate_confirmed_settlement(miden_adapter, pool, settlement, book_tx).await
        }
        // Inputs return by nullifier: a parent someone else consumed is
        // retired, every other parent goes back to the matcher.
        TxOutcome::NeverCommits => {
            release_rejected_settlement(miden_adapter, pool, settlement, book_tx).await
        }
        TxOutcome::Unknown { resubmittable } => {
            // The executor may have stopped after preparing but before the
            // node received the transaction. Resubmitting the same proven
            // transaction keeps its ID; the outcome is still read from it.
            let status = settlement.attempt.settlement_status()?;
            let retry_due = settlement
                .retry_at
                .is_none_or(|at| tokio::time::Instant::now() >= at);
            if resubmittable && status != SettlementStatus::Uncertain && retry_due {
                settlement.retry_at = Some(tokio::time::Instant::now() + RECOVERY_RETRY_DELAY);
                retry_recorded_transaction(client, pool, settlement).await?;
            }
            Ok(false)
        }
    }
}

async fn release_rejected_settlement(
    adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    settlement: &PendingSettlement,
    book_tx: &mpsc::Sender<BookUpdate>,
) -> ExecResult<bool> {
    let consumed = match adapter
        .lock()
        .await
        .check_consumed_notes(&settlement.parent_notes)
        .await
    {
        Ok(consumed) => consumed,
        Err(error) => {
            tracing::warn!(error = %error, "nullifier RPC failed; retrying settlement release next tick");
            return Ok(false);
        }
    };
    let attempt_id = settlement.id_bytes().to_vec();
    pool.write_book(book_tx, move |conn| {
        db::postgres_db::finish_discarded_settlement_tx(conn, &attempt_id, &consumed)
    })
    .await?;
    Ok(true)
}

/// Our transaction committed: retire its parents and activate its remainders.
/// A remainder may already be consumed by the time we confirm, so its
/// nullifier is checked first; a consumed one keeps its FIFO slot but is
/// never activated. Returns `false` (retry next tick) if that lookup fails.
async fn activate_confirmed_settlement(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    settlement: &PendingSettlement,
    book_tx: &mpsc::Sender<BookUpdate>,
) -> ExecResult<bool> {
    let consumed_children = if settlement.child_notes.is_empty() {
        HashSet::new()
    } else {
        match miden_adapter
            .lock()
            .await
            .check_consumed_notes(&settlement.child_notes)
            .await
        {
            Ok(consumed) => consumed,
            Err(error) => {
                tracing::warn!(%error, "remainder nullifier lookup failed; confirming next tick");
                return Ok(false);
            }
        }
    };
    let attempt_id = settlement.id_bytes().to_vec();
    pool.write_book(book_tx, move |conn| {
        db::postgres_db::confirm_settlement_tx(conn, &attempt_id, &consumed_children)
    })
    .await?;
    Ok(true)
}

/// A crash may occur after the PostgreSQL prepare write but before the client
/// stores submission. Re-proving and submitting this *same* executed result
/// retains its ID. A deliberate rejection is ambiguous if the first copy
/// already landed, so it is quarantined rather than reactivating parents.
async fn retry_recorded_transaction(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    pool: &DbPool,
    settlement: &mut PendingSettlement,
) -> ExecResult<()> {
    let attempt = &settlement.attempt;
    let result = TransactionResult::read_from(&mut SliceReader::new(&attempt.tx_result))?;
    if result.id().to_bytes().as_slice() != attempt.tx_id {
        return Err(SettlementError::RecordedTransactionIdMismatch.into());
    }
    let proven = match client.lock().await.prove_transaction(&result).await {
        Ok(proven) => proven,
        Err(error) => {
            tracing::warn!(tx_id = %result.id(), %error, "recovery proof failed; retrying later");
            return Ok(());
        }
    };
    let submission = client
        .lock()
        .await
        .submit_proven_transaction(proven, &result)
        .await;
    match submission {
        Ok(height) => {
            if let Err(error) = client.lock().await.apply_transaction(&result, height).await {
                tracing::warn!(tx_id = %result.id(), %error, "recovered submission accepted but local store update failed");
            }
        }
        Err(ClientError::SubmissionOutcomeUnknown { .. }) => {
            tracing::warn!(tx_id = %result.id(), "recovered submission still has no definite outcome");
        }
        Err(ClientError::RpcError(error)) => {
            // This retry was rejected, but the original submission may have
            // been sent before a crash. Keep it reserved until its recorded
            // transaction expires; rejection of the retry cannot disprove the
            // original copy.
            tracing::warn!(tx_id = %result.id(), %error, "recovery retry rejected; waiting for expiry");
        }
        Err(error) => {
            let attempt_id = attempt.tx_id.clone();
            pool.write(move |conn| {
                db::postgres_db::mark_settlement_uncertain_tx(conn, &attempt_id)
            })
            .await?;
            settlement.mark_uncertain();
            tracing::error!(tx_id = %result.id(), %error,
                "recorded transaction rejected; parents remain reserved for inspection");
        }
    }
    Ok(())
}

/// Pack consecutive independently solvent groups without copying note bytes.
/// Validate all boundaries before moving anything out of the caller's batch.
fn split_batch(batch: &mut ExecutionBatch) -> ExecResult<Vec<ExecutionBatch>> {
    let total = batch.filled_notes.len();
    let ends: &[usize] = if batch.group_ends.is_empty() {
        std::slice::from_ref(&total)
    } else {
        &batch.group_ends
    };
    let mut previous = 0;
    let mut packed = 0;
    let mut sizes = Vec::new();
    for &end in ends {
        if end <= previous || end > total {
            return Err(SettlementError::InvalidExecutionGroupBoundary {
                previous,
                end,
                total,
            }
            .into());
        }
        let size = end - previous;
        if size > MAX_SETTLEMENT_INPUTS {
            return Err(SettlementError::ExecutionGroupTooLarge {
                size,
                maximum: MAX_SETTLEMENT_INPUTS,
            }
            .into());
        }
        if packed + size > MAX_SETTLEMENT_INPUTS {
            sizes.push(packed);
            packed = 0;
        }
        packed += size;
        previous = end;
    }
    if previous != total {
        return Err(SettlementError::IncompleteExecutionGroups {
            covered: previous,
            total,
        }
        .into());
    }
    if packed > 0 {
        sizes.push(packed);
    }
    let mut notes = std::mem::take(&mut batch.filled_notes).into_iter();
    Ok(sizes
        .into_iter()
        .map(|size| ExecutionBatch {
            filled_notes: notes.by_ref().take(size).collect(),
            group_ends: Vec::new(),
        })
        .collect())
}

/// After mempool acceptance, record each note's enqueue-to-submit duration
/// (now − arrival) into the in-memory swap-eta window and republish it.
/// `arrival_unix` follows the order from durable ingestion through the batch;
/// the pair is read from the shared original note.
fn record_settlement(
    batch: &ExecutionBatch,
    stats: &mut SettlementStats,
    stats_tx: &watch::Sender<Arc<SettlementStats>>,
) {
    let now = crate::types::now_unix();

    let mut changed = false;
    for filled in &batch.filled_notes {
        // Pair from the note's own terms (offered, requested) — the direction a
        // wallet queries. Skip anything unparseable.
        let Ok(pswap) = PswapNote::try_from(filled.note.as_ref()) else {
            continue;
        };
        let pair = (
            pswap.offered_asset().faucet_id(),
            pswap.storage().requested_faucet_id(),
        );
        let duration = now.saturating_sub(filled.arrival_unix);
        stats.record(pair, now, duration);
        changed = true;
    }
    if changed {
        // send_replace (not send): overwrite the published stats with the latest
        // and return the prior value (ignored). Unlike `send`, it never errors
        // when the swap-eta reader isn't currently subscribed.
        stats_tx.send_replace(Arc::new(stats.clone()));
    }
}

#[tracing::instrument(skip(client, miden_adapter, pool, batch, book_tx, cancel),
                     fields(batch_size = batch.filled_notes.len()))]
async fn execute_batch(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    batch: &ExecutionBatch,
    book_tx: &mpsc::Sender<BookUpdate>,
    cancel: &CancellationToken,
) -> ExecResult<BatchSubmission> {
    // Nothing is reserved before `prepare_settlement_tx`, and the matcher took
    // these orders off its book on emit, so every early exit hands them back.
    let components = match BatchComponents::prepare(batch, solver_id) {
        Ok(parts) => parts,
        Err(e) => {
            // A batch that cannot be prepared fails the same way on retry,
            // so its orders go straight back to the matcher.
            tracing::error!(error = %e, notes = batch.filled_notes.len(), "batch preparation failed; returning its orders");
            return Ok(BatchSubmission::GiveBack(batch.book_orders()));
        }
    };

    // Fee pre-flight (Miden 0.16): each settlement's fee is paid in the native
    // asset from the solver's own vault. A low balance, or a fee or balance
    // lookup that fails, pauses into verification mode; nothing is submitted.
    if let Err(e) = check_fee_headroom(client, miden_adapter, solver_id).await {
        return Ok(BatchSubmission::Paused(e));
    }

    match submit_with_rpc_backoff(client, solver_id, &components, pool, cancel).await {
        SubmitOutcome::Success(settlement) => {
            // Mempool acceptance is not chain confirmation. The sync poll or
            // ingestion of an expected remainder performs the DB handoff.
            Ok(BatchSubmission::Accepted(settlement))
        }
        SubmitOutcome::Pending(settlement, message) => {
            tracing::warn!(%message, "settlement outcome pending; inputs remain reserved");
            Ok(BatchSubmission::Uncertain(settlement))
        }
        SubmitOutcome::BuildFailed(msg) => {
            // Failure for THIS batch composition; nothing was reserved or
            // submitted. Re-feed immediately: the DB filter returns only the
            // parents still Active, so one consumed during proving drops out.
            tracing::error!(error = %msg, "tx build failed; returning its orders");
            Ok(BatchSubmission::GiveBack(batch.book_orders()))
        }
        SubmitOutcome::Critical(error) => Err(error.into()),
        SubmitOutcome::Cancelled => {
            tracing::info!(
                "submit cancelled during backoff; orders remain Settling for reconciliation"
            );
            Err(ExecutorError::Cancelled)
        }
        // Failed before reservation (execution or proving): nothing was
        // submitted, so the orders simply go back; `release_held` drops any
        // whose note got consumed meanwhile.
        SubmitOutcome::TxError(e, None) => {
            tracing::error!(error = %e, "settlement transaction failed before submission; returning its orders");
            Ok(BatchSubmission::GiveBack(batch.book_orders()))
        }
        SubmitOutcome::TxError(e, Some(mut settlement)) => {
            let attempt_id = settlement.id_bytes().to_vec();
            if let Err(error) = pool
                .write(move |conn| db::postgres_db::mark_settlement_rejected_tx(conn, &attempt_id))
                .await
            {
                if error.is_fatal() {
                    return Err(error.into());
                }
                // The durable row keeps its pre-rejection status; the
                // discard transaction below accepts any unconfirmed status.
                tracing::warn!(%error, "rejected settlement status write failed; continuing");
            }
            settlement.mark_rejected();
            let input_notes = components.notes();
            let note_ids: Vec<_> = input_notes.iter().map(|note| note.id()).collect();
            tracing::error!(error = %e, ?note_ids, "settlement transaction failed; classifying input nullifiers");

            // On any non-fatal failure below the rejection is durable and the
            // parents stay reserved; reconciliation retries the release.
            let consumed = match miden_adapter
                .lock()
                .await
                .check_consumed_notes(&input_notes)
                .await
            {
                Ok(consumed) => consumed,
                Err(error) => {
                    tracing::warn!(%error, "input classification RPC failed; settlement remains reserved");
                    return Ok(BatchSubmission::Uncertain(settlement));
                }
            };
            let consumed_count = consumed.len();
            let tx_id = settlement.attempt.tx_id.clone();
            let released = pool
                .write_book(book_tx, move |conn| {
                    // Release parents and delete the attempt in one transaction.
                    db::postgres_db::finish_discarded_settlement_tx(conn, &tx_id, &consumed)
                })
                .await;
            match released {
                Ok(()) => {
                    tracing::error!(error = ?e, consumed_count, "non-RPC submit failure classified");
                    Ok(BatchSubmission::Released)
                }
                Err(error) if error.is_fatal() => Err(error.into()),
                Err(error) => {
                    tracing::warn!(%error, "discard write failed; settlement left for reconciliation");
                    Ok(BatchSubmission::Uncertain(settlement))
                }
            }
        }
    }
}

/// `Err` when the chain charges a fee and the solver's fee-asset balance is below
/// one settlement's worst case, or when either lookup fails: the solver never
/// submits without knowing it can pay. The balance comes from the local store,
/// so it lags the chain by up to one sync interval.
async fn check_fee_headroom(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
) -> ExecResult<()> {
    let Some((fee_faucet, base_fee)) = miden_adapter.lock().await.fee_parameters().await? else {
        return Ok(());
    };
    if base_fee == 0 {
        return Ok(());
    }
    let need = u64::from(base_fee) * FEE_HEADROOM_MULTIPLIER;
    let have = client
        .lock()
        .await
        .account_reader(solver_id)
        .get_balance(fee_faucet)
        .await?
        .as_u64();
    if have < need {
        return Err(ExecutorError::InsufficientFee {
            have,
            need,
            faucet: fee_faucet,
        });
    }
    Ok(())
}

/// Incoming P2ID/P2IDE notes worth claiming into the solver's vault. On a
/// fee-charging chain each must carry at least one settlement's worst-case fee
/// in the fee asset, so a claim never costs more than it brings in and dust
/// notes can't drain the solver through claim fees. `fee` is `None` when the
/// client can't report fee parameters (mocks); then everything qualifies.
/// Only P2ID/P2IDE: the executor store also holds PSWAP remainders the solver
/// created, which it can consume but must not.
fn select_claimable(notes: Vec<Note>, fee: Option<(AccountId, u32)>) -> Vec<Note> {
    let (p2id, p2ide) = (P2idNote::script_root(), P2ideNote::script_root());
    notes
        .into_iter()
        .filter(|note| {
            let root = note.recipient().script().root();
            root == p2id || root == p2ide
        })
        .filter(|note| match fee {
            Some((fee_faucet, base_fee)) if base_fee > 0 => {
                let fee_amount: u64 = note
                    .assets()
                    .iter_fungible()
                    .filter(|asset| asset.faucet_id() == fee_faucet)
                    .map(|asset| u64::from(asset.amount()))
                    .sum();
                fee_amount >= u64::from(base_fee) * FEE_HEADROOM_MULTIPLIER
            }
            _ => true,
        })
        .take(MAX_CLAIM_NOTES)
        .collect()
}

/// Whether the solver can spend a note now. Time-locked notes wait for a later
/// tick: one note that can't be spent yet would fail the whole claim.
fn spendable_now(status: &NoteConsumptionStatus) -> bool {
    matches!(
        status,
        NoteConsumptionStatus::Consumable | NoteConsumptionStatus::ConsumableWithAuthorization
    )
}

/// Consume the notes paying the solver (see [`select_claimable`]) in one
/// transaction, so funding the solver is just sending it tokens. A new
/// account's first claim also deploys it. Returns how many notes were claimed.
async fn claim_incoming_funds(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
) -> ExecResult<usize> {
    let records = client
        .lock()
        .await
        .get_consumable_notes(Some(solver_id))
        .await?;
    if records.is_empty() {
        return Ok(0);
    }
    let notes: Vec<Note> = records
        .into_iter()
        .filter(|(_, statuses)| {
            statuses
                .iter()
                .any(|(account, status)| *account == solver_id && spendable_now(status))
        })
        .filter_map(|(record, _)| TryInto::<Note>::try_into(record).ok())
        .collect();
    let fee = miden_adapter.lock().await.fee_parameters().await?;
    let notes = select_claimable(notes, fee);
    if notes.is_empty() {
        return Ok(0);
    }

    let count = notes.len();
    let request = TransactionRequestBuilder::new().build_consume_notes(notes)?;
    let tx_id = client
        .lock()
        .await
        .submit_new_transaction(solver_id, request)
        .await?;
    tracing::info!(%tx_id, notes = count, "claimed incoming funds into the solver account");
    Ok(count)
}

/// Run one claim. On failure, log it and return when the next attempt may run.
async fn claim_or_back_off(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
) -> Option<tokio::time::Instant> {
    match claim_incoming_funds(client, miden_adapter, solver_id).await {
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, "claiming incoming funds failed; retrying in 60 s");
            Some(tokio::time::Instant::now() + CLAIM_RETRY_DELAY)
        }
    }
}

fn note_asset_amount(note: &Note) -> u64 {
    note.assets()
        .iter_fungible()
        .next()
        .map(|a| u64::from(a.amount()))
        .unwrap_or(0)
}

/// Spawn the keystore **executor** OS thread: own `current_thread` runtime +
/// `LocalSet`; builds the executor client on-thread (so the `!Send` `Client`
/// never crosses a thread boundary), then runs the executor + the tagless
/// executor-sync task. The sync task discovers no notes (no tag
/// subscriptions); it only keeps the executor client's reference block recent
/// off the critical path (the node rejects txs whose reference block is older
/// than its acceptance window). Submitting does NOT pull chain state — the
/// client optimistically applies its own tx — so consecutive settlements stay
/// consistent without a sync between them.
///
/// Returns the joinable thread handle plus a `oneshot::Receiver` that yields
/// `Ok(())` once the client is built and the tasks are spawned, or the build
/// error — so a startup failure surfaces at the caller's readiness gate
/// instead of dying silently in a detached thread.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_executor_thread(
    factory: Arc<dyn ClientFactory>,
    db_pool: DbPool,
    cancel: CancellationToken,
    solver_id: AccountId,
    exec_rx: mpsc::Receiver<ExecutionBatch>,
    refeed_tx: mpsc::Sender<BookUpdate>,
    stats_tx: watch::Sender<Arc<SettlementStats>>,
    sync_interval: Duration,
) -> anyhow::Result<(
    thread::JoinHandle<()>,
    oneshot::Receiver<anyhow::Result<()>>,
)> {
    use anyhow::Context;
    let (exec_ready_tx, exec_ready_rx) = oneshot::channel::<anyhow::Result<()>>();
    let executor_thread = thread::Builder::new()
        .name("executor-client".into())
        .spawn(move || {
            crate::start::run_on_local_runtime("executor-client", async move {
                let setup = async {
                    let client = factory.build_executor().await.context("build_executor")?;
                    let rpc = factory.rpc().context("build executor rpc")?;
                    let shared: Arc<Mutex<Client<FilesystemKeyStore>>> =
                        Arc::new(Mutex::new(client));
                    let adapter: Arc<Mutex<dyn MidenClient>> =
                        Arc::new(Mutex::new(MidenClientAdapter {
                            client: shared.clone(),
                            rpc,
                        }));
                    // Miden 0.16 seals transaction inputs against synced chain
                    // headers, so an unsynced client can't submit. Sync once
                    // before accepting batches; the periodic sync task below
                    // sleeps before its first tick.
                    adapter
                        .lock()
                        .await
                        .sync_state()
                        .await
                        .context("initial executor sync")?;
                    anyhow::Ok((shared, adapter))
                };
                let (executor_shared, executor_adapter) = match setup.await {
                    Ok(handles) => handles,
                    Err(error) => {
                        let _ = exec_ready_tx.send(Err(error));
                        return;
                    }
                };
                match executor_adapter.lock().await.fee_parameters().await {
                    Ok(Some((faucet, base_fee))) => {
                        tracing::info!(fee_faucet = %faucet, base_fee, "chain fee parameters");
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "could not read chain fee parameters at boot"),
                }

                let run_client = executor_shared.clone();
                let run_adapter = executor_adapter.clone();
                let run_cancel = cancel.clone();
                let mut executor_handle = tokio::task::spawn_local(async move {
                    run_executor(
                        run_client,
                        run_adapter,
                        solver_id,
                        db_pool,
                        exec_rx,
                        refeed_tx,
                        stats_tx,
                        run_cancel,
                    )
                    .await;
                });

                let sync_adapter = executor_adapter.clone();
                let sync_client = executor_shared.clone();
                let sync_cancel = cancel.clone();
                let mut executor_sync_handle = tokio::task::spawn_local(async move {
                    // Claim funds sent while the solver was down (a new account's first
                    // claim also deploys it), then again after every successful sync.
                    // Claiming here, not before readiness, keeps a slow proof from
                    // delaying startup. A failed claim backs off.
                    let mut claim_retry_at = claim_or_back_off(&sync_client, &sync_adapter, solver_id).await;
                    loop {
                        tokio::select! {
                            _ = sync_cancel.cancelled() => break,
                            _ = tokio::time::sleep(sync_interval) => {
                                if let Err(e) = sync_adapter.lock().await.sync_state().await {
                                    tracing::warn!(error = %e, "executor-client tagless sync failed; will retry next tick");
                                    continue;
                                }
                                if claim_retry_at.is_some_and(|at| tokio::time::Instant::now() < at) {
                                    continue;
                                }
                                claim_retry_at = claim_or_back_off(&sync_client, &sync_adapter, solver_id).await;
                            }
                        }
                    }
                });

                let _ = exec_ready_tx.send(Ok(()));
                // Unexpected exit of either task → propagate a global shutdown
                // immediately (the `exec_rx`-drop → matcher path only fires on
                // the *next* batch send, and never if only the sync task dies).
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = &mut executor_handle => {
                        tracing::error!("executor task exited unexpectedly; triggering shutdown");
                        cancel.cancel();
                    }
                    _ = &mut executor_sync_handle => {
                        tracing::error!("executor-sync task exited unexpectedly; triggering shutdown");
                        cancel.cancel();
                    }
                }
                // Drain inside the runtime so the executor `Client` Arc refs
                // drop here (runtime still entered), not in `LocalSet::drop`
                // after `block_on` returns (which would panic in the `!Send`
                // Client's destructor with no runtime context).
                //
                // The `is_finished()` guard is load-bearing: if the `select!`
                // above ended via a `&mut *_handle` arm, that handle was
                // already polled to completion there. A `JoinHandle` is a
                // one-shot future — awaiting it again panics with "JoinHandle
                // polled after completion". So only `.await` the handles the
                // `select!` did NOT already drive to completion; `abort()` on a
                // finished task is a harmless no-op.
                executor_handle.abort();
                executor_sync_handle.abort();
                if !executor_handle.is_finished() {
                    let _ = executor_handle.await;
                }
                if !executor_sync_handle.is_finished() {
                    let _ = executor_sync_handle.await;
                }
            });
        })
        .context("spawn executor thread")?;
    Ok((executor_thread, exec_ready_rx))
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use crate::types::FilledNote;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::{NoteId, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::PswapNoteStorage;

    const BASE_FEE: u32 = 7;

    #[test]
    fn rpc_rejection_is_not_an_unknown_submission() {
        let error = ClientError::RpcError(miden_client::rpc::RpcError::InvalidNodeEndpoint(
            "mock rejection".to_string(),
        ));
        assert!(!submission_outcome_is_unknown(&error));
    }

    fn fee_faucet() -> AccountId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
    }

    fn other_faucet() -> AccountId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
    }

    /// A public P2ID note paying the solver `amount` of `faucet`.
    fn p2id(faucet: AccountId, amount: u64, rng: &mut RandomCoin) -> Note {
        P2idNote::builder()
            .sender(faucet)
            .target(
                ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
                    .try_into()
                    .unwrap(),
            )
            .assets(vec![FungibleAsset::new(faucet, amount).unwrap()])
            .note_type(NoteType::Public)
            .generate_serial_number(rng)
            .build()
            .unwrap()
            .into()
    }

    fn ids(notes: &[Note]) -> Vec<NoteId> {
        notes.iter().map(Note::id).collect()
    }

    #[test]
    fn executor_rechecks_surplus_before_and_after_splitting() {
        let base = fee_faucet();
        let quote = other_faucet();
        let solver_id = ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
            .try_into()
            .unwrap();
        let mut rng = RandomCoin::new(Word::default());
        let creator =
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
                .try_into()
                .unwrap();
        let mut make_note = |offered, requested| -> Note {
            let storage = PswapNoteStorage::builder()
                .min_requested_asset(requested)
                .min_fill_step(AssetAmount::new(1).unwrap())
                .creator_account_id(creator)
                .build();
            PswapNote::builder()
                .sender(solver_id)
                .storage(storage)
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(offered)
                .build()
                .unwrap()
                .into()
        };
        let seller = make_note(
            FungibleAsset::new(base, 11).unwrap(),
            FungibleAsset::new(quote, 18).unwrap(),
        );
        let buyer = make_note(
            FungibleAsset::new(quote, 22).unwrap(),
            FungibleAsset::new(base, 10).unwrap(),
        );
        let batch = ExecutionBatch {
            group_ends: Vec::new(),
            filled_notes: [(seller, 20), (buyer, 10)]
                .into_iter()
                .enumerate()
                .map(|(index, (note, requested_filled))| FilledNote {
                    note_id: note.id(),
                    priority_seq: index as u64 + 1,
                    requested_filled,
                    note: Arc::new(note),
                    arrival_unix: 1,
                })
                .collect(),
        };
        let components = BatchComponents::prepare(&batch, solver_id).unwrap();
        assert_eq!(components.inputs.len(), 2);
        for (input, filled) in components.inputs.iter().zip(&batch.filled_notes) {
            assert_eq!(input.note.id(), filled.note_id);
            assert!(input.remainder.is_none());
        }
        let sources = batch.book_orders();
        for (source, filled) in sources.iter().zip(&batch.filled_notes) {
            assert_eq!(source.id(), filled.note_id);
            assert_eq!(source.priority_seq, filled.priority_seq);
            assert!(Arc::ptr_eq(&source.note, &filled.note));
        }
        let residual: HashMap<_, _> = components
            .surplus_assets
            .into_iter()
            .filter_map(|asset| {
                asset
                    .as_fungible()
                    .map(|fungible| (fungible.faucet_id(), fungible.amount().as_u64()))
            })
            .collect();
        assert_eq!(residual.get(&base), Some(&1));
        assert_eq!(residual.get(&quote), Some(&2));

        let mut partial = batch.clone();
        partial.filled_notes[0].requested_filled = 9;
        partial.filled_notes[1].requested_filled = 5;
        let components = BatchComponents::prepare(&partial, solver_id).unwrap();
        assert_eq!(components.expected_output_recipients.len(), 4);
        for input in &components.inputs {
            let remainder = input.remainder.as_ref().unwrap();
            assert_ne!(remainder.id(), input.note.id());
            assert_ne!(input.payback_id, remainder.id());
        }

        let mut insolvent = batch.clone();
        insolvent.filled_notes[0].requested_filled = 23;
        assert!(BatchComponents::prepare(&insolvent, solver_id).is_err());

        // Cross the transaction bound with independently solvent groups of
        // real, distinct PSWAP notes, not just synthetic note-count metadata.
        let mut combined = ExecutionBatch {
            filled_notes: Vec::new(),
            group_ends: Vec::new(),
        };
        for _ in 0..=MAX_SETTLEMENT_INPUTS / 2 {
            let seller = make_note(
                FungibleAsset::new(base, 11).unwrap(),
                FungibleAsset::new(quote, 18).unwrap(),
            );
            let buyer = make_note(
                FungibleAsset::new(quote, 22).unwrap(),
                FungibleAsset::new(base, 10).unwrap(),
            );
            for (note, payment) in [(seller, 20), (buyer, 10)] {
                combined.filled_notes.push(FilledNote {
                    note_id: note.id(),
                    priority_seq: combined.filled_notes.len() as u64 + 1,
                    requested_filled: payment,
                    note: Arc::new(note),
                    arrival_unix: 1,
                });
            }
            combined.group_ends.push(combined.filled_notes.len());
        }
        let transactions = split_batch(&mut combined).unwrap();
        assert_eq!(transactions.len(), 2);
        for tx in transactions {
            let components = BatchComponents::prepare(&tx, solver_id).unwrap();
            assert!(components.inputs.len() <= miden_protocol::MAX_INPUT_NOTES_PER_TX);
            assert!(
                components.expected_output_recipients.len()
                    < miden_protocol::MAX_OUTPUT_NOTES_PER_TX
            );
        }
    }

    fn sized_batch(group_sizes: &[usize]) -> ExecutionBatch {
        let mut group_ends = Vec::new();
        let mut filled_notes = Vec::new();
        let mut rng = RandomCoin::new(Word::default());
        for &size in group_sizes {
            for _ in 0..size {
                let index = filled_notes.len() as u64 + 1;
                let note = Arc::new(p2id(fee_faucet(), 1, &mut rng));
                filled_notes.push(FilledNote {
                    note_id: note.id(),
                    priority_seq: index,
                    requested_filled: 1,
                    note,
                    arrival_unix: 1,
                });
            }
            group_ends.push(filled_notes.len());
        }
        ExecutionBatch {
            filled_notes,
            group_ends,
        }
    }

    #[test]
    fn executor_splits_all_five_pairs_without_splitting_counterparties() {
        let group_size = crate::clearing::ClearingConfig::default().max_orders_per_side * 2;
        let mut batch = sized_batch(&[group_size; 5]);
        let expected: Vec<_> = batch.filled_notes.iter().map(|note| note.note_id).collect();
        let transactions = split_batch(&mut batch).unwrap();
        assert!(transactions.len() > 1);
        for tx in &transactions {
            assert!(tx.filled_notes.len() <= MAX_SETTLEMENT_INPUTS);
            assert_eq!(tx.filled_notes.len() % group_size, 0);
        }
        assert_eq!(
            transactions
                .iter()
                .flat_map(|tx| tx.filled_notes.iter().map(|note| note.note_id))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(batch.filled_notes.is_empty());
    }

    #[test]
    fn executor_packs_small_pairs_together_and_preserves_legacy_batches() {
        for grouped in [true, false] {
            let mut batch = sized_batch(&[2, 2, 2]);
            if !grouped {
                batch.group_ends.clear();
            }
            let transactions = split_batch(&mut batch).unwrap();
            assert_eq!(transactions.len(), 1);
            assert_eq!(transactions[0].filled_notes.len(), 6);
        }
    }

    #[test]
    fn executor_rejects_invalid_boundaries_without_losing_notes() {
        for ends in [vec![2, 2], vec![2, 5], vec![2], vec![0, 4]] {
            let mut batch = sized_batch(&[2, 2]);
            batch.group_ends = ends;
            assert!(split_batch(&mut batch).is_err());
            assert_eq!(batch.filled_notes.len(), 4);
        }
        let mut oversized = sized_batch(&[MAX_SETTLEMENT_INPUTS + 1]);
        assert!(split_batch(&mut oversized).is_err());
        assert_eq!(oversized.filled_notes.len(), MAX_SETTLEMENT_INPUTS + 1);
    }

    /// Only notes carrying at least one settlement's worst-case fee in the fee
    /// asset are claimed, so dust can't drain the solver through claim fees.
    #[test]
    fn claims_only_notes_worth_their_claim_fee() {
        let mut rng = RandomCoin::new(Word::default());
        let need = u64::from(BASE_FEE) * FEE_HEADROOM_MULTIPLIER;
        let enough = p2id(fee_faucet(), need, &mut rng);
        let dust = p2id(fee_faucet(), need - 1, &mut rng);
        let no_fee_asset = p2id(other_faucet(), 1_000_000, &mut rng);

        let picked = select_claimable(
            vec![enough.clone(), dust, no_fee_asset],
            Some((fee_faucet(), BASE_FEE)),
        );

        assert_eq!(ids(&picked), vec![enough.id()]);
    }

    /// With no fee to pay (or no fee parameters reported), every incoming P2ID
    /// qualifies.
    #[test]
    fn a_fee_free_chain_claims_every_incoming_note() {
        let mut rng = RandomCoin::new(Word::default());
        let notes = vec![
            p2id(fee_faucet(), 1, &mut rng),
            p2id(other_faucet(), 1, &mut rng),
        ];

        assert_eq!(
            select_claimable(notes.clone(), Some((fee_faucet(), 0))).len(),
            2
        );
        assert_eq!(select_claimable(notes, None).len(), 2);
    }

    /// Time-locked notes are left for a later tick instead of failing the claim.
    #[test]
    fn only_notes_spendable_now_are_claimed() {
        use miden_protocol::block::BlockNumber;
        assert!(spendable_now(&NoteConsumptionStatus::Consumable));
        assert!(spendable_now(
            &NoteConsumptionStatus::ConsumableWithAuthorization
        ));
        assert!(!spendable_now(&NoteConsumptionStatus::ConsumableAfter(
            BlockNumber::from(10_u32)
        )));
        assert!(!spendable_now(
            &NoteConsumptionStatus::UnconsumableConditions
        ));
        assert!(!spendable_now(&NoteConsumptionStatus::NeverConsumable(
            "not for us".into()
        )));
    }

    #[test]
    fn claims_are_capped_per_transaction() {
        let mut rng = RandomCoin::new(Word::default());
        let notes: Vec<Note> = (0..MAX_CLAIM_NOTES + 5)
            .map(|_| p2id(fee_faucet(), 1_000_000, &mut rng))
            .collect();

        assert_eq!(
            select_claimable(notes, Some((fee_faucet(), BASE_FEE))).len(),
            MAX_CLAIM_NOTES
        );
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::db::postgres_models::NewOrderRow;
    use crate::db::postgres_schema::{orders, settlement_attempts};
    use crate::db::postgres_test::TestDb;
    use crate::ingest::ChainResult;
    use crate::ingest::SyncResult;
    use crate::types::OrderStatus;
    use anyhow::Result;
    use diesel::prelude::*;
    use miden_protocol::asset::AssetAmount;
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::PswapNoteStorage;

    struct RecoveryAdapter {
        consumed: bool,
        fail_nullifier_lookup: bool,
    }

    #[async_trait::async_trait(?Send)]
    impl MidenClient for RecoveryAdapter {
        async fn subscribe_pair(&mut self, _: TokenId, _: TokenId) -> ChainResult<()> {
            Ok(())
        }

        async fn sync_state(&mut self) -> ChainResult<SyncResult> {
            Err(ChainError::Test(
                "unexpected sync in rejected-settlement recovery test",
            ))
        }

        async fn stored_notes(&mut self) -> ChainResult<Vec<Note>> {
            Ok(Vec::new())
        }

        async fn check_consumed_notes(&mut self, notes: &[Note]) -> ChainResult<HashSet<NoteId>> {
            if self.fail_nullifier_lookup {
                return Err(ChainError::Test("nullifier lookup unavailable"));
            }
            Ok(if self.consumed {
                notes.iter().map(Note::id).collect()
            } else {
                HashSet::new()
            })
        }

        async fn transaction_committed(
            &mut self,
            _: AccountId,
            _: TransactionId,
            _: BlockNumber,
            _: BlockNumber,
        ) -> ChainResult<bool> {
            Ok(false)
        }

        async fn fetch_token_metadata(&mut self, _: TokenId) -> ChainResult<Option<(u8, String)>> {
            Ok(None)
        }
    }

    fn parent_note() -> Result<Note> {
        parent_note_with(Word::default())
    }

    fn parent_note_with(serial: Word) -> Result<Note> {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
        let sender = ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE.try_into()?;
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        let storage = PswapNoteStorage::builder()
            .min_requested_asset(FungibleAsset::new(requested, 10)?)
            .min_fill_step(AssetAmount::new(1)?)
            .creator_account_id(creator)
            .build();
        Ok(PswapNote::builder()
            .sender(sender)
            .storage(storage)
            .serial_number(serial)
            .note_type(NoteType::Public)
            .offered_asset(FungibleAsset::new(offered, 11)?)
            .build()?
            .into())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn rejected_settlement_release_uses_nullifier_evidence_before_book_update() -> Result<()>
    {
        for (consumed, fail_lookup, expected_status) in [
            (false, false, "active"),
            (true, false, "onchain_nullified"),
            (false, true, "settling"),
        ] {
            let test_db = TestDb::new().await?;
            let pool = test_db.pool.clone();
            let parent = parent_note()?;
            let parent_id = parent.id();
            let tx_id = TransactionId::new(
                Word::default(),
                Word::default(),
                Word::default(),
                Word::default(),
            );
            let attempt = SettlementAttemptRow {
                tx_id: tx_id.to_bytes().to_vec(),
                tx_result: vec![1],
                status: SettlementStatus::Prepared.as_str().into(),
            };
            let attempt_for_write = attempt.clone();
            let order_row = NewOrderRow::ingested(&parent, 1)?;
            let input = SettlementInputRow {
                tx_id: attempt.tx_id.clone(),
                parent_note_id: parent_id.to_bytes().to_vec(),
                child_note_id: None,
                child_note_data: None,
            };
            pool.write(move |conn| {
                db::postgres_db::insert_orders_batch_tx(conn, &[order_row], 1)?;
                db::postgres_db::prepare_settlement_tx(conn, &attempt_for_write, &[input])?;
                db::postgres_db::mark_settlement_rejected_tx(conn, &attempt_for_write.tx_id)
            })
            .await?;

            let mut rejected_attempt = attempt;
            rejected_attempt.status = SettlementStatus::Rejected.as_str().into();
            let settlement = PendingSettlement {
                tx_id,
                attempt: rejected_attempt,
                parent_notes: vec![parent],
                child_notes: Vec::new(),
                retry_at: None,
            };
            let adapter: Arc<Mutex<dyn MidenClient>> = Arc::new(Mutex::new(RecoveryAdapter {
                consumed,
                fail_nullifier_lookup: fail_lookup,
            }));
            let (book_tx, mut book_rx) = mpsc::channel(1);
            let resolved =
                release_rejected_settlement(&adapter, &pool, &settlement, &book_tx).await?;
            assert_eq!(resolved, !fail_lookup);

            let parent_bytes = parent_id.to_bytes().to_vec();
            let (status, unresolved) = pool
                .read(move |conn| {
                    let status = orders::table
                        .find(parent_bytes)
                        .select(orders::status)
                        .first::<String>(conn)?;
                    let unresolved = settlement_attempts::table.count().get_result::<i64>(conn)?;
                    Ok((status, unresolved))
                })
                .await?;
            assert_eq!(status, expected_status);
            assert_eq!(unresolved, i64::from(fail_lookup));
            if fail_lookup {
                assert!(book_rx.try_recv().is_err());
            } else {
                let update = book_rx.try_recv()?;
                assert_eq!(update.active.len(), usize::from(!consumed));
                assert_eq!(update.removed.len(), usize::from(consumed));
            }
        }
        Ok(())
    }

    fn tagged(tag: usize) -> ExecutionBatch {
        ExecutionBatch {
            filled_notes: Vec::new(),
            group_ends: vec![tag],
        }
    }

    fn tags(batches: &[ExecutionBatch]) -> Vec<usize> {
        batches.iter().map(|batch| batch.group_ends[0]).collect()
    }

    #[test]
    fn verification_holds_the_failed_tick_and_retries_it_in_order() {
        let mut verification = Verification::default();
        assert!(verification.take_retries().is_empty());

        // Pausing on transaction 1 holds it and the rest of its tick.
        verification.pause([tagged(1), tagged(2), tagged(3)]);
        assert!(verification.active);
        assert!(
            verification.take_retries().is_empty(),
            "nothing runs while verifying"
        );

        verification.resume();
        assert_eq!(tags(&verification.take_retries()), vec![1, 2, 3]);
        assert!(
            verification.take_retries().is_empty(),
            "retried exactly once"
        );

        // An unreturned give-back pauses without retrying anything.
        verification.pause(std::iter::empty());
        assert!(verification.active);
        verification.resume();
        assert!(verification.take_retries().is_empty());
    }

    #[test]
    fn node_outcome_waits_until_the_validity_window_has_passed() {
        let expiration = BlockNumber::from(100_u32);
        assert_eq!(
            node_outcome(true, BlockNumber::from(50_u32), expiration, true),
            TxOutcome::Committed
        );
        assert_eq!(
            node_outcome(false, BlockNumber::from(100_u32), expiration, true),
            TxOutcome::Unknown {
                resubmittable: true
            },
            "still valid in its expiration block"
        );
        assert_eq!(
            node_outcome(false, BlockNumber::from(101_u32), expiration, false),
            TxOutcome::NeverCommits
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn release_held_returns_live_orders_and_retires_consumed_ones() -> Result<()> {
        for (consumed, fail_lookup) in [(false, false), (true, false), (false, true)] {
            let test_db = TestDb::new().await?;
            let pool = test_db.pool.clone();
            let notes = [
                parent_note_with(Word::from([1_u32, 0, 0, 0]))?,
                parent_note_with(Word::from([2_u32, 0, 0, 0]))?,
            ];
            let rows: Vec<_> = notes
                .iter()
                .map(|note| NewOrderRow::ingested(note, 1))
                .collect::<crate::db::DbResult<_>>()?;
            pool.write(move |conn| {
                db::postgres_db::insert_orders_batch_tx(conn, &rows, 1)?;
                Ok(())
            })
            .await?;
            let mut held: Vec<_> = notes
                .iter()
                .map(|note| BookOrder {
                    priority_seq: 1,
                    arrival_unix: 1,
                    note: Arc::new(note.clone()),
                })
                .collect();
            let adapter: Arc<Mutex<dyn MidenClient>> = Arc::new(Mutex::new(RecoveryAdapter {
                consumed,
                fail_nullifier_lookup: fail_lookup,
            }));
            let (book_tx, mut book_rx) = mpsc::channel(1);

            let released = release_held(&adapter, &pool, &book_tx, &mut held).await;
            if fail_lookup {
                assert!(released.is_err());
                assert_eq!(held.len(), 2, "kept for the next verification tick");
                assert!(book_rx.try_recv().is_err());
                continue;
            }
            released?;
            assert!(held.is_empty());
            let update = book_rx.try_recv()?;
            let expected_status = if consumed {
                assert_eq!(update.removed.len(), 2);
                assert!(update.active.is_empty());
                OrderStatus::OnchainNullified
            } else {
                assert_eq!(update.active.len(), 2);
                OrderStatus::Active
            };
            let statuses = pool
                .read(|conn| Ok(orders::table.select(orders::status).load::<String>(conn)?))
                .await?;
            assert!(statuses
                .iter()
                .all(|status| status == expected_status.as_str()));
        }
        Ok(())
    }
}

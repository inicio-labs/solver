use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use consume_script::ConsumeAssetScript;
use miden_client::store::TransactionFilter;
use miden_client::transaction::{
    DiscardCause, NoteArgs, TransactionRequest, TransactionRequestBuilder, TransactionResult,
    TransactionStatus,
};
use miden_client::ClientError;
use miden_client::{keystore::FilesystemKeyStore, Client};
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    crypto::utils::{Deserializable, Serializable, SliceReader},
    note::{Note, NoteId, NoteRecipient},
    transaction::{InputNote, TransactionId},
};
use miden_standards::note::{NoteConsumptionStatus, P2idNote, P2ideNote, PswapNote};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::db::models::{SettlementAttemptRow, SettlementInputRow};
use crate::db::{self, DbPool};
use crate::ingest::{MidenClient, MidenClientAdapter};
use crate::swap_eta::SettlementStats;
use crate::types::{BookUpdate, ExecutionBatch, IngestOrder, TokenId};

// ── Backoff knobs ──────────────────────────────────────────────────────────

/// Maximum number of retry attempts for transient RPC errors. The first
/// submit is "attempt 0," so the helper makes up to `MAX_RPC_RETRIES + 1`
/// total submit calls before giving up.
const MAX_RPC_RETRIES: u32 = 5;

/// Initial backoff sleep on the first RPC retry. Doubles each attempt.
const INITIAL_RPC_BACKOFF: Duration = Duration::from_millis(500);

/// Maximum per-attempt backoff sleep. Caps `INITIAL_RPC_BACKOFF * 2^n`.
const MAX_RPC_BACKOFF: Duration = Duration::from_secs(30);

/// Upper bound on one transaction's fee, in multiples of the verification base
/// fee: `fee = base × (⌊log2 cycles⌋ + 1)` ≤ 30 × base at the 2^29 cycle cap,
/// and the auth procedure may reserve up to 2×.
const FEE_HEADROOM_MULTIPLIER: u64 = 64;

/// How long a batch the executor hands back unsettled (no fee headroom, or
/// failed preparation) waits before its orders return to the matcher, so a
/// persistent condition isn't rebuilt and rejected every tick.
const HELD_REFEED_DELAY: Duration = Duration::from_secs(30);

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

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Outcome of `submit_with_rpc_backoff`. The executor's main path branches on
/// this to decide the post-submit state-machine transition.
enum SubmitOutcome {
    /// Accepted into the mempool; confirmation is handled separately.
    Success,
    /// Submission may have landed, or its local store update failed. Keep
    /// inputs reserved until chain reconciliation establishes the outcome.
    Pending(String),
    /// Submit failed with a non-RPC error (i.e., chain-side rejection).
    /// Classify per-note via the nullifier check, mark consumed orders
    /// OnchainNullified, re-feed the rest to the matcher.
    TxError(ClientError, Option<Vec<u8>>),
    /// `build_tx_request` failed deterministically for this batch
    /// composition. The submit never landed, so every order is still valid:
    /// revert to Active and re-feed so the live matcher reconsiders them
    /// (it dropped them on emit and never re-reads the DB mid-run).
    BuildFailed(String),
    /// Cancellation during backoff leaves the attempted transaction reserved.
    Cancelled,
}

enum BatchSubmission {
    Accepted,
    Uncertain,
    Returned,
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

impl BatchComponents {
    /// Parse each input once and predict its outputs. Exact per-token balances
    /// must be solvent before the transaction can be built.
    fn prepare(batch: &ExecutionBatch, solver_id: AccountId) -> Result<Self> {
        let mut inputs = Vec::with_capacity(batch.filled_notes.len());
        let mut expected_output_recipients = Vec::new();

        // Net flow per token: positive = surplus staying with solver, negative = insolvent.
        // i128 is lossless for any u64 sum encountered here — no wrap risk on `as i128`.
        let mut flow: HashMap<TokenId, i128> = HashMap::new();

        for filled in &batch.filled_notes {
            let note = Arc::clone(&filled.note);

            let pswap = PswapNote::try_from(note.as_ref())
                .map_err(|e| anyhow!("failed to parse PswapNote: {}", e))?;

            let offered_asset = pswap.offered_asset();
            let offered_token = offered_asset.faucet_id();
            let requested_token = pswap.storage().requested_faucet_id();

            *flow.entry(offered_token).or_default() += u64::from(offered_asset.amount()) as i128;

            let fill_asset = FungibleAsset::new(requested_token, filled.requested_filled)
                .map_err(|e| anyhow!("failed to create fill asset: {}", e))?;

            // Both-zero args make the script fall back to a vault-funded full fill.
            if filled.requested_filled == 0 {
                bail!("zero fill for note {}", filled.note_id);
            }
            let note_args = PswapNote::create_args(0, filled.requested_filled)
                .map_err(|e| anyhow!("failed to create note args: {}", e))?;

            let (p2id, remainder) = pswap
                .execute(solver_id, None, Some(fill_asset))
                .map_err(|e| anyhow!("pswap execute failed: {}", e))?;
            let payback_id = Note::from(p2id.clone()).id();

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
                *flow.entry(offered_token).or_default() -= note_asset_amount(&rem_note) as i128;
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
                bail!(
                    "insolvent batch: token {:?} has deficit of {}",
                    token,
                    net.abs()
                );
            }
            if *net > 0 {
                let amount = u64::try_from(*net).map_err(|_| {
                    anyhow!("surplus exceeds u64 range for token {:?}: {}", token, net)
                })?;
                surplus_assets.push(
                    FungibleAsset::new(*token, amount)
                        .map_err(|e| anyhow!("surplus asset: {}", e))?
                        .into(),
                );
            }
        }

        Ok(Self {
            inputs,
            expected_output_recipients,
            surplus_assets,
        })
    }

    fn request(&self) -> Result<TransactionRequest> {
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
        }

        builder
            .build()
            .context("failed to build transaction request")
    }

    fn settlement_inputs(&self, result: &TransactionResult) -> Result<Vec<SettlementInputRow>> {
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
                anyhow::ensure!(
                    output_ids.contains(&payback_id),
                    "expected payback {} absent from executed outputs",
                    payback_id
                );
                if let Some(note) = remainder {
                    anyhow::ensure!(
                        output_ids.contains(&note.id()),
                        "expected remainder {} absent from executed outputs",
                        note.id()
                    );
                }
                Ok(SettlementInputRow {
                    tx_id: tx_id.clone(),
                    parent_note_id: input.note.id().to_bytes().to_vec(),
                    payback_note_id: payback_id.to_bytes().to_vec(),
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
        Err(error) => return SubmitOutcome::BuildFailed(error.to_string()),
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
        Err(error) => return SubmitOutcome::BuildFailed(error.to_string()),
    };
    let proven = match client.lock().await.prove_transaction(&result).await {
        Ok(proven) => proven,
        Err(error) => return SubmitOutcome::TxError(error, None),
    };
    let tx_id = result.id().to_bytes().to_vec();
    let attempt = SettlementAttemptRow {
        tx_id: tx_id.clone(),
        tx_result: result.to_bytes(),
        status: "prepared".to_string(),
    };
    let persisted = pool
        .write_conn()
        .map_err(anyhow::Error::from)
        .and_then(|mut conn| db::prepare_settlement(&mut conn, &attempt, &inputs));
    if let Err(error) = persisted {
        return SubmitOutcome::BuildFailed(format!(
            "persist settlement before submission: {error}"
        ));
    }

    let mut backoff = INITIAL_RPC_BACKOFF;
    let mut unknown_seen = false;
    for attempt in 0..=MAX_RPC_RETRIES {
        let submit_res = {
            let mut c = client.lock().await;
            c.submit_proven_transaction(proven.clone(), &result).await
        };
        match submit_res {
            Ok(height) => {
                if let Ok(mut conn) = pool.write_conn() {
                    if let Err(error) = db::mark_settlement_submitted(&mut conn, &tx_id) {
                        tracing::error!(%error, "accepted settlement status write failed");
                    }
                }
                return match client.lock().await.apply_transaction(&result, height).await {
                    Ok(()) => SubmitOutcome::Success,
                    Err(error) => SubmitOutcome::Pending(format!(
                        "transaction accepted but local store update failed: {error}"
                    )),
                };
            }
            Err(e) if matches!(e, ClientError::SubmissionOutcomeUnknown { .. }) => {
                unknown_seen = true;
                if attempt == MAX_RPC_RETRIES {
                    return SubmitOutcome::Pending(format!(
                        "submission outcome unknown for {}: {e}",
                        result.id()
                    ));
                }
                tracing::warn!(attempt, error = %e, "submission outcome unknown; retrying same transaction");
            }
            Err(e) if matches!(e, ClientError::RpcError(_)) => {
                // The node may have accepted the request before the connection
                // failed. A later rejection cannot disprove that first copy.
                unknown_seen = true;
                if attempt == MAX_RPC_RETRIES {
                    return SubmitOutcome::Pending(format!(
                        "submission of {} may have landed despite RPC failure: {e}",
                        result.id()
                    ));
                }
                tracing::warn!(
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "transient RPC error, backing off"
                );
            }
            Err(e) if unknown_seen => {
                return SubmitOutcome::Pending(format!(
                    "earlier submission of {} may have landed; later rejection: {e}",
                    result.id()
                ));
            }
            Err(e) => return SubmitOutcome::TxError(e, Some(tx_id)),
        }
        tokio::select! {
            _ = cancel.cancelled() => return SubmitOutcome::Cancelled,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_RPC_BACKOFF);
    }
    unreachable!("loop exits via return inside the matched arms")
}

/// Shutdown-aware re-feed into the matcher via the same channel ingest uses.
/// Stops early if the matcher channel is closed (it's tearing down).
async fn refeed_orders(
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    orders: Vec<IngestOrder>,
) -> Result<()> {
    pool.update_book(book_tx, |conn| db::active_book_update(conn, orders))
        .await
}

/// On the TxError classification path, fetch which input notes are consumed
/// on-chain. Keep IDs typed until the database boundary.
async fn classify_input_notes(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    batch: &ExecutionBatch,
    input_notes: &[Note],
) -> Result<(HashSet<NoteId>, Vec<IngestOrder>)> {
    let consumed_ids = {
        let mut adapter = miden_adapter.lock().await;
        adapter.check_consumed_notes(input_notes).await?
    };

    let mut active_orders: Vec<IngestOrder> = Vec::new();

    for filled in &batch.filled_notes {
        if !consumed_ids.contains(&filled.note_id) {
            active_orders.push(filled.to_ingest_order());
        }
    }

    Ok((consumed_ids, active_orders))
}

/// DIAGNOSTIC (temporary): on a tx failure, dump EVERYTHING about the batch consume.
/// For each note being consumed: id / details-commitment / serial / nullifier / assets /
/// attachments / offered+requested; whether its nullifier is already consumed on-chain;
/// whether it exists in our executor store (looked up by id AND by details-commitment, to
/// catch a row stored under a DIFFERENT id); and an explicit MATCH of the store copy vs the
/// note we're actually consuming (state, commitment, attachments). Then the COMPLETE VM/tx
/// error — Display + full nested Debug + the whole source chain. Nothing truncated.
async fn log_batch_consume_diagnostics(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    notes: &[Note],
    error: &ClientError,
) {
    // On-chain "ever consumed?" nullifier check for the whole set, one query.
    let consumed_set = {
        let mut a = miden_adapter.lock().await;
        a.check_consumed_notes(notes).await.unwrap_or_default()
    };
    // Store records: by-id lookup, plus a full scan to catch a row stored under a DIFFERENT id.
    let by_id = {
        let ids: Vec<_> = notes.iter().map(|n| n.id()).collect();
        let c = client.lock().await;
        c.get_input_notes(miden_client::store::NoteFilter::List(ids))
            .await
            .unwrap_or_default()
    };
    let all_store = {
        let c = client.lock().await;
        c.get_input_notes(miden_client::store::NoteFilter::All)
            .await
            .unwrap_or_default()
    };

    tracing::error!(
        note_count = notes.len(),
        "================ BATCH CONSUME DIAGNOSTICS ================"
    );

    for (i, note) in notes.iter().enumerate() {
        let id = note.id();
        let commitment_hex = note.details_commitment().to_hex();
        let pswap = PswapNote::try_from(note).ok();
        let (offered, requested) = match &pswap {
            Some(p) => (
                format!("{:?}", p.offered_asset()),
                format!(
                    "faucet={} amount={}",
                    p.storage().requested_faucet_id(),
                    p.storage().min_requested_asset().amount().as_u64()
                ),
            ),
            None => (
                "<non-PSWAP (p2id payback?)>".to_string(),
                "<n/a>".to_string(),
            ),
        };

        // (1) The note we are TRYING TO CONSUME (rebuilt from the order-book raw bytes).
        tracing::error!(
            idx = i,
            note_id = %id,
            details_commitment = %commitment_hex,
            serial_number = ?note.serial_num(),
            nullifier = %note.nullifier(),
            nullifier_consumed_onchain = consumed_set.contains(&id),
            attachments_count = note.attachments().num_attachments(),
            attachments = ?note.attachments(),
            assets = ?note.assets(),
            offered = %offered,
            requested = %requested,
            "CONSUMING NOTE"
        );

        // (2) In our store under the SAME id?  (3) ... or under a DIFFERENT id but same details?
        let by_id_hit = by_id.iter().find(|r| r.id() == Some(id));
        let by_commitment_hit = all_store
            .iter()
            .find(|r| r.details_commitment().to_hex() == commitment_hex);
        match (by_id_hit, by_commitment_hit) {
            (Some(r), _) => {
                let attachments_match = r.attachments() == note.attachments();
                let verdict = if attachments_match {
                    "IDENTICAL"
                } else {
                    "*** ATTACHMENTS MISMATCH ***"
                };
                tracing::error!(
                    note_id = %id,
                    store_state = ?r.state(),
                    store_details_commitment = %r.details_commitment().to_hex(),
                    store_attachments_count = r.attachments().num_attachments(),
                    store_attachments = ?r.attachments(),
                    store_inclusion_proof = ?r.inclusion_proof(),
                    attachments_match,
                    verdict,
                    "  -> IN STORE (found by id): store copy vs consumed note"
                );
            }
            (None, Some(r)) => tracing::error!(
                consumed_note_id = %id,
                store_note_id = ?r.id(),
                store_state = ?r.state(),
                consumed_attachments_count = note.attachments().num_attachments(),
                store_attachments_count = r.attachments().num_attachments(),
                consumed_attachments = ?note.attachments(),
                store_attachments = ?r.attachments(),
                store_inclusion_proof = ?r.inclusion_proof(),
                "  -> *** ID DRIFT: store has this note under a DIFFERENT id (same details) — body differs ***"
            ),
            (None, None) => tracing::error!(
                note_id = %id,
                "  -> NOT IN STORE (neither by id nor by details) -> consumed UNAUTHENTICATED"
            ),
        }
    }

    // (4) THE WHOLE VM / TX ERROR — every word.
    tracing::error!("================ FULL VM / TX ERROR (Display) ================");
    tracing::error!("{}", error);
    tracing::error!(
        "================ FULL VM / TX ERROR (pretty Debug, complete nested) ================"
    );
    tracing::error!("{:#?}", error);
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
    let mut depth = 0u32;
    while let Some(s) = src {
        tracing::error!(depth, "  caused by: {}", s);
        src = s.source();
        depth += 1;
    }
    tracing::error!("================ END BATCH CONSUME DIAGNOSTICS ================");
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
    mut exec_rx: mpsc::Receiver<ExecutionBatch>,
    book_tx: mpsc::Sender<BookUpdate>,
    // In-memory swap-eta settlement-time window; republished on each success.
    stats_tx: watch::Sender<Arc<SettlementStats>>,
    cancel: CancellationToken,
) {
    // Owned here (executor thread) and published over `stats_tx`. Ephemeral —
    // no DB persistence; rebuilds after a restart.
    let mut stats = SettlementStats::new();
    let mut reconcile_tick = tokio::time::interval(SETTLEMENT_RECONCILE_INTERVAL);
    let mut recovery_retry_at = HashMap::new();
    loop {
        // Cancellation is only checked BETWEEN batches. Once execute_batch
        // starts, only the backoff sleep is cancel-aware — the on-chain submit
        // runs to completion before a result is observed.
        let mut batch = tokio::select! {
            _ = cancel.cancelled() => break,
            _ = reconcile_tick.tick() => {
                if let Err(error) = reconcile_settlements(&client, &miden_adapter, &pool, &book_tx, &mut recovery_retry_at).await {
                    tracing::warn!(%error, "settlement reconciliation failed");
                }
                continue;
            }
            opt = exec_rx.recv() => match opt {
                Some(b) => b,
                None => break,  // channel closed → upstream is gone
            },
        };

        if batch.filled_notes.is_empty() {
            continue;
        }

        let transactions = match split_batch(&mut batch) {
            Ok(transactions) => transactions,
            Err(error) => {
                tracing::error!(%error, "cannot split execution batch safely; returning orders");
                refeed_unprepared(&pool, &batch, &book_tx, &cancel);
                continue;
            }
        };
        for batch in transactions {
            if cancel.is_cancelled() {
                // Unsubmitted groups remain Active in the DB for boot recovery.
                break;
            }
            let result = execute_batch(
                &client,
                &miden_adapter,
                solver_id,
                &pool,
                &batch,
                &book_tx,
                &cancel,
            )
            .await;

            match result {
                Ok(BatchSubmission::Accepted) => {
                    tracing::info!(
                        notes = batch.filled_notes.len(),
                        "batch accepted for settlement"
                    );
                    record_settlement(&batch, &mut stats, &stats_tx);
                }
                Ok(BatchSubmission::Uncertain) => {
                    tracing::warn!(
                        notes = batch.filled_notes.len(),
                        "batch submission outcome uncertain"
                    );
                }
                Ok(BatchSubmission::Returned) => {}
                Err(e) => {
                    tracing::error!(error = %e, notes = batch.filled_notes.len(), "batch recovery failed; stopping pipeline");
                    cancel.cancel();
                    return;
                }
            }
        }
    }

    tracing::info!("executor shutting down");
}

/// The executor client syncs independently. Once its local transaction record
/// says Committed, atomically hand the original FIFO slot to the child note.
async fn reconcile_settlements(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    recovery_retry_at: &mut HashMap<Vec<u8>, tokio::time::Instant>,
) -> Result<()> {
    let attempts = {
        let mut conn = pool.read_conn()?;
        db::unresolved_settlements(&mut conn)?
    };
    for attempt in attempts {
        if attempt.status == "rejected" {
            release_rejected_settlement(miden_adapter, pool, &attempt.tx_id, book_tx).await?;
            continue;
        }
        let tx_id = TransactionId::read_from(&mut SliceReader::new(&attempt.tx_id))?;
        let records = client
            .lock()
            .await
            .get_transactions(TransactionFilter::Ids(vec![tx_id]))
            .await?;
        let Some(record) = records.into_iter().next() else {
            // The executor may have crashed after network acceptance but before
            // its own client store recorded the transaction. A known output
            // proves the whole atomic settlement committed, even for full fills.
            if expected_payback_is_included(miden_adapter, pool, &attempt.tx_id).await? {
                activate_confirmed_settlement(pool, &attempt.tx_id, book_tx).await?;
                continue;
            }
            if attempt.status != "uncertain"
                && recovery_retry_at
                    .get(&attempt.tx_id)
                    .is_none_or(|at| tokio::time::Instant::now() >= *at)
            {
                recovery_retry_at.insert(
                    attempt.tx_id.clone(),
                    tokio::time::Instant::now() + RECOVERY_RETRY_DELAY,
                );
                retry_recorded_transaction(client, pool, &attempt).await?;
            }
            continue;
        };
        match record.status {
            TransactionStatus::Committed { .. } => {
                activate_confirmed_settlement(pool, &attempt.tx_id, book_tx).await?;
            }
            TransactionStatus::Discarded(reason) => {
                // A client-side discard can race with a previously accepted copy.
                // Its payback proves commitment even if the local transaction
                // record never moved to Committed.
                if expected_payback_is_included(miden_adapter, pool, &attempt.tx_id).await? {
                    activate_confirmed_settlement(pool, &attempt.tx_id, book_tx).await?;
                    continue;
                }
                if matches!(
                    reason,
                    DiscardCause::Stale | DiscardCause::DiscardedInitialState
                ) {
                    // Stale is only a local age limit, not on-chain expiry. The
                    // original transaction may still land. Its descendants are
                    // discarded locally as DiscardedInitialState, which is not
                    // proof that their on-chain dependency can never commit.
                    let mut conn = pool.write_conn()?;
                    db::mark_settlement_uncertain(&mut conn, &attempt.tx_id)?;
                    tracing::warn!(%tx_id, "stale local transaction remains reserved");
                    continue;
                }
                release_rejected_settlement(miden_adapter, pool, &attempt.tx_id, book_tx).await?;
                tracing::warn!(%tx_id, %reason, "discarded settlement classified by input nullifiers");
            }
            TransactionStatus::Pending => {}
        }
    }
    Ok(())
}

async fn release_rejected_settlement(
    adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    tx_id: &[u8],
    book_tx: &mpsc::Sender<BookUpdate>,
) -> Result<()> {
    let parents = db::settlement_parents(&mut *pool.read_conn()?, tx_id)?;
    let notes = parents
        .iter()
        .map(|parent| parent.note.as_ref().clone())
        .collect::<Vec<_>>();
    let consumed = adapter.lock().await.check_consumed_notes(&notes).await?;
    pool.update_book(book_tx, |conn| {
        db::finish_discarded_settlement(conn, tx_id, &consumed)
    })
    .await
}

async fn expected_payback_is_included(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    tx_id: &[u8],
) -> Result<bool> {
    let payback_id = {
        let mut conn = pool.read_conn()?;
        db::settlement_payback_id(&mut conn, tx_id)?
    };
    miden_adapter
        .lock()
        .await
        .note_is_included(payback_id)
        .await
}

async fn activate_confirmed_settlement(
    pool: &DbPool,
    tx_id: &[u8],
    book_tx: &mpsc::Sender<BookUpdate>,
) -> Result<()> {
    pool.update_book(book_tx, |conn| db::confirm_settlement(conn, tx_id))
        .await
}

/// A crash may occur after the SQLite prepare write but before the client
/// stores submission. Re-proving and submitting this *same* executed result
/// retains its ID. A deliberate rejection is ambiguous if the first copy
/// already landed, so it is quarantined rather than reactivating parents.
async fn retry_recorded_transaction(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    pool: &DbPool,
    attempt: &SettlementAttemptRow,
) -> Result<()> {
    let result = TransactionResult::read_from(&mut SliceReader::new(&attempt.tx_result))?;
    anyhow::ensure!(
        result.id().to_bytes().as_slice() == attempt.tx_id,
        "recorded transaction ID mismatch"
    );
    let proven = client.lock().await.prove_transaction(&result).await?;
    let submission = client
        .lock()
        .await
        .submit_proven_transaction(proven, &result)
        .await;
    match submission {
        Ok(height) => {
            let mut conn = pool.write_conn()?;
            db::mark_settlement_submitted(&mut conn, &attempt.tx_id)?;
            drop(conn);
            if let Err(error) = client.lock().await.apply_transaction(&result, height).await {
                tracing::warn!(tx_id = %result.id(), %error, "recovered submission accepted but local store update failed");
            }
        }
        Err(ClientError::SubmissionOutcomeUnknown { .. } | ClientError::RpcError(_)) => {
            tracing::warn!(tx_id = %result.id(), "recovered submission still has no definite outcome");
        }
        Err(error) => {
            let mut conn = pool.write_conn()?;
            db::mark_settlement_uncertain(&mut conn, &attempt.tx_id)?;
            tracing::error!(tx_id = %result.id(), %error,
                "recorded transaction rejected; parents remain reserved for inspection");
        }
    }
    Ok(())
}

/// Pack consecutive independently solvent groups without copying note bytes.
/// Validate all boundaries before moving anything out of the caller's batch.
fn split_batch(batch: &mut ExecutionBatch) -> Result<Vec<ExecutionBatch>> {
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
        anyhow::ensure!(
            end > previous && end <= total,
            "invalid execution group boundary"
        );
        let size = end - previous;
        anyhow::ensure!(
            size <= MAX_SETTLEMENT_INPUTS,
            "indivisible match group exceeds transaction note limit"
        );
        if packed + size > MAX_SETTLEMENT_INPUTS {
            sizes.push(packed);
            packed = 0;
        }
        packed += size;
        previous = end;
    }
    anyhow::ensure!(previous == total, "execution groups do not cover all notes");
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

fn refeed_unprepared(
    pool: &DbPool,
    batch: &ExecutionBatch,
    book_tx: &mpsc::Sender<BookUpdate>,
    cancel: &CancellationToken,
) {
    refeed_later(
        pool,
        book_tx,
        cancel,
        batch.source_orders(),
        HELD_REFEED_DELAY,
    );
}

/// After mempool acceptance, record each note's enqueue-to-submit duration
/// (now − arrival) into the in-memory swap-eta window and republish it.
/// Everything is in-memory: `arrival_unix` was stamped by the matcher and rides
/// on the batch; the pair is read from the shared original note. No DB.
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
) -> Result<BatchSubmission> {
    let components = match BatchComponents::prepare(batch, solver_id) {
        Ok(parts) => parts,
        Err(e) => {
            // Nothing is marked Settling yet and the matcher dropped these orders on
            // emit, so hand back the original orders — otherwise
            // they're stranded until the next restart. Delayed, so a batch that
            // fails deterministically isn't rebuilt every tick.
            tracing::error!(
                error = %e,
                notes = batch.filled_notes.len(),
                "batch preparation failed; re-feeding its orders after a pause"
            );
            refeed_unprepared(pool, batch, book_tx, cancel);
            return Ok(BatchSubmission::Returned);
        }
    };

    // Fee pre-flight (Miden 0.16): each settlement's fee is paid in the native
    // asset from the solver's own vault. Nothing is marked Settling yet, so on a
    // shortfall hand the orders back after a pause.
    if let Err(e) = check_fee_headroom(client, miden_adapter, solver_id).await {
        tracing::warn!(error = %e, "deferring fee-starved batch");
        let orders = batch.source_orders();
        refeed_later(pool, book_tx, cancel, orders, HELD_REFEED_DELAY);
        return Ok(BatchSubmission::Returned);
    }

    match submit_with_rpc_backoff(client, solver_id, &components, pool, cancel).await {
        SubmitOutcome::Success => {
            // Mempool acceptance is not chain confirmation. The sync poll or
            // ingestion of an expected remainder performs the DB handoff.
            Ok(BatchSubmission::Accepted)
        }
        SubmitOutcome::Pending(message) => {
            tracing::warn!(%message, "settlement outcome pending; inputs remain reserved");
            Ok(BatchSubmission::Uncertain)
        }
        SubmitOutcome::BuildFailed(msg) => {
            // Deterministic failure for THIS batch composition; the individual
            // orders remain Active (submit never landed). Re-feed so
            // the matcher can reconsider (and possibly compose a different
            // batch) without waiting for a process restart.
            let orders = batch.source_orders();
            refeed_orders(pool, book_tx, orders).await?;
            tracing::error!(error = %msg, "tx build failed; re-fed active orders");
            Ok(BatchSubmission::Returned)
        }
        SubmitOutcome::Cancelled => {
            tracing::info!(
                "submit cancelled during backoff; orders remain Settling for reconciliation"
            );
            Err(anyhow!("submit cancelled during backoff"))
        }
        SubmitOutcome::TxError(e, tx_id) => {
            if let Some(ref tx_id) = tx_id {
                db::mark_settlement_rejected(&mut *pool.write_conn()?, tx_id)?;
            }
            // DIAGNOSTIC: full per-note dump (id/serial/nullifier/attachments/offered+
            // requested), on-chain nullifier check, store-existence + store-vs-consumed
            // MATCH, and the COMPLETE VM/tx error — nothing truncated.
            let input_notes = components.notes();
            log_batch_consume_diagnostics(client, miden_adapter, &input_notes, &e).await;

            // Non-RPC error: classify per-note via the nullifier check.
            let (consumed, active_orders) =
                classify_input_notes(miden_adapter, batch, &input_notes).await?;
            let consumed_count = consumed.len();
            let active_count = active_orders.len();

            // DB updates first, then re-feed the actives. Idempotent against
            // a concurrent ingest update (status guard in mark_orders_onchain_nullified).
            pool.update_book(book_tx, |conn| {
                if let Some(tx_id) = tx_id {
                    // Release parents and delete the attempt in one transaction.
                    db::finish_discarded_settlement(conn, &tx_id, &consumed)
                } else {
                    // Execution/proving failed before durable reservation.
                    let consumed_bytes: Vec<_> = consumed.iter().map(|id| id.to_bytes()).collect();
                    db::mark_orders_onchain_nullified(conn, &consumed_bytes)?;
                    let mut update = db::active_book_update(conn, active_orders)?;
                    update.removed.extend(consumed);
                    Ok(update)
                }
            })
            .await?;

            tracing::error!(
                error = ?e,
                consumed_count,
                refed_count = active_count,
                "non-RPC submit failure classified"
            );
            Ok(BatchSubmission::Returned)
        }
    }
}

/// `Err` when the chain charges a fee and the solver's fee-asset balance is below
/// one settlement's worst case. The balance comes from the local store, so it lags
/// the chain by up to one sync interval. A failed lookup is logged and lets the
/// batch through — the submit itself is the real check.
async fn check_fee_headroom(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
) -> Result<()> {
    let fees = miden_adapter.lock().await.fee_parameters().await;
    let (fee_faucet, base_fee) = match fees {
        Ok(Some(params)) => params,
        Ok(None) => return Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "fee parameters unavailable; skipping fee pre-flight");
            return Ok(());
        }
    };
    if base_fee == 0 {
        return Ok(());
    }
    let need = u64::from(base_fee) * FEE_HEADROOM_MULTIPLIER;
    let balance = client
        .lock()
        .await
        .account_reader(solver_id)
        .get_balance(fee_faucet)
        .await;
    match balance {
        Ok(have) if have.as_u64() < need => bail!(
            "solver fee-asset balance {} is below the {need} one settlement may cost \
             (fee faucet {fee_faucet}); fund the solver account",
            have.as_u64()
        ),
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "solver fee-asset balance unavailable; skipping fee pre-flight");
            Ok(())
        }
    }
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
) -> Result<usize> {
    let records = client
        .lock()
        .await
        .get_consumable_notes(Some(solver_id))
        .await
        .context("list consumable notes")?;
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
    let fee = miden_adapter
        .lock()
        .await
        .fee_parameters()
        .await
        .context("read fee parameters")?;
    let notes = select_claimable(notes, fee);
    if notes.is_empty() {
        return Ok(0);
    }

    let count = notes.len();
    let request = TransactionRequestBuilder::new()
        .build_consume_notes(notes)
        .context("build claim request")?;
    let tx_id = client
        .lock()
        .await
        .submit_new_transaction(solver_id, request)
        .await
        .context("submit claim transaction")?;
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

/// Hand orders back to the matcher after `delay`, without blocking the executor.
/// Used when the executor holds a batch it can't settle yet. On shutdown the
/// orders stay `Active` in the DB (they were never marked Settling) and the next
/// boot rehydrates them.
fn refeed_later(
    pool: &DbPool,
    book_tx: &mpsc::Sender<BookUpdate>,
    cancel: &CancellationToken,
    orders: Vec<IngestOrder>,
    delay: Duration,
) {
    let pool = pool.clone();
    let (book_tx, cancel) = (book_tx.clone(), cancel.clone());
    tokio::task::spawn_local(async move {
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = tokio::time::sleep(delay) => {
                if let Err(error) = refeed_orders(&pool, &book_tx, orders).await {
                    tracing::error!(%error, "deferred re-feed failed; stopping pipeline");
                    cancel.cancel();
                }
            },
        }
    });
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
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (exec_ready_tx, exec_ready_rx) = oneshot::channel::<Result<()>>();
    let exec_factory = factory;
    let exec_db = db_pool;
    let exec_cancel = cancel;
    let exec_refeed_tx = refeed_tx;
    let exec_stats_tx = stats_tx;
    let exec_sync_interval = sync_interval;
    let executor_thread = thread::Builder::new()
        .name("executor-client".into())
        .spawn(move || {
            crate::start::run_on_local_runtime("executor-client", async move {
                let client = match exec_factory.build_executor().await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = exec_ready_tx.send(Err(e.context("build_executor")));
                        return;
                    }
                };
                let exec_rpc = match exec_factory.rpc() {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = exec_ready_tx.send(Err(e.context("build executor rpc")));
                        return;
                    }
                };
                let executor_shared: Arc<Mutex<Client<FilesystemKeyStore>>> =
                    Arc::new(Mutex::new(client));
                let executor_adapter: Arc<Mutex<dyn MidenClient>> =
                    Arc::new(Mutex::new(MidenClientAdapter {
                        client: executor_shared.clone(),
                        rpc: exec_rpc,
                    }));

                // Miden 0.16 seals transaction inputs against synced chain headers, so
                // an unsynced client can't submit. Sync once before accepting batches;
                // the periodic sync task below sleeps before its first tick.
                if let Err(e) = executor_adapter.lock().await.sync_state().await {
                    let _ = exec_ready_tx.send(Err(e.context("initial executor sync")));
                    return;
                }
                match executor_adapter.lock().await.fee_parameters().await {
                    Ok(Some((faucet, base_fee))) => {
                        tracing::info!(fee_faucet = %faucet, base_fee, "chain fee parameters");
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "could not read chain fee parameters at boot"),
                }

                let run_client = executor_shared.clone();
                let run_adapter = executor_adapter.clone();
                let run_cancel = exec_cancel.clone();
                let mut executor_handle = tokio::task::spawn_local(async move {
                    run_executor(
                        run_client,
                        run_adapter,
                        solver_id,
                        exec_db,
                        exec_rx,
                        exec_refeed_tx,
                        exec_stats_tx,
                        run_cancel,
                    )
                    .await;
                });

                let sync_adapter = executor_adapter.clone();
                let sync_client = executor_shared.clone();
                let sync_cancel = exec_cancel.clone();
                let mut executor_sync_handle = tokio::task::spawn_local(async move {
                    // Claim funds sent while the solver was down (a new account's first
                    // claim also deploys it), then again after every successful sync.
                    // Claiming here, not before readiness, keeps a slow proof from
                    // delaying startup. A failed claim backs off.
                    let mut claim_retry_at = claim_or_back_off(&sync_client, &sync_adapter, solver_id).await;
                    loop {
                        tokio::select! {
                            _ = sync_cancel.cancelled() => break,
                            _ = tokio::time::sleep(exec_sync_interval) => {
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
                    _ = exec_cancel.cancelled() => {}
                    _ = &mut executor_handle => {
                        tracing::error!("executor task exited unexpectedly; triggering shutdown");
                        exec_cancel.cancel();
                    }
                    _ = &mut executor_sync_handle => {
                        tracing::error!("executor-sync task exited unexpectedly; triggering shutdown");
                        exec_cancel.cancel();
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
        let sources = batch.source_orders();
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
                components.expected_output_recipients.len() + 1
                    <= miden_protocol::MAX_OUTPUT_NOTES_PER_TX
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

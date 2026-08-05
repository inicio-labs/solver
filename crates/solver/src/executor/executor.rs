use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use consume_script::ConsumeAssetScript;
use miden_client::transaction::{NoteArgs, TransactionRequest, TransactionRequestBuilder};
use miden_client::ClientError;
use miden_client::{keystore::FilesystemKeyStore, Client};
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    crypto::utils::{Deserializable, Serializable, SliceReader},
    note::{Note, NoteRecipient},
};
use miden_standards::note::PswapNote;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::db::{self, DbPool};
use crate::ingest::{MidenClient, MidenClientAdapter};
use crate::swap_eta::SettlementStats;
use crate::types::{ExecutionBatch, FilledNote, IngestOrder, OrderStatus, TokenId};

// ── Backoff knobs ──────────────────────────────────────────────────────────

/// Maximum number of retry attempts for transient RPC errors. The first
/// submit is "attempt 0," so the helper makes up to `MAX_RPC_RETRIES + 1`
/// total submit calls before giving up.
const MAX_RPC_RETRIES: u32 = 5;

/// Initial backoff sleep on the first RPC retry. Doubles each attempt.
const INITIAL_RPC_BACKOFF: Duration = Duration::from_millis(500);

/// Maximum per-attempt backoff sleep. Caps `INITIAL_RPC_BACKOFF * 2^n`.
const MAX_RPC_BACKOFF: Duration = Duration::from_secs(30);

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Outcome of `submit_with_rpc_backoff`. The executor's main path branches on
/// this to decide the post-submit state-machine transition.
enum SubmitOutcome {
    /// On-chain submit succeeded — proceed to mark Executed.
    Success,
    /// Backoff loop ran out of attempts; the chain is still unreachable.
    /// Revert orders to Active so the next solver restart can retry.
    RpcExhausted(ClientError),
    /// Submit failed with a non-RPC error (i.e., chain-side rejection).
    /// Classify per-note via the nullifier check, mark consumed orders
    /// OnchainNullified, re-feed the rest to the matcher.
    TxError(ClientError),
    /// `build_tx_request` failed deterministically for this batch
    /// composition. The submit never landed, so every order is still valid:
    /// revert to Active and re-feed so the live matcher reconsiders them
    /// (it dropped them on emit and never re-reads the DB mid-run).
    BuildFailed(String),
    /// Cancellation token fired during a backoff sleep = graceful shutdown.
    /// Leave orders in Settling; next-boot recovery flips them to Active
    /// (do NOT re-feed — the matcher/order_tx are tearing down).
    Cancelled,
}

/// Inputs the executor pre-computes once for a batch; the backoff loop
/// re-uses these to rebuild a fresh `TransactionRequest` each retry (the
/// builder consumes its inputs on `.build()`).
struct BatchComponents {
    input_notes: Vec<(Note, Option<NoteArgs>)>,
    expected_output_recipients: Vec<NoteRecipient>,
    surplus_assets: Vec<Asset>,
}

/// Coarse error classifier. Phase 1: anything wrapped in `RpcError(_)` is
/// transient; everything else (proof failure, chain-side rejection, malformed
/// request, etc.) routes through the consumed-notes check.
fn is_transient_rpc_error(err: &ClientError) -> bool {
    matches!(err, ClientError::RpcError(_))
}

/// Build (or rebuild) a `TransactionRequest` from pre-computed components.
/// Each `.build()` call consumes its builder, so we call this fresh per
/// submit attempt during the backoff loop.
fn build_tx_request(components: &BatchComponents) -> Result<TransactionRequest> {
    let mut builder = TransactionRequestBuilder::new()
        .input_notes(components.input_notes.clone())
        .expected_output_recipients(components.expected_output_recipients.clone());

    if !components.surplus_assets.is_empty() {
        let data = ConsumeAssetScript::prepare(&components.surplus_assets);
        builder = builder
            .custom_script(ConsumeAssetScript::tx_script())
            .script_arg(data.commitment_arg)
            .extend_advice_map([data.advice_map_entry]);
    }

    builder.build().context("failed to build transaction request")
}

/// Submit the prepared tx with bounded exponential backoff on transient RPC
/// errors. Releases the client mutex between attempts so the ingest task can
/// keep syncing during backoff sleeps. Cancel-aware: a `cancel.cancelled()`
/// during a sleep aborts the loop within one tokio tick.
async fn submit_with_rpc_backoff(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    solver_id: AccountId,
    components: &BatchComponents,
    cancel: &CancellationToken,
) -> SubmitOutcome {
    let mut backoff = INITIAL_RPC_BACKOFF;

    for attempt in 0..=MAX_RPC_RETRIES {
        let request = match build_tx_request(components) {
            Ok(r) => r,
            Err(e) => {
                // Deterministic — retrying won't help, and this is NOT a
                // shutdown (that's `Cancelled`). Surface as `BuildFailed` so
                // the caller reverts to Active AND re-feeds the still-valid
                // orders to the live matcher (the submit never landed).
                tracing::error!(error = %e, "build_tx_request failed");
                return SubmitOutcome::BuildFailed(e.to_string());
            }
        };

        let submit_res = {
            let mut c = client.lock().await;
            c.submit_new_transaction(solver_id, request).await
            // lock released here, BEFORE the sleep below
        };

        match submit_res {
            Ok(_tx_id) => return SubmitOutcome::Success,
            Err(e) if is_transient_rpc_error(&e) => {
                if attempt == MAX_RPC_RETRIES {
                    return SubmitOutcome::RpcExhausted(e);
                }
                tracing::warn!(
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "transient RPC error, backing off"
                );
                tokio::select! {
                    _ = cancel.cancelled() => return SubmitOutcome::Cancelled,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_RPC_BACKOFF);
            }
            Err(e) => return SubmitOutcome::TxError(e),
        }
    }
    unreachable!("loop exits via return inside the matched arms")
}

/// Parse PSWAP notes from the batch, compute the per-token flow, derive the
/// fill arguments and surplus assets, and assemble all the components needed
/// to construct a `TransactionRequest`. Returns the prepared components plus
/// the deserialized notes (separately, so the executor can pass them to
/// `check_consumed_notes` on the classify path).
fn prepare_batch_components(
    batch: &ExecutionBatch,
    solver_id: AccountId,
) -> Result<(BatchComponents, Vec<Note>)> {
    let mut input_notes = Vec::new();
    let mut expected_output_recipients = Vec::new();
    let mut input_notes_only: Vec<Note> = Vec::new();

    // Net flow per token: positive = surplus staying with solver, negative = insolvent.
    // i128 is lossless for any u64 sum encountered here — no wrap risk on `as i128`.
    let mut flow: HashMap<TokenId, i128> = HashMap::new();

    for filled in &batch.filled_notes {
        let note = Note::read_from(&mut SliceReader::new(&filled.raw_note_data))
            .context("failed to deserialize note from raw data")?;

        let pswap = PswapNote::try_from(&note)
            .map_err(|e| anyhow!("failed to parse PswapNote: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_token = offered_asset.faucet_id();
        let requested_token = pswap.storage().requested_faucet_id();

        *flow.entry(offered_token).or_default() += u64::from(offered_asset.amount()) as i128;

        let fill_asset = FungibleAsset::new(requested_token, filled.requested_filled)
            .map_err(|e| anyhow!("failed to create fill asset: {}", e))?;

        let note_args = PswapNote::create_args(0, filled.requested_filled)
            .map_err(|e| anyhow!("failed to create note args: {}", e))?;

        input_notes.push((note.clone(), Some(note_args)));
        input_notes_only.push(note.clone());

        let (p2id, remainder) = pswap
            .execute(solver_id, None, Some(fill_asset))
            .map_err(|e| anyhow!("pswap execute failed: {}", e))?;

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

        if let Some(rem_pswap) = remainder {
            let rem_note = Note::from(rem_pswap);
            *flow.entry(offered_token).or_default() -= note_asset_amount(&rem_note) as i128;
            expected_output_recipients.push(rem_note.recipient().clone());
        }
    }

    // Negative flow means we owe more than we have — batch is insolvent.
    let mut surplus_assets: Vec<Asset> = Vec::new();
    for (token, net) in &flow {
        if *net < 0 {
            bail!("insolvent batch: token {:?} has deficit of {}", token, net.abs());
        }
        if *net > 0 {
            let amount = u64::try_from(*net).map_err(|_| {
                anyhow!("surplus exceeds u64 range for token {:?}: {}", token, net)
            })?;
            surplus_assets.push(Asset::Fungible(
                FungibleAsset::new(*token, amount)
                    .map_err(|e| anyhow!("surplus asset: {}", e))?,
            ));
        }
    }

    Ok((
        BatchComponents {
            input_notes,
            expected_output_recipients,
            surplus_assets,
        },
        input_notes_only,
    ))
}

/// Rebuild the `IngestOrder` for one filled note so the matcher can re-add
/// it to its book. Shared by every re-feed path (classify / RpcExhausted /
/// BuildFailed).
fn rebuild_ingest_order(filled: &FilledNote, note: &Note) -> Result<IngestOrder> {
    let parsed = crate::types::Order::from_note(note)
        .context("failed to re-parse Note for re-feed")?;
    Ok(IngestOrder {
        note_id: filled.note_id,
        offered_token: parsed.offered_faucet_id,
        requested_token: parsed.requested_faucet_id,
        offered_amount: parsed.offered_amount,
        requested_amount: parsed.requested_amount,
        raw_note_data: filled.raw_note_data.clone(),
    })
}

/// Rebuild *every* order in the batch (no nullifier filtering). Used by the
/// RpcExhausted / BuildFailed paths: the submit never landed, so all orders
/// remain valid and must return to the live matcher.
fn rebuild_all_orders(batch: &ExecutionBatch, input_notes: &[Note]) -> Result<Vec<IngestOrder>> {
    batch
        .filled_notes
        .iter()
        .zip(input_notes.iter())
        .map(|(filled, note)| rebuild_ingest_order(filled, note))
        .collect()
}

/// Shutdown-aware re-feed into the matcher via the same channel ingest uses.
/// Stops early if the matcher channel is closed (it's tearing down).
async fn refeed_orders(order_tx: &mpsc::Sender<IngestOrder>, orders: Vec<IngestOrder>) {
    for order in orders {
        if order_tx.send(order).await.is_err() {
            tracing::warn!("order_tx send failed during re-feed; matcher likely shut down");
            break;
        }
    }
}

/// On the TxError classification path, fetch which input notes are consumed
/// on-chain. Returns the partitioned source-id byte-vecs (consumed vs active)
/// plus the active orders re-built as `IngestOrder` for re-feed.
async fn classify_input_notes(
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    batch: &ExecutionBatch,
    input_notes: &[Note],
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<IngestOrder>)> {
    let consumed_ids = {
        let mut adapter = miden_adapter.lock().await;
        adapter.check_consumed_notes(input_notes).await?
    };

    let mut consumed_bytes: Vec<Vec<u8>> = Vec::new();
    let mut active_bytes: Vec<Vec<u8>> = Vec::new();
    let mut active_orders: Vec<IngestOrder> = Vec::new();

    for (filled, note) in batch.filled_notes.iter().zip(input_notes.iter()) {
        let id_bytes = filled.note_id.to_bytes().to_vec();
        if consumed_ids.contains(&filled.note_id) {
            consumed_bytes.push(id_bytes);
        } else {
            active_orders.push(rebuild_ingest_order(filled, note)?);
            active_bytes.push(id_bytes);
        }
    }

    Ok((consumed_bytes, active_bytes, active_orders))
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
        c.get_input_notes(miden_client::store::NoteFilter::List(ids)).await.unwrap_or_default()
    };
    let all_store = {
        let c = client.lock().await;
        c.get_input_notes(miden_client::store::NoteFilter::All).await.unwrap_or_default()
    };

    tracing::error!(note_count = notes.len(), "================ BATCH CONSUME DIAGNOSTICS ================");

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
                    p.storage().requested_asset().amount().as_u64()
                ),
            ),
            None => ("<non-PSWAP (p2id payback?)>".to_string(), "<n/a>".to_string()),
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
        let by_commitment_hit =
            all_store.iter().find(|r| r.details_commitment().to_hex() == commitment_hex);
        match (by_id_hit, by_commitment_hit) {
            (Some(r), _) => {
                let attachments_match = r.attachments() == note.attachments();
                let verdict =
                    if attachments_match { "IDENTICAL" } else { "*** ATTACHMENTS MISMATCH ***" };
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
    tracing::error!("================ FULL VM / TX ERROR (pretty Debug, complete nested) ================");
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

/// Run the executor loop: listen for ExecutionBatches and submit Miden transactions.
///
/// Each batch transitions its source orders Active → Settling (before submit)
/// → Executed on success. Failure paths:
///   * RPC error → bounded exponential backoff (max 5 attempts, 500ms → 30s);
///     if still failing, revert to Active.
///   * Non-RPC error → classify each input note via `check_consumed_notes`;
///     consumed → OnchainNullified, active → re-fed to matcher via `order_tx`.
///   * Cancellation during backoff → leave Settling; boot recovery cleans up.
///
/// Boot-time recovery of any leftover `Settling` rows is handled by
/// `db::reset_all_settling_to_active` in `pipeline::prepare_db`.
///
/// Locking: the executor shares a `Mutex<Client>` with the ingest/subscribe
/// adapter. The lock is acquired per-submit-attempt and dropped before each
/// backoff sleep so ingest isn't starved.
#[allow(clippy::too_many_arguments)]
pub async fn run_executor(
    client: Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: DbPool,
    mut exec_rx: mpsc::Receiver<ExecutionBatch>,
    order_tx: mpsc::Sender<IngestOrder>,
    // In-memory swap-eta settlement-time window; republished on each success.
    stats_tx: watch::Sender<Arc<SettlementStats>>,
    cancel: CancellationToken,
) {
    // Owned here (executor thread) and published over `stats_tx`. Ephemeral —
    // no DB persistence; rebuilds after a restart.
    let mut stats = SettlementStats::new();
    loop {
        // Cancellation is only checked BETWEEN batches. Once execute_batch
        // starts, only the backoff sleep is cancel-aware — the on-chain submit
        // runs to completion before a result is observed.
        let batch = tokio::select! {
            _ = cancel.cancelled() => break,
            opt = exec_rx.recv() => match opt {
                Some(b) => b,
                None => break,  // channel closed → upstream is gone
            },
        };

        if batch.filled_notes.is_empty() {
            continue;
        }

        let result = execute_batch(
            &client,
            &miden_adapter,
            solver_id,
            &pool,
            &batch,
            &order_tx,
            &cancel,
        )
        .await;

        match result {
            Ok(_) => {
                tracing::info!(notes = batch.filled_notes.len(), "batch executed successfully");
                record_settlement(&batch, &mut stats, &stats_tx);
            }
            Err(e) => tracing::error!(error = %e, notes = batch.filled_notes.len(), "batch execution failed"),
        }
    }

    tracing::info!("executor shutting down");
}

/// After a successful settlement, record each note's settlement duration
/// (now − arrival) into the in-memory swap-eta window and republish it.
/// Everything is in-memory: `arrival_unix` was stamped by the matcher and rides
/// on the batch; the pair is parsed from the note's own bytes. No DB.
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
        let Ok(note) = Note::read_from(&mut SliceReader::new(&filled.raw_note_data)) else {
            continue;
        };
        let Ok(parsed) = crate::types::Order::from_note(&note) else {
            continue;
        };
        let pair = (parsed.offered_faucet_id, parsed.requested_faucet_id);
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

#[tracing::instrument(skip(client, miden_adapter, pool, batch, order_tx, cancel),
                     fields(batch_size = batch.filled_notes.len()))]
async fn execute_batch(
    client: &Arc<Mutex<Client<FilesystemKeyStore>>>,
    miden_adapter: &Arc<Mutex<dyn MidenClient>>,
    solver_id: AccountId,
    pool: &DbPool,
    batch: &ExecutionBatch,
    order_tx: &mpsc::Sender<IngestOrder>,
    cancel: &CancellationToken,
) -> Result<()> {
    let (components, input_notes_only) = prepare_batch_components(batch, solver_id)?;

    // Collect source order ids for the status transitions.
    let source_note_ids: Vec<Vec<u8>> = batch
        .filled_notes
        .iter()
        .map(|f| f.note_id.to_bytes().to_vec())
        .collect();

    // Mark as Settling before submitting so a crash after submit can be recovered.
    {
        let mut conn = pool.write_conn().context("acquire write conn for Settling mark")?;
        db::update_orders_status(&mut conn, &source_note_ids, OrderStatus::Settling)
            .context("mark orders Settling")?;
    }

    match submit_with_rpc_backoff(client, solver_id, &components, cancel).await {
        SubmitOutcome::Success => {
            let mut conn = pool
                .write_conn()
                .context("acquire write conn for Executed mark")?;
            db::update_orders_status(&mut conn, &source_note_ids, OrderStatus::Executed)
                .context("mark orders Executed")?;
            Ok(())
        }
        SubmitOutcome::RpcExhausted(e) => {
            // Chain unreachable after all retries. The submit never landed, so
            // every order is still valid. Revert to Active AND re-feed to the
            // LIVE matcher — it dropped these on emit and never re-reads the
            // DB mid-run, so a DB-only revert strands them until a restart.
            revert_to_active(pool, &source_note_ids);
            match rebuild_all_orders(batch, &input_notes_only) {
                Ok(orders) => refeed_orders(order_tx, orders).await,
                Err(re) => tracing::error!(
                    error = %re,
                    "rebuild for RPC-exhausted re-feed failed; orders recoverable only at next boot"
                ),
            }
            tracing::error!(
                error = %e,
                "submit RPC-failed after exhausted retries; reverted to Active + re-fed"
            );
            Err(anyhow!("submit RPC-failed after exhausted retries: {e}"))
        }
        SubmitOutcome::BuildFailed(msg) => {
            // Deterministic failure for THIS batch composition; the individual
            // orders remain valid (submit never landed). Revert + re-feed so
            // the matcher can reconsider (and possibly compose a different
            // batch) without waiting for a process restart.
            revert_to_active(pool, &source_note_ids);
            match rebuild_all_orders(batch, &input_notes_only) {
                Ok(orders) => refeed_orders(order_tx, orders).await,
                Err(re) => tracing::error!(
                    error = %re,
                    "rebuild for build-failed re-feed failed; orders recoverable only at next boot"
                ),
            }
            tracing::error!(error = %msg, "tx build failed; reverted to Active + re-fed");
            Err(anyhow!("tx build failed: {msg}"))
        }
        SubmitOutcome::Cancelled => {
            // Genuine shutdown (cancel token fired during backoff). Don't
            // touch DB state and do NOT re-feed — the matcher/order_tx are
            // tearing down; boot recovery flips Settling → Active.
            tracing::info!("submit cancelled during backoff; orders left Settling");
            Err(anyhow!("submit cancelled during backoff"))
        }
        SubmitOutcome::TxError(e) => {
            // DIAGNOSTIC: full per-note dump (id/serial/nullifier/attachments/offered+
            // requested), on-chain nullifier check, store-existence + store-vs-consumed
            // MATCH, and the COMPLETE VM/tx error — nothing truncated.
            log_batch_consume_diagnostics(client, miden_adapter, &input_notes_only, &e).await;

            // Non-RPC error: classify per-note via the nullifier check.
            let (consumed_bytes, active_bytes, active_orders) =
                classify_input_notes(miden_adapter, batch, &input_notes_only).await?;

            // DB updates first, then re-feed the actives. Idempotent against
            // a concurrent ingest update (status guard in mark_orders_onchain_nullified).
            {
                let mut conn = pool
                    .write_conn()
                    .context("acquire write conn for classify-error transition")?;
                if !consumed_bytes.is_empty() {
                    db::mark_orders_onchain_nullified(&mut conn, &consumed_bytes)
                        .context("mark consumed orders OnchainNullified")?;
                }
                if !active_bytes.is_empty() {
                    db::update_orders_status(&mut conn, &active_bytes, OrderStatus::Active)
                        .context("revert active-after-classify orders to Active")?;
                }
            }

            // Re-feed actives via the SAME order_tx channel ingest uses. Matcher's
            // drain logic handles them identically to fresh ingest events.
            refeed_orders(order_tx, active_orders).await;

            tracing::error!(
                error = ?e,
                consumed_count = consumed_bytes.len(),
                refed_count = active_bytes.len(),
                "non-RPC submit failure classified"
            );
            Err(anyhow!("submit failed (tx error, classified): {e}"))
        }
    }
}

/// Best-effort revert of a batch's orders back to `Active`. Used by the
/// RpcExhausted path. If the UPDATE fails (write pool exhausted), the orders
/// stay in `Settling` until next-boot recovery cleans them up — we log loudly.
fn revert_to_active(pool: &DbPool, source_note_ids: &[Vec<u8>]) {
    match pool.write_conn() {
        Ok(mut conn) => {
            if let Err(revert_err) =
                db::update_orders_status(&mut conn, source_note_ids, OrderStatus::Active)
            {
                tracing::error!(
                    error = %revert_err,
                    "CRITICAL: revert to Active failed after RPC-exhausted submit — orders stuck in Settling until restart"
                );
            }
        }
        Err(_) => {
            tracing::error!(
                "CRITICAL: could not acquire write conn to revert orders — orders stuck in Settling until restart"
            );
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
    refeed_tx: mpsc::Sender<IngestOrder>,
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
                let sync_cancel = exec_cancel.clone();
                let mut executor_sync_handle = tokio::task::spawn_local(async move {
                    loop {
                        tokio::select! {
                            _ = sync_cancel.cancelled() => break,
                            _ = tokio::time::sleep(exec_sync_interval) => {
                                if let Err(e) = sync_adapter.lock().await.sync_state().await {
                                    tracing::warn!(error = %e, "executor-client tagless sync failed; will retry next tick");
                                }
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

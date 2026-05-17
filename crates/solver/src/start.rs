//! Top-level entry point: takes a constructed Miden client + parsed config,
//! wires up the full pipeline, and runs until shutdown.
//!
//! Lives in the library so `main.rs` stays tiny — its only jobs are to load
//! `solver.toml`, build the Miden client, set up the Ctrl-C handler, and
//! hand off to `start`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{NoteId, NoteType, PswapNote};
use miden_client::Client;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::Note;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::SolverConfig;
use crate::db;
use crate::executor;
use crate::ingest::{MidenClient, SyncResult};
use crate::pipeline::{self, PipelineConfig};
use crate::price::HttpPriceClient;
use crate::types::TokenId;

/// Adapter that wraps the real `miden_client::Client` behind our `MidenClient`
/// trait abstraction. The same trait is implemented by `MockMidenClient` for
/// tests; this adapter makes the typed Client interchangeable in production.
///
/// Locking strategy: we hold `Arc<Mutex<Client>>` so the executor (which needs
/// the typed Client for `submit_new_transaction`) and ingest/admin (which go
/// through this adapter) can both share the same underlying instance without
/// fighting over ownership.
///
/// Note discovery: we read PSWAPs from two `SyncSummary` fields:
///   * `new_public_notes` — notes the screener inserted (not previously tracked).
///     Production path for PSWAPs created by other accounts.
///   * `committed_notes` — notes the screener marked as already tracked, including
///     the solver's own output notes that just got committed. This covers
///     **remainder PSWAPs** produced when the solver partially fills a swap:
///     the remainder lives in the solver's `output_notes` table, so without
///     this second pass the matcher would never reconsider it. The ingest
///     filter drops non-PSWAP notes (paybacks, surplus moves), so this is
///     safe to be generous about.
///
/// Dedup: the adapter is intentionally stateless. `committed_notes` is
/// edge-triggered (a note appears on exactly one sync — the one whose
/// `sync_notes` block range covers its inclusion block — and never again,
/// since ranges advance and are non-overlapping), and `new_public_notes`
/// (Insert) / `committed_notes` (Update) are disjoint within a sync. Any
/// residual double-emit is absorbed durably downstream: `ingest_once`
/// forwards an order to the matcher only when `insert_notes_batch` reports
/// it as newly inserted (the `orders` primary key is the dedup authority,
/// which also survives restarts).
struct MidenClientAdapter {
    client: Arc<Mutex<Client<FilesystemKeyStore>>>,
}

#[async_trait(?Send)]
impl MidenClient for MidenClientAdapter {
    async fn subscribe_pair(&mut self, offered: TokenId, requested: TokenId) -> Result<()> {
        // PSWAP discovery tags only depend on faucet IDs; the amounts in the
        // FungibleAsset args to `create_tag` are placeholders.
        let offered_asset = FungibleAsset::new(offered, 1)
            .map_err(|e| anyhow!("invalid offered asset for tag: {e}"))?;
        let requested_asset = FungibleAsset::new(requested, 1)
            .map_err(|e| anyhow!("invalid requested asset for tag: {e}"))?;
        let tag = PswapNote::create_tag(NoteType::Public, &offered_asset, &requested_asset);

        let mut client = self.client.lock().await;
        client
            .add_note_tag(tag)
            .await
            .map_err(|e| anyhow!("add_note_tag failed: {e}"))?;
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(block_num, new_pub, new_priv, committed))]
    async fn sync_state(&mut self) -> Result<SyncResult> {
        let mut client = self.client.lock().await;
        let summary = client
            .sync_state()
            .await
            .map_err(|e| anyhow!("sync_state failed: {e}"))?;

        // Populate span fields so structured logs carry sync stats.
        let span = tracing::Span::current();
        span.record("block_num", summary.block_num.as_u64());
        span.record("new_pub", summary.new_public_notes.len());
        span.record("new_priv", summary.new_private_notes.len());
        span.record("committed", summary.committed_notes.len());

        let mut new_notes: Vec<Note> = Vec::new();

        // 1. Notes the Client inserted into its input-notes table on this sync
        //    (discovered by tag, not previously tracked).
        for note_id in summary
            .new_public_notes
            .iter()
            .chain(summary.new_private_notes.iter())
        {
            match client.get_input_note(*note_id).await {
                Ok(Some(record)) => match (&record).try_into() {
                    Ok(note) => new_notes.push(note),
                    Err(e) => tracing::error!(%note_id, error = %e, "InputNoteRecord → Note conversion failed"),
                },
                Ok(None) => {
                    tracing::warn!(%note_id, "note reported by sync but not in input-notes store");
                }
                Err(e) => {
                    tracing::error!(%note_id, error = %e, "get_input_note failed");
                }
            }
        }

        // 2. Tracked notes that became committed on this sync — including the
        //    solver's own output notes (remainder PSWAPs from partial fills).
        //    Try `input_note` first (covers the case where the note was both
        //    received and committed in the same window), fall back to
        //    `output_note`. The ingest filter drops non-PSWAP notes, so
        //    paybacks and surplus-move notes here are harmless.
        for note_id in &summary.committed_notes {
            if let Ok(Some(record)) = client.get_input_note(*note_id).await {
                if let Ok(note) = (&record).try_into() {
                    new_notes.push(note);
                    continue;
                }
            }
            match client.get_output_note(*note_id).await {
                Ok(Some(record)) => match Note::try_from(record) {
                    Ok(note) => new_notes.push(note),
                    Err(e) => tracing::error!(%note_id, error = %e, "OutputNoteRecord → Note conversion failed"),
                },
                Ok(None) => {
                    tracing::warn!(%note_id, "committed note not in input or output store");
                }
                Err(e) => {
                    tracing::error!(%note_id, error = %e, "get_output_note failed");
                }
            }
        }

        Ok(SyncResult {
            block_num: summary.block_num.as_u64(),
            new_notes,
            consumed_notes: summary.consumed_notes.clone(),
        })
    }

    async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>> {
        if notes.is_empty() {
            return Ok(HashSet::new());
        }

        // Map nullifier → NoteId so we can recover the IDs from the RPC response.
        let mut nullifier_to_id = HashMap::new();
        let mut nullifiers = BTreeSet::new();
        for note in notes {
            let nullifier = note.nullifier();
            nullifier_to_id.insert(nullifier, note.id());
            nullifiers.insert(nullifier);
        }

        let mut client = self.client.lock().await;
        let rpc = client.test_rpc_api();
        let heights = rpc
            .get_nullifier_commit_heights(nullifiers, BlockNumber::GENESIS)
            .await
            .map_err(|e| anyhow!("get_nullifier_commit_heights failed: {e}"))?;
        drop(client);

        let mut consumed = HashSet::new();
        for (nullifier, maybe_height) in heights {
            if maybe_height.is_some() {
                if let Some(id) = nullifier_to_id.get(&nullifier) {
                    consumed.insert(*id);
                }
            }
        }
        Ok(consumed)
    }
}

/// Wire up the full solver pipeline and run until shutdown.
///
/// **Must be called from within a `tokio::task::LocalSet` context.** Internally
/// uses `spawn_local` for all tasks because `Client<FilesystemKeyStore>` is
/// `!Send` (upstream `Arc<dyn Trait>` fields without `Send + Sync` bounds).
///
/// Owns: DB pool, shared-symbol-map for price feed, the typed Miden client
/// (shared between executor + adapter), all pipeline tasks, and the executor.
///
/// Reads from env:
/// - `SOLVER_ADMIN_TOKEN` — bearer token for admin endpoints. When unset, admin
///   routes return 404 (server still binds for symmetry with handles).
/// - `COINGECKO_API_KEY` — sent as `x-cg-demo-api-key`. Optional; without it,
///   public-tier rate limits apply.
///
/// Exposes (all 127.0.0.1 only):
/// - `admin_port` — auth-gated POST/PATCH/DELETE for token registry.
/// - `obs_port`   — auth-free `GET /health` (liveness) and `GET /readyz`
///   (readiness, gated on DB reachability + `last successful sync` age).
pub async fn start(
    client: Client<FilesystemKeyStore>,
    solver_id: AccountId,
    config: SolverConfig,
    cancel: CancellationToken,
) -> Result<()> {
    // 1. DB pool (caller-owned so HttpPriceClient + executor can share it).
    let db_pool = db::init_db(&config.solver.store_path, config.solver.read_pool_size)
        .context("init_db")?;

    // 2. Env-sourced secrets.
    let admin_token = std::env::var("SOLVER_ADMIN_TOKEN").ok();
    if admin_token.is_none() {
        tracing::warn!(
            "SOLVER_ADMIN_TOKEN not set — admin endpoints disabled (all /admin/* paths return 404). \
             Set this env var to enable token registration and symbol updates without a restart."
        );
    }
    let coingecko_api_key = std::env::var("COINGECKO_API_KEY").ok();

    // 3. Shared symbol map. spawn_pipeline hydrates it from DB after seeding,
    //    so initialising with an empty map is fine.
    let symbol_map = Arc::new(RwLock::new(HashMap::new()));

    // 4. Production price client.
    let price_client = HttpPriceClient::new(symbol_map.clone(), coingecko_api_key);

    // 5. Flatten configured pairs → token list with optional symbols.
    let mut initial_tokens: Vec<(TokenId, Option<String>)> = Vec::new();
    for pair in &config.pairs {
        let x = AccountId::from_hex(&pair.asset_x_faucet_id)
            .with_context(|| format!("invalid asset_x_faucet_id for pair {}", pair.name))?;
        let y = AccountId::from_hex(&pair.asset_y_faucet_id)
            .with_context(|| format!("invalid asset_y_faucet_id for pair {}", pair.name))?;
        initial_tokens.push((x, pair.asset_x_external_symbol.clone()));
        initial_tokens.push((y, pair.asset_y_external_symbol.clone()));
    }

    // 6. Wrap the typed client so the adapter (for ingest/admin) and the
    //    executor (for submit_new_transaction) can share it. Each task locks
    //    only briefly when it needs the client — no long-held locks.
    let shared_client: Arc<Mutex<Client<FilesystemKeyStore>>> = Arc::new(Mutex::new(client));
    let adapter = MidenClientAdapter {
        client: shared_client.clone(),
    };

    // 7. Build the observability state. The shared `last_sync` atomic is
    //    initialised to `now()` here so /readyz is healthy during the boot
    //    grace period before the first sync completes.
    let obs_state = crate::obs::ObsState::new(
        db_pool.clone(),
        config.engine.readiness_freshness_secs,
    );
    let last_sync_handle = obs_state.last_sync_handle();

    // 8. Build the PipelineConfig.
    let pipeline_config = PipelineConfig {
        db_pool: db_pool.clone(),
        ingest_interval: Duration::from_millis(config.engine.fetch_interval_ms),
        price_interval: Duration::from_millis(config.engine.price_interval_ms),
        match_interval: Duration::from_millis(config.engine.pulse_interval_ms),
        initial_tokens,
        admin_port: config.engine.admin_port,
        admin_token,
        symbol_map,
        triangular_enabled: config.engine.triangular_enabled,
        cancel: cancel.clone(),
        last_sync_unix_seconds: last_sync_handle,
    };

    // 9. Spawn ingest / matcher / price / admin tasks.
    let handles = pipeline::spawn_pipeline(pipeline_config, adapter, price_client)
        .await
        .context("spawn_pipeline")?;

    // 10. Spawn the observability server. No auth; binds 127.0.0.1 only.
    //     Watches the shared cancel token so it shuts down with the rest.
    let obs_port = config.engine.obs_port;
    let obs_cancel = cancel.clone();
    let obs_router = obs_state.router();
    let obs_handle = tokio::task::spawn_local(async move {
        let listener = match tokio::net::TcpListener::bind(format!("127.0.0.1:{obs_port}")).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, port = obs_port, "failed to bind observability port");
                return;
            }
        };
        if let Err(e) = axum::serve(listener, obs_router)
            .with_graceful_shutdown(async move { obs_cancel.cancelled().await })
            .await
        {
            tracing::error!(error = %e, "observability server failed");
        }
    });

    // 11. Spawn the executor (via spawn_local since Client is !Send).
    //     The executor takes `Arc<Mutex<Client>>` (for `submit_new_transaction`)
    //     and `Arc<Mutex<dyn MidenClient>>` (for `check_consumed_notes` on the
    //     TxError classify path). Both point to the same underlying Client —
    //     two handles, one lock, per-attempt acquisition so ingest isn't
    //     starved during backoff sleeps.
    let executor_client = shared_client.clone();
    let executor_adapter = handles.miden_adapter.clone();
    let executor_pool = db_pool.clone();
    let executor_cancel = cancel.clone();
    let executor_order_tx = handles.order_tx.clone();
    let executor_handle = tokio::task::spawn_local(async move {
        executor::run_executor(
            executor_client,
            executor_adapter,
            solver_id,
            executor_pool,
            handles.exec_rx,
            executor_order_tx,
            executor_cancel,
        )
        .await;
    });

    // 12. Await shutdown.
    tokio::select! {
        _ = cancel.cancelled() => {
            tracing::info!("cancellation received");
        }
        res = handles.ingest_handle => {
            tracing::info!(?res, "ingest task exited");
        }
        res = handles.matcher_handle => {
            tracing::info!(?res, "matcher task exited");
        }
        res = handles.price_handle => {
            tracing::info!(?res, "price task exited");
        }
        res = handles.admin_handle => {
            tracing::info!(?res, "admin task exited");
        }
        res = obs_handle => {
            tracing::info!(?res, "observability task exited");
        }
        res = executor_handle => {
            tracing::info!(?res, "executor task exited");
        }
    }

    // Trigger cancel for any remaining tasks. Idempotent.
    cancel.cancel();
    Ok(())
}

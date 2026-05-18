//! Top-level entry point: takes a [`ClientFactory`] + parsed config, wires up
//! the full pipeline across **three** execution contexts and runs until
//! shutdown.
//!
//! L2 threading model: `Client<AUTH>` is `!Send`, so each client lives on its
//! own OS thread (own `current_thread` runtime + `LocalSet`), built there via
//! the factory. The `Send` services (matcher, price, admin, obs) stay on the
//! caller's LocalSet — the "main coordination thread". They are connected only
//! by `Send` channels.
//!
//! Lives in the library so `main.rs` stays tiny — its only jobs are to load
//! `solver.toml`, construct a `ClientFactory`, set up the Ctrl-C handler, and
//! hand off to `start`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, RwLock};
use std::thread;
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
use tokio::sync::{oneshot, Mutex};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::config::SolverConfig;
use crate::db;
use crate::executor;
use crate::ingest::{MidenClient, SyncResult};
use crate::pipeline::{self, PipelineConfig};
use crate::price::{PriceClient, SharedSymbolMap};
use crate::types::TokenId;

/// Build a `current_thread` tokio runtime + `LocalSet` and run `fut` to
/// completion on it. Used as the body of each client OS thread so the `!Send`
/// `Client` it constructs never crosses a thread boundary.
fn run_on_local_runtime<F: std::future::Future<Output = ()>>(
    thread_name: &str,
    fut: F,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(thread = thread_name, error = %e, "failed to build thread runtime");
            return;
        }
    };
    let local = LocalSet::new();
    local.block_on(&rt, fut);
}

/// Adapter that wraps the real `miden_client::Client` behind our `MidenClient`
/// trait abstraction. The same trait is implemented by `MockMidenClient` for
/// tests; this adapter makes the typed Client interchangeable in production.
///
/// Locking strategy: we hold `Arc<Mutex<Client>>` so the executor (which needs
/// the typed Client for `submit_new_transaction`) and ingest/admin (which go
/// through this adapter) can both share the same underlying instance without
/// fighting over ownership.
///
/// Note discovery (post the keyless-ingest / keystore-executor split):
/// PSWAPs come from a single `SyncSummary` source —
///   * `new_public_notes` ∪ `new_private_notes` — notes the screener
///     inserted into the input-notes table on this sync (tag-discovered,
///     not previously tracked).
///
/// The **ingest client is keyless and tracks no accounts**, so it has no
/// `output_notes` table and its `committed_notes` never carries the
/// solver's own notes. Solver-produced **remainder PSWAPs** (from partial
/// fills) are `Public` and tag-matched, so the ingest client re-discovers
/// them here via `new_public_notes` on the sync after the executor's
/// settle commits — exactly like any externally-created PSWAP. (The old
/// single-client model needed a second `committed_notes` pass because the
/// solver client owned the account and its remainder surfaced as its own
/// committed output note; that pass is dead post-split and was removed.)
/// The **executor client** subscribes no tags, so its `sync_state`
/// discovers nothing — it runs only to keep the chain tip / solver
/// account fresh; its returned notes are discarded by the sync task.
///
/// Dedup: the adapter is intentionally stateless. `new_public_notes` /
/// `new_private_notes` are edge-triggered (a note appears on exactly one
/// sync — the one whose block range covers its inclusion block — and
/// never again, since ranges advance and are non-overlapping). Any
/// residual double-emit is absorbed durably downstream: `ingest_once`
/// forwards an order to the matcher only when `insert_notes_batch`
/// reports it as newly inserted (the `orders` primary key is the dedup
/// authority, which also survives restarts).
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

    #[tracing::instrument(skip(self), fields(block_num, new_pub, new_priv))]
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

        let mut new_notes: Vec<Note> = Vec::new();

        // Notes the Client inserted into its input-notes table on this sync
        // (tag-discovered, not previously tracked). This is the *only* PSWAP
        // discovery path — see the struct doc for why the old
        // `committed_notes` second pass is dead post-split.
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
        // GENESIS is intentional, not a placeholder: this is an "ever
        // consumed?" existence check, so it must scan the full nullifier
        // history. (The node serves this from its nullifier set; the
        // `from` block only bounds the *response* range, not correctness.)
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
    factory: Arc<dyn ClientFactory>,
    make_price_client: impl FnOnce(
        SharedSymbolMap,
        Option<String>,
    ) -> Result<Box<dyn PriceClient + Send + Sync>>,
    solver_id: AccountId,
    config: SolverConfig,
    cancel: CancellationToken,
) -> Result<()> {
    // 1. DB pool (caller-owned so HttpPriceClient + executor can share it).
    let db_pool = db::init_db(&config.solver.app_db_path, config.solver.read_pool_size)
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

    // 3. Shared symbol map. `prepare_db` hydrates it from DB after seeding,
    //    so initialising with an empty map is fine.
    let symbol_map = Arc::new(RwLock::new(HashMap::new()));

    // 4. Price client built via the injected builder (prod = HttpPriceClient
    //    with this symbol map + API key; tests inject a MockPriceClient).
    //    `start` keeps ownership of `symbol_map` (shared with admin) and only
    //    hands a clone to the builder.
    let price_client = make_price_client(symbol_map.clone(), coingecko_api_key)
        .context("build price client")?;

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

    // 6. (L2) Clients are no longer built here — each is constructed on its
    //    own OS thread below (a `!Send` `Client` cannot cross threads). The
    //    `factory` carries only `Send` config and is cloned into each thread.

    // 7. Build the observability state. The shared `last_sync` atomic is
    //    initialised to `now()` here so /readyz is healthy during the boot
    //    grace period before the first sync completes.
    let obs_state = crate::obs::ObsState::new(
        db_pool.clone(),
        config.engine.readiness_freshness_secs,
    );
    let last_sync_handle = obs_state.last_sync_handle();

    // 8. Build the PipelineConfig.
    let pipeline_config = PipelineConfig::new(
        &config.engine,
        db_pool.clone(),
        initial_tokens,
        admin_token,
        symbol_map,
        cancel.clone(),
        last_sync_handle.clone(),
    );

    // 9. Cross-thread channels + DB-only boot work (no client) on this thread.
    let channels = pipeline::create_channels();
    pipeline::prepare_db(&pipeline_config).context("prepare_db")?;

    // 10. Spawn the `Send` services (price, matcher, admin) on THIS thread's
    //     LocalSet — the main coordination thread.
    let core = pipeline::spawn_core_services(
        &pipeline_config,
        price_client,
        channels.order_rx,
        channels.consumed_rx,
        channels.price_tx,
        channels.price_rx,
        channels.exec_tx,
        channels.subscribe_tx,
    );

    // 11. Observability server (Send; on the main thread).
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

    // 12. INGEST THREAD: own runtime + LocalSet; builds the keyless ingest
    //     client on-thread, then runs subscribe-relay + ingest. Reports
    //     readiness (or a build/subscribe error) on a oneshot so a startup
    //     failure surfaces here instead of dying silently in a detached thread.
    let (ingest_ready_tx, ingest_ready_rx) = oneshot::channel::<Result<()>>();
    let ingest_factory = factory.clone();
    let ingest_db = db_pool.clone();
    let ingest_cancel = cancel.clone();
    let ingest_order_tx = channels.order_tx.clone();
    let ingest_consumed_tx = channels.consumed_tx;
    let ingest_subscribe_rx = channels.subscribe_rx;
    let ingest_interval = Duration::from_millis(config.engine.fetch_interval_ms);
    let ingest_last_sync: Arc<AtomicI64> = last_sync_handle;
    let ingest_thread = thread::Builder::new()
        .name("ingest-client".into())
        .spawn(move || {
            run_on_local_runtime("ingest-client", async move {
                let client = match ingest_factory.build_ingest().await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = ingest_ready_tx.send(Err(e.context("build_ingest")));
                        return;
                    }
                };
                let adapter: Arc<Mutex<dyn MidenClient>> =
                    Arc::new(Mutex::new(MidenClientAdapter {
                        client: Arc::new(Mutex::new(client)),
                    }));
                let mut h = match pipeline::spawn_ingest_tasks(
                    adapter,
                    ingest_db,
                    ingest_order_tx,
                    ingest_consumed_tx,
                    ingest_subscribe_rx,
                    ingest_interval,
                    ingest_cancel.clone(),
                    ingest_last_sync,
                )
                .await
                {
                    Ok(h) => h,
                    Err(e) => {
                        let _ = ingest_ready_tx.send(Err(e.context("spawn_ingest_tasks")));
                        return;
                    }
                };
                let _ = ingest_ready_tx.send(Ok(()));
                // If a task exits *unexpectedly* (not via cancel) the main
                // coordination loop has no other signal — `order_tx` keeps
                // other live senders, so its `order_rx` never closes and the
                // matcher would silently run a stale book. Propagate a global
                // shutdown (`ingest_cancel` is a clone of the root token).
                tokio::select! {
                    _ = ingest_cancel.cancelled() => {}
                    _ = &mut h.ingest_handle => {
                        tracing::error!("ingest task exited unexpectedly; triggering shutdown");
                        ingest_cancel.cancel();
                    }
                    _ = &mut h.subscribe_handle => {
                        tracing::error!("subscribe-relay task exited unexpectedly; triggering shutdown");
                        ingest_cancel.cancel();
                    }
                }
                // Drain inside the runtime: abort + await both tasks so their
                // `Client` Arc refs are dropped *here* (runtime still entered).
                // Otherwise `LocalSet::drop` after `block_on` returns would
                // force-drop the `!Send` Client with no runtime context, whose
                // Drop then panics ("panic in a destructor during cleanup").
                h.ingest_handle.abort();
                h.subscribe_handle.abort();
                let _ = h.ingest_handle.await;
                let _ = h.subscribe_handle.await;
            });
        })
        .context("spawn ingest thread")?;

    // 13. EXECUTOR THREAD: own runtime + LocalSet; builds the keystore
    //     executor client on-thread, then runs the executor + the tagless
    //     executor-sync task. The sync task discovers no notes (no tag
    //     subscriptions); it only keeps the executor client's reference block
    //     recent off the critical path (the node rejects txs whose reference
    //     block is older than its acceptance window). Submitting does NOT pull
    //     chain state — the client optimistically applies its own tx — so
    //     consecutive settlements stay consistent without a sync between them.
    let (exec_ready_tx, exec_ready_rx) = oneshot::channel::<Result<()>>();
    let exec_factory = factory.clone();
    let exec_db = db_pool.clone();
    let exec_cancel = cancel.clone();
    let exec_rx = channels.exec_rx;
    let exec_refeed_tx = channels.order_tx.clone();
    let exec_sync_interval = Duration::from_millis(config.engine.fetch_interval_ms);
    let executor_thread = thread::Builder::new()
        .name("executor-client".into())
        .spawn(move || {
            run_on_local_runtime("executor-client", async move {
                let client = match exec_factory.build_executor().await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = exec_ready_tx.send(Err(e.context("build_executor")));
                        return;
                    }
                };
                let executor_shared: Arc<Mutex<Client<FilesystemKeyStore>>> =
                    Arc::new(Mutex::new(client));
                let executor_adapter: Arc<Mutex<dyn MidenClient>> =
                    Arc::new(Mutex::new(MidenClientAdapter {
                        client: executor_shared.clone(),
                    }));

                let run_client = executor_shared.clone();
                let run_adapter = executor_adapter.clone();
                let run_cancel = exec_cancel.clone();
                let mut executor_handle = tokio::task::spawn_local(async move {
                    executor::run_executor(
                        run_client,
                        run_adapter,
                        solver_id,
                        exec_db,
                        exec_rx,
                        exec_refeed_tx,
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
                executor_handle.abort();
                executor_sync_handle.abort();
                let _ = executor_handle.await;
                let _ = executor_sync_handle.await;
            });
        })
        .context("spawn executor thread")?;

    // 14. Startup gate: both client threads must report ready (client built +
    //     tasks spawned) before startup is considered successful. Any build /
    //     subscribe failure -> cancel everything, join, return the error.
    let startup: Result<()> = async {
        match ingest_ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(anyhow!("ingest thread exited before signalling readiness")),
        }
        match exec_ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(anyhow!("executor thread exited before signalling readiness")),
        }
        Ok(())
    }
    .await;

    if let Err(e) = startup {
        cancel.cancel();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = ingest_thread.join();
            let _ = executor_thread.join();
        })
        .await;
        return Err(e).context("startup failed");
    }
    tracing::info!("both client threads ready; solver running");

    // 15. Await shutdown: cancellation, or any main-thread Send service
    //     exiting. The client threads are joined in step 16.
    tokio::select! {
        _ = cancel.cancelled() => {
            tracing::info!("cancellation received");
        }
        res = core.matcher_handle => {
            tracing::info!(?res, "matcher task exited");
        }
        res = core.price_handle => {
            tracing::info!(?res, "price task exited");
        }
        res = core.admin_handle => {
            tracing::info!(?res, "admin task exited");
        }
        res = obs_handle => {
            tracing::info!(?res, "observability task exited");
        }
    }

    // 16. Trigger cancel (idempotent) and join the client threads so their
    //     runtimes drain before the process exits.
    cancel.cancel();
    let _ = tokio::task::spawn_blocking(move || {
        if let Err(e) = ingest_thread.join() {
            tracing::error!(?e, "ingest thread panicked");
        }
        if let Err(e) = executor_thread.join() {
            tracing::error!(?e, "executor thread panicked");
        }
    })
    .await;
    Ok(())
}

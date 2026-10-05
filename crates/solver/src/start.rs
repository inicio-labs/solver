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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use miden_protocol::account::AccountId;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::config::SolverConfig;
use crate::db;
use crate::matcher::ClearingRuntime;
use crate::pipeline::{self, PipelineConfig};
use crate::price::{PriceClient, SharedTokenMap};
use crate::types::TokenId;

#[derive(Debug, Error)]
#[error("critical solver worker stopped unexpectedly")]
struct CriticalWorkerStopped;

const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);

async fn join_client_threads(
    ingest: std::thread::JoinHandle<()>,
    executor: std::thread::JoinHandle<()>,
    price_api: std::thread::JoinHandle<()>,
    optional: Vec<(&'static str, std::thread::JoinHandle<()>)>,
) -> Result<()> {
    let joined = tokio::task::spawn_blocking(move || {
        let mut failed = false;
        for (name, handle) in [
            ("ingest", ingest),
            ("executor", executor),
            ("price-api", price_api),
        ]
        .into_iter()
        .chain(optional)
        {
            if let Err(error) = handle.join() {
                tracing::error!(thread = name, ?error, "solver worker panicked");
                failed = true;
            }
        }
        if failed {
            Err(anyhow!("a solver worker panicked during shutdown"))
        } else {
            Ok(())
        }
    });
    tokio::time::timeout(SHUTDOWN_DEADLINE, joined)
        .await
        .context("solver workers did not stop within fifteen seconds; supervisor must restart")?
        .context("solver worker join task stopped")?
}

/// Wait for a worker thread's readiness report.
async fn ready(rx: oneshot::Receiver<Result<()>>, thread: &str) -> Result<()> {
    rx.await.unwrap_or_else(|_| {
        Err(anyhow!(
            "{thread} thread exited before signalling readiness"
        ))
    })
}

/// Long-running tasks of a client thread, named for the shutdown log.
pub(crate) type ClientTasks = Vec<(&'static str, tokio::task::JoinHandle<()>)>;

/// Readiness of a client thread, reported once its setup finished.
pub(crate) type ClientReady = oneshot::Receiver<anyhow::Result<()>>;

/// Run a `!Send` Miden client on its own OS thread. `setup` builds the client
/// and spawns its tasks; its result is reported on the returned readiness
/// channel. The thread then supervises those tasks: if one exits on its own
/// (not via `cancel`), the whole solver shuts down, because nothing else would
/// notice a dead ingest or executor.
pub(crate) fn spawn_client_thread<S, F>(
    name: &'static str,
    cancel: CancellationToken,
    setup: S,
) -> anyhow::Result<(std::thread::JoinHandle<()>, ClientReady)>
where
    S: FnOnce() -> F + Send + 'static,
    F: std::future::Future<Output = anyhow::Result<ClientTasks>>,
{
    let (ready_tx, ready_rx) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            run_on_local_runtime(name, async move {
                let tasks = match setup().await {
                    Ok(tasks) => tasks,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                supervise(tasks, &cancel).await;
            });
        })
        .with_context(|| format!("spawn {name} thread"))?;
    Ok((thread, ready_rx))
}

/// Wait for cancellation or the first task to exit, then stop and drain every
/// task inside this runtime, so the `!Send` client they share is dropped here
/// rather than by `LocalSet::drop` after the runtime is gone (which panics).
async fn supervise(tasks: ClientTasks, cancel: &CancellationToken) {
    let aborts: Vec<_> = tasks.iter().map(|(_, task)| task.abort_handle()).collect();
    let mut running = tokio::task::JoinSet::new();
    for (name, task) in tasks {
        running.spawn_local(async move {
            let _ = task.await;
            name
        });
    }
    tokio::select! {
        _ = cancel.cancelled() => {}
        Some(Ok(name)) = running.join_next() => {
            tracing::error!(task = name, "client task exited unexpectedly; triggering shutdown");
            cancel.cancel();
        }
    }
    for abort in aborts {
        abort.abort();
    }
    while running.join_next().await.is_some() {}
}

/// Build a `current_thread` tokio runtime + `LocalSet` and run `fut` to
/// completion on it. Used as the body of each client OS thread so the `!Send`
/// `Client` it constructs never crosses a thread boundary.
pub(crate) fn run_on_local_runtime<F: std::future::Future<Output = ()>>(thread_name: &str, fut: F) {
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

/// Wire up the full solver pipeline and run until shutdown.
///
/// **Must be called from within a `tokio::task::LocalSet` context.** Internally
/// uses `spawn_local` for all tasks because `Client<FilesystemKeyStore>` is
/// `!Send` (upstream `Arc<dyn Trait>` fields without `Send + Sync` bounds).
///
/// Owns: DB pool, shared-symbol-map for price feed, all pipeline tasks, and
/// the ingest and executor client threads (each builds its own Miden client).
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
        SharedTokenMap,
        Option<String>,
    ) -> Result<Box<dyn PriceClient + Send + Sync>>,
    solver_id: AccountId,
    config: SolverConfig,
    cancel: CancellationToken,
) -> Result<()> {
    // Internal worker failure must not look like an operator-requested stop.
    // Cancelling this child stops the pipeline without cancelling its parent.
    let shutdown_requested = cancel;
    let cancel = shutdown_requested.child_token();
    // 1. DB pool (caller-owned so HttpPriceClient + executor can share it).
    let writer_url = std::env::var("SOLVER_DATABASE_URL")
        .context("set SOLVER_DATABASE_URL for the PostgreSQL application database")?;
    let reader_url = std::env::var("SOLVER_READ_DATABASE_URL")
        .context("set SOLVER_READ_DATABASE_URL for the PostgreSQL read-only role")?;
    let app_name = format!("solver/{}", solver_id.to_hex());
    let db_pool = db::DbPool::open(
        writer_url,
        reader_url,
        config.solver.read_pool_size,
        app_name,
    )
    .await
    .context("open PostgreSQL application database")?;
    let db_fatal = db_pool.fatal_token();

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
    let token_map = Arc::new(RwLock::new(HashMap::new()));

    // 4. Price client built via the injected builder (prod = HttpPriceClient
    //    with this symbol map + API key; tests inject a MockPriceClient).
    //    `start` keeps ownership of `token_map` (shared with admin) and only
    //    hands a clone to the builder.
    let price_client =
        make_price_client(token_map.clone(), coingecko_api_key).context("build price client")?;

    // 5. Flatten configured pairs → token list with optional symbols.
    let mut initial_tokens: Vec<(TokenId, Option<String>)> = Vec::new();
    let mut pairs = Vec::with_capacity(config.pairs.len());
    for pair in &config.pairs {
        let x = AccountId::from_hex(&pair.asset_x_faucet_id)
            .with_context(|| format!("invalid asset_x_faucet_id for pair {}", pair.name))?;
        let y = AccountId::from_hex(&pair.asset_y_faucet_id)
            .with_context(|| format!("invalid asset_y_faucet_id for pair {}", pair.name))?;
        pairs.push((x, y));
        initial_tokens.push((x, pair.asset_x_external_symbol.clone()));
        initial_tokens.push((y, pair.asset_y_external_symbol.clone()));
    }

    // 6. Each Miden client is built on its own OS thread below (a `!Send`
    //    `Client` cannot cross threads); `factory` carries only `Send` config.

    // 7. Build the channels and observability state. The shared `last_sync` atomic is
    //    initialised to `now()` here so /readyz is healthy during the boot
    //    grace period before the first sync completes.
    let channels = pipeline::create_channels();
    let obs_state = crate::obs::ObsState::new(
        db_pool.clone(),
        config.engine.readiness_freshness_secs,
        channels.book_tx.clone(),
        channels.exec_tx.clone(),
    );
    let last_sync_handle = obs_state.last_sync_handle();

    // 8. Build the PipelineConfig.
    let pipeline_config = PipelineConfig::new(
        &config.engine,
        db_pool.clone(),
        initial_tokens,
        admin_token,
        token_map,
        cancel.clone(),
    );

    // 9. DB-only boot work (no client) on this thread.
    pipeline::prepare_db(&pipeline_config)
        .await
        .context("prepare_db")?;

    // Last successful price-refresh timestamp (shared: bumped by the price feed,
    // read by the price-query API for staleness). Init to 0 so the API reports
    // stale until the first real fetch (no fabricated-fresh empty snapshot).
    let last_price_update = Arc::new(std::sync::atomic::AtomicI64::new(0));

    let (clearing_bootstrap, bootstrap_rx) = tokio::sync::oneshot::channel();
    let routing = config.engine.router_enabled.then(|| {
        crate::router::Routing::new(
            channels.quotes_rx.clone(),
            channels.route_tx.clone(),
            config.engine.router_inflight_ttl_ms,
        )
    });
    let maker_markets = pairs
        .iter()
        .map(|&(x, y)| crate::maker::market_key(x, y))
        .collect();
    let clearing = ClearingRuntime {
        bootstrap: bootstrap_rx,
        routing,
        prices: channels.precise_rx.clone(),
        pairs,
        config: crate::clearing::ClearingConfig {
            protocol_fee_ppm: config.engine.clearing_fee_ppm,
            ..crate::clearing::ClearingConfig::default()
        },
        max_price_age_ms: config.engine.price_staleness_secs.saturating_mul(1_000),
        maker_settlement_buffer_ms: config.engine.maker_settlement_buffer_ms,
        max_source_age_ms: config
            .engine
            .clearing_max_source_age_secs
            .saturating_mul(1_000),
        max_source_skew_ms: config
            .engine
            .clearing_max_source_skew_secs
            .saturating_mul(1_000),
    };

    // 10. Spawn the `Send` services (price, matcher, admin) on THIS thread's
    //     LocalSet — the main coordination thread.
    let core = pipeline::spawn_core_services(
        &pipeline_config,
        price_client,
        channels.book_rx,
        channels.precise_tx,
        last_price_update.clone(),
        channels.exec_tx,
        channels.swap_snapshot_tx,
        channels.subscribe_tx,
        clearing,
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

    // 12. INGEST THREAD (keyless) and 13. EXECUTOR THREAD (keystore): each
    //     gets its own OS thread with a `current_thread` runtime + `LocalSet`;
    //     the `!Send` `Client` is built on-thread and never crosses a boundary.
    //     Both report readiness (or a build/spawn error) on a oneshot so a
    //     startup failure surfaces at the gate below instead of dying silently
    //     in a detached thread. The thread bodies live next to the code they
    //     run — `crate::ingest` / `crate::executor`.
    let (ingest_thread, ingest_ready_rx) = crate::ingest::spawn_ingest_thread(
        factory.clone(),
        db_pool.clone(),
        cancel.clone(),
        channels.book_tx.clone(),
        channels.subscribe_rx,
        Duration::from_millis(config.engine.fetch_interval_ms),
        last_sync_handle,
        solver_id,
        clearing_bootstrap,
    )?;
    // Finish durable-note recovery and book hydration before the executor can
    // confirm/reactivate attempts. This prevents a stale startup snapshot from
    // overwriting an outcome produced concurrently with hydration.
    let ingest_ready = tokio::select! {
        ready = ready(ingest_ready_rx, "ingest") => ready,
        _ = db_fatal.cancelled() => Err(anyhow!("critical PostgreSQL failure during ingest startup")),
    };
    if let Err(error) = ingest_ready {
        cancel.cancel();
        let joined = tokio::task::spawn_blocking(move || ingest_thread.join());
        if tokio::time::timeout(SHUTDOWN_DEADLINE, joined)
            .await
            .is_err()
        {
            tracing::error!("ingest thread did not stop within shutdown deadline");
        }
        return Err(error).context("ingest startup recovery failed");
    }
    let (executor_thread, exec_ready_rx) = crate::executor::spawn_executor_thread(
        factory.clone(),
        db_pool.clone(),
        cancel.clone(),
        solver_id,
        channels.exec_rx,
        channels.book_tx.clone(),
        channels.stats_tx,
        Duration::from_millis(config.engine.fetch_interval_ms),
        Duration::from_millis(config.engine.verify_interval_ms),
    )?;

    // 13b. PRICE-QUERY API THREAD (public, read-only): its own OS thread +
    //      multi-thread runtime so wallet traffic can't starve settlement. Reads
    //      the precise price side-channel + DB; never touches a `!Send` client.
    let price_api_cfg = crate::price_api::PriceApiConfig {
        bind: config.engine.price_query_bind.clone(),
        port: config.engine.price_query_port,
        max_inflight: config.engine.price_query_max_inflight,
        max_batch: config.engine.price_query_max_batch,
        timeout_ms: config.engine.price_query_timeout_ms,
        vs_currency: config.engine.price_vs_currency.clone(),
        precision: config.engine.price_precision.clone(),
        staleness_secs: config.engine.price_staleness_secs,
        price_interval_ms: config.engine.price_interval_ms,
        swap_matching_trigger_ms: config.engine.pulse_interval_ms,
        swap_sync_ms: config.engine.fetch_interval_ms,
        swap_proving_ms: config.engine.swap_proving_estimate_ms,
        swap_block_ms: config.engine.swap_block_time_ms,
        swap_offmarket_tol_bps: config.engine.swap_offmarket_tolerance_bps,
    };
    let (price_api_thread, price_api_ready_rx) = crate::price_api::spawn_price_api_thread(
        price_api_cfg,
        channels.precise_rx,
        channels.swap_snapshot_rx,
        channels.stats_rx,
        db_pool.clone(),
        last_price_update,
        cancel.clone(),
    )?;

    // 13c. ROUTER THREAD (external liquidity RFQ websocket): its own OS thread +
    //      multi-thread runtime (like the price-API) so DEX traffic can't stall
    //      settlement. Only spawned when enabled; allow-list tokens come from the
    //      `SOLVER_ROUTER_TOKENS` env var (comma-separated).
    let (router_thread, router_ready_rx) = if config.engine.router_enabled {
        let auth_tokens: Vec<String> = std::env::var("SOLVER_ROUTER_TOKENS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if auth_tokens.is_empty() {
            tracing::warn!(
                "router_enabled but SOLVER_ROUTER_TOKENS is empty — all DEX connections rejected"
            );
        }
        let router_cfg = crate::router::RouterConfig {
            bind: config.engine.router_bind.clone(),
            port: config.engine.router_port,
            max_connections: config.engine.router_max_connections,
            max_msg_bytes: config.engine.router_max_msg_bytes,
            quote_ttl_ms: config.engine.router_quote_ttl_ms,
            auth_tokens,
        };
        match crate::router::spawn_router_thread(
            router_cfg,
            channels.quotes_tx,
            channels.route_rx,
            cancel.clone(),
        ) {
            Ok((t, r)) => (Some(t), Some(r)),
            Err(e) => {
                // Router failed to start — tear down the already-spawned services
                // (same cancel + join path as a startup-gate failure) so nothing is
                // left running after `start` returns.
                cancel.cancel();
                if let Err(join_error) = join_client_threads(
                    ingest_thread,
                    executor_thread,
                    price_api_thread,
                    Vec::new(),
                )
                .await
                {
                    tracing::error!(%join_error, "solver workers did not stop cleanly");
                }
                return Err(e).context("router startup failed");
            }
        }
    } else {
        (None, None)
    };
    let mut optional_threads: Vec<_> = router_thread.map(|t| ("router", t)).into_iter().collect();

    // 13d. MAKER GATEWAY THREAD (ADR 0003): gRPC maker commands and the maker
    //      intake writer on their own OS thread, runtime and database session,
    //      so maker traffic cannot slow ingest or settlement. Only spawned when
    //      enabled; committed cancels reach the matcher in book update order.
    let gateway_ready_rx = if config.engine.maker_gateway_enabled {
        let gateway_cfg = crate::gateway::GatewayConfig {
            bind: config.engine.maker_gateway_bind.clone(),
            port: config.engine.maker_gateway_port,
            watch_interval: Duration::from_millis(config.engine.maker_watch_interval_ms),
            maker_store_path: std::path::PathBuf::from(format!(
                "{}.maker.sqlite3",
                config.solver.ingest_store_path
            )),
            markets: maker_markets,
            round_submits: config.engine.maker_intake_round_submits,
            submit_queue: config.engine.maker_intake_submit_queue,
            cancel_queue: config.engine.maker_intake_cancel_queue,
            stream: crate::gateway::StreamConfig {
                buffer: config.engine.maker_stream_buffer,
                heartbeat: Duration::from_millis(config.engine.maker_stream_heartbeat_ms),
            },
        };
        // The core writer notifies it after every commit that appended maker
        // events.
        let maker_events = crate::gateway::EventWake::global().clone();
        let spawned = factory.rpc().and_then(|rpc| {
            crate::gateway::spawn_gateway_thread(
                gateway_cfg,
                db_pool.clone(),
                maker_events,
                rpc,
                channels.book_tx.clone(),
                cancel.clone(),
            )
        });
        match spawned {
            Ok((thread, ready_rx)) => {
                optional_threads.push(("maker-gateway", thread));
                Some(ready_rx)
            }
            Err(e) => {
                cancel.cancel();
                if let Err(join_error) = join_client_threads(
                    ingest_thread,
                    executor_thread,
                    price_api_thread,
                    optional_threads,
                )
                .await
                {
                    tracing::error!(%join_error, "solver workers did not stop cleanly");
                }
                return Err(e).context("maker gateway startup failed");
            }
        }
    } else {
        None
    };

    // 14. Startup gate: both client threads must report ready (client built +
    //     tasks spawned) before startup is considered successful. Any build /
    //     subscribe failure -> cancel everything, join, return the error.
    let startup: Result<()> = tokio::select! {
      result = async {
        ready(exec_ready_rx, "executor").await?;
        ready(price_api_ready_rx, "price-api").await?;
        if let Some(rx) = router_ready_rx {
            ready(rx, "router").await?;
        }
        if let Some(rx) = gateway_ready_rx {
            ready(rx, "maker-gateway").await?;
        }
        Ok(())
      } => result,
      _ = db_fatal.cancelled() => Err(anyhow!("critical PostgreSQL failure during startup")),
    };

    if let Err(e) = startup {
        cancel.cancel();
        if let Err(join_error) = join_client_threads(
            ingest_thread,
            executor_thread,
            price_api_thread,
            optional_threads,
        )
        .await
        {
            tracing::error!(%join_error, "solver workers did not stop cleanly");
        }
        return Err(e).context("startup failed");
    }
    tracing::info!("ingest + executor + price-api threads ready; solver running");

    // 15. Await shutdown: cancellation, or any main-thread Send service
    //     exiting. The client threads are joined in step 16.
    tokio::select! {
        _ = db_fatal.cancelled() => {
            tracing::error!("critical PostgreSQL failure; stopping the whole solver");
        }
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
    join_client_threads(
        ingest_thread,
        executor_thread,
        price_api_thread,
        optional_threads,
    )
    .await?;
    if !shutdown_requested.is_cancelled() {
        return Err(CriticalWorkerStopped.into());
    }
    Ok(())
}

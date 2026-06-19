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
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::config::SolverConfig;
use crate::db;
use crate::pipeline::{self, PipelineConfig};
use crate::price::{PriceClient, SharedSymbolMap};
use crate::types::TokenId;

/// Build a `current_thread` tokio runtime + `LocalSet` and run `fut` to
/// completion on it. Used as the body of each client OS thread so the `!Send`
/// `Client` it constructs never crosses a thread boundary.
pub(crate) fn run_on_local_runtime<F: std::future::Future<Output = ()>>(
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

    // Last successful price-refresh timestamp (shared: bumped by the price feed,
    // read by the price-query API for staleness). Init to 0 so the API reports
    // stale until the first real fetch (no fabricated-fresh empty snapshot).
    let last_price_update = Arc::new(std::sync::atomic::AtomicI64::new(0));

    // 10. Spawn the `Send` services (price, matcher, admin) on THIS thread's
    //     LocalSet — the main coordination thread.
    let core = pipeline::spawn_core_services(
        &pipeline_config,
        price_client,
        channels.order_rx,
        channels.consumed_rx,
        channels.price_tx,
        channels.price_rx,
        channels.precise_tx,
        last_price_update.clone(),
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
        channels.order_tx.clone(),
        channels.consumed_tx,
        channels.subscribe_rx,
        Duration::from_millis(config.engine.fetch_interval_ms),
        last_sync_handle,
    )?;
    let (executor_thread, exec_ready_rx) = crate::executor::spawn_executor_thread(
        factory.clone(),
        db_pool.clone(),
        cancel.clone(),
        solver_id,
        channels.exec_rx,
        channels.order_tx.clone(),
        Duration::from_millis(config.engine.fetch_interval_ms),
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
    };
    let (price_api_thread, price_api_ready_rx) = crate::price_api::spawn_price_api_thread(
        price_api_cfg,
        channels.precise_rx,
        db_pool.clone(),
        last_price_update,
        cancel.clone(),
    )?;

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
        match price_api_ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(anyhow!("price-api thread exited before signalling readiness")),
        }
        Ok(())
    }
    .await;

    if let Err(e) = startup {
        cancel.cancel();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = ingest_thread.join();
            let _ = executor_thread.join();
            let _ = price_api_thread.join();
        })
        .await;
        return Err(e).context("startup failed");
    }
    tracing::info!("ingest + executor + price-api threads ready; solver running");

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
        if let Err(e) = price_api_thread.join() {
            tracing::error!(?e, "price-api thread panicked");
        }
    })
    .await;
    Ok(())
}

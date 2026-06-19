use std::env;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use tokio_util::sync::CancellationToken;

use solver::config::SolverConfig;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod client_factory;
use client_factory::ProdClientFactory;

/// Initialise the global `tracing` subscriber.
///
/// Output format is controlled by `LOG_FORMAT`:
///   * unset / "pretty" → human-friendly compact form with ANSI colours
///   * "json"           → newline-delimited JSON for log aggregators
///
/// Verbosity is controlled by `RUST_LOG`. Default is `info,solver=info` so
/// pipeline-lifecycle lines appear out-of-the-box but solver internals stay
/// quiet until an operator opts in. Set `RUST_LOG=solver=debug` to expose
/// per-tick matcher detail.
fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,solver=info"));

    let want_json = env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if want_json {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(false),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().compact())
            .init();
    }
}

/// CLI surface: a single optional `--config <PATH>`. Precedence is
/// clap-native — explicit flag > `$SOLVER_CONFIG` env > the `solver.toml`
/// default. `--help`/`--version` are auto-generated; an unknown flag or
/// `--config` with no value is a clap usage error (non-zero exit).
fn cli() -> clap::Command {
    clap::Command::new("solver-bin")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            clap::Arg::new("config")
                .long("config")
                .value_name("PATH")
                .env("SOLVER_CONFIG")
                .default_value("solver.toml")
                .help("Path to the TOML config file"),
        )
}

async fn run() -> Result<()> {
    // Init the global tracing subscriber before any other log lines fire.
    init_tracing();

    // Parses argv; `--help` / `--version` / usage errors print and exit here.
    let config_path = cli()
        .get_matches()
        .get_one::<String>("config")
        .expect("`config` always has a default_value")
        .clone();

    let config = SolverConfig::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;
    let solver_id = AccountId::from_hex(&config.solver.account_id)
        .with_context(|| format!("invalid solver account_id {:?}", config.solver.account_id))?;

    // L2: clients are built on their own OS threads (inside `start`), so
    // here we only construct a `Send` factory that carries the config
    // needed to build them. A build failure still surfaces as a clean
    // startup error via the per-thread readiness gate in `start`.
    let factory: Arc<dyn solver::ClientFactory> = Arc::new(ProdClientFactory::from_config(&config));

    // Cancellation token: triggered by Ctrl-C, passed into solver::start
    // so every pipeline task can shut down cleanly between iterations.
    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    tokio::task::spawn_local(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received, triggering graceful shutdown");
            cancel_for_signal.cancel();
        }
    });

    solver::start(
        factory,
        solver::price::build_http_price_client,
        solver_id,
        config,
        cancel,
    )
    .await
}

fn main() -> anyhow::Result<()> {
    // Single-threaded runtime + LocalSet. Required because `Client` is `!Send`
    // (its `Arc<dyn Trait>` fields lack `Send + Sync` bounds upstream), so the
    // pipeline tasks must use `tokio::task::spawn_local` and stay on this thread.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run())
}

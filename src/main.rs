use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use tokio_util::sync::CancellationToken;

use solver::config::SolverConfig;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod client_factory;
mod provision;
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

/// CLI surface: an optional `--config <PATH>` plus operator subcommands.
/// With no subcommand the solver runs. Config precedence is clap-native — explicit
/// flag > `$SOLVER_CONFIG` env > the `solver.toml` default. `--help`/`--version`
/// are auto-generated; an unknown flag or `--config` with no value is a clap
/// usage error (non-zero exit).
fn cli() -> clap::Command {
    clap::Command::new("solver-bin")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            clap::Arg::new("config")
                .long("config")
                .value_name("PATH")
                .env("SOLVER_CONFIG")
                .default_value("solver.toml")
                .global(true)
                .help("Path to the TOML config file"),
        )
        .subcommand(clap::Command::new("provision-account").about(
            "Create the solver account in the executor store and keystore, and print its id",
        ))
        .subcommand(
            clap::Command::new("fund-account")
                .about("Consume the notes sent to the solver account, which deploys and funds it"),
        )
        .subcommand(
            clap::Command::new("migrate-db")
                .about("Apply PostgreSQL schema migrations using SOLVER_MIGRATION_DATABASE_URL"),
        )
        .subcommand(
            clap::Command::new("check-db")
                .about("Check PostgreSQL schema compatibility using SOLVER_DATABASE_URL"),
        )
}

async fn run(matches: clap::ArgMatches) -> Result<()> {
    let config_path = matches
        .get_one::<String>("config")
        .expect("`config` always has a default_value")
        .clone();

    let config = SolverConfig::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;
    match matches.subcommand_name() {
        Some("provision-account") => return provision::provision_account(&config).await,
        Some("fund-account") => return provision::fund_account(&config).await,
        _ => {}
    }
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

    // Price source: the solver always uses its real `HttpPriceClient`; only the
    // base URL varies. `[engine].price_api_base_url` lets devnet/local point it
    // at a self-hosted or mock CoinGecko-compatible service (default: public
    // CoinGecko). Resolved up front so the injected closure has a single type.
    let price_base = config
        .engine
        .price_api_base_url
        .clone()
        .unwrap_or_else(|| solver::price::COINGECKO_BASE.to_string());
    if config.engine.price_api_base_url.is_some() {
        tracing::info!(base = %price_base, "price feed pointed at custom base URL");
    }
    let price_vs_currency = config.engine.price_vs_currency.clone();
    let make_price_client = move |symbol_map, api_key| {
        solver::price::build_http_price_client_with_base(
            symbol_map,
            api_key,
            price_base,
            price_vs_currency,
        )
    };

    solver::start(factory, make_price_client, solver_id, config, cancel).await
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    // Run operator-only PostgreSQL commands before creating a Tokio runtime:
    // Diesel performs synchronous network I/O.
    let matches = cli().get_matches();
    match matches.subcommand_name() {
        Some("migrate-db") => {
            let url = env::var("SOLVER_MIGRATION_DATABASE_URL")
                .context("set SOLVER_MIGRATION_DATABASE_URL for migrate-db")?;
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            let applied = solver::db::postgres_migrations::migrate(&mut conn)?;
            println!(
                "PostgreSQL schema ready ({} new migration(s))",
                applied.len()
            );
            return Ok(());
        }
        Some("check-db") => {
            let url =
                env::var("SOLVER_DATABASE_URL").context("set SOLVER_DATABASE_URL for check-db")?;
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            solver::db::postgres_migrations::verify(&mut conn)?;
            println!("PostgreSQL schema matches this solver binary");
            return Ok(());
        }
        _ => {}
    }

    // Single-threaded runtime + LocalSet. Required because `Client` is `!Send`
    // (its `Arc<dyn Trait>` fields lack `Send + Sync` bounds upstream), so the
    // pipeline tasks must use `tokio::task::spawn_local` and stay on this thread.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    let result = local.block_on(&rt, run(matches));
    drop(local);
    // A lost PostgreSQL reply can leave a synchronous libpq call running on a
    // blocking worker. Do not let runtime teardown hang the process after the
    // solver has decided to fail-stop; the process supervisor starts a fresh
    // instance that hydrates from committed state.
    rt.shutdown_timeout(Duration::from_secs(5));
    result
}

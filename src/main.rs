mod client_main {
    use std::path::PathBuf;
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use miden_client::{
        builder::ClientBuilder,
        keystore::FilesystemKeyStore,
        rpc::{Endpoint, GrpcClient, NodeRpcClient},
        Client,
    };
    use miden_client_sqlite_store::ClientBuilderSqliteExt;
    use miden_protocol::account::AccountId;
    use tokio_util::sync::CancellationToken;

    use solver::config::SolverConfig;
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

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
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,solver=info"));

        let want_json =
            std::env::var("LOG_FORMAT").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false);

        if want_json {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json().with_current_span(true).with_span_list(false))
                .init();
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().compact())
                .init();
        }
    }

    fn parse_account_id(hex_str: &str) -> Result<AccountId> {
        AccountId::from_hex(hex_str)
            .with_context(|| format!("Failed to parse account ID: {}", hex_str))
    }

    /// Build the Miden RPC client + sqlite store + keystore from config.
    async fn build_miden_client(
        config: &SolverConfig,
    ) -> Result<Client<FilesystemKeyStore>> {
        let endpoint = Endpoint::try_from(config.rpc.endpoint.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse endpoint: {}", e))?;
        let rpc_client: Arc<dyn NodeRpcClient> =
            Arc::new(GrpcClient::new(&endpoint, config.rpc.timeout_ms));

        let keystore_path = PathBuf::from(&config.solver.keystore_path);
        let keystore = Arc::new(
            FilesystemKeyStore::new(keystore_path)
                .context("Failed to initialize keystore")?,
        );

        let store_path = PathBuf::from(&config.solver.store_path);

        ClientBuilder::new()
            .rpc(rpc_client)
            .sqlite_store(store_path)
            .authenticator(keystore)
            .in_debug_mode(config.engine.debug_mode.into())
            .build()
            .await
            .context("Failed to build Miden client")
    }

    /// Resolve the config path. Precedence: `--config <path>` CLI flag, then
    /// `$SOLVER_CONFIG` env var, then `"solver.toml"` in the current working
    /// directory.
    fn resolve_config_path(args: &[String]) -> String {
        args.iter()
            .position(|a| a == "--config")
            .and_then(|i| args.get(i + 1).cloned())
            .or_else(|| std::env::var("SOLVER_CONFIG").ok())
            .unwrap_or_else(|| "solver.toml".to_string())
    }

    fn print_help() {
        println!("Usage: solver-bin [--config <path>]");
        println!();
        println!("Options:");
        println!("  --config <path>   Path to the TOML config file.");
        println!("                    Default: $SOLVER_CONFIG, or 'solver.toml' if unset.");
        println!("  -h, --help        Show this help.");
    }

    pub async fn run() -> Result<()> {
        let args: Vec<String> = std::env::args().collect();

        if args.iter().any(|a| a == "--help" || a == "-h") {
            print_help();
            return Ok(());
        }

        // Init the global tracing subscriber before any other log lines fire.
        init_tracing();

        let config_path = resolve_config_path(&args);
        let config = SolverConfig::load(&config_path)
            .with_context(|| format!("failed to load config from {config_path}"))?;
        let solver_id = parse_account_id(&config.solver.account_id)
            .context("Failed to parse solver account ID")?;

        let client = build_miden_client(&config).await?;

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

        solver::start(client, solver_id, config, cancel).await
    }
}

fn main() -> anyhow::Result<()> {
    // Single-threaded runtime + LocalSet. Required because `Client` is `!Send`
    // (its `Arc<dyn Trait>` fields lack `Send + Sync` bounds upstream), so the
    // pipeline tasks must use `tokio::task::spawn_local` and stay on this thread.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, client_main::run())
}

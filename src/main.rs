#[cfg(not(feature = "client"))]
fn main() {
    eprintln!("The solver binary requires the 'client' feature. Build with: cargo build --features client");
    std::process::exit(1);
}

#[cfg(feature = "client")]
mod client_main {
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use miden_client::{
        builder::ClientBuilder,
        keystore::FilesystemKeyStore,
        rpc::{Endpoint, GrpcClient, NodeRpcClient},
    };
    use miden_client_sqlite_store::ClientBuilderSqliteExt;
    use miden_protocol::account::AccountId;
    use tokio::sync::broadcast;

    use solver::events::SolverEvent;
    use solver::order::AssetPair;
    use solver_bin::config::SolverConfig;
    use solver_bin::ws_server;

    fn parse_account_id(hex_str: &str) -> Result<AccountId> {
        AccountId::from_hex(hex_str).with_context(|| format!("Failed to parse account ID: {}", hex_str))
    }

    pub async fn run() -> Result<()> {
        let config = SolverConfig::load("solver.toml")?;

        let solver_id =
            parse_account_id(&config.solver.account_id).context("Failed to parse solver account ID")?;

        let mut pairs: Vec<AssetPair> = Vec::new();
        for pair_cfg in &config.pairs {
            let faucet_x = parse_account_id(&pair_cfg.asset_x_faucet_id)
                .with_context(|| format!("Failed to parse faucet X for pair {}", pair_cfg.name))?;
            let faucet_y = parse_account_id(&pair_cfg.asset_y_faucet_id)
                .with_context(|| format!("Failed to parse faucet Y for pair {}", pair_cfg.name))?;
            pairs.push(AssetPair::new(faucet_x, faucet_y));
        }

        let endpoint = Endpoint::try_from(config.rpc.endpoint.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse endpoint: {}", e))?;
        let rpc_client: Arc<dyn NodeRpcClient> =
            Arc::new(GrpcClient::new(&endpoint, config.rpc.timeout_ms));

        let keystore_path = std::path::PathBuf::from(&config.solver.keystore_path);
        let keystore = Arc::new(
            FilesystemKeyStore::new(keystore_path)
                .context("Failed to initialize keystore")?,
        );

        let store_path = std::path::PathBuf::from(&config.solver.store_path);

        let mut client = ClientBuilder::new()
            .rpc(rpc_client)
            .sqlite_store(store_path)
            .authenticator(keystore)
            .in_debug_mode(true.into())
            .build()
            .await
            .context("Failed to build Miden client")?;

        let (event_tx, _) = broadcast::channel::<SolverEvent>(256);

        if config.dashboard.enabled {
            let ws_port = config.dashboard.ws_port;
            let router = ws_server::build_router(event_tx.clone());
            let addr = format!("0.0.0.0:{}", ws_port);
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .with_context(|| format!("Failed to bind WebSocket server to {}", addr))?;
            println!("WebSocket server listening on ws://{}:{}/ws", "0.0.0.0", ws_port);
            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, router).await {
                    eprintln!("WebSocket server error: {}", e);
                }
            });
        }

        solver::start(
            &mut client,
            solver_id,
            pairs,
            config.engine.fetch_interval_ms,
            config.engine.pulse_interval_ms,
            Some(event_tx),
        )
        .await
    }
}

#[cfg(feature = "client")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    client_main::run().await
}

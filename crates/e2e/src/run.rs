//! `e2e run` — boot the real solver pipeline in-process against devnet with a
//! deterministic fixed-price feed (no CoinGecko), let it ingest + match + settle
//! the PSWAPs created by `load`, then report the solver's balance delta (the
//! spread it captured = proof of settlement).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::rpc::{Endpoint, GrpcClient, NodeRpcClient};
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use tokio_util::sync::CancellationToken;

use solver::config::SolverConfig;
use solver::price::{MockPriceClient, PriceClient, PriceSnapshot};

use crate::accounts;
use crate::artifacts::{self, Artifacts};
use crate::devnet::{self, DEVNET_RPC};

/// Fixed USD price (cents) the matcher sees for both test tokens. Equal prices
/// keep the crossing condition simple: each opposing order offers more than the
/// other requests, so there is positive surplus for the solver.
const PRICE_CENTS: u64 = 100;

/// e2e [`solver::ClientFactory`] — same shape as the production `ProdClientFactory`,
/// but built with `for_devnet()` so the executor uses the remote prover.
struct E2eFactory {
    ingest_store: String,
    executor_store: String,
    keystore: String,
}

#[async_trait::async_trait(?Send)]
impl solver::ClientFactory for E2eFactory {
    async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>> {
        ClientBuilder::for_devnet()
            .sqlite_store(PathBuf::from(&self.ingest_store))
            .build()
            .await
            .context("build e2e ingest client")
    }

    async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>> {
        let keystore =
            Arc::new(FilesystemKeyStore::new(PathBuf::from(&self.keystore)).context("keystore")?);
        ClientBuilder::for_devnet()
            .authenticator(keystore)
            .sqlite_store(PathBuf::from(&self.executor_store))
            .build()
            .await
            .context("build e2e executor client")
    }

    fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>> {
        let endpoint = Endpoint::try_from(DEVNET_RPC).map_err(|e| anyhow!("endpoint: {e}"))?;
        Ok(Arc::new(GrpcClient::new(&endpoint, 10_000)))
    }
}

pub async fn run(secs: u64) -> Result<()> {
    let art = Artifacts::load(&artifacts::artifacts_path())?;
    let config =
        SolverConfig::load(&artifacts::solver_config_path()).context("load generated solver config")?;
    let solver_id =
        AccountId::from_hex(&art.solver_account_id).map_err(|e| anyhow!("solver id: {e}"))?;
    let token_a = AccountId::from_hex(&art.token_a.faucet_id).map_err(|e| anyhow!("token_a: {e}"))?;
    let token_b = AccountId::from_hex(&art.token_b.faucet_id).map_err(|e| anyhow!("token_b: {e}"))?;

    // Pre-run balances (best-effort).
    let (pre_a, pre_b) = read_solver_balances(&art, solver_id, token_a, token_b).await;

    // Deterministic price feed: both tokens at the same USD price.
    let mut prices: PriceSnapshot = HashMap::new();
    prices.insert(token_a, PRICE_CENTS);
    prices.insert(token_b, PRICE_CENTS);
    let make_price = move |_token_map, _api_key| {
        Ok(Box::new(MockPriceClient::new(prices)) as Box<dyn PriceClient + Send + Sync>)
    };

    let factory: Arc<dyn solver::ClientFactory> = Arc::new(E2eFactory {
        ingest_store: art.solver_ingest_store_path.clone(),
        executor_store: art.solver_executor_store_path.clone(),
        keystore: art.solver_keystore_path.clone(),
    });

    let cancel = CancellationToken::new();
    let c_sig = cancel.clone();
    tokio::task::spawn_local(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received — shutting down");
            c_sig.cancel();
        }
    });
    let c_to = cancel.clone();
    tokio::task::spawn_local(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        tracing::info!(secs, "run window elapsed — shutting down");
        c_to.cancel();
    });

    tracing::info!(%solver_id, secs, "starting solver in-process against devnet (fixed prices)…");
    let result = solver::start(factory, make_price, solver_id, config, cancel).await;

    // Post-run balances → delta = spread captured = settlement proof.
    let (post_a, post_b) = read_solver_balances(&art, solver_id, token_a, token_b).await;
    println!("\n========================= E2E RUN SUMMARY =========================");
    println!(
        "solver {:<4}: {pre_a} -> {post_a}   (Δ {})",
        art.token_a.symbol,
        post_a as i128 - pre_a as i128
    );
    println!(
        "solver {:<4}: {pre_b} -> {post_b}   (Δ {})",
        art.token_b.symbol,
        post_b as i128 - pre_b as i128
    );
    println!("a positive Δ on both tokens = opposing PSWAPs matched, settled on-chain,");
    println!("and the solver captured the spread.");
    println!("==================================================================\n");

    result
}

/// Best-effort: short-lived client on the solver store → sync → read balances.
async fn read_solver_balances(
    art: &Artifacts,
    solver_id: AccountId,
    token_a: AccountId,
    token_b: AccountId,
) -> (u64, u64) {
    match devnet::build_client(&art.solver_executor_store_path, &art.solver_keystore_path).await {
        Ok((mut cli, _)) => {
            let _ = cli.sync_state().await;
            let a = accounts::balance(&cli, solver_id, token_a).await.unwrap_or(0);
            let b = accounts::balance(&cli, solver_id, token_b).await.unwrap_or(0);
            (a, b)
        }
        Err(e) => {
            tracing::warn!(error = %e, "could not read solver balances");
            (0, 0)
        }
    }
}

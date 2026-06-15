//! Mock PSWAP mirroring service — a testnet harness.
//!
//! On testnet there are no natural counter-orders, so the solver never matches.
//! This daemon watches for user PSWAP notes and posts *favorable* counter-orders
//! from a single funded account, so the solver's full match→settle pipeline runs
//! end-to-end. It refills its inventory from faucets when low.
//!
//! Run: `mock-mirror [path/to/mock.toml]` (defaults to `./mock.toml`).

mod config;
mod mirror;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::rpc::{Endpoint, GrpcClient, NodeRpcClient};
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use tracing_subscriber::EnvFilter;

use config::MockConfig;

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let path = std::env::args().nth(1).unwrap_or_else(|| "mock.toml".to_string());
    let cfg = MockConfig::load(&path).with_context(|| format!("load {path}"))?;

    let mut client = build_client(&cfg).await?;
    mirror::run(&mut client, &cfg).await
}

/// Build the account-tracking client (RPC + sqlite store + filesystem keystore).
/// The mock account — and any faucet accounts it mints from — must already be
/// provisioned into the store/keystore out-of-band.
async fn build_client(cfg: &MockConfig) -> Result<Client<FilesystemKeyStore>> {
    let endpoint = Endpoint::try_from(cfg.rpc.endpoint.as_str())
        .map_err(|e| anyhow::anyhow!("parse endpoint: {e}"))?;
    let rpc: Arc<dyn NodeRpcClient> = Arc::new(GrpcClient::new(&endpoint, cfg.rpc.timeout_ms));
    let keystore = Arc::new(
        FilesystemKeyStore::new(PathBuf::from(&cfg.mock.keystore_path))
            .context("open keystore")?,
    );
    ClientBuilder::new()
        .rpc(rpc)
        .sqlite_store(PathBuf::from(&cfg.mock.store_path))
        .authenticator(keystore)
        .build()
        .await
        .context("build miden client")
}

fn main() -> Result<()> {
    // `Client` is `!Send` → single-threaded runtime + LocalSet, like the solver.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run())
}

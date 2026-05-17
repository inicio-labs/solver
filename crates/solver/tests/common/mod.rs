//! Shared helpers for the integration tests in this directory.
//!
//! The architecture: build a `miden_testing::MockChain` with our actors and
//! faucets, wrap it in `miden_client::testing::mock::MockRpcApi` (which
//! implements `NodeRpcClient`), build a real `Client<FilesystemKeyStore>`
//! against that, and feed the client into `solver::start`. Drive virtual
//! time with `tokio::time::advance` + `yield_now` to let the solver tasks
//! tick deterministically.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::rpc::NodeRpcClient;
use miden_client::testing::mock::MockRpcApi;
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use miden_testing::MockChain;
use tempfile::TempDir;

/// Build a `Client<FilesystemKeyStore>` backed by `MockRpcApi` + a tempdir-scoped
/// SQLite store + keystore. Each test gets a fresh tempdir so they don't share
/// on-disk state.
pub async fn build_test_client(
    rpc: Arc<MockRpcApi>,
    keystore_dir: PathBuf,
    store_path: PathBuf,
) -> Result<Client<FilesystemKeyStore>> {
    let keystore = Arc::new(
        FilesystemKeyStore::new(keystore_dir)
            .map_err(|e| anyhow!("FilesystemKeyStore::new: {e}"))?,
    );
    let rpc_dyn: Arc<dyn NodeRpcClient> = rpc;

    ClientBuilder::new()
        .rpc(rpc_dyn)
        .sqlite_store(store_path)
        .authenticator(keystore)
        .in_debug_mode(true.into())
        .build()
        .await
        .map_err(|e| anyhow!("ClientBuilder::build: {e}"))
}

/// Build a keyless **ingest** `Client<FilesystemKeyStore>` backed by the same
/// `MockRpcApi` (shared mock chain) but with **no authenticator and no tracked
/// account** — the chain-watching path. Mirrors production `build_ingest_client`.
/// The `FilesystemKeyStore` type parameter is only a phantom here.
pub async fn build_test_ingest_client(
    rpc: Arc<MockRpcApi>,
    store_path: PathBuf,
) -> Result<Client<FilesystemKeyStore>> {
    let rpc_dyn: Arc<dyn NodeRpcClient> = rpc;

    ClientBuilder::new()
        .rpc(rpc_dyn)
        .sqlite_store(store_path)
        .in_debug_mode(true.into())
        .build()
        .await
        .map_err(|e| anyhow!("ClientBuilder::build (ingest): {e}"))
}

/// Allocate per-test tempdir paths for keystore + sqlite store.
pub fn temp_paths() -> Result<(TempDir, PathBuf, PathBuf)> {
    let dir = TempDir::new()?;
    let keystore = dir.path().join("keystore");
    std::fs::create_dir_all(&keystore)?;
    let store = dir.path().join("store.sqlite3");
    Ok((dir, keystore, store))
}

/// Test [`solver::ClientFactory`] for L2: builds the ingest + executor clients
/// **on their own threads** against the shared `MockRpcApi`. Holds only Send
/// config (`Arc<MockRpcApi>` + paths). The solver account/keys must already be
/// on disk at `executor_store`/`keystore` (provisioned by a throwaway client
/// in test setup) — the rebuilt executor client reloads them from there, just
/// as a production restart would.
pub struct MockClientFactory {
    pub rpc: Arc<MockRpcApi>,
    pub ingest_store: PathBuf,
    pub executor_store: PathBuf,
    pub keystore: PathBuf,
}

#[async_trait::async_trait(?Send)]
impl solver::ClientFactory for MockClientFactory {
    async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>> {
        let mut c = build_test_ingest_client(self.rpc.clone(), self.ingest_store.clone()).await?;
        c.ensure_genesis_in_place()
            .await
            .map_err(|e| anyhow!("ingest genesis: {e}"))?;
        Ok(c)
    }

    async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>> {
        let mut c = build_test_client(
            self.rpc.clone(),
            self.keystore.clone(),
            self.executor_store.clone(),
        )
        .await?;
        c.ensure_genesis_in_place()
            .await
            .map_err(|e| anyhow!("executor genesis: {e}"))?;
        Ok(c)
    }
}

/// Real-time analogue of [`wait_for`] for the L2 threaded model. The solver's
/// ingest/executor threads run on their own real-time runtimes (the test's
/// `start_paused` virtual clock cannot reach them), so we poll on wall-clock
/// time instead of driving virtual time. Each iteration: check the predicate,
/// then `prove_block()` to commit any pending submitted txs, then sleep
/// `poll`. Bails after `max_iterations`.
pub async fn wait_for_realtime<F>(
    rpc: &MockRpcApi,
    max_iterations: u32,
    poll: Duration,
    mut check: F,
) -> Result<()>
where
    F: FnMut(&MockChain) -> bool,
{
    for _ in 0..max_iterations {
        {
            if check(&rpc.mock_chain.read()) {
                return Ok(());
            }
        }
        rpc.prove_block();
        tokio::time::sleep(poll).await;
    }
    if check(&rpc.mock_chain.read()) {
        return Ok(());
    }
    Err(anyhow!(
        "wait_for_realtime timed out after {max_iterations} iterations"
    ))
}

/// Poll for a chain-state predicate, deterministically driving virtual time
/// and the chain forward between checks. Returns when `check` is true; bails
/// after `max_iterations` virtual ticks.
///
/// Requires the test to be `#[tokio::test(start_paused = true)]` so
/// `tokio::time::advance` works. Per iteration:
/// 1. Check the chain state.
/// 2. Advance virtual time past the slowest pipeline interval.
/// 3. Yield several times to let solver tasks run.
/// 4. Call `prove_block()` to commit any pending submitted txs onto the chain.
pub async fn wait_for<F>(rpc: &MockRpcApi, max_iterations: u32, mut check: F) -> Result<()>
where
    F: FnMut(&MockChain) -> bool,
{
    for _ in 0..max_iterations {
        if check(&rpc.mock_chain.read()) {
            return Ok(());
        }
        tokio::time::advance(Duration::from_millis(500)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        rpc.prove_block();
    }
    Err(anyhow!("wait_for timed out after {max_iterations} iterations"))
}

/// Sum of fungible-asset balances of `faucet` held in `account_id`'s vault,
/// reading from the latest committed MockChain state.
pub fn vault_balance(chain: &MockChain, account_id: AccountId, faucet: AccountId) -> u64 {
    use miden_protocol::asset::Asset;
    chain
        .committed_account(account_id)
        .map(|account| {
            account
                .vault()
                .assets()
                .filter_map(|asset| match asset {
                    Asset::Fungible(f) if f.faucet_id() == faucet => Some(u64::from(f.amount())),
                    _ => None,
                })
                .sum::<u64>()
        })
        .unwrap_or(0)
}

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::rpc::encryption::TransactionEncryptionKey;
use miden_client::rpc::NodeRpcClient;
use miden_client::testing::mock::MockRpcApi;
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::dsa::eddsa_25519_sha512::KeyExchangeKey;
use miden_testing::MockChain;
use tempfile::TempDir;

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

/// One fresh PostgreSQL application schema for a full solver integration test.
/// Each integration file has one solver test, so its process-local URL settings
/// cannot race another test in the same process.
pub struct PgSchema {
    pub url: String,
    base_url: String,
    name: String,
    prior_writer: Option<String>,
    prior_reader: Option<String>,
}

impl PgSchema {
    pub async fn new() -> Result<Self> {
        let base_url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let setup_url = base_url.clone();
        let (name, url) = tokio::task::spawn_blocking(move || -> Result<_> {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!("solver_it_{}_{}_{}", std::process::id(), nonce, sequence);
            let mut admin = solver::db::postgres_migrations::connect(&setup_url)?;
            admin.batch_execute(&format!("CREATE SCHEMA {name}"))?;
            let separator = if setup_url.contains('?') { '&' } else { '?' };
            let url = format!("{setup_url}{separator}options=-csearch_path%3D{name}");
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            solver::db::postgres_migrations::migrate(&mut conn)?;
            Ok((name, url))
        })
        .await??;
        let prior_writer = std::env::var("SOLVER_DATABASE_URL").ok();
        let prior_reader = std::env::var("SOLVER_READ_DATABASE_URL").ok();
        std::env::set_var("SOLVER_DATABASE_URL", &url);
        std::env::set_var("SOLVER_READ_DATABASE_URL", &url);
        Ok(Self {
            url,
            base_url,
            name,
            prior_writer,
            prior_reader,
        })
    }
}

impl Drop for PgSchema {
    fn drop(&mut self) {
        match &self.prior_writer {
            Some(value) => std::env::set_var("SOLVER_DATABASE_URL", value),
            None => std::env::remove_var("SOLVER_DATABASE_URL"),
        }
        match &self.prior_reader {
            Some(value) => std::env::set_var("SOLVER_READ_DATABASE_URL", value),
            None => std::env::remove_var("SOLVER_READ_DATABASE_URL"),
        }
        let base = self.base_url.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(mut admin) = solver::db::postgres_migrations::connect(&base) {
                let _ = admin.batch_execute(&format!("DROP SCHEMA {name} CASCADE"));
            }
        })
        .join();
    }
}

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

    let mut client = ClientBuilder::new()
        .rpc(rpc_dyn)
        .sqlite_store(store_path)
        .authenticator(keystore)
        .build()
        .await
        .map_err(|e| anyhow!("ClientBuilder::build: {e}"))?;
    seed_encryption_key(&mut client).await?;
    Ok(client)
}

/// Miden 0.16 seals every submitted transaction's inputs against the validators'
/// encryption key, which `MockRpcApi` does not serve (it can't produce the
/// validator attestation). Seed an unattested key instead: the mock never unseals,
/// so any key bound to this chain's genesis works.
async fn seed_encryption_key(client: &mut Client<FilesystemKeyStore>) -> Result<()> {
    client
        .ensure_genesis_in_place()
        .await
        .map_err(|e| anyhow!("genesis: {e}"))?;
    let (genesis, _) = client
        .get_block_header_by_num(BlockNumber::GENESIS)
        .await
        .map_err(|e| anyhow!("genesis header: {e}"))?
        .ok_or_else(|| anyhow!("genesis header missing after ensure_genesis_in_place"))?;
    let key = TransactionEncryptionKey::new_unattested(
        b"mock".to_vec(),
        KeyExchangeKey::new().public_key(),
        genesis.commitment(),
    );
    client
        .seed_transaction_encryption_key(key)
        .await
        .map_err(|e| anyhow!("seed encryption key: {e}"))
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

    fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>> {
        let r: Arc<dyn NodeRpcClient> = self.rpc.clone();
        Ok(r)
    }
}

/// Factory whose `build_ingest` succeeds (keyless client) but `build_executor`
/// always fails — exercises the L2 startup-failure path: the executor
/// readiness `oneshot` carries `Err`, so `start()` must cancel, join *both*
/// client OS threads, and return a clean error (no hang, no SIGABRT).
pub struct FailingExecutorFactory {
    pub rpc: Arc<MockRpcApi>,
    pub ingest_store: PathBuf,
}

#[async_trait::async_trait(?Send)]
impl solver::ClientFactory for FailingExecutorFactory {
    async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>> {
        let mut c = build_test_ingest_client(self.rpc.clone(), self.ingest_store.clone()).await?;
        c.ensure_genesis_in_place()
            .await
            .map_err(|e| anyhow!("ingest genesis: {e}"))?;
        Ok(c)
    }

    async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>> {
        Err(anyhow!(
            "injected executor build failure (startup-failure test)"
        ))
    }

    fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>> {
        let r: Arc<dyn NodeRpcClient> = self.rpc.clone();
        Ok(r)
    }
}

/// Wait for the executor to finish proving and submit a settlement before the
/// test advances the mock chain. Proving empty blocks while the executor works
/// can pass the transaction's expiry height before it is submitted.
pub async fn wait_for_submitted_settlement<F>(
    db_url: &str,
    max_wait: Duration,
    solver_stopped: F,
) -> Result<()>
where
    F: Fn() -> bool,
{
    let started = std::time::Instant::now();
    loop {
        if solver_stopped() {
            return Err(anyhow!("solver stopped before submitting the settlement"));
        }
        let url = db_url.to_owned();
        let submitted = tokio::task::spawn_blocking(move || -> Result<bool> {
            use solver::db::postgres_schema::settlement_attempts;

            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            // A prepared attempt is written only after proving, immediately
            // before submission; it cannot be confirmed (and deleted) until
            // the test advances the mock chain.
            let count: i64 = settlement_attempts::table.count().get_result(&mut conn)?;
            Ok(count > 0)
        })
        .await??;
        if submitted {
            return Ok(());
        }
        if started.elapsed() >= max_wait {
            return Err(anyhow!("settlement was not submitted within {max_wait:?}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
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
    Err(anyhow!(
        "wait_for timed out after {max_iterations} iterations"
    ))
}

/// Sum of fungible-asset balances of `faucet` held in `account_id`'s vault,
/// reading from the latest committed MockChain state.
pub fn vault_balance(chain: &MockChain, account_id: AccountId, faucet: AccountId) -> u64 {
    chain
        .committed_account(account_id)
        .map(|account| {
            account
                .vault()
                .assets()
                .filter_map(|asset| asset.as_fungible())
                .filter(|asset| asset.faucet_id() == faucet)
                .map(|asset| asset.amount().as_u64())
                .sum::<u64>()
        })
        .unwrap_or(0)
}

/// A mock Binance server on its own thread and runtime, so a test thread busy
/// driving the mock chain cannot stall its quotes. Stops when dropped.
pub struct BinanceStub {
    mock: mock_binance::MockThread,
}

impl BinanceStub {
    /// Serve `markets` (`SYMBOL`, base asset, quote asset, mid price), each
    /// quoted with a 10 bps spread around the mid, so a test that passed on
    /// the bid or the ask instead of the midpoint would notice.
    pub fn start(markets: &[(&str, &str, &str, &str)]) -> Self {
        let markets: Vec<_> = markets
            .iter()
            .map(|(symbol, base, quote, mid)| {
                let mid: f64 = mid.parse().expect("decimal mid");
                let bid = (mid * 0.9995).to_string();
                let ask = (mid * 1.0005).to_string();
                mock_binance::Market::new(symbol, base, quote, &bid, &ask)
            })
            .collect();
        Self {
            mock: mock_binance::MockThread::start(markets, Default::default()),
        }
    }

    /// A `[binance]` section pointing both readers at this server. The mock
    /// republishes every 250 ms, so a 30 s TTL only expires if the feed
    /// itself stops publishing.
    pub fn config(&self) -> solver::config::BinanceConfig {
        solver::config::BinanceConfig {
            stream_endpoints: [self.mock.ws_url(), self.mock.ws_url()],
            rest_endpoint: self.mock.rest_url(),
            quote_ttl: std::time::Duration::from_secs(30),
            max_spread_bps: 100,
            retry_min: std::time::Duration::from_millis(50),
            retry_max: std::time::Duration::from_millis(500),
            ..Default::default()
        }
    }
}

/// The `[engine]` section the solver integration tests run with.
pub fn engine_config() -> solver::config::EngineConfig {
    solver::config::EngineConfig {
        pulse_interval_ms: 200,
        fetch_interval_ms: 100,
        clearing_fee_ppm: 0,
        admin_port: 0,
        debug_mode: false,
        obs_port: 0,
        readiness_freshness_secs: 60,
        verify_interval_ms: 5_000,
        // Any free port: a fixed one fails startup when something else holds it.
        price_query_port: 0,
        price_query_bind: "127.0.0.1".to_string(),
        price_query_max_inflight: 128,
        price_query_max_batch: 50,
        price_query_timeout_ms: 3000,
        price_precision: "full".to_string(),
        swap_proving_estimate_ms: 2000,
        swap_block_time_ms: 6000,
        swap_offmarket_tolerance_bps: 50,
        router_enabled: false,
        router_bind: "127.0.0.1".to_string(),
        router_port: 0,
        router_max_connections: 64,
        router_max_msg_bytes: 16384,
        router_quote_ttl_ms: 20_000,
        router_inflight_ttl_ms: 30_000,
    }
}

/// A pair priced by the Binance market `symbol` between `x_asset` and `y_asset`.
pub fn pair_config(
    name: &str,
    x: AccountId,
    x_asset: &str,
    y: AccountId,
    y_asset: &str,
    symbol: &str,
) -> solver::config::AssetPairConfig {
    let asset = |code: &str| solver::price::AssetCode::parse(code).expect("valid asset code");
    solver::config::AssetPairConfig {
        name: name.to_string(),
        asset_x_faucet_id: x.to_hex(),
        asset_x_binance_asset: Some(asset(x_asset)),
        asset_y_faucet_id: y.to_hex(),
        asset_y_binance_asset: Some(asset(y_asset)),
        binance_symbol: Some(solver::price::Symbol::parse(symbol).expect("valid symbol")),
    }
}

/// A pair whose `x` token has no Binance market: it never clears internally.
pub fn unpriced_pair_config(
    name: &str,
    x: AccountId,
    y: AccountId,
    y_asset: &str,
) -> solver::config::AssetPairConfig {
    solver::config::AssetPairConfig {
        name: name.to_string(),
        asset_x_faucet_id: x.to_hex(),
        asset_x_binance_asset: None,
        asset_y_faucet_id: y.to_hex(),
        asset_y_binance_asset: Some(
            solver::price::AssetCode::parse(y_asset).expect("valid asset code"),
        ),
        binance_symbol: None,
    }
}

/// Orders in the solver's database with `status`.
pub async fn count_orders(db_url: &str, status: &'static str) -> i64 {
    let url = db_url.to_owned();
    tokio::task::spawn_blocking(move || {
        use solver::db::postgres_schema::orders;
        let mut conn = solver::db::postgres_migrations::connect(&url).expect("connect");
        orders::table
            .filter(orders::status.eq(status))
            .count()
            .get_result::<i64>(&mut conn)
            .expect("count orders")
    })
    .await
    .expect("count task")
}

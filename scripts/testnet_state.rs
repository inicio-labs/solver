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
use serde::{Deserialize, Serialize};

const STATE_FILE: &str = "test_state.json";
const TESTNET_ENDPOINT: &str = "https://rpc.testnet.miden.io";
const TESTNET_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Serialize, Deserialize)]
pub struct TestState {
    pub faucet_usdt_id: String,
    pub faucet_eth_id: String,
    pub alice_id: String,
    pub bob_id: String,
}

impl TestState {
    pub fn new(
        faucet_usdt: AccountId,
        faucet_eth: AccountId,
        alice: AccountId,
        bob: AccountId,
    ) -> Self {
        Self {
            faucet_usdt_id: faucet_usdt.to_hex(),
            faucet_eth_id: faucet_eth.to_hex(),
            alice_id: alice.to_hex(),
            bob_id: bob.to_hex(),
        }
    }

    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(STATE_FILE, json).context("Failed to write test state file")?;
        println!("State saved to {}", STATE_FILE);
        Ok(())
    }

    pub fn load() -> Result<Self> {
        let json = std::fs::read_to_string(STATE_FILE)
            .context("Failed to read test state file. Did you run setup first?")?;
        serde_json::from_str(&json).context("Failed to parse test state file")
    }

    pub fn faucet_usdt(&self) -> Result<AccountId> {
        AccountId::from_hex(&self.faucet_usdt_id).context("Failed to parse USDT faucet ID")
    }

    pub fn faucet_eth(&self) -> Result<AccountId> {
        AccountId::from_hex(&self.faucet_eth_id).context("Failed to parse ETH faucet ID")
    }

    pub fn alice(&self) -> Result<AccountId> {
        AccountId::from_hex(&self.alice_id).context("Failed to parse Alice ID")
    }

    pub fn bob(&self) -> Result<AccountId> {
        AccountId::from_hex(&self.bob_id).context("Failed to parse Bob ID")
    }
}

/// Build a client connected to testnet with the given keystore and store paths.
pub async fn build_testnet_client(
    keystore_path: &str,
    store_path: &str,
) -> Result<(Client<FilesystemKeyStore>, Arc<FilesystemKeyStore>)> {
    let endpoint = Endpoint::try_from(TESTNET_ENDPOINT)
        .map_err(|e| anyhow::anyhow!("Failed to parse endpoint: {}", e))?;
    let rpc_client: Arc<dyn NodeRpcClient> =
        Arc::new(GrpcClient::new(&endpoint, TESTNET_TIMEOUT_MS));

    let keystore_path = std::path::PathBuf::from(keystore_path);
    let keystore = Arc::new(
        FilesystemKeyStore::new(keystore_path).context("Failed to initialize keystore")?,
    );

    let store_path = std::path::PathBuf::from(store_path);

    let client = ClientBuilder::new()
        .rpc(rpc_client)
        .sqlite_store(store_path)
        .authenticator(keystore.clone())
        .in_debug_mode(true.into())
        .build()
        .await
        .context("Failed to build client")?;

    Ok((client, keystore))
}

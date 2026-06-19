//! Mock PSWAP mirroring service — a devnet/testnet liquidity harness.
//!
//! On a fresh network there are no natural counter-orders, so the solver never
//! matches. This daemon watches for user PSWAP notes and posts *favorable*
//! counter-orders from a single account, so the solver's full match→settle
//! pipeline runs end-to-end. It is funded **externally** (mint the public
//! faucets to its address) — it never mints itself.
//!
//! Subcommands:
//!   `mock-mirror provision [mock.toml]` — create the mock account, print its
//!       address (hex + bech32), then exit. Fund that address, set
//!       `[mock].account_id`, and run.
//!   `mock-mirror run [mock.toml]`       — run the mirror loop (the default if
//!       the first arg is omitted or is a bare config path).

mod config;
mod mirror;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::account::component::BasicWallet;
use miden_client::account::{
    AccountBuilder, AccountBuilderSchemaCommitmentExt, AccountFile, AccountType,
};
use miden_client::auth::{AuthSchemeId, AuthSecretKey, AuthSingleSig};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::rpc::{Endpoint, GrpcClient, NodeRpcClient};
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::address::Address;
use rand::RngCore;
use tracing_subscriber::EnvFilter;

use config::MockConfig;

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // args: `mock-mirror [provision|run] [config]`. A bare path (no subcommand)
    // is treated as `run <path>` for back-compat.
    let args: Vec<String> = std::env::args().collect();
    let (cmd, path): (&str, String) = match args.get(1).map(String::as_str) {
        Some("provision") => ("provision", args.get(2).cloned().unwrap_or_else(default_config)),
        Some("run") => ("run", args.get(2).cloned().unwrap_or_else(default_config)),
        Some(p) => ("run", p.to_string()),
        None => ("run", default_config()),
    };

    let cfg = MockConfig::load(&path).with_context(|| format!("load {path}"))?;

    match cmd {
        "provision" => provision(&cfg).await,
        _ => {
            let (mut client, _keystore) = build_client(&cfg).await?;
            mirror::run(&mut client, &cfg).await
        }
    }
}

fn default_config() -> String {
    "mock.toml".to_string()
}

/// Build the account-tracking client (RPC + sqlite store + filesystem keystore).
/// Returns the keystore handle too, since key registration during provisioning
/// needs the same instance handed to the builder.
async fn build_client(
    cfg: &MockConfig,
) -> Result<(Client<FilesystemKeyStore>, Arc<FilesystemKeyStore>)> {
    let endpoint = Endpoint::try_from(cfg.rpc.endpoint.as_str())
        .map_err(|e| anyhow::anyhow!("parse endpoint: {e}"))?;
    let rpc: Arc<dyn NodeRpcClient> = Arc::new(GrpcClient::new(&endpoint, cfg.rpc.timeout_ms));
    let keystore = Arc::new(
        FilesystemKeyStore::new(PathBuf::from(&cfg.mock.keystore_path)).context("open keystore")?,
    );
    let client = ClientBuilder::new()
        .rpc(rpc)
        .sqlite_store(PathBuf::from(&cfg.mock.store_path))
        .authenticator(keystore.clone())
        .build()
        .await
        .context("build miden client")?;
    Ok((client, keystore))
}

/// Create a fresh **public** wallet for the mock, persist its key + account into
/// the configured keystore/store, and print the address. The account is deployed
/// lazily on its first transaction; the user funds it out-of-band by minting the
/// public faucets to the printed `mdev…` address.
async fn provision(cfg: &MockConfig) -> Result<()> {
    let (mut client, keystore) = build_client(cfg).await?;

    let key = AuthSecretKey::new_falcon512_poseidon2();
    let auth = AuthSingleSig::new(key.public_key().to_commitment(), AuthSchemeId::Falcon512Poseidon2);

    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);

    let account = AccountBuilder::new(seed)
        .account_type(AccountType::Public)
        .with_auth_component(auth)
        .with_component(BasicWallet)
        .build_with_schema_commitment()
        .context("build wallet account")?;

    let id = account.id();
    keystore.add_key(&key, id).await.context("add key to keystore")?;
    client.add_account(&account, false).await.context("add account to store")?;

    // Optional portable export: a `.mac` file bundling the account + its key
    // (sensitive — it contains the secret key). The running client uses the
    // keystore + store; this is a backup you can keep / re-import elsewhere.
    if let Some(mac) = &cfg.mock.account_file {
        AccountFile::new(account, vec![key])
            .write(mac)
            .with_context(|| format!("write account file {mac}"))?;
    }

    let hex = id.to_hex();
    let bech32 = Address::new(id).encode(cfg.rpc.network_id()?);
    println!("\n=== mock-mirror account provisioned ===");
    println!("hex     : {hex}");
    println!("bech32  : {bech32}  (network: {})", cfg.rpc.network);
    println!("keystore: {}", cfg.mock.keystore_path);
    println!("store   : {}", cfg.mock.store_path);
    if let Some(mac) = &cfg.mock.account_file {
        println!("mac file: {mac}  (portable account+key backup)");
    }
    println!("\nNext steps:");
    println!("  1. set  [mock] account_id = \"{hex}\"  in your config");
    println!("  2. fund this account by minting IBTC / IUSDT / IETH / IMIDEN");
    println!("     from your faucet to:  {bech32}");
    println!("  3. run:  mock-mirror run <config>\n");
    Ok(())
}

fn main() -> Result<()> {
    // `Client` is `!Send` → single-threaded runtime + LocalSet, like the solver.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run())
}

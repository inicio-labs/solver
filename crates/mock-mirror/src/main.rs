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
mod ops;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_client::account::component::BasicWallet;
use miden_client::account::{
    AccountBuilder, AccountBuilderSchemaCommitmentExt, AccountFile, AccountType,
};
use miden_client::auth::{AuthSchemeId, AuthSecretKey, AuthSingleSig};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::note::NoteType;
use miden_client::rpc::{Endpoint, GrpcClient, NodeRpcClient};
use miden_client::{Client, RemoteTransactionProver};
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use miden_protocol::address::{Address, NetworkId};
use miden_protocol::asset::FungibleAsset;
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

    // Stateless / ops subcommands handled before the config-driven run/provision.
    match args.get(1).map(String::as_str) {
        // `addr <id>...` — convert bech32 (mdev…/mtst…/mm…) ↔ hex. No config.
        Some("addr") => return addr_convert(&args[2..]),
        // `claim <config>` — consume minted notes into the account's vault.
        Some("claim") => return run_claim(&args[2..]).await,
        // `pswap <config> <offer_faucet> <offer_amt> <req_faucet> <req_amt>`.
        Some("pswap") => return run_pswap(&args[2..]).await,
        // `balance <config> <faucet_hex>...` — print vault balances.
        Some("balance") => return run_balance(&args[2..]).await,
        _ => {}
    }

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

/// Load the configured account id + a built client.
async fn account_and_client(config: &str) -> Result<(AccountId, Client<FilesystemKeyStore>)> {
    let cfg = MockConfig::load(config).with_context(|| format!("load {config}"))?;
    let account = AccountId::from_hex(
        cfg.mock.account_id.as_deref().context("mock.account_id required")?,
    )
    .context("parse mock.account_id")?;
    let (client, _keystore) = build_client(&cfg).await?;
    Ok((account, client))
}

/// `claim <config>` — consume the faucet's mint notes into the account's vault.
async fn run_claim(a: &[String]) -> Result<()> {
    let config = a.first().context("usage: mock-mirror claim <config>")?;
    let (account, mut client) = account_and_client(config).await?;
    let n = ops::claim(&mut client, account).await?;
    println!("claimed {n} note(s) into {}", account.to_hex());
    Ok(())
}

/// `pswap <config> <offer_faucet> <offer_amt> <req_faucet> <req_amt> [public|private]`.
/// The trailing arg sets the PAYBACK note type (default public); the PSWAP order
/// itself is always public so the solver can discover it.
async fn run_pswap(a: &[String]) -> Result<()> {
    let usage = "usage: mock-mirror pswap <config> <offer_faucet> <offer_amt> <req_faucet> <req_amt> [public|private]";
    let config = a.first().context(usage)?;
    let offer_faucet = AccountId::from_hex(a.get(1).context(usage)?).context("offer_faucet")?;
    let offer_amt: u64 = a.get(2).context(usage)?.parse().context("offer_amt")?;
    let req_faucet = AccountId::from_hex(a.get(3).context(usage)?).context("req_faucet")?;
    let req_amt: u64 = a.get(4).context(usage)?.parse().context("req_amt")?;
    let payback = match a.get(5).map(String::as_str) {
        Some("private") => NoteType::Private,
        Some("public") | None => NoteType::Public,
        Some(other) => anyhow::bail!("payback type must be public|private, got {other:?}"),
    };

    let (account, mut client) = account_and_client(config).await?;
    let offered = FungibleAsset::new(offer_faucet, offer_amt).map_err(|e| anyhow::anyhow!("offered: {e}"))?;
    let requested = FungibleAsset::new(req_faucet, req_amt).map_err(|e| anyhow::anyhow!("requested: {e}"))?;
    ops::create_pswap(&mut client, account, offered, requested, payback).await?;
    println!(
        "PSWAP created from {}: offer {offer_amt} of {} / request {req_amt} of {} (payback {payback:?})",
        account.to_hex(),
        offer_faucet.to_hex(),
        req_faucet.to_hex()
    );
    Ok(())
}

/// `balance <config> <faucet_hex>...` — print vault balances.
async fn run_balance(a: &[String]) -> Result<()> {
    let config = a.first().context("usage: mock-mirror balance <config> <faucet_hex>...")?;
    let faucets: Vec<AccountId> = a[1..]
        .iter()
        .map(|s| AccountId::from_hex(s).with_context(|| format!("parse faucet {s}")))
        .collect::<Result<_>>()?;
    let (account, mut client) = account_and_client(config).await?;
    println!("account {}", account.to_hex());
    ops::balances(&mut client, account, &faucets).await
}

/// Convert account ids between bech32 and hex (both directions).
fn addr_convert(ids: &[String]) -> Result<()> {
    for s in ids {
        if let Ok((net, id)) = AccountId::from_bech32(s) {
            println!("{s}  ->  hex {}  (network {net:?})", id.to_hex());
        } else if let Ok(id) = AccountId::from_hex(s) {
            let dev = Address::new(id).encode(NetworkId::Devnet);
            let test = Address::new(id).encode(NetworkId::Testnet);
            println!("{s}  ->  devnet {dev}  |  testnet {test}");
        } else {
            println!("{s}  ->  ERROR: not a valid bech32 or hex account id");
        }
    }
    Ok(())
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
    let mut builder = ClientBuilder::new()
        .rpc(rpc)
        .sqlite_store(PathBuf::from(&cfg.mock.store_path))
        .authenticator(keystore.clone());
    if let Some(url) = &cfg.rpc.prover_endpoint {
        // Raise the prover timeout above the miden-client 10s default: a busy
        // public testnet prover routinely takes longer, and a spurious timeout
        // is what strands an un-countered order.
        let prover = RemoteTransactionProver::new(url.clone())
            .with_timeout(Duration::from_millis(cfg.rpc.prover_timeout_ms));
        builder = builder.prover(Arc::new(prover));
    }
    let client = builder.build().await.context("build miden client")?;
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

//! Devnet client construction.
//!
//! `ClientBuilder::for_devnet()` pre-wires the devnet gRPC endpoint
//! (`rpc.devnet.miden.io`) AND the remote transaction prover
//! (`tx-prover.devnet.miden.io`), so proof generation is offloaded to the
//! network prover — fast, no local proving required. We only layer on the
//! SQLite store + filesystem keystore.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::Client;
use miden_client_sqlite_store::ClientBuilderSqliteExt;

/// Devnet RPC endpoint (informational; `for_devnet()` sets it internally).
pub const DEVNET_RPC: &str = "https://rpc.devnet.miden.io";

/// Build a devnet [`Client`] backed by `store_path` (SQLite) and a filesystem
/// keystore at `keystore_path`. Returns the client and a handle to the same
/// keystore (so callers can `add_key` for accounts they create).
pub async fn build_client(
    store_path: &str,
    keystore_path: &str,
) -> Result<(Client<FilesystemKeyStore>, Arc<FilesystemKeyStore>)> {
    let keystore = Arc::new(
        FilesystemKeyStore::new(PathBuf::from(keystore_path)).context("initialize keystore")?,
    );

    let client = ClientBuilder::for_devnet()
        .authenticator(keystore.clone())
        .sqlite_store(PathBuf::from(store_path))
        .build()
        .await
        .context("build devnet client")?;

    Ok((client, keystore))
}

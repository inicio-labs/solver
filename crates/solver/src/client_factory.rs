//! L2 client construction seam.
//!
//! At L2 each `!Send` miden [`Client`] lives on its own OS thread, so
//! [`start`](crate::start) cannot receive pre-built clients (a `!Send` value
//! cannot be moved into a spawned thread). It receives a [`ClientFactory`]
//! instead: the factory itself is `Send + Sync` — it holds only Send config
//! (an endpoint string in production, an `Arc<MockRpcApi>` in tests) — so it
//! can be cloned into each client thread, where the client is built *on that
//! thread* via this trait.
//!
//! The trait methods use `#[async_trait(?Send)]`: the returned futures are
//! `!Send` (they construct the `!Send` `Client`), which is fine because they
//! are awaited inside the owning thread's `LocalSet`. The trait *object*
//! (`Arc<dyn ClientFactory>`) is still `Send + Sync` via the supertrait
//! bounds, so it crosses the thread boundary cleanly.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::rpc::NodeRpcClient;
use miden_client::Client;

#[async_trait(?Send)]
pub trait ClientFactory: Send + Sync + 'static {
    /// Build the keyless **ingest** client: RPC + its own sqlite store, with
    /// **no authenticator and no tracked account**. The chain-watching path
    /// holds no signing keys.
    async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>>;

    /// Build the keystore-backed **executor** client: RPC + sqlite store +
    /// `FilesystemKeyStore` authenticator. The signing path; the solver
    /// account + keys are provisioned into the store/keystore out-of-band.
    async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>>;

    /// Build a standalone RPC client for the **same node** the clients use.
    ///
    /// Used by the executor's zombie-note classification path
    /// (`MidenClientAdapter::check_consumed_notes`) to query nullifier commit
    /// heights directly, instead of reaching a `Client`'s internal RPC via
    /// the `#[cfg(feature = "testing")]`-gated `Client::test_rpc_api()`
    /// accessor — which previously forced the production build to enable
    /// miden-client's `testing` feature. Synchronous: constructing a gRPC
    /// client / cloning a mock handle does no I/O.
    fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>>;
}

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::{
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    rpc::{Endpoint, GrpcClient, NodeRpcClient},
    Client,
};
use miden_client_sqlite_store::ClientBuilderSqliteExt;

use solver::config::SolverConfig;

/// Production [`solver::ClientFactory`]. Holds only `Send` config
/// (endpoint string, timeout, paths, debug flag) so it can be cloned into
/// each client OS thread; `build_*` run on the owning thread and construct
/// the RPC client + miden `Client` there, so the `!Send` `Client` and the
/// tonic gRPC internals never cross a thread boundary.
pub(crate) struct ProdClientFactory {
    endpoint: String,
    timeout_ms: u64,
    debug: bool,
    ingest_store_path: String,
    executor_store_path: String,
    keystore_path: String,
}

impl ProdClientFactory {
    pub(crate) fn from_config(config: &SolverConfig) -> Self {
        Self {
            endpoint: config.rpc.endpoint.clone(),
            timeout_ms: config.rpc.timeout_ms,
            debug: config.engine.debug_mode,
            ingest_store_path: config.solver.ingest_store_path.clone(),
            executor_store_path: config.solver.executor_store_path.clone(),
            keystore_path: config.solver.keystore_path.clone(),
        }
    }

    fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>> {
        let endpoint = Endpoint::try_from(self.endpoint.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse endpoint: {}", e))?;
        Ok(Arc::new(GrpcClient::new(&endpoint, self.timeout_ms)))
    }
}

#[async_trait::async_trait(?Send)]
impl solver::ClientFactory for ProdClientFactory {
    /// Keyless **ingest** client: RPC + its own sqlite store, **no
    /// authenticator and no tracked account** — the chain-watching path
    /// never signs, so it holds no keys on disk. The `FilesystemKeyStore`
    /// type parameter is only a phantom; no keystore is constructed.
    async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>> {
        ClientBuilder::new()
            .rpc(self.rpc()?)
            .sqlite_store(PathBuf::from(&self.ingest_store_path))
            .in_debug_mode(self.debug.into())
            .build()
            .await
            .context("Failed to build ingest (keyless) Miden client")
    }

    /// **Executor** client: RPC + sqlite store + `FilesystemKeyStore`
    /// authenticator. The signing path; the solver account + keys are
    /// provisioned out-of-band into the store/keystore by the operator.
    async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>> {
        let keystore = Arc::new(
            FilesystemKeyStore::new(PathBuf::from(&self.keystore_path))
                .context("Failed to initialize keystore")?,
        );
        ClientBuilder::new()
            .rpc(self.rpc()?)
            .sqlite_store(PathBuf::from(&self.executor_store_path))
            .authenticator(keystore)
            .in_debug_mode(self.debug.into())
            .build()
            .await
            .context("Failed to build executor Miden client")
    }
}

//! Configuration types for the solver binary, sourced from `solver.toml`.
//!
//! Living in the library so both `solver::start` and `main.rs` can read the
//! same struct without a reverse dependency from library → binary.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SolverConfig {
    pub rpc: RpcConfig,
    pub solver: SolverAccountConfig,
    pub pairs: Vec<AssetPairConfig>,
    pub engine: EngineConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RpcConfig {
    pub endpoint: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SolverAccountConfig {
    pub account_id: String,
    pub keystore_path: String,
    /// Diesel/SQLite **application DB** path (orders, notes, tokens, sync
    /// state). Distinct from the two miden-client stores below.
    pub app_db_path: String,
    /// **Executor** miden-client sqlite store path. The signing path; the
    /// solver account state lives here, its keys in `keystore_path`.
    pub executor_store_path: String,
    /// **Keyless ingest** miden-client sqlite store path. The chain-watching
    /// path holds no signing keys and syncs independently of the executor.
    /// Must be a different file from `executor_store_path` and `app_db_path`.
    pub ingest_store_path: String,
    /// Number of concurrent SQLite read connections. Defaults to 4 if omitted.
    /// Bump if the matcher hydration / admin queries become read-contended.
    #[serde(default = "default_read_pool_size")]
    pub read_pool_size: u32,
}

fn default_read_pool_size() -> u32 {
    4
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AssetPairConfig {
    pub name: String,
    pub asset_x_faucet_id: String,
    /// Optional CoinGecko-style ID (e.g. `"tether"`, `"ethereum"`) for the
    /// `asset_x` faucet's underlying token. Used by the production price
    /// client to look up USD prices. Tokens without a mapping fall back to
    /// the 1-cent default in matching.
    #[serde(default)]
    pub asset_x_external_symbol: Option<String>,
    pub asset_y_faucet_id: String,
    /// See `asset_x_external_symbol`.
    #[serde(default)]
    pub asset_y_external_symbol: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineConfig {
    pub pulse_interval_ms: u64,
    pub fetch_interval_ms: u64,
    /// How often the price feed task polls upstream (CoinGecko) for new
    /// prices. Matcher reads from a watch channel on every pulse regardless,
    /// so this only affects how stale the prices can get, not the matcher's
    /// tick rate.
    pub price_interval_ms: u64,
    /// Whether the 3-edge cycle (triangular) matching phase runs each tick.
    /// Direct (pairwise) matching always runs. Defaults to `true` when
    /// omitted from `solver.toml` so existing configs continue to behave
    /// the same. Disable to skip the O(T³) enumeration on large token sets.
    #[serde(default = "default_true")]
    pub triangular_enabled: bool,
    /// TCP port the admin HTTP server binds on `127.0.0.1`. Defaults to 3001.
    #[serde(default = "default_admin_port")]
    pub admin_port: u16,
    /// Whether to run the Miden client in debug mode. Enables MASM debug
    /// instrumentation. Useful for testnet diagnostics; MUST be `false` for
    /// mainnet. Defaults to `false` when omitted from `solver.toml`.
    #[serde(default)]
    pub debug_mode: bool,
    /// TCP port the observability HTTP server binds on `127.0.0.1`. Exposes
    /// `/health` (liveness) and `/readyz` (readiness). No auth — meant for
    /// process supervisors and monitoring scrapers. Defaults to 9090.
    #[serde(default = "default_obs_port")]
    pub obs_port: u16,
    /// Readiness threshold in seconds. `/readyz` returns 503 if the time
    /// since the last successful sync_state exceeds this. Tune for chain
    /// block time + expected RPC latency. Defaults to 60s.
    #[serde(default = "default_readiness_freshness_secs")]
    pub readiness_freshness_secs: u64,
}

fn default_true() -> bool {
    true
}

fn default_admin_port() -> u16 {
    3001
}

fn default_obs_port() -> u16 {
    9090
}

fn default_readiness_freshness_secs() -> u64 {
    60
}

impl SolverConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        toml::from_str(&content).context("Failed to parse config file")
    }

    pub fn save(&self, path: &str) -> Result<()> {
        let content = toml::to_string_pretty(self).context("Failed to serialize config")?;
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write config file: {}", path))
    }
}

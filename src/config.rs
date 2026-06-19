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
    pub store_path: String,
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
}

fn default_true() -> bool {
    true
}

fn default_admin_port() -> u16 {
    3001
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

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SolverConfig {
    pub rpc: RpcConfig,
    pub solver: SolverAccountConfig,
    pub pairs: Vec<AssetPairConfig>,
    pub engine: EngineConfig,
}

#[derive(Debug, Deserialize)]
pub struct RpcConfig {
    pub endpoint: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct SolverAccountConfig {
    pub account_id: String,
    pub keystore_path: String,
    pub store_path: String,
}

#[derive(Debug, Deserialize)]
pub struct AssetPairConfig {
    pub name: String,
    pub asset_x_faucet_id: String,
    pub asset_y_faucet_id: String,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub pulse_interval_ms: u64,
    pub fetch_interval_ms: u64,
}

impl SolverConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        toml::from_str(&content).context("Failed to parse config file")
    }
}

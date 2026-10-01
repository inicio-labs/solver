//! Configuration types for the solver binary, sourced from `solver.toml`.
//!
//! Living in the library so both `solver::start` and `main.rs` can read the
//! same struct without a reverse dependency from library → binary.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
enum ConfigError {
    #[error("engine.price_precision must be \"full\" or an integer 0..=18, got {0:?}")]
    InvalidPricePrecision(String),
    #[error("engine.price_vs_currency must be non-empty")]
    EmptyPriceCurrency,
    #[error("engine.clearing_fee_ppm must be below {maximum}, got {fee}")]
    InvalidClearingFee { fee: u32, maximum: u32 },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolverConfig {
    pub rpc: RpcConfig,
    pub solver: SolverAccountConfig,
    pub pairs: Vec<AssetPairConfig>,
    pub engine: EngineConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    pub endpoint: String,
    pub timeout_ms: u64,
    /// Optional remote transaction-prover URL (e.g.
    /// `https://tx-prover.devnet.miden.io`). When set, the executor offloads
    /// proof generation to it instead of proving locally — essential on small
    /// hosts where local STARK proving is too slow / memory-heavy. Omit to
    /// prove locally.
    #[serde(default)]
    pub prover_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolverAccountConfig {
    pub account_id: String,
    pub keystore_path: String,
    /// **Executor** miden-client sqlite store path. The signing path; the
    /// solver account state lives here, its keys in `keystore_path`.
    pub executor_store_path: String,
    /// **Keyless ingest** miden-client sqlite store path. The chain-watching
    /// path holds no signing keys and syncs independently of the executor.
    /// Must be a different file from `executor_store_path`.
    pub ingest_store_path: String,
    /// Number of concurrent PostgreSQL read connections. Defaults to 4 if omitted.
    /// Bump if the matcher hydration / admin queries become read-contended.
    #[serde(default = "default_read_pool_size")]
    pub read_pool_size: u32,
}

fn default_read_pool_size() -> u32 {
    4
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    pub pulse_interval_ms: u64,
    pub fetch_interval_ms: u64,
    /// How often the price feed task polls upstream (CoinGecko) for new
    /// prices. Matcher reads from a watch channel on every pulse regardless,
    /// so this only affects how stale the prices can get, not the matcher's
    /// tick rate.
    pub price_interval_ms: u64,
    /// Protocol fee and minimum eligibility edge in ppm. Zero disables fees.
    #[serde(default)]
    pub clearing_fee_ppm: u32,
    /// Maximum age of each provider's own price timestamp when clearing.
    #[serde(default = "default_clearing_source_age_secs")]
    pub clearing_max_source_age_secs: u64,
    /// Maximum difference between the two provider price timestamps.
    #[serde(default = "default_clearing_source_skew_secs")]
    pub clearing_max_source_skew_secs: u64,
    /// TCP port the admin HTTP server binds on `127.0.0.1`. Defaults to 3001.
    #[serde(default = "default_admin_port")]
    pub admin_port: u16,
    /// Ignored since Miden 0.16 (miden-client removed debug mode); kept so
    /// existing `solver.toml` files still parse. A warning is logged if set.
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
    /// Override the price-API base URL. Defaults to the public CoinGecko
    /// endpoint. Point this at a self-hosted or **mock** CoinGecko-compatible
    /// service (e.g. `http://127.0.0.1:8089/api/v3/simple/price`) for devnet /
    /// local runs where the faucet tokens aren't listed and no key is available.
    /// The solver uses its normal `HttpPriceClient` either way — only the URL
    /// changes. Pairs still map tokens → ids via `asset_*_external_symbol`.
    #[serde(default)]
    pub price_api_base_url: Option<String>,

    // ── Public price-query HTTP API (wallets fetch token prices) ──────────────
    // Distinct from `price_api_base_url` above, which is the UPSTREAM source we
    // call; these configure the endpoint we SERVE. It runs on its own OS thread.
    /// Port the price-query API binds. Default 8080.
    #[serde(default = "default_price_query_port")]
    pub price_query_port: u16,
    /// Bind address. Default `"127.0.0.1"` (loopback). Set `"0.0.0.0"` to expose
    /// publicly — front it with a reverse proxy / rate limiter.
    #[serde(default = "default_price_query_bind")]
    pub price_query_bind: String,
    /// Max concurrent in-flight requests; excess is shed with `503`. Default 128.
    #[serde(default = "default_price_query_max_inflight")]
    pub price_query_max_inflight: usize,
    /// Max token ids per batch (`/v1/prices?ids=`); over-limit → `400`. Default 50.
    #[serde(default = "default_price_query_max_batch")]
    pub price_query_max_batch: usize,
    /// Per-request timeout in ms. Default 3000.
    #[serde(default = "default_price_query_timeout_ms")]
    pub price_query_timeout_ms: u64,
    /// Decimal places of the returned price NUMBER: `"full"` or `"0"`..`"18"`
    /// (mirrors CoinGecko's `precision`). One value applied to the price; distinct
    /// from a token's on-chain decimals. Default `"full"`. Overridable per request.
    #[serde(default = "default_price_precision")]
    pub price_precision: String,
    /// Quote currency (CoinGecko `vs_currencies`). Default `"usd"`. Must be a
    /// CoinGecko-supported vs_currency (usd/eur/btc/…), NOT a coin like `"usdt"`.
    #[serde(default = "default_price_vs_currency")]
    pub price_vs_currency: String,
    /// Max age (secs) of the last SUCCESSFUL price refresh before the price-query
    /// API treats prices as stale (→ `503` unless `?allow_stale=true`). Default 30.
    /// Set ≥ 2 × (price_interval_ms / 1000).
    #[serde(default = "default_price_staleness_secs")]
    pub price_staleness_secs: u64,

    // ── Swap time-estimation API (`/v1/swap-eta`) ─────────────────────────────
    /// Estimated proof-generation time (ms) for a settlement tx — a term of the
    /// next-batch ETA. Calibrate to the deployment's prover. Default 2000.
    #[serde(default = "default_swap_proving_estimate_ms")]
    pub swap_proving_estimate_ms: u64,
    /// Estimated chain block time (ms) — a term of the next-batch ETA. Default 6000.
    #[serde(default = "default_swap_block_time_ms")]
    pub swap_block_time_ms: u64,
    /// Slack (bps) before an order is flagged `offMarket` vs the oracle mid.
    /// Default 50 (0.5%).
    #[serde(default = "default_swap_offmarket_tolerance_bps")]
    pub swap_offmarket_tolerance_bps: u64,
    // ── External liquidity routing (RFQ websocket to other DEXes) ─────────────
    /// Enable the external-liquidity router (websocket RFQ server + matcher
    /// external pass). Default `false` (opt-in). Allow-list tokens are sourced
    /// from the `SOLVER_ROUTER_TOKENS` env var (comma-separated), not config.
    #[serde(default)]
    pub router_enabled: bool,
    /// Router websocket bind address. Default `"127.0.0.1"`; `"0.0.0.0"` exposes it.
    #[serde(default = "default_router_bind")]
    pub router_bind: String,
    /// Router websocket port. Default 8090.
    #[serde(default = "default_router_port")]
    pub router_port: u16,
    /// Max concurrent DEX websocket connections. Default 64.
    #[serde(default = "default_router_max_connections")]
    pub router_max_connections: usize,
    /// Max inbound websocket message size (bytes). Default 16384.
    #[serde(default = "default_router_max_msg_bytes")]
    pub router_max_msg_bytes: usize,
    /// How long a DEX's standing quote stays selectable (ms). Default 20000.
    #[serde(default = "default_router_quote_ttl_ms")]
    pub router_quote_ttl_ms: u64,
    /// How long a handed-over note waits for the DEX's on-chain consume before it
    /// reactivates (ms). Set above realistic consume latency. Default 30000.
    #[serde(default = "default_router_inflight_ttl_ms")]
    pub router_inflight_ttl_ms: u64,
}

/// Resolved price precision (decimal places of the price NUMBER): `Full` or a
/// fixed `0..=18`. Mirrors CoinGecko's `precision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PricePrecision {
    Full,
    Fixed(u8),
}

impl PricePrecision {
    /// Parse `"full"` (case-insensitive) or an integer `0..=18`.
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("full") {
            return Some(Self::Full);
        }
        s.parse::<u8>().ok().filter(|n| *n <= 18).map(Self::Fixed)
    }
}

fn default_clearing_source_age_secs() -> u64 {
    60
}

fn default_clearing_source_skew_secs() -> u64 {
    30
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

fn default_price_query_port() -> u16 {
    8080
}
fn default_price_query_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_price_query_max_inflight() -> usize {
    128
}
fn default_price_query_max_batch() -> usize {
    50
}
fn default_price_query_timeout_ms() -> u64 {
    3000
}
fn default_price_precision() -> String {
    "full".to_string()
}
fn default_price_vs_currency() -> String {
    "usd".to_string()
}
fn default_price_staleness_secs() -> u64 {
    30
}
fn default_swap_proving_estimate_ms() -> u64 {
    2000
}
fn default_swap_block_time_ms() -> u64 {
    6000
}
fn default_swap_offmarket_tolerance_bps() -> u64 {
    50
}
fn default_router_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_router_port() -> u16 {
    8090
}
fn default_router_max_connections() -> usize {
    64
}
fn default_router_max_msg_bytes() -> usize {
    16384
}
fn default_router_quote_ttl_ms() -> u64 {
    20_000
}
fn default_router_inflight_ttl_ms() -> u64 {
    30_000
}

impl SolverConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: SolverConfig =
            toml::from_str(&content).context("Failed to parse config file")?;
        config.validate()?;
        config.warn_ignored_settings();
        Ok(config)
    }

    /// Validate fields that have constrained domains (fail fast at boot).
    fn validate(&self) -> std::result::Result<(), ConfigError> {
        if PricePrecision::parse(&self.engine.price_precision).is_none() {
            return Err(ConfigError::InvalidPricePrecision(
                self.engine.price_precision.clone(),
            ));
        }
        if self.engine.price_vs_currency.trim().is_empty() {
            return Err(ConfigError::EmptyPriceCurrency);
        }
        if self.engine.clearing_fee_ppm >= crate::clearing::PPM_DENOMINATOR {
            return Err(ConfigError::InvalidClearingFee {
                fee: self.engine.clearing_fee_ppm,
                maximum: crate::clearing::PPM_DENOMINATOR,
            });
        }
        Ok(())
    }

    /// The Miden client no longer exposes debug mode.
    fn warn_ignored_settings(&self) {
        if self.engine.debug_mode {
            tracing::warn!("engine.debug_mode is ignored: miden-client 0.16 removed debug mode");
        }
    }

    pub fn save(&self, path: &str) -> Result<()> {
        let content = toml::to_string_pretty(self).context("Failed to serialize config")?;
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write config file: {}", path))
    }
}

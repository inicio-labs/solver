//! Configuration types for the solver binary, sourced from `solver.toml`.
//!
//! Living in the library so both `solver::start` and `main.rs` can read the
//! same struct without a reverse dependency from library → binary.

use std::time::Duration;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::price::{AssetCode, ClearingMarket, FeedConfig, MarketPlan, RetryPolicy, Symbol};
use crate::types::TokenId;

/// Binance pings every stream connection this often.
const BINANCE_PING_INTERVAL_MS: u64 = 20_000;
/// Binance's limit on stream connection attempts per 5 minutes per client IP;
/// exceeding it gets the IP rate limited, then banned.
const BINANCE_CONNECTION_ATTEMPT_LIMIT: usize = 300;

#[derive(Debug, Error)]
enum ConfigError {
    #[error("engine.price_precision must be \"full\" or an integer 0..=18, got {0:?}")]
    InvalidPricePrecision(String),
    #[error("binance.{0} must be set to a positive value")]
    ZeroBinanceSetting(&'static str),
    #[error("binance.{name} {url:?} must be a {scheme} URL")]
    InvalidEndpoint {
        name: &'static str,
        url: String,
        scheme: &'static str,
    },
    #[error("binance.idle_timeout_ms must exceed Binance's 20 s ping interval")]
    IdleTimeoutTooShort,
    #[error("binance.max_spread_bps must be at most 10000, got {0}")]
    SpreadTooWide(u32),
    #[error("binance.retry_min_ms must not exceed binance.retry_max_ms")]
    RetryRange,
    #[error("binance.connection_lifetime_secs must be below Binance's 24-hour limit")]
    LifetimeTooLong,
    #[error("binance.max_connection_attempts must be below Binance's per-IP limit of {limit}, got {value}")]
    TooManyConnectionAttempts { value: usize, limit: usize },
    #[error("engine.clearing_fee_ppm must be below {maximum}, got {fee}")]
    InvalidClearingFee { fee: u32, maximum: u32 },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolverConfig {
    pub rpc: RpcConfig,
    pub solver: SolverAccountConfig,
    pub pairs: Vec<AssetPairConfig>,
    pub engine: EngineConfig,
    pub binance: BinanceConfig,
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
    /// Binance asset code of the `asset_x` token (e.g. `"ETH"`). The price API
    /// values the token by the `<ASSET><binance.valuation_quote_asset>` market;
    /// without a code the token has no price.
    #[serde(default)]
    pub asset_x_binance_asset: Option<AssetCode>,
    pub asset_y_faucet_id: String,
    /// See `asset_x_binance_asset`.
    #[serde(default)]
    pub asset_y_binance_asset: Option<AssetCode>,
    /// Approved Binance Spot symbol of the direct market between the two
    /// assets (e.g. `"ETHUSDT"`); both asset codes are then required. Without
    /// it the pair does not clear internally. Which asset is Binance's base
    /// comes from `exchangeInfo`, so either pair orientation works.
    #[serde(default)]
    pub binance_symbol: Option<Symbol>,
}

impl AssetPairConfig {
    /// The pair's faucets, `(asset_x, asset_y)`.
    pub fn faucets(&self) -> Result<(TokenId, TokenId)> {
        let parse = |hex: &str, side: &str| {
            AccountId::from_hex(hex)
                .with_context(|| format!("invalid {side}_faucet_id for pair {}", self.name))
        };
        Ok((
            parse(&self.asset_x_faucet_id, "asset_x")?,
            parse(&self.asset_y_faucet_id, "asset_y")?,
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    pub pulse_interval_ms: u64,
    pub fetch_interval_ms: u64,
    /// Protocol fee and minimum eligibility edge in ppm. Zero disables fees.
    #[serde(default)]
    pub clearing_fee_ppm: u32,
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
    /// While the executor is in verification mode (it cannot settle: no fee
    /// headroom, node RPC or PostgreSQL unavailable), how often it re-checks
    /// whether it can settle again. It retries without limit; the matcher
    /// keeps orders live meanwhile. Defaults to 5000.
    #[serde(default = "default_verify_interval_ms")]
    pub verify_interval_ms: u64,

    // ── Public price-query HTTP API (wallets fetch token prices) ──────────────
    // The endpoint we SERVE, from the Binance snapshot. It runs on its own OS
    // thread.
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
    /// Decimal places of the returned price NUMBER: `"full"` or `"0"`..`"18"`.
    /// One value applied to the price; distinct from a token's on-chain
    /// decimals. Default `"full"` (exact, up to 18 places). Overridable per request.
    #[serde(default = "default_price_precision")]
    pub price_precision: String,

    // ── Swap time-estimation API (`/v1/swap-eta`) ─────────────────────────────
    /// Estimated proof-generation time (ms) for a settlement tx — a term of the
    /// next-batch ETA. Calibrate to the deployment's prover. Default 2000.
    #[serde(default = "default_swap_proving_estimate_ms")]
    pub swap_proving_estimate_ms: u64,
    /// Estimated chain block time (ms) — a term of the next-batch ETA. Default 6000.
    #[serde(default = "default_swap_block_time_ms")]
    pub swap_block_time_ms: u64,
    /// Slack (bps) before an order is flagged `offMarket` vs the Binance mid.
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
/// fixed `0..=18`.
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

fn default_admin_port() -> u16 {
    3001
}

fn default_obs_port() -> u16 {
    9090
}

fn default_readiness_freshness_secs() -> u64 {
    60
}

fn default_verify_interval_ms() -> u64 {
    5_000
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

/// Binance Spot public market data: endpoints, quote validity, and the feed's
/// connection and retry budget (ADR 0004). Every field but `quote_ttl_ms` and
/// `max_spread_bps` has a default; those two must be chosen per deployment.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct BinanceConfig {
    /// Stream base URLs of reader A and reader B. By default reader A uses the
    /// market-data-only endpoint and reader B the main one: separate front
    /// ends, so one endpoint failing does not take out both readers.
    pub stream_endpoints: [String; 2],
    /// REST base URL for `exchangeInfo` checks at startup.
    pub rest_endpoint: String,
    /// A quote is usable while `now - received_at < quote_ttl_ms`, measured
    /// from local receipt.
    pub quote_ttl_ms: u64,
    /// Widest accepted spread, `10_000 × (ask - bid) / mid`, inclusive. A
    /// wider quote makes its market unusable until the next valid one.
    pub max_spread_bps: u32,
    /// Asset the price API values tokens in (`<ASSET><QUOTE>` markets).
    pub valuation_quote_asset: AssetCode,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    /// Reconnect a stream that delivers no frame, not even a ping, this long.
    pub idle_timeout_ms: u64,
    /// Longest planned connection, below Binance's 24-hour limit; each lasts
    /// a random 50–100% of it, so the two readers renew apart.
    pub connection_lifetime_secs: u64,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
    /// Connection attempts per 5 minutes, shared by both readers. Must stay
    /// below Binance's 300 per IP; staying far below leaves room for other
    /// processes on the same IP.
    pub max_connection_attempts: usize,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            stream_endpoints: [
                "wss://data-stream.binance.vision:443".to_string(),
                "wss://stream.binance.com:443".to_string(),
            ],
            rest_endpoint: "https://data-api.binance.vision".to_string(),
            quote_ttl_ms: 0,
            max_spread_bps: 0,
            valuation_quote_asset: AssetCode::parse("USDT").expect("valid asset code"),
            connect_timeout_ms: 10_000,
            request_timeout_ms: 10_000,
            idle_timeout_ms: 3 * BINANCE_PING_INTERVAL_MS,
            connection_lifetime_secs: 23 * 60 * 60,
            retry_min_ms: 500,
            retry_max_ms: 60_000,
            max_connection_attempts: 30,
        }
    }
}

/// `url` parses and uses one of `schemes`.
fn check_endpoint(
    name: &'static str,
    url: &str,
    schemes: [&'static str; 2],
) -> std::result::Result<(), ConfigError> {
    let valid = reqwest::Url::parse(url)
        .is_ok_and(|parsed| schemes.contains(&parsed.scheme()) && parsed.has_host());
    if valid {
        return Ok(());
    }
    Err(ConfigError::InvalidEndpoint {
        name,
        url: url.to_string(),
        scheme: if schemes[0] == "ws" {
            "ws/wss"
        } else {
            "http/https"
        },
    })
}

impl BinanceConfig {
    fn validate(&self) -> std::result::Result<(), ConfigError> {
        for (name, value) in [
            ("quote_ttl_ms", self.quote_ttl_ms),
            ("max_spread_bps", u64::from(self.max_spread_bps)),
            ("connect_timeout_ms", self.connect_timeout_ms),
            ("request_timeout_ms", self.request_timeout_ms),
            ("connection_lifetime_secs", self.connection_lifetime_secs),
            ("retry_min_ms", self.retry_min_ms),
            (
                "max_connection_attempts",
                self.max_connection_attempts as u64,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroBinanceSetting(name));
            }
        }
        for endpoint in &self.stream_endpoints {
            check_endpoint("stream_endpoints", endpoint, ["ws", "wss"])?;
        }
        check_endpoint("rest_endpoint", &self.rest_endpoint, ["http", "https"])?;
        if self.idle_timeout_ms <= BINANCE_PING_INTERVAL_MS {
            return Err(ConfigError::IdleTimeoutTooShort);
        }
        if self.max_spread_bps > 10_000 {
            return Err(ConfigError::SpreadTooWide(self.max_spread_bps));
        }
        if self.retry_min_ms > self.retry_max_ms {
            return Err(ConfigError::RetryRange);
        }
        if self.connection_lifetime_secs >= 24 * 60 * 60 {
            return Err(ConfigError::LifetimeTooLong);
        }
        if self.max_connection_attempts >= BINANCE_CONNECTION_ATTEMPT_LIMIT {
            return Err(ConfigError::TooManyConnectionAttempts {
                value: self.max_connection_attempts,
                limit: BINANCE_CONNECTION_ATTEMPT_LIMIT,
            });
        }
        Ok(())
    }

    pub fn feed_config(&self) -> FeedConfig {
        FeedConfig {
            stream_endpoints: self.stream_endpoints.clone(),
            rest_endpoint: self.rest_endpoint.clone(),
            max_spread_bps: self.max_spread_bps,
            quote_ttl: Duration::from_millis(self.quote_ttl_ms),
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            idle_timeout: Duration::from_millis(self.idle_timeout_ms),
            connection_lifetime: Duration::from_secs(self.connection_lifetime_secs),
            retry: RetryPolicy {
                min_delay: Duration::from_millis(self.retry_min_ms),
                max_delay: Duration::from_millis(self.retry_max_ms),
            },
            max_connection_attempts: self.max_connection_attempts,
        }
    }
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
        if self.engine.clearing_fee_ppm >= crate::clearing::PPM_DENOMINATOR {
            return Err(ConfigError::InvalidClearingFee {
                fee: self.engine.clearing_fee_ppm,
                maximum: crate::clearing::PPM_DENOMINATOR,
            });
        }
        self.binance.validate()
    }

    /// The Binance markets the configuration asks for: each faucet's asset
    /// code, each pair with an approved symbol, and the valuation quote.
    pub fn market_plan(&self) -> Result<MarketPlan> {
        let mut assets = Vec::new();
        let mut clearing = Vec::new();
        for pair in &self.pairs {
            let (x, y) = pair.faucets()?;
            let codes = [
                (x, &pair.asset_x_binance_asset),
                (y, &pair.asset_y_binance_asset),
            ];
            assets.extend(
                codes
                    .into_iter()
                    .filter_map(|(token, code)| Some((token, code.clone()?))),
            );
            match &pair.binance_symbol {
                Some(symbol) => clearing.push(ClearingMarket {
                    name: pair.name.clone(),
                    base: x,
                    quote: y,
                    symbol: symbol.clone(),
                }),
                None => tracing::warn!(
                    pair = %pair.name,
                    "pair has no binance_symbol; it will not clear internally"
                ),
            }
        }
        let quote = self.binance.valuation_quote_asset.clone();
        Ok(MarketPlan::new(assets, clearing, quote)?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::price::MarketError;

    const EXAMPLE: &str = include_str!("../../../solver.toml.example");

    fn example() -> SolverConfig {
        let mut config: SolverConfig = toml::from_str(EXAMPLE).expect("solver.toml.example parses");
        // Replace the placeholders with valid faucet ids.
        config.pairs[0].asset_x_faucet_id = "0x9f0c6ec13c4ed2b1076a2990a9fc29".into();
        config.pairs[0].asset_y_faucet_id = "0x3ae73d7f166f723132e3acbba75e75".into();
        config
    }

    #[test]
    fn example_config_is_valid() {
        let config = example();
        config.validate().unwrap();
        let feed = config.binance.feed_config();
        assert_eq!(feed.quote_ttl, Duration::from_secs(2));
        assert_eq!(feed.max_spread_bps, 50);
        let defaults = BinanceConfig::default();
        assert_eq!(feed.stream_endpoints, defaults.stream_endpoints);
        assert_ne!(
            defaults.stream_endpoints[0], defaults.stream_endpoints[1],
            "the readers default to different endpoints"
        );
        assert_eq!(
            feed.connection_lifetime,
            Duration::from_secs(defaults.connection_lifetime_secs)
        );
    }

    #[test]
    fn example_pair_maps_to_its_binance_market() {
        let plan = example().market_plan().unwrap();
        let symbols: Vec<_> = plan.symbols().into_iter().map(|s| s.to_string()).collect();
        // The clearing market also values ETH; USDT is the valuation quote.
        assert_eq!(symbols, ["ETHUSDT"]);
    }

    #[test]
    fn binance_codes_are_checked_at_load() {
        let lowercase = EXAMPLE.replace(
            r#"binance_symbol = "ETHUSDT""#,
            r#"binance_symbol = "ethusdt""#,
        );
        let config: SolverConfig = toml::from_str(&lowercase).unwrap();
        assert_eq!(
            config.pairs[0].binance_symbol,
            Some(Symbol::parse("ETHUSDT").unwrap())
        );
        let invalid = EXAMPLE.replace(
            r#"asset_y_binance_asset = "ETH""#,
            r#"asset_y_binance_asset = "E-TH""#,
        );
        let error = toml::from_str::<SolverConfig>(&invalid)
            .unwrap_err()
            .to_string();
        assert!(error.contains("asset_y_binance_asset"), "{error}");
    }

    #[test]
    fn pair_mappings_are_checked() {
        let mut config = example();
        config.pairs[0].asset_x_binance_asset = None;
        let error = config.market_plan().unwrap_err();
        assert!(
            matches!(error.downcast_ref(), Some(MarketError::MissingAsset { .. })),
            "{error}"
        );
        let mut config = example();
        config.pairs[0].asset_x_faucet_id = "0xnot-hex".into();
        let error = config.market_plan().unwrap_err().to_string();
        assert!(error.contains("asset_x_faucet_id"), "{error}");
    }

    #[test]
    fn binance_settings_are_checked() {
        type Change = fn(&mut BinanceConfig);
        let check = |change: Change| {
            let mut config = example();
            change(&mut config.binance);
            config.validate().unwrap_err().to_string()
        };
        let zero: [(&str, Change); 7] = [
            ("quote_ttl_ms", |binance| binance.quote_ttl_ms = 0),
            ("max_spread_bps", |binance| binance.max_spread_bps = 0),
            ("connect_timeout_ms", |binance| {
                binance.connect_timeout_ms = 0
            }),
            ("request_timeout_ms", |binance| {
                binance.request_timeout_ms = 0
            }),
            ("connection_lifetime_secs", |binance| {
                binance.connection_lifetime_secs = 0
            }),
            ("retry_min_ms", |binance| binance.retry_min_ms = 0),
            ("max_connection_attempts", |binance| {
                binance.max_connection_attempts = 0
            }),
        ];
        for (name, change) in zero {
            assert!(check(change).contains(name), "{name}");
        }
        assert!(
            check(|binance| binance.stream_endpoints[1] = "https://example.com".into())
                .contains("stream_endpoints")
        );
        assert!(
            check(|binance| binance.rest_endpoint = "wss://example.com".into())
                .contains("rest_endpoint")
        );
        assert!(check(|binance| binance.rest_endpoint = String::new()).contains("rest_endpoint"));
        assert!(check(|binance| binance.idle_timeout_ms = 20_000).contains("idle_timeout_ms"));
        assert!(check(|binance| binance.max_spread_bps = 10_001).contains("at most 10000"));
        assert!(
            check(|binance| binance.retry_min_ms = binance.retry_max_ms + 1)
                .contains("retry_min_ms")
        );
        assert!(
            check(|binance| binance.connection_lifetime_secs = 24 * 60 * 60).contains("24-hour")
        );
        assert!(
            check(|binance| binance.max_connection_attempts = 300).contains("per-IP limit of 300")
        );
        let mut config = example();
        config.binance.max_connection_attempts = 299;
        config.validate().unwrap();
    }

    #[test]
    fn the_binance_section_is_required() {
        let mut table: toml::Table = toml::from_str(EXAMPLE).unwrap();
        table.remove("binance");
        let error = toml::from_str::<SolverConfig>(&table.to_string()).unwrap_err();
        assert!(error.to_string().contains("binance"), "{error}");
    }
}

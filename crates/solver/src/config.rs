//! Configuration types for the solver binary, sourced from `solver.toml`.
//!
//! Living in the library so both `solver::start` and `main.rs` can read the
//! same struct without a reverse dependency from library → binary.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::price::BinanceConfig;
use crate::price::{
    parse_positive_decimal, AssetCode, ClearingMarket, MarketError, MarketPlan, PricePrecision,
    Symbol,
};
use crate::types::TokenId;

/// Binance's limit on stream connection attempts per 5 minutes per client IP;
/// exceeding it gets the IP rate limited, then banned.
const BINANCE_CONNECTION_ATTEMPT_LIMIT: usize = 300;
/// The TTL is what pauses clearing when both readers stall; a typo that makes
/// it minutes long would clear at a frozen price.
const MAX_QUOTE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("engine.price_precision must be \"full\" or an integer 0..=18, got {0:?}")]
    InvalidPricePrecision(String),
    #[error("binance.{0} must be set to a positive value")]
    ZeroBinanceSetting(&'static str),
    #[error("binance.{name} {url:?} must be a {}:// or {}:// URL with a host and no path or query", .schemes[0], .schemes[1])]
    InvalidEndpoint {
        name: &'static str,
        url: String,
        schemes: [&'static str; 2],
    },
    #[error(
        "binance.{name} {url:?} is plaintext; only loopback hosts may use it, since the listing \
         decides each pair's orientation and the stream its price"
    )]
    PlaintextEndpoint { name: &'static str, url: String },
    #[error("binance.quote_ttl_ms must be at most {maximum}, got {value}")]
    TtlTooLong { value: u128, maximum: u128 },
    #[error("binance.min_notional {0:?} must be a positive decimal amount of the quote asset")]
    InvalidMinNotional(String),
    #[error("binance.max_connection_attempts must be between 1 and Binance's per-IP limit of {limit} (exclusive), got {value}")]
    TooManyConnectionAttempts { value: usize, limit: usize },
    #[error("engine.clearing_fee_ppm must be below {maximum}, got {fee}")]
    InvalidClearingFee { fee: u32, maximum: u32 },
    #[error("engine.{0} must be at least 1")]
    ZeroMakerIntakeLimit(&'static str),
    #[error("pair {pair}: invalid {side}_faucet_id: {reason}")]
    InvalidFaucet {
        pair: String,
        side: &'static str,
        reason: String,
    },
    #[error(
        "pair {pair}: binance_symbol {symbol} is neither {forward} nor {reverse}, the two asset \
         codes joined in either order"
    )]
    SymbolMismatch {
        pair: String,
        symbol: Symbol,
        forward: Symbol,
        reverse: Symbol,
    },
    #[error(transparent)]
    Market(MarketError),
}

impl From<MarketError> for ConfigError {
    fn from(error: MarketError) -> Self {
        Self::Market(error)
    }
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

/// A misspelled key here is an error, not a silently ignored one.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
    fn faucets(&self) -> std::result::Result<(TokenId, TokenId), ConfigError> {
        let parse = |hex: &str, side: &'static str| {
            AccountId::from_hex(hex).map_err(|error| ConfigError::InvalidFaucet {
                pair: self.name.clone(),
                side,
                reason: error.to_string(),
            })
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
    // ── Market-maker gRPC gateway (ADR 0003) ──────────────────────────────────
    /// Enable the maker gateway. Default `false`. It serves plaintext HTTP/2:
    /// terminate TLS at a proxy or load balancer in front of it.
    #[serde(default)]
    pub maker_gateway_enabled: bool,
    /// Gateway bind address. Default `"127.0.0.1"` (behind the TLS proxy).
    #[serde(default = "default_maker_gateway_bind")]
    pub maker_gateway_bind: String,
    /// Gateway port. Default 8095.
    #[serde(default = "default_maker_gateway_port")]
    pub maker_gateway_port: u16,
    /// Most submits written in one intake transaction; every waiting cancel
    /// is written too. Default 500.
    #[serde(default = "default_maker_intake_round_submits")]
    pub maker_intake_round_submits: usize,
    /// Submits waiting for the intake before new ones get UNAVAILABLE.
    /// Default 4096.
    #[serde(default = "default_maker_intake_submit_queue")]
    pub maker_intake_submit_queue: usize,
    /// Cancels waiting for the intake, in their own queue. Default 1024.
    #[serde(default = "default_maker_intake_cancel_queue")]
    pub maker_intake_cancel_queue: usize,
    /// Event-stream buffer per subscriber. Replay waits for room; a
    /// subscriber that takes nothing for a whole heartbeat interval is
    /// disconnected and resumes from its cursor. Default 256.
    #[serde(default = "default_maker_stream_buffer")]
    pub maker_stream_buffer: usize,
    /// Event-stream heartbeat (ms), which also re-checks the API key, so a
    /// revoked key ends open streams within it. Default 10000.
    #[serde(default = "default_maker_stream_heartbeat_ms")]
    pub maker_stream_heartbeat_ms: u64,
    /// How often the maker-note watcher checks for new blocks (ms). Default
    /// 1000.
    #[serde(default = "default_maker_watch_interval_ms")]
    pub maker_watch_interval_ms: u64,
    /// Stop selecting MM orders this many milliseconds before their expiry.
    /// Already selected settlements continue. Default 30000; zero is allowed.
    #[serde(default = "default_maker_settlement_buffer_ms")]
    pub maker_settlement_buffer_ms: u64,
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
fn default_maker_gateway_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_maker_gateway_port() -> u16 {
    8095
}
fn default_maker_intake_round_submits() -> usize {
    500
}
fn default_maker_intake_submit_queue() -> usize {
    4096
}
fn default_maker_intake_cancel_queue() -> usize {
    1024
}
fn default_maker_stream_buffer() -> usize {
    256
}
fn default_maker_stream_heartbeat_ms() -> u64 {
    10_000
}
fn default_maker_settlement_buffer_ms() -> u64 {
    30_000
}
fn default_maker_watch_interval_ms() -> u64 {
    1_000
}

/// `url` parses, uses one of `schemes` (plaintext first, TLS second), names a
/// host and nothing more, and is plaintext only for a loopback host.
fn check_endpoint(
    name: &'static str,
    url: &str,
    schemes: [&'static str; 2],
) -> std::result::Result<(), ConfigError> {
    let invalid = || ConfigError::InvalidEndpoint {
        name,
        url: url.to_string(),
        schemes,
    };
    let parsed = reqwest::Url::parse(url).map_err(|_| invalid())?;
    let bare = matches!(parsed.path(), "" | "/")
        && parsed.query().is_none()
        && parsed.fragment().is_none();
    let Some(host) = parsed
        .host_str()
        .filter(|_| bare && schemes.contains(&parsed.scheme()))
    else {
        return Err(invalid());
    };
    let loopback = host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if parsed.scheme() == schemes[0] && !loopback {
        return Err(ConfigError::PlaintextEndpoint {
            name,
            url: url.to_string(),
        });
    }
    Ok(())
}

/// The `[binance]` checks that matter at load: the two required settings,
/// the TTL cap, the per-IP attempt budget, `min_notional`, and the endpoints.
fn check_binance(binance: &BinanceConfig) -> std::result::Result<(), ConfigError> {
    if binance.quote_ttl.is_zero() {
        return Err(ConfigError::ZeroBinanceSetting("quote_ttl_ms"));
    }
    if binance.max_spread_bps == 0 {
        return Err(ConfigError::ZeroBinanceSetting("max_spread_bps"));
    }
    if binance.quote_ttl > MAX_QUOTE_TTL {
        return Err(ConfigError::TtlTooLong {
            value: binance.quote_ttl.as_millis(),
            maximum: MAX_QUOTE_TTL.as_millis(),
        });
    }
    if !(1..BINANCE_CONNECTION_ATTEMPT_LIMIT).contains(&binance.max_connection_attempts) {
        return Err(ConfigError::TooManyConnectionAttempts {
            value: binance.max_connection_attempts,
            limit: BINANCE_CONNECTION_ATTEMPT_LIMIT,
        });
    }
    if let Some(raw) = &binance.min_notional {
        parse_positive_decimal(raw).ok_or_else(|| ConfigError::InvalidMinNotional(raw.clone()))?;
    }
    for endpoint in &binance.stream_endpoints {
        check_endpoint("stream_endpoints", endpoint, ["ws", "wss"])?;
    }
    check_endpoint("rest_endpoint", &binance.rest_endpoint, ["http", "https"])
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

    /// Validate fields that have constrained domains (fail fast at boot),
    /// including the Binance market mapping of the pairs.
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
        for (name, value) in [
            (
                "maker_intake_round_submits",
                self.engine.maker_intake_round_submits,
            ),
            (
                "maker_intake_submit_queue",
                self.engine.maker_intake_submit_queue,
            ),
            (
                "maker_intake_cancel_queue",
                self.engine.maker_intake_cancel_queue,
            ),
            ("maker_stream_buffer", self.engine.maker_stream_buffer),
            (
                "maker_stream_heartbeat_ms",
                usize::try_from(self.engine.maker_stream_heartbeat_ms).unwrap_or(usize::MAX),
            ),
            (
                "maker_watch_interval_ms",
                usize::try_from(self.engine.maker_watch_interval_ms).unwrap_or(usize::MAX),
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroMakerIntakeLimit(name));
            }
        }
        check_binance(&self.binance)?;
        self.market_plan()?;
        Ok(())
    }

    /// Every pair's faucets, `(asset_x, asset_y)`, in configuration order.
    pub(crate) fn faucet_pairs(&self) -> std::result::Result<Vec<(TokenId, TokenId)>, ConfigError> {
        self.pairs.iter().map(AssetPairConfig::faucets).collect()
    }

    /// The Binance markets the configuration asks for: each faucet's asset
    /// code, each pair with an approved symbol, and the valuation quote. A
    /// clearing symbol must be the pair's two asset codes joined, in either
    /// order; which order Binance uses comes from `exchangeInfo` at runtime.
    pub(crate) fn market_plan(&self) -> std::result::Result<MarketPlan, ConfigError> {
        let mut assets = Vec::new();
        let mut clearing = Vec::new();
        for (pair, (x, y)) in self.pairs.iter().zip(self.faucet_pairs()?) {
            let codes = [
                (x, &pair.asset_x_binance_asset),
                (y, &pair.asset_y_binance_asset),
            ];
            assets.extend(
                codes
                    .into_iter()
                    .filter_map(|(token, code)| Some((token, code.clone()?))),
            );
            let Some(symbol) = &pair.binance_symbol else {
                tracing::warn!(
                    pair = %pair.name,
                    "pair has no binance_symbol; it will not clear internally"
                );
                continue;
            };
            if let (Some(code_x), Some(code_y)) =
                (&pair.asset_x_binance_asset, &pair.asset_y_binance_asset)
            {
                let forward = Symbol::of_assets(code_x, code_y);
                let reverse = Symbol::of_assets(code_y, code_x);
                if *symbol != forward && *symbol != reverse {
                    return Err(ConfigError::SymbolMismatch {
                        pair: pair.name.clone(),
                        symbol: symbol.clone(),
                        forward,
                        reverse,
                    });
                }
            }
            clearing.push(ClearingMarket {
                name: pair.name.clone(),
                base: x,
                quote: y,
                symbol: symbol.clone(),
            });
        }
        let quote = self.binance.valuation_quote_asset.clone();
        Ok(MarketPlan::new(assets, clearing, quote)?)
    }

    /// Settings this binary accepts but does not use.
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

    const EXAMPLE: &str = include_str!("../../../solver.toml.example");

    /// `text` parsed, with the example's faucet placeholders replaced by
    /// valid ids.
    fn parsed(text: &str) -> SolverConfig {
        let mut config: SolverConfig = toml::from_str(text).expect("config parses");
        config.pairs[0].asset_x_faucet_id = "0x9f0c6ec13c4ed2b1076a2990a9fc29".into();
        config.pairs[0].asset_y_faucet_id = "0x3ae73d7f166f723132e3acbba75e75".into();
        config
    }

    fn example() -> SolverConfig {
        parsed(EXAMPLE)
    }

    /// The validation error of `example()` after `change`.
    fn check(change: impl FnOnce(&mut SolverConfig)) -> String {
        let mut config = example();
        change(&mut config);
        config.validate().unwrap_err().to_string()
    }

    fn accepted(change: impl FnOnce(&mut SolverConfig)) {
        let mut config = example();
        change(&mut config);
        config.validate().unwrap();
    }

    #[test]
    fn example_config_is_valid() {
        let config = example();
        config.validate().unwrap();
        let binance = &config.binance;
        assert_eq!(binance.quote_ttl, Duration::from_secs(2));
        assert_eq!(binance.max_spread_bps, 50);
        assert_eq!(binance.min_notional, None);
        assert_eq!(binance.validation_timeout, Duration::from_secs(60));
        let defaults = BinanceConfig::default();
        assert_eq!(binance.stream_endpoints, defaults.stream_endpoints);
        assert_ne!(
            defaults.stream_endpoints[0], defaults.stream_endpoints[1],
            "the readers default to different endpoints"
        );
        assert_eq!(binance.connection_lifetime, defaults.connection_lifetime);
    }

    /// Durations are read in the unit their key names, and survive a save.
    #[test]
    fn binance_durations_use_their_key_units() {
        let text = EXAMPLE.replace(
            "max_spread_bps = 50",
            "max_spread_bps = 50\nretry_min_ms = 250\nconnection_lifetime_secs = 7200",
        );
        let config = parsed(&text);
        assert_eq!(config.binance.retry_min, Duration::from_millis(250));
        assert_eq!(
            config.binance.connection_lifetime,
            Duration::from_secs(7200)
        );
        let saved = toml::to_string(&config).unwrap();
        assert!(saved.contains("retry_min_ms = 250"), "{saved}");
        assert!(saved.contains("connection_lifetime_secs = 7200"), "{saved}");
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

    /// A misspelled key must not load as if the setting had been applied.
    #[test]
    fn misspelled_keys_are_rejected() {
        let pair_key = EXAMPLE.replace(
            r#"binance_symbol = "ETHUSDT""#,
            r#"binance_symbl = "ETHUSDT""#,
        );
        let error = toml::from_str::<SolverConfig>(&pair_key)
            .unwrap_err()
            .to_string();
        assert!(error.contains("binance_symbl"), "{error}");
        let binance_key = EXAMPLE.replace(
            "max_spread_bps = 50",
            "max_spread_bps = 50\nstream_endpoint = \"wss://x\"",
        );
        let error = toml::from_str::<SolverConfig>(&binance_key)
            .unwrap_err()
            .to_string();
        assert!(error.contains("stream_endpoint"), "{error}");
    }

    #[test]
    fn pair_mappings_are_checked_at_load() {
        let error = check(|config| config.pairs[0].asset_x_binance_asset = None);
        assert!(error.contains("has no Binance asset code"), "{error}");
        let error = check(|config| config.pairs[0].asset_x_faucet_id = "0xnot-hex".into());
        assert!(error.contains("asset_x_faucet_id"), "{error}");
        let error = check(|config| {
            config.pairs[0].binance_symbol = Some(Symbol::parse("ETHUSTD").unwrap())
        });
        assert!(
            error.contains("ETHUSTD") && error.contains("USDTETH"),
            "{error}"
        );
        // Either order of the two codes is a valid symbol.
        accepted(|config| config.pairs[0].binance_symbol = Some(Symbol::parse("USDTETH").unwrap()));
    }

    /// Only the checks that matter: the two required settings, the TTL cap,
    /// the per-IP attempt budget, `min_notional`, and the endpoints.
    #[test]
    fn binance_settings_are_checked() {
        type Change = fn(&mut BinanceConfig);
        let check = |change: Change| check(|config| change(&mut config.binance));
        assert!(check(|b| b.quote_ttl = Duration::ZERO).contains("quote_ttl_ms"));
        assert!(check(|b| b.max_spread_bps = 0).contains("max_spread_bps"));
        assert!(
            check(|b| b.quote_ttl = MAX_QUOTE_TTL + Duration::from_millis(1))
                .contains("quote_ttl_ms")
        );
        accepted(|c| c.binance.quote_ttl = MAX_QUOTE_TTL);
        assert!(check(|b| b.max_connection_attempts = 0).contains("max_connection_attempts"));
        assert!(check(|b| b.max_connection_attempts = 300).contains("per-IP limit of 300"));
        accepted(|c| c.binance.max_connection_attempts = 299);
        assert!(check(|b| b.min_notional = Some("0".into())).contains("min_notional"));
        assert!(check(|b| b.min_notional = Some("5e3".into())).contains("min_notional"));
        accepted(|c| c.binance.min_notional = Some("0.5".into()));
        assert!(
            check(|b| b.stream_endpoints[1] = "https://example.com".into())
                .contains("stream_endpoints")
        );
        assert!(check(|b| b.rest_endpoint = "wss://example.com".into()).contains("rest_endpoint"));
        assert!(check(|b| b.rest_endpoint = String::new()).contains("rest_endpoint"));
        // A path or query would be mangled when the feed appends its own.
        assert!(
            check(|b| b.stream_endpoints[0] = "wss://stream.binance.com:9443/ws".into())
                .contains("no path or query")
        );
        assert!(
            check(|b| b.rest_endpoint = "https://api.binance.com/?x=1".into())
                .contains("no path or query")
        );
        // Plaintext only on loopback: a listing over HTTP could invert a pair.
        assert!(
            check(|b| b.rest_endpoint = "http://data-api.binance.vision".into())
                .contains("plaintext")
        );
        for endpoint in [
            "ws://127.0.0.1:8089",
            "ws://localhost:8089",
            "ws://[::1]:8089",
        ] {
            let mut config = example();
            config.binance.stream_endpoints = [endpoint.into(), endpoint.into()];
            config.validate().unwrap();
        }
        // The timing settings are not bounded: any value loads.
        accepted(|c| c.binance.retry_min = Duration::from_millis(1));
        accepted(|c| c.binance.connection_lifetime = Duration::from_secs(60));
    }

    #[test]
    fn the_binance_section_is_required() {
        let mut table: toml::Table = toml::from_str(EXAMPLE).unwrap();
        table.remove("binance");
        let error = toml::from_str::<SolverConfig>(&table.to_string()).unwrap_err();
        assert!(error.to_string().contains("binance"), "{error}");
    }

    #[test]
    fn maker_settlement_buffer_defaults_and_can_be_disabled() {
        assert_eq!(example().engine.maker_settlement_buffer_ms, 30_000);
        let explicit = EXAMPLE.replace(
            "# maker_settlement_buffer_ms = 30000",
            "maker_settlement_buffer_ms = 0",
        );
        let config = parsed(&explicit);
        assert_eq!(config.engine.maker_settlement_buffer_ms, 0);
        config.validate().unwrap();
        assert!(check(|c| c.engine.maker_stream_buffer = 0).contains("maker_stream_buffer"));
    }
}

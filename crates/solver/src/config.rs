//! Configuration types for the solver binary, sourced from `solver.toml`.
//!
//! Living in the library so both `solver::start` and `main.rs` can read the
//! same struct without a reverse dependency from library → binary.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::price::{
    parse_positive_decimal, AssetCode, ClearingMarket, FeedConfig, MarketError, MarketPlan,
    QuoteLimits, RetryPolicy, Symbol,
};
use crate::types::TokenId;

/// Binance pings every stream connection this often.
const BINANCE_PING_INTERVAL_MS: u64 = 20_000;
/// Binance's limit on stream connection attempts per 5 minutes per client IP;
/// exceeding it gets the IP rate limited, then banned.
const BINANCE_CONNECTION_ATTEMPT_LIMIT: usize = 300;
/// Binance closes a stream connection after this long.
const BINANCE_CONNECTION_LIFETIME_SECS: u64 = 24 * 60 * 60;
/// The TTL is what pauses clearing when both readers stall; a typo that makes
/// it minutes long would clear at a frozen price.
const MAX_QUOTE_TTL_MS: u64 = 60_000;
/// Longest reconnect backoff: an outage must not keep a reader away for hours.
const MAX_RETRY_MS: u64 = 10 * 60 * 1000;
/// Shortest planned connection: every renewal takes a slot of the shared
/// connection budget.
const MIN_CONNECTION_LIFETIME_SECS: u64 = 60 * 60;

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
    #[error("binance.quote_ttl_ms must be at most {MAX_QUOTE_TTL_MS}, got {0}")]
    TtlTooLong(u64),
    #[error("binance.max_spread_bps must be at most 10000, got {0}")]
    SpreadTooWide(u32),
    #[error("binance.min_notional {0:?} must be a positive decimal amount of the quote asset")]
    InvalidMinNotional(String),
    #[error(
        "binance.idle_timeout_ms must be at least twice Binance's 20 s ping interval ({minimum}), got {value}"
    )]
    IdleTimeoutTooShort { value: u64, minimum: u64 },
    #[error("binance.data_idle_timeout_ms must be at least idle_timeout_ms and quote_ttl_ms")]
    DataIdleTimeoutTooShort,
    #[error("binance.retry_min_ms must not exceed binance.retry_max_ms")]
    RetryRange,
    #[error("binance.retry_max_ms must be at most {MAX_RETRY_MS}, got {0}")]
    RetryMaxTooLong(u64),
    #[error(
        "binance.connection_lifetime_secs must be between {MIN_CONNECTION_LIFETIME_SECS} and \
         Binance's 24-hour limit ({BINANCE_CONNECTION_LIFETIME_SECS}, exclusive), got {0}"
    )]
    LifetimeOutOfRange(u64),
    #[error("binance.max_connection_attempts must be below Binance's per-IP limit of {limit}, got {value}")]
    TooManyConnectionAttempts { value: usize, limit: usize },
    #[error("engine.clearing_fee_ppm must be below {maximum}, got {fee}")]
    InvalidClearingFee { fee: u32, maximum: u32 },
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

    /// `price` as a decimal string, rounded half up: `Full` keeps up to 18
    /// places without trailing zeros, `Fixed(n)` exactly `n` places.
    pub fn format(self, price: Decimal) -> String {
        let places = match self {
            Self::Full => 18,
            Self::Fixed(places) => u32::from(places),
        };
        let mut rounded =
            price.round_dp_with_strategy(places, RoundingStrategy::MidpointAwayFromZero);
        match self {
            Self::Full => rounded = rounded.normalize(),
            Self::Fixed(_) => rounded.rescale(places),
        }
        rounded.to_string()
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
/// `max_spread_bps` has a default; those two must be chosen per deployment. A
/// misspelled key is an error, not a silent fallback to the default.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BinanceConfig {
    /// Stream base URLs of reader A and reader B. By default reader A uses the
    /// market-data-only endpoint and reader B the main one: separate front
    /// ends, so one endpoint failing does not take out both readers. Binance
    /// refuses some regions on the main endpoint (observed for the US); point
    /// both readers at the market-data endpoint there.
    pub stream_endpoints: [String; 2],
    /// REST base URL for `exchangeInfo` checks at startup.
    pub rest_endpoint: String,
    /// A quote is usable while `now - received_at < quote_ttl_ms`, measured
    /// from local receipt, for clearing, swap guidance and wallet prices
    /// alike. At most 60 s: this is what pauses clearing when both readers
    /// stall.
    pub quote_ttl_ms: u64,
    /// Widest accepted spread, `10_000 × (ask - bid) / mid`, inclusive. A
    /// wider quote makes its market invalid until the next valid one.
    pub max_spread_bps: u32,
    /// Least displayed notional (`quantity × price`) on each side of a quote,
    /// in the symbol's quote asset, e.g. `"5000"` USDT. A thinner side makes
    /// the quote invalid, so a one-lot top of book cannot set the price.
    /// Unset accepts any positive size.
    pub min_notional: Option<String>,
    /// Asset the price API values tokens in (`<ASSET><QUOTE>` markets).
    pub valuation_quote_asset: AssetCode,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    /// Reconnect a stream that delivers no frame, not even a ping, this long.
    pub idle_timeout_ms: u64,
    /// Reconnect a stream that delivers no quote this long although it stays
    /// up: a stalled backend keeps pinging. At least the idle timeout and TTL.
    pub data_idle_timeout_ms: u64,
    /// Longest planned connection, below Binance's 24-hour limit; each lasts
    /// a random 50–100% of it, so the two readers renew apart.
    pub connection_lifetime_secs: u64,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
    /// Connection attempts per 5 minutes, shared by both readers. Binance
    /// allows 300 per IP, counted over every process behind that IP, so the
    /// sum across processes must stay below 300.
    pub max_connection_attempts: usize,
    /// How long startup keeps waiting for every configured symbol to be
    /// confirmed before the readers start with the confirmed ones.
    pub validation_timeout_secs: u64,
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
            min_notional: None,
            valuation_quote_asset: AssetCode::parse("USDT").expect("valid asset code"),
            connect_timeout_ms: 10_000,
            request_timeout_ms: 10_000,
            idle_timeout_ms: 3 * BINANCE_PING_INTERVAL_MS,
            data_idle_timeout_ms: 60_000,
            connection_lifetime_secs: 23 * 60 * 60,
            retry_min_ms: 500,
            retry_max_ms: 60_000,
            max_connection_attempts: 30,
            validation_timeout_secs: 60,
        }
    }
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

impl BinanceConfig {
    fn validate(&self) -> std::result::Result<(), ConfigError> {
        for (name, value) in [
            ("quote_ttl_ms", self.quote_ttl_ms),
            ("max_spread_bps", u64::from(self.max_spread_bps)),
            ("connect_timeout_ms", self.connect_timeout_ms),
            ("request_timeout_ms", self.request_timeout_ms),
            ("idle_timeout_ms", self.idle_timeout_ms),
            ("data_idle_timeout_ms", self.data_idle_timeout_ms),
            ("connection_lifetime_secs", self.connection_lifetime_secs),
            ("retry_min_ms", self.retry_min_ms),
            ("retry_max_ms", self.retry_max_ms),
            (
                "max_connection_attempts",
                self.max_connection_attempts as u64,
            ),
            ("validation_timeout_secs", self.validation_timeout_secs),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroBinanceSetting(name));
            }
        }
        for endpoint in &self.stream_endpoints {
            check_endpoint("stream_endpoints", endpoint, ["ws", "wss"])?;
        }
        check_endpoint("rest_endpoint", &self.rest_endpoint, ["http", "https"])?;
        if self.quote_ttl_ms > MAX_QUOTE_TTL_MS {
            return Err(ConfigError::TtlTooLong(self.quote_ttl_ms));
        }
        if self.max_spread_bps > 10_000 {
            return Err(ConfigError::SpreadTooWide(self.max_spread_bps));
        }
        if let Some(raw) = &self.min_notional {
            parse_positive_decimal(raw)
                .ok_or_else(|| ConfigError::InvalidMinNotional(raw.clone()))?;
        }
        if self.idle_timeout_ms < 2 * BINANCE_PING_INTERVAL_MS {
            return Err(ConfigError::IdleTimeoutTooShort {
                value: self.idle_timeout_ms,
                minimum: 2 * BINANCE_PING_INTERVAL_MS,
            });
        }
        if self.data_idle_timeout_ms < self.idle_timeout_ms.max(self.quote_ttl_ms) {
            return Err(ConfigError::DataIdleTimeoutTooShort);
        }
        if self.retry_min_ms > self.retry_max_ms {
            return Err(ConfigError::RetryRange);
        }
        if self.retry_max_ms > MAX_RETRY_MS {
            return Err(ConfigError::RetryMaxTooLong(self.retry_max_ms));
        }
        if !(MIN_CONNECTION_LIFETIME_SECS..BINANCE_CONNECTION_LIFETIME_SECS)
            .contains(&self.connection_lifetime_secs)
        {
            return Err(ConfigError::LifetimeOutOfRange(
                self.connection_lifetime_secs,
            ));
        }
        if self.max_connection_attempts >= BINANCE_CONNECTION_ATTEMPT_LIMIT {
            return Err(ConfigError::TooManyConnectionAttempts {
                value: self.max_connection_attempts,
                limit: BINANCE_CONNECTION_ATTEMPT_LIMIT,
            });
        }
        Ok(())
    }

    /// The feed's runtime settings; call after [`SolverConfig::load`] has
    /// validated them.
    pub(crate) fn feed_config(&self) -> FeedConfig {
        FeedConfig {
            stream_endpoints: self.stream_endpoints.clone(),
            rest_endpoint: self.rest_endpoint.clone(),
            limits: QuoteLimits {
                max_spread_bps: self.max_spread_bps,
                min_notional: self
                    .min_notional
                    .as_deref()
                    .and_then(parse_positive_decimal),
            },
            quote_ttl: Duration::from_millis(self.quote_ttl_ms),
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            idle_timeout: Duration::from_millis(self.idle_timeout_ms),
            data_idle_timeout: Duration::from_millis(self.data_idle_timeout_ms),
            connection_lifetime: Duration::from_secs(self.connection_lifetime_secs),
            retry: RetryPolicy {
                min_delay: Duration::from_millis(self.retry_min_ms),
                max_delay: Duration::from_millis(self.retry_max_ms),
            },
            max_connection_attempts: self.max_connection_attempts,
            validation_timeout: Duration::from_secs(self.validation_timeout_secs),
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
        self.binance.validate()?;
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
        let feed = config.binance.feed_config();
        assert_eq!(feed.quote_ttl, Duration::from_secs(2));
        assert_eq!(feed.limits.max_spread_bps, 50);
        assert_eq!(feed.limits.min_notional, None);
        assert_eq!(feed.validation_timeout, Duration::from_secs(60));
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

    #[test]
    fn binance_settings_are_checked() {
        type Change = fn(&mut BinanceConfig);
        let check = |change: Change| check(|config| change(&mut config.binance));
        let zero: [(&str, Change); 11] = [
            ("quote_ttl_ms", |b| b.quote_ttl_ms = 0),
            ("max_spread_bps", |b| b.max_spread_bps = 0),
            ("connect_timeout_ms", |b| b.connect_timeout_ms = 0),
            ("request_timeout_ms", |b| b.request_timeout_ms = 0),
            ("idle_timeout_ms", |b| b.idle_timeout_ms = 0),
            ("data_idle_timeout_ms", |b| b.data_idle_timeout_ms = 0),
            ("connection_lifetime_secs", |b| {
                b.connection_lifetime_secs = 0
            }),
            ("retry_min_ms", |b| b.retry_min_ms = 0),
            ("retry_max_ms", |b| b.retry_max_ms = 0),
            ("max_connection_attempts", |b| b.max_connection_attempts = 0),
            ("validation_timeout_secs", |b| b.validation_timeout_secs = 0),
        ];
        for (name, change) in zero {
            assert!(check(change).contains(name), "{name}");
        }
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
            check(|b| b.rest_endpoint = "https://api.binance.com/api/v3".into())
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
        assert!(
            check(|b| b.stream_endpoints[0] = "ws://10.0.0.5:8089".into()).contains("plaintext")
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
        assert!(check(|b| b.quote_ttl_ms = MAX_QUOTE_TTL_MS + 1).contains("quote_ttl_ms"));
        assert!(check(|b| b.max_spread_bps = 10_001).contains("at most 10000"));
        assert!(check(|b| b.min_notional = Some("0".into())).contains("min_notional"));
        assert!(check(|b| b.min_notional = Some("5e3".into())).contains("min_notional"));
        assert!(
            check(|b| b.idle_timeout_ms = 2 * BINANCE_PING_INTERVAL_MS - 1)
                .contains("idle_timeout_ms")
        );
        assert!(check(|b| b.data_idle_timeout_ms = b.idle_timeout_ms - 1)
            .contains("data_idle_timeout_ms"));
        assert!(check(|b| b.retry_min_ms = b.retry_max_ms + 1).contains("retry_min_ms"));
        assert!(check(|b| b.retry_max_ms = MAX_RETRY_MS + 1).contains("retry_max_ms"));
        assert!(
            check(|b| b.connection_lifetime_secs = BINANCE_CONNECTION_LIFETIME_SECS)
                .contains("24-hour")
        );
        assert!(
            check(|b| b.connection_lifetime_secs = MIN_CONNECTION_LIFETIME_SECS - 1)
                .contains("connection_lifetime_secs")
        );
        assert!(check(|b| b.max_connection_attempts = 300).contains("per-IP limit of 300"));
    }

    /// The accept side of every boundary.
    #[test]
    fn binance_boundaries_are_accepted() {
        accepted(|c| c.binance.quote_ttl_ms = MAX_QUOTE_TTL_MS);
        accepted(|c| c.binance.max_spread_bps = 10_000);
        accepted(|c| c.binance.min_notional = Some("5000".into()));
        accepted(|c| c.binance.min_notional = Some("0.5".into()));
        accepted(|c| c.binance.idle_timeout_ms = 2 * BINANCE_PING_INTERVAL_MS);
        accepted(|c| {
            c.binance.data_idle_timeout_ms = c.binance.idle_timeout_ms;
        });
        accepted(|c| c.binance.retry_min_ms = c.binance.retry_max_ms);
        accepted(|c| c.binance.retry_max_ms = MAX_RETRY_MS);
        accepted(|c| c.binance.connection_lifetime_secs = MIN_CONNECTION_LIFETIME_SECS);
        accepted(|c| c.binance.connection_lifetime_secs = BINANCE_CONNECTION_LIFETIME_SECS - 1);
        accepted(|c| c.binance.max_connection_attempts = 299);
        let mut config = example();
        config.binance.min_notional = Some("5000".into());
        assert_eq!(
            config.binance.feed_config().limits.min_notional,
            Some(Decimal::from(5000))
        );
    }

    #[test]
    fn the_binance_section_is_required() {
        let mut table: toml::Table = toml::from_str(EXAMPLE).unwrap();
        table.remove("binance");
        let error = toml::from_str::<SolverConfig>(&table.to_string()).unwrap_err();
        assert!(error.to_string().contains("binance"), "{error}");
    }
}

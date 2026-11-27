//! Public, read-only **price-query HTTP API** for swap UIs: a token's price by
//! faucet id (`/v1/price`, `/v1/prices`), a pair's clearing price to build an
//! ask from (`/v2/pair-price`), and what the matcher would do with an order the
//! wallet is about to sign (`/v2/swap-eta`, see [`crate::swap_eta`]).
//! `/v1/swap-eta` keeps its original answers for wallets built against it.
//!
//! ISOLATION (P0): this is a public, unauthenticated surface, so it runs on its
//! OWN OS thread + multi-thread runtime. It never touches the `!Send` miden
//! `Client` or the matcher's single-threaded `LocalSet`, so wallet traffic
//! cannot starve fund settlement. Protections: a runtime of
//! `price_query_workers` threads (default 2, so at most two cores; the accept
//! loop runs on them too), its own HTTP/1.1-only connection loop with a
//! connection cap (kept under the process's open-file limit), a header
//! timeout, a write-stall timeout and a 16 KiB per-connection buffer, request
//! timeout, body cap, batch cap, and a cap on `/v2/swap-eta` requests waiting
//! for liquidity. No handler blocks a worker: the only await is that wait.
//! `/livez` and `/readyz` are for a load balancer's health checks.
//!
//! Prices come from the Binance snapshot ([`crate::price::PriceSnapshot`]): a
//! token is worth the exact midpoint of its `<ASSET><QUOTE>` market, usable
//! while younger than the quote TTL. The registered tokens, with their
//! decimals and tickers, are read from the DB once at startup and kept in
//! memory, so public traffic never touches PostgreSQL; a registration change
//! shows after a restart. The book depth for swap quotes comes from
//! [`mirror_depth`], which this thread runs: the matcher sends one batch of
//! changed orders per finished step.
//! The mirror also remembers each pair's last makers' price, which
//! `/v2/pair-price` uses for a few seconds while the book has nothing
//! crossing inside the band (a maker re-quoting, or a matched order settling).
//! `/v2/pairs` lists the configured clearing pairs and their tokens, built
//! once at startup.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{self, Poll};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use miden_protocol::account::AccountId;
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;

use crate::book_feed::{BookBatch, BookCopy};
use crate::clearing::{BatchPrice, OrderSide};
use crate::db::postgres_models::RegisteredTokenRow;
use crate::matching::types::{BookLevel, SwapBookSnapshot};
use crate::price::PricePrecision;
use crate::price::{MarketPlan, OrderMarket, PriceSnapshot, PriceUnavailable, Valued};
use crate::swap_eta::{
    eval_can_fill, eval_off_market, fill_price, judge, remembered_price, walk_levels, whole_price,
    FillStatus, NoFillReason, PriceBand, Probe, ProposedOrder, Remembered, RememberedPrices,
    SettlementStats, VerdictTerms,
};
use crate::types::TokenId;

/// Knobs for the price-query server (sourced from `EngineConfig` and `BinanceConfig`).
#[derive(Debug, Clone)]
pub struct PriceApiConfig {
    pub bind: String,
    pub port: u16,
    pub max_batch: usize,
    pub timeout_ms: u64,
    /// Worker threads of this API's runtime (`engine.price_query_workers`).
    pub workers: usize,
    /// Open connections kept at once (`engine.price_query_max_connections`).
    pub max_connections: usize,
    /// Time a connection has to send a request's headers, idle kept-alive
    /// time included (`engine.price_query_header_timeout_ms`).
    pub header_timeout_ms: u64,
    /// `/v2/swap-eta`: how long a request may wait for liquidity (its
    /// `wait_ms` default and maximum); `0` turns waiting off.
    pub swap_eta_wait_ms: u64,
    /// The valuation quote asset, as reported in responses (e.g. `"usdt"`).
    pub vs_currency: String,
    /// Default precision (`"full"` or `"0".."18"`); overridable per request.
    pub precision: String,
    // ── swap-eta terms ──
    /// Matcher tick interval (ms) — a term of the next-batch ETA.
    pub swap_matching_trigger_ms: u64,
    /// Ingest sync-poll interval (ms) — the pre-matcher delay, a term of the ETA.
    pub swap_sync_ms: u64,
    /// Estimated proving time (ms) — a term of the ETA.
    pub swap_proving_ms: u64,
    /// Estimated block time (ms) — a term of the ETA.
    pub swap_block_ms: u64,
    /// `engine.swap_offmarket_tolerance_bps` (see there).
    pub swap_offmarket_tolerance_bps: u64,
    /// The matcher's clearing fee, so quotes apply the same rule.
    pub clearing_fee_ppm: u32,
}

/// Shared (Send+Sync) state — no `!Send` client, so it lives on its own thread.
#[derive(Clone)]
pub struct PriceApiState {
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    /// The registered tokens, loaded once at startup.
    tokens: Arc<HashMap<AccountId, RegisteredTokenRow>>,
    /// The `/v2/pairs` answer, built once at startup.
    pairs: Arc<PairsResponse>,
    vs_currency: String,
    default_precision: PricePrecision,
    max_batch: usize,
    // ── swap-eta ──
    /// The matcher's resting levels, from [`mirror_depth`] (read lock-free).
    book_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
    /// Each pair's last makers' price, from [`mirror_depth`]; published just
    /// before the book it was found in.
    remembered_rx: watch::Receiver<Arc<RememberedPrices>>,
    /// In-memory settlement-time window from the executor.
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    /// Next-batch ETA (secs) = ceil((sync + trigger + proving + block)/1000).
    swap_eta_secs: u64,
    verdict_terms: VerdictTerms,
    /// `/v2/swap-eta` liquidity wait: default and maximum.
    swap_eta_wait: Duration,
    /// Places for requests waiting for liquidity ([`MAX_WAITERS`]).
    waiters: Arc<Semaphore>,
}

/// At most this many `/v2/swap-eta` requests wait for liquidity at once; the
/// next one answers straight away.
const MAX_WAITERS: usize = 256;

impl PriceApiState {
    pub fn new(
        cfg: &PriceApiConfig,
        prices: watch::Receiver<Arc<PriceSnapshot>>,
        tokens: Vec<RegisteredTokenRow>,
        markets: &MarketPlan,
        book_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
        remembered_rx: watch::Receiver<Arc<RememberedPrices>>,
        stats_rx: watch::Receiver<Arc<SettlementStats>>,
    ) -> Self {
        let tokens = tokens
            .into_iter()
            .filter_map(|row| Some((row.token().ok()?, row)))
            .collect();
        let pairs = PairsResponse::new(markets, &tokens);
        Self {
            prices,
            tokens: Arc::new(tokens),
            pairs: Arc::new(pairs),
            vs_currency: cfg.vs_currency.clone(),
            default_precision: PricePrecision::parse(&cfg.precision)
                .unwrap_or(PricePrecision::Full),
            max_batch: cfg.max_batch,
            book_rx,
            remembered_rx,
            stats_rx,
            swap_eta_secs: (cfg.swap_sync_ms
                + cfg.swap_matching_trigger_ms
                + cfg.swap_proving_ms
                + cfg.swap_block_ms)
                .div_ceil(1000),
            verdict_terms: VerdictTerms {
                fee_ppm: cfg.clearing_fee_ppm,
                tolerance_bps: cfg.swap_offmarket_tolerance_bps,
            },
            swap_eta_wait: Duration::from_millis(cfg.swap_eta_wait_ms),
            waiters: Arc::new(Semaphore::new(MAX_WAITERS)),
        }
    }
}

// ── Response / error ─────────────────────────────────────────────────────────

#[derive(Serialize)]
struct PriceResponse {
    faucet_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ticker: Option<String>,
    vs_currency: String,
    /// Price of ONE whole token, formatted to `precision` (string to avoid JSON
    /// float ambiguity).
    price: String,
    precision: String,
    /// Token's on-chain decimals (null if unknown at startup).
    decimals: Option<u8>,
    /// Unix secs the quote was received (now, for the quote asset itself).
    as_of: i64,
    stale: bool,
    source: &'static str,
}

enum ApiError {
    BadFaucetId(String),
    BadAmount(String),
    BadRequest(String),
    UnknownFaucet,
    /// Registered, but no Binance market values it: nothing to wait for.
    NoMarket,
    /// Has a market, but no valid quote right now.
    NoPrice,
    /// Startup has not confirmed the Binance markets yet.
    NotReady,
    Stale(i64),
    BadPrecision(String),
    BatchTooLarge(usize),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::BadFaucetId(m) => (StatusCode::BAD_REQUEST, "bad_faucet_id", m),
            ApiError::BadAmount(m) => (StatusCode::BAD_REQUEST, "bad_amount", m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, "bad_request", m),
            ApiError::UnknownFaucet => (
                StatusCode::NOT_FOUND,
                "unknown_faucet",
                "faucet not registered".into(),
            ),
            ApiError::NoMarket => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no_market",
                "no Binance market is configured for this token or pair; it has no price \
                 until the solver's configuration changes"
                    .into(),
            ),
            ApiError::NoPrice => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no_price",
                "no valid price for this token or pair right now".into(),
            ),
            ApiError::NotReady => (
                StatusCode::SERVICE_UNAVAILABLE,
                "not_ready",
                "the solver is still confirming its Binance markets".into(),
            ),
            ApiError::Stale(as_of) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "stale",
                format!("price is stale; last update {as_of} (use ?allow_stale=true to override)"),
            ),
            ApiError::BadPrecision(m) => (StatusCode::BAD_REQUEST, "bad_precision", m),
            ApiError::BatchTooLarge(max) => (
                StatusCode::BAD_REQUEST,
                "batch_too_large",
                format!("at most {max} ids per request"),
            ),
        };
        (status, Json(json!({ "error": code, "message": message }))).into_response()
    }
}

impl From<PriceUnavailable> for ApiError {
    fn from(reason: PriceUnavailable) -> Self {
        match reason {
            PriceUnavailable::NoMarket => ApiError::NoMarket,
            _ => ApiError::NoPrice,
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Unix secs of a quote received `age` ago.
fn received_secs(age: Duration) -> i64 {
    now_secs().saturating_sub(i64::try_from(age.as_secs()).unwrap_or(i64::MAX))
}

fn precision_label(p: PricePrecision) -> String {
    match p {
        PricePrecision::Full => "full".to_string(),
        PricePrecision::Fixed(n) => n.to_string(),
    }
}

fn resolve_precision(
    state: &PriceApiState,
    q: &HashMap<String, String>,
) -> Result<PricePrecision, ApiError> {
    match q.get("precision") {
        Some(p) => PricePrecision::parse(p).ok_or_else(|| {
            ApiError::BadPrecision(format!("must be \"full\" or 0..=18, got {p:?}"))
        }),
        None => Ok(state.default_precision),
    }
}

fn wants_stale(q: &HashMap<String, String>) -> bool {
    q.get("allow_stale")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// One token's price from `snapshot`. `NoMarket` when nothing values it,
/// `NoPrice` when its market has no valid quote; `Stale` when its quote is at
/// least the TTL old and the caller did not allow that.
fn quote_from_row(
    state: &PriceApiState,
    snapshot: &PriceSnapshot,
    account_id: AccountId,
    row: &RegisteredTokenRow,
    precision: PricePrecision,
    allow_stale: bool,
) -> Result<PriceResponse, ApiError> {
    // One clock reading: freshness and `as_of` describe the same instant.
    let now = Instant::now();
    let Valued {
        price,
        received_at,
        fresh,
    } = snapshot.valuation(account_id, now).map_err(|reason| {
        tracing::debug!(faucet = %account_id, %reason, "no price for token");
        ApiError::from(reason)
    })?;
    let age = received_at.map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
    let as_of = received_secs(age);
    if !fresh && !allow_stale {
        return Err(ApiError::Stale(as_of));
    }
    let decimals = row.token_decimals();
    Ok(PriceResponse {
        faucet_id: account_id.to_hex(),
        ticker: row.ticker.clone(),
        vs_currency: state.vs_currency.clone(),
        price: precision.format(price),
        precision: precision_label(precision),
        decimals,
        as_of,
        stale: !fresh,
        source: "binance",
    })
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// `GET /v1/price/{faucet_id}?precision=&allow_stale=`
async fn get_price(
    State(state): State<PriceApiState>,
    Path(faucet_id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<PriceResponse>, ApiError> {
    let precision = resolve_precision(&state, &q)?;
    let account_id = AccountId::from_hex(&faucet_id)
        .map_err(|error| ApiError::BadFaucetId(error.to_string()))?;
    let row = state
        .tokens
        .get(&account_id)
        .ok_or(ApiError::UnknownFaucet)?;
    let snapshot = state.prices.borrow().clone();
    Ok(Json(quote_from_row(
        &state,
        &snapshot,
        account_id,
        row,
        precision,
        wants_stale(&q),
    )?))
}

/// `GET /v1/prices?ids=a,b,c&precision=&allow_stale=` → `{ "<faucet_id>": {..} }`
/// Unknown, unpriced, and (unless `allow_stale`) stale ids are omitted.
async fn get_prices(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<HashMap<String, PriceResponse>>, ApiError> {
    let ids: Vec<&str> = q
        .get("ids")
        .map(String::as_str)
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if ids.len() > state.max_batch {
        return Err(ApiError::BatchTooLarge(state.max_batch));
    }
    let precision = resolve_precision(&state, &q)?;
    let allow_stale = wants_stale(&q);
    let accounts: Vec<_> = ids
        .into_iter()
        .filter_map(|id| AccountId::from_hex(id).ok())
        .collect();
    let snapshot = state.prices.borrow().clone();
    let mut out = HashMap::new();
    for account_id in accounts {
        let Some(row) = state.tokens.get(&account_id) else {
            continue;
        };
        if let Ok(resp) = quote_from_row(&state, &snapshot, account_id, row, precision, allow_stale)
        {
            out.insert(resp.faucet_id.clone(), resp);
        }
    }
    Ok(Json(out))
}

// ── pairs ────────────────────────────────────────────────────────────────────

/// Response for `GET /v2/pairs`: the tokens the solver swaps, each once, and
/// the pairs between them. Field meanings: docs/price-api.md.
#[derive(Clone, Serialize)]
struct PairsResponse {
    tokens: Vec<PairToken>,
    pairs: Vec<Pair>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PairToken {
    faucet_id: String,
    ticker: Option<String>,
    decimals: Option<u8>,
    binance_asset: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Pair {
    name: String,
    binance_symbol: String,
    faucet_ids: [String; 2],
}

impl PairsResponse {
    /// Every configured clearing pair, in configuration order, and its tokens
    /// in order of first appearance. Binance confirms each of these pairs at
    /// startup or the solver does not start; whether one is trading right now
    /// is `/v2/pair-price`'s answer. Startup also requires on-chain decimals
    /// for each token, so `ticker` and `decimals` are set in practice.
    fn new(markets: &MarketPlan, tokens: &HashMap<AccountId, RegisteredTokenRow>) -> Self {
        let mut listed: Vec<AccountId> = Vec::new();
        let pairs = markets
            .clearing_markets()
            .iter()
            .map(|market| {
                for token in [market.base, market.quote] {
                    if !listed.contains(&token) {
                        listed.push(token);
                    }
                }
                Pair {
                    name: market.name.clone(),
                    binance_symbol: market.symbol.to_string(),
                    faucet_ids: [market.base.to_hex(), market.quote.to_hex()],
                }
            })
            .collect();
        let tokens = listed
            .into_iter()
            .map(|token| {
                let row = tokens.get(&token);
                PairToken {
                    faucet_id: token.to_hex(),
                    ticker: row.and_then(|row| row.ticker.clone()),
                    decimals: row.and_then(RegisteredTokenRow::token_decimals),
                    binance_asset: markets
                        .asset_code(token)
                        .expect("MarketPlan::new requires an asset code for every clearing token")
                        .to_string(),
                }
            })
            .collect();
        Self { tokens, pairs }
    }
}

// ── swap-eta ─────────────────────────────────────────────────────────────────

/// Response for `GET /v2/pair-price`: the price a wallet builds its ask from.
/// Field meanings: docs/price-api.md.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PairPriceResponse {
    offered_faucet: String,
    requested_faucet: String,
    market_price: String,
    /// The batch price an order of the pair's reference size would get now
    /// ([`expected_price`]); the mid on a pair without a band.
    expected_price: String,
    /// [`fill_price`]: the expected price less the fee, to build an ask from.
    fill_price: String,
    fee_ppm: u32,
    offered_decimals: u8,
    requested_decimals: u8,
    /// Unix secs the solver received the Binance quote.
    as_of: i64,
}

/// Response for `GET /v2/swap-eta`: the [`crate::swap_eta::Verdict`] on the order, plus time
/// estimates. Field meanings: docs/price-api.md. Optional fields serialise as
/// `null` (stable shape for the wallet), never omitted.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapEtaResponse {
    offered_faucet: String,
    requested_faucet: String,
    offered_amount: String,
    requested_amount: String,
    price_band: Option<PriceBand>,
    fill_status: FillStatus,
    reason: Option<NoFillReason>,
    fillable_offered_amount: String,
    fillable_requested_amount: String,
    max_offered_amount: Option<String>,
    expected_requested_amount: Option<String>,
    fee_ppm: u32,
    fee_amount: Option<String>,
    market_price: Option<String>,
    /// The batch price with this order in the book ([`expected_price`]).
    expected_price: Option<String>,
    fill_price: Option<String>,
    /// The executor's own [`SettlementStats::executor_accepting`].
    settlements_running: bool,
    estimated_seconds: Option<u64>,
    median24h_seconds: Option<u64>,
}

/// A required positive base-unit amount.
fn required_amount(q: &HashMap<String, String>, key: &str) -> Result<u64, ApiError> {
    match parse_amount(q, key)? {
        None => Err(ApiError::BadAmount(format!("missing `{key}`"))),
        Some(0) => Err(ApiError::BadAmount(format!("`{key}` must be > 0"))),
        Some(v) => Ok(v),
    }
}

/// An optional base-unit amount.
fn parse_amount(q: &HashMap<String, String>, key: &str) -> Result<Option<u64>, ApiError> {
    q.get(key)
        .map(|raw| {
            raw.parse()
                .map_err(|_| ApiError::BadAmount(format!("`{key}` must be a u64, got {raw:?}")))
        })
        .transpose()
}

fn parse_faucet(q: &HashMap<String, String>, key: &str) -> Result<AccountId, ApiError> {
    let raw = q
        .get(key)
        .ok_or_else(|| ApiError::BadFaucetId(format!("missing `{key}`")))?;
    AccountId::from_hex(raw).map_err(|e| ApiError::BadFaucetId(format!("`{key}`: {e}")))
}

/// The two faucets of a pair request: valid, distinct hex ids.
fn pair_ids(q: &HashMap<String, String>) -> Result<(AccountId, AccountId), ApiError> {
    let a = parse_faucet(q, "offered_faucet")?;
    let b = parse_faucet(q, "requested_faucet")?;
    if a == b {
        return Err(ApiError::BadRequest(
            "offered_faucet and requested_faucet must differ".into(),
        ));
    }
    Ok((a, b))
}

/// Both tokens' on-chain decimals; `404` unless both are registered.
fn registered_decimals(
    state: &PriceApiState,
    a: AccountId,
    b: AccountId,
) -> Result<(Option<u8>, Option<u8>), ApiError> {
    match (state.tokens.get(&a), state.tokens.get(&b)) {
        (Some(row_a), Some(row_b)) => Ok((row_a.token_decimals(), row_b.token_decimals())),
        _ => Err(ApiError::UnknownFaucet),
    }
}

/// `GET /v2/pairs`: what the solver swaps. It changes only with a restart, so
/// shared caches may keep it for five minutes. Until the price feed has
/// confirmed the markets at startup it answers `503 not_ready`, so a list a
/// failing startup would reject is never served or cached.
async fn get_pairs(State(state): State<PriceApiState>) -> Result<impl IntoResponse, ApiError> {
    if !markets_confirmed(&state) {
        return Err(ApiError::NotReady);
    }
    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=300"),
        )],
        Json(PairsResponse::clone(&state.pairs)),
    ))
}

/// Whether the price feed has confirmed the Binance markets at startup (or
/// no pair is configured, so there is nothing to confirm). Until then
/// `/v2/pairs` and `/readyz` answer `503 not_ready`.
fn markets_confirmed(state: &PriceApiState) -> bool {
    state.pairs.pairs.is_empty()
        || state
            .prices
            .borrow()
            .markets()
            .clearing_pairs()
            .next()
            .is_some()
}

/// `GET /livez`: the API thread answers. It shares the public listener and
/// its connection cap, so a full cap also silences it: a supervisor deciding
/// whether to restart the solver should use the observability server's
/// `/health` on loopback instead, which public traffic cannot reach.
async fn get_livez() -> impl IntoResponse {
    no_store("ok")
}

/// `GET /readyz`: quotes can be served (the markets are confirmed), else
/// `503 not_ready`. For a load balancer's health check.
async fn get_readyz(State(state): State<PriceApiState>) -> impl IntoResponse {
    no_store(if markets_confirmed(&state) {
        Ok("ready")
    } else {
        Err(ApiError::NotReady)
    })
}

/// The pair's sells and buys in `book`, best first, for an order offering
/// `offered` for `requested` on `side`.
fn pair_sides(
    book: &SwapBookSnapshot,
    side: OrderSide,
    offered: AccountId,
    requested: AccountId,
) -> (&[BookLevel], &[BookLevel]) {
    let levels = |pair| book.get(&pair).map_or(&[][..], Vec::as_slice);
    let (own, other) = (levels((offered, requested)), levels((requested, offered)));
    match side {
        OrderSide::SellBase => (own, other),
        OrderSide::BuyBase => (other, own),
    }
}

/// The price a quote is judged at, found as the clearing finds it (ADR 0007),
/// on `book` for the order offering `a` for `b`:
/// 1. the walk with `probe` (the user's order, or the reference order);
/// 2. else, for an order that crosses nothing, the reference walk: the price
///    the makers in the book clear at (an order priced inside the spread is
///    judged at it);
/// 3. else, given `remembered` (pair-price only), while the book has nothing
///    crossing and the pair's remembered price is alive, the clearing's price
///    with that makers' order back;
/// 4. else the mid. A walk error (an overflow on extreme amounts) also gives
///    the mid: what the matcher clears at in that case too.
fn quote_price(
    market: &OrderMarket,
    fee_ppm: u32,
    book: &SwapBookSnapshot,
    remembered: Option<&RememberedPrices>,
    (a, b): (AccountId, AccountId),
    probe: Probe,
) -> BatchPrice {
    let walk = |probe| {
        let (sells, buys) = pair_sides(book, market.side, a, b);
        walk_levels(market, fee_ppm, sells, buys, probe).unwrap_or_else(|error| {
            tracing::debug!(%error, "bounded price failed; quoting at the mid");
            None
        })
    };
    let reference = walk(Probe::Reference);
    let found = match probe {
        Probe::Reference => reference,
        Probe::Order(_) => walk(probe).or(reference),
    };
    if let Some(walked) = found {
        return walked.price;
    }
    let now = tokio::time::Instant::now();
    remembered
        .and_then(|remembered| remembered.get(&(a, b)))
        .filter(|memory| memory.alive(now))
        .and_then(|memory| remembered_price(market, memory).ok().flatten())
        .unwrap_or(market.price)
}

/// `GET /v2/pair-price?offered_faucet=&requested_faucet=`
///
/// The pair's expected batch price for this direction, under the same
/// freshness rule, band and fee as the matcher: the walk with one order of
/// the pair's reference size added (ADR 0007). The wallet builds its ask
/// from `fill_price` and the user's slippage, then asks `/v2/swap-eta` about
/// that exact order.
async fn get_pair_price(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<PairPriceResponse>, ApiError> {
    let (a, b) = pair_ids(&q)?;
    // 404 unless both are registered; the decimals come with the market.
    registered_decimals(&state, a, b)?;
    let now = Instant::now();
    let market = state.prices.borrow().order_market(a, b, now)?;
    let fee_ppm = state.verdict_terms.fee_ppm;
    // The book first: the remembered prices are published before it, so
    // they are at least as new.
    let book = state.book_rx.borrow().clone();
    let remembered = state.remembered_rx.borrow().clone();
    let price = quote_price(
        &market,
        fee_ppm,
        &book,
        Some(&remembered),
        (a, b),
        Probe::Reference,
    );
    let expected = whole_price(&market, price).ok_or(ApiError::NoPrice)?;
    let fill = fill_price(market.side, expected, fee_ppm).ok_or(ApiError::NoPrice)?;
    Ok(Json(PairPriceResponse {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        market_price: PricePrecision::Full.format(market.market),
        expected_price: PricePrecision::Full.format(expected),
        fill_price: PricePrecision::Full.format(fill),
        fee_ppm,
        offered_decimals: market.offered_decimals,
        requested_decimals: market.requested_decimals,
        as_of: received_secs(now.saturating_duration_since(market.received_at)),
    }))
}

/// `GET /v2/swap-eta?offered_faucet=&offered_amount=&requested_faucet=&requested_amount=&min_fill_step=&wait_ms=`
///
/// The [`crate::swap_eta::Verdict`] on the order a wallet is about to sign (offer A / request
/// B, base units), plus the time estimates.
///
/// Long polling (the pattern of SQS `WaitTimeSeconds`): when the order is at
/// market and settlements run but it does not fill in full, the request
/// waits up to `wait_ms` (default and maximum: `swap_eta_wait_ms`; `0`: no
/// wait) for any maker, re-checked on the live book at every book change. It
/// answers as soon as it fills in full or is no longer a liquidity
/// shortfall, or at the deadline with the latest ordinary answer.
async fn get_swap_eta(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let (a, b) = pair_ids(&q)?;
    let order = ProposedOrder {
        offered: required_amount(&q, "offered_amount")?,
        requested: required_amount(&q, "requested_amount")?,
        // As in PSWAP, 0 (the default) means no minimum.
        min_fill_step: parse_amount(&q, "min_fill_step")?.unwrap_or(0),
    };
    let wait =
        match q.get("wait_ms") {
            None => state.swap_eta_wait,
            Some(raw) => Duration::from_millis(raw.parse().map_err(|_| {
                ApiError::BadRequest(format!("`wait_ms` must be a u64, got {raw:?}"))
            })?)
            .min(state.swap_eta_wait),
        };
    // 404 unless both are registered.
    registered_decimals(&state, a, b)?;

    // Mark the current book seen before the first check: a change between a
    // check and the wait still wakes the request.
    let mut book_changes = state.book_rx.clone();
    book_changes.borrow_and_update();
    let deadline = tokio::time::Instant::now() + wait;
    let mut place = None;
    loop {
        let (answer, worth_waiting) = swap_eta_answer(&state, a, b, order)?;
        if !worth_waiting || tokio::time::Instant::now() >= deadline {
            return Ok(no_store(Json(answer)));
        }
        if place.is_none() {
            match Arc::clone(&state.waiters).try_acquire_owned() {
                Ok(permit) => place = Some(permit),
                // Too many waiting already: today's immediate answer.
                Err(_) => return Ok(no_store(Json(answer))),
            }
        }
        tokio::select! {
            changed = book_changes.changed() => {
                // The mirror stopped (shutdown): nothing more will change.
                if changed.is_err() {
                    return Ok(no_store(Json(answer)));
                }
            }
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}

/// One check of the order against fresh snapshots: the answer, and whether
/// it is a liquidity shortfall that a book change could still fill.
fn swap_eta_answer(
    state: &PriceApiState,
    a: AccountId,
    b: AccountId,
    order: ProposedOrder,
) -> Result<(SwapEtaResponse, bool), ApiError> {
    // One price snapshot, one book snapshot, one clock reading.
    let book = state.book_rx.borrow().clone();
    let terms = state.verdict_terms;
    let market = state.prices.borrow().order_market(a, b, Instant::now());
    let (verdict, market_price, expected, fill, unpriced) = match market {
        Ok(market) => {
            let cannot_price = |error| ApiError::BadAmount(format!("cannot price: {error}"));
            let levels = |pair| book.get(&pair).map_or(&[][..], Vec::as_slice);
            // The batch price with this order in the book (ADR 0007); the
            // order is judged at it, as the matcher would clear it, on the
            // live book only: no remembered price, so a re-check after a
            // wait judges the makers who are really back, at their price.
            let price = quote_price(
                &market,
                terms.fee_ppm,
                &book,
                None,
                (a, b),
                Probe::Order(order),
            );
            let verdict = judge(
                market.side,
                price,
                terms,
                order,
                levels((a, b)),
                levels((b, a)),
            )
            .map_err(cannot_price)?;
            let expected = whole_price(&market, price)
                .ok_or_else(|| ApiError::BadAmount("cannot price: the expected price".into()))?;
            let fill = fill_price(market.side, expected, terms.fee_ppm);
            (
                Some(verdict),
                Some(market.market),
                Some(expected),
                fill,
                None,
            )
        }
        // Without a price nothing fills.
        Err(PriceUnavailable::NoMarket) => (None, None, None, None, Some(NoFillReason::NoMarket)),
        Err(_) => (None, None, None, None, Some(NoFillReason::NoPrice)),
    };
    let band = verdict.map(|v| v.band);
    let status = verdict.map_or(FillStatus::None, |v| v.status);
    let price = |value| PricePrecision::Full.format(value);
    let amount = |value: Option<u64>| value.map(|value| value.to_string());

    // Median — same direction (A → B) the note settles as; purely in-memory.
    let now_unix = now_secs().max(0) as u64;
    // Clone the snapshot first: sorting under the borrow would hold up the
    // executor's next publish.
    let stats = state.stats_rx.borrow().clone();
    let median24h_seconds = stats.median_secs((a, b), now_unix);
    let settlements_running = stats.executor_accepting;

    let answer = SwapEtaResponse {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        offered_amount: order.offered.to_string(),
        requested_amount: order.requested.to_string(),
        price_band: band,
        fill_status: status,
        reason: verdict.map_or(unpriced, |v| v.reason()),
        fillable_offered_amount: verdict.map_or(0, |v| v.fillable_offered).to_string(),
        fillable_requested_amount: verdict.map_or(0, |v| v.fillable_requested).to_string(),
        max_offered_amount: amount(verdict.and_then(|v| v.max_offered)),
        expected_requested_amount: amount(verdict.map(|v| v.expected_requested)),
        fee_ppm: terms.fee_ppm,
        fee_amount: amount(verdict.map(|v| v.fee)),
        market_price: market_price.map(price),
        expected_price: expected.map(price),
        fill_price: fill.map(price),
        settlements_running,
        estimated_seconds: (settlements_running
            && band == Some(PriceBand::AtMarket)
            && status != FillStatus::None)
            .then_some(state.swap_eta_secs),
        median24h_seconds,
    };
    // Only a liquidity shortfall waits: anything else answers as today.
    let worth_waiting =
        settlements_running && band == Some(PriceBand::AtMarket) && status != FillStatus::Full;
    Ok((answer, worth_waiting))
}

/// Don't let the router-level `max-age` layer cache a swap answer: it comes
/// from independently-updated snapshots, so a shared max-age would serve a
/// stale fill. `no-store` wins because the layer is `if_not_present`.
fn no_store(body: impl IntoResponse) -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        body,
    )
}

// ── v1 swap-eta (frozen) ─────────────────────────────────────────────────────

/// Response for `GET /v1/swap-eta`, frozen for wallets built against it (new
/// wallets use `/v2/swap-eta`). Two independent liquidity signals — the live
/// book (`can_fill` + `estimated_seconds`) and the Binance midpoint
/// (`off_market` + `market_price`) — plus the historical in-memory median. All
/// optional fields serialise as `null` (stable shape for the wallet), never
/// omitted.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapEtaV1Response {
    offered_faucet: String,
    requested_faucet: String,
    offered_amount: String,
    requested_amount: String,
    /// Book: crosses the best opposite-pair rate AND that level has the depth.
    can_fill: bool,
    /// Market: order priced worse than the Binance mid (why it won't fill).
    /// `null` if the pair has no fresh market price.
    off_market: Option<bool>,
    /// Next-batch ETA (secs); `null` when `can_fill` is false.
    estimated_seconds: Option<u64>,
    /// Binance mid in whole requested tokens per whole offered token, at the
    /// clearing freshness rule; `null` if unpriced.
    market_price: Option<String>,
    /// In-memory rolling per-pair median settlement secs; `null` when no samples.
    median24h_seconds: Option<u64>,
}

/// `GET /v1/swap-eta?offered_faucet=&offered_amount=&requested_faucet=&requested_amount=`
///
/// Given a prospective order (offer A / request B, raw base-unit amounts), report
/// whether it can fill in the next batch against the top of the live book, the
/// market price verdict, and the in-memory 24h median settlement time for the
/// pair.
async fn get_swap_eta_v1(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let (a, b) = pair_ids(&q)?;
    let offered_amount = required_amount(&q, "offered_amount")?;
    let requested_amount = required_amount(&q, "requested_amount")?;
    let (d_a, d_b) = registered_decimals(&state, a, b)?;

    // Book check — the incoming order (offer A, request B) crosses against the
    // top of the OPPOSITE pair (offer B, request A).
    let best = state
        .book_rx
        .borrow()
        .get(&(b, a))
        .and_then(|levels| levels.first().copied());
    let can_fill = eval_can_fill(offered_amount, requested_amount, best);
    let estimated_seconds = can_fill.then_some(state.swap_eta_secs);

    // Market check (advisory; independent of the book): the clearing pair's
    // Binance mid under the same freshness rule the matcher uses.
    let snapshot = state.prices.borrow().clone();
    let market = snapshot.market_price(a, b, Instant::now()).ok();
    let (off_market, market_price) = eval_off_market(
        offered_amount,
        d_a,
        requested_amount,
        d_b,
        market,
        state.verdict_terms.tolerance_bps,
    );

    // Median — same direction (A → B) the note settles as; purely in-memory.
    let now = now_secs().max(0) as u64;
    let stats = state.stats_rx.borrow().clone();
    let median24h_seconds = stats.median_secs((a, b), now);

    Ok(no_store(Json(SwapEtaV1Response {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        offered_amount: offered_amount.to_string(),
        requested_amount: requested_amount.to_string(),
        can_fill,
        off_market,
        estimated_seconds,
        market_price,
        median24h_seconds,
    })))
}

/// How often the depth mirror republishes the book for the quote handlers.
const DEPTH_PUBLISH_INTERVAL: Duration = Duration::from_millis(250);

/// Keep a copy of the matcher's book from its batches ([`crate::book_feed`])
/// and publish its levels for the quote handlers, at most every
/// [`DEPTH_PUBLISH_INTERVAL`]. Runs on this thread: the matcher only notes
/// what its index changed and sends one batch per finished step.
///
/// After each round of changes it also walks every pair those changes
/// touched, both directions, and remembers the makers' price the walk finds
/// (see [`remember_prices`]); pairs nothing touched cost nothing. The
/// remembered prices go out just before the book, so a handler that reads
/// the book first never sees older ones.
async fn mirror_depth(
    mut batches: mpsc::UnboundedReceiver<BookBatch>,
    book_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    remembered_tx: watch::Sender<Arc<RememberedPrices>>,
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    fee_ppm: u32,
) {
    let mut copy = BookCopy::with_levels();
    let mut remembered = RememberedPrices::new();
    let mut previous = Arc::new(SwapBookSnapshot::default());
    while let Some(batch) = batches.recv().await {
        let mut touched = copy.apply(&batch).touched;
        touched.extend(copy.drain(&mut batches).touched);
        let book = Arc::new(copy.snapshot());
        let snapshot = prices.borrow().clone();
        remember_prices(
            &mut remembered,
            &previous,
            &book,
            &snapshot,
            fee_ppm,
            &touched,
            tokio::time::Instant::now(),
        );
        remembered_tx.send_replace(Arc::new(remembered.clone()));
        book_tx.send_replace(Arc::clone(&book));
        // Counted once quotes can see them: the backlog is what they miss.
        copy.publish_applied();
        previous = book;
        tokio::time::sleep(DEPTH_PUBLISH_INTERVAL).await;
    }
}

/// For each pair in `touched`, both directions: the reference walk (as
/// `/v2/pair-price` runs it) on `book`. Found the makers → remember their
/// price. Nothing found → the clock ([`Remembered::empty_since`]) starts when
/// the makers left the book:
/// - if `previous` (the book before these changes) still has makers at
///   today's price, they left with these changes: remember them, clock now;
/// - else keep the old memory and start its clock if not running (also when
///   there is no price right now), so a memory never outlives
///   [`crate::swap_eta::REMEMBER_FOR`] unconfirmed; its clock ran out →
///   forget it.
pub(crate) fn remember_prices(
    remembered: &mut RememberedPrices,
    previous: &SwapBookSnapshot,
    book: &SwapBookSnapshot,
    prices: &PriceSnapshot,
    fee_ppm: u32,
    touched: &HashSet<(TokenId, TokenId)>,
    now: tokio::time::Instant,
) {
    let directions: HashSet<_> = touched
        .iter()
        .flat_map(|&(x, y)| [(x, y), (y, x)])
        .collect();
    for (a, b) in directions {
        let Ok(market) = prices.order_market(a, b, Instant::now()) else {
            start_clock(remembered, (a, b), now);
            continue;
        };
        let makers = |book| {
            let (sells, buys) = pair_sides(book, market.side, a, b);
            match walk_levels(&market, fee_ppm, sells, buys, Probe::Reference) {
                Ok(walked) => walked.map(|walked| Remembered::found(market.side, &walked)),
                Err(error) => {
                    tracing::debug!(%error, "depth mirror: reference walk failed");
                    None
                }
            }
        };
        if let Some(memory) = makers(book) {
            remembered.insert((a, b), memory);
            continue;
        }
        // Only if the memory is missing or its clock already runs: else the
        // makers it holds were found by the last walk, and left just now.
        let unsure = remembered
            .get(&(a, b))
            .is_none_or(|memory| memory.empty_since.is_some());
        match makers(previous).filter(|_| unsure) {
            Some(memory) => {
                let memory = Remembered {
                    empty_since: Some(now),
                    ..memory
                };
                remembered.insert((a, b), memory);
            }
            None => start_clock(remembered, (a, b), now),
        }
    }
}

/// Start `pair`'s clock if it isn't running; forget the memory once it ran out.
fn start_clock(
    remembered: &mut RememberedPrices,
    pair: (TokenId, TokenId),
    now: tokio::time::Instant,
) {
    if let Some(memory) = remembered.get_mut(&pair) {
        memory.empty_since.get_or_insert(now);
        if !memory.alive(now) {
            remembered.remove(&pair);
        }
    }
}

// ── Router + server ──────────────────────────────────────────────────────────

/// Build the full router (used by the server thread AND unit tests).
pub fn build_app(state: PriceApiState, cfg: &PriceApiConfig) -> Router {
    // Quotes move continuously; let shared caches hold a response briefly.
    let cache_value = HeaderValue::from_static("public, max-age=1");

    let v1 = Router::new()
        .route("/price/{faucet_id}", get(get_price))
        .route("/prices", get(get_prices))
        // Frozen for wallets built against it; new wallets use /v2.
        .route("/swap-eta", get(get_swap_eta_v1))
        .with_state(state.clone());
    let v2 = Router::new()
        .route("/pairs", get(get_pairs))
        .route("/pair-price", get(get_pair_price))
        .route("/swap-eta", get(get_swap_eta))
        .with_state(state.clone());
    let health = Router::new()
        .route("/livez", get(get_livez))
        .route("/readyz", get(get_readyz))
        .with_state(state);

    // Public read-only price data → permissive CORS so browser wallets /
    // extensions can fetch it cross-origin. Any origin, GET only (the API is
    // GET-only); preflight OPTIONS is handled by this layer.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET]);

    Router::new()
        .nest("/v1", v1)
        .nest("/v2", v2)
        .merge(health)
        // Outer protections (applied to all routes):
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            cache_value,
        ))
        // One timeout for every route: the processing margin plus the
        // longest `/v2/swap-eta` liquidity wait (no other route waits).
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(cfg.timeout_ms.saturating_add(cfg.swap_eta_wait_ms)),
        ))
        .layer(DefaultBodyLimit::max(4 * 1024))
        // CORS is outermost: it answers preflight + tags every response
        // (including timeouts).
        .layer(cors)
}

/// Spawn the price-query server on its OWN OS thread + multi-thread runtime.
/// Returns the thread handle and a readiness oneshot (Ok once bound, Err on a
/// bind/runtime failure) so startup can gate on it like the ingest thread.
pub fn spawn_price_api_thread(
    cfg: PriceApiConfig,
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    feed_rx: mpsc::UnboundedReceiver<BookBatch>,
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    tokens: Vec<RegisteredTokenRow>,
    markets: MarketPlan,
    cancel: CancellationToken,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let handle = thread::Builder::new()
        .name("price-api".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(cfg.workers)
                .max_blocking_threads(cfg.workers)
                .thread_name("price-api-worker")
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(anyhow!("price-api runtime: {e}")));
                    return;
                }
            };
            rt.block_on(async move {
                let (book_tx, book_rx) = watch::channel(Arc::new(SwapBookSnapshot::default()));
                let (remembered_tx, remembered_rx) =
                    watch::channel(Arc::new(RememberedPrices::new()));
                tokio::spawn(mirror_depth(
                    feed_rx,
                    book_tx,
                    remembered_tx,
                    prices.clone(),
                    cfg.clearing_fee_ppm,
                ));
                let state = PriceApiState::new(
                    &cfg,
                    prices,
                    tokens,
                    &markets,
                    book_rx,
                    remembered_rx,
                    stats_rx,
                );
                let app = build_app(state, &cfg);

                let addr: SocketAddr = match format!("{}:{}", cfg.bind, cfg.port).parse() {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow!("price-api bind address: {e}")));
                        return;
                    }
                };
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow!("price-api bind {addr}: {e}")));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                let max_connections =
                    cap_for_open_file_limit(cfg.max_connections, raise_open_file_limit());
                MAX_CONNECTIONS.store(max_connections as u64, Ordering::Relaxed);
                tracing::info!(
                    %addr,
                    workers = cfg.workers,
                    max_connections,
                    "price-query API listening"
                );
                let limits = ConnectionLimits {
                    max_connections,
                    header_timeout: Duration::from_millis(cfg.header_timeout_ms),
                    write_stall_timeout: WRITE_STALL_TIMEOUT,
                };
                // On a worker, so the accept loop counts against
                // `price_query_workers` too: this thread only waits.
                let server = tokio::spawn(serve(listener, app, limits, cancel));
                if let Err(error) = server.await {
                    tracing::error!(%error, "price-query API accept loop failed");
                }
            });
        })
        .context("spawn price-api thread")?;
    Ok((handle, ready_rx))
}

/// Per-connection buffer for reading a request (hyper's floor is 8 KiB): our
/// requests are a short GET line and a few headers, so this bounds what one
/// connection can hold. `engine.price_query_max_batch` is capped so a full
/// `/v1/prices?ids=` request fits.
const READ_BUFFER_BYTES: usize = 16 * 1024;
/// How long shutdown waits for open connections to finish their requests.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// How long a write may make no progress before its connection is dropped.
/// hyper times out only the wait for a request's headers, so a client that
/// sends requests and never reads the answers would otherwise hold its
/// connection slot forever.
const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// File descriptors kept free for the rest of the solver when the API is
/// full: the database pool and writer, node and prover connections, Binance
/// websockets, SQLite files, the other listeners and the runtimes' own
/// handles, with room to spare.
const FD_RESERVE: u64 = 256;
/// How long accepting pauses after an error that is not one connection's
/// (out of file descriptors, for example), as axum::serve does.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_secs(1);

/// Connections the API holds open now, connections closed at accept because
/// the cap was reached, and the cap in force (after [`cap_for_open_file_limit`]).
static OPEN_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static REFUSED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static MAX_CONNECTIONS: AtomicU64 = AtomicU64::new(0);

/// `(open, refused since start, cap in force)` connections, for `/metrics`.
pub(crate) fn connection_counts() -> (u64, u64, u64) {
    (
        OPEN_CONNECTIONS.load(Ordering::Relaxed),
        REFUSED_CONNECTIONS.load(Ordering::Relaxed),
        MAX_CONNECTIONS.load(Ordering::Relaxed),
    )
}

/// Raise this process's soft open-file limit as far as its hard limit allows
/// and return the soft limit now in force, or `None` if it cannot be read.
/// Every connection holds a descriptor, and the whole process shares the
/// limit, so it decides how many public connections fit beside the solver's
/// own.
#[cfg(unix)]
fn raise_open_file_limit() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes one rlimit through a valid pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return None;
    }
    // macOS refuses values above its per-process maximum even when the hard
    // limit is unlimited, so try smaller targets in turn.
    for target in [limit.rlim_max.min(65_536), 32_768, 10_240] {
        if target <= limit.rlim_cur {
            break;
        }
        let raised = libc::rlimit {
            rlim_cur: target,
            rlim_max: limit.rlim_max,
        };
        // SAFETY: setrlimit reads one rlimit through a valid pointer.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            limit.rlim_cur = target;
            break;
        }
    }
    Some(limit.rlim_cur)
}

#[cfg(not(unix))]
fn raise_open_file_limit() -> Option<u64> {
    None
}

/// The connection cap that fits under the open-file limit: `configured`, or
/// the limit less [`FD_RESERVE`] when that is smaller (logged), so public
/// connections can never take the descriptors the matcher, executor and
/// database pool need.
fn cap_for_open_file_limit(configured: usize, open_file_limit: Option<u64>) -> usize {
    let Some(limit) = open_file_limit else {
        return configured;
    };
    let room = usize::try_from(limit.saturating_sub(FD_RESERVE))
        .unwrap_or(usize::MAX)
        .max(1);
    if configured <= room {
        return configured;
    }
    tracing::warn!(
        configured,
        open_file_limit = limit,
        cap = room,
        "price-query API connection cap lowered to fit the open-file limit"
    );
    room
}

#[derive(Clone, Copy, Debug)]
struct ConnectionLimits {
    max_connections: usize,
    header_timeout: Duration,
    write_stall_timeout: Duration,
}

/// One open connection, counted while it lives.
struct OpenConnection;

impl OpenConnection {
    fn new() -> Self {
        OPEN_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for OpenConnection {
    fn drop(&mut self) {
        OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A socket whose writes and flushes fail with `TimedOut` once they have
/// made no progress for `limit`; any progress restarts the clock. Reads pass
/// through untouched (hyper's header timeout covers them).
struct WriteStallTimeout<S> {
    inner: S,
    limit: Duration,
    stalled: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<S> WriteStallTimeout<S> {
    fn new(inner: S, limit: Duration) -> Self {
        Self {
            inner,
            limit,
            stalled: None,
        }
    }
}

/// `poll` from a write-side call of [`WriteStallTimeout`]: ready clears the
/// stall clock; pending starts it, and fails the call once it runs out.
fn within_stall_limit<T>(
    stalled: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    limit: Duration,
    cx: &mut task::Context<'_>,
    poll: Poll<io::Result<T>>,
) -> Poll<io::Result<T>> {
    if poll.is_ready() {
        *stalled = None;
        return poll;
    }
    let clock = stalled.get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
    match clock.as_mut().poll(cx) {
        Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the client stopped reading",
        ))),
        Poll::Pending => Poll::Pending,
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteStallTimeout<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteStallTimeout<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        within_stall_limit(&mut this.stalled, this.limit, cx, poll)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        within_stall_limit(&mut this.stalled, this.limit, cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        within_stall_limit(&mut this.stalled, this.limit, cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        within_stall_limit(&mut this.stalled, this.limit, cx, poll)
    }
}

/// An accept error that belongs to one connection (its client gave up before
/// accept), not to the listener: accepting goes on at once, as axum::serve does.
fn is_connection_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// Accept connections and serve `app` on each, HTTP/1.1 only, until `cancel`;
/// then stop listening and let open requests finish for at most
/// [`SHUTDOWN_GRACE`] (connections still open after it end with the runtime).
/// A connection over `limits.max_connections` is closed at once and counted,
/// so a flood of connections costs a file descriptor each, briefly, not a task.
async fn serve(
    listener: TcpListener,
    app: Router,
    limits: ConnectionLimits,
    cancel: CancellationToken,
) {
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    let graceful = GracefulShutdown::new();
    loop {
        let stream = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => stream,
                Err(error) if is_connection_error(&error) => continue,
                Err(error) => {
                    // Out of file descriptors, for example: pause instead of
                    // spinning on the same error.
                    tracing::error!(%error, "price-query API accept failed");
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        () = tokio::time::sleep(ACCEPT_ERROR_PAUSE) => continue,
                    }
                }
            },
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            REFUSED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
            drop(stream);
            continue;
        };
        let _ = stream.set_nodelay(true);
        let io = TokioIo::new(WriteStallTimeout::new(stream, limits.write_stall_timeout));
        let connection = http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(limits.header_timeout)
            .max_buf_size(READ_BUFFER_BYTES)
            .serve_connection(io, TowerToHyperService::new(app.clone()));
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            let _slot = slot;
            let _open = OpenConnection::new();
            if let Err(error) = connection.await {
                tracing::debug!(%error, "price-query API connection ended");
            }
        });
    }
    // New connections are refused from here on, not left in the backlog.
    drop(listener);
    let open = graceful.count();
    if tokio::time::timeout(SHUTDOWN_GRACE, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(
            open,
            "price-query API connections still open at shutdown; they end with the runtime"
        );
    }
}

#[cfg(test)]
mod tests;

//! Public, read-only **price-query HTTP API** for swap UIs: a token's price by
//! faucet id (`/v1/price`, `/v1/prices`), a pair's clearing price to build an
//! ask from (`/v2/pair-price`), and what the matcher would do with an order the
//! wallet is about to sign (`/v2/swap-eta`, see [`crate::swap_eta`]).
//! `/v1/swap-eta` keeps its original answers for wallets built against it.
//!
//! ISOLATION (P0): this is a public, unauthenticated surface, so it runs on its
//! OWN OS thread + multi-thread runtime. It never touches the `!Send` miden
//! `Client` or the matcher's single-threaded `LocalSet`, so wallet traffic
//! cannot starve fund settlement. Protections: a tokio-`Semaphore` concurrency
//! limiter (sheds excess with `503`), request timeout, body cap, batch cap.
//!
//! Prices come from the Binance snapshot ([`crate::price::PriceSnapshot`]): a
//! token is worth the exact midpoint of its `<ASSET><QUOTE>` market, usable
//! while younger than the quote TTL. The registered tokens, with their
//! decimals and tickers, are read from the DB once at startup and kept in
//! memory, so public traffic never touches PostgreSQL; a registration change
//! shows after a restart. The book depth for swap quotes comes from
//! [`mirror_depth`], which this thread runs: the matcher only sends changes.
//! `/v2/pairs` lists the configured clearing pairs and their tokens, built
//! once at startup.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use miden_protocol::account::AccountId;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;

use crate::clearing::{BatchPrice, OrderSide};
use crate::db::postgres_models::RegisteredTokenRow;
use crate::matching::types::{BookLevel, SwapBookSnapshot};
use crate::price::PricePrecision;
use crate::price::{MarketPlan, OrderMarket, PriceSnapshot, PriceUnavailable, Valued};
use crate::swap_eta::{
    eval_can_fill, eval_off_market, expected_price, fill_price, judge, whole_price, DepthBook,
    DepthChange, FillStatus, NoFillReason, PriceBand, Probe, ProposedOrder, SettlementStats,
    VerdictTerms,
};

/// Knobs for the price-query server (sourced from `EngineConfig` and `BinanceConfig`).
#[derive(Debug, Clone)]
pub struct PriceApiConfig {
    pub bind: String,
    pub port: u16,
    pub max_inflight: usize,
    pub max_batch: usize,
    pub timeout_ms: u64,
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
    /// In-memory settlement-time window from the executor.
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    /// Next-batch ETA (secs) = ceil((sync + trigger + proving + block)/1000).
    swap_eta_secs: u64,
    verdict_terms: VerdictTerms,
}

impl PriceApiState {
    pub fn new(
        cfg: &PriceApiConfig,
        prices: watch::Receiver<Arc<PriceSnapshot>>,
        tokens: Vec<RegisteredTokenRow>,
        markets: &MarketPlan,
        book_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
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
    let confirmed = state
        .prices
        .borrow()
        .markets()
        .clearing_pairs()
        .next()
        .is_some();
    if !confirmed && !state.pairs.pairs.is_empty() {
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

/// The expected batch price, or the mid if the walk fails (an overflow on
/// extreme amounts): what the matcher clears at in that case too.
fn walked_or_mid(
    market: &OrderMarket,
    fee_ppm: u32,
    sells: &[BookLevel],
    buys: &[BookLevel],
    probe: Probe,
) -> BatchPrice {
    expected_price(market, fee_ppm, sells, buys, probe).unwrap_or_else(|error| {
        tracing::debug!(%error, "bounded price failed; quoting at the mid");
        market.price
    })
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
    let book = state.book_rx.borrow().clone();
    let (sells, buys) = pair_sides(&book, market.side, a, b);
    let price = walked_or_mid(&market, fee_ppm, sells, buys, Probe::Reference);
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

/// `GET /v2/swap-eta?offered_faucet=&offered_amount=&requested_faucet=&requested_amount=&min_fill_step=`
///
/// The [`crate::swap_eta::Verdict`] on the order a wallet is about to sign (offer A / request
/// B, base units), plus the time estimates.
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
    // 404 unless both are registered.
    registered_decimals(&state, a, b)?;

    // One price snapshot, one book snapshot, one clock reading.
    let book = state.book_rx.borrow().clone();
    let terms = state.verdict_terms;
    let market = state.prices.borrow().order_market(a, b, Instant::now());
    let (verdict, market_price, expected, fill, unpriced) = match market {
        Ok(market) => {
            let cannot_price = |error| ApiError::BadAmount(format!("cannot price: {error}"));
            let levels = |pair| book.get(&pair).map_or(&[][..], Vec::as_slice);
            // The batch price with this order in the book (ADR 0007); the
            // order is judged at it, as the matcher would clear it.
            let (sells, buys) = pair_sides(&book, market.side, a, b);
            let price = walked_or_mid(&market, terms.fee_ppm, sells, buys, Probe::Order(order));
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

    Ok(no_store(Json(SwapEtaResponse {
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
    })))
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

/// Mirror the matcher's book depth from its changes and publish it for the
/// quote handlers, at most every [`DEPTH_PUBLISH_INTERVAL`]. Runs on this
/// thread: the matcher only sends the changes its index already made.
async fn mirror_depth(
    mut changes: mpsc::UnboundedReceiver<DepthChange>,
    book_tx: watch::Sender<Arc<SwapBookSnapshot>>,
) {
    let mut depth = DepthBook::default();
    while let Some(change) = changes.recv().await {
        depth.apply(change);
        depth.drain(&mut changes);
        book_tx.send_replace(Arc::new(depth.snapshot()));
        tokio::time::sleep(DEPTH_PUBLISH_INTERVAL).await;
    }
}

/// Concurrency limiter: acquire a permit per request, shed with `503` if none.
async fn concurrency_guard(
    State(sem): State<Arc<Semaphore>>,
    req: Request,
    next: Next,
) -> Response {
    match sem.try_acquire_owned() {
        Ok(_permit) => next.run(req).await,
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "overloaded", "message": "price API at capacity" })),
        )
            .into_response(),
    }
}

// ── Router + server ──────────────────────────────────────────────────────────

/// Build the full router (used by the server thread AND unit tests).
pub fn build_app(state: PriceApiState, cfg: &PriceApiConfig) -> Router {
    let sem = Arc::new(Semaphore::new(cfg.max_inflight.max(1)));
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
        // Outer protections (applied to all routes):
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            cache_value,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(cfg.timeout_ms),
        ))
        .layer(DefaultBodyLimit::max(4 * 1024))
        .layer(from_fn_with_state(sem, concurrency_guard))
        // CORS is outermost: it answers preflight + tags every response
        // (including 503/timeout) without consuming a concurrency permit.
        .layer(cors)
}

/// Spawn the price-query server on its OWN OS thread + multi-thread runtime.
/// Returns the thread handle and a readiness oneshot (Ok once bound, Err on a
/// bind/runtime failure) so startup can gate on it like the ingest thread.
pub fn spawn_price_api_thread(
    cfg: PriceApiConfig,
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    depth_rx: mpsc::UnboundedReceiver<DepthChange>,
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
                tokio::spawn(mirror_depth(depth_rx, book_tx));
                let state = PriceApiState::new(&cfg, prices, tokens, &markets, book_rx, stats_rx);
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
                tracing::info!(%addr, "price-query API listening");
                let shutdown = async move { cancel.cancelled().await };
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown)
                    .await
                {
                    tracing::error!(error = %e, "price-query API server error");
                }
            });
        })
        .context("spawn price-api thread")?;
    Ok((handle, ready_rx))
}

#[cfg(test)]
mod tests;

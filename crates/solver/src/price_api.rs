//! Public, read-only **price-query HTTP API** — wallets fetch a token's current
//! price by faucet id (for swap UIs).
//!
//! ISOLATION (P0): this is a public, unauthenticated surface, so it runs on its
//! OWN OS thread + multi-thread runtime. It never touches the `!Send` miden
//! `Client` or the matcher's single-threaded `LocalSet`, so wallet traffic
//! cannot starve fund settlement. Protections: a tokio-`Semaphore` concurrency
//! limiter (sheds excess with `503`), request timeout, body cap, batch cap.
//!
//! Prices come from the Binance snapshot ([`crate::price::PriceSnapshot`]): a
//! token is worth the exact midpoint of its `<ASSET><QUOTE>` market, usable
//! while younger than the quote TTL. Decimals + ticker come from the DB
//! (fetched on-chain by ingest).

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
use miden_protocol::crypto::utils::Serializable;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;

use crate::db::postgres_models::RegisteredTokenRow;
use crate::db::{self, DbPool};
use crate::matching::types::SwapBookSnapshot;
use crate::price::PricePrecision;
use crate::price::{PriceSnapshot, PriceUnavailable, Valued};
use crate::swap_eta::{
    fill_price, quote, DepthBook, DepthChange, FillStatus, NoFillReason, PriceBand, QuoteOrder,
    QuoteTerms, SettlementStats,
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
    /// How far (bps) the price may still have to move an order's way for
    /// `/v1/swap-eta` to call it `tolerated` rather than `off_market`.
    pub swap_offmarket_tol_bps: u64,
    /// The matcher's clearing fee, so quotes apply the same rule.
    pub clearing_fee_ppm: u32,
}

/// Shared (Send+Sync) state — no `!Send` client, so it lives on its own thread.
#[derive(Clone)]
pub struct PriceApiState {
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    pool: DbPool,
    vs_currency: String,
    default_precision: PricePrecision,
    max_batch: usize,
    // ── swap-eta ──
    /// The matcher's resting levels, from [`mirror_depth`] (read lock-free).
    swap_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
    /// In-memory settlement-time window from the executor.
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    /// Next-batch ETA (secs) = ceil((sync + trigger + proving + block)/1000).
    swap_eta_secs: u64,
    quote_terms: QuoteTerms,
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
    /// Token's on-chain decimals (null until ingest has fetched it).
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
    Stale(i64),
    BadPrecision(String),
    BatchTooLarge(usize),
    Internal,
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
                "no Binance market is configured for this token; it has no price until the \
                 solver's configuration changes"
                    .into(),
            ),
            ApiError::NoPrice => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no_price",
                "no valid price for this token right now".into(),
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
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal error".into(),
            ),
        };
        (status, Json(json!({ "error": code, "message": message }))).into_response()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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
    row: RegisteredTokenRow,
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
        match reason {
            PriceUnavailable::NoMarket => ApiError::NoMarket,
            _ => ApiError::NoPrice,
        }
    })?;
    let age = received_at.map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
    let as_of = now_secs().saturating_sub(i64::try_from(age.as_secs()).unwrap_or(i64::MAX));
    if !fresh && !allow_stale {
        return Err(ApiError::Stale(as_of));
    }
    let decimals = row.token_decimals();
    Ok(PriceResponse {
        faucet_id: account_id.to_hex(),
        ticker: row.ticker,
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
    let row = token_rows(&state, vec![account_id])
        .await?
        .remove(&account_id)
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
    let mut rows = token_rows(&state, accounts.clone()).await?;
    let snapshot = state.prices.borrow().clone();
    let mut out = HashMap::new();
    for account_id in accounts {
        let Some(row) = rows.remove(&account_id) else {
            continue;
        };
        if let Ok(resp) = quote_from_row(&state, &snapshot, account_id, row, precision, allow_stale)
        {
            out.insert(resp.faucet_id.clone(), resp);
        }
    }
    Ok(Json(out))
}

// ── swap-eta ─────────────────────────────────────────────────────────────────

/// Response for `GET /v1/pair-price`: the price a wallet builds its ask from.
/// Prices are whole requested tokens per whole offered token.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PairPriceResponse {
    offered_faucet: String,
    requested_faucet: String,
    /// The Binance mid of the pair's clearing market.
    market_price: String,
    /// The mid after the clearing fee, as this side pays it: the best ask
    /// that fills now.
    fill_price: String,
    fee_ppm: u32,
    /// On-chain decimals, to turn whole-token prices into base units; `null`
    /// until ingest has fetched them.
    offered_decimals: Option<u8>,
    requested_decimals: Option<u8>,
    /// Unix secs the solver received this quote.
    as_of: i64,
}

/// Response for `GET /v1/swap-eta`: what the matcher would do with the order
/// now (see [`crate::swap_eta::quote`]), plus time estimates. Amounts are base
/// units, prices whole requested tokens per whole offered token. Optional
/// fields serialise as `null` (stable shape for the wallet), never omitted.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapEtaResponse {
    offered_faucet: String,
    requested_faucet: String,
    /// The order, as asked.
    offered_amount: String,
    requested_amount: String,
    /// `at_market`, `tolerated` or `off_market`; `null` without a fresh price.
    price_band: Option<PriceBand>,
    /// `full`, `partial` or `none`.
    fill_status: FillStatus,
    /// Why `none`: `price`, `liquidity`, `no_market` or `no_price`.
    reason: Option<NoFillReason>,
    /// How much of the order the book fills now.
    fillable_offered_amount: String,
    fillable_requested_amount: String,
    /// How much of the offered token the book takes at this order's price,
    /// not capped by its size; `null` off market or without a price.
    available_offered_amount: Option<String>,
    /// What a full fill pays now: market value minus the fee, at least the ask.
    expected_requested_amount: Option<String>,
    fee_ppm: u32,
    /// The fee on a full fill now, in the requested token.
    fee_amount: Option<String>,
    /// The Binance mid.
    market_price: Option<String>,
    /// The mid after the fee: what a full fill pays.
    fill_price: Option<String>,
    /// Whether the solver is settling: `false` before its executor starts and
    /// while it is in verification mode (no fee headroom, node or database down).
    accepting_orders: bool,
    /// `fill_status == full` at the market price (kept for older wallets).
    can_fill: bool,
    /// `price_band == off_market` (kept for older wallets).
    off_market: Option<bool>,
    /// Next-batch ETA (secs) for an order the book fills at the market price
    /// while the solver is settling; otherwise `null`.
    estimated_seconds: Option<u64>,
    /// In-memory rolling per-pair median settlement secs; `null` when no samples.
    median24h_seconds: Option<u64>,
}

/// Registered-token rows for `tokens`, in one read; unregistered ones are
/// absent. Any database failure is a 500.
async fn token_rows(
    state: &PriceApiState,
    tokens: Vec<AccountId>,
) -> Result<HashMap<AccountId, RegisteredTokenRow>, ApiError> {
    let keys: Vec<_> = tokens.iter().map(Serializable::to_bytes).collect();
    let mut rows = state
        .pool
        .read_public(move |conn| db::postgres_db::fetch_token_rows_tx(conn, &keys))
        .await
        .map_err(|_| ApiError::Internal)?;
    Ok(tokens
        .into_iter()
        .filter_map(|token| Some((token, rows.remove(&token.to_bytes())?)))
        .collect())
}

/// A required positive base-unit amount.
fn required_amount(q: &HashMap<String, String>, key: &str) -> Result<u64, ApiError> {
    parse_amount(q, key)?.ok_or_else(|| ApiError::BadAmount(format!("missing `{key}`")))
}

/// An optional positive base-unit amount.
fn parse_amount(q: &HashMap<String, String>, key: &str) -> Result<Option<u64>, ApiError> {
    let Some(raw) = q.get(key) else {
        return Ok(None);
    };
    let v: u64 = raw
        .parse()
        .map_err(|_| ApiError::BadAmount(format!("`{key}` must be a u64, got {raw:?}")))?;
    if v == 0 {
        return Err(ApiError::BadAmount(format!("`{key}` must be > 0")));
    }
    Ok(Some(v))
}

fn parse_faucet(q: &HashMap<String, String>, key: &str) -> Result<AccountId, ApiError> {
    let raw = q
        .get(key)
        .ok_or_else(|| ApiError::BadFaucetId(format!("missing `{key}`")))?;
    AccountId::from_hex(raw).map_err(|e| ApiError::BadFaucetId(format!("`{key}`: {e}")))
}

/// The two faucets of a pair request, distinct and both registered.
async fn pair_rows(
    state: &PriceApiState,
    q: &HashMap<String, String>,
) -> Result<(AccountId, AccountId, HashMap<AccountId, RegisteredTokenRow>), ApiError> {
    let a = parse_faucet(q, "offered_faucet")?;
    let b = parse_faucet(q, "requested_faucet")?;
    if a == b {
        return Err(ApiError::BadRequest(
            "offered_faucet and requested_faucet must differ".into(),
        ));
    }
    let rows = token_rows(state, vec![a, b]).await?;
    if !rows.contains_key(&a) || !rows.contains_key(&b) {
        return Err(ApiError::UnknownFaucet);
    }
    Ok((a, b, rows))
}

/// `GET /v1/pair-price?offered_faucet=&requested_faucet=`
///
/// The pair's clearing price, under the same freshness rule and fee as the
/// matcher. The wallet builds its ask from `fill_price` and the user's
/// slippage, then asks `/v1/swap-eta` about that exact order.
async fn get_pair_price(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<PairPriceResponse>, ApiError> {
    let (a, b, rows) = pair_rows(&state, &q).await?;
    let prices = state.prices.borrow().clone();
    let now = Instant::now();
    let unavailable = |reason| match reason {
        PriceUnavailable::NoMarket => ApiError::NoMarket,
        _ => ApiError::NoPrice,
    };
    let (side, _) = prices.order_price(a, b, now).map_err(unavailable)?;
    let (market, received_at) = prices.market_quote(a, b, now).map_err(unavailable)?;
    let fee_ppm = state.quote_terms.fee_ppm;
    let fill = fill_price(side, market, fee_ppm).ok_or(ApiError::NoPrice)?;
    let age = now.saturating_duration_since(received_at);
    let decimals = |token| {
        rows.get(&token)
            .and_then(RegisteredTokenRow::token_decimals)
    };
    Ok(Json(PairPriceResponse {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        market_price: PricePrecision::Full.format(market),
        fill_price: PricePrecision::Full.format(fill),
        fee_ppm,
        offered_decimals: decimals(a),
        requested_decimals: decimals(b),
        as_of: now_secs().saturating_sub(i64::try_from(age.as_secs()).unwrap_or(i64::MAX)),
    }))
}

/// `GET /v1/swap-eta?offered_faucet=&offered_amount=&requested_faucet=&requested_amount=&min_fill_step=`
///
/// Judge the order a wallet is about to sign (offer A / request B, base
/// units): where its price sits, how much of it the live book fills, and the
/// time estimates.
async fn get_swap_eta(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let order = QuoteOrder {
        offered: required_amount(&q, "offered_amount")?,
        requested: required_amount(&q, "requested_amount")?,
        min_fill_step: parse_amount(&q, "min_fill_step")?,
    };
    let (a, b, _) = pair_rows(&state, &q).await?;

    // One price snapshot, one book snapshot, one clock reading.
    let prices = state.prices.borrow().clone();
    let book = state.swap_rx.borrow().clone();
    let now = Instant::now();
    let terms = state.quote_terms;
    let market = prices.market_price(a, b, now).ok();
    let (quoted, unpriced) = match prices.order_price(a, b, now) {
        Ok((side, price)) => {
            let levels = |pair| book.get(&pair).map_or(&[][..], Vec::as_slice);
            let quoted = quote(side, price, terms, order, levels((a, b)), levels((b, a)))
                .map_err(|error| ApiError::BadAmount(format!("cannot price: {error}")))?;
            (Some((side, quoted)), None)
        }
        // Without a price nothing fills.
        Err(PriceUnavailable::NoMarket) => (None, Some(NoFillReason::NoMarket)),
        Err(_) => (None, Some(NoFillReason::NoPrice)),
    };
    let quote = quoted.map(|(_, quote)| quote);
    let band = quote.map(|quote| quote.band);
    let status = quote.map_or(FillStatus::None, |quote| quote.status);
    let fill = quoted
        .zip(market)
        .and_then(|((side, _), market)| fill_price(side, market, terms.fee_ppm));
    let price = |value| PricePrecision::Full.format(value);
    let amount = |value: Option<u64>| value.map(|value| value.to_string());

    // Median — same direction (A → B) the note settles as; purely in-memory.
    let now_unix = now_secs().max(0) as u64;
    let (median24h_seconds, settling) = {
        let stats = state.stats_rx.borrow();
        (stats.median_secs((a, b), now_unix), stats.settling)
    };

    let body = Json(SwapEtaResponse {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        offered_amount: order.offered.to_string(),
        requested_amount: order.requested.to_string(),
        price_band: band,
        fill_status: status,
        reason: quote.map_or(unpriced, |quote| quote.reason),
        fillable_offered_amount: quote.map_or(0, |q| q.fillable_offered).to_string(),
        fillable_requested_amount: quote.map_or(0, |q| q.fillable_requested).to_string(),
        available_offered_amount: amount(quote.and_then(|q| q.available_offered)),
        expected_requested_amount: amount(quote.map(|q| q.expected_requested)),
        fee_ppm: terms.fee_ppm,
        fee_amount: amount(quote.map(|q| q.fee)),
        market_price: market.map(price),
        fill_price: fill.map(price),
        accepting_orders: settling,
        can_fill: band == Some(PriceBand::AtMarket) && status == FillStatus::Full,
        off_market: band.map(|band| band == PriceBand::OffMarket),
        estimated_seconds: (band == Some(PriceBand::AtMarket)
            && status != FillStatus::None
            && settling)
            .then_some(state.swap_eta_secs),
        median24h_seconds,
    });
    // Don't let the router-level `max-age` layer cache this: the quote comes
    // from independently-updated snapshots, so a shared max-age would serve a
    // stale fill. `no-store` wins because the layer is `if_not_present`.
    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        body,
    ))
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
        while let Ok(change) = changes.try_recv() {
            depth.apply(change);
        }
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
    pool: DbPool,
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
                let (book_tx, swap_rx) = watch::channel(Arc::new(SwapBookSnapshot::default()));
                tokio::spawn(mirror_depth(depth_rx, book_tx));
                let default_precision =
                    PricePrecision::parse(&cfg.precision).unwrap_or(PricePrecision::Full);
                let swap_eta_secs = (cfg.swap_sync_ms
                    + cfg.swap_matching_trigger_ms
                    + cfg.swap_proving_ms
                    + cfg.swap_block_ms)
                    .div_ceil(1000);
                let state = PriceApiState {
                    prices,
                    pool,
                    vs_currency: cfg.vs_currency.clone(),
                    default_precision,
                    max_batch: cfg.max_batch,
                    swap_rx,
                    stats_rx,
                    swap_eta_secs,
                    quote_terms: QuoteTerms {
                        fee_ppm: cfg.clearing_fee_ppm,
                        tolerance_bps: cfg.swap_offmarket_tol_bps,
                    },
                };
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

//! Public, read-only **price-query HTTP API** — wallets fetch a token's current
//! price by faucet id (for swap UIs).
//!
//! ISOLATION (P0): this is a public, unauthenticated surface, so it runs on its
//! OWN OS thread + multi-thread runtime. It never touches the `!Send` miden
//! `Client` or the matcher's single-threaded `LocalSet`, so wallet traffic
//! cannot starve fund settlement. Protections: a tokio-`Semaphore` concurrency
//! limiter (sheds excess with `503`), request timeout, body cap, batch cap.
//!
//! Prices come from the precise side-channel ([`crate::price::PreciseSnapshot`],
//! full CoinGecko precision); decimals + ticker from the DB (fetched on-chain by
//! ingest). The matcher's integer-cents path is untouched.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use tokio::sync::{oneshot, watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;

use crate::config::PricePrecision;
use crate::db::{self, DbPool};
use crate::matching::types::SwapBookSnapshot;
use crate::price::PreciseSnapshot;
use crate::swap_eta::{eval_can_fill, eval_off_market, SettlementStats};

/// Knobs for the price-query server (sourced from `EngineConfig`).
#[derive(Debug, Clone)]
pub struct PriceApiConfig {
    pub bind: String,
    pub port: u16,
    pub max_inflight: usize,
    pub max_batch: usize,
    pub timeout_ms: u64,
    pub vs_currency: String,
    /// Default precision (`"full"` or `"0".."18"`); overridable per request.
    pub precision: String,
    pub staleness_secs: u64,
    /// Used for the `Cache-Control: max-age` of responses.
    pub price_interval_ms: u64,
    // ── swap-eta terms ──
    /// Matcher tick interval (ms) — a term of the next-batch ETA.
    pub swap_matching_trigger_ms: u64,
    /// Ingest sync-poll interval (ms) — the pre-matcher delay, a term of the ETA.
    pub swap_sync_ms: u64,
    /// Estimated proving time (ms) — a term of the ETA.
    pub swap_proving_ms: u64,
    /// Estimated block time (ms) — a term of the ETA.
    pub swap_block_ms: u64,
    /// Slack (bps) before flagging `offMarket`.
    pub swap_offmarket_tol_bps: u64,
}

/// Shared (Send+Sync) state — no `!Send` client, so it lives on its own thread.
#[derive(Clone)]
pub struct PriceApiState {
    precise_rx: watch::Receiver<PreciseSnapshot>,
    pool: DbPool,
    last_price_update: Arc<AtomicI64>,
    vs_currency: String,
    default_precision: PricePrecision,
    staleness_secs: i64,
    max_batch: usize,
    // ── swap-eta ──
    /// Top-of-book snapshot from the matcher (read lock-free).
    swap_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
    /// In-memory settlement-time window from the executor.
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    /// Next-batch ETA (secs) = ceil((sync + trigger + proving + block)/1000).
    swap_eta_secs: u64,
    swap_offmarket_tol_bps: u64,
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
    /// Unix secs of the last successful price refresh.
    as_of: i64,
    stale: bool,
    source: String,
}

enum ApiError {
    BadFaucetId(String),
    BadAmount(String),
    BadRequest(String),
    UnknownFaucet,
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
            ApiError::UnknownFaucet => {
                (StatusCode::NOT_FOUND, "unknown_faucet", "faucet not registered".into())
            }
            ApiError::NoPrice => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no_price",
                "no price for this token yet".into(),
            ),
            ApiError::Stale(as_of) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "stale",
                format!("prices are stale; last update {as_of} (use ?allow_stale=true to override)"),
            ),
            ApiError::BadPrecision(m) => (StatusCode::BAD_REQUEST, "bad_precision", m),
            ApiError::BatchTooLarge(max) => (
                StatusCode::BAD_REQUEST,
                "batch_too_large",
                format!("at most {max} ids per request"),
            ),
            ApiError::Internal => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error".into())
            }
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

fn format_price(usd: f64, p: PricePrecision) -> String {
    match p {
        // Shortest round-trippable representation (preserves CoinGecko's value).
        PricePrecision::Full => format!("{usd}"),
        PricePrecision::Fixed(n) => format!("{usd:.*}", n as usize),
    }
}

fn resolve_precision(
    state: &PriceApiState,
    q: &HashMap<String, String>,
) -> Result<PricePrecision, ApiError> {
    match q.get("precision") {
        Some(p) => PricePrecision::parse(p)
            .ok_or_else(|| ApiError::BadPrecision(format!("must be \"full\" or 0..=18, got {p:?}"))),
        None => Ok(state.default_precision),
    }
}

fn wants_stale(q: &HashMap<String, String>) -> bool {
    q.get("allow_stale").map(|v| v == "true" || v == "1").unwrap_or(false)
}

/// Current snapshot age. Returns `(as_of, is_stale)`.
fn staleness(state: &PriceApiState) -> (i64, bool) {
    let as_of = state.last_price_update.load(Ordering::Relaxed);
    (as_of, now_secs().saturating_sub(as_of) > state.staleness_secs)
}

/// Resolve one faucet → quote. `404` if unregistered, `503` if registered but
/// unpriced. Staleness is gated by the caller (it's a global property).
fn quote_one(
    state: &PriceApiState,
    faucet_hex: &str,
    precision: PricePrecision,
    as_of: i64,
    stale: bool,
) -> Result<PriceResponse, ApiError> {
    let account_id =
        AccountId::from_hex(faucet_hex).map_err(|e| ApiError::BadFaucetId(format!("{e}")))?;

    // Registered? (DB is the source of truth for the registered set + decimals.)
    let mut key = Vec::new();
    account_id.write_into(&mut key);
    let row = db::fetch_token_row(&state.pool, &key)
        .map_err(|_| ApiError::Internal)?
        .ok_or(ApiError::UnknownFaucet)?;

    // Price from the precise side-channel.
    let usd = {
        let snap = state.precise_rx.borrow();
        snap.get(&account_id).map(|d| d.usd)
    }
    .ok_or(ApiError::NoPrice)?;

    Ok(PriceResponse {
        faucet_id: account_id.to_hex(),
        ticker: row.ticker,
        vs_currency: state.vs_currency.clone(),
        price: format_price(usd, precision),
        precision: precision_label(precision),
        decimals: row.decimals.map(|d| d as u8),
        as_of,
        stale,
        source: "coingecko".to_string(),
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
    let (as_of, stale) = staleness(&state);
    if stale && !wants_stale(&q) {
        return Err(ApiError::Stale(as_of));
    }
    Ok(Json(quote_one(&state, &faucet_id, precision, as_of, stale)?))
}

/// `GET /v1/prices?ids=a,b,c&precision=&allow_stale=` → `{ "<faucet_id>": {..} }`
/// (CoinGecko-style: unknown/unpriced ids are omitted).
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
    let (as_of, stale) = staleness(&state);
    if stale && !wants_stale(&q) {
        return Err(ApiError::Stale(as_of));
    }
    let mut out = HashMap::new();
    for id in ids {
        if let Ok(resp) = quote_one(&state, id, precision, as_of, stale) {
            out.insert(resp.faucet_id.clone(), resp);
        }
    }
    Ok(Json(out))
}

// ── swap-eta ─────────────────────────────────────────────────────────────────

/// Response for `GET /v1/swap-eta`. Two independent liquidity signals — the live
/// book (`can_fill` + `estimated_seconds`) and the real-time oracle (`off_market`
/// + `market_price`) — plus the historical in-memory median. All optional fields
/// serialise as `null` (stable shape for the wallet), never omitted.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapEtaResponse {
    offered_faucet: String,
    requested_faucet: String,
    offered_amount: String,
    requested_amount: String,
    /// Book: crosses the best opposite-pair rate AND that level has the depth.
    can_fill: bool,
    /// Oracle: order priced worse than market (why it won't fill). `null` if unpriced.
    off_market: Option<bool>,
    /// Next-batch ETA (secs); `null` when `can_fill` is false.
    estimated_seconds: Option<u64>,
    /// Oracle fair rate (requested-per-offered); `null` if unpriced.
    market_price: Option<String>,
    /// In-memory rolling per-pair median settlement secs; `null` when no samples.
    median24h_seconds: Option<u64>,
}

fn key_bytes(id: AccountId) -> Vec<u8> {
    let mut k = Vec::new();
    id.write_into(&mut k);
    k
}

fn parse_amount(q: &HashMap<String, String>, key: &str) -> Result<u64, ApiError> {
    let raw = q.get(key).ok_or_else(|| ApiError::BadAmount(format!("missing `{key}`")))?;
    let v: u64 = raw
        .parse()
        .map_err(|_| ApiError::BadAmount(format!("`{key}` must be a u64, got {raw:?}")))?;
    if v == 0 {
        return Err(ApiError::BadAmount(format!("`{key}` must be > 0")));
    }
    Ok(v)
}

fn parse_faucet(q: &HashMap<String, String>, key: &str) -> Result<AccountId, ApiError> {
    let raw = q.get(key).ok_or_else(|| ApiError::BadFaucetId(format!("missing `{key}`")))?;
    AccountId::from_hex(raw).map_err(|e| ApiError::BadFaucetId(format!("`{key}`: {e}")))
}

/// `GET /v1/swap-eta?offered_faucet=&offered_amount=&requested_faucet=&requested_amount=`
///
/// Given a prospective order (offer A / request B, raw base-unit amounts), report
/// whether it can fill in the next batch against the live book, the oracle price
/// verdict, and the in-memory 24h median settlement time for the pair.
async fn get_swap_eta(
    State(state): State<PriceApiState>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<SwapEtaResponse>, ApiError> {
    let a = parse_faucet(&q, "offered_faucet")?;
    let b = parse_faucet(&q, "requested_faucet")?;
    if a == b {
        return Err(ApiError::BadRequest(
            "offered_faucet and requested_faucet must differ".into(),
        ));
    }
    let offered_amount = parse_amount(&q, "offered_amount")?;
    let requested_amount = parse_amount(&q, "requested_amount")?;

    // Registration gate + decimals (for the oracle compare).
    let row_a = db::fetch_token_row(&state.pool, &key_bytes(a))
        .map_err(|_| ApiError::Internal)?
        .ok_or(ApiError::UnknownFaucet)?;
    let row_b = db::fetch_token_row(&state.pool, &key_bytes(b))
        .map_err(|_| ApiError::Internal)?
        .ok_or(ApiError::UnknownFaucet)?;
    let d_a = row_a.decimals.map(|d| d as u8);
    let d_b = row_b.decimals.map(|d| d as u8);

    // Book check — the incoming order (offer A, request B) crosses against the
    // OPPOSITE pair (offer B, request A).
    let best = state.swap_rx.borrow().get(&(b, a)).copied();
    let can_fill = eval_can_fill(offered_amount, requested_amount, best);
    let estimated_seconds = if can_fill { Some(state.swap_eta_secs) } else { None };

    // Oracle check (advisory; independent of the book).
    let (usd_a, usd_b) = {
        let snap = state.precise_rx.borrow();
        (snap.get(&a).map(|d| d.usd), snap.get(&b).map(|d| d.usd))
    };
    let (off_market, market_price) = eval_off_market(
        offered_amount,
        d_a,
        usd_a,
        requested_amount,
        d_b,
        usd_b,
        state.swap_offmarket_tol_bps,
    );

    // Median — same direction (A → B) the note settles as; purely in-memory.
    let now = now_secs().max(0) as u64;
    let median24h_seconds = state.stats_rx.borrow().median_secs((a, b), now);

    Ok(Json(SwapEtaResponse {
        offered_faucet: a.to_hex(),
        requested_faucet: b.to_hex(),
        offered_amount: offered_amount.to_string(),
        requested_amount: requested_amount.to_string(),
        can_fill,
        off_market,
        estimated_seconds,
        market_price,
        median24h_seconds,
    }))
}

/// Concurrency limiter: acquire a permit per request, shed with `503` if none.
async fn concurrency_guard(State(sem): State<Arc<Semaphore>>, req: Request, next: Next) -> Response {
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
    let cache = format!("public, max-age={}", (cfg.price_interval_ms / 1000).max(1));
    let cache_value =
        HeaderValue::from_str(&cache).unwrap_or_else(|_| HeaderValue::from_static("no-store"));

    let v1 = Router::new()
        .route("/price/{faucet_id}", get(get_price))
        .route("/prices", get(get_prices))
        .route("/swap-eta", get(get_swap_eta))
        .with_state(state);

    // Public read-only price data → permissive CORS so browser wallets /
    // extensions can fetch it cross-origin. Any origin, GET only (the API is
    // GET-only); preflight OPTIONS is handled by this layer.
    let cors = CorsLayer::new().allow_origin(Any).allow_methods([Method::GET]);

    Router::new()
        .nest("/v1", v1)
        // Outer protections (applied to all routes):
        .layer(SetResponseHeaderLayer::if_not_present(header::CACHE_CONTROL, cache_value))
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
#[allow(clippy::too_many_arguments)]
pub fn spawn_price_api_thread(
    cfg: PriceApiConfig,
    precise_rx: watch::Receiver<PreciseSnapshot>,
    swap_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
    stats_rx: watch::Receiver<Arc<SettlementStats>>,
    pool: DbPool,
    last_price_update: Arc<AtomicI64>,
    cancel: CancellationToken,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let handle = thread::Builder::new()
        .name("price-api".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(anyhow!("price-api runtime: {e}")));
                    return;
                }
            };
            rt.block_on(async move {
                let default_precision =
                    PricePrecision::parse(&cfg.precision).unwrap_or(PricePrecision::Full);
                let swap_eta_secs = (cfg.swap_sync_ms
                    + cfg.swap_matching_trigger_ms
                    + cfg.swap_proving_ms
                    + cfg.swap_block_ms)
                    .div_ceil(1000);
                let state = PriceApiState {
                    precise_rx,
                    pool,
                    last_price_update,
                    vs_currency: cfg.vs_currency.clone(),
                    default_precision,
                    staleness_secs: cfg.staleness_secs as i64,
                    max_batch: cfg.max_batch,
                    swap_rx,
                    stats_rx,
                    swap_eta_secs,
                    swap_offmarket_tol_bps: cfg.swap_offmarket_tol_bps,
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
                if let Err(e) = axum::serve(listener, app).with_graceful_shutdown(shutdown).await {
                    tracing::error!(error = %e, "price-query API server error");
                }
            });
        })
        .context("spawn price-api thread")?;
    Ok((handle, ready_rx))
}

#[cfg(test)]
mod tests;

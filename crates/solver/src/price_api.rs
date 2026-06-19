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
use axum::http::{header, HeaderValue, StatusCode};
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
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;

use crate::config::PricePrecision;
use crate::db::{self, DbPool};
use crate::price::PreciseSnapshot;

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
        .with_state(state);

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
}

/// Spawn the price-query server on its OWN OS thread + multi-thread runtime.
/// Returns the thread handle and a readiness oneshot (Ok once bound, Err on a
/// bind/runtime failure) so startup can gate on it like the ingest thread.
pub fn spawn_price_api_thread(
    cfg: PriceApiConfig,
    precise_rx: watch::Receiver<PreciseSnapshot>,
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
                let state = PriceApiState {
                    precise_rx,
                    pool,
                    last_price_update,
                    vs_currency: cfg.vs_currency.clone(),
                    default_precision,
                    staleness_secs: cfg.staleness_secs as i64,
                    max_batch: cfg.max_batch,
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

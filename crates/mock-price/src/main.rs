//! `mock-price` — a tiny standalone **mock CoinGecko** "simple price" service.
//!
//! Lets the solver's real `HttpPriceClient` price tokens that aren't on the
//! public API (devnet / local). Point `[engine].price_api_base_url` at it.
//!
//! The solver queries by the id you put in each pair's `external_symbol`.
//! Simplest convention: `external_symbol = "<faucet id hex>"`, so the price id
//! *is* the faucet id.
//!
//! In every entry the **id is the faucet id**; **`usd` is just the price field**
//! (its value is the price — the field name stays `usd` because that's what the
//! solver reads). So `--price 0x<faucet>=2.5` / `/set?id=0x<faucet>&usd=2.5` mean
//! "faucet 0x<faucet> is worth 2.5". The numbers only need to be on a CONSISTENT
//! scale — the matcher compares ratios, not real dollars (e.g. A=2.5, B=1.0 ⇒
//! 1 A is worth 2.5 B). Do NOT put a faucet id in the price value.
//!
//! Fully runtime-configurable — keep adding faucets without a restart:
//!   * `--prices-file prices.json` (object of `id -> usd`) is re-read on EVERY
//!     request, so editing it to add/change faucets takes effect immediately;
//!   * `GET /set?id=<id>&usd=<f64>` adds/updates one entry and (if a file is
//!     configured) persists it back to the file;
//!   * `--default-usd <f64>` prices ANY id not listed.
//!
//! Endpoints:
//!   * `GET /api/v3/simple/price?ids=<csv>&vs_currencies=usd` → `{"<id>":{"usd":<f64>}}`
//!   * `GET /set?id=<id>&usd=<f64>`   — add/update (persists to file)
//!   * `GET /prices`                  — dump the current table

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use rand::Rng;
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(name = "mock-price", about = "Mock CoinGecko simple-price service (devnet/local)")]
struct Cli {
    /// Port to bind on 127.0.0.1.
    #[arg(long, default_value_t = 8089)]
    port: u16,
    /// Price entries `<id>=<usd>` (repeatable): left = faucet id, right = price
    /// (a plain number; "usd" is just the unit). E.g. `--price 0x<faucet>=2.5`.
    #[arg(long = "price")]
    prices: Vec<String>,
    /// Fallback USD price returned for ANY id not explicitly set.
    #[arg(long)]
    default_usd: Option<f64>,
    /// Live JSON config file `{ "<id>": <usd>, ... }` (re-read each request,
    /// written on `/set`). Created if missing.
    #[arg(long)]
    prices_file: Option<String>,
    /// Per-request random drift in basis points (simulate movement). 0 = static.
    #[arg(long, default_value_t = 0)]
    drift_bps: u32,
}

struct PriceState {
    /// id (faucet hex or chosen symbol) -> USD price.
    prices: RwLock<HashMap<String, f64>>,
    /// Live JSON config file (re-read per request, written on `/set`).
    file: Option<PathBuf>,
    /// Fallback price for any id not listed (None = omit unknown ids).
    default_usd: Option<f64>,
    /// Per-request random drift in basis points (0 = static).
    drift_bps: u32,
}

fn load_file(path: &PathBuf) -> Result<HashMap<String, f64>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parse {} (expected JSON object of id -> usd)", path.display()))
}

fn save_file(path: &PathBuf, map: &HashMap<String, f64>) -> Result<()> {
    let json = serde_json::to_string_pretty(map)?;
    std::fs::write(path, json).with_context(|| format!("write {}", path.display()))
}

/// If a config file is set, reload the in-memory table from it. Best-effort:
/// keeps the last-good table if the file is missing or mid-write.
async fn refresh(state: &PriceState) {
    if let Some(path) = &state.file {
        if path.exists() {
            match load_file(path) {
                Ok(m) => *state.prices.write().await = m,
                Err(e) => {
                    tracing::warn!(error = %e, "prices file unreadable; keeping last-good table")
                }
            }
        }
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mock_price=info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().compact())
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let file = cli.prices_file.map(PathBuf::from);

    // Seed: existing file (if any) + `--price` overrides.
    let mut prices: HashMap<String, f64> = HashMap::new();
    if let Some(path) = &file {
        if path.exists() {
            prices = load_file(path)?;
        }
    }
    for arg in &cli.prices {
        let (id, usd) = arg
            .split_once('=')
            .ok_or_else(|| anyhow!("--price must be `id=usd`, got {arg:?}"))?;
        let usd: f64 = usd.parse().with_context(|| format!("invalid price in {arg:?}"))?;
        prices.insert(id.to_string(), usd);
    }
    // Persist the seed so the file is the single source of truth going forward.
    if let Some(path) = &file {
        save_file(path, &prices)?;
    }

    tracing::info!(
        entries = prices.len(),
        file = ?file,
        default_usd = ?cli.default_usd,
        drift_bps = cli.drift_bps,
        port = cli.port,
        "starting mock CoinGecko"
    );

    let state = Arc::new(PriceState {
        prices: RwLock::new(prices),
        file,
        default_usd: cli.default_usd,
        drift_bps: cli.drift_bps,
    });
    let app = Router::new()
        .route("/api/v3/simple/price", get(simple_price))
        .route("/set", get(set_price))
        .route("/prices", get(list_prices))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", cli.port))
        .await
        .with_context(|| format!("bind 127.0.0.1:{}", cli.port))?;
    let port = cli.port;
    println!(
        "mock CoinGecko on http://127.0.0.1:{port}\n  \
         prices : GET /api/v3/simple/price?ids=<csv>&vs_currencies=usd\n  \
         add    : GET /set?id=<id>&usd=<f64>        (persists to --prices-file if set)\n  \
         list   : GET /prices\n  \
         add via file: edit the --prices-file JSON (id -> usd); re-read every request\n  \
         config : [engine].price_api_base_url = \"http://127.0.0.1:{port}/api/v3/simple/price\""
    );
    axum::serve(listener, app).await.context("price server failed")?;
    Ok(())
}

/// `GET /api/v3/simple/price?ids=a,b&vs_currencies=usd` → `{"a":{"usd":..}}`.
async fn simple_price(
    State(s): State<Arc<PriceState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Json<Value> {
    refresh(&s).await; // pick up any file edits
    let ids = q.get("ids").map(String::as_str).unwrap_or("");
    let map = s.prices.read().await;
    let mut rng = rand::rng();
    let mut out = Map::new();
    for id in ids.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some(base) = map.get(id).copied().or(s.default_usd) else {
            continue; // unknown id and no default → omit, like real CoinGecko
        };
        let usd = if s.drift_bps > 0 {
            let span = s.drift_bps as f64 / 10_000.0;
            base * (1.0 + rng.random_range(-span..=span))
        } else {
            base
        };
        out.insert(id.to_string(), json!({ "usd": usd }));
    }
    Json(Value::Object(out))
}

/// `GET /set?id=<id>&usd=<f64>` — add/update one price; persist to file if set.
async fn set_price(
    State(s): State<Arc<PriceState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Json<Value> {
    let (Some(id), Some(usd)) = (
        q.get("id").cloned(),
        q.get("usd").and_then(|v| v.parse::<f64>().ok()),
    ) else {
        return Json(json!({ "error": "usage: /set?id=<id>&usd=<f64>" }));
    };
    refresh(&s).await; // merge on top of any external file edits
    let snapshot = {
        let mut map = s.prices.write().await;
        map.insert(id.clone(), usd);
        map.clone()
    };
    if let Some(path) = &s.file {
        if let Err(e) = save_file(path, &snapshot) {
            tracing::warn!(error = %e, "failed to persist price to file");
        }
    }
    tracing::info!(%id, usd, "price added/updated");
    Json(json!({ "ok": true, "id": id, "usd": usd, "entries": snapshot.len() }))
}

/// `GET /prices` — current price table.
async fn list_prices(State(s): State<Arc<PriceState>>) -> Json<HashMap<String, f64>> {
    refresh(&s).await;
    Json(s.prices.read().await.clone())
}

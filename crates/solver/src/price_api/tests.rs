//! Price-API unit tests (axum-test). Uses a temp-file DB so the API's read pool
//! sees what the test wrote via the write pool (separate r2d2 pools → `:memory:`
//! would be two distinct databases).

use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum_test::TestServer;
use miden_protocol::account::AccountId;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};
use serde_json::Value;
use tokio::sync::watch;

use super::{build_app, PriceApiConfig, PriceApiState};
use crate::config::PricePrecision;
use crate::db;
use crate::matching::types::{BestLevel, RateKey, SwapBookSnapshot};
use crate::price::{PreciseSnapshot, PriceData};
use crate::swap_eta::SettlementStats;

fn faucet_a() -> AccountId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
}
fn faucet_b() -> AccountId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
}
fn key(id: AccountId) -> Vec<u8> {
    let mut b = Vec::new();
    id.write_into(&mut b);
    b
}
fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

struct Harness {
    server: TestServer,
    _tmp: tempfile::NamedTempFile,
}

fn cfg() -> PriceApiConfig {
    PriceApiConfig {
        bind: "127.0.0.1".into(),
        port: 0,
        max_inflight: 64,
        max_batch: 3,
        timeout_ms: 2000,
        vs_currency: "usd".into(),
        precision: "full".into(),
        staleness_secs: 60,
        price_interval_ms: 5000,
        swap_matching_trigger_ms: 1000,
        swap_sync_ms: 5000,
        swap_proving_ms: 2000,
        swap_block_ms: 6000,
        swap_offmarket_tol_bps: 50,
    }
}

/// Build a harness (quote currency `usd`). `registered` = (faucet, decimals,
/// ticker) rows; `prices` = (faucet, usd) precise-snapshot entries; `last_update`
/// = freshness.
fn harness(
    registered: &[(AccountId, Option<i32>, Option<&str>)],
    prices: &[(AccountId, f64)],
    last_update: i64,
) -> Harness {
    harness_vs(registered, prices, last_update, "usd")
}

fn harness_vs(
    registered: &[(AccountId, Option<i32>, Option<&str>)],
    prices: &[(AccountId, f64)],
    last_update: i64,
    vs: &str,
) -> Harness {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = db::init_db(tmp.path().to_str().unwrap(), 2).unwrap();
    {
        let mut conn = pool.write_conn().unwrap();
        for (id, dec, tick) in registered {
            db::register_token(&mut conn, &key(*id), Some("usd-coin")).unwrap();
            db::set_token_metadata(&mut conn, &key(*id), *dec, *tick).unwrap();
        }
    }
    let mut snap = PreciseSnapshot::new();
    for (id, usd) in prices {
        snap.insert(*id, PriceData { usd: *usd });
    }
    let (_tx, rx) = watch::channel(snap); // rx retains the value after _tx drops
    let (_stx, swap_rx) = watch::channel(Arc::new(SwapBookSnapshot::new()));
    let (_stats_tx, stats_rx) = watch::channel(Arc::new(SettlementStats::new()));
    let state = PriceApiState {
        precise_rx: rx,
        pool,
        last_price_update: Arc::new(AtomicI64::new(last_update)),
        vs_currency: vs.into(),
        default_precision: PricePrecision::Full,
        staleness_secs: 60,
        max_batch: 3,
        swap_rx,
        stats_rx,
        swap_eta_secs: 14, // 5000+1000+2000+6000 ms → 14s (matches cfg())
        swap_offmarket_tol_bps: 50,
    };
    let mut c = cfg();
    c.vs_currency = vs.into();
    Harness { server: TestServer::new(build_app(state, &c)), _tmp: tmp }
}

/// Build a server for the `/v1/swap-eta` tests: seeds registered tokens (with
/// decimals), oracle prices, a top-of-book snapshot, and the settlement window.
#[allow(clippy::type_complexity)]
fn swap_server(
    registered: &[(AccountId, Option<i32>)],
    prices: &[(AccountId, f64)],
    snapshot: SwapBookSnapshot,
    stats: SettlementStats,
) -> Harness {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = db::init_db(tmp.path().to_str().unwrap(), 2).unwrap();
    {
        let mut conn = pool.write_conn().unwrap();
        for (id, dec) in registered {
            db::register_token(&mut conn, &key(*id), Some("usd-coin")).unwrap();
            db::set_token_metadata(&mut conn, &key(*id), *dec, None).unwrap();
        }
    }
    let mut snap = PreciseSnapshot::new();
    for (id, usd) in prices {
        snap.insert(*id, PriceData { usd: *usd });
    }
    let (_tx, precise_rx) = watch::channel(snap);
    let (_stx, swap_rx) = watch::channel(Arc::new(snapshot));
    let (_stats_tx, stats_rx) = watch::channel(Arc::new(stats));
    let state = PriceApiState {
        precise_rx,
        pool,
        last_price_update: Arc::new(AtomicI64::new(now())),
        vs_currency: "usd".into(),
        default_precision: PricePrecision::Full,
        staleness_secs: 60,
        max_batch: 3,
        swap_rx,
        stats_rx,
        swap_eta_secs: 14,
        swap_offmarket_tol_bps: 50,
    };
    Harness { server: TestServer::new(build_app(state, &cfg())), _tmp: tmp }
}

/// Query string for the swap-eta endpoint.
fn swap_url(off: AccountId, off_amt: u64, req: AccountId, req_amt: u64) -> String {
    format!(
        "/v1/swap-eta?offered_faucet={}&offered_amount={}&requested_faucet={}&requested_amount={}",
        off.to_hex(),
        off_amt,
        req.to_hex(),
        req_amt,
    )
}

/// A top-of-book entry on directed pair `(offered, requested)`.
fn level(
    offered_tok: AccountId,
    requested_tok: AccountId,
    requested: u64,
    offered: u64,
    volume: u64,
) -> ((AccountId, AccountId), BestLevel) {
    ((offered_tok, requested_tok), BestLevel { rate: RateKey::new(requested, offered), volume })
}

fn url(faucet: AccountId, q: &str) -> String {
    format!("/v1/price/{}{}", faucet.to_hex(), q)
}

#[tokio::test]
async fn unknown_faucet_is_404() {
    let h = harness(&[], &[], now());
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn registered_but_unpriced_is_503() {
    let h = harness(&[(faucet_a(), Some(6), Some("USDC"))], &[], now());
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn happy_path_returns_price_decimals_ticker() {
    let h = harness(
        &[(faucet_a(), Some(6), Some("USDC"))],
        &[(faucet_a(), 0.99987)],
        now(),
    );
    let r = h.server.get(&url(faucet_a(), "?precision=full")).await;
    assert_eq!(r.status_code(), StatusCode::OK);
    let v: Value = r.json();
    assert_eq!(v["price"].as_str().unwrap(), "0.99987");
    assert_eq!(v["decimals"].as_u64().unwrap(), 6);
    assert_eq!(v["ticker"].as_str().unwrap(), "USDC");
    assert_eq!(v["vs_currency"].as_str().unwrap(), "usd");
    assert!(!v["stale"].as_bool().unwrap());
}

#[tokio::test]
async fn precision_formatting_and_subcent_preserved() {
    let h = harness(&[(faucet_a(), Some(6), None)], &[(faucet_a(), 0.0034)], now());
    // Fixed precision rounds for display...
    let r2: Value = h.server.get(&url(faucet_a(), "?precision=2")).await.json();
    assert_eq!(r2["price"].as_str().unwrap(), "0.00");
    // ...but `full` preserves the sub-cent value (not $0.00).
    let rf: Value = h.server.get(&url(faucet_a(), "?precision=full")).await.json();
    assert_eq!(rf["price"].as_str().unwrap(), "0.0034");
    // Garbage precision → 400.
    let rb = h.server.get(&url(faucet_a(), "?precision=abc")).await;
    assert_eq!(rb.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn decimals_null_until_fetched() {
    let h = harness(&[(faucet_a(), None, None)], &[(faucet_a(), 1.0)], now());
    let v: Value = h.server.get(&url(faucet_a(), "")).await.json();
    assert!(v["decimals"].is_null());
    assert!(v.get("ticker").map(|t| t.is_null()).unwrap_or(true));
}

#[tokio::test]
async fn stale_fails_closed_unless_allowed() {
    let h = harness(&[(faucet_a(), Some(6), None)], &[(faucet_a(), 1.0)], now() - 10_000);
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    let r2 = h.server.get(&url(faucet_a(), "?allow_stale=true")).await;
    assert_eq!(r2.status_code(), StatusCode::OK);
    assert!(r2.json::<Value>()["stale"].as_bool().unwrap());
}

#[tokio::test]
async fn batch_returns_map_and_caps_size() {
    let h = harness(
        &[
            (faucet_a(), Some(6), Some("USDC")),
            (faucet_b(), Some(8), Some("ETH")),
        ],
        &[(faucet_a(), 1.0), (faucet_b(), 3000.0)],
        now(),
    );
    let ids = format!("{},{}", faucet_a().to_hex(), faucet_b().to_hex());
    let r = h.server.get(&format!("/v1/prices?ids={ids}")).await;
    assert_eq!(r.status_code(), StatusCode::OK);
    assert_eq!(r.json::<Value>().as_object().unwrap().len(), 2);
    // max_batch = 3 → 4 ids is rejected.
    let a = faucet_a().to_hex();
    let many = format!("{a},{a},{a},{a}");
    let rc = h.server.get(&format!("/v1/prices?ids={many}")).await;
    assert_eq!(rc.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn precision_boundaries_and_config_default() {
    let h = harness(&[(faucet_a(), Some(6), None)], &[(faucet_a(), 0.99987)], now());
    // precision=0 → integer string (rounds 0.99987 → "1").
    let r0: Value = h.server.get(&url(faucet_a(), "?precision=0")).await.json();
    assert_eq!(r0["price"].as_str().unwrap(), "1");
    assert_eq!(r0["precision"].as_str().unwrap(), "0");
    // precision=18 is in range (max).
    let r18 = h.server.get(&url(faucet_a(), "?precision=18")).await;
    assert_eq!(r18.status_code(), StatusCode::OK);
    assert_eq!(r18.json::<Value>()["precision"].as_str().unwrap(), "18");
    // precision=19 is out of CoinGecko's range → 400.
    let r19 = h.server.get(&url(faucet_a(), "?precision=19")).await;
    assert_eq!(r19.status_code(), StatusCode::BAD_REQUEST);
    // Negative → 400.
    let rneg = h.server.get(&url(faucet_a(), "?precision=-1")).await;
    assert_eq!(rneg.status_code(), StatusCode::BAD_REQUEST);
    // No param → the configured default (full) → unrounded value.
    let rd: Value = h.server.get(&url(faucet_a(), "")).await.json();
    assert_eq!(rd["price"].as_str().unwrap(), "0.99987");
    assert_eq!(rd["precision"].as_str().unwrap(), "full");
}

#[tokio::test]
async fn malformed_faucet_id_is_400() {
    let h = harness(&[(faucet_a(), Some(6), None)], &[(faucet_a(), 1.0)], now());
    // Not 404: a syntactically invalid id is a client error, distinct from an
    // unknown (but well-formed) faucet.
    let r = h.server.get("/v1/price/not-a-hex-id").await;
    assert_eq!(r.status_code(), StatusCode::BAD_REQUEST);
    let v: Value = r.json();
    assert!(v.get("error").is_some(), "error body present: {v}");
}

#[tokio::test]
async fn batch_omits_unknown_and_unpriced_and_empty_is_empty_map() {
    // faucet_a: registered + priced; faucet_b: registered but UNPRICED.
    let h = harness(
        &[(faucet_a(), Some(6), Some("USDC")), (faucet_b(), Some(8), Some("ETH"))],
        &[(faucet_a(), 1.0)],
        now(),
    );
    // ids = priced + unpriced → only the priced one appears (CoinGecko-style omit).
    let ids = format!("{},{}", faucet_a().to_hex(), faucet_b().to_hex());
    let v: Value = h.server.get(&format!("/v1/prices?ids={ids}")).await.json();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.len(), 1);
    assert!(obj.contains_key(&faucet_a().to_hex()));
    assert!(!obj.contains_key(&faucet_b().to_hex()));
    // No ids → 200 with an empty map (not an error).
    let re = h.server.get("/v1/prices").await;
    assert_eq!(re.status_code(), StatusCode::OK);
    assert_eq!(re.json::<Value>().as_object().unwrap().len(), 0);
}

#[tokio::test]
async fn vs_currency_is_configurable_and_reflected() {
    let h = harness_vs(&[(faucet_a(), Some(6), Some("USDC"))], &[(faucet_a(), 0.92)], now(), "eur");
    let v: Value = h.server.get(&url(faucet_a(), "")).await.json();
    assert_eq!(v["vs_currency"].as_str().unwrap(), "eur");
    assert_eq!(v["price"].as_str().unwrap(), "0.92");
}

#[tokio::test]
async fn routing_is_v1_scoped_and_get_only() {
    let h = harness(&[(faucet_a(), Some(6), None)], &[(faucet_a(), 1.0)], now());
    // Unknown route under /v1 → 404.
    let r1 = h.server.get("/v1/bogus").await;
    assert_eq!(r1.status_code(), StatusCode::NOT_FOUND);
    // The same handler without the /v1 prefix is not mounted → 404.
    let r2 = h.server.get(&format!("/price/{}", faucet_a().to_hex())).await;
    assert_eq!(r2.status_code(), StatusCode::NOT_FOUND);
    // Wrong method on a real route → 405.
    let r3 = h.server.post(&format!("/v1/price/{}", faucet_a().to_hex())).await;
    assert_eq!(r3.status_code(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn cors_header_present_for_browser_clients() {
    // A browser wallet / extension fetches cross-origin → the response must
    // carry Access-Control-Allow-Origin, else the browser blocks it.
    let h = harness(&[(faucet_a(), Some(6), Some("USDC"))], &[(faucet_a(), 1.0)], now());
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::OK);
    let acao = r
        .maybe_header("access-control-allow-origin")
        .expect("CORS allow-origin header present");
    assert_eq!(acao.to_str().unwrap(), "*");
}

// ── swap-eta ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn swap_eta_has_liquidity_crosses_with_eta() {
    // Opposite book (offer B, request A) top level: 300 B for 100 A, depth 300.
    let mut snap = SwapBookSnapshot::new();
    let (k, v) = level(faucet_b(), faucet_a(), 100, 300, 300);
    snap.insert(k, v);
    // User: offer 100 A ($2 ea), want 200 B ($1 ea) → fair → not off-market.
    let h = swap_server(
        &[(faucet_a(), Some(8)), (faucet_b(), Some(8))],
        &[(faucet_a(), 2.0), (faucet_b(), 1.0)],
        snap,
        SettlementStats::new(),
    );
    let v: Value = h.server.get(&swap_url(faucet_a(), 100, faucet_b(), 200)).await.json();
    assert!(v["canFill"].as_bool().unwrap());
    assert_eq!(v["estimatedSeconds"].as_u64().unwrap(), 14);
    assert_eq!(v["offMarket"].as_bool().unwrap(), false);
    assert_eq!(v["marketPrice"].as_str().unwrap(), "2"); // usd_a/usd_b = 2
    assert!(v["median24hSeconds"].is_null());
}

#[tokio::test]
async fn swap_eta_no_cross_not_fillable() {
    // Opposite top gives only 150 B per 100 A → user wanting 200 B doesn't cross.
    let mut snap = SwapBookSnapshot::new();
    let (k, val) = level(faucet_b(), faucet_a(), 100, 150, 1000);
    snap.insert(k, val);
    let h = swap_server(&[(faucet_a(), Some(8)), (faucet_b(), Some(8))], &[], snap, SettlementStats::new());
    let v: Value = h.server.get(&swap_url(faucet_a(), 100, faucet_b(), 200)).await.json();
    assert_eq!(v["canFill"].as_bool().unwrap(), false);
    assert!(v["estimatedSeconds"].is_null());
}

#[tokio::test]
async fn swap_eta_crosses_but_thin_volume() {
    // Crosses on rate but only 50 B available < 200 requested → not fillable,
    // and no threshold (price is fine, depth is the blocker).
    let mut snap = SwapBookSnapshot::new();
    let (k, val) = level(faucet_b(), faucet_a(), 100, 300, 50);
    snap.insert(k, val);
    let h = swap_server(&[(faucet_a(), Some(8)), (faucet_b(), Some(8))], &[], snap, SettlementStats::new());
    let v: Value = h.server.get(&swap_url(faucet_a(), 100, faucet_b(), 200)).await.json();
    assert_eq!(v["canFill"].as_bool().unwrap(), false);
}

#[tokio::test]
async fn swap_eta_empty_book_not_fillable() {
    let h = swap_server(
        &[(faucet_a(), Some(8)), (faucet_b(), Some(8))],
        &[],
        SwapBookSnapshot::new(),
        SettlementStats::new(),
    );
    let v: Value = h.server.get(&swap_url(faucet_a(), 100, faucet_b(), 200)).await.json();
    assert_eq!(v["canFill"].as_bool().unwrap(), false);
    assert!(v["median24hSeconds"].is_null());
}

#[tokio::test]
async fn swap_eta_median_present_and_off_market_true() {
    let mut stats = SettlementStats::new();
    for d in [10u64, 30, 20] {
        stats.record((faucet_a(), faucet_b()), now() as u64, d);
    }
    // Greedy order: offer 100 A ($2), request 500 B ($5 > $2) → off-market.
    let h = swap_server(
        &[(faucet_a(), Some(8)), (faucet_b(), Some(8))],
        &[(faucet_a(), 2.0), (faucet_b(), 1.0)],
        SwapBookSnapshot::new(),
        stats,
    );
    let v: Value = h.server.get(&swap_url(faucet_a(), 100, faucet_b(), 500)).await.json();
    assert_eq!(v["median24hSeconds"].as_u64().unwrap(), 20); // median of 10,20,30
    assert_eq!(v["offMarket"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn swap_eta_bad_input() {
    let h = swap_server(
        &[(faucet_a(), Some(8)), (faucet_b(), Some(8))],
        &[],
        SwapBookSnapshot::new(),
        SettlementStats::new(),
    );
    // zero amount → 400
    let r0 = h.server.get(&swap_url(faucet_a(), 0, faucet_b(), 200)).await;
    assert_eq!(r0.status_code(), StatusCode::BAD_REQUEST);
    // same faucet → 400
    let rs = h.server.get(&swap_url(faucet_a(), 100, faucet_a(), 200)).await;
    assert_eq!(rs.status_code(), StatusCode::BAD_REQUEST);
    // bad hex → 400
    let rb = h
        .server
        .get("/v1/swap-eta?offered_faucet=nothex&offered_amount=1&requested_faucet=nothex2&requested_amount=1")
        .await;
    assert_eq!(rb.status_code(), StatusCode::BAD_REQUEST);
    // unknown (well-formed) faucet → 404
    let ru = h.server.get(&swap_url(faucet_unregistered(), 100, faucet_b(), 200)).await;
    assert_eq!(ru.status_code(), StatusCode::NOT_FOUND);
}

fn faucet_unregistered() -> AccountId {
    use miden_protocol::testing::account_id::ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2;
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap()
}

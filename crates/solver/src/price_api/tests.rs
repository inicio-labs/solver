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
use crate::price::{PreciseSnapshot, PriceData};

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
    let state = PriceApiState {
        precise_rx: rx,
        pool,
        last_price_update: Arc::new(AtomicI64::new(last_update)),
        vs_currency: vs.into(),
        default_precision: PricePrecision::Full,
        staleness_secs: 60,
        max_batch: 3,
    };
    let mut c = cfg();
    c.vs_currency = vs.into();
    Harness { server: TestServer::new(build_app(state, &c)), _tmp: tmp }
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

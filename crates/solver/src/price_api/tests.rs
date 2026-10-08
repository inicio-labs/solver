//! Price-API tests against an isolated PostgreSQL schema.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum_test::TestServer;
use miden_protocol::account::AccountId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};
use serde_json::Value;
use tokio::sync::watch;

use super::{build_app, PriceApiConfig, PriceApiState};
use crate::db;
use crate::db::postgres_test::TestDb;
use crate::matching::types::{BookLevel, RateKey, SwapBookSnapshot};
use crate::price::PricePrecision;
use crate::price::PriceSnapshot;
use crate::swap_eta::{QuoteTerms, SettlementStats};

/// Long enough that a slow database setup cannot make a fresh quote stale.
const TTL: Duration = Duration::from_secs(3_600);

fn faucet_a() -> AccountId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
}
fn faucet_b() -> AccountId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
/// Receipt time of a quote that is still fresh, or already stale.
fn received(fresh: bool) -> Instant {
    if fresh {
        Instant::now()
    } else {
        Instant::now().checked_sub(2 * TTL).unwrap()
    }
}

struct Harness {
    server: TestServer,
    _db: TestDb,
}

fn cfg() -> PriceApiConfig {
    PriceApiConfig {
        bind: "127.0.0.1".into(),
        port: 0,
        max_inflight: 64,
        max_batch: 3,
        timeout_ms: 2000,
        vs_currency: "usdt".into(),
        precision: "full".into(),
        swap_matching_trigger_ms: 1000,
        swap_sync_ms: 5000,
        swap_proving_ms: 2000,
        swap_block_ms: 6000,
        swap_offmarket_tol_bps: 50,
        clearing_fee_ppm: 1_000,
    }
}

/// Seed `registered` = (faucet, decimals, ticker) rows and serve `snapshot`.
async fn serve(
    registered: &[(AccountId, Option<u8>, Option<&str>)],
    snapshot: PriceSnapshot,
    book: SwapBookSnapshot,
    stats: SettlementStats,
) -> Harness {
    let test_db = TestDb::new().await.unwrap();
    let pool = test_db.pool.clone();
    let rows: Vec<_> = registered
        .iter()
        .map(|(id, dec, tick)| (*id, *dec, tick.map(str::to_owned)))
        .collect();
    pool.write(move |conn| {
        for (id, dec, tick) in rows {
            db::postgres_db::register_token_tx(conn, id)?;
            db::postgres_db::set_token_metadata_tx(conn, id, dec, tick.as_deref())?;
        }
        Ok(())
    })
    .await
    .unwrap();
    // The receivers keep the values after their senders drop.
    let (_tx, prices) = watch::channel(Arc::new(snapshot));
    let (_book_tx, book_rx) = watch::channel(Arc::new(book));
    let (_stats_tx, stats_rx) = watch::channel(Arc::new(stats));
    let state = PriceApiState {
        prices,
        pool,
        vs_currency: "usdt".into(),
        default_precision: PricePrecision::Full,
        max_batch: 3,
        book_rx,
        stats_rx,
        swap_eta_secs: 14, // 5000+1000+2000+6000 ms → 14s (matches cfg())
        quote_terms: QuoteTerms {
            fee_ppm: 1_000,
            tolerance_bps: 50,
        },
    };
    Harness {
        server: TestServer::new(build_app(state, &cfg())),
        _db: test_db,
    }
}

/// A harness for the price endpoints: `prices` values tokens (whole USDT per
/// whole token; `None` = the token is USDT itself), received fresh or stale.
async fn harness(
    registered: &[(AccountId, Option<u8>, Option<&str>)],
    prices: &[(AccountId, Option<&str>)],
    fresh: bool,
) -> Harness {
    let received_at = received(fresh);
    let valuations: Vec<_> = prices
        .iter()
        .map(|(id, raw)| (*id, raw.map(|raw| (raw, received_at))))
        .collect();
    let snapshot = PriceSnapshot::for_tests(&[], &valuations, TTL);
    serve(
        registered,
        snapshot,
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await
}

/// A harness for `/v1/swap-eta`: `market` is the A/B clearing pair's price in
/// whole B per whole A (`None` = no market), received fresh or stale.
async fn swap_server_at(
    fresh: bool,
    registered: &[(AccountId, Option<u8>)],
    market: Option<&str>,
    book: SwapBookSnapshot,
    stats: SettlementStats,
) -> Harness {
    let registered: Vec<_> = registered
        .iter()
        .map(|(id, dec)| (*id, *dec, None))
        .collect();
    let pairs: Vec<_> = market
        .map(|raw| (faucet_a(), faucet_b(), raw, received(fresh)))
        .into_iter()
        .collect();
    let snapshot = PriceSnapshot::for_tests(&pairs, &[], TTL);
    serve(&registered, snapshot, book, stats).await
}

async fn swap_server(
    registered: &[(AccountId, Option<u8>)],
    market: Option<&str>,
    book: SwapBookSnapshot,
    stats: SettlementStats,
) -> Harness {
    swap_server_at(true, registered, market, book, stats).await
}

/// A book with one level on directed pair `(offered, requested)`.
fn book(
    offered_tok: AccountId,
    requested_tok: AccountId,
    requested: u64,
    offered: u64,
    volume: u64,
) -> SwapBookSnapshot {
    let level = BookLevel {
        rate: RateKey::new(requested, offered),
        volume,
    };
    [((offered_tok, requested_tok), vec![level])].into()
}

/// Stats from an executor that is taking batches.
fn settling() -> SettlementStats {
    let mut stats = SettlementStats::new();
    stats.settling = true;
    stats
}

/// Buyers of A paying 2.14 B per A, eligible at the market of 2 B per A.
fn buyers(volume: u64) -> SwapBookSnapshot {
    book(faucet_b(), faucet_a(), 1_400_000, 3_000_000, volume)
}

fn url(faucet: AccountId, q: &str) -> String {
    format!("/v1/price/{}{}", faucet.to_hex(), q)
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn unknown_faucet_is_404() {
    let h = harness(&[], &[], true).await;
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn registered_but_unpriced_is_503() {
    let h = harness(&[(faucet_a(), Some(6), Some("USDC"))], &[], true).await;
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn happy_path_returns_price_decimals_ticker() {
    let h = harness(
        &[(faucet_a(), Some(6), Some("USDC"))],
        &[(faucet_a(), Some("0.99987"))],
        true,
    )
    .await;
    let r = h.server.get(&url(faucet_a(), "?precision=full")).await;
    assert_eq!(r.status_code(), StatusCode::OK);
    let v: Value = r.json();
    assert_eq!(v["price"].as_str().unwrap(), "0.99987");
    assert_eq!(v["decimals"].as_u64().unwrap(), 6);
    assert_eq!(v["ticker"].as_str().unwrap(), "USDC");
    assert_eq!(v["vs_currency"].as_str().unwrap(), "usdt");
    assert_eq!(v["source"].as_str().unwrap(), "binance");
    assert!(!v["stale"].as_bool().unwrap());
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn precision_formatting_and_subcent_preserved() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("0.0034"))],
        true,
    )
    .await;
    // Fixed precision rounds for display...
    let r2: Value = h.server.get(&url(faucet_a(), "?precision=2")).await.json();
    assert_eq!(r2["price"].as_str().unwrap(), "0.00");
    // ...but `full` preserves the sub-cent value (not $0.00).
    let rf: Value = h
        .server
        .get(&url(faucet_a(), "?precision=full"))
        .await
        .json();
    assert_eq!(rf["price"].as_str().unwrap(), "0.0034");
    // Garbage precision → 400.
    let rb = h.server.get(&url(faucet_a(), "?precision=abc")).await;
    assert_eq!(rb.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn decimals_null_until_fetched() {
    let h = harness(
        &[(faucet_a(), None, None)],
        &[(faucet_a(), Some("1"))],
        true,
    )
    .await;
    let v: Value = h.server.get(&url(faucet_a(), "")).await.json();
    assert!(v["decimals"].is_null());
    assert!(v.get("ticker").map(|t| t.is_null()).unwrap_or(true));
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn stale_fails_closed_unless_allowed() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("1"))],
        false,
    )
    .await;
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.json::<Value>()["error"].as_str().unwrap(), "stale");
    let r2 = h.server.get(&url(faucet_a(), "?allow_stale=true")).await;
    assert_eq!(r2.status_code(), StatusCode::OK);
    let v: Value = r2.json();
    assert!(v["stale"].as_bool().unwrap());
    // `as_of` is the receipt time: two TTLs ago.
    let expected = now() - 2 * TTL.as_secs() as i64;
    assert!((v["as_of"].as_i64().unwrap() - expected).abs() <= 5, "{v}");
}

/// A token without any Binance market gets a distinct, non-retryable code.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn unpriced_token_distinguishes_no_market_from_no_quote() {
    let h = harness(&[(faucet_a(), Some(6), Some("USDC"))], &[], true).await;
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.json::<Value>()["error"].as_str().unwrap(), "no_market");
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn batch_returns_map_and_caps_size() {
    let h = harness(
        &[
            (faucet_a(), Some(6), Some("USDC")),
            (faucet_b(), Some(8), Some("ETH")),
        ],
        &[(faucet_a(), Some("1")), (faucet_b(), Some("3000"))],
        true,
    )
    .await;
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
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn precision_boundaries_and_config_default() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("0.99987"))],
        true,
    )
    .await;
    // precision=0 → integer string (rounds 0.99987 → "1").
    let r0: Value = h.server.get(&url(faucet_a(), "?precision=0")).await.json();
    assert_eq!(r0["price"].as_str().unwrap(), "1");
    assert_eq!(r0["precision"].as_str().unwrap(), "0");
    // precision=18 is in range (max).
    let r18 = h.server.get(&url(faucet_a(), "?precision=18")).await;
    assert_eq!(r18.status_code(), StatusCode::OK);
    assert_eq!(r18.json::<Value>()["precision"].as_str().unwrap(), "18");
    // precision=19 is out of range → 400.
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
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn malformed_faucet_id_is_400() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("1"))],
        true,
    )
    .await;
    // Not 404: a syntactically invalid id is a client error, distinct from an
    // unknown (but well-formed) faucet.
    let r = h.server.get("/v1/price/not-a-hex-id").await;
    assert_eq!(r.status_code(), StatusCode::BAD_REQUEST);
    let v: Value = r.json();
    assert!(v.get("error").is_some(), "error body present: {v}");
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn batch_omits_unknown_and_unpriced_and_empty_is_empty_map() {
    // faucet_a: registered + priced; faucet_b: registered but UNPRICED.
    let h = harness(
        &[
            (faucet_a(), Some(6), Some("USDC")),
            (faucet_b(), Some(8), Some("ETH")),
        ],
        &[(faucet_a(), Some("1"))],
        true,
    )
    .await;
    // ids = priced + unpriced → only the priced one appears.
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
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn valuation_quote_asset_is_worth_exactly_one() {
    let h = harness(
        &[(faucet_a(), Some(6), Some("USDT"))],
        &[(faucet_a(), None)],
        true,
    )
    .await;
    let v: Value = h.server.get(&url(faucet_a(), "")).await.json();
    assert_eq!(v["price"].as_str().unwrap(), "1");
    assert!(!v["stale"].as_bool().unwrap());
    assert!((v["as_of"].as_i64().unwrap() - now()).abs() <= 5);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn batch_omits_stale_unless_allowed() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("1"))],
        false,
    )
    .await;
    let ids = format!("/v1/prices?ids={}", faucet_a().to_hex());
    let v: Value = h.server.get(&ids).await.json();
    assert!(v.as_object().unwrap().is_empty());
    let v: Value = h
        .server
        .get(&format!("{ids}&allow_stale=true"))
        .await
        .json();
    assert!(v[faucet_a().to_hex()]["stale"].as_bool().unwrap());
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn routing_is_v1_scoped_and_get_only() {
    let h = harness(
        &[(faucet_a(), Some(6), None)],
        &[(faucet_a(), Some("1"))],
        true,
    )
    .await;
    // Unknown route under /v1 → 404.
    let r1 = h.server.get("/v1/bogus").await;
    assert_eq!(r1.status_code(), StatusCode::NOT_FOUND);
    // The same handler without the /v1 prefix is not mounted → 404.
    let r2 = h
        .server
        .get(&format!("/price/{}", faucet_a().to_hex()))
        .await;
    assert_eq!(r2.status_code(), StatusCode::NOT_FOUND);
    // Wrong method on a real route → 405.
    let r3 = h
        .server
        .post(&format!("/v1/price/{}", faucet_a().to_hex()))
        .await;
    assert_eq!(r3.status_code(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn cors_header_present_for_browser_clients() {
    // A browser wallet / extension fetches cross-origin → the response must
    // carry Access-Control-Allow-Origin, else the browser blocks it.
    let h = harness(
        &[(faucet_a(), Some(6), Some("USDC"))],
        &[(faucet_a(), Some("1"))],
        true,
    )
    .await;
    let r = h.server.get(&url(faucet_a(), "")).await;
    assert_eq!(r.status_code(), StatusCode::OK);
    let acao = r
        .maybe_header("access-control-allow-origin")
        .expect("CORS allow-origin header present");
    assert_eq!(acao.to_str().unwrap(), "*");
}

// ── swap-eta ──────────────────────────────────────────────────────────────
// The A/B market is 2 B per A (both tokens 0 decimals in the market plan);
// fee 0.1%, tolerance 0.5% (see `serve`).

fn registered() -> [(AccountId, Option<u8>); 2] {
    [(faucet_a(), Some(8)), (faucet_b(), Some(8))]
}

async fn swap_status(h: &Harness, query: &str) -> StatusCode {
    h.server
        .get(&format!("/v1/swap-eta?{query}"))
        .await
        .status_code()
}

async fn swap_get(h: &Harness, query: &str) -> Value {
    let r = h.server.get(&format!("/v1/swap-eta?{query}")).await;
    assert_eq!(r.status_code(), StatusCode::OK, "{}", r.text());
    r.json()
}

/// `offered_faucet=A&requested_faucet=B` plus `rest`.
fn a_for_b(rest: &str) -> String {
    format!(
        "offered_faucet={}&requested_faucet={}&{rest}",
        faucet_a().to_hex(),
        faucet_b().to_hex()
    )
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_quotes_a_full_fill_at_market() {
    let h = swap_server(&registered(), Some("2"), buyers(3_000_000), settling()).await;
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert_eq!(v["priceBand"], "at_market");
    assert_eq!(v["fillStatus"], "full");
    assert!(v["reason"].is_null());
    assert_eq!(v["fillableOfferedAmount"], "1000000");
    assert_eq!(v["fillableRequestedAmount"], "1990000");
    assert_eq!(v["expectedRequestedAmount"], "1998000");
    assert_eq!(v["feePpm"], 1_000);
    assert_eq!(v["feeAmount"], "2000");
    assert_eq!(v["marketPrice"], "2");
    assert_eq!(v["fillPrice"], "1.998");
    // The book takes 3_000_000 B: 1_500_000 A, more than this order.
    assert_eq!(v["availableOfferedAmount"], "1500000");
    assert_eq!(v["acceptingOrders"], true);
    assert_eq!(v["canFill"], true);
    assert_eq!(v["offMarket"], false);
    assert_eq!(v["estimatedSeconds"], 14);
    assert!(v["median24hSeconds"].is_null());
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_quotes_a_partial_fill_and_respects_the_min_fill_step() {
    let h = swap_server(&registered(), Some("2"), buyers(1_000_000), settling()).await;
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert_eq!(v["fillStatus"], "partial");
    assert_eq!(v["fillableOfferedAmount"], "500000");
    assert_eq!(v["fillableRequestedAmount"], "995000");
    assert_eq!(v["availableOfferedAmount"], "500000");
    assert_eq!(v["canFill"], false);
    assert_eq!(v["estimatedSeconds"], 14);
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000&min_fill_step=1000000"),
    )
    .await;
    assert_eq!(v["fillStatus"], "none");
    assert_eq!(v["reason"], "liquidity");
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_has_no_eta_while_the_solver_cannot_settle() {
    // The executor has not started, or is in verification mode: the book
    // still fills the order, but not now.
    let paused = SettlementStats::new();
    let h = swap_server(&registered(), Some("2"), buyers(3_000_000), paused).await;
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert_eq!(v["fillStatus"], "full");
    assert_eq!(v["acceptingOrders"], false);
    assert_eq!(v["canFill"], false);
    assert!(v["estimatedSeconds"].is_null());
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_tolerates_a_small_gap_and_flags_a_large_one() {
    let mut stats = settling();
    for d in [10u64, 30, 20] {
        stats.record((faucet_a(), faucet_b()), now() as u64, d);
    }
    let h = swap_server(&registered(), Some("2"), buyers(3_000_000), stats).await;
    // 2.005 B per A: above the 1.998 fill price, inside the tolerance.
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=2005000"),
    )
    .await;
    assert_eq!(v["priceBand"], "tolerated");
    assert_eq!(v["fillStatus"], "full");
    assert_eq!(v["canFill"], false);
    assert_eq!(v["offMarket"], false);
    assert!(v["estimatedSeconds"].is_null());
    // 2.1 B per A: past it.
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=2100000"),
    )
    .await;
    assert_eq!(v["priceBand"], "off_market");
    assert_eq!(v["fillStatus"], "none");
    assert_eq!(v["reason"], "price");
    assert_eq!(v["offMarket"], true);
    assert_eq!(v["fillableOfferedAmount"], "0");
    assert!(v["availableOfferedAmount"].is_null());
    assert_eq!(v["median24hSeconds"], 20); // median of 10, 20, 30
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_without_a_price_fills_nothing() {
    // No market for the pair at all.
    let h = swap_server(
        &registered(),
        None,
        buyers(3_000_000),
        SettlementStats::new(),
    )
    .await;
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert!(v["priceBand"].is_null());
    assert_eq!(v["fillStatus"], "none");
    assert_eq!(v["reason"], "no_market");
    assert!(v["offMarket"].is_null());
    assert!(v["marketPrice"].is_null());
    assert!(v["availableOfferedAmount"].is_null());

    // A market whose quote is at least the TTL old fails closed.
    let h = swap_server_at(
        false,
        &registered(),
        Some("2"),
        buyers(3_000_000),
        SettlementStats::new(),
    )
    .await;
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert_eq!(v["reason"], "no_price");
    assert!(v["offMarket"].is_null());
    assert!(v["marketPrice"].is_null());
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_reads_the_pair_in_either_direction() {
    // The pair is configured as A/B at 2 B per A; an order offering B for A
    // sees the reciprocal and takes the buyer's side. Sellers of A asking
    // 1.9 B per A fill it.
    let sellers = book(faucet_a(), faucet_b(), 1_900_000, 1_000_000, 1_000_000);
    let h = swap_server(&registered(), Some("2"), sellers, SettlementStats::new()).await;
    let query = format!(
        "offered_faucet={}&requested_faucet={}&offered_amount=2000000&requested_amount=999000",
        faucet_b().to_hex(),
        faucet_a().to_hex()
    );
    let v = swap_get(&h, &query).await;
    assert_eq!(v["marketPrice"], "0.5");
    assert_eq!(v["priceBand"], "at_market");
    assert_eq!(v["fillStatus"], "full");
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_response_is_not_cached() {
    // `/v1/swap-eta` must override the router-level `max-age` cache layer with
    // `no-store`, since its fields come from independently-updated snapshots.
    let h = swap_server(
        &registered(),
        Some("2"),
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await;
    let r = h
        .server
        .get(&format!(
            "/v1/swap-eta?{}",
            a_for_b("offered_amount=100&requested_amount=200")
        ))
        .await;
    let cache = r.headers().get("cache-control").cloned();
    assert_eq!(
        cache
            .expect("cache-control header present")
            .to_str()
            .unwrap(),
        "no-store",
        "swap-eta must not inherit the router's max-age",
    );
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn swap_eta_bad_input() {
    let h = swap_server(
        &registered(),
        Some("2"),
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await;
    // a zero or missing amount, or a zero min fill step → 400
    for query in [
        "offered_amount=0&requested_amount=200",
        "offered_amount=100&requested_amount=200&min_fill_step=0",
        "offered_amount=100",
        "requested_amount=200",
        "min_fill_step=5",
    ] {
        assert_eq!(
            swap_status(&h, &a_for_b(query)).await,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    // same faucet → 400
    let same = format!(
        "offered_faucet={0}&requested_faucet={0}&offered_amount=1&requested_amount=1",
        faucet_a().to_hex()
    );
    assert_eq!(swap_status(&h, &same).await, StatusCode::BAD_REQUEST);
    // bad hex → 400
    let bad = "offered_faucet=nothex&offered_amount=1&requested_faucet=nothex2&requested_amount=1";
    assert_eq!(swap_status(&h, bad).await, StatusCode::BAD_REQUEST);
    // unknown (well-formed) faucet → 404
    let unknown = format!(
        "offered_faucet={}&requested_faucet={}&offered_amount=100&requested_amount=200",
        faucet_unregistered().to_hex(),
        faucet_b().to_hex()
    );
    assert_eq!(swap_status(&h, &unknown).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_reversed_binance_listing_prices_and_quotes_the_same() {
    // Binance lists the pair the other way round (its base is B): the
    // stream carries 0.5, and both endpoints still see 2 B per A.
    let pairs = [(faucet_a(), faucet_b(), "2", received(true))];
    let decimals = [(faucet_a(), 0), (faucet_b(), 0)];
    let snapshot = PriceSnapshot::for_tests_reversed(&pairs, &decimals, TTL);
    let registered: Vec<_> = registered()
        .iter()
        .map(|(id, dec)| (*id, *dec, None))
        .collect();
    let h = serve(&registered, snapshot, buyers(3_000_000), settling()).await;
    let v: Value = h.server.get(&pair_url(faucet_a(), faucet_b())).await.json();
    assert_eq!(v["marketPrice"], "2");
    assert_eq!(v["fillPrice"], "1.998");
    let v = swap_get(
        &h,
        &a_for_b("offered_amount=1000000&requested_amount=1990000"),
    )
    .await;
    assert_eq!(v["priceBand"], "at_market");
    assert_eq!(v["fillStatus"], "full");
    assert_eq!(v["availableOfferedAmount"], "1500000");
}

// ── pair-price ────────────────────────────────────────────────────────────

/// `/v1/pair-price` for an order offering `offered` for `requested`.
fn pair_url(offered: AccountId, requested: AccountId) -> String {
    format!(
        "/v1/pair-price?offered_faucet={}&requested_faucet={}",
        offered.to_hex(),
        requested.to_hex()
    )
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn pair_price_gives_each_side_its_fill_price() {
    let h = swap_server(
        &registered(),
        Some("2"),
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await;
    // Selling A: 2 B per A less the 0.1% fee.
    let v: Value = h.server.get(&pair_url(faucet_a(), faucet_b())).await.json();
    assert_eq!(v["marketPrice"], "2");
    assert_eq!(v["fillPrice"], "1.998");
    assert_eq!(v["feePpm"], 1_000);
    assert_eq!(v["offeredDecimals"], 8);
    assert_eq!(v["requestedDecimals"], 8);
    assert!((v["asOf"].as_i64().unwrap() - now()).abs() <= 5, "{v}");
    // Buying A with B: 0.5 A per B divided by 1.001.
    let v: Value = h.server.get(&pair_url(faucet_b(), faucet_a())).await.json();
    assert_eq!(v["marketPrice"], "0.5");
    assert!(
        v["fillPrice"].as_str().unwrap().starts_with("0.4995004995"),
        "{v}"
    );
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn pair_price_fails_closed_without_a_fresh_price() {
    async fn error(h: &Harness, url: String) -> (StatusCode, Value) {
        let r = h.server.get(&url).await;
        (r.status_code(), r.json::<Value>()["error"].clone())
    }
    let h = swap_server(
        &registered(),
        None,
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await;
    assert_eq!(
        error(&h, pair_url(faucet_a(), faucet_b())).await,
        (StatusCode::SERVICE_UNAVAILABLE, Value::from("no_market"))
    );
    assert_eq!(
        error(&h, pair_url(faucet_unregistered(), faucet_b()))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        error(&h, pair_url(faucet_a(), faucet_a())).await.0,
        StatusCode::BAD_REQUEST
    );
    let h = swap_server_at(
        false,
        &registered(),
        Some("2"),
        SwapBookSnapshot::default(),
        SettlementStats::new(),
    )
    .await;
    assert_eq!(
        error(&h, pair_url(faucet_a(), faucet_b())).await,
        (StatusCode::SERVICE_UNAVAILABLE, Value::from("no_price"))
    );
}

fn faucet_unregistered() -> AccountId {
    use miden_protocol::testing::account_id::ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2;
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap()
}

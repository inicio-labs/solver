use std::collections::HashMap;
use std::time::Duration;

use miden_protocol::account::AccountId;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};

use crate::admin::{self, AdminState};
use crate::db;
use crate::ingest::tests::MockMidenClient;
use crate::ingest::MidenClient;
use crate::matching::price_feed::PriceFeed;
use crate::pipeline::{PipelineConfig, spawn_pipeline};
use crate::price::{MockPriceClient, PriceClient, PriceSnapshot, WatchPriceFeed};
use crate::types::TokenId;
use std::sync::Arc;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helper: create two distinct faucet TokenIds for testing
// ---------------------------------------------------------------------------

fn test_token_a() -> TokenId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
}

fn test_token_b() -> TokenId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
}

fn test_db_pool() -> db::DbPool {
    db::init_db(":memory:").expect("failed to create in-memory DB")
}

// ===========================================================================
// 2. Price module tests
// ===========================================================================

#[test]
fn watch_price_feed_new_returns_empty() {
    let feed = WatchPriceFeed::new();
    // Unknown tokens fall back to 1 cent.
    assert_eq!(feed.usd_price_cents(test_token_a()), 1);
}

#[test]
fn watch_price_feed_set_price_cents() {
    let mut feed = WatchPriceFeed::new();
    let token = test_token_a();
    feed.set_price_cents(token, 200_000);
    assert_eq!(feed.usd_price_cents(token), 200_000);
}

#[test]
fn watch_price_feed_from_map() {
    let token_a = test_token_a();
    let token_b = test_token_b();

    let mut prices: PriceSnapshot = HashMap::new();
    prices.insert(token_a, 200_000);
    prices.insert(token_b, 100);

    let feed = WatchPriceFeed::from_map(prices);
    assert_eq!(feed.usd_price_cents(token_a), 200_000);
    assert_eq!(feed.usd_price_cents(token_b), 100);
}

#[test]
fn watch_price_feed_default_same_as_new() {
    let a = WatchPriceFeed::new();
    let b = WatchPriceFeed::default();
    let token = test_token_a();
    assert_eq!(a.usd_price_cents(token), b.usd_price_cents(token));
}

#[test]
fn watch_price_feed_implements_price_feed_trait() {
    let token_a = test_token_a();
    let token_b = test_token_b();

    let mut feed = WatchPriceFeed::new();
    // token_a is "ETH" at $2000, token_b is "USDC" at $1
    feed.set_price_cents(token_a, 200_000);
    feed.set_price_cents(token_b, 100);

    // Offering 1 ETH ($2000) for 1500 USDC ($1500) -> profitable
    assert!(feed.is_order_profitable(token_a, 1, token_b, 1500));

    // Offering 1 ETH ($2000) for 2500 USDC ($2500) -> not profitable
    assert!(!feed.is_order_profitable(token_a, 1, token_b, 2500));

    // Exact break-even: offering 1 ETH ($2000) for 2000 USDC ($2000) -> profitable (>=)
    assert!(feed.is_order_profitable(token_a, 1, token_b, 2000));
}

#[test]
fn watch_price_feed_from_watch_channel() {
    let token = test_token_a();
    let mut prices: PriceSnapshot = HashMap::new();
    prices.insert(token, 42_00);

    let (_tx, rx) = tokio::sync::watch::channel(prices);
    let feed = WatchPriceFeed::from_watch(&rx);
    assert_eq!(feed.usd_price_cents(token), 42_00);
}

#[tokio::test]
async fn mock_price_client_returns_expected_prices() {
    let token_a = test_token_a();
    let token_b = test_token_b();

    let mut prices: PriceSnapshot = HashMap::new();
    prices.insert(token_a, 200_000);
    prices.insert(token_b, 100);

    let client = MockPriceClient::new(prices.clone());

    let result = client.fetch_prices(&[token_a, token_b]).await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[&token_a], 200_000);
    assert_eq!(result[&token_b], 100);
}

#[tokio::test]
async fn mock_price_client_ignores_token_filter() {
    // MockPriceClient always returns all configured prices regardless of the tokens arg.
    let token_a = test_token_a();

    let mut prices: PriceSnapshot = HashMap::new();
    prices.insert(token_a, 500);

    let client = MockPriceClient::new(prices);

    // Ask for empty token list -- still returns all prices.
    let result = client.fetch_prices(&[]).await.unwrap();
    assert_eq!(result[&token_a], 500);
}

// ===========================================================================
// 3. Admin module tests
// ===========================================================================

#[test]
fn seed_tokens_from_config_inserts_tokens() {
    let pool = test_db_pool();
    let tokens = vec![test_token_a(), test_token_b()];

    admin::seed_tokens_from_config(&pool, &tokens).unwrap();

    let mut conn = pool.get().unwrap();
    let rows = db::get_registered_tokens(&mut conn).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn seed_tokens_from_config_is_idempotent() {
    let pool = test_db_pool();
    let tokens = vec![test_token_a()];

    admin::seed_tokens_from_config(&pool, &tokens).unwrap();
    admin::seed_tokens_from_config(&pool, &tokens).unwrap();

    let mut conn = pool.get().unwrap();
    let rows = db::get_registered_tokens(&mut conn).unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn load_tokens_from_db_round_trips() {
    let pool = test_db_pool();
    let token_a = test_token_a();
    let token_b = test_token_b();
    let tokens = vec![token_a, token_b];

    admin::seed_tokens_from_config(&pool, &tokens).unwrap();

    let mock_client = MockMidenClient::new();
    let shared_client: Arc<Mutex<dyn MidenClient + Send>> = Arc::new(Mutex::new(mock_client));
    let state = AdminState::new(pool, shared_client);

    let loaded = state.load_tokens_from_db().unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded.contains(&token_a));
    assert!(loaded.contains(&token_b));
}

#[test]
fn token_id_serialization_round_trip() {
    let token = test_token_a();

    let mut bytes = Vec::new();
    token.write_into(&mut bytes);

    let deserialized = TokenId::read_from(&mut SliceReader::new(&bytes)).unwrap();
    assert_eq!(deserialized, token);
}

#[test]
fn token_id_hex_round_trip() {
    let token = test_token_a();

    let mut bytes = Vec::new();
    token.write_into(&mut bytes);
    let hex_str = hex::encode(&bytes);

    let decoded = hex::decode(&hex_str).unwrap();
    let recovered = TokenId::read_from(&mut SliceReader::new(&decoded)).unwrap();
    assert_eq!(recovered, token);
}

// ===========================================================================
// 4. Ingest / MockMidenClient tests
// ===========================================================================

#[tokio::test]
async fn mock_miden_client_sync_returns_empty_initially() {
    let mut client = MockMidenClient::new();
    let result = client.sync_state().await.unwrap();
    assert_eq!(result.block_num, 0);
    assert!(result.new_note_ids.is_empty());
}

#[tokio::test]
async fn mock_miden_client_subscribe_pair_succeeds() {
    let mut client = MockMidenClient::new();
    let result = client
        .subscribe_pair(test_token_a(), test_token_b())
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn mock_miden_client_get_notes_empty_ids() {
    let client = MockMidenClient::new();
    let notes = client.get_notes_by_ids(&[]).await.unwrap();
    assert!(notes.is_empty());
}

// ===========================================================================
// 5. Pipeline spawn tests
// ===========================================================================

#[tokio::test]
async fn spawn_pipeline_starts_successfully() {
    // Use a random port to avoid conflicts with other tests.
    let port = 30000 + (std::process::id() % 10000) as u16;

    let config = PipelineConfig {
        database_url: ":memory:".to_string(),
        ingest_interval: Duration::from_secs(3600), // long interval so it doesn't tick
        price_interval: Duration::from_secs(3600),
        match_interval: Duration::from_secs(3600),
        initial_tokens: vec![test_token_a(), test_token_b()],
        admin_port: port,
    };

    let client = MockMidenClient::new();
    let prices = {
        let mut p: PriceSnapshot = HashMap::new();
        p.insert(test_token_a(), 200_000);
        p.insert(test_token_b(), 100);
        p
    };
    let price_client = MockPriceClient::new(prices);

    let handles = spawn_pipeline(config, client, price_client).await.unwrap();

    // Verify all handles are running (not finished).
    assert!(!handles.ingest_handle.is_finished());
    assert!(!handles.price_handle.is_finished());
    assert!(!handles.matcher_handle.is_finished());
    assert!(!handles.admin_handle.is_finished());

    // Clean up: abort spawned tasks.
    handles.ingest_handle.abort();
    handles.price_handle.abort();
    handles.matcher_handle.abort();
    handles.admin_handle.abort();
}

#[tokio::test]
async fn pipeline_channels_are_functional() {
    let port = 31000 + (std::process::id() % 10000) as u16;

    let config = PipelineConfig {
        database_url: ":memory:".to_string(),
        ingest_interval: Duration::from_secs(3600),
        price_interval: Duration::from_secs(3600),
        match_interval: Duration::from_secs(3600),
        initial_tokens: vec![test_token_a()],
        admin_port: port,
    };

    let prices = {
        let mut p: PriceSnapshot = HashMap::new();
        p.insert(test_token_a(), 500);
        p
    };

    let client = MockMidenClient::new();
    let price_client = MockPriceClient::new(prices.clone());

    let handles = spawn_pipeline(config, client, price_client).await.unwrap();

    // The exec_rx channel should be open (sender still held by matcher task).
    assert!(handles.exec_rx.is_empty());

    // Clean up.
    handles.ingest_handle.abort();
    handles.price_handle.abort();
    handles.matcher_handle.abort();
    handles.admin_handle.abort();
}

#[tokio::test]
async fn pipeline_admin_server_binds_and_responds() {
    let port = 32000 + (std::process::id() % 10000) as u16;

    let config = PipelineConfig {
        database_url: ":memory:".to_string(),
        ingest_interval: Duration::from_secs(3600),
        price_interval: Duration::from_secs(3600),
        match_interval: Duration::from_secs(3600),
        initial_tokens: vec![test_token_a()],
        admin_port: port,
    };

    let client = MockMidenClient::new();
    let price_client = MockPriceClient::new(HashMap::new());

    let handles = spawn_pipeline(config, client, price_client).await.unwrap();

    // Give the admin server a moment to bind.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify the admin server is responsive by connecting to it.
    let addr = format!("127.0.0.1:{port}");
    let conn_result = tokio::net::TcpStream::connect(&addr).await;
    assert!(
        conn_result.is_ok(),
        "Admin server should be accepting connections on port {port}"
    );

    // Clean up.
    handles.ingest_handle.abort();
    handles.price_handle.abort();
    handles.matcher_handle.abort();
    handles.admin_handle.abort();
}

#[tokio::test]
async fn subscribe_all_pairs_with_two_tokens() {
    let pool = test_db_pool();
    let token_a = test_token_a();
    let token_b = test_token_b();

    admin::seed_tokens_from_config(&pool, &[token_a, token_b]).unwrap();

    let mock_client = MockMidenClient::new();
    let shared_client: Arc<Mutex<dyn MidenClient + Send>> = Arc::new(Mutex::new(mock_client));
    let state = Arc::new(AdminState::new(pool, shared_client));

    // subscribe_all_pairs should succeed without errors.
    let result = state.subscribe_all_pairs().await;
    assert!(result.is_ok());
}

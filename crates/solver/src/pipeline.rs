use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;

use crate::admin::AdminState;
use crate::db;
use crate::ingest::{self, MidenClient};
use crate::matcher;
use crate::price::{self, PriceClient, PriceSnapshot};
use crate::types::{ExecutionBatch, IngestOrder, TokenId};

/// Configuration for the pipeline.
pub struct PipelineConfig {
    pub database_url: String,
    pub read_pool_size: u32,
    pub ingest_interval: Duration,
    pub price_interval: Duration,
    pub match_interval: Duration,
    pub initial_tokens: Vec<TokenId>,
    pub admin_port: u16,
}

/// Handles returned by spawn_pipeline for graceful shutdown.
pub struct PipelineHandles {
    pub ingest_handle: JoinHandle<()>,
    pub price_handle: JoinHandle<()>,
    pub matcher_handle: JoinHandle<()>,
    pub admin_handle: JoinHandle<()>,
    /// Execution channel receiver — executor consumes from this.
    pub exec_rx: mpsc::Receiver<ExecutionBatch>,
}

pub async fn subscribe_all_pairs(
    db_pool: &db::DbPool,
    client: &mut dyn MidenClient,
) -> anyhow::Result<()> {
    let tokens = db::load_registered_tokens(db_pool)?;
    for i in 0..tokens.len() {
        for j in 0..tokens.len() {
            if i != j {
                client.subscribe_pair(tokens[i], tokens[j]).await?;
            }
        }
    }
    Ok(())
}

/// Spawn the pipeline: ingest → matcher → exec_rx.
///
/// Returns handles and `exec_rx`. The caller is responsible for wiring
/// the executor (e.g. `executor::run_executor`) to consume from `exec_rx`.
pub async fn spawn_pipeline<C, P>(
    config: PipelineConfig,
    client: C,
    price_client: P,
) -> Result<PipelineHandles>
where
    C: MidenClient + Send + 'static,
    P: PriceClient + Send + 'static,
{
    // Initialize database
    let db_pool = db::init_db(&config.database_url, config.read_pool_size)?;

    // Seed initial tokens from config
    db::seed_tokens_from_config(&db_pool, &config.initial_tokens)?;

    // Create channels
    let (order_tx, order_rx) = mpsc::channel::<IngestOrder>(5000);
    let (price_tx, price_rx) = watch::channel::<PriceSnapshot>(HashMap::new());
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionBatch>(5000);

    // Shared client for ingest + admin
    let shared_client: Arc<Mutex<dyn MidenClient + Send>> = Arc::new(Mutex::new(client));

    // Subscribe to all registered token pairs
    subscribe_all_pairs(&db_pool, &mut *shared_client.lock().await).await?;
    let admin_state = Arc::new(AdminState::new(db_pool.clone(), shared_client.clone()));

    // Spawn ingest task
    let ingest_client = shared_client.clone();
    let ingest_pool = db_pool.clone();
    let ingest_interval = config.ingest_interval;
    let ingest_handle = tokio::spawn(async move {
        ingest::run_ingest(ingest_client, ingest_pool, order_tx, ingest_interval).await;
    });

    // Spawn price feed task — reloads token list from DB each tick
    let price_pool = db_pool.clone();
    let price_interval = config.price_interval;
    let price_handle = tokio::spawn(async move {
        price::run_price_feed(price_client, price_pool, price_tx, price_interval).await;
    });

    // Spawn matcher task
    let match_interval = config.match_interval;
    let matcher_handle = tokio::spawn(async move {
        matcher::run_matcher(order_rx, price_rx, exec_tx, match_interval).await;
    });

    // Spawn admin server
    let admin_router = admin_state.router();
    let admin_port = config.admin_port;
    let admin_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{admin_port}"))
            .await
            .expect("failed to bind admin port");
        axum::serve(listener, admin_router)
            .await
            .expect("admin server failed");
    });

    Ok(PipelineHandles {
        ingest_handle,
        price_handle,
        matcher_handle,
        admin_handle,
        exec_rx,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use miden_protocol::account::AccountId;
    use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    use crate::admin::AdminState;
    use crate::db;
    use crate::ingest::tests::MockMidenClient;
    use crate::ingest::MidenClient;
    use crate::matching::price_feed::PriceFeed;
    use crate::price::{MockPriceClient, PriceClient, WatchPriceFeed};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn test_token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn test_token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    fn test_db_pool() -> db::DbPool {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let url = format!("file:pipelinetest{}?mode=memory&cache=shared", n);
        db::init_db(&url, 1).expect("failed to create in-memory DB")
    }

    #[test]
    fn watch_price_feed_new_returns_empty() {
        let feed = WatchPriceFeed::new();
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
        feed.set_price_cents(token_a, 200_000);
        feed.set_price_cents(token_b, 100);

        assert!(feed.is_order_profitable(token_a, 1, token_b, 1500));
        assert!(!feed.is_order_profitable(token_a, 1, token_b, 2500));
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
        let token_a = test_token_a();

        let mut prices: PriceSnapshot = HashMap::new();
        prices.insert(token_a, 500);

        let client = MockPriceClient::new(prices);
        let result = client.fetch_prices(&[]).await.unwrap();
        assert_eq!(result[&token_a], 500);
    }

    #[test]
    fn seed_tokens_from_config_inserts_tokens() {
        let pool = test_db_pool();
        let tokens = vec![test_token_a(), test_token_b()];

        db::seed_tokens_from_config(&pool, &tokens).unwrap();

        let mut conn = pool.read_conn().unwrap();
        let rows = db::get_registered_tokens(&mut conn).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn seed_tokens_from_config_is_idempotent() {
        let pool = test_db_pool();
        let tokens = vec![test_token_a()];

        db::seed_tokens_from_config(&pool, &tokens).unwrap();
        db::seed_tokens_from_config(&pool, &tokens).unwrap();

        let mut conn = pool.read_conn().unwrap();
        let rows = db::get_registered_tokens(&mut conn).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn load_tokens_from_db_round_trips() {
        let pool = test_db_pool();
        let token_a = test_token_a();
        let token_b = test_token_b();

        db::seed_tokens_from_config(&pool, &[token_a, token_b]).unwrap();

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

    #[tokio::test]
    async fn mock_miden_client_sync_returns_empty_initially() {
        let mut client = MockMidenClient::new();
        let result = client.sync_state().await.unwrap();
        assert_eq!(result.block_num, 0);
        assert!(result.new_notes.is_empty());
    }

    #[tokio::test]
    async fn mock_miden_client_subscribe_pair_succeeds() {
        let mut client = MockMidenClient::new();
        let result = client.subscribe_pair(test_token_a(), test_token_b()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mock_miden_client_sync_returns_no_notes_initially() {
        let mut client = MockMidenClient::new();
        let result = client.sync_state().await.unwrap();
        assert!(result.new_notes.is_empty());
    }

    #[tokio::test]
    async fn spawn_pipeline_starts_successfully() {
        let port = 30000 + (std::process::id() % 10000) as u16;

        let config = PipelineConfig {
            database_url: "file:spawn_pipeline_test?mode=memory&cache=shared".to_string(),
            read_pool_size: 1,
            ingest_interval: Duration::from_secs(3600),
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

        assert!(!handles.ingest_handle.is_finished());
        assert!(!handles.price_handle.is_finished());
        assert!(!handles.matcher_handle.is_finished());
        assert!(!handles.admin_handle.is_finished());

        handles.ingest_handle.abort();
        handles.price_handle.abort();
        handles.matcher_handle.abort();
        handles.admin_handle.abort();
    }

    #[tokio::test]
    async fn pipeline_channels_are_functional() {
        let port = 31000 + (std::process::id() % 10000) as u16;

        let config = PipelineConfig {
            database_url: "file:pipeline_channels_test?mode=memory&cache=shared".to_string(),
            read_pool_size: 1,
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
        let price_client = MockPriceClient::new(prices);

        let handles = spawn_pipeline(config, client, price_client).await.unwrap();

        assert!(handles.exec_rx.is_empty());

        handles.ingest_handle.abort();
        handles.price_handle.abort();
        handles.matcher_handle.abort();
        handles.admin_handle.abort();
    }

    #[tokio::test]
    async fn pipeline_admin_server_binds_and_responds() {
        let port = 32000 + (std::process::id() % 10000) as u16;

        let config = PipelineConfig {
            database_url: "file:pipeline_admin_test?mode=memory&cache=shared".to_string(),
            read_pool_size: 1,
            ingest_interval: Duration::from_secs(3600),
            price_interval: Duration::from_secs(3600),
            match_interval: Duration::from_secs(3600),
            initial_tokens: vec![test_token_a()],
            admin_port: port,
        };

        let client = MockMidenClient::new();
        let price_client = MockPriceClient::new(HashMap::new());

        let handles = spawn_pipeline(config, client, price_client).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let addr = format!("127.0.0.1:{port}");
        let conn_result = tokio::net::TcpStream::connect(&addr).await;
        assert!(
            conn_result.is_ok(),
            "Admin server should be accepting connections on port {port}"
        );

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

        db::seed_tokens_from_config(&pool, &[token_a, token_b]).unwrap();

        let mut mock_client = MockMidenClient::new();
        let result = subscribe_all_pairs(&pool, &mut mock_client).await;
        assert!(result.is_ok());
    }
}

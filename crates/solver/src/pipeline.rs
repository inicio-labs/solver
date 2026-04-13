use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;

use crate::admin::{self, AdminState};
use crate::db;
use crate::ingest::{self, MidenClient};
use crate::matcher;
use crate::price::{self, PriceClient, PriceSnapshot};
use crate::types::{ExecutionBatch, IngestOrder, TokenId};

/// Configuration for the pipeline.
pub struct PipelineConfig {
    pub database_url: String,
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
    let pool = db::init_db(&config.database_url)?;

    // Seed initial tokens from config
    admin::seed_tokens_from_config(&pool, &config.initial_tokens)?;

    // Create channels
    let (order_tx, order_rx) = mpsc::channel::<IngestOrder>(5000);
    let (price_tx, price_rx) = watch::channel::<PriceSnapshot>(HashMap::new());
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionBatch>(5000);

    // Shared client for ingest + admin
    let shared_client: Arc<Mutex<dyn MidenClient + Send>> = Arc::new(Mutex::new(client));

    // Subscribe to all registered token pairs
    let admin_state = Arc::new(AdminState::new(pool.clone(), shared_client.clone()));
    admin_state.subscribe_all_pairs().await?;

    // Load token list for price fetcher
    let tokens = admin_state.load_tokens_from_db()?;

    // Spawn ingest task
    let ingest_client = shared_client.clone();
    let ingest_pool = pool.clone();
    let ingest_interval = config.ingest_interval;
    let ingest_handle = tokio::spawn(async move {
        ingest::run_ingest(ingest_client, ingest_pool, order_tx, ingest_interval).await;
    });

    // Spawn price feed task
    let price_interval = config.price_interval;
    let price_handle = tokio::spawn(async move {
        price::run_price_feed(price_client, tokens, price_tx, price_interval).await;
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

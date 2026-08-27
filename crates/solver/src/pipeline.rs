use anyhow::Result;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::NoteId;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admin::AdminState;
use crate::config::EngineConfig;
use crate::db;
use crate::ingest::{self, MidenClient};
use crate::matcher;
use crate::matching::types::SwapBookSnapshot;
use crate::price::{self, PreciseSnapshot, PriceClient, PriceSnapshot, SharedTokenMap};
use crate::swap_eta::SettlementStats;
use crate::router::{RouteBatch, QuotesSnapshot};
use crate::types::{ExecutionBatch, IngestOrder, TokenId};

/// Bounded buffer for the high-volume pipeline channels (orders, exec
/// batches, consumed-note notifications), used by `create_channels`.
const PIPELINE_CHANNEL_BUF: usize = 5000;
/// Admin → subscribe-relay buffer. Low-traffic (infrequent operator actions).
const SUBSCRIBE_CHANNEL_BUF: usize = 100;

/// Configuration for the pipeline.
pub struct PipelineConfig {
    /// Pre-initialised DB pool. Caller owns construction so the same pool can
    /// be shared with `HttpPriceClient` (which hydrates the symbol cache from
    /// it at boot).
    pub db_pool: db::DbPool,
    pub ingest_interval: Duration,
    pub price_interval: Duration,
    pub match_interval: Duration,
    /// Tokens to register at boot, each with an optional CoinGecko-style
    /// external symbol for price-feed lookups. Seeded from `solver.toml`
    /// `[[pairs]]` entries.
    pub initial_tokens: Vec<(TokenId, Option<String>)>,
    pub admin_port: u16,
    pub admin_token: Option<String>,
    /// Enable the 3-edge cycle (triangular) matching phase. Defaults to `true`
    /// in [`EngineConfig`]; disable to skip the O(T³) enumeration when only
    /// direct matching is desired (e.g. with a large registered token set).
    pub triangular_enabled: bool,
    /// Shared in-memory faucet-id → external-symbol cache. Hydrated from DB
    /// at boot and mutated by admin handlers. Pass the same Arc as the one
    /// used to construct the `HttpPriceClient` so both see the latest mapping.
    pub token_map: SharedTokenMap,
    /// Cancellation signal for graceful shutdown. Triggered by the binary
    /// on Ctrl-C (or any external shutdown event). Each pipeline task watches
    /// this token via `tokio::select!` and exits cleanly between iterations.
    pub cancel: CancellationToken,
    /// Shared `last successful sync` timestamp (unix seconds), bumped by the
    /// ingest task after every successful `sync_state`. Wired through to the
    /// observability `/readyz` endpoint via `obs::ObsState`. Passing the same
    /// `Arc` to both sides makes readiness reflect real ingest progress.
    pub last_sync_unix_seconds: Arc<AtomicI64>,
}

impl PipelineConfig {
    /// Assemble from the parsed [`EngineConfig`] plus the caller-owned
    /// runtime handles. Centralises the millisecond → [`Duration`]
    /// conversions so that unit mapping lives in exactly one place; every
    /// field stays mandatory (the struct has no defaults), so a forgotten
    /// argument is still a compile error at the call site.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: &EngineConfig,
        db_pool: db::DbPool,
        initial_tokens: Vec<(TokenId, Option<String>)>,
        admin_token: Option<String>,
        token_map: SharedTokenMap,
        cancel: CancellationToken,
        last_sync_unix_seconds: Arc<AtomicI64>,
    ) -> Self {
        Self {
            db_pool,
            ingest_interval: Duration::from_millis(engine.fetch_interval_ms),
            price_interval: Duration::from_millis(engine.price_interval_ms),
            match_interval: Duration::from_millis(engine.pulse_interval_ms),
            initial_tokens,
            admin_port: engine.admin_port,
            admin_token,
            triangular_enabled: engine.triangular_enabled,
            token_map,
            cancel,
            last_sync_unix_seconds,
        }
    }
}

pub async fn subscribe_all_pairs(
    db_pool: &db::DbPool,
    client: &mut dyn MidenClient,
) -> anyhow::Result<()> {
    let tokens = db::load_registered_tokens(db_pool)?;
    // Cache each registered token's on-chain metadata once, at boot. Config
    // `[[pairs]]` are seeded into the DB before this runs (`prepare_db`), so this
    // is the registration point for config tokens — the runtime (admin) path is
    // handled by the subscribe relay below.
    for token in &tokens {
        ensure_token_metadata(client, db_pool, *token).await;
    }
    for i in 0..tokens.len() {
        for j in 0..tokens.len() {
            if i != j {
                client.subscribe_pair(tokens[i], tokens[j]).await?;
            }
        }
    }
    Ok(())
}

/// Fetch a token's on-chain metadata (decimals, ticker) from the node and cache
/// it in `registered_tokens` — but only the first time, since decimals are an
/// immutable faucet property. Idempotent (a no-op once `decimals` is set) and
/// best-effort: on any RPC/DB error it logs and leaves the row NULL, so the next
/// registration touching this token (or a restart) retries. Must run on the
/// ingest thread — the only place the `!Send` Miden client lives.
async fn ensure_token_metadata(
    client: &mut dyn MidenClient,
    pool: &db::DbPool,
    token: TokenId,
) {
    let key = token.to_bytes();

    // Already cached? Check first so we never re-hit the RPC for a known token
    // (the admin path replays a token across every pair it forms).
    match pool.write_conn() {
        Ok(mut conn) => match db::get_registered_token(&mut conn, &key) {
            Ok(Some(row)) if row.decimals.is_some() => return, // already have it
            Ok(Some(_)) => {}                                  // registered, still missing → fetch
            Ok(None) => return,                                // not registered → nothing to annotate
            Err(e) => {
                tracing::warn!(%token, error = %e, "ensure_token_metadata: db read failed");
                return;
            }
        },
        Err(e) => {
            tracing::warn!(%token, error = %e, "ensure_token_metadata: write_conn failed");
            return;
        }
    }

    match client.fetch_token_metadata(token).await {
        Ok(Some((decimals, ticker))) => match pool.write_conn() {
            Ok(mut conn) => {
                if let Err(e) = db::set_token_metadata(
                    &mut conn,
                    &key,
                    Some(i32::from(decimals)),
                    Some(&ticker),
                ) {
                    tracing::warn!(%token, error = %e, "ensure_token_metadata: persist failed");
                } else {
                    tracing::info!(%token, decimals, ticker = %ticker, "fetched on-chain token metadata");
                }
            }
            Err(e) => tracing::warn!(%token, error = %e, "ensure_token_metadata: write_conn failed"),
        },
        Ok(None) => {
            tracing::debug!(%token, "no on-chain metadata (private/non-faucet); will retry on next registration")
        }
        Err(e) => {
            tracing::warn!(%token, error = %e, "fetch_token_metadata failed; will retry on next registration")
        }
    }
}

// ===========================================================================
// Decomposed pipeline (L2). The client-bound tasks (ingest, subscribe-relay)
// live on the ingest OS thread; the `Send` services (matcher, price, admin)
// stay on the main coordination thread. Channels are created once and split
// between them — all channel payloads are `Send`. (The former single-thread
// `spawn_pipeline`/`PipelineHandles` were removed: production used the
// decomposed path and they were exercised only by their own unit tests.)
// ===========================================================================

/// Cross-thread channel endpoints, created once on the main thread and split
/// between the main coordination thread and the client threads.
pub struct PipelineChannels {
    pub order_tx: mpsc::Sender<IngestOrder>,
    pub order_rx: mpsc::Receiver<IngestOrder>,
    pub consumed_tx: mpsc::Sender<NoteId>,
    pub consumed_rx: mpsc::Receiver<NoteId>,
    pub price_tx: watch::Sender<PriceSnapshot>,
    pub price_rx: watch::Receiver<PriceSnapshot>,
    /// Full-precision price side-channel — consumed only by the price-query API.
    pub precise_tx: watch::Sender<PreciseSnapshot>,
    pub precise_rx: watch::Receiver<PreciseSnapshot>,
    /// Top-of-book snapshot (matcher → swap-eta API), latest-wins.
    pub swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    pub swap_snapshot_rx: watch::Receiver<Arc<SwapBookSnapshot>>,
    /// In-memory settlement-time window (executor → swap-eta API), latest-wins.
    pub stats_tx: watch::Sender<Arc<SettlementStats>>,
    pub stats_rx: watch::Receiver<Arc<SettlementStats>>,
    pub exec_tx: mpsc::Sender<ExecutionBatch>,
    pub exec_rx: mpsc::Receiver<ExecutionBatch>,
    pub subscribe_tx: mpsc::Sender<(TokenId, TokenId)>,
    pub subscribe_rx: mpsc::Receiver<(TokenId, TokenId)>,
    /// Standing DEX quotes: router → matcher (latest-wins).
    pub quotes_tx: watch::Sender<Arc<QuotesSnapshot>>,
    pub quotes_rx: watch::Receiver<Arc<QuotesSnapshot>>,
    /// Selected notes: matcher → router (delivered to DEXes over websocket).
    pub route_tx: mpsc::Sender<RouteBatch>,
    pub route_rx: mpsc::Receiver<RouteBatch>,
}

pub fn create_channels() -> PipelineChannels {
    let (order_tx, order_rx) = mpsc::channel::<IngestOrder>(PIPELINE_CHANNEL_BUF);
    let (consumed_tx, consumed_rx) = mpsc::channel::<NoteId>(PIPELINE_CHANNEL_BUF);
    let (price_tx, price_rx) = watch::channel::<PriceSnapshot>(HashMap::new());
    let (precise_tx, precise_rx) = watch::channel::<PreciseSnapshot>(HashMap::new());
    // Two separate swap-eta feeds, NOT one combined channel: they have two
    // independent producers on two threads — the matcher publishes the live
    // top-of-book each tick (fillability), the executor publishes settlement
    // durations after each settlement (the 24h median). A `watch` value has
    // replace semantics, so co-writing one struct from both would need a shared
    // lock + read-modify-write (lost-update race). One channel per producer =
    // each is the sole, lock-free writer of its own stream. Both read by the
    // swap-eta handler in price_api.rs.
    let (swap_snapshot_tx, swap_snapshot_rx) =
        watch::channel::<Arc<SwapBookSnapshot>>(Arc::new(SwapBookSnapshot::new()));
    let (stats_tx, stats_rx) = watch::channel::<Arc<SettlementStats>>(Arc::new(SettlementStats::new()));
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionBatch>(PIPELINE_CHANNEL_BUF);
    let (subscribe_tx, subscribe_rx) = mpsc::channel::<(TokenId, TokenId)>(SUBSCRIBE_CHANNEL_BUF);
    let (quotes_tx, quotes_rx) = watch::channel::<Arc<QuotesSnapshot>>(Arc::new(std::collections::HashMap::new()));
    let (route_tx, route_rx) = mpsc::channel::<RouteBatch>(PIPELINE_CHANNEL_BUF);
    PipelineChannels {
        order_tx,
        order_rx,
        consumed_tx,
        consumed_rx,
        price_tx,
        price_rx,
        precise_tx,
        precise_rx,
        swap_snapshot_tx,
        swap_snapshot_rx,
        stats_tx,
        stats_rx,
        exec_tx,
        exec_rx,
        subscribe_tx,
        subscribe_rx,
        quotes_tx,
        quotes_rx,
        route_tx,
        route_rx,
    }
}

/// Boot recovery + token seed + symbol-map hydrate. Touches only the DB (no
/// miden client), so it runs on the main thread before any client thread.
pub fn prepare_db(config: &PipelineConfig) -> Result<()> {
    {
        let mut conn = config.db_pool.write_conn()?;
        let n = db::reset_all_settling_to_active(&mut conn)?;
        if n > 0 {
            tracing::info!(
                count = n,
                "boot recovery: reset Settling orders to Active for re-matching"
            );
        }
    }
    db::seed_tokens_from_config(&config.db_pool, &config.initial_tokens)?;
    {
        let loaded = db::load_token_symbols(&config.db_pool)?;
        let mut map = crate::price::write_token_map(&config.token_map);
        *map = loaded;
    }
    Ok(())
}

/// Handles for the `Send` services spawned on the main coordination thread.
pub struct CoreHandles {
    pub matcher_handle: JoinHandle<()>,
    pub price_handle: JoinHandle<()>,
    pub admin_handle: JoinHandle<()>,
}

/// Spawn the `Send` services (price feed, matcher, admin HTTP) on the
/// CALLER's LocalSet (the main coordination thread). None of these touch a
/// miden client. `prepare_db` must have been called first.
#[allow(clippy::too_many_arguments)]
pub fn spawn_core_services<P: PriceClient + 'static>(
    config: &PipelineConfig,
    price_client: P,
    order_rx: mpsc::Receiver<IngestOrder>,
    consumed_rx: mpsc::Receiver<NoteId>,
    price_tx: watch::Sender<PriceSnapshot>,
    price_rx: watch::Receiver<PriceSnapshot>,
    precise_tx: watch::Sender<PreciseSnapshot>,
    last_price_update: Arc<AtomicI64>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    subscribe_tx: mpsc::Sender<(TokenId, TokenId)>,
    router_hooks: Option<matcher::RouterHooks>,
) -> CoreHandles {
    // Price feed — broadcasts cents (matcher) + precise (price API) snapshots.
    let price_token_map = config.token_map.clone();
    let price_interval = config.price_interval;
    let price_cancel = config.cancel.clone();
    let price_handle = tokio::task::spawn_local(async move {
        tokio::select! {
            _ = price::run_price_feed(price_client, price_token_map, price_tx, precise_tx, last_price_update, price_interval) => {}
            _ = price_cancel.cancelled() => {}
        }
    });

    // Matcher.
    let match_interval = config.match_interval;
    let matcher_pool = config.db_pool.clone();
    let matcher_cancel = config.cancel.clone();
    let triangular_enabled = config.triangular_enabled;
    let matcher_handle = tokio::task::spawn_local(async move {
        matcher::run_matcher(
            matcher_pool,
            order_rx,
            consumed_rx,
            price_rx,
            exec_tx,
            match_interval,
            triangular_enabled,
            swap_snapshot_tx,
            router_hooks,
            matcher_cancel,
        )
        .await;
    });

    // Admin HTTP server.
    let admin_state = Arc::new(AdminState::new(
        config.db_pool.clone(),
        subscribe_tx,
        config.token_map.clone(),
    ));
    let admin_router = admin_state.router(config.admin_token.clone().map(Arc::new));
    let admin_port = config.admin_port;
    let admin_cancel = config.cancel.clone();
    let admin_handle = tokio::task::spawn_local(async move {
        let listener =
            match tokio::net::TcpListener::bind(format!("127.0.0.1:{admin_port}")).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        port = admin_port,
                        error = %e,
                        "failed to bind admin port; triggering graceful shutdown"
                    );
                    admin_cancel.cancel();
                    return;
                }
            };
        let shutdown_cancel = admin_cancel.clone();
        if let Err(e) = axum::serve(listener, admin_router)
            .with_graceful_shutdown(async move { shutdown_cancel.cancelled().await })
            .await
        {
            tracing::error!(error = %e, "admin server failed; triggering graceful shutdown");
            admin_cancel.cancel();
        }
    });

    CoreHandles {
        matcher_handle,
        price_handle,
        admin_handle,
    }
}

/// Handles for the `!Send` client-bound tasks spawned on the ingest thread.
pub struct IngestHandles {
    pub ingest_handle: JoinHandle<()>,
    pub subscribe_handle: JoinHandle<()>,
}

/// Spawn the `!Send` client-bound tasks (subscribe-relay + ingest) on the
/// INGEST thread's LocalSet. Subscribes all configured pairs first (uses the
/// ingest adapter), then spawns the relay + ingest loops. Must be called from
/// within the ingest thread's `LocalSet`.
pub async fn spawn_ingest_tasks(
    adapter: Arc<Mutex<dyn MidenClient>>,
    db_pool: db::DbPool,
    order_tx: mpsc::Sender<IngestOrder>,
    consumed_tx: mpsc::Sender<NoteId>,
    mut subscribe_rx: mpsc::Receiver<(TokenId, TokenId)>,
    ingest_interval: Duration,
    cancel: CancellationToken,
    last_sync_unix_seconds: Arc<AtomicI64>,
) -> Result<IngestHandles> {
    // Subscribe to all registered token pairs (uses the ingest client).
    subscribe_all_pairs(&db_pool, &mut *adapter.lock().await).await?;

    // Subscribe-relay task: admin (on the main thread) sends (offered,
    // requested) tuples across the channel; this task applies them via the
    // ingest client. It is also the registration point where a newly-registered
    // token's on-chain metadata is fetched (once) — admin can't, since the
    // `!Send` client lives on this thread.
    let subscribe_client = adapter.clone();
    let subscribe_pool = db_pool.clone();
    let subscribe_cancel = cancel.clone();
    let subscribe_handle = tokio::task::spawn_local(async move {
        loop {
            tokio::select! {
                _ = subscribe_cancel.cancelled() => break,
                Some((offered, requested)) = subscribe_rx.recv() => {
                    let mut client = subscribe_client.lock().await;
                    if let Err(e) = client.subscribe_pair(offered, requested).await {
                        tracing::warn!(%offered, %requested, error = %e, "subscribe_pair failed");
                    }
                    // Cache on-chain metadata for any token new to us (no-op if
                    // already known).
                    ensure_token_metadata(&mut *client, &subscribe_pool, offered).await;
                    ensure_token_metadata(&mut *client, &subscribe_pool, requested).await;
                }
                else => break,
            }
        }
    });

    // Ingest task.
    let ingest_handle = tokio::task::spawn_local(async move {
        ingest::run_ingest(
            adapter,
            db_pool,
            order_tx,
            consumed_tx,
            ingest_interval,
            cancel,
            last_sync_unix_seconds,
        )
        .await;
    });

    Ok(IngestHandles {
        ingest_handle,
        subscribe_handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        assert_eq!(feed.price_cents(test_token_a()), None);
    }

    #[test]
    fn watch_price_feed_set_price_cents() {
        let mut feed = WatchPriceFeed::new();
        let token = test_token_a();
        feed.set_price_cents(token, 200_000);
        assert_eq!(feed.price_cents(token), Some(200_000));
    }

    #[test]
    fn watch_price_feed_from_map() {
        let token_a = test_token_a();
        let token_b = test_token_b();

        let mut prices: PriceSnapshot = HashMap::new();
        prices.insert(token_a, 200_000);
        prices.insert(token_b, 100);

        let feed = WatchPriceFeed::from_map(prices);
        assert_eq!(feed.price_cents(token_a), Some(200_000));
        assert_eq!(feed.price_cents(token_b), Some(100));
    }

    #[test]
    fn is_order_profitable_excludes_unpriced_token() {
        let token_a = test_token_a();
        let token_b = test_token_b();
        let mut feed = WatchPriceFeed::new();
        feed.set_price_cents(token_a, 100);

        // requested side unpriced ⇒ excluded regardless of amounts.
        assert!(!feed.is_order_profitable(token_a, 1_000_000, token_b, 1));
        // offered side unpriced ⇒ excluded.
        assert!(!feed.is_order_profitable(token_b, 1_000_000, token_a, 1));

        // Both priced ⇒ normal profitability comparison resumes.
        feed.set_price_cents(token_b, 100);
        assert!(feed.is_order_profitable(token_a, 10, token_b, 10));
        assert!(!feed.is_order_profitable(token_a, 1, token_b, 10));
    }

    #[test]
    fn watch_price_feed_default_same_as_new() {
        let a = WatchPriceFeed::new();
        let b = WatchPriceFeed::default();
        let token = test_token_a();
        assert_eq!(a.price_cents(token), b.price_cents(token));
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
        assert_eq!(feed.price_cents(token), Some(42_00));
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
        // MockPriceClient stores cents as full-precision USD (cents / 100).
        assert_eq!(result[&token_a].usd, 2000.0);
        assert_eq!(result[&token_b].usd, 1.0);
    }

    #[tokio::test]
    async fn mock_price_client_ignores_token_filter() {
        let token_a = test_token_a();

        let mut prices: PriceSnapshot = HashMap::new();
        prices.insert(token_a, 500);

        let client = MockPriceClient::new(prices);
        let result = client.fetch_prices(&[]).await.unwrap();
        assert_eq!(result[&token_a].usd, 5.0);
    }

    #[test]
    fn seed_tokens_from_config_inserts_tokens() {
        let pool = test_db_pool();
        let tokens = vec![(test_token_a(), None), (test_token_b(), None)];

        db::seed_tokens_from_config(&pool, &tokens).unwrap();

        let mut conn = pool.read_conn().unwrap();
        let rows = db::get_registered_tokens(&mut conn).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn seed_tokens_from_config_is_idempotent() {
        let pool = test_db_pool();
        let tokens = vec![(test_token_a(), None)];

        db::seed_tokens_from_config(&pool, &tokens).unwrap();
        db::seed_tokens_from_config(&pool, &tokens).unwrap();

        let mut conn = pool.read_conn().unwrap();
        let rows = db::get_registered_tokens(&mut conn).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn load_tokens_from_db_round_trips() {
        use std::sync::RwLock;
        let pool = test_db_pool();
        let token_a = test_token_a();
        let token_b = test_token_b();

        db::seed_tokens_from_config(&pool, &[(token_a, None), (token_b, None)]).unwrap();

        let (subscribe_tx, _rx) = mpsc::channel::<(TokenId, TokenId)>(8);
        let token_map = Arc::new(RwLock::new(HashMap::new()));
        let state = AdminState::new(pool, subscribe_tx, token_map);

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
    async fn subscribe_all_pairs_with_two_tokens() {
        let pool = test_db_pool();
        let token_a = test_token_a();
        let token_b = test_token_b();

        db::seed_tokens_from_config(&pool, &[(token_a, None), (token_b, None)]).unwrap();

        let mut mock_client = MockMidenClient::new();
        let result = subscribe_all_pairs(&pool, &mut mock_client).await;
        assert!(result.is_ok());
    }

    /// Metadata is fetched + cached at registration (the subscribe pass), not on
    /// an ingest tick.
    #[tokio::test]
    async fn subscribe_all_pairs_caches_on_chain_metadata() {
        let pool = test_db_pool();
        let token_a = test_token_a();
        let token_b = test_token_b();
        db::seed_tokens_from_config(&pool, &[(token_a, None), (token_b, None)]).unwrap();

        // Freshly seeded rows carry no metadata yet.
        let key_a = token_a.to_bytes();
        {
            let mut conn = pool.read_conn().unwrap();
            let row = db::get_registered_token(&mut conn, &key_a).unwrap().unwrap();
            assert!(row.decimals.is_none() && row.ticker.is_none());
        }

        // Subscribing (the boot registration point) fetches + persists it once.
        let mut mock_client = MockMidenClient::new();
        mock_client.set_token_metadata(8, "MTA");
        subscribe_all_pairs(&pool, &mut mock_client).await.unwrap();

        let mut conn = pool.read_conn().unwrap();
        let row = db::get_registered_token(&mut conn, &key_a).unwrap().unwrap();
        assert_eq!(row.decimals, Some(8));
        assert_eq!(row.ticker.as_deref(), Some("MTA"));
    }
}

use anyhow::{Context, Result};
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admin::AdminState;
use crate::config::EngineConfig;
use crate::db;
use crate::ingest::{self, MidenClient};
use crate::matcher;
use crate::matching::types::SwapBookSnapshot;
use crate::price::{self, PreciseSnapshot, PriceClient, SharedTokenMap};
use crate::router::{QuotesSnapshot, RouteBatch};
use crate::swap_eta::SettlementStats;
use crate::types::{BookUpdate, ExecutionBatch, TokenId};

/// Bounded buffer for the high-volume pipeline channels (orders, exec
/// batches, consumed-note notifications), used by `create_channels`.
const PIPELINE_CHANNEL_BUF: usize = 5000;
/// At most one batch waits for the executor. When it stops receiving
/// (verification mode) the matcher sees no free capacity on the next tick and
/// skips it, so orders stay live in its book instead of queueing at stale
/// prices.
const EXEC_CHANNEL_BUF: usize = 1;
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
    let tokens = db_pool
        .read(db::postgres_db::load_registered_tokens_tx)
        .await?;
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
async fn ensure_token_metadata(client: &mut dyn MidenClient, pool: &db::DbPool, token: TokenId) {
    let key = token.to_bytes();

    // Already cached? Check first so we never re-hit the RPC for a known token
    // (the admin path replays a token across every pair it forms).
    let lookup_key = key.clone();
    match pool
        .read(move |conn| db::postgres_db::get_registered_token_tx(conn, &lookup_key))
        .await
    {
        // Registered but still missing metadata: fetch it below.
        Ok(Some(row)) if row.decimals.is_none() => {}
        // Already annotated, or not registered: nothing to do.
        Ok(_) => return,
        Err(e) => {
            tracing::warn!(%token, error = %e, "ensure_token_metadata: db read failed");
            return;
        }
    }

    match client.fetch_token_metadata(token).await {
        Ok(Some((decimals, ticker))) => {
            let ticker_for_db = ticker.clone();
            let saved = pool
                .write(move |conn| {
                    db::postgres_db::set_token_metadata_tx(
                        conn,
                        token,
                        Some(decimals),
                        Some(&ticker_for_db),
                    )
                })
                .await;
            match saved {
                Ok(_) => {
                    tracing::info!(%token, decimals, ticker = %ticker, "fetched on-chain token metadata")
                }
                Err(error) => {
                    tracing::warn!(%token, %error, "ensure_token_metadata: persist failed")
                }
            }
        }
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
    pub quotes_tx: watch::Sender<Arc<QuotesSnapshot>>,
    pub quotes_rx: watch::Receiver<Arc<QuotesSnapshot>>,
    pub route_tx: mpsc::Sender<RouteBatch>,
    pub route_rx: mpsc::Receiver<RouteBatch>,
    pub book_tx: mpsc::Sender<BookUpdate>,
    pub book_rx: mpsc::Receiver<BookUpdate>,
    /// Exact references for clearing and f64 values for the price API.
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
}

pub fn create_channels() -> PipelineChannels {
    let (book_tx, book_rx) = mpsc::channel::<BookUpdate>(PIPELINE_CHANNEL_BUF);
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
    let (stats_tx, stats_rx) =
        watch::channel::<Arc<SettlementStats>>(Arc::new(SettlementStats::new()));
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionBatch>(EXEC_CHANNEL_BUF);
    let (subscribe_tx, subscribe_rx) = mpsc::channel::<(TokenId, TokenId)>(SUBSCRIBE_CHANNEL_BUF);
    let (quotes_tx, quotes_rx) = watch::channel(Arc::new(QuotesSnapshot::new()));
    let (route_tx, route_rx) = mpsc::channel(PIPELINE_CHANNEL_BUF);
    PipelineChannels {
        quotes_tx,
        quotes_rx,
        route_tx,
        route_rx,
        book_tx,
        book_rx,
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
    }
}

/// Token seed + symbol-map hydrate. Outstanding settlements stay reserved
/// until the executor reconciles them; resetting them could rematch a consumed parent.
pub async fn prepare_db(config: &PipelineConfig) -> Result<()> {
    let initial_tokens = config.initial_tokens.clone();
    config
        .db_pool
        .write(move |conn| db::postgres_db::seed_tokens_from_config_tx(conn, &initial_tokens))
        .await?;
    {
        let loaded = config
            .db_pool
            .read(db::postgres_db::load_token_symbols_tx)
            .await?;
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
    book_rx: mpsc::Receiver<BookUpdate>,
    precise_tx: watch::Sender<PreciseSnapshot>,
    last_price_update: Arc<AtomicI64>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    swap_snapshot_tx: watch::Sender<Arc<SwapBookSnapshot>>,
    subscribe_tx: mpsc::Sender<(TokenId, TokenId)>,
    clearing: matcher::ClearingRuntime,
) -> CoreHandles {
    // Price feed — publishes exact snapshots for clearing and the price API.
    let price_token_map = config.token_map.clone();
    let price_interval = config.price_interval;
    let price_cancel = config.cancel.clone();
    let price_handle = tokio::task::spawn_local(async move {
        tokio::select! {
            _ = price::run_price_feed(price_client, price_token_map, precise_tx, last_price_update, price_interval) => {}
            _ = price_cancel.cancelled() => {}
        }
    });

    // Matcher.
    let match_interval = config.match_interval;
    let matcher_cancel = config.cancel.clone();
    let matcher_handle = tokio::task::spawn_local(async move {
        if let Err(error) = matcher::run_matcher(
            book_rx,
            exec_tx,
            match_interval,
            swap_snapshot_tx,
            clearing,
            matcher_cancel.clone(),
        )
        .await
        {
            tracing::error!(%error, "matcher failed; requiring recovery");
            matcher_cancel.cancel();
        }
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
        let listener = match tokio::net::TcpListener::bind(format!("127.0.0.1:{admin_port}")).await
        {
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
#[allow(clippy::too_many_arguments)]
pub async fn spawn_ingest_tasks(
    adapter: Arc<Mutex<dyn MidenClient>>,
    db_pool: db::DbPool,
    book_tx: mpsc::Sender<BookUpdate>,
    mut subscribe_rx: mpsc::Receiver<(TokenId, TokenId)>,
    ingest_interval: Duration,
    cancel: CancellationToken,
    last_sync_unix_seconds: Arc<AtomicI64>,
    solver_id: miden_protocol::account::AccountId,
    clearing_bootstrap: oneshot::Sender<matcher::ClearingBootstrap>,
) -> Result<IngestHandles> {
    // Subscribe to all registered token pairs (uses the ingest client).
    subscribe_all_pairs(&db_pool, &mut *adapter.lock().await).await?;

    ingest::SyncResult::recover_postgres(&mut *adapter.lock().await, &db_pool, solver_id).await?;
    let bootstrap = reconcile_clearing_book(&db_pool, &mut *adapter.lock().await).await?;
    clearing_bootstrap
        .send(bootstrap)
        .map_err(|_| anyhow::anyhow!("clearing matcher stopped before startup reconciliation"))?;

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
            book_tx,
            ingest_interval,
            cancel,
            last_sync_unix_seconds,
            solver_id,
        )
        .await;
    });

    Ok(IngestHandles {
        ingest_handle,
        subscribe_handle,
    })
}

/// This is the clearer’s only database hydration. Fail closed if the chain
/// cannot confirm whether persisted inputs have already been consumed.
async fn reconcile_clearing_book(
    pool: &db::DbPool,
    client: &mut dyn MidenClient,
) -> Result<matcher::ClearingBootstrap> {
    let (mut orders, decimals) = pool
        .read(|conn| {
            let orders = db::postgres_db::load_active_orders_tx(conn)?;
            let mut decimals = HashMap::new();
            for token in db::postgres_db::get_registered_tokens_tx(conn)? {
                if let Some(value) = token.decimals {
                    decimals.insert(
                        TokenId::read_from_bytes(&token.token_id)?,
                        u8::try_from(value)?,
                    );
                }
            }
            Ok((orders, decimals))
        })
        .await?;
    let mut consumed = std::collections::HashSet::new();
    // Bound each RPC request; notes were decoded once at the DB boundary.
    for chunk in orders.chunks(miden_protocol::MAX_INPUT_NOTES_PER_TX) {
        let notes = chunk
            .iter()
            .map(|order| order.note.as_ref().clone())
            .collect::<Vec<_>>();
        consumed.extend(
            client
                .check_consumed_notes(&notes)
                .await
                .context("reconcile clearing notes against chain")?,
        );
    }
    if !consumed.is_empty() {
        let ids: Vec<_> = consumed.iter().map(|id| id.to_bytes().to_vec()).collect();
        pool.write(move |conn| {
            db::postgres_db::mark_orders_onchain_nullified_tx(conn, &ids).map(|_| ())
        })
        .await?;
        orders.retain(|order| !consumed.contains(&order.id()));
    }
    Ok(matcher::ClearingBootstrap { orders, decimals })
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::note::Note;
    use miden_protocol::note::NoteId;
    use std::collections::HashMap;

    use miden_protocol::account::AccountId;
    use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    use crate::admin::AdminState;
    use crate::db;
    use crate::db::postgres_test::TestDb;
    use crate::ingest::tests::MockMidenClient;
    use crate::ingest::MidenClient;
    use crate::matching::price_feed::PriceFeed;
    use crate::price::{MockPriceClient, PriceClient, PriceSnapshot, WatchPriceFeed};
    use std::sync::Arc;

    fn test_token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn test_token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    async fn persist_clearing_notes(pool: &db::DbPool) -> Vec<NoteId> {
        use crate::db::postgres_models::NewOrderRow;
        use miden_protocol::asset::{AssetAmount, FungibleAsset};
        use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
        use miden_protocol::note::NoteType;
        use miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;
        use miden_protocol::Word;
        use miden_standards::note::{PswapNote, PswapNoteStorage};
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let mut rng = RandomCoin::new(Word::default());
        let mut ids = Vec::new();
        let mut order_rows = Vec::new();
        for _ in 0..2 {
            let note: Note = PswapNote::builder()
                .sender(creator)
                .storage(
                    PswapNoteStorage::builder()
                        .creator_account_id(creator)
                        .min_requested_asset(FungibleAsset::new(test_token_b(), 18).unwrap())
                        .min_fill_step(AssetAmount::new(1).unwrap())
                        .build(),
                )
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(FungibleAsset::new(test_token_a(), 10).unwrap())
                .build()
                .unwrap()
                .into();
            let order_row = NewOrderRow::ingested(&note, 1).unwrap();
            order_rows.push(order_row);
            ids.push(note.id());
        }
        pool.write(move |conn| {
            for token in [test_token_a(), test_token_b()] {
                db::postgres_db::register_token_tx(conn, token, None)?;
                db::postgres_db::set_token_metadata_tx(conn, token, Some(6), None)?;
            }
            db::postgres_db::insert_orders_batch_tx(conn, &order_rows, 1)?;
            Ok(())
        })
        .await
        .unwrap();
        ids
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn clearing_bootstrap_removes_consumed_notes_and_preserves_fifo_and_decimals() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let ids = persist_clearing_notes(pool).await;
        let before = pool
            .read(db::postgres_db::load_active_orders_tx)
            .await
            .unwrap();
        let mut client = MockMidenClient::new();
        client.mark_consumed_silent(vec![ids[0]]);
        let bootstrap = reconcile_clearing_book(&pool, &mut client).await.unwrap();
        assert_eq!(bootstrap.orders.len(), 1);
        assert_eq!(bootstrap.orders[0].id(), ids[1]);
        assert_eq!(bootstrap.orders[0].priority_seq, before[1].priority_seq);
        assert_eq!(bootstrap.decimals.get(&test_token_a()), Some(&6));
        assert_eq!(
            pool.read(db::postgres_db::load_active_orders_tx)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn clearing_bootstrap_fails_closed_on_nullifier_rpc_error() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        persist_clearing_notes(pool).await;
        let mut client = MockMidenClient::new();
        client.fail_consumed_check = true;
        assert!(reconcile_clearing_book(&pool, &mut client).await.is_err());
        assert_eq!(
            pool.read(db::postgres_db::load_active_orders_tx)
                .await
                .unwrap()
                .len(),
            2
        );
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

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn seed_tokens_from_config_inserts_tokens() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let tokens = vec![(test_token_a(), None), (test_token_b(), None)];

        pool.write(move |conn| db::postgres_db::seed_tokens_from_config_tx(conn, &tokens))
            .await
            .unwrap();

        let rows = pool
            .read(db::postgres_db::get_registered_tokens_tx)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn seed_tokens_from_config_is_idempotent() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let tokens = vec![(test_token_a(), None)];

        for _ in 0..2 {
            let tokens = tokens.clone();
            pool.write(move |conn| db::postgres_db::seed_tokens_from_config_tx(conn, &tokens))
                .await
                .unwrap();
        }

        let rows = pool
            .read(db::postgres_db::get_registered_tokens_tx)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn load_tokens_from_db_round_trips() {
        use std::sync::RwLock;
        let test_db = TestDb::new().await.unwrap();
        let pool = test_db.pool.clone();
        let token_a = test_token_a();
        let token_b = test_token_b();

        pool.write(move |conn| {
            db::postgres_db::seed_tokens_from_config_tx(conn, &[(token_a, None), (token_b, None)])
        })
        .await
        .unwrap();

        let (subscribe_tx, _rx) = mpsc::channel::<(TokenId, TokenId)>(8);
        let token_map = Arc::new(RwLock::new(HashMap::new()));
        let state = AdminState::new(pool, subscribe_tx, token_map);

        let loaded = state.load_tokens_from_db().await.unwrap();
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
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn subscribe_all_pairs_with_two_tokens() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let token_a = test_token_a();
        let token_b = test_token_b();

        pool.write(move |conn| {
            db::postgres_db::seed_tokens_from_config_tx(conn, &[(token_a, None), (token_b, None)])
        })
        .await
        .unwrap();

        let mut mock_client = MockMidenClient::new();
        let result = subscribe_all_pairs(&pool, &mut mock_client).await;
        assert!(result.is_ok());
    }

    /// Metadata is fetched + cached at registration (the subscribe pass), not on
    /// an ingest tick.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn subscribe_all_pairs_caches_on_chain_metadata() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let token_a = test_token_a();
        let token_b = test_token_b();
        pool.write(move |conn| {
            db::postgres_db::seed_tokens_from_config_tx(conn, &[(token_a, None), (token_b, None)])
        })
        .await
        .unwrap();

        // Freshly seeded rows carry no metadata yet.
        let key_a = token_a.to_bytes();
        let lookup_key = key_a.clone();
        let row = pool
            .read(move |conn| db::postgres_db::get_registered_token_tx(conn, &lookup_key))
            .await
            .unwrap()
            .unwrap();
        assert!(row.decimals.is_none() && row.ticker.is_none());

        // Subscribing (the boot registration point) fetches + persists it once.
        let mut mock_client = MockMidenClient::new();
        mock_client.set_token_metadata(8, "MTA");
        subscribe_all_pairs(&pool, &mut mock_client).await.unwrap();

        let row = pool
            .read(move |conn| db::postgres_db::get_registered_token_tx(conn, &key_a))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.decimals, Some(8));
        assert_eq!(row.ticker.as_deref(), Some("MTA"));
    }
}

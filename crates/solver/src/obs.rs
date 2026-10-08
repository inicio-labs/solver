//! Observability HTTP server: liveness, readiness, and local solver metrics.
//!
//! All endpoints are unauthenticated by design — supervisors and monitoring
//! scrapers shouldn't need a bearer token. The server binds on `127.0.0.1`
//! only and runs on a separate port (`obs_port`) from the admin server so
//! operators can firewall them independently.
//!
//! ## Semantics
//!
//! * `GET /health` — always returns `200 OK` with body `"ok"`. Tells a
//!   supervisor (systemd, k8s liveness) that the process is alive and able
//!   to serve HTTP. If this fails the supervisor should restart the process.
//!
//! * `GET /readyz` — returns `200 OK` only when both:
//!     1. A PostgreSQL read answers and the writer session still holds its
//!        ownership lock (the schema is verified once, at startup).
//!     2. The time since the last successful `sync_state` is below the
//!        configured freshness threshold.
//!   Otherwise returns `503 Service Unavailable` with a short text body
//!   indicating which check failed. Used by load balancers / k8s readiness
//!   probes to stop routing traffic during transient degradation WITHOUT
//!   restarting the process.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tokio::sync::{mpsc, watch};

use crate::db::postgres_pool::{LatencySnapshot, LATENCY_BUCKET_US};
use crate::db::DbPool;
use crate::price::{FeedMetrics, PriceSnapshot};
use crate::types::{now_unix, BookUpdate, ExecutionBatch};

/// Shared observability state.
///
/// `last_sync_unix_seconds` is initialised to the wall-clock time at start
/// (a grace period) and updated by the ingest task after each successful
/// `sync_state`. `/readyz` uses it to gate readiness.
#[derive(Clone)]
pub struct ObsState {
    pub db_pool: DbPool,
    pub last_sync_unix_seconds: Arc<AtomicU64>,
    pub readiness_freshness_secs: u64,
    /// Read only for their remaining capacity in `/metrics`.
    book_tx: mpsc::Sender<BookUpdate>,
    exec_tx: mpsc::Sender<ExecutionBatch>,
    /// Latest Binance quotes, for per-symbol age in `/metrics`.
    prices: watch::Receiver<Arc<PriceSnapshot>>,
    feed_metrics: Arc<FeedMetrics>,
}

impl ObsState {
    pub fn new(
        db_pool: DbPool,
        readiness_freshness_secs: u64,
        book_tx: mpsc::Sender<BookUpdate>,
        exec_tx: mpsc::Sender<ExecutionBatch>,
        prices: watch::Receiver<Arc<PriceSnapshot>>,
        feed_metrics: Arc<FeedMetrics>,
    ) -> Self {
        Self {
            db_pool,
            last_sync_unix_seconds: Arc::new(AtomicU64::new(now_unix())),
            readiness_freshness_secs,
            book_tx,
            exec_tx,
            prices,
            feed_metrics,
        }
    }

    /// Build the observability router (`/health`, `/readyz`, `/metrics`).
    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .with_state(self)
    }

    /// Handle for the ingest task to record successful syncs. Cheap clone —
    /// just bumps the Arc<AtomicU64> rather than passing the whole state.
    pub fn last_sync_handle(&self) -> Arc<AtomicU64> {
        self.last_sync_unix_seconds.clone()
    }
}

async fn health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

fn append_latency(body: &mut String, metric: &str, latency: &LatencySnapshot) {
    let bounds = LATENCY_BUCKET_US
        .iter()
        .map(|us| (*us as f64 / 1_000_000.0).to_string())
        .chain(["+Inf".to_string()]);
    for (bound, count) in bounds.zip(latency.buckets) {
        let _ = writeln!(body, "{metric}_bucket{{le=\"{bound}\"}} {count}");
    }
    let _ = writeln!(body, "{metric}_sum {}", latency.sum_us as f64 / 1_000_000.0);
    let _ = writeln!(body, "{metric}_count {}", latency.buckets[8]);
}

/// Price feed health and per-symbol quote state (rendered by the feed), and
/// pairs the matcher skipped for want of a usable price.
fn append_price_metrics(body: &mut String, feed: &FeedMetrics, snapshot: &PriceSnapshot) {
    feed.render(snapshot, body);
    for (reason, count) in crate::matcher::price_skips() {
        let _ = writeln!(
            body,
            "solver_matcher_price_skips_total{{reason=\"{reason}\"}} {count}"
        );
    }
}

async fn metrics(
    State(state): State<ObsState>,
) -> ([(axum::http::HeaderName, &'static str); 1], String) {
    let pool = state.db_pool.telemetry_snapshot();
    let book_capacity = state.book_tx.capacity();
    let exec_capacity = state.exec_tx.capacity();
    let mut body = format!(
        "solver_db_read_total {}\n\
         solver_db_read_errors_total {}\n\
         solver_db_write_total {}\n\
         solver_db_write_errors_total {}\n\
         solver_db_lock_timeouts_total {}\n\
         solver_db_statement_timeouts_total {}\n\
         solver_db_deadlocks_total {}\n\
         solver_db_writer_reconnects_total {}\n\
         solver_db_read_connections {}\n\
         solver_db_read_idle_connections {}\n\
         solver_db_writer_busy {}\n\
         solver_db_fatal_shutdown_requested {}\n\
         solver_matcher_book_channel_remaining {}\n\
         solver_matcher_executor_channel_remaining {}\n\
         solver_matcher_executor_full_skipped_ticks_total {}\n\
         solver_matcher_ordered_update_backlog {}\n",
        pool.read_total,
        pool.read_errors,
        pool.write_total,
        pool.write_errors,
        pool.lock_timeouts,
        pool.statement_timeouts,
        pool.deadlocks,
        pool.writer_reconnects,
        pool.read_connections,
        pool.read_idle_connections,
        u8::from(pool.writer_busy),
        u8::from(pool.fatal_shutdown_requested),
        book_capacity,
        exec_capacity,
        crate::matcher::skipped_executor_full_ticks(),
        crate::matcher::ordered_update_backlog(),
    );
    for (metric, latency) in [
        ("solver_db_read_pool_wait_seconds", &pool.read_wait),
        ("solver_db_read_duration_seconds", &pool.read_duration),
        ("solver_db_writer_wait_seconds", &pool.writer_wait),
        ("solver_db_write_duration_seconds", &pool.write_duration),
    ] {
        append_latency(&mut body, metric, latency);
    }
    let snapshot = state.prices.borrow().clone();
    append_price_metrics(&mut body, &state.feed_metrics, &snapshot);
    ([(CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

async fn readyz(State(state): State<ObsState>) -> (StatusCode, String) {
    // 1. A read answers and the writer session still holds its lock.
    if let Err(e) = state.db_pool.readiness_check().await {
        tracing::warn!(error = %e, "readyz: DB unreachable");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("db unreachable: {e}"),
        );
    }

    // 2. Last sync recent enough?
    let last = state.last_sync_unix_seconds.load(Ordering::Relaxed);
    let age = now_unix().saturating_sub(last);
    if age > state.readiness_freshness_secs {
        tracing::warn!(
            age_secs = age,
            threshold_secs = state.readiness_freshness_secs,
            "readyz: sync stale"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "sync stale: {}s since last successful sync (threshold {}s)",
                age, state.readiness_freshness_secs
            ),
        );
    }

    (StatusCode::OK, "ready".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_test::TestDb;
    use crate::price::test_support::{eth, usdt};
    use axum_test::TestServer;
    use std::time::{Duration, Instant};

    /// A valid quote older than the TTL is `valid` but not `fresh`: alerts on
    /// `fresh` see a stalled feed.
    #[test]
    fn price_metrics_report_feed_health_and_quote_freshness() {
        let feed = FeedMetrics::new(["wss://a.test".into(), "wss://a.test".into()]);
        feed.connected[1].store(true, Ordering::Relaxed);
        feed.connections[1].store(3, Ordering::Relaxed);
        feed.frames[0].store(7, Ordering::Relaxed);
        feed.conflicting_updates.store(4, Ordering::Relaxed);
        feed.last_publish_delay_us.store(1_500, Ordering::Relaxed);
        let received = Instant::now().checked_sub(Duration::from_secs(5)).unwrap();
        let snapshot = PriceSnapshot::for_tests(
            &[(eth(), usdt(), "2000", received)],
            &[],
            Duration::from_secs(1),
        );
        let symbol = snapshot.quotes().next().unwrap().0.to_string();
        let mut body = String::new();
        append_price_metrics(&mut body, &feed, &snapshot);
        for line in [
            // Two readers on one endpoint stay apart by position.
            "solver_price_feed_connected{reader=\"0\",endpoint=\"wss://a.test\"} 0".to_string(),
            "solver_price_feed_connected{reader=\"1\",endpoint=\"wss://a.test\"} 1".to_string(),
            "solver_price_feed_connections_total{reader=\"1\",endpoint=\"wss://a.test\"} 3"
                .to_string(),
            "solver_price_feed_frames_total{reader=\"0\",endpoint=\"wss://a.test\"} 7".to_string(),
            "solver_price_feed_conflicting_updates_total 4".to_string(),
            "solver_price_feed_publish_delay_seconds 0.0015".to_string(),
            format!("solver_price_quote_valid{{symbol=\"{symbol}\"}} 1"),
            format!("solver_price_quote_fresh{{symbol=\"{symbol}\"}} 0"),
            "solver_matcher_price_skips_total{reason=\"stale\"} ".to_string(),
        ] {
            assert!(body.contains(&line), "missing {line:?} in:\n{body}");
        }
        let prefix = format!("solver_price_quote_age_seconds{{symbol=\"{symbol}\"}} ");
        let age = body
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .unwrap();
        assert!(age.parse::<f64>().unwrap() >= 5.0);
    }

    fn test_state(db: &TestDb) -> ObsState {
        let (book_tx, _) = tokio::sync::mpsc::channel(1);
        let (exec_tx, _) = tokio::sync::mpsc::channel(1);
        ObsState::new(
            db.pool.clone(),
            60,
            book_tx,
            exec_tx,
            watch::channel(Arc::new(PriceSnapshot::default())).1,
            Arc::new(FeedMetrics::default()),
        )
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn health_returns_200() {
        let db = TestDb::new().await.unwrap();
        let state = test_state(&db);
        let server = TestServer::new(state.router());
        let res = server.get("/health").await;
        res.assert_status_ok();
        res.assert_text("ok");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_returns_200_when_fresh() {
        let db = TestDb::new().await.unwrap();
        let state = test_state(&db);
        // Constructor initialises last_sync to "now", so first /readyz must pass.
        let server = TestServer::new(state.router());
        let res = server.get("/readyz").await;
        res.assert_status_ok();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_returns_503_when_sync_stale() {
        let db = TestDb::new().await.unwrap();
        let state = test_state(&db);
        // Force last_sync into the distant past so the freshness check fails.
        state
            .last_sync_unix_seconds
            .store(now_unix() - 3600, Ordering::Relaxed);
        let server = TestServer::new(state.router());
        let res = server.get("/readyz").await;
        res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
        assert!(res.text().contains("sync stale"));
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_updates_when_handle_writes() {
        let db = TestDb::new().await.unwrap();
        let state = test_state(&db);
        let handle = state.last_sync_handle();
        // Stale first, then refreshed via the handle the ingest task would hold.
        state
            .last_sync_unix_seconds
            .store(now_unix() - 3600, Ordering::Relaxed);
        let server = TestServer::new(state.clone().router());
        let stale = server.get("/readyz").await;
        stale.assert_status(StatusCode::SERVICE_UNAVAILABLE);

        handle.store(now_unix(), Ordering::Relaxed);
        let fresh = server.get("/readyz").await;
        fresh.assert_status_ok();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn metrics_exposes_pool_and_channel_capacity() {
        let db = TestDb::new().await.unwrap();
        let (book_tx, _book_rx) = tokio::sync::mpsc::channel(3);
        let (exec_tx, _exec_rx) = tokio::sync::mpsc::channel(2);
        let state = ObsState::new(
            db.pool.clone(),
            60,
            book_tx,
            exec_tx,
            watch::channel(Arc::new(PriceSnapshot::default())).1,
            Arc::new(FeedMetrics::default()),
        );
        db.pool.read(|_| Ok(())).await.unwrap();
        let server = TestServer::new(state.router());
        let response = server.get("/metrics").await;
        response.assert_status_ok();
        let body = response.text();
        assert!(body.contains("solver_db_read_total 1"));
        assert!(body.contains("solver_db_fatal_shutdown_requested 0"));
        assert!(body.contains("solver_db_read_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(body.contains("solver_matcher_book_channel_remaining 3"));
        assert!(body.contains("solver_matcher_executor_channel_remaining 2"));
        db.pool.fatal_token().cancel();
        let failed = server.get("/metrics").await;
        assert!(failed
            .text()
            .contains("solver_db_fatal_shutdown_requested 1"));
        server
            .get("/readyz")
            .await
            .assert_status(StatusCode::SERVICE_UNAVAILABLE);
    }
}

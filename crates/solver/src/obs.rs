//! Observability HTTP server: liveness, readiness, and local solver metrics.
//!
//! Both endpoints are unauthenticated by design — supervisors and monitoring
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
//!     1. PostgreSQL schema and the original writer ownership session are healthy.
//!     2. The time since the last successful `sync_state` is below the
//!        configured freshness threshold.
//!   Otherwise returns `503 Service Unavailable` with a short text body
//!   indicating which check failed. Used by load balancers / k8s readiness
//!   probes to stop routing traffic during transient degradation WITHOUT
//!   restarting the process.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tokio::sync::mpsc;

use crate::db::DbPool;
use crate::types::{BookUpdate, ExecutionBatch};

/// Shared observability state.
///
/// `last_sync_unix_seconds` is initialised to the wall-clock time at start
/// (a grace period) and updated by the ingest task after each successful
/// `sync_state`. `/readyz` uses it to gate readiness.
#[derive(Clone)]
pub struct ObsState {
    pub db_pool: DbPool,
    pub last_sync_unix_seconds: Arc<AtomicI64>,
    pub readiness_freshness_secs: u64,
    book_tx: Option<mpsc::Sender<BookUpdate>>,
    exec_tx: Option<mpsc::Sender<ExecutionBatch>>,
}

impl ObsState {
    pub fn new(db_pool: DbPool, readiness_freshness_secs: u64) -> Self {
        Self {
            db_pool,
            last_sync_unix_seconds: Arc::new(AtomicI64::new(unix_now())),
            readiness_freshness_secs,
            book_tx: None,
            exec_tx: None,
        }
    }

    pub fn with_channels(
        mut self,
        book_tx: mpsc::Sender<BookUpdate>,
        exec_tx: mpsc::Sender<ExecutionBatch>,
    ) -> Self {
        self.book_tx = Some(book_tx);
        self.exec_tx = Some(exec_tx);
        self
    }

    /// Build the observability router (`/health` + `/readyz`).
    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .with_state(self)
    }

    /// Handle for the ingest task to record successful syncs. Cheap clone —
    /// just bumps the Arc<AtomicI64> rather than passing the whole state.
    pub fn last_sync_handle(&self) -> Arc<AtomicI64> {
        self.last_sync_unix_seconds.clone()
    }
}

/// Current Unix time in seconds, saturating to i64 (good until year 292277026596).
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

fn append_latency_buckets(body: &mut String, metric: &str, buckets: &[u64; 9]) {
    use std::fmt::Write;

    const BOUNDS: [&str; 9] = [
        "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1", "+Inf",
    ];
    for (bound, count) in BOUNDS.into_iter().zip(buckets) {
        let _ = writeln!(body, "{metric}_bucket{{le=\"{bound}\"}} {count}");
    }
    let _ = writeln!(body, "{metric}_count {}", buckets[8]);
}

async fn metrics(
    State(state): State<ObsState>,
) -> ([(axum::http::HeaderName, &'static str); 1], String) {
    let pool = state.db_pool.telemetry_snapshot();
    let book_capacity = state.book_tx.as_ref().map_or(0, mpsc::Sender::capacity);
    let exec_capacity = state.exec_tx.as_ref().map_or(0, mpsc::Sender::capacity);
    let mut body = format!(
        "solver_db_read_total {}\n\
         solver_db_read_errors_total {}\n\
         solver_db_read_pool_wait_seconds_sum {}\n\
         solver_db_read_duration_seconds_sum {}\n\
         solver_db_write_total {}\n\
         solver_db_write_errors_total {}\n\
         solver_db_writer_wait_seconds_sum {}\n\
         solver_db_write_duration_seconds_sum {}\n\
         solver_db_lock_timeouts_total {}\n\
         solver_db_statement_timeouts_total {}\n\
         solver_db_deadlocks_total {}\n\
         solver_db_writer_reconnects_total {}\n\
         solver_db_read_connections {}\n\
         solver_db_read_idle_connections {}\n\
         solver_db_writer_busy {}\n\
         solver_db_ownership_lost {}\n\
         solver_db_fatal_shutdown_requested {}\n\
         solver_matcher_book_channel_remaining {}\n\
         solver_matcher_executor_channel_remaining {}\n\
         solver_matcher_executor_full_skipped_ticks_total {}\n",
        pool.read_total,
        pool.read_errors,
        pool.read_wait_us as f64 / 1_000_000.0,
        pool.read_duration_us as f64 / 1_000_000.0,
        pool.write_total,
        pool.write_errors,
        pool.writer_wait_us as f64 / 1_000_000.0,
        pool.write_duration_us as f64 / 1_000_000.0,
        pool.lock_timeouts,
        pool.statement_timeouts,
        pool.deadlocks,
        pool.writer_reconnects,
        pool.read_connections,
        pool.read_idle_connections,
        u8::from(pool.writer_busy),
        u8::from(pool.ownership_lost),
        u8::from(pool.fatal_shutdown_requested),
        book_capacity,
        exec_capacity,
        crate::matcher::skipped_executor_full_ticks(),
    );
    append_latency_buckets(
        &mut body,
        "solver_db_read_pool_wait_seconds",
        &pool.read_wait_buckets,
    );
    append_latency_buckets(
        &mut body,
        "solver_db_read_duration_seconds",
        &pool.read_duration_buckets,
    );
    append_latency_buckets(
        &mut body,
        "solver_db_writer_wait_seconds",
        &pool.writer_wait_buckets,
    );
    append_latency_buckets(
        &mut body,
        "solver_db_write_duration_seconds",
        &pool.write_duration_buckets,
    );
    ([(CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

async fn readyz(State(state): State<ObsState>) -> (StatusCode, String) {
    // 1. Verify the exact PostgreSQL schema and the original writer lock.
    if let Err(e) = state.db_pool.readiness_check().await {
        tracing::warn!(error = %e, "readyz: DB unreachable");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("db unreachable: {e}"),
        );
    }

    // 2. Last sync recent enough?
    let last = state.last_sync_unix_seconds.load(Ordering::Relaxed);
    let now = unix_now();
    let age = now.saturating_sub(last);
    if age > state.readiness_freshness_secs as i64 {
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
    use axum_test::TestServer;

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn health_returns_200() {
        let db = TestDb::new().await.unwrap();
        let state = ObsState::new(db.pool.clone(), 60);
        let server = TestServer::new(state.router());
        let res = server.get("/health").await;
        res.assert_status_ok();
        res.assert_text("ok");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_returns_200_when_fresh() {
        let db = TestDb::new().await.unwrap();
        let state = ObsState::new(db.pool.clone(), 60);
        // Constructor initialises last_sync to "now", so first /readyz must pass.
        let server = TestServer::new(state.router());
        let res = server.get("/readyz").await;
        res.assert_status_ok();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_returns_503_when_sync_stale() {
        let db = TestDb::new().await.unwrap();
        let state = ObsState::new(db.pool.clone(), 60);
        // Force last_sync into the distant past so the freshness check fails.
        state
            .last_sync_unix_seconds
            .store(unix_now() - 3600, Ordering::Relaxed);
        let server = TestServer::new(state.router());
        let res = server.get("/readyz").await;
        res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
        assert!(res.text().contains("sync stale"));
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn readyz_updates_when_handle_writes() {
        let db = TestDb::new().await.unwrap();
        let state = ObsState::new(db.pool.clone(), 60);
        let handle = state.last_sync_handle();
        // Stale first, then refreshed via the handle the ingest task would hold.
        state
            .last_sync_unix_seconds
            .store(unix_now() - 3600, Ordering::Relaxed);
        let server = TestServer::new(state.clone().router());
        let stale = server.get("/readyz").await;
        stale.assert_status(StatusCode::SERVICE_UNAVAILABLE);

        handle.store(unix_now(), Ordering::Relaxed);
        let fresh = server.get("/readyz").await;
        fresh.assert_status_ok();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn metrics_exposes_pool_and_channel_capacity() {
        let db = TestDb::new().await.unwrap();
        let (book_tx, _book_rx) = tokio::sync::mpsc::channel(3);
        let (exec_tx, _exec_rx) = tokio::sync::mpsc::channel(2);
        let state = ObsState::new(db.pool.clone(), 60).with_channels(book_tx, exec_tx);
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

//! Observability HTTP server: liveness (`/health`) + readiness (`/readyz`).
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
//!     1. The DB write pool can hand out a connection (DB is reachable).
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
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;

use crate::db::DbPool;

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
}

impl ObsState {
    pub fn new(db_pool: DbPool, readiness_freshness_secs: u64) -> Self {
        Self {
            db_pool,
            last_sync_unix_seconds: Arc::new(AtomicI64::new(unix_now())),
            readiness_freshness_secs,
        }
    }

    /// Build the observability router (`/health` + `/readyz`).
    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/readyz", get(readyz))
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

async fn readyz(State(state): State<ObsState>) -> (StatusCode, String) {
    // 1. DB reachable? Try to acquire a write conn — fast on a healthy pool,
    //    fails immediately if exhausted or the file is gone.
    if let Err(e) = state.db_pool.write_conn() {
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
    use crate::db;
    use axum_test::TestServer;

    fn test_pool() -> DbPool {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let url = format!("file:obstest{}?mode=memory&cache=shared", n);
        db::init_db(&url, 1).expect("failed to create in-memory DB")
    }

    #[tokio::test]
    async fn health_returns_200() {
        let state = ObsState::new(test_pool(), 60);
        let server = TestServer::new(state.router());
        let res = server.get("/health").await;
        res.assert_status_ok();
        res.assert_text("ok");
    }

    #[tokio::test]
    async fn readyz_returns_200_when_fresh() {
        let state = ObsState::new(test_pool(), 60);
        // Constructor initialises last_sync to "now", so first /readyz must pass.
        let server = TestServer::new(state.router());
        let res = server.get("/readyz").await;
        res.assert_status_ok();
    }

    #[tokio::test]
    async fn readyz_returns_503_when_sync_stale() {
        let state = ObsState::new(test_pool(), 60);
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
    async fn readyz_updates_when_handle_writes() {
        let state = ObsState::new(test_pool(), 60);
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
}

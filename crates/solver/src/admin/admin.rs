use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;

use crate::db::{self, DbPool, DbResult};
use crate::types::TokenId;

/// Command sent to the subscribe task: subscribe both directions of a pair
/// to the underlying Miden client.
///
/// We send via a channel rather than holding a `MidenClient` directly because
/// the production `Client<FilesystemKeyStore>` is `!Send`, which conflicts
/// with axum's requirement that handler state be `Send + Sync`. The subscribe
/// task runs on the ingest thread, which owns the client; admin only holds a
/// `Sender` (which is always `Send + Sync`).
pub type SubscribeSender = mpsc::Sender<(TokenId, TokenId)>;

/// Shared state for admin routes. Must be `Send + Sync` for axum's router.
pub struct AdminState {
    pool: DbPool,
    /// Channel to the subscribe task. Admin sends one message per direction
    /// when a new token is registered; the subscribe task processes them in
    /// order. Failures only log — admin call still succeeds since the DB row
    /// is the source of truth, and a restart subscribes every registered pair.
    subscribe_tx: SubscribeSender,
}

impl AdminState {
    pub fn new(pool: DbPool, subscribe_tx: SubscribeSender) -> Self {
        Self { pool, subscribe_tx }
    }

    /// Build the admin router.
    ///
    /// When `admin_token` is `Some`, all routes require an `Authorization: Bearer <token>`
    /// header whose value matches in constant time. When `None`, no admin routes are
    /// registered — every `/admin/*` path returns 404. Token management still works
    /// via `solver.toml` → restart.
    pub fn router(self: Arc<Self>, admin_token: Option<Arc<String>>) -> Router {
        let Some(token) = admin_token else {
            return Router::new();
        };
        Router::new()
            .route("/admin/tokens", get(list_tokens))
            .route("/admin/tokens", post(add_token))
            .route("/admin/tokens", delete(remove_token))
            .layer(middleware::from_fn_with_state(token, require_bearer_token))
            .with_state(self)
    }

    pub async fn load_tokens_from_db(&self) -> DbResult<Vec<TokenId>> {
        self.pool
            .read(db::postgres_db::load_registered_tokens_tx)
            .await
    }

    /// Register `token`; `true` when it is new. A new token is subscribed
    /// against every registered token, in both directions.
    ///
    /// Commit and subscriptions run in their own task, so a caller that
    /// disconnects cannot skip the subscriptions (a retry would only answer
    /// "already registered"). Send failures only log: the database row is the
    /// source of truth and a restart re-subscribes every registered pair.
    async fn register_token(&self, token: TokenId) -> DbResult<bool> {
        let (pool, subscribe_tx) = (self.pool.clone(), self.subscribe_tx.clone());
        tokio::spawn(async move {
            let existing = pool
                .write(move |conn| {
                    if !db::postgres_db::register_token_tx(conn, token)? {
                        return Ok(None);
                    }
                    Ok(Some(db::postgres_db::load_registered_tokens_tx(conn)?))
                })
                .await?;
            let Some(existing) = existing else {
                return Ok(false);
            };
            for other in existing.into_iter().filter(|other| *other != token) {
                for pair in [(token, other), (other, token)] {
                    if let Err(error) = subscribe_tx.send(pair).await {
                        tracing::warn!(%error, "admin: subscribe channel send failed");
                    }
                }
            }
            Ok(true)
        })
        .await?
    }
}

/// Parse a hex token ID; only its canonical serialization is accepted.
fn parse_token(hex_id: &str) -> Result<TokenId, StatusCode> {
    let bytes = hex::decode(hex_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token =
        TokenId::read_from(&mut SliceReader::new(&bytes)).map_err(|_| StatusCode::BAD_REQUEST)?;
    if token.to_bytes() != bytes {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(token)
}

// ── Auth Middleware ─────────────────────────────────────────────────────────

async fn require_bearer_token(
    State(expected): State<Arc<String>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let provided = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    match provided {
        Some(t) if bool::from(t.as_bytes().ct_eq(expected.as_bytes())) => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

// ── Route Handlers ──────────────────────────────────────────────────────────

async fn list_tokens(
    State(state): State<Arc<AdminState>>,
) -> Result<Json<Vec<TokenResponse>>, StatusCode> {
    let tokens = state
        .load_tokens_from_db()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = tokens
        .into_iter()
        .map(|t| {
            let mut bytes = Vec::new();
            t.write_into(&mut bytes);
            TokenResponse {
                token_id: hex::encode(&bytes),
            }
        })
        .collect();

    Ok(Json(response))
}

async fn add_token(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<(StatusCode, &'static str), StatusCode> {
    let token = parse_token(&req.token_id)?;
    let inserted = state
        .register_token(token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if inserted {
        Ok((StatusCode::CREATED, "registered"))
    } else {
        Ok((StatusCode::OK, "already registered"))
    }
}

async fn remove_token(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<StatusCode, StatusCode> {
    let token = parse_token(&req.token_id)?;
    let deleted = state
        .pool
        .write(move |conn| db::postgres_db::unregister_token_tx(conn, token))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(if deleted {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    })
}

#[derive(Deserialize)]
pub struct TokenRequest {
    pub token_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct TokenResponse {
    pub token_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use miden_protocol::account::AccountId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };
    use serde_json::json;

    use crate::db::postgres_test::TestDb;

    fn test_token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn test_token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    const TEST_TOKEN: &str = "test-admin-token";

    async fn make_state() -> (Arc<AdminState>, TestDb) {
        let test_db = TestDb::new().await.unwrap();
        // Tests don't exercise the subscribe path; create a channel whose
        // receiver is dropped immediately. Sends will fail but admin handlers
        // log and continue.
        let (subscribe_tx, _) = mpsc::channel::<(TokenId, TokenId)>(8);
        let state = Arc::new(AdminState::new(test_db.pool.clone(), subscribe_tx));
        (state, test_db)
    }

    async fn test_server() -> (TestServer, TestDb) {
        let (state, db) = make_state().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer test-admin-token"),
        );
        (server, db)
    }

    fn token_hex(token: TokenId) -> String {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        hex::encode(bytes)
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_returns_created_first_time() {
        let (server, _db) = test_server().await;
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::CREATED);
        assert_eq!(res.text(), "registered");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_returns_ok_on_duplicate() {
        let (server, _db) = test_server().await;
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.post("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.text(), "already registered");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_returns_bad_request_for_invalid_hex() {
        let (server, _db) = test_server().await;
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": "not_hex!" }))
            .await;
        res.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn remove_token_returns_ok_when_found() {
        let (server, _db) = test_server().await;
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.delete("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn remove_token_returns_not_found_when_missing() {
        let (server, _db) = test_server().await;
        let res = server
            .delete("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn requests_without_bearer_token_return_unauthorized() {
        let (state, _db) = make_state().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let server = TestServer::new(state.router(Some(token)));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn requests_with_wrong_token_return_unauthorized() {
        let (state, _db) = make_state().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer wrong"),
        );
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn router_with_no_admin_token_returns_404() {
        let (state, _db) = make_state().await;
        let server = TestServer::new(state.router(None));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn list_tokens_returns_all_registered() {
        let (server, _db) = test_server().await;
        server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_b()) }))
            .await;

        let res = server.get("/admin/tokens").await;
        res.assert_status_ok();
        let body: Vec<TokenResponse> = res.json();
        assert_eq!(body.len(), 2);
        let ids: Vec<_> = body.iter().map(|t| t.token_id.as_str()).collect();
        assert!(ids.contains(&token_hex(test_token_a()).as_str()));
        assert!(ids.contains(&token_hex(test_token_b()).as_str()));
    }
}

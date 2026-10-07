use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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
    /// Tokens with a Binance market in `solver.toml`. Prices come only from
    /// that file, so registering any other token would leave it unpriced.
    binance_tokens: HashSet<TokenId>,
}

impl AdminState {
    pub fn new(
        pool: DbPool,
        subscribe_tx: SubscribeSender,
        binance_tokens: HashSet<TokenId>,
    ) -> Self {
        Self {
            pool,
            subscribe_tx,
            binance_tokens,
        }
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
    /// "already registered"). The caller is answered as soon as the commit is
    /// known; the sends follow without holding the request. Send failures
    /// only log: the database row is the source of truth and a restart
    /// re-subscribes every registered pair.
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
            // Detached from the request: the caller is answered now, and a
            // slow relay cannot hold the response or lose the sends.
            tokio::spawn(async move {
                for other in existing.into_iter().filter(|other| *other != token) {
                    for pair in [(token, other), (other, token)] {
                        if let Err(error) = subscribe_tx.send(pair).await {
                            tracing::warn!(%error, "admin: subscribe channel send failed");
                        }
                    }
                }
            });
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
    if !state.binance_tokens.contains(&token) {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            "token has no Binance market in solver.toml; add it there and restart",
        ));
    }
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

/// A field this API does not know is rejected rather than accepted and
/// silently dropped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
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

    type Subscriptions = mpsc::Receiver<(TokenId, TokenId)>;

    async fn make_state() -> (Arc<AdminState>, TestDb, Subscriptions) {
        let test_db = TestDb::new().await.unwrap();
        let (subscribe_tx, subscribe_rx) = mpsc::channel::<(TokenId, TokenId)>(8);
        let binance_tokens = HashSet::from([test_token_a(), test_token_b()]);
        let state = Arc::new(AdminState::new(
            test_db.pool.clone(),
            subscribe_tx,
            binance_tokens,
        ));
        (state, test_db, subscribe_rx)
    }

    async fn test_server() -> (TestServer, TestDb, Subscriptions) {
        let (state, db, subscriptions) = make_state().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer test-admin-token"),
        );
        (server, db, subscriptions)
    }

    fn token_hex(token: TokenId) -> String {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        hex::encode(bytes)
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_returns_created_first_time() {
        let (server, _db, _subscriptions) = test_server().await;
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
        let (server, _db, _subscriptions) = test_server().await;
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.post("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.text(), "already registered");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_returns_bad_request_for_invalid_hex() {
        let (server, _db, _subscriptions) = test_server().await;
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": "not_hex!" }))
            .await;
        res.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn remove_token_returns_ok_when_found() {
        let (server, _db, _subscriptions) = test_server().await;
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.delete("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn remove_token_returns_not_found_when_missing() {
        let (server, _db, _subscriptions) = test_server().await;
        let res = server
            .delete("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn requests_without_bearer_token_return_unauthorized() {
        let (state, _db, _subscriptions) = make_state().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let server = TestServer::new(state.router(Some(token)));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn requests_with_wrong_token_return_unauthorized() {
        let (state, _db, _subscriptions) = make_state().await;
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
        let (state, _db, _subscriptions) = make_state().await;
        let server = TestServer::new(state.router(None));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn list_tokens_returns_all_registered() {
        let (server, _db, _subscriptions) = test_server().await;
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
    /// A new token is subscribed against every other registered token in both
    /// directions, after the caller has already been answered.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn registering_subscribes_both_directions() {
        let (server, _db, mut subscriptions) = test_server().await;
        let (a, b) = (test_token_a(), test_token_b());
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(a) }))
            .await;
        res.assert_status(StatusCode::CREATED);
        assert!(
            subscriptions.try_recv().is_err(),
            "nothing to pair the first token with"
        );
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(b) }))
            .await;
        res.assert_status(StatusCode::CREATED);
        let mut received = Vec::new();
        for _ in 0..2 {
            let pair =
                tokio::time::timeout(std::time::Duration::from_secs(5), subscriptions.recv())
                    .await
                    .expect("subscriptions arrive after the response")
                    .expect("channel open");
            received.push(pair);
        }
        received.sort();
        let mut expected = vec![(a, b), (b, a)];
        expected.sort();
        assert_eq!(received, expected);
    }

    /// A token without a Binance market in the configuration is refused and
    /// not registered: it could never be priced.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_token_without_a_binance_market_is_rejected() {
        let (server, _db, _subscriptions) = test_server().await;
        let unmapped = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap();
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(unmapped) }))
            .await;
        res.assert_status(StatusCode::UNPROCESSABLE_ENTITY);
        assert!(res.text().contains("solver.toml"));
        let listed = server.get("/admin/tokens").await;
        assert_eq!(listed.json::<Vec<TokenResponse>>().len(), 0);
    }

    /// A request carrying a field this API does not know is refused instead of
    /// being registered with the field silently dropped.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn unknown_request_fields_are_rejected() {
        let (server, _db, _subscriptions) = test_server().await;
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()), "ticker": "USDC" }))
            .await;
        res.assert_status(StatusCode::UNPROCESSABLE_ENTITY);
        let listed = server.get("/admin/tokens").await;
        assert_eq!(listed.json::<Vec<TokenResponse>>().len(), 0);
    }
}

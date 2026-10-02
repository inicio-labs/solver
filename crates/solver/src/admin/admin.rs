use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use diesel::PgConnection;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, Mutex};

use crate::db::{self, DbPool, DbResult};
use crate::price::SharedTokenMap;
use crate::types::TokenId;

/// Command sent to the subscribe task: subscribe both directions of a pair
/// to the underlying Miden client.
///
/// We send via a channel rather than holding a `MidenClient` directly because
/// the production `Client<FilesystemKeyStore>` is `!Send`, which conflicts
/// with axum's requirement that handler state be `Send + Sync`. The subscribe
/// task lives in the same `LocalSet` and owns the client; we just give it a
/// `Sender` (which is always `Send + Sync`).
pub type SubscribeSender = mpsc::Sender<(TokenId, TokenId)>;

/// Shared state for admin routes. Must be `Send + Sync` for axum's router.
pub struct AdminState {
    pool: DbPool,
    /// Channel to the subscribe task. Admin sends one message per direction
    /// when a new token is registered; the subscribe task processes them in
    /// order. Failures only log — admin call still succeeds since the DB row
    /// is the source of truth (the matcher will see the new token on its
    /// next hydration).
    subscribe_tx: SubscribeSender,
    /// In-memory faucet-id → external-symbol cache, shared with `HttpPriceClient`.
    /// Mutated atomically alongside DB writes so the price client always sees
    /// the latest mapping without a DB read per fetch.
    token_map: SharedTokenMap,
    /// One token mutation (database write + cache update) at a time.
    write_order: Arc<Mutex<()>>,
}

impl AdminState {
    pub fn new(pool: DbPool, subscribe_tx: SubscribeSender, token_map: SharedTokenMap) -> Self {
        Self {
            pool,
            subscribe_tx,
            token_map,
            write_order: Arc::new(Mutex::new(())),
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
            .route("/admin/tokens", patch(update_token_symbol_handler))
            .route("/admin/tokens", delete(remove_token))
            .layer(middleware::from_fn_with_state(token, require_bearer_token))
            .with_state(self)
    }

    pub async fn load_tokens_from_db(&self) -> DbResult<Vec<TokenId>> {
        self.pool
            .read(db::postgres_db::load_registered_tokens_tx)
            .await
    }

    /// Run one token mutation and keep the in-memory symbol cache equal to
    /// the database. `operation` returns `(changed, value)`; when `changed`,
    /// the cache entry for `token` becomes `symbol` (`None` removes it).
    ///
    /// Mutations run one at a time, each with its cache update, so the cache
    /// changes in commit order. They run in their own task: an HTTP caller
    /// that disconnects mid-request cannot leave a committed row without its
    /// cache update.
    async fn write_token<T, F>(
        &self,
        token: TokenId,
        symbol: Option<String>,
        operation: F,
    ) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> DbResult<(bool, T)> + Send + 'static,
    {
        let order = self.write_order.clone().lock_owned().await;
        let (pool, token_map) = (self.pool.clone(), self.token_map.clone());
        tokio::spawn(async move {
            let _order = order;
            let (changed, value) = pool.write(operation).await?;
            if changed {
                let mut map = crate::price::write_token_map(&token_map);
                match symbol {
                    Some(symbol) => map.insert(token, symbol),
                    None => map.remove(&token),
                };
            }
            Ok(value)
        })
        .await?
    }

    /// Register `token`; `true` when it is new. A new token is subscribed
    /// against every registered token, in both directions.
    async fn register_token(&self, token: TokenId, symbol: Option<String>) -> DbResult<bool> {
        let db_symbol = symbol.clone();
        let existing = self
            .write_token(token, symbol, move |conn| {
                if !db::postgres_db::register_token_tx(conn, token, db_symbol.as_deref())? {
                    return Ok((false, None));
                }
                let existing = db::postgres_db::load_registered_tokens_tx(conn)?;
                Ok((true, Some(existing)))
            })
            .await?;
        let Some(existing) = existing else {
            return Ok(false);
        };
        // Send failures only log: the database row is the source of truth and
        // a restart re-subscribes every registered pair.
        for other in existing.into_iter().filter(|other| *other != token) {
            for pair in [(token, other), (other, token)] {
                if let Err(error) = self.subscribe_tx.send(pair).await {
                    tracing::warn!(%error, "admin: subscribe channel send failed");
                }
            }
        }
        Ok(true)
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
        .register_token(token, req.external_symbol)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if inserted {
        Ok((StatusCode::CREATED, "registered"))
    } else {
        Ok((StatusCode::OK, "already registered"))
    }
}

async fn update_token_symbol_handler(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<StatusCode, StatusCode> {
    let token = parse_token(&req.token_id)?;
    let symbol = req.external_symbol;
    let db_symbol = symbol.clone();
    let updated = state
        .write_token(token, symbol, move |conn| {
            let updated =
                db::postgres_db::update_token_symbol_tx(conn, token, db_symbol.as_deref())?;
            Ok((updated, updated))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(if updated {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    })
}

async fn remove_token(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<StatusCode, StatusCode> {
    let token = parse_token(&req.token_id)?;
    let deleted = state
        .write_token(token, None, move |conn| {
            let deleted = db::postgres_db::unregister_token_tx(conn, token)?;
            Ok((deleted, deleted))
        })
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
    #[serde(default)]
    pub external_symbol: Option<String>,
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
    use std::collections::HashMap;
    use std::sync::RwLock;

    use crate::db::postgres_test::TestDb;

    fn test_token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn test_token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    const TEST_TOKEN: &str = "test-admin-token";

    async fn make_state_with_map() -> (Arc<AdminState>, SharedTokenMap, TestDb) {
        let test_db = TestDb::new().await.unwrap();
        // Tests don't exercise the subscribe path; create a channel whose
        // receiver is dropped immediately. Sends will fail but admin handlers
        // log and continue.
        let (subscribe_tx, _) = mpsc::channel::<(TokenId, TokenId)>(8);
        let token_map: SharedTokenMap = Arc::new(RwLock::new(HashMap::new()));
        let state = Arc::new(AdminState::new(
            test_db.pool.clone(),
            subscribe_tx,
            token_map.clone(),
        ));
        (state, token_map, test_db)
    }

    async fn make_state() -> (Arc<AdminState>, TestDb) {
        let (state, _, db) = make_state_with_map().await;
        (state, db)
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

    /// Returns (server, cache) so tests can inspect the in-memory cache.
    async fn test_server_with_cache() -> (TestServer, SharedTokenMap, TestDb) {
        let (state, cache, db) = make_state_with_map().await;
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer test-admin-token"),
        );
        (server, cache, db)
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

    // ── New: symbol-cache tests ────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn add_token_with_symbol_persists_to_cache() {
        let (server, cache, _db) = test_server_with_cache().await;
        let res = server
            .post("/admin/tokens")
            .json(&json!({
                "token_id": token_hex(test_token_a()),
                "external_symbol": "usd-coin"
            }))
            .await;
        res.assert_status(StatusCode::CREATED);
        let map = cache.read().unwrap();
        assert_eq!(
            map.get(&test_token_a()).map(String::as_str),
            Some("usd-coin")
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn patch_token_symbol_updates_cache_and_db() {
        let (server, cache, _db) = test_server_with_cache().await;
        // Register without a symbol.
        server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        // Patch it in.
        let res = server
            .patch("/admin/tokens")
            .json(&json!({
                "token_id": token_hex(test_token_a()),
                "external_symbol": "ethereum"
            }))
            .await;
        res.assert_status(StatusCode::OK);
        let map = cache.read().unwrap();
        assert_eq!(
            map.get(&test_token_a()).map(String::as_str),
            Some("ethereum")
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn patch_token_symbol_returns_404_for_unknown() {
        let (server, cache, _db) = test_server_with_cache().await;
        let res = server
            .patch("/admin/tokens")
            .json(&json!({
                "token_id": token_hex(test_token_a()),
                "external_symbol": "ethereum"
            }))
            .await;
        res.assert_status(StatusCode::NOT_FOUND);
        assert!(cache.read().unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn delete_token_clears_cache_entry() {
        let (server, cache, _db) = test_server_with_cache().await;
        let body = json!({
            "token_id": token_hex(test_token_a()),
            "external_symbol": "usd-coin"
        });
        server.post("/admin/tokens").json(&body).await;
        assert!(cache.read().unwrap().contains_key(&test_token_a()));

        let res = server
            .delete("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::OK);
        assert!(!cache.read().unwrap().contains_key(&test_token_a()));
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn token_writes_update_the_cache_in_commit_order_despite_caller_abort() {
        let (state, map, _db) = make_state_with_map().await;
        let token = test_token_a();
        assert!(state.register_token(token, None).await.unwrap());

        // The first caller disconnects after its write started; its cache
        // update must still land, and before the second write's.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let first_state = state.clone();
        let first = tokio::spawn(async move {
            first_state
                .write_token(token, Some("first".into()), move |conn| {
                    let _ = started_tx.send(());
                    std::thread::sleep(std::time::Duration::from_millis(80));
                    db::postgres_db::update_token_symbol_tx(conn, token, Some("first"))?;
                    Ok((true, ()))
                })
                .await
        });
        started_rx.await.unwrap();
        first.abort();
        state
            .write_token(token, Some("second".into()), move |conn| {
                db::postgres_db::update_token_symbol_tx(conn, token, Some("second"))?;
                Ok((true, ()))
            })
            .await
            .unwrap();
        assert_eq!(
            map.read().unwrap().get(&token).map(String::as_str),
            Some("second")
        );
    }
}

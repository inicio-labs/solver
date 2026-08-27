use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;

use crate::db::{self, DbPool};
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
}

impl AdminState {
    pub fn new(
        pool: DbPool,
        subscribe_tx: SubscribeSender,
        token_map: SharedTokenMap,
    ) -> Self {
        Self { pool, subscribe_tx, token_map }
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

    pub fn load_tokens_from_db(&self) -> anyhow::Result<Vec<TokenId>> {
        db::load_registered_tokens(&self.pool)
    }

    /// Update the in-memory symbol cache. Lock held briefly, no awaits.
    fn set_cache(&self, token: TokenId, symbol: Option<String>) {
        let mut map = crate::price::write_token_map(&self.token_map);
        match symbol {
            Some(s) => {
                map.insert(token, s);
            }
            None => {
                map.remove(&token);
            }
        }
    }

    async fn register_token(
        &self,
        new_token: TokenId,
        external_symbol: Option<String>,
    ) -> anyhow::Result<bool> {
        let mut token_bytes = Vec::new();
        new_token.write_into(&mut token_bytes);

        let mut conn = self.pool.write_conn()?;
        let inserted = db::register_token(&mut conn, &token_bytes, external_symbol.as_deref())?;

        if inserted {
            // Reflect the new mapping in the in-memory cache (if any).
            if external_symbol.is_some() {
                self.set_cache(new_token, external_symbol.clone());
            }

            // Tell the subscribe task to subscribe both directions of every
            // existing pair involving the new token. Channel send failures
            // are logged but don't fail the admin call — the DB row is the
            // source of truth and a restart will reconcile.
            let existing = self.load_tokens_from_db()?;
            for token in &existing {
                if *token != new_token {
                    if let Err(e) = self.subscribe_tx.send((new_token, *token)).await {
                        tracing::warn!(error = %e, "admin: subscribe channel send failed");
                    }
                    if let Err(e) = self.subscribe_tx.send((*token, new_token)).await {
                        tracing::warn!(error = %e, "admin: subscribe channel send failed");
                    }
                }
            }
        }

        Ok(inserted)
    }
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
        Some(t) if bool::from(t.as_bytes().ct_eq(expected.as_bytes())) => {
            Ok(next.run(req).await)
        }
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

// ── Route Handlers ──────────────────────────────────────────────────────────

async fn list_tokens(
    State(state): State<Arc<AdminState>>,
) -> Result<Json<Vec<TokenResponse>>, StatusCode> {
    let tokens = state
        .load_tokens_from_db()
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
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token = TokenId::read_from(&mut SliceReader::new(&bytes))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

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
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token = TokenId::read_from(&mut SliceReader::new(&bytes))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut conn = state
        .pool
        .write_conn()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let updated = db::update_token_symbol(&mut conn, &bytes, req.external_symbol.as_deref())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !updated {
        return Ok(StatusCode::NOT_FOUND);
    }
    state.set_cache(token, req.external_symbol);
    Ok(StatusCode::OK)
}

async fn remove_token(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<StatusCode, StatusCode> {
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token = TokenId::read_from(&mut SliceReader::new(&bytes))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut conn = state
        .pool
        .write_conn()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let deleted = db::unregister_token(&mut conn, &bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if deleted {
        state.set_cache(token, None);
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
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
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };
    use miden_protocol::account::AccountId;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::RwLock;

    use crate::db;

    fn test_token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn test_token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    fn unique_db_url() -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("file:admintest{}?mode=memory&cache=shared", n)
    }

    const TEST_TOKEN: &str = "test-admin-token";

    fn make_state_with_map() -> (Arc<AdminState>, SharedTokenMap) {
        let pool = db::init_db(&unique_db_url(), 1).unwrap();
        // Tests don't exercise the subscribe path; create a channel whose
        // receiver is dropped immediately. Sends will fail but admin handlers
        // log and continue.
        let (subscribe_tx, _) = mpsc::channel::<(TokenId, TokenId)>(8);
        let token_map: SharedTokenMap = Arc::new(RwLock::new(HashMap::new()));
        let state = Arc::new(AdminState::new(pool, subscribe_tx, token_map.clone()));
        (state, token_map)
    }

    pub fn make_state() -> Arc<AdminState> {
        make_state_with_map().0
    }

    fn test_server() -> TestServer {
        let state = make_state();
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer test-admin-token"),
        );
        server
    }

    /// Returns (server, cache) so tests can inspect the in-memory cache.
    fn test_server_with_cache() -> (TestServer, SharedTokenMap) {
        let (state, cache) = make_state_with_map();
        let token = Arc::new(TEST_TOKEN.to_string());
        let mut server = TestServer::new(state.router(Some(token)));
        server.add_header(
            AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer test-admin-token"),
        );
        (server, cache)
    }

    fn token_hex(token: TokenId) -> String {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        hex::encode(bytes)
    }

    #[tokio::test]
    async fn add_token_returns_created_first_time() {
        let server = test_server();
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::CREATED);
        assert_eq!(res.text(), "registered");
    }

    #[tokio::test]
    async fn add_token_returns_ok_on_duplicate() {
        let server = test_server();
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.post("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.text(), "already registered");
    }

    #[tokio::test]
    async fn add_token_returns_bad_request_for_invalid_hex() {
        let server = test_server();
        let res = server
            .post("/admin/tokens")
            .json(&json!({ "token_id": "not_hex!" }))
            .await;
        res.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn remove_token_returns_ok_when_found() {
        let server = test_server();
        let body = json!({ "token_id": token_hex(test_token_a()) });
        server.post("/admin/tokens").json(&body).await;
        let res = server.delete("/admin/tokens").json(&body).await;
        res.assert_status(StatusCode::OK);
    }

    #[tokio::test]
    async fn remove_token_returns_not_found_when_missing() {
        let server = test_server();
        let res = server
            .delete("/admin/tokens")
            .json(&json!({ "token_id": token_hex(test_token_a()) }))
            .await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn requests_without_bearer_token_return_unauthorized() {
        let state = make_state();
        let token = Arc::new(TEST_TOKEN.to_string());
        let server = TestServer::new(state.router(Some(token)));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn requests_with_wrong_token_return_unauthorized() {
        let state = make_state();
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
    async fn router_with_no_admin_token_returns_404() {
        let state = make_state();
        let server = TestServer::new(state.router(None));
        let res = server.get("/admin/tokens").await;
        res.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_tokens_returns_all_registered() {
        let server = test_server();
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
    async fn add_token_with_symbol_persists_to_cache() {
        let (server, cache) = test_server_with_cache();
        let res = server
            .post("/admin/tokens")
            .json(&json!({
                "token_id": token_hex(test_token_a()),
                "external_symbol": "usd-coin"
            }))
            .await;
        res.assert_status(StatusCode::CREATED);
        let map = cache.read().unwrap();
        assert_eq!(map.get(&test_token_a()).map(String::as_str), Some("usd-coin"));
    }

    #[tokio::test]
    async fn patch_token_symbol_updates_cache_and_db() {
        let (server, cache) = test_server_with_cache();
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
        assert_eq!(map.get(&test_token_a()).map(String::as_str), Some("ethereum"));
    }

    #[tokio::test]
    async fn patch_token_symbol_returns_404_for_unknown() {
        let (server, cache) = test_server_with_cache();
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
    async fn delete_token_clears_cache_entry() {
        let (server, cache) = test_server_with_cache();
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
}

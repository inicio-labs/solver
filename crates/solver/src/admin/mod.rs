use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::db::{self, DbPool};
use crate::ingest::MidenClient;
use crate::types::TokenId;

/// Shared state for admin routes.
/// Uses `dyn MidenClient` to avoid generics leaking into axum handlers.
pub struct AdminState {
    pool: DbPool,
    client: Arc<Mutex<dyn MidenClient + Send>>,
}

impl AdminState {
    pub fn new(pool: DbPool, client: Arc<Mutex<dyn MidenClient + Send>>) -> Self {
        Self { pool, client }
    }

    /// Build the admin router.
    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/admin/tokens", get(list_tokens))
            .route("/admin/tokens", post(add_token))
            .route("/admin/tokens", delete(remove_token))
            .with_state(self)
    }

    pub fn load_tokens_from_db(&self) -> anyhow::Result<Vec<TokenId>> {
        db::load_registered_tokens(&self.pool)
    }


    async fn register_token(&self, new_token: TokenId) -> anyhow::Result<bool> {
        let mut token_bytes = Vec::new();
        new_token.write_into(&mut token_bytes);

        let mut conn = self.pool.write_conn()?;
        let inserted = db::register_token(&mut conn, &token_bytes)?;

        if inserted {
            let existing = self.load_tokens_from_db()?;
            let mut client = self.client.lock().await;
            for token in &existing {
                if *token != new_token {
                    client.subscribe_pair(new_token, *token).await?;
                    client.subscribe_pair(*token, new_token).await?;
                }
            }
        }

        Ok(inserted)
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
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut conn = state
        .pool
        .write_conn()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let deleted = db::unregister_token(&mut conn, &bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if deleted {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
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
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };
    use miden_protocol::account::AccountId;
    use serde_json::json;

    use crate::db;
    use crate::ingest::tests::MockMidenClient;

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

    fn test_server() -> TestServer {
        let pool = db::init_db(&unique_db_url(), 1).unwrap();
        let client = Arc::new(Mutex::new(MockMidenClient::new()));
        let state = Arc::new(AdminState::new(pool, client));
        TestServer::new(state.router())
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
}

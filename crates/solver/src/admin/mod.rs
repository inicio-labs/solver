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

    /// Load all registered tokens from DB as TokenIds.
    pub fn load_tokens_from_db(&self) -> anyhow::Result<Vec<TokenId>> {
        let mut conn = self.pool.get()?;
        let rows = db::get_registered_tokens(&mut conn)?;
        let mut tokens = Vec::new();
        for row in rows {
            let token = TokenId::read_from(&mut SliceReader::new(&row.token_id))
                .map_err(|e| anyhow::anyhow!("invalid token: {e}"))?;
            tokens.push(token);
        }
        Ok(tokens)
    }

    /// Generate all pairs from registered tokens and subscribe the client to each.
    pub async fn subscribe_all_pairs(&self) -> anyhow::Result<()> {
        let tokens = self.load_tokens_from_db()?;
        let mut client = self.client.lock().await;
        for i in 0..tokens.len() {
            for j in 0..tokens.len() {
                if i != j {
                    client.subscribe_pair(tokens[i], tokens[j]).await?;
                }
            }
        }
        Ok(())
    }

    /// Register a new token: persist to DB, then subscribe all new pairs with existing tokens.
    async fn register_token(&self, new_token: TokenId) -> anyhow::Result<bool> {
        let mut token_bytes = Vec::new();
        new_token.write_into(&mut token_bytes);

        let mut conn = self.pool.get()?;
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
) -> Result<StatusCode, StatusCode> {
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token = TokenId::read_from(&mut SliceReader::new(&bytes))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let inserted = state
        .register_token(token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if inserted {
        Ok(StatusCode::CREATED)
    } else {
        Ok(StatusCode::OK)
    }
}

async fn remove_token(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<TokenRequest>,
) -> Result<StatusCode, StatusCode> {
    let bytes = hex::decode(&req.token_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut conn = state
        .pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let deleted = db::unregister_token(&mut conn, &bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if deleted {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// Seed tokens from config into the DB (idempotent).
pub fn seed_tokens_from_config(pool: &DbPool, tokens: &[TokenId]) -> anyhow::Result<()> {
    let mut conn = pool.get()?;
    for token in tokens {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        db::register_token(&mut conn, &bytes)?;
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct TokenRequest {
    pub token_id: String,
}

#[derive(Serialize)]
pub struct TokenResponse {
    pub token_id: String,
}

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use crate::matching::price_feed::UsdCents;
use crate::price::{PriceClient, PriceSnapshot};
use crate::types::TokenId;

/// Shared, in-memory faucet-id → external-symbol mapping. Hydrated from DB
/// at boot; admin handlers update both DB and this cache atomically so the
/// price client always sees the latest mapping without a DB read per fetch.
pub type SharedSymbolMap = Arc<RwLock<HashMap<TokenId, String>>>;

/// Acquire a read guard on the shared symbol map, recovering from lock
/// poisoning instead of panicking.
///
/// A `std::sync::RwLock` stays poisoned permanently once any thread panics
/// while holding it, so `.read().expect(..)` would turn one unrelated panic
/// into a *permanent* crash source on every subsequent price fetch / admin
/// call. The protected value is only a `HashMap<TokenId, String>`; a panic
/// by a prior holder cannot leave it in an invariant-violating state, so
/// recovering the guard via `PoisonError::into_inner()` is strictly safe
/// and makes poisoning a non-event.
pub fn read_symbol_map(m: &SharedSymbolMap) -> RwLockReadGuard<'_, HashMap<TokenId, String>> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

/// Write-guard counterpart of [`read_symbol_map`]; same poison-recovery
/// rationale.
pub fn write_symbol_map(m: &SharedSymbolMap) -> RwLockWriteGuard<'_, HashMap<TokenId, String>> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

/// Default public CoinGecko "simple price" endpoint. Override per-deployment via
/// `[engine].price_api_base_url` (e.g. a self-hosted or mock price service for
/// devnet/local) — see [`HttpPriceClient::new_with_base`].
pub const COINGECKO_BASE: &str = "https://api.coingecko.com/api/v3/simple/price";

#[derive(Deserialize)]
struct CgPriceEntry {
    usd: f64,
}

pub struct HttpPriceClient {
    http: reqwest::Client,
    api_key: Option<String>,
    symbol_map: SharedSymbolMap,
    base: String,
}

/// Construct the production [`PriceClient`] (CoinGecko HTTP) as a boxed
/// trait object — the exact `Result<Box<dyn PriceClient + Send + Sync>>`
/// shape `crate::start` expects for price-client dependency injection.
///
/// Keeps the `Box`/unsizing/`?` plumbing in the library, next to
/// `HttpPriceClient`, so the binary's injection site is a single name
/// (`solver::price::build_http_price_client`) instead of an inline closure
/// with an explicit trait-object `as` cast.
pub fn build_http_price_client(
    symbol_map: SharedSymbolMap,
    api_key: Option<String>,
) -> Result<Box<dyn PriceClient + Send + Sync>> {
    Ok(Box::new(HttpPriceClient::new(symbol_map, api_key)?))
}

/// Like [`build_http_price_client`] but with an explicit base URL — point the
/// **real** production price path at a self-hosted or mock CoinGecko-compatible
/// endpoint (devnet/local) instead of the public API. A `None`/default base
/// just yields the public CoinGecko client.
pub fn build_http_price_client_with_base(
    symbol_map: SharedSymbolMap,
    api_key: Option<String>,
    base: String,
) -> Result<Box<dyn PriceClient + Send + Sync>> {
    Ok(Box::new(HttpPriceClient::new_with_base(symbol_map, api_key, base)?))
}

impl HttpPriceClient {
    /// Construct a client targeting the public CoinGecko API. The `api_key`,
    /// if provided, is sent as the `x-cg-demo-api-key` header (Demo API tier).
    ///
    /// Fallible: building the underlying `reqwest` client can fail if the
    /// system TLS backend cannot initialise. Surfaced as an error so the
    /// caller can fail startup cleanly rather than panic+abort.
    pub fn new(symbol_map: SharedSymbolMap, api_key: Option<String>) -> Result<Self> {
        Self::new_with_base(symbol_map, api_key, COINGECKO_BASE.to_string())
    }

    /// Construct a client targeting a custom CoinGecko-compatible base URL —
    /// e.g. a self-hosted or **mock** price service for devnet/local runs where
    /// the public API has no listing and no key is available. `new` defaults to
    /// [`COINGECKO_BASE`]. The endpoint must answer
    /// `GET {base}?ids=<csv>&vs_currencies=usd` with `{"<id>":{"usd":<f64>}}`.
    pub fn new_with_base(
        symbol_map: SharedSymbolMap,
        api_key: Option<String>,
        base: String,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed to build reqwest client for price feed")?;
        Ok(Self {
            http,
            api_key,
            symbol_map,
            base,
        })
    }
}

#[async_trait]
impl PriceClient for HttpPriceClient {
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PriceSnapshot> {
        // 1. Snapshot the shared mapping under the read lock. Cloning the
        //    matched entries (small HashMap) avoids holding the lock across
        //    the awaited HTTP call.
        let requested: HashMap<TokenId, String> = {
            let map = read_symbol_map(&self.symbol_map);
            tokens
                .iter()
                .filter_map(|t| map.get(t).map(|s| (*t, s.clone())))
                .collect()
        };
        if requested.is_empty() {
            return Ok(PriceSnapshot::new());
        }

        // 2. Build the CoinGecko request.
        let ids: Vec<&str> = requested.values().map(|s| s.as_str()).collect();
        let ids_csv = ids.join(",");
        let url = format!("{}?ids={ids_csv}&vs_currencies=usd", self.base);

        let mut req = self.http.get(&url);
        if let Some(key) = &self.api_key {
            req = req.header("x-cg-demo-api-key", key);
        }

        let resp = req.send().await.context("coingecko HTTP request")?;
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("(unset)");
            return Err(anyhow!(
                "coingecko rate-limited (429); retry-after={retry_after}; \
                 matcher will continue on last known prices"
            ));
        }
        if !status.is_success() {
            return Err(anyhow!("coingecko returned status {}", status));
        }
        let body: HashMap<String, CgPriceEntry> =
            resp.json().await.context("parse coingecko response")?;

        // 3. Map symbols back to TokenIds and convert USD → cents.
        let mut out = PriceSnapshot::new();
        for (token, symbol) in &requested {
            if let Some(entry) = body.get(symbol.as_str()) {
                let cents = (entry.usd * 100.0).round() as u64;
                out.insert(*token, cents as UsdCents);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    fn shared_map(entries: Vec<(TokenId, &str)>) -> SharedSymbolMap {
        let mut m = HashMap::new();
        for (t, s) in entries {
            m.insert(t, s.to_string());
        }
        Arc::new(RwLock::new(m))
    }

    #[tokio::test]
    async fn fetch_prices_returns_mapped_cents_for_known_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/simple/price"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"usd-coin":{"usd":1.0},"ethereum":{"usd":3942.17}}"#),
            )
            .mount(&server)
            .await;

        let map = shared_map(vec![(token_a(), "usd-coin"), (token_b(), "ethereum")]);
        let client = HttpPriceClient::new_with_base(
            map,
            None,
            format!("{}/simple/price", server.uri()),
        )
        .expect("build HttpPriceClient");
        let prices = client
            .fetch_prices(&[token_a(), token_b()])
            .await
            .expect("ok");

        assert_eq!(prices.get(&token_a()).copied(), Some(100));
        assert_eq!(prices.get(&token_b()).copied(), Some(394_217));
    }

    #[tokio::test]
    async fn fetch_prices_omits_tokens_without_mapping() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/simple/price"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(r#"{"usd-coin":{"usd":1.0}}"#),
            )
            .mount(&server)
            .await;

        // Only token_a has a mapping.
        let map = shared_map(vec![(token_a(), "usd-coin")]);
        let client = HttpPriceClient::new_with_base(
            map,
            None,
            format!("{}/simple/price", server.uri()),
        )
        .expect("build HttpPriceClient");
        let prices = client
            .fetch_prices(&[token_a(), token_b()])
            .await
            .expect("ok");

        assert_eq!(prices.len(), 1);
        assert!(prices.contains_key(&token_a()));
        assert!(!prices.contains_key(&token_b()));
    }

    #[tokio::test]
    async fn fetch_prices_returns_empty_when_no_known_tokens() {
        // No mock mounted — if we hit the network, the test fails on connection error.
        let map = shared_map(vec![]);
        let client = HttpPriceClient::new_with_base(
            map,
            None,
            "http://unreachable.invalid/simple/price".to_string(),
        )
        .expect("build HttpPriceClient");
        let prices = client.fetch_prices(&[token_a()]).await.expect("ok");
        assert!(prices.is_empty());
    }

    #[tokio::test]
    async fn fetch_prices_propagates_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/simple/price"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let map = shared_map(vec![(token_a(), "usd-coin")]);
        let client = HttpPriceClient::new_with_base(
            map,
            None,
            format!("{}/simple/price", server.uri()),
        )
        .expect("build HttpPriceClient");
        let err = client.fetch_prices(&[token_a()]).await.unwrap_err();
        assert!(err.to_string().contains("503"), "got: {err}");
    }

    #[tokio::test]
    async fn fetch_prices_handles_429_with_distinct_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/simple/price"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("retry-after", "60"),
            )
            .mount(&server)
            .await;

        let map = shared_map(vec![(token_a(), "usd-coin")]);
        let client = HttpPriceClient::new_with_base(
            map,
            None,
            format!("{}/simple/price", server.uri()),
        )
        .expect("build HttpPriceClient");
        let err = client.fetch_prices(&[token_a()]).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("rate-limited"), "got: {msg}");
        assert!(msg.contains("60"), "retry-after should appear: {msg}");
    }
}

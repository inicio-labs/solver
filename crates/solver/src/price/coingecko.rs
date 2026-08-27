use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

use crate::price::{read_token_map, PriceClient, PriceData, PreciseSnapshot, SharedTokenMap};
use crate::types::TokenId;

/// Default public CoinGecko "simple price" endpoint. Override per-deployment via
/// `[engine].price_api_base_url` (e.g. a self-hosted or mock price service for
/// devnet/local) — see [`HttpPriceClient::new_with_base`].
pub const COINGECKO_BASE: &str = "https://api.coingecko.com/api/v3/simple/price";

pub struct HttpPriceClient {
    http: reqwest::Client,
    api_key: Option<String>,
    token_map: SharedTokenMap,
    base: String,
    /// CoinGecko `vs_currencies` (quote currency), e.g. `"usd"`. Also the key of
    /// the per-id price object in the response.
    vs_currency: String,
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
    token_map: SharedTokenMap,
    api_key: Option<String>,
) -> Result<Box<dyn PriceClient + Send + Sync>> {
    Ok(Box::new(HttpPriceClient::new(token_map, api_key)?))
}

/// Like [`build_http_price_client`] but with an explicit base URL + quote
/// currency — point the **real** production price path at a self-hosted or mock
/// CoinGecko-compatible endpoint (devnet/local). `vs_currency` is CoinGecko's
/// `vs_currencies` (e.g. `"usd"`).
pub fn build_http_price_client_with_base(
    token_map: SharedTokenMap,
    api_key: Option<String>,
    base: String,
    vs_currency: String,
) -> Result<Box<dyn PriceClient + Send + Sync>> {
    Ok(Box::new(HttpPriceClient::new_with_base(token_map, api_key, base, vs_currency)?))
}

impl HttpPriceClient {
    /// Construct a client targeting the public CoinGecko API. The `api_key`,
    /// if provided, is sent as the `x-cg-demo-api-key` header (Demo API tier).
    ///
    /// Fallible: building the underlying `reqwest` client can fail if the
    /// system TLS backend cannot initialise. Surfaced as an error so the
    /// caller can fail startup cleanly rather than panic+abort.
    pub fn new(token_map: SharedTokenMap, api_key: Option<String>) -> Result<Self> {
        Self::new_with_base(token_map, api_key, COINGECKO_BASE.to_string(), "usd".to_string())
    }

    /// Construct a client targeting a custom CoinGecko-compatible base URL +
    /// quote currency — e.g. a self-hosted or **mock** price service for
    /// devnet/local. `new` defaults to [`COINGECKO_BASE`] + `"usd"`. The endpoint
    /// must answer `GET {base}?ids=<csv>&vs_currencies=<vs>&precision=full` with
    /// `{"<id>":{"<vs>":<f64>}}`.
    pub fn new_with_base(
        token_map: SharedTokenMap,
        api_key: Option<String>,
        base: String,
        vs_currency: String,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed to build reqwest client for price feed")?;
        Ok(Self {
            http,
            api_key,
            token_map,
            base,
            vs_currency,
        })
    }
}

#[async_trait]
impl PriceClient for HttpPriceClient {
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PreciseSnapshot> {
        // 1. Snapshot the shared mapping under the read lock. Cloning the
        //    matched entries (small HashMap) avoids holding the lock across
        //    the awaited HTTP call.
        let requested: HashMap<TokenId, String> = {
            let map = read_token_map(&self.token_map);
            tokens
                .iter()
                .filter_map(|t| map.get(t).map(|s| (*t, s.clone())))
                .collect()
        };
        if requested.is_empty() {
            return Ok(PreciseSnapshot::new());
        }

        // 2. Build the request. `precision=full` so we keep CoinGecko's complete
        //    value (it otherwise default-rounds); `vs_currencies` is configurable.
        let ids: Vec<&str> = requested.values().map(|s| s.as_str()).collect();
        let ids_csv = ids.join(",");
        let url = format!(
            "{}?ids={ids_csv}&vs_currencies={}&precision=full",
            self.base, self.vs_currency
        );

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
        // Response shape: { "<id>": { "<vs_currency>": <f64> }, ... }
        let body: HashMap<String, HashMap<String, f64>> =
            resp.json().await.context("parse coingecko response")?;

        // 3. Map symbols back to TokenIds; keep the full-precision value. Drop any
        //    non-finite / negative value at the edge so it reads as "unpriced"
        //    (excluded from matching) rather than corrupting it.
        let mut out = PreciseSnapshot::new();
        for (token, symbol) in &requested {
            if let Some(usd) = body.get(symbol.as_str()).and_then(|m| m.get(&self.vs_currency)) {
                if usd.is_finite() && *usd >= 0.0 {
                    out.insert(*token, PriceData { usd: *usd });
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};
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

    fn shared_map(entries: Vec<(TokenId, &str)>) -> SharedTokenMap {
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
            "usd".to_string(),
        )
        .expect("build HttpPriceClient");
        let prices = client
            .fetch_prices(&[token_a(), token_b()])
            .await
            .expect("ok");

        assert_eq!(prices.get(&token_a()).map(|d| d.usd), Some(1.0));
        assert_eq!(prices.get(&token_b()).map(|d| d.usd), Some(3942.17));
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
            "usd".to_string(),
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
            "usd".to_string(),
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
            "usd".to_string(),
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
            "usd".to_string(),
        )
        .expect("build HttpPriceClient");
        let err = client.fetch_prices(&[token_a()]).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("rate-limited"), "got: {msg}");
        assert!(msg.contains("60"), "retry-after should appear: {msg}");
    }
}

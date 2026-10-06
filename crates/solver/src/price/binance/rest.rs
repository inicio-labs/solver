//! Public `exchangeInfo` lookups, one symbol per request.
//!
//! A request listing several symbols fails as a whole (HTTP 400, code -1121)
//! when one of them is unknown, so each symbol is checked on its own and an
//! unknown one is reported without hiding the others.

use std::time::Duration;

use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

use super::market::{AssetCode, Listing, Symbol};

/// For HTTP 429 or 418 (rate limited or banned), the server's `Retry-After`
/// minimum wait if it sent one; `None` for any other status.
pub(super) fn rate_limit(status: StatusCode, headers: &HeaderMap) -> Option<Option<Duration>> {
    (status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT).then(|| {
        headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
    })
}

/// A failed lookup; see [`LookupError::is_transient`].
#[derive(Debug, Error)]
pub(super) enum LookupError {
    #[error("exchangeInfo request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("exchangeInfo rate limited (HTTP {status}); retry after {retry_after:?}")]
    RateLimited {
        status: StatusCode,
        retry_after: Option<Duration>,
    },
    #[error("exchangeInfo answered HTTP {0}")]
    Status(StatusCode),
    #[error("exchangeInfo body exceeds {0} bytes")]
    TooLarge(usize),
    #[error("malformed exchangeInfo response: {0}")]
    Malformed(String),
}

impl LookupError {
    /// The server-imposed minimum wait, if any.
    pub(super) fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Whether retrying can help: network failures, rate limits, Binance's
    /// WAF (403), timeouts and server errors. Any other answer will not change
    /// on retry and rejects only this symbol.
    pub(super) fn is_transient(&self) -> bool {
        match self {
            Self::Request(_) | Self::RateLimited { .. } => true,
            Self::Status(status) => {
                status.is_server_error()
                    || *status == StatusCode::FORBIDDEN
                    || *status == StatusCode::REQUEST_TIMEOUT
            }
            Self::TooLarge(_) | Self::Malformed(_) => false,
        }
    }
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
}

/// Read at most `limit` bytes of `response`'s body.
async fn bounded_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, LookupError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(LookupError::TooLarge(limit));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > limit {
            return Err(LookupError::TooLarge(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Binance's listing of `symbol`; `None` when Binance rejects the symbol.
pub(super) async fn fetch_listing(
    http: &reqwest::Client,
    base_url: &str,
    symbol: &Symbol,
    body_limit: usize,
) -> Result<Option<Listing>, LookupError> {
    let url = format!(
        "{}/api/v3/exchangeInfo?symbol={symbol}",
        base_url.trim_end_matches('/')
    );
    let response = http.get(url).send().await?;
    let status = response.status();
    if let Some(retry_after) = rate_limit(status, response.headers()) {
        return Err(LookupError::RateLimited {
            status,
            retry_after,
        });
    }
    // Binance answers a request about an unknown or invalid symbol with 400.
    if status == StatusCode::BAD_REQUEST {
        let body = bounded_body(response, body_limit).await?;
        tracing::debug!(%symbol, body = %String::from_utf8_lossy(&body), "exchangeInfo rejected symbol");
        return Ok(None);
    }
    if !status.is_success() {
        return Err(LookupError::Status(status));
    }
    let body = bounded_body(response, body_limit).await?;
    let malformed = |reason: String| LookupError::Malformed(reason);
    let info: ExchangeInfo =
        serde_json::from_slice(&body).map_err(|error| malformed(error.to_string()))?;
    let [entry] = info.symbols.as_slice() else {
        return Err(malformed(format!(
            "expected one symbol, got {}",
            info.symbols.len()
        )));
    };
    if entry.symbol != symbol.as_str() {
        return Err(malformed(format!(
            "asked for {symbol}, got {}",
            entry.symbol
        )));
    }
    let asset = |code: &str| AssetCode::parse(code).map_err(|error| malformed(error.to_string()));
    Ok(Some(Listing {
        base_asset: asset(&entry.base_asset)?,
        quote_asset: asset(&entry.quote_asset)?,
        status: entry.status.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::price::binance::test_support::{listing, symbol};

    const LIMIT: usize = 1024;

    async fn lookup(response: ResponseTemplate) -> Result<Option<Listing>, LookupError> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/exchangeInfo"))
            .and(query_param("symbol", "ETHUSDT"))
            .respond_with(response)
            .mount(&server)
            .await;
        fetch_listing(
            &reqwest::Client::new(),
            &server.uri(),
            &symbol("ETHUSDT"),
            LIMIT,
        )
        .await
    }

    fn entry(symbol: &str, base: &str) -> serde_json::Value {
        serde_json::json!({
            "symbol": symbol, "status": "TRADING", "baseAsset": base, "quoteAsset": "USDT",
        })
    }

    fn listed(entries: Vec<serde_json::Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({ "symbols": entries }))
    }

    #[tokio::test]
    async fn a_listing_is_parsed_and_an_unknown_symbol_is_none() {
        let found = lookup(listed(vec![entry("ETHUSDT", "ETH")])).await.unwrap();
        assert_eq!(found, Some(listing("ETH", "USDT")));
        let unknown = ResponseTemplate::new(400).set_body_string(r#"{"code":-1121}"#);
        assert_eq!(lookup(unknown).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rate_limits_carry_their_retry_after() {
        for status in [429, 418] {
            let error = lookup(ResponseTemplate::new(status).insert_header("Retry-After", "7"))
                .await
                .unwrap_err();
            assert!(error.is_transient(), "{error}");
            assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        }
        let unparsable = ResponseTemplate::new(429).insert_header("Retry-After", "soon");
        assert_eq!(lookup(unparsable).await.unwrap_err().retry_after(), None);
    }

    #[tokio::test]
    async fn only_answers_that_can_change_are_transient() {
        for status in [403, 408, 500, 503] {
            assert!(lookup(ResponseTemplate::new(status))
                .await
                .unwrap_err()
                .is_transient());
        }
        let permanent = [
            ResponseTemplate::new(404),
            ResponseTemplate::new(200).set_body_string("not json"),
            listed(vec![]),
            listed(vec![entry("ETHUSDT", "ETH"), entry("ETHUSDT", "ETH")]),
            listed(vec![entry("BTCUSDT", "BTC")]),
            listed(vec![entry("ETHUSDT", "ETH-2")]),
            ResponseTemplate::new(200).set_body_string("x".repeat(LIMIT + 1)),
        ];
        for response in permanent {
            let error = lookup(response).await.unwrap_err();
            assert!(!error.is_transient(), "{error}");
        }
    }
}

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

/// Largest accepted `exchangeInfo` body for one symbol.
pub(super) const MAX_BODY_BYTES: usize = 256 * 1024;
/// Binance bans last up to three days; a longer `Retry-After` is a server
/// bug, not an instruction to wait a century.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3 * 24 * 60 * 60);
/// Binance's error code for a symbol it does not list.
const BAD_SYMBOL: i64 = -1121;
/// Bound on server strings copied into errors and logs.
const MAX_QUOTED: usize = 64;

/// Whether `status` is HTTP 429 (rate limited) or 418 (banned).
pub(super) fn is_rate_limited(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT
}

/// The server's `Retry-After` minimum wait in seconds, if it sent one,
/// capped at three days.
pub(super) fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|seconds| Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

/// At most [`MAX_QUOTED`] characters of a server string, escaped.
fn quoted(text: &str) -> String {
    let short: String = text.chars().take(MAX_QUOTED).collect();
    if short.len() < text.len() {
        format!("{short:?}…")
    } else {
        format!("{short:?}")
    }
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
    /// HTTP 400 with a code other than "unknown symbol": the request itself
    /// is wrong, which a retry will not change.
    #[error("exchangeInfo rejected the request (code {code}): {message}")]
    InvalidRequest { code: i64, message: String },
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

    /// Whether Binance asked for a pause: HTTP 429/418, or 403 from its WAF,
    /// which it documents as a possible rate-limit violation.
    pub(super) fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited { .. })
            || matches!(self, Self::Status(status) if *status == StatusCode::FORBIDDEN)
    }

    /// Whether retrying can help: network failures, rate limits, Binance's
    /// WAF (403), timeouts and server errors. Any other answer will not change
    /// on retry, though it is still retried in case the endpoint itself was
    /// misbehaving.
    pub(super) fn is_transient(&self) -> bool {
        match self {
            Self::Request(_) | Self::RateLimited { .. } => true,
            Self::Status(status) => {
                status.is_server_error()
                    || *status == StatusCode::FORBIDDEN
                    || *status == StatusCode::REQUEST_TIMEOUT
            }
            Self::InvalidRequest { .. } | Self::TooLarge(_) | Self::Malformed(_) => false,
        }
    }
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default = "default_true")]
    is_spot_trading_allowed: bool,
}

#[derive(Deserialize)]
struct ApiError {
    code: i64,
    #[serde(default)]
    msg: String,
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

/// Binance's listing of `symbol`; `None` when Binance does not list it.
pub(super) async fn fetch_listing(
    http: &reqwest::Client,
    base_url: &str,
    symbol: &Symbol,
) -> Result<Option<Listing>, LookupError> {
    let url = format!(
        "{}/api/v3/exchangeInfo?symbol={symbol}",
        base_url.trim_end_matches('/')
    );
    let response = http.get(url).send().await?;
    let status = response.status();
    if is_rate_limited(status) {
        return Err(LookupError::RateLimited {
            status,
            retry_after: retry_after(response.headers()),
        });
    }
    if status == StatusCode::BAD_REQUEST {
        let body = bounded_body(response, MAX_BODY_BYTES).await?;
        let text = String::from_utf8_lossy(&body);
        return match serde_json::from_slice::<ApiError>(&body) {
            Ok(error) if error.code == BAD_SYMBOL => {
                tracing::debug!(%symbol, "exchangeInfo does not list the symbol");
                Ok(None)
            }
            Ok(error) => Err(LookupError::InvalidRequest {
                code: error.code,
                message: quoted(&error.msg),
            }),
            Err(_) => Err(LookupError::InvalidRequest {
                code: 0,
                message: quoted(&text),
            }),
        };
    }
    if !status.is_success() {
        return Err(LookupError::Status(status));
    }
    let body = bounded_body(response, MAX_BODY_BYTES).await?;
    let info: ExchangeInfo =
        serde_json::from_slice(&body).map_err(|error| LookupError::Malformed(error.to_string()))?;
    let [entry] = info.symbols.as_slice() else {
        return Err(LookupError::Malformed(format!(
            "expected one symbol, got {}",
            info.symbols.len()
        )));
    };
    if entry.symbol != symbol.as_str() {
        return Err(LookupError::Malformed(format!(
            "asked for {symbol}, got {}",
            quoted(&entry.symbol)
        )));
    }
    let asset = |code: &str| {
        AssetCode::parse(code).map_err(|error| LookupError::Malformed(error.to_string()))
    };
    // Statuses are upper-case words; anything else is not Binance's answer.
    if entry.status.is_empty()
        || entry.status.len() > 32
        || !entry
            .status
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'_')
    {
        return Err(LookupError::Malformed(format!(
            "unexpected status {}",
            quoted(&entry.status)
        )));
    }
    Ok(Some(Listing {
        base_asset: asset(&entry.base_asset)?,
        quote_asset: asset(&entry.quote_asset)?,
        status: entry.status.clone(),
        spot_trading_allowed: entry.is_spot_trading_allowed,
    }))
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::price::binance::test_support::{listing, symbol};

    async fn lookup(response: ResponseTemplate) -> Result<Option<Listing>, LookupError> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/exchangeInfo"))
            .and(query_param("symbol", "ETHUSDT"))
            .respond_with(response)
            .mount(&server)
            .await;
        fetch_listing(&reqwest::Client::new(), &server.uri(), &symbol("ETHUSDT")).await
    }

    fn entry(symbol: &str, base: &str) -> serde_json::Value {
        serde_json::json!({
            "symbol": symbol, "status": "TRADING", "baseAsset": base, "quoteAsset": "USDT",
            "isSpotTradingAllowed": true,
        })
    }

    fn listed(entries: Vec<serde_json::Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({ "symbols": entries }))
    }

    #[tokio::test]
    async fn a_listing_is_parsed_and_an_unknown_symbol_is_none() {
        let found = lookup(listed(vec![entry("ETHUSDT", "ETH")])).await.unwrap();
        assert_eq!(found, Some(listing("ETH", "USDT")));
        let unknown =
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#);
        assert_eq!(lookup(unknown).await.unwrap(), None);
        // The spot flag and a non-trading status come through.
        let mut restricted = entry("ETHUSDT", "ETH");
        restricted["isSpotTradingAllowed"] = serde_json::json!(false);
        restricted["status"] = serde_json::json!("BREAK");
        let found = lookup(listed(vec![restricted])).await.unwrap().unwrap();
        assert!(!found.spot_trading_allowed);
        assert_eq!(found.status, "BREAK");
    }

    /// Only code -1121 means "unknown symbol"; any other 400 is a request
    /// error that must be reported, not a quietly rejected market.
    #[tokio::test]
    async fn other_bad_requests_are_errors() {
        let illegal = ResponseTemplate::new(400).set_body_string(
            r#"{"code":-1100,"msg":"Illegal characters found in parameter 'symbol'."}"#,
        );
        let error = lookup(illegal).await.unwrap_err();
        assert!(
            matches!(error, LookupError::InvalidRequest { code: -1100, .. }),
            "{error}"
        );
        assert!(!error.is_transient());
        assert!(error.to_string().contains("Illegal characters"));
        let html = ResponseTemplate::new(400).set_body_string("<html>".repeat(100));
        let error = lookup(html).await.unwrap_err();
        assert!(matches!(error, LookupError::InvalidRequest { code: 0, .. }));
        // Long server text is cut, so a log line stays a line.
        assert!(error.to_string().len() < 160, "{error}");
    }

    #[tokio::test]
    async fn rate_limits_carry_their_retry_after() {
        for status in [429, 418] {
            let error = lookup(ResponseTemplate::new(status).insert_header("Retry-After", "7"))
                .await
                .unwrap_err();
            assert!(error.is_transient(), "{error}");
            assert!(error.is_rate_limited());
            assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        }
        let unparsable = ResponseTemplate::new(429).insert_header("Retry-After", "soon");
        assert_eq!(lookup(unparsable).await.unwrap_err().retry_after(), None);
        // An absurd wait is capped instead of overflowing a deadline.
        let absurd =
            ResponseTemplate::new(418).insert_header("Retry-After", "18446744073709551615");
        assert_eq!(
            lookup(absurd).await.unwrap_err().retry_after(),
            Some(MAX_RETRY_AFTER)
        );
        // A WAF block asks for a pause too.
        let blocked = lookup(ResponseTemplate::new(403)).await.unwrap_err();
        assert!(blocked.is_rate_limited());
        assert_eq!(blocked.retry_after(), None);
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
            ResponseTemplate::new(200).set_body_string("x".repeat(MAX_BODY_BYTES + 1)),
        ];
        for response in permanent {
            let error = lookup(response).await.unwrap_err();
            assert!(!error.is_transient(), "{error}");
        }
        // A status that is not an upper-case word cannot reach the logs.
        let mut forged = entry("ETHUSDT", "ETH");
        forged["status"] = serde_json::json!("TRADING\nINFO forged line");
        let error = lookup(listed(vec![forged])).await.unwrap_err();
        assert!(matches!(error, LookupError::Malformed(_)), "{error}");
        assert!(!error.to_string().contains('\n'));
    }
}

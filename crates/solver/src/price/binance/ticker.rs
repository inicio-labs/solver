//! Parse and validate one combined-stream `bookTicker` frame.
//!
//! ```json
//! {"stream":"ethusdt@bookTicker","data":{"u":81782039482,"s":"ETHUSDT",
//!  "b":"2718.65000000","B":"16.91070000","a":"2718.66000000","A":"12.68980000"}}
//! ```
//!
//! A frame whose subscribed symbol or update ID cannot be identified is
//! discarded without touching published state. An identifiable frame becomes an
//! [`Observation`]; if its prices fail validation, the observation marks the
//! symbol unusable. Prices are parsed exactly; nothing passes through `f64`.

use std::time::Instant;

use serde::Deserialize;
use serde_json::value::RawValue;

use super::market::Markets;
use super::snapshot::Observation;
use crate::clearing::ReferencePrice;

/// Why an identifiable quote is unusable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum QuoteRejection {
    /// A bid or ask price is missing, malformed, or zero.
    BadPrice,
    /// A displayed quantity is missing, malformed, or zero.
    BadQuantity,
    /// `bid > ask`.
    Crossed,
    /// The spread exceeds the configured maximum.
    TooWide,
    /// Exact arithmetic overflowed.
    Overflow,
}

/// Why a frame was discarded without changing published state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum Discard {
    /// Not a combined-stream frame with a symbol and update ID.
    Malformed,
    /// The symbol is not subscribed.
    UnknownSymbol,
    /// The stream name does not match the payload's symbol.
    StreamMismatch,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: BookTicker<'a>,
}

#[derive(Deserialize)]
struct BookTicker<'a> {
    u: u64,
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow, default)]
    b: Option<&'a RawValue>,
    #[serde(rename = "B", borrow, default)]
    bid_quantity: Option<&'a RawValue>,
    #[serde(borrow, default)]
    a: Option<&'a RawValue>,
    #[serde(rename = "A", borrow, default)]
    ask_quantity: Option<&'a RawValue>,
}

/// A positive decimal sent as a JSON string, parsed exactly. Binance never
/// escapes digits, so a string that needs unescaping is rejected.
fn positive_decimal(raw: Option<&RawValue>) -> Option<ReferencePrice> {
    let text: &str = serde_json::from_str(raw?.get()).ok()?;
    ReferencePrice::from_decimal(text).ok()
}

impl BookTicker<'_> {
    /// The exact midpoint of a valid quote.
    fn midpoint(&self, max_spread_bps: u32) -> Result<ReferencePrice, QuoteRejection> {
        let bid = positive_decimal(self.b).ok_or(QuoteRejection::BadPrice)?;
        let ask = positive_decimal(self.a).ok_or(QuoteRejection::BadPrice)?;
        positive_decimal(self.bid_quantity).ok_or(QuoteRejection::BadQuantity)?;
        positive_decimal(self.ask_quantity).ok_or(QuoteRejection::BadQuantity)?;
        let overflow = |_| QuoteRejection::Overflow;
        if !ReferencePrice::is_ordered(bid, ask).map_err(overflow)? {
            return Err(QuoteRejection::Crossed);
        }
        if !ReferencePrice::spread_within_bps(bid, ask, max_spread_bps).map_err(overflow)? {
            return Err(QuoteRejection::TooWide);
        }
        ReferencePrice::midpoint(bid, ask).map_err(overflow)
    }
}

/// Parse one text frame received at `received_at`. A quote whose spread
/// exceeds `max_spread_bps` (inclusive limit) is unusable.
pub(crate) fn parse_frame(
    text: &str,
    markets: &Markets,
    max_spread_bps: u32,
    received_at: Instant,
) -> Result<Observation, Discard> {
    let Envelope { stream, data } = serde_json::from_str(text).map_err(|_| Discard::Malformed)?;
    let symbol = markets.symbol_index(data.s).ok_or(Discard::UnknownSymbol)?;
    if stream != markets.stream_name(symbol) {
        return Err(Discard::StreamMismatch);
    }
    Ok(Observation {
        symbol,
        update_id: data.u,
        received_at,
        mid: data.midpoint(max_spread_bps),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::price::binance::market::MarketPlan;
    use crate::price::binance::test_support::{asset, eth, listing, price, symbol};

    const MAX_SPREAD_BPS: u32 = 100;

    /// Subscribed to ETHUSDT only (valuation of an ETH faucet).
    fn markets() -> Markets {
        let plan = MarketPlan::new([(eth(), asset("ETH"))], Vec::new(), asset("USDT")).unwrap();
        let (markets, issues) = plan.resolve(&HashMap::from([(
            symbol("ETHUSDT"),
            Some(listing("ETH", "USDT")),
        )]));
        assert!(issues.is_empty());
        markets
    }

    fn frame(data: &str) -> String {
        format!(r#"{{"stream":"ethusdt@bookTicker","data":{data}}}"#)
    }

    fn quote(fields: &str) -> String {
        frame(&format!(r#"{{"u":5,"s":"ETHUSDT",{fields}}}"#))
    }

    fn parse(text: &str) -> Result<Observation, Discard> {
        parse_frame(text, &markets(), MAX_SPREAD_BPS, Instant::now())
    }

    #[test]
    fn valid_quote_yields_exact_midpoint() {
        let text = frame(
            r#"{"u":81782039482,"s":"ETHUSDT","b":"2718.65000000","B":"16.91070000","a":"2718.66000000","A":"12.68980000"}"#,
        );
        let observation = parse(&text).unwrap();
        assert_eq!(observation.symbol, 0);
        assert_eq!(observation.update_id, 81_782_039_482);
        assert_eq!(observation.mid, Ok(price("2718.655")));
    }

    #[test]
    fn identifiable_bad_quotes_are_rejected() {
        let huge = format!("1{}", "0".repeat(60));
        let overflowing = format!(r#""b":"0.000000000000000001","B":"1","a":"{huge}","A":"1""#);
        let cases = [
            (
                r#""b":"0.00000000","B":"1","a":"2","A":"1""#,
                QuoteRejection::BadPrice,
            ),
            (
                r#""b":2718.65,"B":"1","a":"2718.66","A":"1""#,
                QuoteRejection::BadPrice,
            ),
            (
                r#""b":"1e3","B":"1","a":"2","A":"1""#,
                QuoteRejection::BadPrice,
            ),
            (
                r#""b":"\u0031","B":"1","a":"2","A":"1""#,
                QuoteRejection::BadPrice,
            ),
            (r#""B":"1","a":"2","A":"1""#, QuoteRejection::BadPrice),
            (
                r#""b":"1","B":"0.00000000","a":"1","A":"1""#,
                QuoteRejection::BadQuantity,
            ),
            (
                r#""b":"1","B":"1","a":"1","A":"-1""#,
                QuoteRejection::BadQuantity,
            ),
            (
                r#""b":"101","B":"1","a":"100","A":"1""#,
                QuoteRejection::Crossed,
            ),
            // 10_000 × 2 / 100 = 200 bps > 100.
            (
                r#""b":"99","B":"1","a":"101","A":"1""#,
                QuoteRejection::TooWide,
            ),
            (overflowing.as_str(), QuoteRejection::Overflow),
        ];
        for (fields, expected) in cases {
            let observation = parse(&quote(fields)).unwrap();
            assert_eq!(observation.update_id, 5);
            assert_eq!(observation.mid, Err(expected), "{fields}");
        }
        // Just inside the limit: 10_000 × 1 / 100.5 ≈ 99.5 bps.
        assert!(parse(&quote(r#""b":"100","B":"1","a":"101","A":"1""#))
            .unwrap()
            .mid
            .is_ok());
    }

    #[test]
    fn unidentifiable_frames_are_discarded() {
        let cases = [
            ("not json".to_string(), Discard::Malformed),
            (r#"{"result":null,"id":1}"#.to_string(), Discard::Malformed),
            (frame(r#"{"s":"ETHUSDT","b":"1","B":"1","a":"1","A":"1"}"#), Discard::Malformed),
            (frame(r#"{"u":-1,"s":"ETHUSDT"}"#), Discard::Malformed),
            (frame(r#"{"u":"5","s":"ETHUSDT"}"#), Discard::Malformed),
            (frame(r#"{"u":5,"u":6,"s":"ETHUSDT"}"#), Discard::Malformed),
            (frame(r#"{"u":5,"s":"BTCUSDT","b":"1","B":"1","a":"1","A":"1"}"#), Discard::UnknownSymbol),
            (frame(r#"{"u":5,"s":"ethusdt","b":"1","B":"1","a":"1","A":"1"}"#), Discard::UnknownSymbol),
            (
                r#"{"stream":"btcusdt@bookTicker","data":{"u":5,"s":"ETHUSDT","b":"1","B":"1","a":"1","A":"1"}}"#
                    .to_string(),
                Discard::StreamMismatch,
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(parse(&text).unwrap_err(), expected, "{text}");
        }
    }
}

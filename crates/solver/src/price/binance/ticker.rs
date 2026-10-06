//! Parse and validate one combined-stream `bookTicker` frame.
//!
//! ```json
//! {"stream":"ethusdt@bookTicker","data":{"u":81782039482,"s":"ETHUSDT",
//!  "b":"2718.65000000","B":"16.91070000","a":"2718.66000000","A":"12.68980000"}}
//! ```
//!
//! A frame whose subscribed symbol or update ID cannot be identified is
//! discarded without touching published state. An identifiable frame becomes an
//! [`Observation`]; if its quote fails validation, the observation marks the
//! symbol invalid. Prices are parsed exactly; nothing passes through `f64`.
//! Binance's `serverShutdown` event, which precedes a disconnect, is reported
//! so the reader can reconnect before the server closes the socket.

use std::time::Instant;

use serde::Deserialize;
use serde_json::value::RawValue;

use super::market::Markets;
use super::snapshot::Observation;
use crate::clearing::{MidpointError, ReferencePrice};

/// Why an identifiable quote is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum QuoteRejection {
    /// A bid or ask price is missing, malformed, or zero.
    BadPrice,
    /// A displayed quantity is missing, malformed, or zero.
    BadQuantity,
    /// The best bid is above the best ask (`bid > ask`). A consistent order
    /// book never shows this, so the update is garbled or out of date and
    /// its midpoint is not a price.
    Crossed,
    /// The spread is wider than `binance.max_spread_bps`: the spread in
    /// basis points, `10_000 × (ask − bid) / mid`, computed exactly, is above
    /// the limit (equal is accepted). A wide spread means a thin or
    /// disrupted book whose midpoint is not a reliable price.
    TooWide,
    /// A side's displayed notional is below the configured minimum.
    ThinBook,
    /// Exact arithmetic overflowed.
    Overflow,
}

/// Why a frame was discarded without changing published state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum Discard {
    /// Not a combined-stream frame with a symbol and update ID.
    Malformed,
    /// The symbol is not subscribed.
    UnknownSymbol,
    /// The stream name does not match the payload's symbol.
    StreamMismatch,
}

/// What makes a quote usable, from configuration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct QuoteLimits {
    /// Widest accepted spread, inclusive.
    pub(crate) max_spread_bps: u32,
    /// Least displayed notional (`quantity × price`, in the symbol's quote
    /// asset) on each side; `None` accepts any positive quantity.
    pub(crate) min_notional: Option<ReferencePrice>,
}

/// A parsed text frame.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Frame {
    Quote(Observation),
    /// Binance is about to close this connection.
    ServerShutdown,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: BookTicker<'a>,
}

/// The `data` object of a `bookTicker` frame. The `serde` attributes tell the
/// derived `Deserialize` how to read it:
///
/// - `rename = "u"` etc. map Binance's one-letter JSON keys to readable
///   field names (`u` update ID, `s` symbol, `b`/`B` best bid price and
///   quantity, `a`/`A` best ask price and quantity).
/// - `borrow` makes a `&str` or `&RawValue` field point into the received
///   frame text instead of copying it, so parsing a frame allocates nothing.
/// - `default` makes a missing price or quantity `None` instead of a parse
///   error: the frame still identifies its symbol and update ID, so it marks
///   the symbol invalid rather than being discarded.
/// - Prices and quantities stay `RawValue`, the untouched JSON token, so
///   [`positive_decimal`] can require a JSON string and parse its digits
///   exactly; a JSON number would go through `f64`.
#[derive(Deserialize)]
struct BookTicker<'a> {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s", borrow)]
    symbol: &'a str,
    #[serde(rename = "b", borrow, default)]
    bid_price: Option<&'a RawValue>,
    #[serde(rename = "B", borrow, default)]
    bid_quantity: Option<&'a RawValue>,
    #[serde(rename = "a", borrow, default)]
    ask_price: Option<&'a RawValue>,
    #[serde(rename = "A", borrow, default)]
    ask_quantity: Option<&'a RawValue>,
}

/// The fields that identify a `serverShutdown` event, raw or in a combined
/// stream envelope.
#[derive(Deserialize)]
struct EventProbe<'a> {
    #[serde(borrow, default)]
    stream: Option<&'a str>,
    #[serde(borrow, default)]
    e: Option<&'a str>,
    #[serde(borrow, default)]
    data: Option<EventName<'a>>,
}

#[derive(Deserialize)]
struct EventName<'a> {
    #[serde(borrow, default)]
    e: Option<&'a str>,
}

fn is_server_shutdown(text: &str) -> bool {
    const EVENT: &str = "serverShutdown";
    serde_json::from_str::<EventProbe>(text).is_ok_and(|probe| {
        probe.stream == Some("!serverShutdown")
            || probe.e == Some(EVENT)
            || probe.data.is_some_and(|data| data.e == Some(EVENT))
    })
}

/// A positive decimal sent as a JSON string, parsed exactly. Binance never
/// escapes digits, so a string that needs unescaping is rejected.
fn positive_decimal(raw: Option<&RawValue>) -> Option<ReferencePrice> {
    let text: &str = serde_json::from_str(raw?.get()).ok()?;
    ReferencePrice::from_decimal(text).ok()
}

impl BookTicker<'_> {
    /// The exact midpoint of a valid quote.
    fn midpoint(&self, limits: QuoteLimits) -> Result<ReferencePrice, QuoteRejection> {
        let bid = positive_decimal(self.bid_price).ok_or(QuoteRejection::BadPrice)?;
        let ask = positive_decimal(self.ask_price).ok_or(QuoteRejection::BadPrice)?;
        let bid_quantity =
            positive_decimal(self.bid_quantity).ok_or(QuoteRejection::BadQuantity)?;
        let ask_quantity =
            positive_decimal(self.ask_quantity).ok_or(QuoteRejection::BadQuantity)?;
        let mid = ReferencePrice::validated_midpoint(bid, ask, limits.max_spread_bps).map_err(
            |error| match error {
                MidpointError::Crossed => QuoteRejection::Crossed,
                MidpointError::TooWide => QuoteRejection::TooWide,
                MidpointError::Overflow => QuoteRejection::Overflow,
            },
        )?;
        if let Some(minimum) = limits.min_notional {
            let deep = |quantity, price| {
                ReferencePrice::notional_at_least(quantity, price, minimum)
                    .map_err(|_| QuoteRejection::Overflow)
            };
            if !deep(bid_quantity, bid)? || !deep(ask_quantity, ask)? {
                return Err(QuoteRejection::ThinBook);
            }
        }
        Ok(mid)
    }
}

/// Parse one text frame received at `received_at`.
pub(crate) fn parse_frame(
    text: &str,
    markets: &Markets,
    limits: QuoteLimits,
    received_at: Instant,
) -> Result<Frame, Discard> {
    let Ok(Envelope { stream, data }) = serde_json::from_str::<Envelope>(text) else {
        return if is_server_shutdown(text) {
            Ok(Frame::ServerShutdown)
        } else {
            Err(Discard::Malformed)
        };
    };
    let symbol = markets
        .symbol_index(data.symbol)
        .ok_or(Discard::UnknownSymbol)?;
    if stream != markets.stream_name(symbol) {
        return Err(Discard::StreamMismatch);
    }
    Ok(Frame::Quote(Observation {
        symbol,
        update_id: data.update_id,
        received_at,
        mid: data.midpoint(limits),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::price::binance::test_support::{eth_valuation_markets, price};

    const LIMITS: QuoteLimits = QuoteLimits {
        max_spread_bps: 100,
        min_notional: None,
    };

    fn frame(data: &str) -> String {
        format!(r#"{{"stream":"ethusdt@bookTicker","data":{data}}}"#)
    }

    fn quote(fields: &str) -> String {
        frame(&format!(r#"{{"u":5,"s":"ETHUSDT",{fields}}}"#))
    }

    fn parse_with(text: &str, limits: QuoteLimits) -> Result<Observation, Discard> {
        match parse_frame(text, &eth_valuation_markets(), limits, Instant::now())? {
            Frame::Quote(observation) => Ok(observation),
            Frame::ServerShutdown => panic!("not a quote: {text}"),
        }
    }

    fn parse(text: &str) -> Result<Observation, Discard> {
        parse_with(text, LIMITS)
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

    /// With a minimum notional, a one-lot side makes the quote invalid.
    #[test]
    fn thin_sides_are_rejected_under_a_minimum_notional() {
        let limits = QuoteLimits {
            max_spread_bps: 100,
            min_notional: Some(price("5000")),
        };
        // 2 × 2500 = 5000 on each side: exactly the floor is accepted.
        let deep = quote(r#""b":"2500","B":"2","a":"2501","A":"2""#);
        assert_eq!(parse_with(&deep, limits).unwrap().mid, Ok(price("2500.5")));
        let thin_bid = quote(r#""b":"2500","B":"1.999","a":"2501","A":"2""#);
        assert_eq!(
            parse_with(&thin_bid, limits).unwrap().mid,
            Err(QuoteRejection::ThinBook)
        );
        let thin_ask = quote(r#""b":"2500","B":"2","a":"2501","A":"0.001""#);
        assert_eq!(
            parse_with(&thin_ask, limits).unwrap().mid,
            Err(QuoteRejection::ThinBook)
        );
        // Spread and order are checked before depth.
        let crossed = quote(r#""b":"2501","B":"0.001","a":"2500","A":"0.001""#);
        assert_eq!(
            parse_with(&crossed, limits).unwrap().mid,
            Err(QuoteRejection::Crossed)
        );
        // Without a floor the same thin quote is valid.
        assert!(parse(&thin_ask).unwrap().mid.is_ok());
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

    #[test]
    fn server_shutdown_is_recognized_in_either_form() {
        for text in [
            r#"{"e":"serverShutdown","E":1770123456789}"#,
            r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789}}"#,
            r#"{"stream":"ethusdt@bookTicker","data":{"e":"serverShutdown","E":1}}"#,
        ] {
            assert!(
                matches!(
                    parse_frame(text, &eth_valuation_markets(), LIMITS, Instant::now()),
                    Ok(Frame::ServerShutdown)
                ),
                "{text}"
            );
        }
        // Other events stay malformed.
        assert_eq!(
            parse(r#"{"stream":"!other","data":{"e":"other"}}"#).unwrap_err(),
            Discard::Malformed
        );
    }
}

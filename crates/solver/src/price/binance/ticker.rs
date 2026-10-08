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
//! symbol invalid. Prices are read as exact decimals; nothing passes through
//! `f64`.
//! Binance's `serverShutdown` event, which precedes a disconnect, is reported
//! so the reader can reconnect before the server closes the socket.

use std::time::Instant;

use serde::Deserialize;
use serde_json::value::RawValue;

use rust_decimal::Decimal;

use super::market::Markets;
use super::snapshot::Observation;

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
    /// A value is too large or too precise for exact decimal arithmetic
    /// (28 significant digits).
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
    pub(crate) min_notional: Option<Decimal>,
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

/// A positive decimal sent as a JSON string, read exactly: digits with one
/// decimal point at most, no sign, exponent or escape.
fn positive_decimal(raw: Option<&RawValue>) -> Option<Decimal> {
    let text: &str = serde_json::from_str(raw?.get()).ok()?;
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return None;
    }
    let value = Decimal::from_str_exact(text).ok()?;
    (value > Decimal::ZERO).then(|| value.normalize())
}

impl BookTicker<'_> {
    /// The midpoint `(bid + ask) / 2` of a valid quote.
    fn midpoint(&self, limits: QuoteLimits) -> Result<Decimal, QuoteRejection> {
        use QuoteRejection::{BadPrice, BadQuantity, Crossed, Overflow, ThinBook, TooWide};
        let bid = positive_decimal(self.bid_price).ok_or(BadPrice)?;
        let ask = positive_decimal(self.ask_price).ok_or(BadPrice)?;
        let bid_quantity = positive_decimal(self.bid_quantity).ok_or(BadQuantity)?;
        let ask_quantity = positive_decimal(self.ask_quantity).ok_or(BadQuantity)?;
        if bid > ask {
            return Err(Crossed);
        }
        let sum = bid.checked_add(ask).ok_or(Overflow)?;
        let mid = sum.checked_div(Decimal::TWO).ok_or(Overflow)?;
        // Halving adds one decimal place. A `Decimal` holds 28 significant
        // digits, so refuse the rare sum whose half would be rounded.
        if mid.checked_mul(Decimal::TWO) != Some(sum) {
            return Err(Overflow);
        }
        // Spread in basis points is `10_000 × (ask − bid) / mid`; compare it
        // with the limit without dividing.
        let spread = (ask - bid).checked_mul(Decimal::from(10_000u32));
        let limit = Decimal::from(limits.max_spread_bps).checked_mul(mid);
        if spread.ok_or(Overflow)? > limit.ok_or(Overflow)? {
            return Err(TooWide);
        }
        if let Some(minimum) = limits.min_notional {
            let notional =
                |quantity: Decimal, price: Decimal| quantity.checked_mul(price).ok_or(Overflow);
            if notional(bid_quantity, bid)? < minimum || notional(ask_quantity, ask)? < minimum {
                return Err(ThinBook);
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
        // More digits than a `Decimal` holds cannot be read exactly.
        let too_long = format!(r#""b":"1","B":"1","a":"1{}","A":"1""#, "0".repeat(60));
        // The largest `Decimal`: the sum of bid and ask overflows.
        let max = "79228162514264337593543950335";
        let overflowing = format!(r#""b":"{max}","B":"1","a":"{max}","A":"1""#);
        // 28 decimal places: the midpoint would need a 29th.
        let too_precise = r#""b":"0.0000000000000000000000000001","B":"1","a":"0.0000000000000000000000000002","A":"1""#;
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
            (too_long.as_str(), QuoteRejection::BadPrice),
            (overflowing.as_str(), QuoteRejection::Overflow),
            (too_precise, QuoteRejection::Overflow),
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

    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// `ticks` × 10^-8 as Binance writes it, with eight decimal places.
        fn binance_text(ticks: u64) -> String {
            format!("{}.{:08}", ticks / 100_000_000, ticks % 100_000_000)
        }

        fn mid_of(
            bid_ticks: u64,
            ask_ticks: u64,
            max_spread_bps: u32,
        ) -> Result<Decimal, QuoteRejection> {
            let fields = format!(
                r#""b":"{}","B":"1","a":"{}","A":"1""#,
                binance_text(bid_ticks),
                binance_text(ask_ticks)
            );
            let limits = QuoteLimits {
                max_spread_bps,
                min_notional: None,
            };
            parse_with(&quote(&fields), limits).unwrap().mid
        }

        proptest! {
            /// The midpoint lies between bid and ask and is exactly half their sum.
            #[test]
            fn midpoint_is_exact(
                bid_ticks in 1u64..1_000_000_000_000_000,
                spread_ticks in 0u64..1_000_000_000,
            ) {
                let ask_ticks = bid_ticks + spread_ticks;
                let mid = mid_of(bid_ticks, ask_ticks, 10_000).unwrap();
                let (bid, ask) = (Decimal::new(bid_ticks as i64, 8), Decimal::new(ask_ticks as i64, 8));
                prop_assert!(bid <= mid && mid <= ask);
                prop_assert_eq!(mid * Decimal::TWO, bid + ask);
            }

            /// Against integer arithmetic on tick counts: with `ask = bid + k`
            /// ticks, the exact spread is `20_000·k / (2·bid + k)` bps, and a
            /// spread equal to the limit is accepted.
            #[test]
            fn spread_limit_matches_an_integer_oracle(
                bid_ticks in 1u64..1_000_000_000_000,
                spread_ticks in 0u64..1_000_000_000,
            ) {
                let ask_ticks = bid_ticks + spread_ticks;
                let numerator = 20_000 * u128::from(spread_ticks);
                let denominator = 2 * u128::from(bid_ticks) + u128::from(spread_ticks);
                let floor = u32::try_from(numerator / denominator).unwrap();
                let exact = numerator % denominator == 0;
                prop_assert_eq!(mid_of(bid_ticks, ask_ticks, floor).is_ok(), exact);
                prop_assert!(mid_of(bid_ticks, ask_ticks, floor + 1).is_ok());
                if floor > 0 {
                    prop_assert_eq!(
                        mid_of(bid_ticks, ask_ticks, floor - 1),
                        Err(QuoteRejection::TooWide)
                    );
                }
            }
        }
    }
}

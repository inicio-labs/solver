//! Websocket RFQ wire protocol (JSON, `type`-tagged) — the shared contract
//! between the solver's router and a filler. Both message enums derive
//! `Serialize + Deserialize` so each side can encode what it sends and decode
//! what it receives. This module is dependency-free apart from serde.

use serde::{Deserialize, Serialize};

/// A trading pair as hex account ids, in the note's `(offered, requested)`
/// orientation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PairSpec {
    pub offered: String,
    pub requested: String,
}

/// Messages a DEX (client) sends to the router.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Pairs the DEX can fill (for `Ask` targeting; quotes still gate per pair).
    Subscribe { pairs: Vec<PairSpec> },
    /// A standing quote for one pair. Resend before expiry to refresh.
    Quote {
        pair: PairSpec,
        /// Price = requested-token per offered-token, per WHOLE token, as a
        /// decimal string (e.g. "2.05"). Parsed to an exact rational — never a
        /// float on the wire.
        price: String,
        /// Max requested-token quantity (base units) the DEX will take.
        quantity: u64,
        /// Optional shorter validity (ms); capped at the server's quote TTL.
        #[serde(default)]
        valid_for_ms: Option<u64>,
    },
}

/// Messages the router sends to a DEX.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    AuthOk,
    Ask {
        pairs: Vec<PairSpec>,
    },
    Handover {
        note_id: String,
        fill_amount: u64,
        /// Hex-encoded serialized PSWAP note for the DEX to consume on-chain.
        note_hex: String,
        /// The price the solver requires this note be filled at — the DEX's own
        /// quoted price echoed back (requested-per-offered, per whole token), as
        /// a decimal string. "Fill this note at `fill_price`," independent of the
        /// note's intrinsic on-chain rate.
        fill_price: String,
    },
    Error {
        code: String,
        msg: String,
    },
}

/// Parse a non-negative decimal price string (e.g. "2.05", "100", "0.999") into
/// an exact rational `(num, den)` = value, where `value = num / den`. Returns
/// `None` on malformed input; both parts are always > 0 on success.
pub fn parse_decimal_price(s: &str) -> Option<(u128, u128)> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
        || frac_part.len() > 30
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let num: u128 = digits.parse().ok()?;
    let den: u128 = 10u128.checked_pow(frac_part.len() as u32)?;
    if num == 0 || den == 0 {
        return None;
    }
    Some((num, den))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_price_parsing() {
        assert_eq!(parse_decimal_price("2"), Some((2, 1)));
        assert_eq!(parse_decimal_price("2.05"), Some((205, 100)));
        assert_eq!(parse_decimal_price("0.999"), Some((999, 1000)));
        assert_eq!(parse_decimal_price("100.0"), Some((1000, 10)));
        assert_eq!(parse_decimal_price(""), None);
        assert_eq!(parse_decimal_price("-1"), None);
        assert_eq!(parse_decimal_price("0"), None);
        assert_eq!(parse_decimal_price("1.2.3"), None);
        assert_eq!(parse_decimal_price("abc"), None);
    }

    #[test]
    fn client_and_server_msgs_round_trip() {
        // ClientMsg serializes (client side) and deserializes (server side).
        let q = ClientMsg::Quote {
            pair: PairSpec { offered: "0xaa".into(), requested: "0xbb".into() },
            price: "2.5".into(),
            quantity: 1000,
            valid_for_ms: Some(5000),
        };
        let j = serde_json::to_string(&q).unwrap();
        assert!(j.contains("\"type\":\"quote\""));
        let _back: ClientMsg = serde_json::from_str(&j).unwrap();

        // ServerMsg serializes (server side) and deserializes (client side).
        let h = ServerMsg::Handover {
            note_id: "0x1".into(),
            fill_amount: 7,
            note_hex: "ab".into(),
            fill_price: "2.05".into(),
        };
        let j = serde_json::to_string(&h).unwrap();
        assert!(j.contains("\"type\":\"handover\""));
        assert!(j.contains("\"fill_price\":\"2.05\""));
        let back: ServerMsg = serde_json::from_str(&j).unwrap();
        assert!(matches!(back, ServerMsg::Handover { fill_amount: 7, .. }));
    }

    use proptest::prelude::*;

    proptest! {
        /// Arbitrary input (any unicode string) must never panic the parser.
        #[test]
        fn prop_parse_decimal_price_never_panics(s in ".*") {
            let _ = parse_decimal_price(&s);
        }

        /// Any well-formed non-zero decimal parses to a positive rational.
        #[test]
        fn prop_well_formed_decimals_parse(int in 0u64..=1_000_000, frac_digits in 0usize..=6) {
            let frac = "0".repeat(frac_digits);
            let s = if frac_digits == 0 { int.to_string() } else { format!("{int}.{frac}9") };
            let parsed = parse_decimal_price(&s);
            prop_assert!(parsed.is_some(), "well-formed decimal {s} should parse");
            let (num, den) = parsed.unwrap();
            prop_assert!(num > 0 && den > 0);
        }
    }
}

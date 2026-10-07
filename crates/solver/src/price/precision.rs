//! How the price API and swap guidance write a price: `"full"` or a fixed
//! number of decimal places, chosen in `engine.price_precision` or per request.

use rust_decimal::{Decimal, RoundingStrategy};

/// Resolved price precision (decimal places of the price NUMBER): `Full` or a
/// fixed `0..=18`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PricePrecision {
    Full,
    Fixed(u8),
}

impl PricePrecision {
    /// Parse `"full"` (case-insensitive) or an integer `0..=18`.
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("full") {
            return Some(Self::Full);
        }
        s.parse::<u8>().ok().filter(|n| *n <= 18).map(Self::Fixed)
    }

    /// `price` as a decimal string, rounded half up: `Full` keeps up to 18
    /// places without trailing zeros, `Fixed(n)` exactly `n` places.
    pub fn format(self, price: Decimal) -> String {
        let places = match self {
            Self::Full => 18,
            Self::Fixed(places) => u32::from(places),
        };
        let mut rounded =
            price.round_dp_with_strategy(places, RoundingStrategy::MidpointAwayFromZero);
        match self {
            Self::Full => rounded = rounded.normalize(),
            Self::Fixed(_) => rounded.rescale(places),
        }
        rounded.to_string()
    }
}

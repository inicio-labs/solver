use miden_protocol::asset::AssetAmount;
use ruint::aliases::U256;

use super::config::PPM_DENOMINATOR;
use super::types::{BatchPrice, ClearingError, ReferencePrice};

pub(crate) trait WideOperand {
    fn wide(self) -> U256;
}

impl WideOperand for U256 {
    fn wide(self) -> U256 {
        self
    }
}

impl WideOperand for AssetAmount {
    fn wide(self) -> U256 {
        U256::from(self.as_u64())
    }
}

impl WideOperand for u64 {
    fn wide(self) -> U256 {
        U256::from(self)
    }
}

impl WideOperand for u32 {
    fn wide(self) -> U256 {
        U256::from(self)
    }
}

fn power_of_ten(decimals: u8) -> Result<U256, ClearingError> {
    U256::from(10u8)
        .checked_pow(U256::from(decimals))
        .ok_or(ClearingError::ArithmeticOverflow)
}

pub(crate) fn checked_mul<L: WideOperand, R: WideOperand>(
    left: L,
    right: R,
) -> Result<U256, ClearingError> {
    left.wide()
        .checked_mul(right.wide())
        .ok_or(ClearingError::ArithmeticOverflow)
}

pub(crate) fn mul_div_floor<A: WideOperand, M: WideOperand, D: WideOperand>(
    amount: A,
    multiplier: M,
    divisor: D,
) -> Result<U256, ClearingError> {
    let divisor = divisor.wide();
    if divisor == U256::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    Ok(checked_mul(amount, multiplier)? / divisor)
}

pub(crate) fn mul_div_ceil<A: WideOperand, M: WideOperand, D: WideOperand>(
    amount: A,
    multiplier: M,
    divisor: D,
) -> Result<U256, ClearingError> {
    let divisor = divisor.wide();
    if divisor == U256::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    Ok(checked_mul(amount, multiplier)?.div_ceil(divisor))
}

pub(crate) fn to_asset_amount(value: U256) -> Result<AssetAmount, ClearingError> {
    let value = u64::try_from(value).map_err(|_| ClearingError::ArithmeticOverflow)?;
    Ok(AssetAmount::new(value)?)
}

pub(crate) fn ppm_floor(gross: U256, rate_ppm: u32) -> Result<U256, ClearingError> {
    mul_div_floor(gross, rate_ppm, PPM_DENOMINATOR)
}

impl ReferencePrice {
    pub fn from_ratio(numerator: u64, denominator: u64) -> Result<Self, ClearingError> {
        Self::new(U256::from(numerator), U256::from(denominator))
    }

    fn new(numerator: U256, denominator: U256) -> Result<Self, ClearingError> {
        if numerator == U256::ZERO || denominator == U256::ZERO {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let common = numerator.gcd(denominator);
        Ok(Self {
            numerator: numerator / common,
            denominator: denominator / common,
        })
    }

    /// Parse a provider's decimal string exactly, e.g. Binance's
    /// `"123.45000000"`. Exponents, signs, and zero are rejected.
    pub fn from_decimal(raw: &str) -> Result<Self, ClearingError> {
        let (whole, fraction) = match raw.split_once('.') {
            Some((whole, fraction)) if !fraction.is_empty() => (whole, fraction),
            Some(_) => return Err(ClearingError::InvalidOraclePrice),
            None => (raw, ""),
        };
        if whole.is_empty()
            || !whole.bytes().all(|character| character.is_ascii_digit())
            || !fraction.bytes().all(|character| character.is_ascii_digit())
        {
            return Err(ClearingError::InvalidOraclePrice);
        }
        fn invalid<E>(_: E) -> ClearingError {
            ClearingError::InvalidOraclePrice
        }
        let decimals = u8::try_from(fraction.len()).map_err(invalid)?;
        let denominator = power_of_ten(decimals).map_err(invalid)?;
        let parse = |digits: &str| match digits {
            "" => Ok(U256::ZERO),
            digits => U256::from_str_radix(digits, 10).map_err(invalid),
        };
        let numerator = checked_mul(parse(whole)?, denominator)
            .map_err(invalid)?
            .checked_add(parse(fraction)?)
            .ok_or(ClearingError::InvalidOraclePrice)?;
        Self::new(numerator, denominator)
    }

    /// Parse a JSON number without a floating-point round trip. Providers may
    /// emit small prices in scientific notation even when `precision=full`.
    pub fn from_json_number(raw: &str) -> Result<Self, ClearingError> {
        let exponent_at = raw.bytes().position(|byte| byte == b'e' || byte == b'E');
        let Some(index) = exponent_at else {
            return Self::from_decimal(raw);
        };
        let price = Self::from_decimal(&raw[..index])?;
        let exponent = &raw[index + 1..];
        let (negative, digits) = if let Some(digits) = exponent.strip_prefix('-') {
            (true, digits)
        } else {
            (false, exponent.strip_prefix('+').unwrap_or(exponent))
        };
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let magnitude = digits
            .parse::<u8>()
            .map_err(|_| ClearingError::InvalidOraclePrice)?;
        let factor = power_of_ten(magnitude).map_err(|_| ClearingError::InvalidOraclePrice)?;
        if negative {
            Self::new(price.numerator, checked_mul(price.denominator, factor)?)
        } else {
            Self::new(checked_mul(price.numerator, factor)?, price.denominator)
        }
        .map_err(|_| ClearingError::InvalidOraclePrice)
    }

    pub(crate) const ONE: Self = Self {
        numerator: U256::ONE,
        denominator: U256::ONE,
    };

    /// Exact `(bid + ask) / 2`.
    pub fn midpoint(bid: Self, ask: Self) -> Result<Self, ClearingError> {
        let (bid_scaled, ask_scaled) = Self::common_numerators(bid, ask)?;
        let sum = bid_scaled
            .checked_add(ask_scaled)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        let denominator = checked_mul(checked_mul(bid.denominator, ask.denominator)?, 2u32)?;
        Self::new(sum, denominator)
    }

    /// The same market quoted the other way round, `1 / self`.
    #[must_use]
    pub fn reciprocal(self) -> Self {
        Self {
            numerator: self.denominator,
            denominator: self.numerator,
        }
    }

    /// Whether `bid <= ask`, compared exactly.
    pub fn is_ordered(bid: Self, ask: Self) -> Result<bool, ClearingError> {
        let (bid_scaled, ask_scaled) = Self::common_numerators(bid, ask)?;
        Ok(bid_scaled <= ask_scaled)
    }

    /// Whether `10_000 × (ask - bid) / ((bid + ask) / 2) <= max_bps`, without
    /// rounding the spread. A crossed quote (`bid > ask`) is never within.
    pub fn spread_within_bps(bid: Self, ask: Self, max_bps: u32) -> Result<bool, ClearingError> {
        let (bid_scaled, ask_scaled) = Self::common_numerators(bid, ask)?;
        let Some(spread) = ask_scaled.checked_sub(bid_scaled) else {
            return Ok(false);
        };
        let sum = bid_scaled
            .checked_add(ask_scaled)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        // Both sides share the denominator bid.denominator × ask.denominator.
        Ok(checked_mul(spread, 20_000u32)? <= checked_mul(sum, max_bps)?)
    }

    /// Both prices' numerators over their common denominator.
    fn common_numerators(left: Self, right: Self) -> Result<(U256, U256), ClearingError> {
        Ok((
            checked_mul(left.numerator, right.denominator)?,
            checked_mul(right.numerator, left.denominator)?,
        ))
    }

    /// Decimal string with exactly `places` fractional digits, rounded half up.
    pub fn to_fixed_decimal(self, places: u8) -> Result<String, ClearingError> {
        let (whole, fraction) = self.round_half_up(places)?;
        Ok(match fraction {
            Some(fraction) => format!("{whole}.{fraction}"),
            None => whole.to_string(),
        })
    }

    /// Like [`Self::to_fixed_decimal`] without trailing fractional zeros, so a
    /// price with at most `places` decimals prints exactly.
    pub fn to_trimmed_decimal(self, places: u8) -> Result<String, ClearingError> {
        let (whole, fraction) = self.round_half_up(places)?;
        Ok(
            match fraction
                .as_deref()
                .map(|digits| digits.trim_end_matches('0'))
            {
                Some(digits) if !digits.is_empty() => format!("{whole}.{digits}"),
                _ => whole.to_string(),
            },
        )
    }

    /// Whole part and zero-padded fractional digits (`None` for no places).
    fn round_half_up(self, places: u8) -> Result<(U256, Option<String>), ClearingError> {
        let scale = power_of_ten(places)?;
        let scaled = checked_mul(self.numerator, scale)?;
        let mut units = scaled / self.denominator;
        if checked_mul(scaled % self.denominator, 2u32)? >= self.denominator {
            units += U256::ONE;
        }
        let fraction = (places > 0)
            .then(|| format!("{:0>width$}", units % scale, width = usize::from(places)));
        Ok((units / scale, fraction))
    }
}

impl BatchPrice {
    pub fn from_ratio(quote_units: u64, base_units: u64) -> Result<Self, ClearingError> {
        Self::new(U256::from(quote_units), U256::from(base_units))
    }

    pub(crate) fn new(quote_units: U256, base_units: U256) -> Result<Self, ClearingError> {
        if quote_units == U256::ZERO || base_units == U256::ZERO {
            return Err(ClearingError::InvalidPrice);
        }
        let common = quote_units.gcd(base_units);
        Ok(Self {
            quote_units: quote_units / common,
            base_units: base_units / common,
        })
    }

    /// Both reference prices refer to one whole token. Convert them to quote
    /// base units per base base unit with the on-chain token decimals.
    pub fn from_reference_prices(
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<Self, ClearingError> {
        let quote_units = checked_mul(
            checked_mul(base_price.numerator, quote_price.denominator)?,
            power_of_ten(quote_decimals)?,
        )?;
        let base_units = checked_mul(
            checked_mul(base_price.denominator, quote_price.numerator)?,
            power_of_ten(base_decimals)?,
        )?;
        Self::new(quote_units, base_units)
    }

    /// `price` is whole quote tokens per whole base token. Convert it to quote
    /// base units per base base unit with the on-chain token decimals:
    /// `price × 10^quote_decimals / 10^base_decimals`.
    pub fn from_pair_price(
        price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<Self, ClearingError> {
        Self::new(
            checked_mul(price.numerator, power_of_ten(quote_decimals)?)?,
            checked_mul(price.denominator, power_of_ten(base_decimals)?)?,
        )
    }

    pub(crate) fn quote_for_base_floor(self, base: U256) -> Result<U256, ClearingError> {
        mul_div_floor(base, self.quote_units, self.base_units)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_price_uses_token_decimals() {
        let btc = ReferencePrice::from_decimal("100000.00000000").unwrap();
        let usdt = ReferencePrice::from_decimal("1").unwrap();
        let price = BatchPrice::from_reference_prices(btc, usdt, 8, 6).unwrap();
        assert_eq!(price.quote_units, U256::from(1_000u64));
        assert_eq!(price.base_units, U256::ONE);
    }

    #[test]
    fn decimal_input_is_exact() {
        let price = ReferencePrice::from_decimal("0.00012500").unwrap();
        assert_eq!(price.numerator, U256::ONE);
        assert_eq!(price.denominator, U256::from(8_000u64));
        for invalid in ["0", "-1", "1e4", "1.", ".1", "1.2.3"] {
            assert!(ReferencePrice::from_decimal(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn json_scientific_price_is_exact() {
        let price = ReferencePrice::from_json_number("1.2500e-4").unwrap();
        assert_eq!(price.numerator, U256::ONE);
        assert_eq!(price.denominator, U256::from(8_000u64));
        let price = ReferencePrice::from_json_number("1.25E+4").unwrap();
        assert_eq!(price.numerator, U256::from(12_500u64));
        assert_eq!(price.denominator, U256::ONE);
        for invalid in ["1e", "1e-", "1e999", "1e2e3", "-1e2"] {
            assert!(
                ReferencePrice::from_json_number(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    fn decimal(raw: &str) -> ReferencePrice {
        ReferencePrice::from_decimal(raw).unwrap()
    }

    #[test]
    fn midpoint_is_exact() {
        let mid = ReferencePrice::midpoint(decimal("2718.65"), decimal("2718.66")).unwrap();
        assert_eq!(mid, decimal("2718.655"));
        let mid = ReferencePrice::midpoint(decimal("0.00000001"), decimal("0.00000002")).unwrap();
        assert_eq!(mid, decimal("0.000000015"));
        assert_eq!(
            ReferencePrice::midpoint(decimal("7"), decimal("7")).unwrap(),
            decimal("7")
        );
    }

    #[test]
    fn spread_limit_is_inclusive_and_unrounded() {
        // mid 100, spread 2: exactly 200 bps.
        let (bid, ask) = (decimal("99"), decimal("101"));
        assert!(ReferencePrice::spread_within_bps(bid, ask, 200).unwrap());
        assert!(!ReferencePrice::spread_within_bps(bid, ask, 199).unwrap());
        // 200.0000001 bps must not round down to 200.
        let ask = decimal("101.00000001");
        assert!(!ReferencePrice::spread_within_bps(bid, ask, 200).unwrap());
        assert!(ReferencePrice::spread_within_bps(bid, bid, 0).unwrap());
        assert!(!ReferencePrice::spread_within_bps(ask, bid, u32::MAX).unwrap());
        assert!(ReferencePrice::is_ordered(bid, bid).unwrap());
        assert!(!ReferencePrice::is_ordered(ask, bid).unwrap());
    }

    #[test]
    fn overflow_is_an_error_not_a_panic() {
        let huge = decimal(&format!("1{}", "0".repeat(60)));
        let tiny = decimal("0.000000000000000001");
        fn overflow<T>(result: Result<T, ClearingError>) -> bool {
            matches!(result, Err(ClearingError::ArithmeticOverflow))
        }
        assert!(overflow(ReferencePrice::midpoint(tiny, huge)));
        assert!(overflow(ReferencePrice::is_ordered(tiny, huge)));
        assert!(overflow(ReferencePrice::spread_within_bps(tiny, huge, 1)));
        assert!(overflow(BatchPrice::from_pair_price(huge, 0, 78)));
        assert!(overflow(huge.to_fixed_decimal(18)));
    }

    #[test]
    fn pair_price_conversion_uses_unequal_decimals() {
        // 1 whole base token (18 decimals) = 2718.655 quote tokens (6 decimals):
        // 2_718_655 / 10^15, reduced by their common factor 5.
        let price = BatchPrice::from_pair_price(decimal("2718.655"), 18, 6).unwrap();
        assert_eq!(price.quote_units, U256::from(543_731u64));
        assert_eq!(price.base_units, U256::from(200_000_000_000_000u64));
    }

    #[test]
    fn decimal_formatting_rounds_half_up() {
        let price = decimal("2718.655");
        assert_eq!(price.to_trimmed_decimal(18).unwrap(), "2718.655");
        assert_eq!(price.to_fixed_decimal(2).unwrap(), "2718.66");
        assert_eq!(price.to_fixed_decimal(0).unwrap(), "2719");
        assert_eq!(price.to_fixed_decimal(4).unwrap(), "2718.6550");
        assert_eq!(decimal("10").to_trimmed_decimal(18).unwrap(), "10");
        assert_eq!(decimal("10").to_fixed_decimal(2).unwrap(), "10.00");
        let third = ReferencePrice::from_ratio(1, 3).unwrap();
        assert_eq!(
            third.to_trimmed_decimal(18).unwrap(),
            "0.333333333333333333"
        );
        let two_thirds = ReferencePrice::from_ratio(2, 3).unwrap();
        assert_eq!(two_thirds.to_fixed_decimal(2).unwrap(), "0.67");
        assert_eq!(decimal("0.004").to_trimmed_decimal(2).unwrap(), "0");
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// A positive decimal with up to eight fractional digits, as Binance sends.
        fn price() -> impl Strategy<Value = ReferencePrice> {
            (1u64..1_000_000_000_000_000, 0u32..=8).prop_map(|(units, digits)| {
                ReferencePrice::from_ratio(units, 10u64.pow(digits)).unwrap()
            })
        }

        proptest! {
            #[test]
            fn midpoint_lies_between_and_is_exact(first in price(), second in price()) {
                let (bid, ask) = if ReferencePrice::is_ordered(first, second).unwrap() {
                    (first, second)
                } else {
                    (second, first)
                };
                let mid = ReferencePrice::midpoint(bid, ask).unwrap();
                prop_assert!(ReferencePrice::is_ordered(bid, mid).unwrap());
                prop_assert!(ReferencePrice::is_ordered(mid, ask).unwrap());
                // 2 × mid = bid + ask, cross-multiplied.
                let sum_numerator = bid.numerator * ask.denominator + ask.numerator * bid.denominator;
                let sum_denominator = bid.denominator * ask.denominator;
                prop_assert_eq!(
                    U256::from(2u8) * mid.numerator * sum_denominator,
                    sum_numerator * mid.denominator
                );
            }

            /// The base-unit price is the whole-token price scaled by the
            /// decimals, in lowest terms.
            #[test]
            fn pair_price_conversion_preserves_the_ratio(
                price in price(),
                base_decimals in 0u8..=18,
                quote_decimals in 0u8..=18,
            ) {
                let batch = BatchPrice::from_pair_price(price, base_decimals, quote_decimals).unwrap();
                let ten = |power: u8| U256::from(10u8).pow(U256::from(power));
                prop_assert_eq!(
                    batch.quote_units * price.denominator * ten(base_decimals),
                    batch.base_units * price.numerator * ten(quote_decimals)
                );
                prop_assert_eq!(batch.quote_units.gcd(batch.base_units), U256::ONE);
            }

            /// Against integer arithmetic on tick counts: `ask = bid + k` ticks
            /// of 10^-8, so the exact spread is `20_000·k / (2·bid + k)` bps.
            #[test]
            fn spread_limit_matches_an_integer_oracle(
                bid_ticks in 1u64..1_000_000_000_000,
                spread_ticks in 0u64..1_000_000_000,
            ) {
                let ticks = |count: u64| ReferencePrice::from_ratio(count, 100_000_000).unwrap();
                let (bid, ask) = (ticks(bid_ticks), ticks(bid_ticks + spread_ticks));
                let numerator = 20_000 * u128::from(spread_ticks);
                let denominator = 2 * u128::from(bid_ticks) + u128::from(spread_ticks);
                let floor = u32::try_from(numerator / denominator).unwrap();
                let exact = numerator % denominator == 0;
                prop_assert_eq!(ReferencePrice::spread_within_bps(bid, ask, floor).unwrap(), exact);
                prop_assert!(ReferencePrice::spread_within_bps(bid, ask, floor + 1).unwrap());
                if floor > 0 {
                    prop_assert!(!ReferencePrice::spread_within_bps(bid, ask, floor - 1).unwrap());
                }
            }

            #[test]
            fn trimmed_decimal_round_trips_terminating_prices(price in price()) {
                let text = price.to_trimmed_decimal(18).unwrap();
                prop_assert_eq!(ReferencePrice::from_decimal(&text).unwrap(), price);
            }
        }
    }
}

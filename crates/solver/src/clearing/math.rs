use miden_protocol::asset::AssetAmount;
use ruint::aliases::U256;

use super::types::{ClearingError, ExactPrice, ReferencePrice, PPM_DENOMINATOR};

pub(crate) fn greatest_common_divisor(mut left: U256, mut right: U256) -> U256 {
    while right != U256::ZERO {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn power_of_ten(decimals: u8) -> Result<U256, ClearingError> {
    let mut value = U256::ONE;
    for _ in 0..decimals {
        value = checked_mul(value, U256::from(10u8))?;
    }
    Ok(value)
}

pub(crate) fn checked_mul(left: U256, right: U256) -> Result<U256, ClearingError> {
    left.checked_mul(right)
        .ok_or(ClearingError::ArithmeticOverflow)
}

pub(crate) fn mul_div_floor(
    amount: U256,
    multiplier: U256,
    divisor: U256,
) -> Result<U256, ClearingError> {
    if divisor == U256::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    Ok(checked_mul(amount, multiplier)? / divisor)
}

pub(crate) fn mul_div_ceil(
    amount: U256,
    multiplier: U256,
    divisor: U256,
) -> Result<U256, ClearingError> {
    if divisor == U256::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    let product = checked_mul(amount, multiplier)?;
    let quotient = product / divisor;
    if product % divisor == U256::ZERO {
        Ok(quotient)
    } else {
        quotient
            .checked_add(U256::ONE)
            .ok_or(ClearingError::ArithmeticOverflow)
    }
}

pub(crate) fn to_asset_amount(value: U256) -> Result<AssetAmount, ClearingError> {
    let value = u64::try_from(value).map_err(|_| ClearingError::ArithmeticOverflow)?;
    Ok(AssetAmount::new(value)?)
}

pub(crate) fn ppm_floor(gross: U256, rate_ppm: u32) -> Result<U256, ClearingError> {
    mul_div_floor(gross, U256::from(rate_ppm), U256::from(PPM_DENOMINATOR))
}

impl ReferencePrice {
    pub fn from_ratio(numerator: u64, denominator: u64) -> Result<Self, ClearingError> {
        Self::new(U256::from(numerator), U256::from(denominator))
    }

    fn new(numerator: U256, denominator: U256) -> Result<Self, ClearingError> {
        if numerator == U256::ZERO || denominator == U256::ZERO {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let common = greatest_common_divisor(numerator, denominator);
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
        let decimals =
            u8::try_from(fraction.len()).map_err(|_| ClearingError::InvalidOraclePrice)?;
        let denominator = power_of_ten(decimals).map_err(|_| ClearingError::InvalidOraclePrice)?;
        let mut numerator = U256::ZERO;
        for digit in whole.bytes().chain(fraction.bytes()) {
            numerator = numerator
                .checked_mul(U256::from(10u8))
                .and_then(|value| value.checked_add(U256::from(digit - b'0')))
                .ok_or(ClearingError::InvalidOraclePrice)?;
        }
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
}

impl ExactPrice {
    pub fn from_ratio(quote_units: u64, base_units: u64) -> Result<Self, ClearingError> {
        Self::new(U256::from(quote_units), U256::from(base_units))
    }

    pub(crate) fn new(quote_units: U256, base_units: U256) -> Result<Self, ClearingError> {
        if quote_units == U256::ZERO || base_units == U256::ZERO {
            return Err(ClearingError::InvalidPrice);
        }
        let common = greatest_common_divisor(quote_units, base_units);
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

    pub(crate) fn quote_for_base_floor(self, base: U256) -> Result<U256, ClearingError> {
        mul_div_floor(base, self.quote_units, self.base_units)
    }

    pub(crate) fn base_for_quote_ceil(self, quote: U256) -> Result<U256, ClearingError> {
        mul_div_ceil(quote, self.base_units, self.quote_units)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_price_uses_token_decimals() {
        let btc = ReferencePrice::from_decimal("100000.00000000").unwrap();
        let usdt = ReferencePrice::from_decimal("1").unwrap();
        let price = ExactPrice::from_reference_prices(btc, usdt, 8, 6).unwrap();
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
}

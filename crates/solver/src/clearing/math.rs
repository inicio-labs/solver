use miden_protocol::asset::AssetAmount;

use super::types::{ClearingError, ExactPrice, Ratio, ReferencePrice, Wide, PPM_DENOMINATOR};

fn gcd(mut a: Wide, mut b: Wide) -> Wide {
    while b != Wide::ZERO {
        let next = a % b;
        a = b;
        b = next;
    }
    a
}

fn power_of_ten(decimals: u8) -> Result<Wide, ClearingError> {
    let mut value = Wide::ONE;
    for _ in 0..decimals {
        value = value
            .checked_mul(Wide::from(10u8))
            .ok_or(ClearingError::ArithmeticOverflow)?;
    }
    Ok(value)
}

pub(crate) fn mul_div_floor(a: Wide, b: Wide, divisor: Wide) -> Result<Wide, ClearingError> {
    if divisor == Wide::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    Ok(a.checked_mul(b).ok_or(ClearingError::ArithmeticOverflow)? / divisor)
}

pub(crate) fn mul_div_ceil(a: Wide, b: Wide, divisor: Wide) -> Result<Wide, ClearingError> {
    if divisor == Wide::ZERO {
        return Err(ClearingError::ArithmeticOverflow);
    }
    let product = a.checked_mul(b).ok_or(ClearingError::ArithmeticOverflow)?;
    let quotient = product / divisor;
    if product % divisor == Wide::ZERO {
        Ok(quotient)
    } else {
        quotient
            .checked_add(Wide::ONE)
            .ok_or(ClearingError::ArithmeticOverflow)
    }
}

pub(crate) fn asset_amount_from_wide(value: Wide) -> Result<AssetAmount, ClearingError> {
    let value = u64::try_from(value).map_err(|_| ClearingError::ArithmeticOverflow)?;
    Ok(AssetAmount::new(value)?)
}

pub(crate) fn ppm_floor(gross: Wide, rate_ppm: u32) -> Result<Wide, ClearingError> {
    mul_div_floor(gross, Wide::from(rate_ppm), Wide::from(PPM_DENOMINATOR))
}

impl Ratio {
    pub(crate) fn new(num: Wide, den: Wide) -> Result<Self, ClearingError> {
        if den == Wide::ZERO || num < den {
            return Err(ClearingError::InternalInvariant("scale ratio below one"));
        }
        let common = gcd(num, den);
        Ok(Self {
            num: num / common,
            den: den / common,
        })
    }

    /// Round down only when expanding a requested fill into the common
    /// allocation coordinate.
    pub(crate) fn scale_up_floor(self, amount: AssetAmount) -> Result<Wide, ClearingError> {
        mul_div_floor(Wide::from(amount.as_u64()), self.num, self.den)
    }

    /// Round up only when converting a non-endpoint allocation back into an
    /// actual note payment.
    pub(crate) fn scale_down_ceil(self, scaled: Wide) -> Result<AssetAmount, ClearingError> {
        asset_amount_from_wide(mul_div_ceil(scaled, self.den, self.num)?)
    }
}

impl ReferencePrice {
    /// Parse a provider's decimal string exactly, e.g. Binance's
    /// `"123.45000000"`. Exponents, signs, and zero are rejected.
    pub fn from_decimal(raw: &str) -> Result<Self, ClearingError> {
        let (whole, fraction) = match raw.split_once('.') {
            Some((whole, fraction)) if !fraction.is_empty() => (whole, fraction),
            Some(_) => return Err(ClearingError::InvalidOraclePrice),
            None => (raw, ""),
        };
        if whole.is_empty()
            || !whole.bytes().all(|c| c.is_ascii_digit())
            || !fraction.bytes().all(|c| c.is_ascii_digit())
        {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let decimals =
            u8::try_from(fraction.len()).map_err(|_| ClearingError::InvalidOraclePrice)?;
        let denominator = power_of_ten(decimals).map_err(|_| ClearingError::InvalidOraclePrice)?;
        let mut numerator = Wide::ZERO;
        for digit in whole.bytes().chain(fraction.bytes()) {
            numerator = numerator
                .checked_mul(Wide::from(10u8))
                .and_then(|value| value.checked_add(Wide::from(digit - b'0')))
                .ok_or(ClearingError::InvalidOraclePrice)?;
        }
        if numerator == Wide::ZERO {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let common = gcd(numerator, denominator);
        Ok(Self {
            numerator: numerator / common,
            denominator: denominator / common,
        })
    }

    /// Parse a JSON number without a floating-point round trip. Providers may
    /// emit small prices in scientific notation even when `precision=full`.
    pub fn from_json_number(raw: &str) -> Result<Self, ClearingError> {
        let exponent_at = raw.bytes().position(|byte| byte == b'e' || byte == b'E');
        let Some(index) = exponent_at else {
            return Self::from_decimal(raw);
        };
        let mut price = Self::from_decimal(&raw[..index])?;
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
            price.denominator = price
                .denominator
                .checked_mul(factor)
                .ok_or(ClearingError::InvalidOraclePrice)?;
        } else {
            price.numerator = price
                .numerator
                .checked_mul(factor)
                .ok_or(ClearingError::InvalidOraclePrice)?;
        }
        let common = gcd(price.numerator, price.denominator);
        price.numerator /= common;
        price.denominator /= common;
        Ok(price)
    }
}

impl ExactPrice {
    pub fn new(quote_units: Wide, base_units: Wide) -> Result<Self, ClearingError> {
        if quote_units == Wide::ZERO || base_units == Wide::ZERO {
            return Err(ClearingError::InvalidPrice);
        }
        let common = gcd(quote_units, base_units);
        Ok(Self {
            quote_units: quote_units / common,
            base_units: base_units / common,
        })
    }

    /// Both reference prices refer to one WHOLE token. Convert them to quote base
    /// units per base base unit with on-chain decimals, exactly:
    /// P = (base_ref / quote_ref) * 10^quote_decimals / 10^base_decimals.
    /// Polaris must freeze the two source prices in one snapshot and enforce
    /// its own maximum age and cross-price timestamp skew.
    pub fn from_reference_prices(
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<Self, ClearingError> {
        if base_price.numerator == Wide::ZERO
            || base_price.denominator == Wide::ZERO
            || quote_price.numerator == Wide::ZERO
            || quote_price.denominator == Wide::ZERO
        {
            return Err(ClearingError::InvalidOraclePrice);
        }
        let quote_units = base_price
            .numerator
            .checked_mul(quote_price.denominator)
            .and_then(|value| value.checked_mul(power_of_ten(quote_decimals).ok()?))
            .ok_or(ClearingError::ArithmeticOverflow)?;
        let base_units = base_price
            .denominator
            .checked_mul(quote_price.numerator)
            .and_then(|value| value.checked_mul(power_of_ten(base_decimals).ok()?))
            .ok_or(ClearingError::ArithmeticOverflow)?;
        Self::new(quote_units, base_units)
    }

    pub(crate) fn quote_for_base_floor(self, base: Wide) -> Result<Wide, ClearingError> {
        mul_div_floor(base, self.quote_units, self.base_units)
    }

    pub(crate) fn base_for_quote_ceil(self, quote: Wide) -> Result<Wide, ClearingError> {
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
        assert_eq!(price.quote_units, Wide::from(1_000u64));
        assert_eq!(price.base_units, Wide::ONE);
        // One BTC base unit corresponds to 1,000 USDT base units here.
    }

    #[test]
    fn decimal_input_is_exact() {
        let price = ReferencePrice::from_decimal("0.00012500").unwrap();
        assert_eq!(price.numerator, Wide::ONE);
        assert_eq!(price.denominator, Wide::from(8_000u64));
        for invalid in ["0", "-1", "1e4", "1.", ".1", "1.2.3"] {
            assert!(ReferencePrice::from_decimal(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn json_scientific_price_is_exact() {
        let price = ReferencePrice::from_json_number("1.2500e-4").unwrap();
        assert_eq!(price.numerator, Wide::ONE);
        assert_eq!(price.denominator, Wide::from(8_000u64));
        let price = ReferencePrice::from_json_number("1.25E+4").unwrap();
        assert_eq!(price.numerator, Wide::from(12_500u64));
        assert_eq!(price.denominator, Wide::ONE);
        for invalid in ["1e", "1e-", "1e999", "1e2e3", "-1e2"] {
            assert!(
                ReferencePrice::from_json_number(invalid).is_err(),
                "{invalid}"
            );
        }
    }
}

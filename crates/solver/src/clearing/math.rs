use miden_protocol::asset::AssetAmount;
use ruint::aliases::U256;
use rust_decimal::Decimal;

use super::config::PPM_DENOMINATOR;
use super::types::{BatchPrice, ClearingError};

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

    /// `price` whole quote tokens per whole base token, in base units:
    /// `price × 10^quote_decimals` quote units per `10^base_decimals` base
    /// units. A `Decimal` is `mantissa / 10^scale`, so both sides are whole
    /// numbers and nothing is rounded.
    pub fn from_whole_price(
        price: Decimal,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<Self, ClearingError> {
        let mantissa = u128::try_from(price.mantissa()).map_err(|_| ClearingError::InvalidPrice)?;
        let scale = u8::try_from(price.scale()).map_err(|_| ClearingError::InvalidPrice)?;
        Self::new(
            checked_mul(U256::from(mantissa), power_of_ten(quote_decimals)?)?,
            checked_mul(power_of_ten(scale)?, power_of_ten(base_decimals)?)?,
        )
    }

    /// The same price seen from the other token: base and quote swap.
    #[must_use]
    pub fn inverse(self) -> Self {
        Self {
            quote_units: self.base_units,
            base_units: self.quote_units,
        }
    }

    pub(crate) fn quote_for_base_floor(self, base: U256) -> Result<U256, ClearingError> {
        mul_div_floor(base, self.quote_units, self.base_units)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(raw: &str) -> Decimal {
        Decimal::from_str_exact(raw).unwrap()
    }

    #[test]
    fn whole_price_uses_token_decimals() {
        // 100000 USDT (6 decimals) per BTC (8 decimals): 1000 USDT units per
        // BTC unit.
        let price = BatchPrice::from_whole_price(dec("100000.00000000"), 8, 6).unwrap();
        assert_eq!(price.quote_units, U256::from(1_000u64));
        assert_eq!(price.base_units, U256::ONE);
    }

    #[test]
    fn whole_price_converts_with_unequal_decimals() {
        // 1 whole base token (18 decimals) = 2718.655 quote tokens (6 decimals):
        // 2_718_655 / 10^15, reduced by their common factor 5.
        let price = BatchPrice::from_whole_price(dec("2718.655"), 18, 6).unwrap();
        assert_eq!(price.quote_units, U256::from(543_731u64));
        assert_eq!(price.base_units, U256::from(200_000_000_000_000u64));
        // Trailing zeros change nothing.
        assert_eq!(
            BatchPrice::from_whole_price(dec("2718.65500000"), 18, 6).unwrap(),
            price
        );
        // Seen from the other token, the two numbers swap.
        let inverse = price.inverse();
        assert_eq!(inverse.quote_units, price.base_units);
        assert_eq!(inverse.base_units, price.quote_units);
    }

    #[test]
    fn whole_price_rejects_zero_negative_and_overflow() {
        assert!(BatchPrice::from_whole_price(Decimal::ZERO, 6, 6).is_err());
        assert!(BatchPrice::from_whole_price(dec("-1"), 6, 6).is_err());
        assert!(matches!(
            BatchPrice::from_whole_price(Decimal::MAX, 0, 78),
            Err(ClearingError::ArithmeticOverflow)
        ));
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// A positive decimal with up to eight fractional digits, as Binance sends.
        fn price() -> impl Strategy<Value = Decimal> {
            (1i64..1_000_000_000_000_000, 0u32..=8)
                .prop_map(|(units, places)| Decimal::new(units, places))
        }

        proptest! {
            /// The base-unit price is the whole-token price scaled by the
            /// decimals, in lowest terms.
            #[test]
            fn whole_price_conversion_preserves_the_ratio(
                price in price(),
                base_decimals in 0u8..=18,
                quote_decimals in 0u8..=18,
            ) {
                let batch = BatchPrice::from_whole_price(price, base_decimals, quote_decimals).unwrap();
                let ten = |power: u32| U256::from(10u8).pow(U256::from(power));
                let mantissa = U256::from(u128::try_from(price.mantissa()).unwrap());
                prop_assert_eq!(
                    batch.quote_units * ten(price.scale()) * ten(base_decimals.into()),
                    batch.base_units * mantissa * ten(quote_decimals.into())
                );
                prop_assert_eq!(batch.quote_units.gcd(batch.base_units), U256::ONE);
            }
        }
    }
}

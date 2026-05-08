use crate::matching::types::*;

/// One USD cent — the base unit for all price values in this module.
/// All prices are fixed-point integers denominated in cents,
/// avoiding floating-point rounding in value comparisons.
pub type UsdCents = u64;

pub trait PriceFeed {
    /// Price per token unit, denominated in [`UsdCents`].
    /// e.g. ETH ≈ 200_000 ($2 000.00), USDC = 100 ($1.00).
    fn usd_price_cents(&self, token: TokenId) -> UsdCents;

    /// Returns `true` when the USD value offered is at least the USD value requested.
    fn is_order_profitable(
        &self,
        offered_token: TokenId,
        offered_amount: Amount,
        requested_token: TokenId,
        requested_amount: Amount,
    ) -> bool {
        let offered_value = offered_amount as u128 * self.usd_price_cents(offered_token) as u128;
        let requested_value =
            requested_amount as u128 * self.usd_price_cents(requested_token) as u128;
        offered_value >= requested_value
    }
}

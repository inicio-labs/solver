use crate::matching::types::*;

/// One USD cent — the base unit for all price values in this module.
/// All prices are fixed-point integers denominated in cents,
/// avoiding floating-point rounding in value comparisons.
pub type UsdCents = u64;

pub trait PriceFeed {
    /// Price per token unit in [`UsdCents`], or `None` when the token has no
    /// known price. A missing price means the token is **not matchable** —
    /// callers must exclude it, never substitute a default. (A bogus default
    /// silently corrupts profitability: e.g. a 1¢ fallback values an unpriced
    /// asset at ~0, so the solver either drops all its matches or, when both
    /// sides are unpriced, compares raw token-unit amounts and can settle a
    /// value-losing trade.)
    /// e.g. ETH ≈ 200_000 ($2 000.00), USDC = 100 ($1.00).
    fn price_cents(&self, token: TokenId) -> Option<UsdCents>;

    /// `true` iff **both** tokens are priced and the USD value offered is at
    /// least the USD value requested. An unpriced token on either side
    /// returns `false` — the order is excluded from matching this tick.
    fn is_order_profitable(
        &self,
        offered_token: TokenId,
        offered_amount: Amount,
        requested_token: TokenId,
        requested_amount: Amount,
    ) -> bool {
        let (Some(offered_price), Some(requested_price)) = (
            self.price_cents(offered_token),
            self.price_cents(requested_token),
        ) else {
            return false;
        };
        let offered_value = offered_amount as u128 * offered_price as u128;
        let requested_value = requested_amount as u128 * requested_price as u128;
        offered_value >= requested_value
    }
}

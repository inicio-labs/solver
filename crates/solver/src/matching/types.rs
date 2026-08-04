pub use crate::types::{TokenId, OrderId, Amount, OrderStatus};
use std::cmp::Ordering;
use std::collections::HashSet;

/// Identifier for an external DEX connection (assigned by the router). Recorded
/// against a parked note so reactivation can log which DEX it was handed to.
pub type DexId = u64;

/// Computes how much offered asset is released for a given fill of the requested asset.
/// Matches the on-chain PSWAP calculation: offered_total * fill_amount / requested_total.
fn calculate_output_amount(offered_total: Amount, requested_total: Amount, fill_amount: Amount) -> Amount {
    ((offered_total as u128 * fill_amount as u128) / requested_total as u128) as Amount
}

/// BTreeMap key for ordering orders by rate. Uses cross-multiplication
/// for exact comparison — no floating point, no precision loss.
///
/// Rate = requested / offered. Lower rate = more generous order.
///
/// INVARIANT: `rate_key()` depends only on `Order::requested` and
/// `Order::offered`, both of which are immutable for the lifetime of the
/// order. Partial fills update `requested_remaining` but never the key, so
/// it is safe to keep an order in the BTreeMap-keyed index after filling it.
/// Any future change that makes the rate key depend on a mutable field
/// MUST also re-index the order in `OrderBook::pair_index`.
#[derive(Clone, Copy, Debug)]
pub struct RateKey {
    pub requested: Amount,
    pub offered: Amount,
}

impl RateKey {
    pub fn new(requested: Amount, offered: Amount) -> Self {
        Self { requested, offered }
    }
}

impl PartialEq for RateKey {
    fn eq(&self, other: &Self) -> bool {
        (self.requested as u128) * (other.offered as u128)
            == (other.requested as u128) * (self.offered as u128)
    }
}
impl Eq for RateKey {}

impl PartialOrd for RateKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RateKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = self.requested as u128 * other.offered as u128;
        let rhs = other.requested as u128 * self.offered as u128;
        lhs.cmp(&rhs)
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: OrderId,
    pub offered_token: TokenId,
    pub requested_token: TokenId,
    /// Total offered amount (immutable).
    pub offered: Amount,
    /// Total requested amount (immutable).
    pub requested: Amount,
    /// How much of requested is still unfilled.
    pub requested_remaining: Amount,
}

/// Result of matching two orders (internal use).
pub struct MatchResult {
    pub surplus_offered: Amount,
    pub surplus_requested: Amount,
}

impl Order {
    /// Rate key for BTreeMap ordering. Exact integer comparison.
    pub fn rate_key(&self) -> RateKey {
        RateKey::new(self.requested, self.offered)
    }

    /// Is this order still active?
    pub fn is_active(&self) -> bool {
        self.requested_remaining > 0
    }

    /// Is this order completely filled?
    pub fn is_completely_filled(&self) -> bool {
        self.requested_remaining == 0
    }

    /// How much of requested has been filled so far.
    pub fn requested_filled(&self) -> Amount {
        self.requested - self.requested_remaining
    }

    /// How much of the offered asset would be released for a given requested fill.
    pub fn offered_for(&self, requested_fill: Amount) -> Amount {
        calculate_output_amount(self.offered, self.requested, requested_fill)
    }

    /// Inverse (floor): given some offered amount, how much requested does that correspond to?
    pub fn requested_for(&self, offered_amount: Amount) -> Amount {
        calculate_output_amount(self.requested, self.offered, offered_amount)
    }

    /// Ceiling inverse of offered_for: minimum requested fill such that
    /// offered_for(result) >= target.
    pub fn min_fill_for_release(&self, target: Amount) -> Amount {
        if target == 0 {
            return 0;
        }
        if target >= self.offered {
            return self.requested;
        }

        let fill = ((target as u128 * self.requested as u128) / self.offered as u128) as Amount;
        let fill = fill.min(self.requested);

        // Safety check: integer truncation could cause a 1-unit miss.
        if self.offered_for(fill) >= target {
            fill
        } else {
            (fill + 1).min(self.requested)
        }
    }

    /// How much of the offered asset can still be released.
    pub fn offered_remaining(&self) -> Amount {
        self.offered_for(self.requested_remaining)
    }

    /// Fill some of the requested side. Returns offered released.
    pub fn fill(&mut self, requested_filled: Amount) -> Amount {
        let requested_filled = requested_filled.min(self.requested_remaining);
        let released = self.offered_for(requested_filled);
        self.requested_remaining -= requested_filled;
        released
    }

    /// Fill completely — zero remaining. Returns offered released.
    pub fn full_fill(&mut self) -> Amount {
        let released = self.offered_remaining();
        self.requested_remaining = 0;
        released
    }

    /// Is matching this order against another order profitable?
    pub fn is_profitable_with(&self, other: &Order) -> bool {
        (self.offered as u128 * other.offered as u128)
            > (self.requested as u128 * other.requested as u128)
    }

    /// Match this order against another counter-order.
    /// Returns None if no trade is possible.
    pub fn match_with(&mut self, other: &mut Order) -> Option<MatchResult> {
        if !self.is_active() || !other.is_active() {
            return None;
        }
        if !self.is_profitable_with(other) {
            return None;
        }

        let self_sends = self.offered_remaining().min(other.requested_remaining);
        if self_sends == 0 {
            return None;
        }

        // Other fills with what self sends, releasing its offered token back to self
        let other_releases = other.fill(self_sends);
        if other_releases == 0 {
            return None;
        }

        // Self fills with what other released, capped at what self still needs
        let self_receives = other_releases.min(self.requested_remaining);
        let self_releases = self.fill(self_receives);

        // Profitability invariant: self must release at least as much as it sends
        if self_releases < self_sends {
            return None;
        }

        Some(MatchResult {
            surplus_offered: self_releases.saturating_sub(self_sends),
            surplus_requested: other_releases.saturating_sub(self_receives),
        })
    }
}

#[derive(Debug)]
pub struct SettlementBatch {
    /// Set of order IDs that were touched (fully or partially filled).
    pub filled_orders: HashSet<OrderId>,
    pub protocol_balances: Vec<(TokenId, Amount)>,
    pub cycles_executed: u64,
    pub remaining_orders: u64,
}

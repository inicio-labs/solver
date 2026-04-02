use miden_protocol::account::AccountId;
use std::collections::HashSet;

pub type TokenId = AccountId;
pub type OrderId = u32;
pub type Amount = u32;

const PRECISION_FACTOR: u64 = 100_000;

/// Computes how much offered asset is released for a given fill of the requested asset.
/// Matches the on-chain PSWAP MASM calculation exactly.
fn calculate_output_amount(offered_total: Amount, requested_total: Amount, fill_amount: Amount) -> Amount {
    if requested_total == fill_amount {
        return offered_total;
    }
    if fill_amount == 0 {
        return 0;
    }

    if offered_total > requested_total {
        let ratio = (offered_total as u64 * PRECISION_FACTOR) / requested_total as u64;
        ((fill_amount as u64 * ratio) / PRECISION_FACTOR) as Amount
    } else {
        let ratio = (requested_total as u64 * PRECISION_FACTOR) / offered_total as u64;
        ((fill_amount as u64 * PRECISION_FACTOR) / ratio) as Amount
    }
}

/// BTreeMap key for ordering orders by rate. Uses cross-multiplication
/// for exact comparison — no floating point, no precision loss.
///
/// Rate = requested / offered. Lower rate = more generous order.
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
        (self.requested as u64) * (other.offered as u64)
            == (other.requested as u64) * (self.offered as u64)
    }
}
impl Eq for RateKey {}

impl PartialOrd for RateKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RateKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let lhs = self.requested as u64 * other.offered as u64;
        let rhs = other.requested as u64 * self.offered as u64;
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
    ///
    /// Guarantees: if target <= offered, then offered_for(min_fill_for_release(target)) >= target.
    /// Uses ceiling division to round UP, ensuring the forward calculation never undershoots.
    pub fn min_fill_for_release(&self, target: Amount) -> Amount {
        if target == 0 {
            return 0;
        }
        if target >= self.offered {
            return self.requested;
        }

        let p = PRECISION_FACTOR as u128;

        let fill = if self.offered > self.requested {
            // Forward: ratio = (offered * P) / requested, result = (fill * ratio) / P
            // Need: (fill * ratio) / P >= target → fill >= ceil(target * P / ratio)
            let ratio = (self.offered as u128 * p) / self.requested as u128;
            ((target as u128 * p + ratio - 1) / ratio) as Amount
        } else {
            // Forward: ratio = (requested * P) / offered, result = (fill * P) / ratio
            // Need: (fill * P) / ratio >= target → fill >= ceil(target * ratio / P)
            let ratio = (self.requested as u128 * p) / self.offered as u128;
            ((target as u128 * ratio + p - 1) / p) as Amount
        };

        let fill = fill.min(self.requested);

        // Safety verify: ratio truncation could theoretically cause a 1-unit miss.
        // If so, bump by 1.
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
        assert!(requested_filled <= self.requested_remaining, "fill exceeds requested_remaining");
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
        (self.offered as u64 * other.offered as u64)
            > (self.requested as u64 * other.requested as u64)
    }

    /// Match this order against another counter-order.
    /// Returns None if no trade is possible.
    pub fn match_with(&mut self, other: &mut Order) -> Option<MatchResult> {
        if !self.is_active() || !other.is_active() {
            return None;
        }

        let self_can_release = self.offered_remaining();
        let other_wants = other.requested_remaining;

        if self_can_release == 0 || other_wants == 0 {
            return None;
        }

        let (self_offered_released, other_offered_released, self_requested_filled, other_requested_filled) =
            if self_can_release <= other_wants {
                // Self is bottleneck → full fill self (no precision loss)
                let self_or = self.full_fill();
                let other_rf = self_or;
                let other_or = other.fill(other_rf);
                (self_or, other_or, self.requested, other_rf)
            } else {
                // Other is bottleneck → partial fill self, full fill other.
                // Use ceiling inverse to guarantee self releases >= other_wants.
                let self_rf = self.min_fill_for_release(other_wants);

                if self_rf == 0 {
                    // Can't express partial self fill — fall back to full fill self
                    let self_or = self.full_fill();
                    let other_rf = self_or.min(other.requested_remaining);
                    let other_or = other.fill(other_rf);
                    (self_or, other_or, self.requested, other_rf)
                } else {
                    // Guard: if ceiling pushed self_rf beyond what other can cover,
                    // fall back to full fill self (the spread was too tight).
                    let other_can_release = other.offered_remaining();
                    if self_rf > other_can_release {
                        let self_or = self.full_fill();
                        let other_rf = self_or.min(other.requested_remaining);
                        let other_or = other.fill(other_rf);
                        (self_or, other_or, self.requested, other_rf)
                    } else {
                        // Fill self first → guaranteed release >= other_wants
                        let self_or = self.fill(self_rf);
                        // Other receives exactly what self released
                        let other_rf = self_or.min(other.requested_remaining);
                        let other_or = other.fill(other_rf);
                        (self_or, other_or, self_rf, other_rf)
                    }
                }
            };

        let surplus_offered = self_offered_released.saturating_sub(other_requested_filled);
        let surplus_requested = other_offered_released.saturating_sub(self_requested_filled);

        Some(MatchResult {
            surplus_offered,
            surplus_requested,
        })
    }
}

#[derive(Debug)]
pub struct SettlementBatch {
    /// Set of order IDs that were touched (fully or partially filled).
    pub filled_orders: HashSet<OrderId>,
    pub protocol_balances: Vec<(TokenId, Amount)>,
    pub cycles_executed: u32,
    pub remaining_orders: u32,
}

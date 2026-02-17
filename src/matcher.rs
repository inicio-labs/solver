use crate::order::Order;

/// A single order with its computed fill and output amounts.
#[derive(Debug, Clone)]
pub struct FilledOrder {
    pub order: Order,
    pub fill_amount: u64,   // requested asset received
    pub output_amount: u64, // offered asset released (calculate_output_amount result)
    pub is_partial: bool,   // fill_amount < requested_amount
}

/// A group of matched orders from both sides, ready for execution as a single transaction.
#[derive(Debug)]
pub struct MatchGroup {
    pub side_a: Vec<FilledOrder>, // offers X, wants Y
    pub side_b: Vec<FilledOrder>, // offers Y, wants X
    pub total_x: u64,            // sum(side_a output_amounts)
    pub total_y: u64,            // sum(side_b output_amounts)
    pub demand_x: u64,           // sum(side_b fill_amounts) — X that B-side needs
    pub demand_y: u64,           // sum(side_a fill_amounts) — Y that A-side needs
    pub surplus_x: u64,          // total_x - demand_x (solver spread)
    pub surplus_y: u64,          // total_y - demand_y (solver spread)
}

/// Prefix sums for offered and required amounts on one side.
struct PrefixSums {
    offered: Vec<u128>,
    required: Vec<u128>,
}

impl PrefixSums {
    fn build(orders: &[Order]) -> Self {
        let n = orders.len();
        let mut offered = Vec::with_capacity(n);
        let mut required = Vec::with_capacity(n);

        let mut sum_offered: u128 = 0;
        let mut sum_required: u128 = 0;

        for order in orders {
            sum_offered += order.offered_amount as u128;
            sum_required += order.requested_amount as u128;
            offered.push(sum_offered);
            required.push(sum_required);
        }

        PrefixSums { offered, required }
    }

    fn offered_sum(&self, idx: usize) -> u128 {
        if idx >= self.offered.len() {
            return *self.offered.last().unwrap_or(&0);
        }
        self.offered[idx]
    }

    fn required_sum(&self, idx: usize) -> u128 {
        if idx >= self.required.len() {
            return *self.required.last().unwrap_or(&0);
        }
        self.required[idx]
    }

    fn len(&self) -> usize {
        self.offered.len()
    }
}

pub struct Matcher;

impl Matcher {
    /// Minimum fill ratio required for an order to be included in a match.
    /// Orders filled below this fraction of their requested amount are skipped.
    const MIN_FILL_RATIO_PCT: u64 = 25;

    /// Run the bilateral convergence matching algorithm.
    ///
    /// Orders are separated into two sides based on which asset they offer relative
    /// to the pair's canonical ordering (asset_x, asset_y):
    /// - Side A: offers X, wants Y
    /// - Side B: offers Y, wants X
    pub fn run(
        orders: Vec<Order>,
        asset_x: miden_protocol::account::AccountId,
        asset_y: miden_protocol::account::AccountId,
    ) -> Option<MatchGroup> {
        // 1. Separate sides
        let (mut side_a, mut side_b) = Self::separate_sides(orders, asset_x, asset_y);

        if side_a.is_empty() || side_b.is_empty() {
            return None;
        }

        // 2. Sort both sides by price ratio ascending (cheapest first)
        side_a.sort_by(|a, b| a.price_ratio().partial_cmp(&b.price_ratio()).unwrap());
        side_b.sort_by(|a, b| a.price_ratio().partial_cmp(&b.price_ratio()).unwrap());

        // 3. Build prefix sums
        let prefix_a = PrefixSums::build(&side_a);
        let prefix_b = PrefixSums::build(&side_b);

        // 4. Converge: alternating binary search
        let (i, j) = Self::converge(&prefix_a, &prefix_b);

        // Check feasibility: at least some liquidity must flow in both directions.
        {
            let available_y = prefix_b.offered_sum(j);
            let available_x = prefix_a.offered_sum(i);
            if available_y == 0 || available_x == 0 {
                return None;
            }
        }

        // 5. Compute fill amounts
        Self::compute_fills(&mut side_a, &mut side_b, i, j, &prefix_a, &prefix_b);

        // 6. Generate match group (many-to-many)
        Self::generate_match_group(side_a, side_b, i, j)
    }

    /// Separate orders into Side A (offers X, wants Y) and Side B (offers Y, wants X).
    fn separate_sides(
        orders: Vec<Order>,
        asset_x: miden_protocol::account::AccountId,
        _asset_y: miden_protocol::account::AccountId,
    ) -> (Vec<Order>, Vec<Order>) {
        let mut side_a = Vec::new();
        let mut side_b = Vec::new();

        for order in orders {
            if order.offered_faucet_id == asset_x {
                side_a.push(order);
            } else {
                side_b.push(order);
            }
        }

        (side_a, side_b)
    }

    /// Alternating binary search convergence.
    /// Returns (i, j): the frontier indices on each side.
    fn converge(prefix_a: &PrefixSums, prefix_b: &PrefixSums) -> (usize, usize) {
        if prefix_a.len() == 0 || prefix_b.len() == 0 {
            return (0, 0);
        }

        let mut i = prefix_a.len() - 1;
        let mut j = prefix_b.len() - 1;

        loop {
            // Available Y from B[0..=j] must cover A's required Y up to i
            let available_y = prefix_b.offered_sum(j);
            let new_i = Self::binary_search_max(&prefix_a.required, available_y);

            // Available X from A[0..=i] must cover B's required X up to j
            let available_x = prefix_a.offered_sum(new_i);
            let new_j = Self::binary_search_max(&prefix_b.required, available_x);

            if new_i == i && new_j == j {
                break;
            }
            i = new_i;
            j = new_j;
        }

        (i, j)
    }

    /// Binary search for the maximum index where prefix[idx] <= available.
    /// Returns 0 if no valid index exists (caller must check feasibility).
    fn binary_search_max(prefix: &[u128], available: u128) -> usize {
        if prefix.is_empty() || prefix[0] > available {
            return 0;
        }

        let mut lo = 0usize;
        let mut hi = prefix.len() - 1;

        while lo < hi {
            let mid = lo + (hi - lo + 1) / 2;
            if prefix[mid] <= available {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        lo
    }

    /// Returns true if the order's fill amount is at least MIN_FILL_RATIO_PCT% of its requested amount.
    fn meets_min_fill(order: &Order) -> bool {
        if order.fill_amount == 0 {
            return false;
        }
        // fill_amount * 100 >= requested_amount * MIN_FILL_RATIO_PCT (avoids floating point)
        (order.fill_amount as u128) * 100
            >= (order.requested_amount as u128) * (Self::MIN_FILL_RATIO_PCT as u128)
    }

    /// Compute fill amounts for all orders up to the frontier.
    fn compute_fills(
        side_a: &mut [Order],
        side_b: &mut [Order],
        i: usize,
        j: usize,
        prefix_a: &PrefixSums,
        prefix_b: &PrefixSums,
    ) {
        // Fully fill orders before the frontier
        for order in side_a.iter_mut().take(i) {
            order.fill_amount = order.requested_amount;
        }
        for order in side_b.iter_mut().take(j) {
            order.fill_amount = order.requested_amount;
        }

        // Frontier partial fill for side A order i
        let total_available_y = prefix_b.offered_sum(j);
        let required_before_i = if i > 0 { prefix_a.required_sum(i - 1) } else { 0 };
        let remaining_y = total_available_y.saturating_sub(required_before_i);
        let frontier_a_requested = side_a[i].requested_amount as u128;
        let fill_a = std::cmp::min(remaining_y, frontier_a_requested) as u64;
        side_a[i].fill_amount = fill_a;

        // Frontier partial fill for side B order j
        let total_available_x = prefix_a.offered_sum(i);
        let required_before_j = if j > 0 { prefix_b.required_sum(j - 1) } else { 0 };
        let remaining_x = total_available_x.saturating_sub(required_before_j);
        let frontier_b_requested = side_b[j].requested_amount as u128;
        let fill_b = std::cmp::min(remaining_x, frontier_b_requested) as u64;
        side_b[j].fill_amount = fill_b;
    }

    /// Check if a FilledOrder meets the minimum fill ratio threshold.
    fn filled_meets_min_fill(f: &FilledOrder) -> bool {
        if f.fill_amount == 0 {
            return false;
        }
        (f.fill_amount as u128) * 100
            >= (f.order.requested_amount as u128) * (Self::MIN_FILL_RATIO_PCT as u128)
    }

    /// Build a FilledOrder from an Order, computing output_amount and is_partial.
    fn build_filled_order(o: Order) -> FilledOrder {
        let output_amount = miden_swapp::PswapNote::calculate_output_amount(
            o.offered_amount,
            o.requested_amount,
            o.fill_amount,
        );
        let is_partial = o.fill_amount < o.requested_amount;
        let fill_amount = o.fill_amount;
        FilledOrder {
            order: o,
            fill_amount,
            output_amount,
            is_partial,
        }
    }

    /// Recompute a FilledOrder's output_amount after its fill_amount was adjusted.
    fn recompute_filled_order(f: &mut FilledOrder) {
        f.output_amount = miden_swapp::PswapNote::calculate_output_amount(
            f.order.offered_amount,
            f.order.requested_amount,
            f.fill_amount,
        );
        f.is_partial = f.fill_amount < f.order.requested_amount;
    }

    /// Generate a match group containing all filled orders from both sides.
    ///
    /// After filtering by min_fill, solvency may be violated (filtered-out frontier
    /// orders' output was counted in the other side's demand). A convergence loop
    /// reduces frontier fill amounts and removes orders that fall below min_fill
    /// until group solvency holds.
    fn generate_match_group(
        side_a: Vec<Order>,
        side_b: Vec<Order>,
        i: usize,
        j: usize,
    ) -> Option<MatchGroup> {
        let mut filled_a: Vec<FilledOrder> = side_a
            .into_iter()
            .take(i + 1)
            .filter(|o| Self::meets_min_fill(o))
            .map(Self::build_filled_order)
            .collect();

        let mut filled_b: Vec<FilledOrder> = side_b
            .into_iter()
            .take(j + 1)
            .filter(|o| Self::meets_min_fill(o))
            .map(Self::build_filled_order)
            .collect();

        if filled_a.is_empty() || filled_b.is_empty() {
            return None;
        }

        // Solvency adjustment loop: after filtering, demand may exceed supply.
        // Reduce frontier (last) fill amounts until solvency holds on both sides.
        loop {
            let total_x: u64 = filled_a.iter().map(|f| f.output_amount).sum();
            let demand_x: u64 = filled_b.iter().map(|f| f.fill_amount).sum();

            let mut changed = false;

            // If B side demands more X than A side supplies, reduce last B's fill
            if demand_x > total_x {
                let excess = demand_x - total_x;
                if let Some(last_b) = filled_b.last_mut() {
                    if last_b.fill_amount > excess {
                        last_b.fill_amount -= excess;
                        Self::recompute_filled_order(last_b);
                        if !Self::filled_meets_min_fill(last_b) {
                            filled_b.pop();
                        }
                    } else {
                        filled_b.pop();
                    }
                    changed = true;
                }
            }

            // If A side demands more Y than B side supplies, reduce last A's fill
            // Recompute since filled_b may have changed
            let recomputed_total_y: u64 = filled_b.iter().map(|f| f.output_amount).sum();
            let recomputed_demand_y: u64 = filled_a.iter().map(|f| f.fill_amount).sum();
            if recomputed_demand_y > recomputed_total_y {
                let excess = recomputed_demand_y - recomputed_total_y;
                if let Some(last_a) = filled_a.last_mut() {
                    if last_a.fill_amount > excess {
                        last_a.fill_amount -= excess;
                        Self::recompute_filled_order(last_a);
                        if !Self::filled_meets_min_fill(last_a) {
                            filled_a.pop();
                        }
                    } else {
                        filled_a.pop();
                    }
                    changed = true;
                }
            }

            if filled_a.is_empty() || filled_b.is_empty() {
                return None;
            }

            if !changed {
                break;
            }
        }

        let total_x: u64 = filled_a.iter().map(|f| f.output_amount).sum();
        let total_y: u64 = filled_b.iter().map(|f| f.output_amount).sum();
        let demand_x: u64 = filled_b.iter().map(|f| f.fill_amount).sum();
        let demand_y: u64 = filled_a.iter().map(|f| f.fill_amount).sum();
        let surplus_x = total_x.saturating_sub(demand_x);
        let surplus_y = total_y.saturating_sub(demand_y);

        Some(MatchGroup {
            side_a: filled_a,
            side_b: filled_b,
            total_x,
            total_y,
            demand_x,
            demand_y,
            surplus_x,
            surplus_y,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_crypto::rand::RpoRandomCoin;
    use miden_protocol::account::{AccountId, AccountIdVersion, AccountStorageMode, AccountType};
    use miden_protocol::asset::{Asset, FungibleAsset};
    use miden_protocol::note::{NoteAttachment, NoteAttachmentScheme, NoteType};
    use miden_protocol::{Word, ZERO};

    fn make_faucet_id(seed: u64) -> AccountId {
        AccountId::dummy(
            [seed as u8; 15],
            AccountIdVersion::Version0,
            AccountType::FungibleFaucet,
            AccountStorageMode::Public,
        )
    }

    fn make_account_id(seed: u64) -> AccountId {
        AccountId::dummy(
            [seed as u8; 15],
            AccountIdVersion::Version0,
            AccountType::RegularAccountImmutableCode,
            AccountStorageMode::Public,
        )
    }

    fn make_test_order(
        offered_faucet: AccountId,
        offered_amount: u64,
        requested_faucet: AccountId,
        requested_amount: u64,
    ) -> Order {
        // Create a minimal note for testing
        let offered_asset =
            Asset::Fungible(FungibleAsset::new(offered_faucet, offered_amount).unwrap());
        let requested_asset =
            Asset::Fungible(FungibleAsset::new(requested_faucet, requested_amount).unwrap());
        let creator = make_account_id(99);

        let note = miden_swapp::PswapNote::create(
            creator,
            offered_asset,
            requested_asset,
            NoteType::Public,
            NoteAttachment::new_word(NoteAttachmentScheme::none(), Word::from([ZERO; 4])),
            &mut RpoRandomCoin::new([ZERO; 4].into()),
        )
        .expect("Failed to create test note");

        Order {
            note,
            offered_faucet_id: offered_faucet,
            offered_amount,
            requested_faucet_id: requested_faucet,
            requested_amount,
            creator_id: creator,
            fill_amount: 0,
        }
    }

    #[test]
    fn test_no_orders() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);
        let result = Matcher::run(vec![], faucet_x, faucet_y);
        assert!(result.is_none());
    }

    #[test]
    fn test_one_side_only() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        let order = make_test_order(faucet_x, 100, faucet_y, 50);
        let result = Matcher::run(vec![order], faucet_x, faucet_y);
        assert!(result.is_none());
    }

    #[test]
    fn test_simple_full_match() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // A offers 100 X, wants 50 Y
        let order_a = make_test_order(faucet_x, 100, faucet_y, 50);
        // B offers 50 Y, wants 100 X
        let order_b = make_test_order(faucet_y, 50, faucet_x, 100);

        let group = Matcher::run(vec![order_a, order_b], faucet_x, faucet_y).expect("Should match");
        assert_eq!(group.side_a.len(), 1);
        assert_eq!(group.side_b.len(), 1);

        assert_eq!(group.side_a[0].fill_amount, 50); // A gets fully filled (50 Y)
        assert_eq!(group.side_b[0].fill_amount, 100); // B gets fully filled (100 X)
    }

    #[test]
    fn test_partial_match() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // A offers 100 X, wants 50 Y
        let order_a = make_test_order(faucet_x, 100, faucet_y, 50);
        // B offers 25 Y, wants 50 X (only half of what A needs)
        let order_b = make_test_order(faucet_y, 25, faucet_x, 50);

        let group = Matcher::run(vec![order_a, order_b], faucet_x, faucet_y).expect("Should match");
        assert_eq!(group.side_a.len(), 1);
        assert_eq!(group.side_b.len(), 1);

        // A only gets 25 Y (partial fill)
        assert_eq!(group.side_a[0].fill_amount, 25);
        // B gets fully filled (50 X)
        assert_eq!(group.side_b[0].fill_amount, 50);
    }

    #[test]
    fn test_multiple_orders_convergence() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // Side A: two orders offering X
        let a1 = make_test_order(faucet_x, 50, faucet_y, 25);
        let a2 = make_test_order(faucet_x, 50, faucet_y, 25);

        // Side B: one order offering Y wanting all the X
        let b1 = make_test_order(faucet_y, 50, faucet_x, 100);

        let group = Matcher::run(vec![a1, a2, b1], faucet_x, faucet_y);
        assert!(group.is_some());
    }

    #[test]
    fn test_prefix_sums() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        let o1 = make_test_order(faucet_x, 100, faucet_y, 50);
        let o2 = make_test_order(faucet_x, 200, faucet_y, 80);

        let prefix = PrefixSums::build(&[o1, o2]);
        assert_eq!(prefix.offered_sum(0), 100);
        assert_eq!(prefix.offered_sum(1), 300);
        assert_eq!(prefix.required_sum(0), 50);
        assert_eq!(prefix.required_sum(1), 130);
    }

    #[test]
    fn test_binary_search_max() {
        let prefix = vec![10u128, 30, 60, 100];
        assert_eq!(Matcher::binary_search_max(&prefix, 5), 0);
        assert_eq!(Matcher::binary_search_max(&prefix, 10), 0);
        assert_eq!(Matcher::binary_search_max(&prefix, 30), 1);
        assert_eq!(Matcher::binary_search_max(&prefix, 59), 1);
        assert_eq!(Matcher::binary_search_max(&prefix, 60), 2);
        assert_eq!(Matcher::binary_search_max(&prefix, 100), 3);
        assert_eq!(Matcher::binary_search_max(&prefix, 200), 3);
    }

    #[test]
    fn test_no_match_below_min_fill_threshold() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // A offers 10 X, wants 100 Y
        let order_a = make_test_order(faucet_x, 10, faucet_y, 100);
        // B offers 10 Y, wants 100 X
        let order_b = make_test_order(faucet_y, 10, faucet_x, 100);

        let result = Matcher::run(vec![order_a, order_b], faucet_x, faucet_y);
        // Both fills would be 10% of requested (below 25% threshold), so no match
        assert!(result.is_none());
    }

    #[test]
    fn test_match_at_min_fill_threshold() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // A offers 100 X, wants 100 Y
        let order_a = make_test_order(faucet_x, 100, faucet_y, 100);
        // B offers 25 Y, wants 25 X — exactly 25% of A's request
        let order_b = make_test_order(faucet_y, 25, faucet_x, 25);

        let group = Matcher::run(vec![order_a, order_b], faucet_x, faucet_y).expect("Should match");
        // A gets 25 Y (25% of 100 — meets threshold), B gets 25 X (100% — meets threshold)
        assert_eq!(group.side_a.len(), 1);
        assert_eq!(group.side_b.len(), 1);
        assert_eq!(group.side_a[0].fill_amount, 25);
        assert_eq!(group.side_b[0].fill_amount, 25);
    }

    #[test]
    fn test_no_match_same_side() {
        let faucet_x = make_faucet_id(1);
        let faucet_y = make_faucet_id(2);

        // Both orders offer X, want Y - no counterparty
        let order_a = make_test_order(faucet_x, 10, faucet_y, 100);
        let order_b = make_test_order(faucet_x, 20, faucet_y, 50);

        let result = Matcher::run(vec![order_a, order_b], faucet_x, faucet_y);
        assert!(result.is_none());
    }

    // ========== Test Infrastructure ==========

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;
    const MONTE_CARLO_ITERATIONS: usize = 200;

    fn make_random_order(
        rng: &mut impl Rng,
        offered_faucet: AccountId,
        requested_faucet: AccountId,
        lo: u64,
        hi: u64,
    ) -> Order {
        let offered_amount = rng.random_range(lo..=hi);
        let requested_amount = rng.random_range(lo..=hi);
        make_test_order(offered_faucet, offered_amount, requested_faucet, requested_amount)
    }

    fn make_random_orders(
        rng: &mut impl Rng,
        faucet_x: AccountId,
        faucet_y: AccountId,
        n_a: usize,
        n_b: usize,
        lo: u64,
        hi: u64,
    ) -> Vec<Order> {
        let mut orders = Vec::new();
        for _ in 0..n_a {
            orders.push(make_random_order(rng, faucet_x, faucet_y, lo, hi));
        }
        for _ in 0..n_b {
            orders.push(make_random_order(rng, faucet_y, faucet_x, lo, hi));
        }
        orders
    }

    fn assert_group_invariants(group: &MatchGroup) {
        for f in group.side_a.iter().chain(group.side_b.iter()) {
            assert!(f.fill_amount <= f.order.requested_amount);
            assert!(f.output_amount <= f.order.offered_amount);
            assert!(
                f.fill_amount as u128 * 100
                    >= f.order.requested_amount as u128 * 25,
                "Below 25% min fill: fill={} req={}",
                f.fill_amount, f.order.requested_amount
            );
        }
        // Group-level solvency
        assert!(
            group.total_x >= group.demand_x,
            "X solvency violated: total_x={} < demand_x={}",
            group.total_x, group.demand_x
        );
        assert!(
            group.total_y >= group.demand_y,
            "Y solvency violated: total_y={} < demand_y={}",
            group.total_y, group.demand_y
        );
    }

    // ========== A. Mathematical Invariants (Monte Carlo) ==========

    #[test]
    fn test_invariant_fill_never_exceeds_requested() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                for f in group.side_a.iter().chain(group.side_b.iter()) {
                    assert!(f.fill_amount <= f.order.requested_amount);
                }
            }
        }
    }

    #[test]
    fn test_invariant_output_never_exceeds_offered() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                for f in group.side_a.iter().chain(group.side_b.iter()) {
                    assert!(f.output_amount <= f.order.offered_amount);
                }
            }
        }
    }

    #[test]
    fn test_invariant_price_satisfaction() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                for f in group.side_a.iter().chain(group.side_b.iter()) {
                    if f.fill_amount > 0 {
                        let lhs = f.output_amount as u128 * f.order.requested_amount as u128;
                        let rhs = f.order.offered_amount as u128 * f.fill_amount as u128;
                        assert!(lhs <= rhs + 1, "Price violated");
                    }
                }
            }
        }
    }

    #[test]
    fn test_invariant_min_fill_ratio_respected() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                for f in group.side_a.iter().chain(group.side_b.iter()) {
                    assert!(
                        f.fill_amount as u128 * 100
                            >= f.order.requested_amount as u128 * 25,
                        "Below 25%: fill={} req={}",
                        f.fill_amount, f.order.requested_amount
                    );
                }
            }
        }
    }

    #[test]
    fn test_invariant_asset_conservation_global() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                let total_output_x: u128 = group.side_a.iter().map(|f| f.output_amount as u128).sum();
                let total_output_y: u128 = group.side_b.iter().map(|f| f.output_amount as u128).sum();
                let total_offered_x: u128 =
                    group.side_a.iter().map(|f| f.order.offered_amount as u128).sum();
                let total_offered_y: u128 =
                    group.side_b.iter().map(|f| f.order.offered_amount as u128).sum();
                assert!(total_output_x <= total_offered_x, "X conservation violated");
                assert!(total_output_y <= total_offered_y, "Y conservation violated");
            }
        }
    }

    #[test]
    fn test_invariant_deterministic_output() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..50 {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            let orders_clone = orders.clone();
            let g1 = Matcher::run(orders, fx, fy);
            let g2 = Matcher::run(orders_clone, fx, fy);
            match (&g1, &g2) {
                (None, None) => {}
                (Some(g1), Some(g2)) => {
                    assert_eq!(g1.side_a.len(), g2.side_a.len());
                    assert_eq!(g1.side_b.len(), g2.side_b.len());
                    assert_eq!(g1.total_x, g2.total_x);
                    assert_eq!(g1.total_y, g2.total_y);
                    for (a, b) in g1.side_a.iter().zip(g2.side_a.iter()) {
                        assert_eq!(a.fill_amount, b.fill_amount);
                        assert_eq!(a.output_amount, b.output_amount);
                    }
                    for (a, b) in g1.side_b.iter().zip(g2.side_b.iter()) {
                        assert_eq!(a.fill_amount, b.fill_amount);
                        assert_eq!(a.output_amount, b.output_amount);
                    }
                }
                _ => panic!("Determinism violated: one run returned Some, the other None"),
            }
        }
    }

    #[test]
    fn test_invariant_input_order_independence() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..50 {
            let na = rng.random_range(2..=8usize);
            let nb = rng.random_range(2..=8usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            let mut shuffled = orders.clone();
            for i in (1..shuffled.len()).rev() {
                let j = rng.random_range(0..=i);
                shuffled.swap(i, j);
            }
            let g1 = Matcher::run(orders, fx, fy);
            let g2 = Matcher::run(shuffled, fx, fy);
            let mut fills1: Vec<u64> = g1.iter()
                .flat_map(|g| g.side_a.iter().chain(g.side_b.iter()).map(|f| f.fill_amount))
                .collect();
            let mut fills2: Vec<u64> = g2.iter()
                .flat_map(|g| g.side_a.iter().chain(g.side_b.iter()).map(|f| f.fill_amount))
                .collect();
            fills1.sort();
            fills2.sort();
            assert_eq!(fills1, fills2, "Fill multisets differ after shuffle");
        }
    }

    #[test]
    fn test_invariant_group_solvency() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let na = rng.random_range(1..=10usize);
            let nb = rng.random_range(1..=10usize);
            let orders = make_random_orders(&mut rng, fx, fy, na, nb, 1, 10_000);
            if let Some(group) = Matcher::run(orders, fx, fy) {
                assert!(
                    group.total_x >= group.demand_x,
                    "X solvency violated: total_x={} < demand_x={}",
                    group.total_x, group.demand_x
                );
                assert!(
                    group.total_y >= group.demand_y,
                    "Y solvency violated: total_y={} < demand_y={}",
                    group.total_y, group.demand_y
                );
            }
        }
    }

    // ========== B. Edge Cases ==========

    #[test]
    fn test_edge_single_order_exact_match() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let a = make_test_order(fx, 100, fy, 100);
        let b = make_test_order(fy, 100, fx, 100);
        let group = Matcher::run(vec![a, b], fx, fy).expect("Should match");
        assert_eq!(group.side_a.len(), 1);
        assert_eq!(group.side_b.len(), 1);
        assert_eq!(group.side_a[0].fill_amount, 100);
        assert_eq!(group.side_b[0].fill_amount, 100);
        assert_eq!(group.side_a[0].output_amount, 100);
        assert_eq!(group.side_b[0].output_amount, 100);
    }

    #[test]
    fn test_edge_identical_prices_many_orders() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut orders = Vec::new();
        for _ in 0..5 {
            orders.push(make_test_order(fx, 100, fy, 100));
        }
        for _ in 0..5 {
            orders.push(make_test_order(fy, 100, fx, 100));
        }
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        assert_eq!(group.side_a.len(), 5);
        assert_eq!(group.side_b.len(), 5);
    }

    #[test]
    fn test_edge_many_to_one() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut orders = Vec::new();
        for _ in 0..10 {
            orders.push(make_test_order(fx, 10, fy, 10));
        }
        orders.push(make_test_order(fy, 100, fx, 100));
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        // Many-to-many: all filled orders from both sides
        assert_eq!(group.side_a.len(), 10);
        assert_eq!(group.side_b.len(), 1);
    }

    #[test]
    fn test_edge_one_to_many() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut orders = Vec::new();
        orders.push(make_test_order(fx, 100, fy, 100));
        for _ in 0..10 {
            orders.push(make_test_order(fy, 10, fx, 10));
        }
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        // Many-to-many: all filled orders from both sides
        assert_eq!(group.side_a.len(), 1);
        assert_eq!(group.side_b.len(), 10);
    }

    #[test]
    fn test_edge_amount_one() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let a = make_test_order(fx, 1, fy, 1);
        let b = make_test_order(fy, 1, fx, 1);
        let group = Matcher::run(vec![a, b], fx, fy).expect("Should match");
        assert_eq!(group.side_a[0].output_amount, 1);
        assert_eq!(group.side_b[0].output_amount, 1);
    }

    #[test]
    fn test_edge_large_amounts() {
        // Test calculate_output_amount near the overflow boundary.
        // Overflow occurs when offered * 100_000 > u64::MAX (~1.84e14 threshold).
        let safe_large = 100_000_000_000_000u64; // 1e14, safe
        let result = miden_swapp::PswapNote::calculate_output_amount(
            safe_large, safe_large, safe_large,
        );
        assert_eq!(result, safe_large);

        // Verify overflow at threshold
        let overflow_value = 184_467_440_737_096u64;
        let caught = std::panic::catch_unwind(|| {
            miden_swapp::PswapNote::calculate_output_amount(overflow_value, 1, 1)
        });
        assert!(caught.is_err(), "Expected overflow panic for value > ~1.84e14");
    }

    #[test]
    fn test_edge_zero_result_from_rounding() {
        // When offered << requested, output rounds to 0
        let result = miden_swapp::PswapNote::calculate_output_amount(1, 1_000_000, 1);
        assert_eq!(result, 0, "Tiny offered / large requested should round to 0");
    }

    // ========== C. Spread / Surplus Known-Answer Tests ==========

    #[test]
    fn test_spread_symmetric_no_surplus() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let a = make_test_order(fx, 100, fy, 100);
        let b = make_test_order(fy, 100, fx, 100);
        let group = Matcher::run(vec![a, b], fx, fy).expect("Should match");
        assert_eq!(group.surplus_x, 0);
        assert_eq!(group.surplus_y, 0);
    }

    #[test]
    fn test_spread_asymmetric_positive_surplus() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        // A offers 200 X wants 50 Y (willing to pay 4:1)
        let a = make_test_order(fx, 200, fy, 50);
        // B offers 200 Y wants 50 X (willing to pay 4:1)
        let b = make_test_order(fy, 200, fx, 50);
        let group = Matcher::run(vec![a, b], fx, fy).expect("Should match");
        assert_eq!(group.side_a[0].fill_amount, 50);
        assert_eq!(group.side_b[0].fill_amount, 50);
        assert!(group.surplus_x > 0, "Expected positive X surplus, got {}", group.surplus_x);
        assert!(group.surplus_y > 0, "Expected positive Y surplus, got {}", group.surplus_y);
    }

    #[test]
    fn test_spread_calculation_known_answer_2_to_1() {
        let result = miden_swapp::PswapNote::calculate_output_amount(200, 100, 50);
        assert_eq!(result, 100);
    }

    #[test]
    fn test_spread_calculation_known_answer_3_to_2() {
        let result = miden_swapp::PswapNote::calculate_output_amount(300, 200, 100);
        assert_eq!(result, 150);
    }

    #[test]
    fn test_spread_calculation_rounding_down() {
        let result = miden_swapp::PswapNote::calculate_output_amount(100, 300, 100);
        assert_eq!(result, 33);
    }

    #[test]
    fn test_spread_calculation_identity() {
        for &amount in &[1u64, 100, 9999, 1_000_000] {
            let result = miden_swapp::PswapNote::calculate_output_amount(amount, amount, amount);
            assert_eq!(result, amount, "Identity failed for amount {}", amount);
        }
    }

    // ========== D. Convergence Stress ==========

    #[test]
    fn test_convergence_stress_100_orders() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED);
        let orders = make_random_orders(&mut rng, fx, fy, 100, 100, 1, 10_000);
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        assert_group_invariants(&group);
    }

    #[test]
    fn test_convergence_terminates_adversarial() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut orders = Vec::new();
        // 20 A orders: offers 1 X, wants 1000 Y
        for _ in 0..20 {
            orders.push(make_test_order(fx, 1, fy, 1000));
        }
        // 20 B orders: offers 1 Y, wants 1000 X
        for _ in 0..20 {
            orders.push(make_test_order(fy, 1, fx, 1000));
        }
        // Should terminate; fills would be far below 25% threshold
        let result = Matcher::run(orders, fx, fy);
        assert!(result.is_none(), "Expected no match for adversarial prices");
    }

    #[test]
    fn test_convergence_unbalanced_sides() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut rng = StdRng::seed_from_u64(SEED + 1);
        let orders = make_random_orders(&mut rng, fx, fy, 50, 3, 1, 10_000);
        if let Some(group) = Matcher::run(orders, fx, fy) {
            // B side limited to at most 3
            assert!(group.side_b.len() <= 3);
            assert_group_invariants(&group);
        }
    }

    // ========== E. calculate_output_amount Rounding ==========

    #[test]
    fn test_calculate_output_full_fill_returns_offered() {
        let calc = miden_swapp::PswapNote::calculate_output_amount;
        // Case 1: offered > requested
        assert_eq!(calc(200, 100, 100), 200);
        // Case 2: offered < requested
        assert_eq!(calc(100, 200, 200), 100);
        // Equal
        assert_eq!(calc(500, 500, 500), 500);
    }

    #[test]
    fn test_calculate_output_half_fill() {
        let calc = miden_swapp::PswapNote::calculate_output_amount;
        assert_eq!(calc(200, 100, 50), 100);
        assert_eq!(calc(100, 200, 100), 50);
    }

    #[test]
    fn test_calculate_output_precision_loss_boundary() {
        let calc = miden_swapp::PswapNote::calculate_output_amount;
        // Case 1 (offered > requested): 7/3 ratio with full fill
        // Ideal: 7, actual: 6 due to double integer division truncation
        assert_eq!(calc(7, 3, 3), 6);
        // Case 2 (offered <= requested): 3/7 ratio with full fill
        // No precision loss here — Case 2 division order preserves accuracy
        assert_eq!(calc(3, 7, 7), 3);
    }

    #[test]
    fn test_calculate_output_monte_carlo_proportionality() {
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..MONTE_CARLO_ITERATIONS {
            let offered = rng.random_range(1..=1_000_000u64);
            let requested = rng.random_range(1..=1_000_000u64);
            let fill = rng.random_range(1..=requested);
            let result = miden_swapp::PswapNote::calculate_output_amount(offered, requested, fill);
            let ideal = (fill as f64) * (offered as f64) / (requested as f64);
            // Tolerance accounts for PRECISION_FACTOR (100k) truncation
            let tolerance = (fill as f64) / 100_000.0 + 2.0;
            assert!(
                (result as f64 - ideal).abs() <= tolerance,
                "result={} ideal={:.2} tol={:.2} (offered={}, requested={}, fill={})",
                result, ideal, tolerance, offered, requested, fill
            );
        }
    }

    #[test]
    fn test_calculate_output_safe_range() {
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..500 {
            let offered = rng.random_range(1..=10_000_000u64);
            let requested = rng.random_range(1..=10_000_000u64);
            let fill = rng.random_range(1..=requested);
            let result = miden_swapp::PswapNote::calculate_output_amount(offered, requested, fill);
            assert!(
                result <= offered,
                "Output {} > offered {} (requested={}, fill={})",
                result, offered, requested, fill
            );
        }
    }

    // ========== F. Robustness / Defensive ==========

    #[test]
    fn test_robustness_single_order_below_min_fill() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        // A offers 10 X, wants 1000 Y. B offers 10 Y, wants 10 X.
        // A's fill will be 10 (1% of 1000) → below 25% threshold
        let a = make_test_order(fx, 10, fy, 1000);
        let b = make_test_order(fy, 10, fx, 10);
        let result = Matcher::run(vec![a, b], fx, fy);
        assert!(result.is_none());
    }

    #[test]
    fn test_robustness_all_same_price() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let n = 8;
        let mut orders = Vec::new();
        for _ in 0..n {
            orders.push(make_test_order(fx, 50, fy, 50));
        }
        for _ in 0..n {
            orders.push(make_test_order(fy, 50, fx, 50));
        }
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        assert_eq!(group.side_a.len(), n);
        assert_eq!(group.side_b.len(), n);
    }

    #[test]
    fn test_robustness_duplicate_orders() {
        let fx = make_faucet_id(1);
        let fy = make_faucet_id(2);
        let mut orders = Vec::new();
        for _ in 0..3 {
            orders.push(make_test_order(fx, 100, fy, 100));
        }
        for _ in 0..3 {
            orders.push(make_test_order(fy, 100, fx, 100));
        }
        let group = Matcher::run(orders, fx, fy).expect("Should match");
        assert_eq!(group.side_a.len(), 3);
        assert_eq!(group.side_b.len(), 3);
    }

    // ========== G. binary_search_max Extended ==========

    #[test]
    fn test_binary_search_empty_prefix() {
        assert_eq!(Matcher::binary_search_max(&[], 10), 0);
    }

    #[test]
    fn test_binary_search_single_element() {
        assert_eq!(Matcher::binary_search_max(&[5], 5), 0);
        assert_eq!(Matcher::binary_search_max(&[5], 10), 0);
        assert_eq!(Matcher::binary_search_max(&[5], 4), 0);
    }

    #[test]
    fn test_binary_search_all_equal() {
        assert_eq!(Matcher::binary_search_max(&[10, 10, 10], 10), 2);
    }

    #[test]
    fn test_binary_search_monte_carlo() {
        let mut rng = StdRng::seed_from_u64(SEED);
        for _ in 0..500 {
            let len = rng.random_range(1..=50usize);
            let mut prefix = Vec::with_capacity(len);
            let mut sum = 0u128;
            for _ in 0..len {
                sum += rng.random_range(1..=100u128);
                prefix.push(sum);
            }
            let available = rng.random_range(0..=sum + 50);
            let bs_result = Matcher::binary_search_max(&prefix, available);
            // Linear scan for expected result
            if prefix[0] > available {
                assert_eq!(bs_result, 0);
            } else {
                let mut linear_result = 0;
                for i in 0..prefix.len() {
                    if prefix[i] <= available {
                        linear_result = i;
                    }
                }
                assert_eq!(
                    bs_result, linear_result,
                    "bs={} linear={} available={}",
                    bs_result, linear_result, available
                );
            }
        }
    }
}

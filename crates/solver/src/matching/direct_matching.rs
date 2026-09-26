use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::HashSet;

/// Collect all token pairs where orders exist in both directions.
fn collect_matchable_pairs<F: PriceFeed>(book: &OrderBook<F>) -> Vec<(TokenId, TokenId)> {
    let mut pairs = HashSet::new();
    let all_tokens: Vec<TokenId> = book.tokens.iter().copied().collect();

    for &token_a in &all_tokens {
        for &token_b in &all_tokens {
            if token_a >= token_b {
                continue;
            }
            let has_ab = book.has_orders(token_a, token_b);
            let has_ba = book.has_orders(token_b, token_a);

            if has_ab && has_ba {
                pairs.insert((token_a, token_b));
            }
        }
    }

    pairs.into_iter().collect()
}

/// Direct matching: greedy on BTreeMap.
/// Inserts filled order IDs into the provided set. Returns number of cycles executed.
pub fn run_direct_matching<F: PriceFeed>(book: &mut OrderBook<F>, filled_orders: &mut HashSet<OrderId>) -> u64 {
    let mut cycles_executed = 0u64;

    let pairs = collect_matchable_pairs(book);
    for (token_a, token_b) in pairs {
        cycles_executed += match_user_orders(book, token_a, token_b, filled_orders);
    }

    cycles_executed
}

/// Most re-plans per pair per tick before [`plan_pair`] falls back to a
/// per-match walk. With the hopeless skip, re-plans only resolve cascades.
pub(crate) const MAX_PAIR_RETRIES: usize = 8;

/// User-to-user matching for a pair. Returns number of cycles executed.
///
/// PSWAP (Miden 0.16) rejects a consumption whose total fill is below
/// `min(min_fill_step, requested)`, and the tick settles as one transaction,
/// so one sub-floor note would fail the whole batch. The pair is therefore
/// planned on clones by [`plan_pair`] and committed only once every touched
/// order is completely filled or at/above its floor.
fn match_user_orders<F: PriceFeed>(
    book: &mut OrderBook<F>,
    token_a: TokenId,
    token_b: TokenId,
    filled_orders: &mut HashSet<OrderId>,
) -> u64 {
    // Audit C2: the direct path must consult the price feed, exactly
    // like the triangular path does. Triangular's gate is "every leg's
    // token is priced, else skip the cycle" (price_feed.rs doctrine: a
    // missing price means "not matchable", never a bogus default); its
    // USD value-safety is the cycle surplus ≥ 0. The direct analogue:
    // require BOTH tokens of this pair to be priced here, and rely on
    // `match_with`'s per-token surplus logic for the value-safety of the
    // executed amounts — that non-loss invariant is exactly what the
    // 10k-trial settlement-solvency test proves, and is the direct
    // counterpart of triangular's surplus ≥ 0. (Gating each order on
    // `is_order_profitable` instead is wrong: it rejects normal spread
    // orders where one side asks more USD than it offers.)
    if book.feed.price_cents(token_a).is_none() || book.feed.price_cents(token_b).is_none() {
        return 0;
    }

    // Both sides in `best_order` order: ascending rate, FIFO within a rate.
    let a_side = book.notes_for_pair(token_a, token_b);
    let b_side = book.notes_for_pair(token_b, token_a);
    let floors_a: Vec<Amount> = a_side.iter().map(|o| book.min_fill_floor(o)).collect();
    let floors_b: Vec<Amount> = b_side.iter().map(|o| book.min_fill_floor(o)).collect();

    let plan = plan_pair(a_side, b_side, &floors_a, &floors_b, MAX_PAIR_RETRIES);
    let cycles = plan.cycles;
    if cycles == 0 {
        return 0;
    }
    let surplus = [(token_a, plan.surplus_a), (token_b, plan.surplus_b)];
    let touched = plan.into_touched();
    filled_orders.extend(touched.iter().map(|o| o.id));
    book.apply_pair_plan(touched, surplus);
    cycles
}

/// One pair's matching, computed on clones of its orders.
pub(crate) struct PairPlan {
    a: Vec<Order>,
    b: Vec<Order>,
    /// Orders matched in this plan. Tracked per walk rather than inferred from
    /// `requested_filled`, so an order never trades unless it was matched here.
    touched_a: Vec<bool>,
    touched_b: Vec<bool>,
    /// Surplus in the a-side's offered token and in the b-side's offered token.
    pub(crate) surplus_a: Amount,
    pub(crate) surplus_b: Amount,
    pub(crate) cycles: u64,
}

impl PairPlan {
    fn empty() -> Self {
        Self {
            a: Vec::new(),
            b: Vec::new(),
            touched_a: Vec::new(),
            touched_b: Vec::new(),
            surplus_a: 0,
            surplus_b: 0,
            cycles: 0,
        }
    }

    /// Orders this plan filled (fully or partially), in their planned state.
    pub(crate) fn into_touched(self) -> Vec<Order> {
        touched_only(self.a, self.touched_a).chain(touched_only(self.b, self.touched_b)).collect()
    }

    /// Indices of touched orders left partly filled below their floor.
    fn short_of_floor(&self, floors_a: &[Amount], floors_b: &[Amount]) -> (Vec<usize>, Vec<usize>) {
        (
            short_of_floor(&self.a, &self.touched_a, floors_a),
            short_of_floor(&self.b, &self.touched_b, floors_b),
        )
    }
}

/// Plans one pair: today's greedy walk (best order on each side, `match_with`
/// on copies, stop at the first `None`) plus two floor rules.
///
/// - **Hopeless skip.** When an order first reaches the top of its side, the
///   most it can receive is every open counter order that crosses it (it is
///   first in line). If even that is below its floor, it sits the tick out.
/// - **Re-plan.** After a walk, orders left partly filled below their floor
///   are excluded and the walk re-run: their fill may have counted on counter
///   liquidity that a later skip removed (a cascade). Re-plan exclusions carry
///   over; hopeless skips are recomputed each walk, because excluding an order
///   frees liquidity for others.
///
/// After `max_retries` re-plans, one [`walk`] in per-match mode, sound by
/// construction. Every returned plan leaves each touched order completely
/// filled or at/above its floor; with every floor `0` the first walk is
/// returned unchanged, identical to the pre-floor loop.
pub(crate) fn plan_pair(
    a_side: Vec<Order>,
    b_side: Vec<Order>,
    floors_a: &[Amount],
    floors_b: &[Amount],
    max_retries: usize,
) -> PairPlan {
    let mut excluded_a = vec![false; a_side.len()];
    let mut excluded_b = vec![false; b_side.len()];
    for _ in 0..=max_retries {
        let plan = walk(
            a_side.clone(),
            b_side.clone(),
            floors_a,
            floors_b,
            excluded_a.clone(),
            excluded_b.clone(),
            false,
        );
        let (bad_a, bad_b) = plan.short_of_floor(floors_a, floors_b);
        if bad_a.is_empty() && bad_b.is_empty() {
            return plan;
        }
        bad_a.into_iter().for_each(|i| excluded_a[i] = true);
        bad_b.into_iter().for_each(|j| excluded_b[j] = true);
    }

    let (n_a, n_b) = (a_side.len(), b_side.len());
    let plan = walk(a_side, b_side, floors_a, floors_b, vec![false; n_a], vec![false; n_b], true);
    let (bad_a, bad_b) = plan.short_of_floor(floors_a, floors_b);
    if !bad_a.is_empty() || !bad_b.is_empty() {
        // Unreachable: per-match mode applies a match only if it leaves both
        // orders at/above their floor or complete. Never risk a failing batch.
        tracing::error!("per-match fallback left a sub-floor fill; matching nothing on this pair");
        return PairPlan::empty();
    }
    plan
}

/// One greedy pass over a pair; `excluded` orders never trade.
///
/// In per-match mode each match is checked on the copies *before* it is
/// applied: it goes ahead only if both orders end completely filled or at/above
/// their floor, else the failing order is skipped. Only an untouched order can
/// fail that check — a touched one is already complete or at/above its floor,
/// and fills only grow — so a skip never leaves a sub-floor partial behind.
fn walk(
    mut a: Vec<Order>,
    mut b: Vec<Order>,
    floors_a: &[Amount],
    floors_b: &[Amount],
    mut excluded_a: Vec<bool>,
    mut excluded_b: Vec<bool>,
    per_match: bool,
) -> PairPlan {
    let mut touched_a = vec![false; a.len()];
    let mut touched_b = vec![false; b.len()];
    let (mut surplus_a, mut surplus_b, mut cycles): (Amount, Amount, u64) = (0, 0, 0);
    let (mut cursor_a, mut cursor_b) = (0, 0);

    // Terminates on its own (every applied match strictly lowers the counter's
    // `requested_remaining`; every skip excludes an order), but rounding can
    // leave both orders partial, so matches aren't bounded by the order count.
    // The cap turns a pathological spin into "match nothing on this pair".
    let max_iters = 4 * (a.len() + b.len()) + 16;
    let mut iters = 0;
    loop {
        iters += 1;
        if iters > max_iters {
            tracing::error!(
                orders = a.len() + b.len(),
                "direct-matching walk hit its iteration cap; matching nothing on this pair"
            );
            return PairPlan::empty();
        }
        let Some(i) = next_open(&a, &excluded_a, &mut cursor_a) else { break };
        let Some(j) = next_open(&b, &excluded_b, &mut cursor_b) else { break };

        // Hopeless skip, for orders not yet matched this walk. With a floor of
        // 0 the right-hand side is 0, so this never fires (pre-floor behaviour).
        if !touched_a[i] && max_receivable(&a[i], &b, &excluded_b) < floor_left(&a[i], floors_a[i]) {
            excluded_a[i] = true;
            continue;
        }
        if !touched_b[j] && max_receivable(&b[j], &a, &excluded_a) < floor_left(&b[j], floors_b[j]) {
            excluded_b[j] = true;
            continue;
        }

        // `match_with` can mutate both orders and then return `None`, so match
        // on copies and write back only on `Some` (as `apply_match` does).
        let (mut x, mut y) = (a[i].clone(), b[j].clone());
        let Some(result) = x.match_with(&mut y) else { break };

        if per_match {
            let (x_ok, y_ok) = (meets_floor(&x, floors_a[i]), meets_floor(&y, floors_b[j]));
            if !(x_ok && y_ok) {
                debug_assert!(x_ok || !touched_a[i], "per-match walk would skip a touched order");
                debug_assert!(y_ok || !touched_b[j], "per-match walk would skip a touched order");
                excluded_a[i] |= !x_ok;
                excluded_b[j] |= !y_ok;
                continue;
            }
        }

        a[i] = x;
        b[j] = y;
        touched_a[i] = true;
        touched_b[j] = true;
        surplus_a = surplus_a.saturating_add(result.surplus_offered);
        surplus_b = surplus_b.saturating_add(result.surplus_requested);
        cycles += 1;
    }

    PairPlan { a, b, touched_a, touched_b, surplus_a, surplus_b, cycles }
}

/// First order at or after `cursor` that is still active and not excluded.
/// Orders before the cursor are complete or excluded, and neither reopens
/// within a walk, so the cursor only moves forward.
fn next_open(side: &[Order], excluded: &[bool], cursor: &mut usize) -> Option<usize> {
    while *cursor < side.len() && (excluded[*cursor] || !side[*cursor].is_active()) {
        *cursor += 1;
    }
    (*cursor < side.len()).then_some(*cursor)
}

/// The most `order` can receive this tick: the remaining offered side of every
/// open counter order that crosses it. An upper bound — `order` is first in
/// line on its side, and a counter never releases more than it has left.
fn max_receivable(order: &Order, counters: &[Order], excluded: &[bool]) -> Amount {
    counters
        .iter()
        .zip(excluded)
        .filter(|(c, &ex)| !ex && c.is_active() && order.is_profitable_with(c))
        .fold(0, |sum: Amount, (c, _)| sum.saturating_add(c.offered_remaining()))
}

/// What `order` still needs this tick to reach `floor`.
fn floor_left(order: &Order, floor: Amount) -> Amount {
    floor.saturating_sub(order.requested_filled())
}

/// Whether the script accepts `order`'s total fill: complete, or at/above `floor`.
fn meets_floor(order: &Order, floor: Amount) -> bool {
    order.requested_remaining == 0 || order.requested_filled() >= floor
}

fn short_of_floor(side: &[Order], touched: &[bool], floors: &[Amount]) -> Vec<usize> {
    (0..side.len()).filter(|&k| touched[k] && !meets_floor(&side[k], floors[k])).collect()
}

fn touched_only(side: Vec<Order>, touched: Vec<bool>) -> impl Iterator<Item = Order> {
    side.into_iter().zip(touched).filter_map(|(o, t)| t.then_some(o))
}

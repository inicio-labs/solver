//! Minimum-fill (PSWAP `min_fill_step`) handling in direct matching: the planner
//! never commits a partial fill below an order's floor, never stalls a pair on an
//! order whose floor can't be met, and still lets several counters add up to one
//! order's fill (the chain checks each note's total fill per consumption).

use proptest::prelude::*;
use std::collections::HashSet;

use super::{eth, make_note_id, usdc, NoteIdGen};
use crate::matching::direct_matching::{plan_pair, run_direct_matching, MAX_PAIR_RETRIES};
use crate::matching::order_book::OrderBook;
use crate::matching::types::{Amount, Order, OrderId, TokenId};
use crate::price::WatchPriceFeed;

fn feed() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed
}

/// A fresh order (nothing filled yet), for driving `plan_pair` directly.
fn order(id: OrderId, offered_token: TokenId, requested_token: TokenId, offered: Amount, requested: Amount) -> Order {
    Order { id, offered_token, requested_token, offered, requested, requested_remaining: requested }
}

fn filled(book: &OrderBook<WatchPriceFeed>, id: OrderId) -> Amount {
    book.orders.get(&id).map_or(0, |o| o.requested_filled())
}

fn is_complete(book: &OrderBook<WatchPriceFeed>, id: OrderId) -> bool {
    book.orders.get(&id).is_some_and(|o| o.requested_remaining == 0)
}

/// Y is the best-priced order but its floor (500) exceeds all the crossing counter
/// liquidity (100): it is skipped in the same pass and the worse-priced X fills.
/// A loop that stopped at the first rejection would stall the pair on Y.
#[test]
fn hopeless_top_order_is_skipped_and_the_next_one_fills() {
    let mut book = OrderBook::new(feed());
    let mut ids = NoteIdGen::new();
    let (y, x, b1) = (ids.next(), ids.next(), ids.next());
    book.add_user_order_with_min_fill(y, usdc(), eth(), 10_000, 1_000, 500);
    book.add_user_order_with_min_fill(x, usdc(), eth(), 1_000, 110, 0);
    book.add_user_order_with_min_fill(b1, eth(), usdc(), 100, 900, 0);

    let mut touched = HashSet::new();
    run_direct_matching(&mut book, &mut touched);

    assert_eq!(filled(&book, y), 0, "Y can't reach its floor, so it must not trade");
    assert!(!touched.contains(&y));
    assert_eq!(filled(&book, x), 100, "X takes the liquidity Y can't use");
    assert!(is_complete(&book, b1));
}

/// Y (floor 100) is filled 50 + 50 by two counters in one tick: one on-chain
/// consumption whose total fill is exactly at the floor, which the script accepts.
#[test]
fn two_counters_add_up_to_reach_the_floor() {
    let mut book = OrderBook::new(feed());
    let mut ids = NoteIdGen::new();
    let (y, b1, b2) = (ids.next(), ids.next(), ids.next());
    book.add_user_order_with_min_fill(y, usdc(), eth(), 10_000, 1_000, 100);
    book.add_user_order_with_min_fill(b1, eth(), usdc(), 50, 400, 0);
    book.add_user_order_with_min_fill(b2, eth(), usdc(), 50, 400, 0);

    let mut touched = HashSet::new();
    run_direct_matching(&mut book, &mut touched);

    assert_eq!(filled(&book, y), 100);
    assert!(is_complete(&book, b1) && is_complete(&book, b2));
    assert_eq!(touched.len(), 3);
}

/// With only 50 + 40 available, Y (floor 100) can't be satisfied: it sits out and
/// the next order on its side, X, takes both counters.
#[test]
fn short_liquidity_goes_to_the_next_order() {
    let mut book = OrderBook::new(feed());
    let mut ids = NoteIdGen::new();
    let (y, x, b1, b2) = (ids.next(), ids.next(), ids.next(), ids.next());
    book.add_user_order_with_min_fill(y, usdc(), eth(), 10_000, 1_000, 100);
    book.add_user_order_with_min_fill(x, usdc(), eth(), 1_000, 110, 0);
    book.add_user_order_with_min_fill(b1, eth(), usdc(), 50, 400, 0);
    book.add_user_order_with_min_fill(b2, eth(), usdc(), 40, 320, 0);

    let mut touched = HashSet::new();
    run_direct_matching(&mut book, &mut touched);

    assert_eq!(filled(&book, y), 0);
    assert_eq!(filled(&book, x), 90);
    assert!(is_complete(&book, b1) && is_complete(&book, b2));
}

/// An order smaller than its step can only be taken whole, and a whole fill is
/// always accepted even though it is below `min_fill_step`.
#[test]
fn a_completing_fill_below_the_step_is_allowed() {
    let mut book = OrderBook::new(feed());
    let mut ids = NoteIdGen::new();
    let (y, b1) = (ids.next(), ids.next());
    book.add_user_order_with_min_fill(y, usdc(), eth(), 300, 30, 100);
    book.add_user_order_with_min_fill(b1, eth(), usdc(), 50, 400, 0);

    let mut touched = HashSet::new();
    run_direct_matching(&mut book, &mut touched);

    assert!(is_complete(&book, y));
}

/// A step above the order size clamps the floor to the full size, so only a
/// complete fill is allowed; with too little liquidity the order sits out.
#[test]
fn a_step_above_the_order_size_allows_only_a_full_fill() {
    let mut book = OrderBook::new(feed());
    let mut ids = NoteIdGen::new();
    let (y, b1) = (ids.next(), ids.next());
    book.add_user_order_with_min_fill(y, usdc(), eth(), 1_000, 100, 500);
    book.add_user_order_with_min_fill(b1, eth(), usdc(), 50, 400, 0);

    let mut touched = HashSet::new();
    run_direct_matching(&mut book, &mut touched);

    assert!(touched.is_empty());
    assert_eq!(filled(&book, y), 0);
    assert_eq!(filled(&book, b1), 0);
}

/// Vaibhav's case: ten big orders on top, and after the first fills, what's left is
/// below every other one's floor. They are skipped in the same pass and X fills.
/// Run with no re-plans allowed: had the first walk broken a floor, the per-match
/// fallback would have taken over and skipped Y1 as well.
#[test]
fn stacked_big_orders_are_skipped_in_one_pass() {
    let mut ids = NoteIdGen::new();
    let y1 = order(ids.next(), usdc(), eth(), 12_000, 1_200);
    let ys: Vec<Order> = (0..9).map(|_| order(ids.next(), usdc(), eth(), 50_000, 5_000)).collect();
    let x = order(ids.next(), usdc(), eth(), 3_000, 310);
    let bs: Vec<Order> = (0..3).map(|_| order(ids.next(), eth(), usdc(), 500, 4_500)).collect();

    let mut a_side = vec![y1.clone()];
    a_side.extend(ys.iter().cloned());
    a_side.push(x.clone());
    let floors_a: Vec<Amount> = a_side.iter().map(|o| if o.id == x.id { 0 } else { 1_000 }).collect();
    let floors_b = vec![0; bs.len()];

    let touched = plan_pair(a_side, bs.clone(), &floors_a, &floors_b, 0).into_touched();
    let get = |id: OrderId| touched.iter().find(|o| o.id == id);

    assert!(get(y1.id).is_some_and(|o| o.requested_remaining == 0), "Y1 fills completely");
    assert!(ys.iter().all(|y| get(y.id).is_none()), "Y2..Y10 sit out");
    assert!(get(x.id).is_some_and(|o| o.requested_filled() > 0), "X takes what's left");
    assert!(bs.iter().all(|b| get(b.id).is_some_and(|o| o.requested_remaining == 0)));
}

/// A cascade: b1 passes its own hopeless check (it counts Z's liquidity), Y fills
/// it only partly, then Z turns out to be hopeless, so b1 ends below its floor.
/// The re-plan excludes b1; nothing else can trade, so the pair matches nothing
/// rather than committing a partial the chain would reject.
#[test]
fn a_cascade_is_resolved_by_a_replan() {
    let mut ids = NoteIdGen::new();
    let y = order(ids.next(), usdc(), eth(), 500, 50);
    let z = order(ids.next(), usdc(), eth(), 10_000, 1_050);
    let b1 = order(ids.next(), eth(), usdc(), 100, 900);

    let plan = plan_pair(vec![y, z], vec![b1], &[0, 1_000], &[800], MAX_PAIR_RETRIES);

    assert_eq!(plan.cycles, 0);
    assert!(plan.into_touched().is_empty());
}

/// With no re-plans allowed, the same cascade reaches the per-match fallback. It
/// refuses the Y–b1 match (it would leave b1 below its floor) but still settles
/// what is safe: Y against b2.
#[test]
fn the_fallback_skips_only_what_it_cannot_leave_at_its_floor() {
    let mut ids = NoteIdGen::new();
    let y = order(ids.next(), usdc(), eth(), 500, 50);
    let z = order(ids.next(), usdc(), eth(), 10_000, 1_050);
    let b1 = order(ids.next(), eth(), usdc(), 100, 900);
    let b2 = order(ids.next(), eth(), usdc(), 20, 190);
    let (y_id, b1_id, b2_id) = (y.id, b1.id, b2.id);

    let plan = plan_pair(vec![y, z], vec![b1, b2], &[0, 1_000], &[800, 0], 0);
    assert_eq!(plan.cycles, 1);
    let touched = plan.into_touched();
    let get = |id: OrderId| touched.iter().find(|o| o.id == id);

    assert_eq!(get(y_id).map(|o| o.requested_filled()), Some(20));
    assert!(get(b2_id).is_some_and(|o| o.requested_remaining == 0));
    assert!(get(b1_id).is_none(), "b1 must not trade below its floor");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    /// Random two-token books with random steps: after direct matching every
    /// touched order is complete or at/above its floor, untouched orders are
    /// unchanged, nothing is over-filled, and the solver's net flow — computed the
    /// way the chain settles each note (`offered_for(total fill)`) — is
    /// non-negative in both tokens.
    #[test]
    fn floors_hold_and_the_solver_never_loses(
        specs in prop::collection::vec(
            (any::<bool>(), 1u64..=100_000, 1u64..=100_000, prop_oneof![Just(0u64), 1u64..=150_000]),
            1..24,
        ),
    ) {
        let mut book = OrderBook::new(feed());
        let mut placed = Vec::new();
        for (k, (sells_usdc, offered, requested, step)) in specs.into_iter().enumerate() {
            let id = make_note_id(10_000 + k as u64);
            let (off, req) = if sells_usdc { (usdc(), eth()) } else { (eth(), usdc()) };
            book.add_user_order_with_min_fill(id, off, req, offered, requested, step);
            placed.push((id, off, requested, step));
        }

        let mut touched = HashSet::new();
        run_direct_matching(&mut book, &mut touched);

        let (mut net_eth, mut net_usdc) = (0i128, 0i128);
        for (id, off, requested, step) in &placed {
            let o = &book.orders[id];
            prop_assert!(o.requested_remaining <= *requested);
            if !touched.contains(id) {
                prop_assert_eq!(o.requested_remaining, *requested, "an untouched order was filled");
                continue;
            }
            let floor = (*step).min(*requested);
            prop_assert!(
                o.requested_remaining == 0 || o.requested_filled() >= floor,
                "sub-floor partial: filled {} of {}, floor {}", o.requested_filled(), requested, floor
            );
            let released = o.offered_for(o.requested_filled()) as i128;
            let received = o.requested_filled() as i128;
            if *off == eth() {
                net_eth += released;
                net_usdc -= received;
            } else {
                net_usdc += released;
                net_eth -= received;
            }
        }
        prop_assert!(net_eth >= 0 && net_usdc >= 0, "solver loses: eth {}, usdc {}", net_eth, net_usdc);
    }
}

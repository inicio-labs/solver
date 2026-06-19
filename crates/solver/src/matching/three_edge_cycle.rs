use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

// ── Data Structures ─────────────────────────────────────────────────────────

/// Canonical triangle: rotated so tri.0 is the minimum token.
/// Cycle direction is always tri.0 → tri.1 → tri.2 → tri.0.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Triangle(TokenId, TokenId, TokenId);

impl Triangle {
    fn canonical(a: TokenId, b: TokenId, c: TokenId) -> Self {
        if a <= b && a <= c {
            Triangle(a, b, c)
        } else if b <= a && b <= c {
            Triangle(b, c, a)
        } else {
            Triangle(c, a, b)
        }
    }

    fn token_id_a(&self) -> TokenId {
        self.0
    }
    fn token_id_b(&self) -> TokenId {
        self.1
    }
    fn token_id_c(&self) -> TokenId {
        self.2
    }

    fn pairs(&self) -> [(TokenId, TokenId); 3] {
        [(self.0, self.1), (self.1, self.2), (self.2, self.0)]
    }
}

/// Heap entry: a triangle with its best orders and absolute surplus.
#[derive(Clone, Debug)]
struct CycleEntry {
    triangle: Triangle,
    order_ab: OrderId,
    order_bc: OrderId,
    order_ca: OrderId,
    surplus_usd: u128,
}

impl PartialEq for CycleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.surplus_usd == other.surplus_usd
    }
}
impl Eq for CycleEntry {}
impl PartialOrd for CycleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CycleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.surplus_usd.cmp(&other.surplus_usd)
    }
}

enum BottleneckLeg {
    AB,
    BC,
    CA,
}

struct FillResult {
    fill_a: Amount,
    fill_b: Amount,
    fill_c: Amount,
    surplus_a: Amount,
    surplus_b: Amount,
    surplus_c: Amount,
}

/// Combined simulation result: fill amounts, surplus per token, and total USD surplus.
struct CycleSimulation {
    fills: FillResult,
    surplus_usd: u128,
}

// ── Phase 1: Build ──────────────────────────────────────────────────────────

/// Enumerate all profitable triangles, compute surplus, return heap entries.
/// Also populates the triangles_by_pair reverse index.
fn enumerate_triangles<F: PriceFeed>(
    book: &mut OrderBook<F>,
    triangles_by_pair: &mut HashMap<(TokenId, TokenId), Vec<Triangle>>,
) -> Vec<CycleEntry> {
    let mut seen = HashSet::<Triangle>::new();
    let mut entries = Vec::new();
    let all_tokens: Vec<TokenId> = book.tokens.iter().copied().collect();

    for &token_b in &all_tokens {
        let incoming = book.incoming_neighbors(token_b);
        let outgoing = book.neighbors(token_b);

        for &token_a in &incoming {
            for &token_c in &outgoing {
                if token_c == token_a || token_c == token_b {
                    continue;
                }
                if !book.has_orders(token_c, token_a) {
                    continue;
                }

                let triangle = Triangle::canonical(token_a, token_b, token_c);
                if !seen.insert(triangle) {
                    continue;
                }

                if let Some(entry) = try_build_entry(book, triangle) {
                    for pair in triangle.pairs() {
                        triangles_by_pair.entry(pair).or_default().push(triangle);
                    }
                    entries.push(entry);
                }
            }
        }
    }

    entries
}

/// Try to build a CycleEntry for a triangle using current best orders.
fn try_build_entry<F: PriceFeed>(
    book: &mut OrderBook<F>,
    triangle: Triangle,
) -> Option<CycleEntry> {
    let id_ab = book.best_order(triangle.0, triangle.1)?.id;
    let id_bc = book.best_order(triangle.1, triangle.2)?.id;
    let id_ca = book.best_order(triangle.2, triangle.0)?.id;

    let order_ab = &book.orders[&id_ab];
    let order_bc = &book.orders[&id_bc];
    let order_ca = &book.orders[&id_ca];

    // Profitability check (u128 — no overflow)
    let lhs = order_ab.offered as u128 * order_bc.offered as u128 * order_ca.offered as u128;
    let rhs =
        order_ab.requested as u128 * order_bc.requested as u128 * order_ca.requested as u128;
    if lhs <= rhs {
        return None;
    }

    let simulation = simulate_full_cycle(book, id_ab, id_bc, id_ca)?;
    if simulation.surplus_usd == 0 {
        return None;
    }

    Some(CycleEntry {
        triangle,
        order_ab: id_ab,
        order_bc: id_bc,
        order_ca: id_ca,
        surplus_usd: simulation.surplus_usd,
    })
}

// ── Surplus & Fill Simulation ─────────────────────────────────────────────

/// Fill a single leg: consume `input` tokens, capped by the order's remaining capacity.
/// Returns (filled_amount, released_amount).
fn fill_leg(order: &Order, input: Amount) -> (Amount, Amount) {
    let filled = input.min(order.requested_remaining);
    let released = order.offered_for(filled);
    (filled, released)
}

/// Simulate fills through three legs in cycle order, starting from the bottleneck.
/// Leg 0 is the bottleneck and is always fully consumed (fill = requested_remaining).
/// Returns [(filled, released); 3] per leg, or None if any leg fills zero.
fn simulate_cycle_fills(legs: [&Order; 3]) -> Option<[(Amount, Amount); 3]> {
    // Leg 0 is the bottleneck — always fully fill it.
    let filled_0 = legs[0].requested_remaining;
    let released_0 = legs[0].offered_for(filled_0);
    let (filled_1, released_1) = fill_leg(legs[1], released_0);
    let (filled_2, released_2) = fill_leg(legs[2], released_1);

    if filled_0 == 0 || filled_1 == 0 || filled_2 == 0 {
        return None;
    }

    Some([
        (filled_0, released_0),
        (filled_1, released_1),
        (filled_2, released_2),
    ])
}

/// Simulate fill propagation for a cycle and compute per-token surplus.
///
/// Rotates legs so the bottleneck is first, propagates fills through the cycle,
/// then maps results back to per-token (released, fill) pairs.
///   Token A: released by order AB, consumed by order CA
///   Token B: released by order BC, consumed by order AB
///   Token C: released by order CA, consumed by order BC
fn simulate_cycle(
    order_ab: &Order,
    order_bc: &Order,
    order_ca: &Order,
    bottleneck: &BottleneckLeg,
) -> Option<FillResult> {
    let [(released_a, fill_a), (released_b, fill_b), (released_c, fill_c)] = match bottleneck {
        BottleneckLeg::AB => {
            let legs = simulate_cycle_fills([order_ab, order_ca, order_bc])?;
            [
                (legs[0].1, legs[1].0),
                (legs[2].1, legs[0].0),
                (legs[1].1, legs[2].0),
            ]
        }
        BottleneckLeg::BC => {
            let legs = simulate_cycle_fills([order_bc, order_ab, order_ca])?;
            [
                (legs[1].1, legs[2].0),
                (legs[0].1, legs[1].0),
                (legs[2].1, legs[0].0),
            ]
        }
        BottleneckLeg::CA => {
            let legs = simulate_cycle_fills([order_ca, order_bc, order_ab])?;
            [
                (legs[2].1, legs[0].0),
                (legs[1].1, legs[2].0),
                (legs[0].1, legs[1].0),
            ]
        }
    };

    Some(FillResult {
        fill_a,
        fill_b,
        fill_c,
        surplus_a: released_a.saturating_sub(fill_a),
        surplus_b: released_b.saturating_sub(fill_b),
        surplus_c: released_c.saturating_sub(fill_c),
    })
}

/// Determine which leg is the bottleneck (smallest USD capacity).
fn find_bottleneck(usd_ab: u128, usd_bc: u128, usd_ca: u128) -> BottleneckLeg {
    if usd_ab <= usd_bc && usd_ab <= usd_ca {
        BottleneckLeg::AB
    } else if usd_bc <= usd_ca {
        BottleneckLeg::BC
    } else {
        BottleneckLeg::CA
    }
}

/// Full cycle simulation: find bottleneck, simulate fills, compute USD surplus.
/// Single source of truth used by both `try_build_entry` and `execute_cycle`.
fn simulate_full_cycle<F: PriceFeed>(
    book: &OrderBook<F>,
    id_ab: OrderId,
    id_bc: OrderId,
    id_ca: OrderId,
) -> Option<CycleSimulation> {
    let order_ab = &book.orders[&id_ab];
    let order_bc = &book.orders[&id_bc];
    let order_ca = &book.orders[&id_ca];

    if !order_ab.is_active() || !order_bc.is_active() || !order_ca.is_active() {
        return None;
    }

    // Any leg's token unpriced ⇒ cycle is not matchable (same as a
    // zero/loss cycle: skip it rather than value it on a bogus default).
    let (Some(price_a), Some(price_b), Some(price_c)) = (
        book.feed.price_cents(order_ab.offered_token),
        book.feed.price_cents(order_ab.requested_token),
        book.feed.price_cents(order_bc.requested_token),
    ) else {
        return None;
    };

    let usd_ab = order_ab.requested_remaining as u128 * price_b as u128;
    let usd_bc = order_bc.requested_remaining as u128 * price_c as u128;
    let usd_ca = order_ca.requested_remaining as u128 * price_a as u128;

    let bottleneck = find_bottleneck(usd_ab, usd_bc, usd_ca);

    let bottleneck_usd = match bottleneck {
        BottleneckLeg::AB => usd_ab,
        BottleneckLeg::BC => usd_bc,
        BottleneckLeg::CA => usd_ca,
    };

    if bottleneck_usd == 0 {
        return None;
    }

    let fills = simulate_cycle(order_ab, order_bc, order_ca, &bottleneck)?;

    let surplus_usd = fills.surplus_a as u128 * price_a as u128
        + fills.surplus_b as u128 * price_b as u128
        + fills.surplus_c as u128 * price_c as u128;

    Some(CycleSimulation { fills, surplus_usd })
}

// ── Phase 2: Execute Loop ───────────────────────────────────────────────────

/// 3-edge cycle matching: heap-based triangular matching.
///
/// Phase 1: enumerate all profitable triangles, push to max-heap by surplus.
/// Phase 2: pop best, execute, re-evaluate dirty triangles, repeat.
///
/// Inserts filled order IDs into the provided set. Returns number of cycles executed.
pub fn run_three_edge_cycle<F: PriceFeed>(book: &mut OrderBook<F>, filled_orders: &mut HashSet<OrderId>) -> u64 {
    let mut cycles_executed = 0u64;

    // Phase 1: build
    let mut triangles_by_pair: HashMap<(TokenId, TokenId), Vec<Triangle>> = HashMap::new();
    let entries = enumerate_triangles(book, &mut triangles_by_pair);
    let mut heap: BinaryHeap<CycleEntry> = entries.into_iter().collect();

    // Phase 2: execute
    while let Some(entry) = heap.pop() {
        // Stale check: recompute simulation from current order state.
        // The heap entry's surplus_usd may be outdated if orders were partially filled.
        let simulation = match simulate_full_cycle(
            book,
            entry.order_ab,
            entry.order_bc,
            entry.order_ca,
        ) {
            Some(sim) => sim,
            None => {
                // Orders inactive — try refreshing with current best orders.
                if let Some(fresh_entry) = try_build_entry(book, entry.triangle) {
                    heap.push(fresh_entry);
                }
                continue;
            }
        };

        // If surplus changed (partial fills), re-push with correct priority.
        if simulation.surplus_usd != entry.surplus_usd {
            if simulation.surplus_usd > 0 {
                heap.push(CycleEntry {
                    surplus_usd: simulation.surplus_usd,
                    ..entry
                });
            }
            continue;
        }

        if !execute_cycle(book, &entry, &simulation.fills, filled_orders) {
            // Invariant violation — already logged inside execute_cycle. Skip
            // this entry; the next heap iteration handles whatever's left.
            continue;
        }
        cycles_executed += 1;

        // Re-evaluate triangles sharing affected pairs (deduplicated).
        let mut affected_set = HashSet::new();
        for pair in entry.triangle.pairs() {
            if let Some(triangles) = triangles_by_pair.get(&pair) {
                for &triangle in triangles {
                    affected_set.insert(triangle);
                }
            }
        }
        for triangle in affected_set {
            if let Some(fresh_entry) = try_build_entry(book, triangle) {
                heap.push(fresh_entry);
            }
        }
    }

    cycles_executed
}

/// Execute a 3-cycle: fill orders, collect surplus, cleanup exhausted.
///
/// `simulate_cycle` and `fill()` use the same `offered_for()` calculation
/// (original offered/requested ratio never changes), so simulation surplus
/// is identical to actual surplus — no need to recompute from fill results.
///
/// Returns `true` on success. Returns `false` (after logging) if any of the
/// three orders has been removed from the book between simulation and
/// execution — a "should never happen" invariant violation that would
/// otherwise have been an `.unwrap()` panic. On a `false` return the caller
/// must not count this as an executed cycle. Note that a partial fill on
/// `order_ab` is not unwound if a later order is missing; the matcher's
/// next-tick DB hydration self-corrects the discrepancy.
fn execute_cycle<F: PriceFeed>(
    book: &mut OrderBook<F>,
    entry: &CycleEntry,
    simulation: &FillResult,
    filled_orders: &mut HashSet<OrderId>,
) -> bool {
    // Execute fills: deduct requested_remaining on each order.
    let Some(order_ab) = book.orders.get_mut(&entry.order_ab) else {
        tracing::warn!(
            order_ab = %entry.order_ab,
            "execute_cycle: order_ab unexpectedly missing — skipping cycle"
        );
        return false;
    };
    order_ab.fill(simulation.fill_b);

    let Some(order_ca) = book.orders.get_mut(&entry.order_ca) else {
        tracing::warn!(
            order_ca = %entry.order_ca,
            "execute_cycle: order_ca unexpectedly missing — skipping cycle (order_ab already partially filled)"
        );
        return false;
    };
    order_ca.fill(simulation.fill_a);

    let Some(order_bc) = book.orders.get_mut(&entry.order_bc) else {
        tracing::warn!(
            order_bc = %entry.order_bc,
            "execute_cycle: order_bc unexpectedly missing — skipping cycle (order_ab + order_ca already partially filled)"
        );
        return false;
    };
    order_bc.fill(simulation.fill_c);

    filled_orders.insert(entry.order_ab);
    filled_orders.insert(entry.order_ca);
    filled_orders.insert(entry.order_bc);

    book.cleanup_if_filled(entry.order_ab);
    book.cleanup_if_filled(entry.order_ca);
    book.cleanup_if_filled(entry.order_bc);

    // Collect surplus per token (already computed by simulation).
    let triangle = entry.triangle;
    book.add_protocol_balance(triangle.token_id_a(), simulation.surplus_a);
    book.add_protocol_balance(triangle.token_id_b(), simulation.surplus_b);
    book.add_protocol_balance(triangle.token_id_c(), simulation.surplus_c);
    true
}

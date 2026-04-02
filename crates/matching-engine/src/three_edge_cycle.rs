use crate::order_book::OrderBook;
use crate::price_feed::PriceFeed;
use crate::types::*;
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
}

/// Heap entry: a triangle with its best orders and surplus ranking.
#[derive(Clone, Debug)]
struct CycleEntry {
    triangle: Triangle,
    order_ab: OrderId,
    order_bc: OrderId,
    order_ca: OrderId,
    surplus_bps: u64,
}

impl PartialEq for CycleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.surplus_bps == other.surplus_bps
    }
}
impl Eq for CycleEntry {}
impl PartialOrd for CycleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CycleEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.surplus_bps.cmp(&other.surplus_bps)
    }
}

enum BottleneckLeg {
    AB,
    BC,
    CA,
}

struct FillResult {
    fill_b: Amount,
    fill_a: Amount,
    fill_c: Amount,
    surplus_a: Amount,
    surplus_b: Amount,
    surplus_c: Amount,
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

    for &b in &all_tokens {
        let incoming_b = book.incoming_neighbors(b);
        let outgoing_b = book.neighbors(b);

        for &a in &incoming_b {
            for &c in &outgoing_b {
                if c == a || c == b {
                    continue;
                }
                if !book.has_orders(c, a) {
                    continue;
                }

                let tri = Triangle::canonical(a, b, c);
                if !seen.insert(tri) {
                    continue;
                }

                if let Some(entry) = try_build_entry(book, tri) {
                    // Register in reverse index
                    triangles_by_pair.entry((tri.0, tri.1)).or_default().push(tri);
                    triangles_by_pair.entry((tri.1, tri.2)).or_default().push(tri);
                    triangles_by_pair.entry((tri.2, tri.0)).or_default().push(tri);
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
    tri: Triangle,
) -> Option<CycleEntry> {
    let id_ab = book.best_order(tri.0, tri.1)?.id;
    let id_bc = book.best_order(tri.1, tri.2)?.id;
    let id_ca = book.best_order(tri.2, tri.0)?.id;

    let o_ab = &book.orders[id_ab as usize];
    let o_bc = &book.orders[id_bc as usize];
    let o_ca = &book.orders[id_ca as usize];

    // Profitability check (u128 — no overflow)
    let lhs = o_ab.offered as u128 * o_bc.offered as u128 * o_ca.offered as u128;
    let rhs = o_ab.requested as u128 * o_bc.requested as u128 * o_ca.requested as u128;
    if lhs <= rhs {
        return None;
    }

    let bps = compute_surplus_bps(book, id_ab, id_bc, id_ca)?;
    if bps == 0 {
        return None;
    }

    Some(CycleEntry {
        triangle: tri,
        order_ab: id_ab,
        order_bc: id_bc,
        order_ca: id_ca,
        surplus_bps: bps,
    })
}

// ── Surplus & Fill Chain ────────────────────────────────────────────────────

/// Compute surplus in basis points for a triangle (read-only simulation).
fn compute_surplus_bps<F: PriceFeed>(
    book: &OrderBook<F>,
    id_ab: OrderId,
    id_bc: OrderId,
    id_ca: OrderId,
) -> Option<u64> {
    let ab = &book.orders[id_ab as usize];
    let bc = &book.orders[id_bc as usize];
    let ca = &book.orders[id_ca as usize];

    if !ab.is_active() || !bc.is_active() || !ca.is_active() {
        return None;
    }

    // USD value of each leg's remaining capacity
    let price_a = book.feed.usd_price_cents(ab.offered_token) as u128;
    let price_b = book.feed.usd_price_cents(ab.requested_token) as u128;
    let price_c = book.feed.usd_price_cents(bc.requested_token) as u128;

    let usd_ab = ab.requested_remaining as u128 * price_b;
    let usd_bc = bc.requested_remaining as u128 * price_c;
    let usd_ca = ca.requested_remaining as u128 * price_a;

    let bottleneck = if usd_ab <= usd_bc && usd_ab <= usd_ca {
        BottleneckLeg::AB
    } else if usd_bc <= usd_ca {
        BottleneckLeg::BC
    } else {
        BottleneckLeg::CA
    };

    let bottleneck_usd = match bottleneck {
        BottleneckLeg::AB => usd_ab,
        BottleneckLeg::BC => usd_bc,
        BottleneckLeg::CA => usd_ca,
    };

    if bottleneck_usd == 0 {
        return None;
    }

    let result = forward_chain(ab, bc, ca, &bottleneck)?;

    let surplus_usd = result.surplus_a as u128 * price_a
        + result.surplus_b as u128 * price_b
        + result.surplus_c as u128 * price_c;

    Some(((surplus_usd * 10_000) / bottleneck_usd) as u64)
}

/// Simulate fill propagation starting from the bottleneck leg.
///
/// Token flow: AB releases A → CA consumes A, releases C → BC consumes C, releases B → AB consumes B.
fn forward_chain(
    ab: &Order,
    bc: &Order,
    ca: &Order,
    bottleneck: &BottleneckLeg,
) -> Option<FillResult> {
    let (fill_b, fill_a, fill_c, released_a, released_b, released_c) = match bottleneck {
        BottleneckLeg::AB => {
            let fill_b = ab.requested_remaining;
            let released_a = ab.offered_for(fill_b);
            let fill_a = released_a.min(ca.requested_remaining);
            let released_c = ca.offered_for(fill_a);
            let fill_c = released_c.min(bc.requested_remaining);
            let released_b = bc.offered_for(fill_c);
            (fill_b, fill_a, fill_c, released_a, released_b, released_c)
        }
        BottleneckLeg::BC => {
            let fill_c = bc.requested_remaining;
            let released_b = bc.offered_for(fill_c);
            let fill_b = released_b.min(ab.requested_remaining);
            let released_a = ab.offered_for(fill_b);
            let fill_a = released_a.min(ca.requested_remaining);
            let released_c = ca.offered_for(fill_a);
            (fill_b, fill_a, fill_c, released_a, released_b, released_c)
        }
        BottleneckLeg::CA => {
            let fill_a = ca.requested_remaining;
            let released_c = ca.offered_for(fill_a);
            let fill_c = released_c.min(bc.requested_remaining);
            let released_b = bc.offered_for(fill_c);
            let fill_b = released_b.min(ab.requested_remaining);
            let released_a = ab.offered_for(fill_b);
            (fill_b, fill_a, fill_c, released_a, released_b, released_c)
        }
    };

    if fill_b == 0 || fill_a == 0 || fill_c == 0 {
        return None;
    }

    Some(FillResult {
        fill_b,
        fill_a,
        fill_c,
        surplus_a: released_a.saturating_sub(fill_a),
        surplus_b: released_b.saturating_sub(fill_b),
        surplus_c: released_c.saturating_sub(fill_c),
    })
}

// ── Phase 2: Execute Loop ───────────────────────────────────────────────────

/// 3-edge cycle matching: heap-based triangular matching.
///
/// Phase 1: enumerate all profitable triangles, push to max-heap by surplus.
/// Phase 2: pop best, execute, re-evaluate dirty triangles, repeat.
///
/// Returns (filled_order_ids, cycles_executed).
pub fn run_three_edge_cycle<F: PriceFeed>(
    book: &mut OrderBook<F>,
) -> (HashSet<OrderId>, u32) {
    let mut filled_orders = HashSet::new();
    let mut cycles_executed = 0u32;

    // Phase 1: build
    let mut triangles_by_pair: HashMap<(TokenId, TokenId), Vec<Triangle>> = HashMap::new();
    let entries = enumerate_triangles(book, &mut triangles_by_pair);
    let mut heap: BinaryHeap<CycleEntry> = entries.into_iter().collect();

    // Phase 2: execute
    let mut dirty: HashSet<(TokenId, TokenId)> = HashSet::new();

    while let Some(entry) = heap.pop() {
        // Stale check: all 3 orders must still be active
        {
            let ab = &book.orders[entry.order_ab as usize];
            let bc = &book.orders[entry.order_bc as usize];
            let ca = &book.orders[entry.order_ca as usize];
            if !ab.is_active() || !bc.is_active() || !ca.is_active() {
                // Try refreshing with current best orders
                if let Some(fresh) = try_build_entry(book, entry.triangle) {
                    heap.push(fresh);
                }
                continue;
            }
        }

        // Execute
        if execute_cycle(book, &entry, &mut filled_orders) {
            cycles_executed += 1;

            let t = entry.triangle;
            dirty.insert((t.0, t.1));
            dirty.insert((t.1, t.2));
            dirty.insert((t.2, t.0));

            // Re-evaluate triangles sharing dirty pairs
            for pair in dirty.drain() {
                if let Some(tris) = triangles_by_pair.get(&pair) {
                    for &tri in tris {
                        if let Some(fresh) = try_build_entry(book, tri) {
                            heap.push(fresh);
                        }
                    }
                }
            }
        }
    }

    (filled_orders, cycles_executed)
}

/// Execute a 3-cycle: fill orders, track surplus, cleanup exhausted.
///
/// Uses actual released values from fill() — not simulated values — to compute
/// surplus. This guarantees surplus accounting matches on-chain settlement exactly.
fn execute_cycle<F: PriceFeed>(
    book: &mut OrderBook<F>,
    entry: &CycleEntry,
    filled_orders: &mut HashSet<OrderId>,
) -> bool {
    let ab = &book.orders[entry.order_ab as usize];
    let bc = &book.orders[entry.order_bc as usize];
    let ca = &book.orders[entry.order_ca as usize];

    // Determine bottleneck via USD normalization
    let price_a = book.feed.usd_price_cents(ab.offered_token) as u128;
    let price_b = book.feed.usd_price_cents(ab.requested_token) as u128;
    let price_c = book.feed.usd_price_cents(bc.requested_token) as u128;

    let usd_ab = ab.requested_remaining as u128 * price_b;
    let usd_bc = bc.requested_remaining as u128 * price_c;
    let usd_ca = ca.requested_remaining as u128 * price_a;

    let bottleneck = if usd_ab <= usd_bc && usd_ab <= usd_ca {
        BottleneckLeg::AB
    } else if usd_bc <= usd_ca {
        BottleneckLeg::BC
    } else {
        BottleneckLeg::CA
    };

    let sim = match forward_chain(ab, bc, ca, &bottleneck) {
        Some(r) => r,
        None => return false,
    };

    let t = entry.triangle;

    // Execute fills and capture actual released amounts.
    // Token flow: AB releases A, CA consumes A releases C, BC consumes C releases B.
    let actual_released_a = book.orders[entry.order_ab as usize].fill(sim.fill_b);
    filled_orders.insert(entry.order_ab);

    let actual_released_c = book.orders[entry.order_ca as usize].fill(sim.fill_a);
    filled_orders.insert(entry.order_ca);

    let actual_released_b = book.orders[entry.order_bc as usize].fill(sim.fill_c);
    filled_orders.insert(entry.order_bc);

    // Cleanup exhausted orders
    if book.orders[entry.order_ab as usize].is_completely_filled() {
        book.cleanup_order(entry.order_ab);
    }
    if book.orders[entry.order_ca as usize].is_completely_filled() {
        book.cleanup_order(entry.order_ca);
    }
    if book.orders[entry.order_bc as usize].is_completely_filled() {
        book.cleanup_order(entry.order_bc);
    }

    // Surplus from actual values (not simulation).
    // Token A: AB released it, CA consumed fill_a of it.
    // Token C: CA released it, BC consumed fill_c of it.
    // Token B: BC released it, AB consumed fill_b of it.
    let surplus_a = actual_released_a.saturating_sub(sim.fill_a);
    let surplus_c = actual_released_c.saturating_sub(sim.fill_c);
    let surplus_b = actual_released_b.saturating_sub(sim.fill_b);

    if surplus_a > 0 {
        book.add_protocol_balance(t.0, surplus_a);
    }
    if surplus_b > 0 {
        book.add_protocol_balance(t.1, surplus_b);
    }
    if surplus_c > 0 {
        book.add_protocol_balance(t.2, surplus_c);
    }

    true
}

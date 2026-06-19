//! Adversarial property-based tests (proptest) — black-hat fuzzing of the
//! matching math. Generates arbitrary `u64` order amounts (including the
//! extremes) and asserts the invariants an attacker would try to break:
//!
//!  * the solver can NEVER be made to release more of a token than it
//!    receives (no fund loss) on a direct match;
//!  * the matcher never panics / over-fills / underflows on any input;
//!  * the triangular matcher does not panic on large (but realistic) amounts.

use proptest::prelude::*;
use std::collections::HashSet;

use super::{eth, make_note_id, sol, usdc};
use crate::matching::direct_matching::run_direct_matching;
use crate::matching::order_book::OrderBook;
use crate::matching::three_edge_cycle::run_three_edge_cycle;
use crate::matching::types::Order;
use crate::price::WatchPriceFeed;

fn order(seed: u64, off_tok: crate::types::TokenId, req_tok: crate::types::TokenId, offered: u64, requested: u64) -> Order {
    Order {
        id: make_note_id(seed),
        offered_token: off_tok,
        requested_token: req_tok,
        offered,
        requested,
        requested_remaining: requested,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// A direct match must never make the solver pay out more of a token than it
    /// takes in. Reconstructs the per-token flow from the order deltas and
    /// asserts the solver's net is non-negative on BOTH tokens, for any amounts.
    #[test]
    fn direct_match_solver_never_loses(
        off_a in 1u64..=u64::MAX,
        req_a in 1u64..=u64::MAX,
        off_b in 1u64..=u64::MAX,
        req_b in 1u64..=u64::MAX,
    ) {
        // a: offers ETH, wants USDC.   b: offers USDC, wants ETH.
        let mut a = order(1, eth(), usdc(), off_a, req_a);
        let mut b = order(2, usdc(), eth(), off_b, req_b);

        if let Some(res) = a.match_with(&mut b) {
            // Fresh orders: filled = requested - remaining.
            let a_recv_usdc = req_a - a.requested_remaining; // USDC into a
            let a_sent_eth = a.offered_for(a_recv_usdc);     // ETH out of a
            let b_recv_eth = req_b - b.requested_remaining;  // ETH into b
            let b_sent_usdc = b.offered_for(b_recv_eth);     // USDC out of b

            // Solver net per token (must be >= 0 — else the solver lost funds).
            let net_eth = a_sent_eth as i128 - b_recv_eth as i128;
            let net_usdc = b_sent_usdc as i128 - a_recv_usdc as i128;

            prop_assert!(net_eth >= 0, "solver LOSES ETH: net={net_eth} (a:{off_a}/{req_a} b:{off_b}/{req_b})");
            prop_assert!(net_usdc >= 0, "solver LOSES USDC: net={net_usdc} (a:{off_a}/{req_a} b:{off_b}/{req_b})");

            // Reported surplus must equal the reconstructed net flow.
            prop_assert_eq!(res.surplus_offered as i128, net_eth);
            prop_assert_eq!(res.surplus_requested as i128, net_usdc);

            // No over-fill / underflow.
            prop_assert!(a.requested_remaining <= req_a);
            prop_assert!(b.requested_remaining <= req_b);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// The triangular matcher must not panic on large-but-realistic amounts.
    /// Amounts up to 1e14 base units (~1M tokens @ 8 decimals) drive the
    /// `offered_ab * offered_bc * offered_ca` triple product past u128.
    #[test]
    fn triangular_never_panics(
        off_ab in 1u64..=100_000_000_000_000,
        off_bc in 1u64..=100_000_000_000_000,
        off_ca in 1u64..=100_000_000_000_000,
        req_ab in 1u64..=100_000_000_000_000,
        req_bc in 1u64..=100_000_000_000_000,
        req_ca in 1u64..=100_000_000_000_000,
    ) {
        let mut feed = WatchPriceFeed::new();
        feed.set_price_cents(eth(), 100);
        feed.set_price_cents(usdc(), 100);
        feed.set_price_cents(sol(), 100);
        let mut book = OrderBook::new(feed);

        // A 3-cycle: ETH->USDC, USDC->SOL, SOL->ETH.
        book.add_user_order(make_note_id(1), eth(), usdc(), off_ab, req_ab);
        book.add_user_order(make_note_id(2), usdc(), sol(), off_bc, req_bc);
        book.add_user_order(make_note_id(3), sol(), eth(), off_ca, req_ca);

        let mut filled = HashSet::new();
        // MUST NOT PANIC (and must leave a consistent book).
        let _ = run_three_edge_cycle(&mut book, &mut filled);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    /// Fully random small book run through direct + triangular matching must
    /// never panic and must never leave an order over-filled.
    #[test]
    fn engine_random_never_panics(
        specs in prop::collection::vec(
            (0usize..3, 0usize..3, 1u64..=1_000_000_000_000_000u64, 1u64..=1_000_000_000_000_000u64),
            0..14,
        ),
        p0 in 1u64..=10_000_000, p1 in 1u64..=10_000_000, p2 in 1u64..=10_000_000,
    ) {
        let toks = [eth(), usdc(), sol()];
        let mut feed = WatchPriceFeed::new();
        feed.set_price_cents(toks[0], p0);
        feed.set_price_cents(toks[1], p1);
        feed.set_price_cents(toks[2], p2);
        let mut book = OrderBook::new(feed);

        let mut seed = 100u64;
        let mut totals: Vec<(crate::types::OrderId, u64)> = Vec::new();
        for (oi, ri, offered, requested) in specs {
            if oi == ri { continue; } // offered/requested token must differ
            let id = make_note_id(seed);
            seed += 7;
            book.add_user_order(id, toks[oi], toks[ri], offered, requested);
            totals.push((id, requested));
        }

        let mut filled = HashSet::new();
        let _ = run_direct_matching(&mut book, &mut filled);
        let _ = run_three_edge_cycle(&mut book, &mut filled);

        // No order may end up with requested_remaining > its original requested.
        for (id, req) in totals {
            if let Some(o) = book.orders.get(&id) {
                prop_assert!(o.requested_remaining <= req, "over-fill/underflow on {id}");
            }
        }
    }
}

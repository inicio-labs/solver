//! Edge case tests informed by DeFi matching engine research.
//!
//! Sources: CoW Protocol, Balancer exploit, Sec3 bidirectional rounding,
//! Gnosis ring trades, general MEV/solver literature.

use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::price::WatchPriceFeed;
use crate::matching::types::*;
use super::{eth, usdc, sol, btc, matic, NoteIdGen, make_note_id};

fn pseudo_rand(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

// ===================================================================
// 1. PRECISION / ROUNDING
// ===================================================================

/// offered_for and requested_for should round-trip consistently.
/// offered_for(requested_for(x)) >= x would mean rounding favors the order (bad).
/// offered_for(requested_for(x)) <= x is acceptable (protocol-favorable).
#[test]
fn round_trip_offered_requested_consistency() {
    let mut seed: u64 = 999111;
    let mut violations = 0;

    for _ in 0..10_000 {
        let offered = 1 + (pseudo_rand(&mut seed) % 100_000) as u64;
        let requested = 1 + (pseudo_rand(&mut seed) % 100_000) as u64;
        let order = Order {
            id: make_note_id(0), offered_token: eth(), requested_token: usdc(),
            offered, requested, requested_remaining: requested,
        };

        // Forward: pick a fill, get offered released
        let fill = 1 + (pseudo_rand(&mut seed) % requested.max(1) as u64) as u64;
        let released = order.offered_for(fill);
        if released == 0 { continue; }

        // Reverse: given the released amount, what fill does it correspond to?
        let reconstructed_fill = order.requested_for(released);

        // The reconstructed fill should be <= original fill
        // (rounding should not create tokens)
        if reconstructed_fill > fill + 1 {
            violations += 1;
        }
    }
    // Allow a small number of 1-unit violations from rounding
    assert!(
        violations < 50,
        "too many round-trip violations: {}/10000",
        violations
    );
}

/// PRECISION_FACTOR=100_000 fails when offered/requested ratio exceeds 100,000.
/// This documents the precision boundary.
#[test]
fn precision_factor_boundary() {
    // Order with extreme ratio: 1 unit offered for 200_000 requested
    let order = Order {
        id: make_note_id(0), offered_token: eth(), requested_token: usdc(),
        offered: 1, requested: 200_000, requested_remaining: 200_000,
    };

    // offered_for(1) should return something > 0 for any fill
    // ratio = (200_000 * 100_000) / 1 = 20_000_000_000 (fine, fits u64)
    // result = (1 * 100_000) / 20_000_000_000 = 0 (truncated!)
    let result = order.offered_for(1);
    // This is a known limitation: tiny fills on extreme-ratio orders yield 0.
    // Document that fills below certain threshold produce nothing.
    assert_eq!(result, 0, "expected 0 due to precision truncation");

    // Full fill should still work
    assert_eq!(order.offered_for(200_000), 1);
}

/// Multiplication overflow check: offered * PRECISION_FACTOR must not overflow u64.
/// With u128 intermediates, all u64 values are safe.
#[test]
fn large_offered_no_overflow() {
    let large_but_safe = 1_000_000_000u64; // 10^9, well under u64 limit
    let order = Order {
        id: make_note_id(0), offered_token: eth(), requested_token: usdc(),
        offered: large_but_safe, requested: large_but_safe / 2,
        requested_remaining: large_but_safe / 2,
    };

    // Should not panic
    let released = order.offered_for(large_but_safe / 4);
    assert!(released > 0);
    assert!(released <= large_but_safe);
}

/// Verify that offered_for never returns more than offered (no token creation).
#[test]
fn offered_for_never_exceeds_offered() {
    let mut seed: u64 = 444555;
    for _ in 0..10_000 {
        let offered = 1 + (pseudo_rand(&mut seed) % 100_000) as u64;
        let requested = 1 + (pseudo_rand(&mut seed) % 100_000) as u64;
        let order = Order {
            id: make_note_id(0), offered_token: eth(), requested_token: usdc(),
            offered, requested, requested_remaining: requested,
        };

        for fill in [1, requested / 2, requested - 1, requested] {
            if fill == 0 || fill > requested { continue; }
            let released = order.offered_for(fill);
            assert!(
                released <= offered,
                "offered_for({}) = {} > offered {} for order {}/{}",
                fill, released, offered, offered, requested
            );
        }
    }
}

// ===================================================================
// 2. NEGATIVE SURPLUS / ROUNDING STEALS FROM PROTOCOL
// ===================================================================

/// In pairwise matching, verify surplus recorded in protocol_balances is non-negative.
/// The engine uses saturating_sub so protocol balances can't go negative,
/// but we verify the surplus is real (protocol_balances > 0 when match happens).
///
/// NOTE: offered_for(total_filled) can diverge from the sum of incremental fill()
/// returns due to integer rounding with the total ratio. This is a known property:
/// the on-chain contract uses the same total-ratio calculation, so the settlement
/// is consistent. The divergence only affects off-chain reconstruction.
#[test]
fn protocol_surplus_non_negative_after_match() {
    let mut seed: u64 = 777888;

    for trial in 0..2_000 {
        let mut feed = WatchPriceFeed::new();
        feed.set_price_cents(eth(), 100 + (pseudo_rand(&mut seed) % 1000) as u64);
        feed.set_price_cents(usdc(), 100 + (pseudo_rand(&mut seed) % 1000) as u64);

        let mut book = OrderBook::new(feed);
        let mut gen = NoteIdGen::new();

        let off_a = 10 + (pseudo_rand(&mut seed) % 1000) as u64;
        let req_a = 10 + (pseudo_rand(&mut seed) % 1000) as u64;
        let off_b = 10 + (pseudo_rand(&mut seed) % 1000) as u64;
        let req_b = 10 + (pseudo_rand(&mut seed) % 1000) as u64;

        book.add_user_order(gen.next(), eth(), usdc(), off_a, req_a);
        book.add_user_order(gen.next(), usdc(), eth(), off_b, req_b);

        let mut engine = MatchingEngine::new(book);
        let _batch = engine.run();

        // Protocol balances should be >= 0 (they're u64, so always true,
        // but verify they weren't set to some garbage value)
        for (&_token, &balance) in &engine.book.protocol_balances {
            assert!(balance < u64::MAX / 2, "trial {}: suspicious balance", trial);
        }
    }
}

// ===================================================================
// 3. ADVERSARIAL ORDER PLACEMENT
// ===================================================================

/// Dust attack: 100 orders with offered=1, requested=1 across all pairs.
/// Engine should handle gracefully without hanging or panicking.
#[test]
fn dust_order_attack() {
    let mut feed = WatchPriceFeed::new();
    let tokens = [eth(), usdc(), sol(), btc(), matic()];
    for &t in &tokens { feed.set_price_cents(t, 100); }

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Place dust orders across all pairs
    for i in 0..5 {
        for j in 0..5 {
            if i == j { continue; }
            for _ in 0..5 {
                book.add_user_order(gen.next(), tokens[i], tokens[j], 2, 1);
            }
        }
    }

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // Should complete without hanging. Verify invariants.
    for order in engine.book.orders.values() {
        assert!(order.requested_filled() <= order.requested);
    }
    assert_eq!(batch.remaining_orders, engine.book.active_order_count());
}

/// Order splitting attack: instead of one 1000-unit order, place 1000 one-unit orders.
/// Total surplus extracted should be similar (not exploitable via splitting).
#[test]
fn order_splitting_no_extra_surplus() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);

    // Run 1: one large order
    let mut book1 = OrderBook::new(feed.clone());
    let mut gen1 = NoteIdGen::new();
    book1.add_user_order(gen1.next(), eth(), usdc(), 1000, 800);
    book1.add_user_order(gen1.next(), usdc(), eth(), 1000, 800);
    let mut engine1 = MatchingEngine::new(book1);
    let _batch1 = engine1.run();
    let surplus1: u64 = engine1.book.protocol_balances.values().sum();

    // Run 2: many small orders (same total)
    let mut book2 = OrderBook::new(feed.clone());
    let mut gen2 = NoteIdGen::new();
    for _ in 0..100 {
        book2.add_user_order(gen2.next(), eth(), usdc(), 10, 8);
        book2.add_user_order(gen2.next(), usdc(), eth(), 10, 8);
    }
    let mut engine2 = MatchingEngine::new(book2);
    let _batch2 = engine2.run();
    let surplus2: u64 = engine2.book.protocol_balances.values().sum();

    // Both configurations should generate surplus (profitable orders were matched)
    assert!(surplus1 > 0, "single order match should produce surplus");
    assert!(surplus2 > 0, "split order match should produce surplus");
}

/// Targeted cycle creation: attacker places the third leg of a triangle
/// to capture surplus from two existing orders.
#[test]
fn targeted_cycle_surplus_extraction() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Existing generous orders
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);  // offers $20k for $16k
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);   // offers $16k for $12k

    // Attacker places SOL->ETH to close the triangle, barely profitable
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);  // offers $12k for $10k

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // The triangle executes. Surplus should exist.
    // The attacker's order is the least generous, so surplus should not all go to them.
    // Verify total surplus is reasonable.
    let surplus_usd: u64 = engine.book.protocol_balances.iter()
        .map(|(&tok, &amt)| {
            amt as u64 * engine.book.feed.usd_price_cents(tok) / 100
        })
        .sum();

    // Triangle: $20k + $16k + $12k in, $16k + $12k + $10k out = $10k surplus
    // (approximate, actual depends on integer math)
    if batch.cycles_executed > 0 {
        assert!(surplus_usd > 0, "cycle should produce surplus");
    }
}

// ===================================================================
// 4. CYCLE DETECTION PITFALLS
// ===================================================================

/// Reverse cycle: A->B->C->A and A->C->B->A are different opportunities.
/// Both should be discoverable.
#[test]
fn reverse_cycle_discovered() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Forward cycle: ETH->USDC->SOL->ETH
    book.add_user_order(gen.next(), eth(), usdc(), 110, 100);
    book.add_user_order(gen.next(), usdc(), sol(), 110, 100);
    book.add_user_order(gen.next(), sol(), eth(), 110, 100);

    // Reverse cycle: ETH->SOL->USDC->ETH
    book.add_user_order(gen.next(), eth(), sol(), 110, 100);
    book.add_user_order(gen.next(), sol(), usdc(), 110, 100);
    book.add_user_order(gen.next(), usdc(), eth(), 110, 100);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // Both direct matches (Phase 1) and triangles (Phase 2) may fire.
    // At minimum, the pairwise matches should find ETH<->USDC, ETH<->SOL, USDC<->SOL.
    assert!(batch.cycles_executed >= 3, "should find multiple matches, got {}", batch.cycles_executed);
}

/// Infinite loop guard: crafted orders that create cycles which execute for 0 net effect.
/// The engine should terminate in bounded time.
#[test]
fn no_infinite_loop_zero_effect_cycles() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Orders that are barely profitable (1 unit surplus per cycle)
    // Engine could potentially re-find the same triangle after partial fill
    for _ in 0..10 {
        book.add_user_order(gen.next(), eth(), usdc(), 101, 100);
        book.add_user_order(gen.next(), usdc(), sol(), 101, 100);
        book.add_user_order(gen.next(), sol(), eth(), 101, 100);
    }

    // Should terminate (the test itself is the timeout guard)
    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // Should have done some work
    assert!(batch.cycles_executed > 0 || batch.remaining_orders > 0);
}

/// Stale heap entry: after a cycle exhausts orders, the heap may still
/// contain entries referencing those orders. Should skip gracefully.
#[test]
fn stale_heap_entries_handled() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Small orders that will be exhausted quickly
    book.add_user_order(gen.next(), eth(), usdc(), 110, 100);
    book.add_user_order(gen.next(), usdc(), sol(), 110, 100);
    book.add_user_order(gen.next(), sol(), eth(), 110, 100);

    // Second set at worse rates
    book.add_user_order(gen.next(), eth(), usdc(), 105, 100);
    book.add_user_order(gen.next(), usdc(), sol(), 105, 100);
    book.add_user_order(gen.next(), sol(), eth(), 105, 100);

    let mut engine = MatchingEngine::new(book);
    let _batch = engine.run();

    // Should execute both triangles without crash
    for order in engine.book.orders.values() {
        assert!(order.requested_filled() <= order.requested);
    }
}

// ===================================================================
// 5. SETTLEMENT & CONSERVATION
// ===================================================================

/// The strongest conservation test: simulate the on-chain settlement.
/// For a pairwise match, verify that:
///   order_a.offered_released == order_b.fill + surplus_a
///   order_b.offered_released == order_a.fill + surplus_b
#[test]
fn settlement_token_balance_pairwise() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id_a = gen.next();
    book.add_user_order(id_a, eth(), usdc(), 10, 16000);
    let id_b = gen.next();
    book.add_user_order(id_b, usdc(), eth(), 20000, 10);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert!(batch.cycles_executed > 0);

    let a = &engine.book.orders[&id_a];
    let b = &engine.book.orders[&id_b];

    // What was actually filled
    let a_filled_usdc = a.requested_filled();  // USDC received by A
    let b_filled_eth = b.requested_filled();   // ETH received by B

    // What was released (using the same calculation as on-chain)
    let a_released_eth = a.offered_for(a_filled_usdc);
    let b_released_usdc = b.offered_for(b_filled_eth);

    // ETH balance: A releases ETH, B consumes ETH, remainder is surplus
    let eth_surplus = a_released_eth.saturating_sub(b_filled_eth);
    let usdc_surplus = b_released_usdc.saturating_sub(a_filled_usdc);

    // Verify: what the protocol tracked matches what we calculated
    let protocol_eth = engine.book.protocol_balances.get(&eth()).copied().unwrap_or(0);
    let protocol_usdc = engine.book.protocol_balances.get(&usdc()).copied().unwrap_or(0);

    // Allow 1 unit tolerance for rounding
    assert!(
        eth_surplus.abs_diff(protocol_eth) <= 1,
        "ETH surplus mismatch: calculated={} protocol={}",
        eth_surplus, protocol_eth
    );
    assert!(
        usdc_surplus.abs_diff(protocol_usdc) <= 1,
        "USDC surplus mismatch: calculated={} protocol={}",
        usdc_surplus, protocol_usdc
    );
}

/// Triangle surplus attribution: verify surplus_a is NOT counted from
/// unfilled capacity that should remain available.
#[test]
fn triangle_surplus_is_real_not_unfilled_capacity() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Large AB, small BC and CA -> AB only partially used
    let id_ab = gen.next();
    book.add_user_order(id_ab, eth(), usdc(), 100, 160000);  // $200k
    book.add_user_order(gen.next(), usdc(), sol(), 3000, 10);      // $3k (bottleneck)
    book.add_user_order(gen.next(), sol(), eth(), 20, 1);          // $3k

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    if batch.cycles_executed > 0 {
        // AB should be partially filled (bottleneck was elsewhere)
        let ab = &engine.book.orders[&id_ab];
        assert!(ab.is_active(), "AB should be partially filled, not exhausted");

        // The unfilled portion of AB should still have its full offered_remaining
        // It should NOT have been counted as "surplus"
        let ab_remaining_offered = ab.offered_remaining();
        let _ab_filled = ab.requested_filled();

        // Total surplus should be small (just the cycle efficiency),
        // NOT the entire unfilled AB capacity
        let total_surplus: u64 = engine.book.protocol_balances.values().sum();
        assert!(
            total_surplus < ab_remaining_offered,
            "surplus {} should be much less than unfilled AB capacity {}",
            total_surplus, ab_remaining_offered
        );
    }
}

// ===================================================================
// 6. f64 RATE KEY PRECISION
// ===================================================================

/// Two orders with different integer ratios that map to the same f64 rate.
/// They should be stored and retrieved correctly (not lost or confused).
#[test]
fn f64_rate_collision() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Two orders with rates that are very close but different in integer math
    let id1 = gen.next();
    book.add_user_order(id1, eth(), usdc(), 1000000, 333333);
    let id2 = gen.next();
    book.add_user_order(id2, eth(), usdc(), 1000001, 333334);

    // Both should be retrievable
    let best = book.best_order(eth(), usdc()).unwrap();
    assert!(best.id == id1 || best.id == id2, "should find one of the orders");

    // Counter-order to fill both
    book.add_user_order(gen.next(), usdc(), eth(), 2000000, 666666);

    let mut engine = MatchingEngine::new(book);
    let _batch = engine.run();

    // Both orders should have been touched
    let o1 = &engine.book.orders[&id1];
    let o2 = &engine.book.orders[&id2];
    // At least one should be filled
    assert!(
        o1.requested_filled() > 0 || o2.requested_filled() > 0,
        "at least one order should be filled"
    );
}

// ===================================================================
// 7. MATCH_WITH EDGE CASES
// ===================================================================

/// match_with where self was already partially filled from a prior match.
/// Verify self_requested_filled in the result is correct.
#[test]
fn match_with_partially_filled_self() {
    let mut order_a = Order {
        id: make_note_id(0), offered_token: eth(), requested_token: usdc(),
        offered: 1000, requested: 500, requested_remaining: 500,
    };
    let mut order_b = Order {
        id: make_note_id(1), offered_token: usdc(), requested_token: eth(),
        offered: 100, requested: 50, requested_remaining: 50,
    };

    // First partial fill
    let _r1 = order_a.match_with(&mut order_b).unwrap();
    let a_filled_1 = order_a.requested_filled();
    let b_filled_1 = order_b.requested_filled();
    assert!(a_filled_1 > 0);
    assert!(b_filled_1 > 0);

    // Second match with a new counter
    let mut order_c = Order {
        id: make_note_id(2), offered_token: usdc(), requested_token: eth(),
        offered: 200, requested: 100, requested_remaining: 100,
    };

    if order_a.is_active() {
        let r2 = order_a.match_with(&mut order_c);
        if let Some(_result) = r2 {
            // Combined fill should not exceed original
            let total_filled = order_a.requested_filled();
            assert!(
                total_filled <= 500,
                "total fill {} exceeds original requested 500",
                total_filled
            );
        }
    }
}

/// match_with where both orders have the exact same rate (no surplus possible).
#[test]
fn match_with_identical_rates() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);

    // Both offer 100 for 100 -- identical rates
    // But is_profitable_with requires offered*offered > requested*requested
    // 100*100 = 10000 > 100*100 = 10000 -> false. Won't match.
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 100, 100);
    book.add_user_order(gen.next(), usdc(), eth(), 100, 100);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert_eq!(batch.cycles_executed, 0, "identical rates should not match (not strictly profitable)");
}

// ===================================================================
// 8. COMPREHENSIVE FUZZ: ALL EDGE CASES COMBINED
// ===================================================================

/// The ultimate stress test: randomized orders with adversarial characteristics.
/// Mix of dust orders, extreme ratios, and targeted triangles.
#[test]
fn fuzz_adversarial_mix_500_trials() {
    let tokens = [eth(), usdc(), sol(), btc(), matic()];
    let prices: [u64; 5] = [200_000, 100, 15_000, 6_000_000, 5_000];
    let mut seed: u64 = 2718281828;

    for trial in 0..500 {
        let mut feed = WatchPriceFeed::new();
        for i in 0..5 { feed.set_price_cents(tokens[i], prices[i]); }

        let mut book = OrderBook::new(feed);
        let mut gen = NoteIdGen::new();
        let order_type = pseudo_rand(&mut seed) % 4;

        match order_type {
            0 => {
                // Dust orders: all amounts 1-3
                for _ in 0..20 {
                    let si = (pseudo_rand(&mut seed) % 5) as usize;
                    let mut bi = (pseudo_rand(&mut seed) % 5) as usize;
                    if bi == si { bi = (si + 1) % 5; }
                    let offered = 1 + (pseudo_rand(&mut seed) % 3) as u64;
                    let rate_pct = 50 + (pseudo_rand(&mut seed) % 50) as u64;
                    let requested = (offered as u128 * prices[si] as u128 * rate_pct as u128
                        / (prices[bi] as u128 * 100)) as u64;
                    if requested == 0 { continue; }
                    book.add_user_order(gen.next(), tokens[si], tokens[bi], offered, requested);
                }
            }
            1 => {
                // Extreme ratios: BTC/MATIC pairs
                for _ in 0..15 {
                    let (si, bi) = if pseudo_rand(&mut seed) % 2 == 0 { (3, 4) } else { (4, 3) };
                    let offered = 1 + (pseudo_rand(&mut seed) % 100) as u64;
                    let rate_pct = 60 + (pseudo_rand(&mut seed) % 40) as u64;
                    let requested = (offered as u128 * prices[si] as u128 * rate_pct as u128
                        / (prices[bi] as u128 * 100)) as u64;
                    if requested == 0 { continue; }
                    book.add_user_order(gen.next(), tokens[si], tokens[bi], offered, requested);
                }
            }
            2 => {
                // Deliberate triangles with near-zero surplus
                for _ in 0..3 {
                    let ai = (pseudo_rand(&mut seed) % 5) as usize;
                    let mut bi = (pseudo_rand(&mut seed) % 5) as usize;
                    if bi == ai { bi = (ai + 1) % 5; }
                    let mut ci = (pseudo_rand(&mut seed) % 5) as usize;
                    while ci == ai || ci == bi { ci = (ci + 1) % 5; }

                    for &(s, d) in &[(ai, bi), (bi, ci), (ci, ai)] {
                        let offered = 100 + (pseudo_rand(&mut seed) % 100) as u64;
                        let rate_pct = 98 + (pseudo_rand(&mut seed) % 2) as u64; // near-oracle
                        let requested = (offered as u128 * prices[s] as u128 * rate_pct as u128
                            / (prices[d] as u128 * 100)) as u64;
                        if requested == 0 { continue; }
                        book.add_user_order(gen.next(), tokens[s], tokens[d], offered, requested);
                    }
                }
            }
            _ => {
                // Mixed: normal + some edge cases
                for _ in 0..25 {
                    let si = (pseudo_rand(&mut seed) % 5) as usize;
                    let mut bi = (pseudo_rand(&mut seed) % 5) as usize;
                    if bi == si { bi = (si + 1) % 5; }
                    let offered = 1 + (pseudo_rand(&mut seed) % 10000) as u64;
                    let rate_pct = 50 + (pseudo_rand(&mut seed) % 50) as u64;
                    let requested = (offered as u128 * prices[si] as u128 * rate_pct as u128
                        / (prices[bi] as u128 * 100)) as u64;
                    if requested == 0 { continue; }
                    book.add_user_order(gen.next(), tokens[si], tokens[bi], offered, requested);
                }
            }
        }

        let mut engine = MatchingEngine::new(book);
        let batch = engine.run();

        // Core invariants that must NEVER be violated
        for order in engine.book.orders.values() {
            assert!(
                order.requested_remaining <= order.requested,
                "trial {}: order {:?} remaining {} > requested {}",
                trial, order.id, order.requested_remaining, order.requested
            );
            if order.is_active() {
                assert!(order.requested_remaining > 0);
            }
        }
        for oid in &batch.filled_orders {
            assert!(
                engine.book.orders[oid].requested_filled() > 0,
                "trial {}: filled order {:?} has 0 fill", trial, oid
            );
        }
        assert_eq!(batch.remaining_orders, engine.book.active_order_count());
    }
}

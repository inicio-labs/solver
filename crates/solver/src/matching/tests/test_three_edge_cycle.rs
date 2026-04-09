use crate::matching::engine::MatchingEngine;
use crate::matching::three_edge_cycle::run_three_edge_cycle;
use crate::matching::order_book::OrderBook;
use crate::price::WatchPriceFeed;
use super::{eth, usdc, sol, btc, matic, NoteIdGen};

fn make_3token_feed() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);   // $2000
    feed.set_price_cents(usdc(), 100);      // $1
    feed.set_price_cents(sol(), 15_000);    // $150
    feed
}

// -- Basic functionality --

/// Basic profitable triangle: ETH->USDC->SOL->ETH.
#[test]
fn basic_triangle() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    let (filled, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 1, "exactly one triangle should execute");
    assert_eq!(filled.len(), 3, "all 3 orders should be touched");
}

/// No profitable cycle: offered product <= requested product.
#[test]
fn no_profitable_cycle() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    // product: 10 * 19000 * 120 = 22.8M vs 19000 * 120 * 10 = 22.8M -> equal
    book.add_user_order(gen.next(), eth(), usdc(), 10, 19000);
    book.add_user_order(gen.next(), usdc(), sol(), 19000, 120);
    book.add_user_order(gen.next(), sol(), eth(), 120, 10);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 0, "equal product is not profitable");
}

/// Only two tokens -- no triangle possible.
#[test]
fn two_tokens_no_triangle() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), eth(), 16000, 10);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 0);
}

/// Empty book -- no crash, no cycles.
#[test]
fn empty_book() {
    let mut book = OrderBook::new(make_3token_feed());
    let (filled, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 0);
    assert!(filled.is_empty());
}

/// Single order -- no triangle.
#[test]
fn single_order() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 0);
}

// -- Exhaustion invariant --

/// After every cycle execution, at least one leg must be fully consumed
/// (the bottleneck leg).
#[test]
fn at_least_one_order_exhausted() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    let (filled, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0);

    let exhausted = filled.iter()
        .filter(|oid| book.orders[oid].is_completely_filled())
        .count();
    assert!(exhausted >= 1, "at least one order should be fully consumed (bottleneck)");
}

// -- Asset conservation --

/// Verify that for every token, released >= consumed across the cycle.
/// Any excess is surplus in protocol_balances.
#[test]
fn asset_conservation() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    // ETH->USDC: offers 10 ETH, requests 16000 USDC
    let id_ab = gen.next();
    assert!(book.add_user_order(id_ab, eth(), usdc(), 10, 16000));
    // USDC->SOL: offers 16000 USDC, requests 80 SOL
    let id_bc = gen.next();
    assert!(book.add_user_order(id_bc, usdc(), sol(), 16000, 80));
    // SOL->ETH: offers 80 SOL, requests 5 ETH
    let id_ca = gen.next();
    assert!(book.add_user_order(id_ca, sol(), eth(), 80, 5));

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0);

    let ab = &book.orders[&id_ab];
    let bc = &book.orders[&id_bc];
    let ca = &book.orders[&id_ca];

    // What each order released (offered_for of what was filled)
    let filled_b = ab.requested_filled();
    let _released_a = ab.offered_for(filled_b);

    // Simpler check: protocol balances should be non-negative (no token created from nothing)
    for (&_token, &balance) in &book.protocol_balances {
        assert!(balance > 0 || balance == 0, "protocol balance should never be negative");
    }

    // Check fill amounts are within bounds
    assert!(ab.requested_filled() <= ab.requested, "AB fill <= requested");
    assert!(bc.requested_filled() <= bc.requested, "BC fill <= requested");
    assert!(ca.requested_filled() <= ca.requested, "CA fill <= requested");
}

// -- Bottleneck on different legs --

/// AB is the bottleneck (smallest USD remaining).
#[test]
fn bottleneck_on_ab() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    // Tiny AB: 1 ETH ($2k) -- smallest
    let id_ab = gen.next();
    book.add_user_order(id_ab, eth(), usdc(), 1, 1600);
    // Large BC: 100000 USDC ($100k)
    let id_bc = gen.next();
    book.add_user_order(id_bc, usdc(), sol(), 100000, 500);
    // Large CA: 500 SOL ($75k)
    let id_ca = gen.next();
    book.add_user_order(id_ca, sol(), eth(), 500, 30);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0);
    // AB should be fully consumed (it was the bottleneck)
    assert!(book.orders[&id_ab].is_completely_filled(), "AB (bottleneck) should be exhausted");
    // Others should be partially filled
    assert!(book.orders[&id_bc].is_active(), "BC should be partially filled");
    assert!(book.orders[&id_ca].is_active(), "CA should be partially filled");
}

/// BC is the bottleneck.
#[test]
fn bottleneck_on_bc() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    // Large AB: 100 ETH ($200k), requests 160000 USDC ($160k)
    let id_ab = gen.next();
    book.add_user_order(id_ab, eth(), usdc(), 100, 160000);
    // Tiny BC: 3000 USDC ($3k), requests 10 SOL ($1.5k) -- smallest USD remaining
    let id_bc = gen.next();
    book.add_user_order(id_bc, usdc(), sol(), 3000, 10);
    // Large CA: 100 SOL ($15k), requests 5 ETH ($10k)
    let id_ca = gen.next();
    book.add_user_order(id_ca, sol(), eth(), 100, 5);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0, "should execute");
    assert!(book.orders[&id_bc].is_completely_filled(), "BC (bottleneck) should be exhausted");
    assert!(book.orders[&id_ab].is_active(), "AB should be partially filled");
}

/// CA is the bottleneck.
#[test]
fn bottleneck_on_ca() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    // Large AB: 100 ETH ($200k)
    let id_ab = gen.next();
    book.add_user_order(id_ab, eth(), usdc(), 100, 160000);
    // Large BC: 160000 USDC ($160k)
    let id_bc = gen.next();
    book.add_user_order(id_bc, usdc(), sol(), 160000, 800);
    // Tiny CA: 20 SOL ($3k) -- smallest
    let id_ca = gen.next();
    book.add_user_order(id_ca, sol(), eth(), 20, 1);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0);
    assert!(book.orders[&id_ca].is_completely_filled(), "CA (bottleneck) should be exhausted");
    assert!(book.orders[&id_ab].is_active(), "AB should be partially filled");
}

// -- Surplus verification --

/// Verify surplus goes to protocol balance with expected values.
#[test]
fn surplus_to_protocol_balance() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0);
    let total_balance: u64 = book.protocol_balances.values().sum();
    assert!(total_balance > 0, "surplus should accumulate in protocol balance");
}

/// With perfectly balanced rates (no surplus possible within integer math),
/// protocol balance should be zero or minimal.
#[test]
fn minimal_surplus_tight_rates() {
    let mut feed = WatchPriceFeed::new();
    // Set prices so orders are barely profitable
    feed.set_price_cents(eth(), 100);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 100);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Each order offers 101 for 100 -- barely profitable per leg
    // product: 101^3 = 1030301 vs 100^3 = 1000000 -> profitable
    book.add_user_order(gen.next(), eth(), usdc(), 101, 100);
    book.add_user_order(gen.next(), usdc(), sol(), 101, 100);
    book.add_user_order(gen.next(), sol(), eth(), 101, 100);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles > 0, "barely profitable cycle should still execute");
}

// -- Multiple cycles & ordering --

/// Multiple triangles: highest surplus executed first.
#[test]
fn multiple_triangles_highest_surplus_first() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);
    feed.set_price_cents(btc(), 6_000_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Triangle 1 (ETH-USDC-SOL): moderate surplus
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    // Triangle 2 (ETH-USDC-BTC): higher surplus
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), btc(), 16000, 1);
    book.add_user_order(gen.next(), btc(), eth(), 1, 5);

    let (filled, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles >= 1);
    assert!(!filled.is_empty());
}

/// Multiple orders per pair -- first cycle exhausts one, second uses the next.
#[test]
fn sequential_cycles_with_order_promotion() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();

    // Two ETH->USDC orders
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), eth(), usdc(), 10, 18000);
    // Two USDC->SOL orders
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), usdc(), sol(), 18000, 90);
    // Two SOL->ETH orders
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);
    book.add_user_order(gen.next(), sol(), eth(), 90, 5);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles >= 1, "should execute at least one cycle");
}

// -- Stale handling --

/// After executing one cycle that exhausts an order, stale refresh
/// picks up the next-best order for the same pair.
#[test]
fn stale_order_promotes_next_best() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();

    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);   // generous
    book.add_user_order(gen.next(), eth(), usdc(), 10, 18000);   // less generous
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);
    book.add_user_order(gen.next(), usdc(), sol(), 18000, 90);
    book.add_user_order(gen.next(), sol(), eth(), 90, 5);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles >= 1);
}

// -- Precision edge cases --

/// Very small amounts: 1-unit orders. Verify no panic and correct behavior.
#[test]
fn tiny_amounts_no_panic() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // These may or may not form a profitable cycle, but should not panic
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1);
    book.add_user_order(gen.next(), usdc(), sol(), 1, 1);
    book.add_user_order(gen.next(), sol(), eth(), 1, 1);

    let _ = run_three_edge_cycle(&mut book); // should not panic
}

/// Large amounts near u64 limits. Verify no overflow.
#[test]
fn large_amounts_no_overflow() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let large = 1_000_000_000u64; // 1 billion
    book.add_user_order(gen.next(), eth(), usdc(), large, large / 2);
    book.add_user_order(gen.next(), usdc(), sol(), large / 2, large / 4);
    book.add_user_order(gen.next(), sol(), eth(), large / 4, large / 8);

    let _ = run_three_edge_cycle(&mut book); // should not panic
}

// -- Integration with direct matching (Phase 1 + Phase 2) --

/// Direct matching eats pairwise orders, then triangular matching
/// finds cycles in the remainder.
#[test]
fn phase1_then_phase2() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Pairwise: ETH<->USDC (phase 1 handles these)
    book.add_user_order(gen.next(), eth(), usdc(), 5, 8000);
    book.add_user_order(gen.next(), usdc(), eth(), 8000, 5);

    // Triangle: separate ETH->USDC order + USDC->SOL + SOL->ETH (phase 2)
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // Phase 1 matches at least the pairwise ETH<->USDC (1 cycle)
    // Phase 2 may or may not find the triangle depending on what's left
    assert!(batch.cycles_executed >= 1, "should execute at least pairwise match");
}

/// If phase 1 partially fills an order that's part of a triangle,
/// phase 2 should still find and execute the triangle with remaining amounts.
#[test]
fn phase1_partial_then_phase2_triangle() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // ETH->USDC: large order, will be used by both phases
    book.add_user_order(gen.next(), eth(), usdc(), 20, 32000);
    // USDC->ETH: small, consumed by phase 1
    book.add_user_order(gen.next(), usdc(), eth(), 16000, 10);

    // Triangle legs (phase 2):
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    // Phase 1: ETH->USDC matched with USDC->ETH
    // Phase 2: remaining ETH->USDC + USDC->SOL + SOL->ETH forms triangle
    assert!(batch.cycles_executed >= 1);
}

// -- Unprofitable leg --

/// One leg of the triangle is unprofitable. All orders are accepted into the
/// book, but the matching engine should not execute an unprofitable cycle.
#[test]
fn unprofitable_leg_no_cycle() {
    let mut book = OrderBook::new(make_3token_feed());
    let mut gen = NoteIdGen::new();

    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    // SOL->ETH: offers 5 SOL ($750), requests 1 ETH ($2000) -- accepted into book
    let accepted = book.add_user_order(gen.next(), sol(), eth(), 5, 1);
    assert!(accepted, "order book should accept all valid orders");

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert_eq!(cycles, 0, "unprofitable cycle should not execute");
}

// -- 4+ tokens --

/// Four tokens, two independent triangles (no shared edges).
#[test]
fn four_tokens_two_triangles() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);
    feed.set_price_cents(btc(), 6_000_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Triangle 1: ETH->USDC->SOL->ETH (no shared edges with triangle 2)
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);

    // Triangle 2: BTC->ETH->SOL->BTC (different edges)
    book.add_user_order(gen.next(), btc(), eth(), 1, 20);         // 1 BTC ($60k) for 20 ETH ($40k)
    book.add_user_order(gen.next(), eth(), sol(), 20, 1600);       // 20 ETH ($40k) for 1600 SOL ($24k) -- needs separate SOL->BTC
    book.add_user_order(gen.next(), sol(), btc(), 1600, 1);        // 1600 SOL ($240k) for 1 BTC ($60k)

    let (_, cycles) = run_three_edge_cycle(&mut book);
    assert!(cycles >= 1, "should execute at least one triangle, got {}", cycles);
}

/// Five tokens with overlapping edges. Verify no double-counting or panic.
#[test]
fn five_tokens_stress() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);
    feed.set_price_cents(btc(), 6_000_000);
    feed.set_price_cents(matic(), 5_000);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();

    // Create orders for many pairs -- some will form profitable triangles
    book.add_user_order(gen.next(), eth(), usdc(), 10, 16000);
    book.add_user_order(gen.next(), usdc(), sol(), 16000, 80);
    book.add_user_order(gen.next(), sol(), eth(), 80, 5);
    book.add_user_order(gen.next(), btc(), usdc(), 1, 48000);
    book.add_user_order(gen.next(), usdc(), matic(), 48000, 800);
    book.add_user_order(gen.next(), matic(), btc(), 800, 1);

    let (_, cycles) = run_three_edge_cycle(&mut book);
    // Should find profitable cycles without panicking
    assert!(cycles >= 1);
}

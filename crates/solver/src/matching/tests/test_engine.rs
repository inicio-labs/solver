use crate::matching::engine::MatchingEngine;
use crate::matching::order_book::OrderBook;
use crate::price::WatchPriceFeed;
use super::{eth, usdc, sol, NoteIdGen};

fn make_feed() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed
}

#[test]
fn engine_basic() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1600);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert!(batch.cycles_executed > 0);
    assert!(!batch.filled_orders.is_empty());
}

#[test]
fn engine_empty() {
    let feed = make_feed();
    let book = OrderBook::new(feed);
    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert_eq!(batch.cycles_executed, 0);
    assert!(batch.filled_orders.is_empty());
}

#[test]
fn triangular_disabled_skips_3cycle_phase() {
    // Build a book where the ONLY possible match is a triangular cycle
    // (no direct counter-orders exist for any pair). With triangular enabled
    // the cycle should execute; with it disabled, nothing matches.
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);

    fn build_triangle_book(feed: WatchPriceFeed) -> OrderBook<WatchPriceFeed> {
        let mut book = OrderBook::new(feed);
        let mut gen = NoteIdGen::new();
        // Profitable triangle: offered_product (10*10*11) > requested_product (10*10*10).
        // Each leg goes in only one direction, so no direct (2-party) match is
        // possible — only the triangular phase can clear these.
        book.add_user_order(gen.next(), eth(), usdc(), 10, 10);  // ETH → USDC (10 ETH for 10 USDC)
        book.add_user_order(gen.next(), usdc(), sol(), 10, 10);  // USDC → SOL (10 USDC for 10 SOL)
        book.add_user_order(gen.next(), sol(), eth(), 11, 10);   // SOL → ETH (11 SOL for 10 ETH — surplus)
        book
    }

    // With triangular enabled (default), the cycle executes.
    let mut engine_with = MatchingEngine::new(build_triangle_book(make_feed_3()));
    let batch_with = engine_with.run();
    assert!(
        batch_with.cycles_executed > 0,
        "triangular enabled: expected at least one cycle"
    );

    // Same book, triangular disabled: no direct counter-orders exist for any
    // of the three pairs, so nothing should match.
    let mut engine_without = MatchingEngine::new(build_triangle_book(make_feed_3()))
        .with_triangular_enabled(false);
    let batch_without = engine_without.run();
    assert_eq!(
        batch_without.cycles_executed, 0,
        "triangular disabled: no cycles should execute when only a triangle is possible"
    );
    assert!(
        batch_without.filled_orders.is_empty(),
        "triangular disabled: no orders should be filled"
    );
}

fn make_feed_3() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 200_000);
    feed.set_price_cents(usdc(), 100);
    feed.set_price_cents(sol(), 15_000);
    feed
}

#[test]
fn engine_multi_pair() {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed.set_price_cents(sol(), 150);

    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1600);
    book.add_user_order(gen.next(), usdc(), sol(), 150, 1);
    book.add_user_order(gen.next(), sol(), usdc(), 1, 120);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert!(batch.cycles_executed >= 2);
    assert_eq!(batch.filled_orders.len(), 4);
}

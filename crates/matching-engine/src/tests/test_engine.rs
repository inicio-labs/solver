use crate::engine::MatchingEngine;
use crate::order_book::OrderBook;
use crate::price_feed::SimpleMapFeed;
use super::{eth, usdc, sol};

fn make_feed() -> SimpleMapFeed {
    let mut feed = SimpleMapFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed
}

#[test]
fn engine_basic() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 1600);

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
fn engine_multi_pair() {
    let mut feed = SimpleMapFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed.set_price_cents(sol(), 150);

    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 1600);
    book.add_user_order(usdc(), sol(), 150, 1);
    book.add_user_order(sol(), usdc(), 1, 120);

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();
    assert!(batch.cycles_executed >= 2);
    assert_eq!(batch.filled_orders.len(), 4);
}

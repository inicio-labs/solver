use crate::direct_matching::run_direct_matching;
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
fn basic_match() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 1600);

    let (filled, cycles) = run_direct_matching(&mut book);
    assert!(cycles > 0);
    assert_eq!(filled.len(), 2);
    assert_eq!(book.active_order_count(), 0);
}

#[test]
fn no_match_at_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 2000);

    let (_, cycles) = run_direct_matching(&mut book);
    assert_eq!(cycles, 0);
}

#[test]
fn multiple_matches() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 20000, 10);
    book.add_user_order(usdc(), eth(), 40000, 20);
    book.add_user_order(eth(), usdc(), 10, 16000);
    book.add_user_order(eth(), usdc(), 20, 32000);

    let (filled, cycles) = run_direct_matching(&mut book);
    assert!(cycles >= 2);
    assert_eq!(filled.len(), 4);
}

#[test]
fn partial_fill() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 20000, 10);
    book.add_user_order(eth(), usdc(), 1, 1600);

    let (filled, _) = run_direct_matching(&mut book);
    assert!(!filled.is_empty());
    assert!(book.active_order_count() >= 1, "larger order should remain partially filled");
}

#[test]
fn one_sided_no_match() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);

    let (_, cycles) = run_direct_matching(&mut book);
    assert_eq!(cycles, 0);
}

#[test]
fn surplus_to_protocol_balance() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 1600);

    let _ = run_direct_matching(&mut book);
    let usdc_balance = book.protocol_balances.get(&usdc()).copied().unwrap_or(0);
    assert!(usdc_balance > 0, "surplus should be in protocol balance");
}

#[test]
fn multiple_pairs() {
    let mut feed = SimpleMapFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed.set_price_cents(sol(), 150);

    let mut book = OrderBook::new(feed);
    book.add_user_order(usdc(), eth(), 2000, 1);
    book.add_user_order(eth(), usdc(), 1, 1600);
    book.add_user_order(usdc(), sol(), 150, 1);
    book.add_user_order(sol(), usdc(), 1, 120);

    let (_, cycles) = run_direct_matching(&mut book);
    assert!(cycles >= 2);
}

#[test]
fn filled_order_details() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let order_a_id = book.add_user_order(usdc(), eth(), 2000, 1).unwrap();
    let order_b_id = book.add_user_order(eth(), usdc(), 1, 1600).unwrap();

    let (filled, _) = run_direct_matching(&mut book);

    assert!(filled.contains(&order_a_id));
    assert!(filled.contains(&order_b_id));

    // Check order state directly from book
    let order_a = &book.orders[order_a_id as usize];
    assert!(order_a.is_completely_filled());
    assert_eq!(order_a.requested_filled(), 1); // filled all 1 ETH requested

    let order_b = &book.orders[order_b_id as usize];
    assert!(order_b.is_completely_filled());
    assert_eq!(order_b.requested_filled(), 1600); // filled all 1600 USDC requested
}

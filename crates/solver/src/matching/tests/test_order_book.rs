use crate::matching::order_book::OrderBook;
use crate::price::WatchPriceFeed;
use super::{eth, usdc, NoteIdGen};

fn make_feed() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed
}

#[test]
fn add_order_at_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    assert!(book.add_user_order(gen.next(), usdc(), eth(), 2000, 1));
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn add_order_below_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 3000 USDC, request 1 ETH -- offering more than oracle -> accepted
    assert!(book.add_user_order(gen.next(), usdc(), eth(), 3000, 1));
}

#[test]
fn accepts_order_above_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 1900 USDC, request 1 ETH -- order book accepts all valid orders;
    // profitability filtering is the matching engine's responsibility.
    assert!(book.add_user_order(gen.next(), usdc(), eth(), 1900, 1));
}

#[test]
fn best_order_returns_cheapest() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1); // rate 1/3000 -- cheaper
    book.add_user_order(gen.next(), usdc(), eth(), 2500, 1); // rate 1/2500

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(best.offered, 3000, "most generous order should be 3000:1");
}

#[test]
fn best_order_returns_best_rate() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1600); // rate 1600
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1800); // rate 1800
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1500); // rate 1500 -- best (lowest rate = most generous)

    let best = book.best_order(eth(), usdc()).unwrap();
    assert_eq!(best.requested, 1500, "best order should have lowest rate (1500)");
}

#[test]
fn best_order_skips_inactive() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    assert!(book.add_user_order(id, usdc(), eth(), 2000, 1));
    book.orders.get_mut(&id).unwrap().full_fill();
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1);

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(best.offered, 3000);
}

#[test]
fn cleanup_removes_inactive() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    assert!(book.add_user_order(id, usdc(), eth(), 2000, 1));
    book.orders.get_mut(&id).unwrap().full_fill();
    book.cleanup_order(id);
    assert_eq!(book.active_order_count(), 0);
}

#[test]
fn protocol_balance_add_deduct() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_protocol_balance(eth(), 5);
    assert_eq!(book.protocol_balances[&eth()], 5);
    book.deduct_protocol_balance(eth(), 2);
    assert_eq!(book.protocol_balances[&eth()], 3);
}

#[test]
#[should_panic(expected = "protocol balance underflow")]
fn protocol_balance_underflow() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_protocol_balance(eth(), 1);
    book.deduct_protocol_balance(eth(), 2);
}

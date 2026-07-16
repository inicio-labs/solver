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
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn add_order_below_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 3000 USDC, request 1 ETH -- offering more than oracle -> accepted
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1);
}

#[test]
fn accepts_order_above_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 1900 USDC, request 1 ETH -- order book accepts all valid orders;
    // profitability filtering is the matching engine's responsibility.
    book.add_user_order(gen.next(), usdc(), eth(), 1900, 1);
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
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.orders.get_mut(&id).unwrap().full_fill();
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1);

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(best.offered, 3000);
}

#[test]
fn best_level_none_when_empty() {
    let book = OrderBook::new(make_feed());
    assert!(book.best_level(usdc(), eth()).is_none());
}

#[test]
fn best_level_single_order_volume() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 2000);
    assert_eq!(vol, 2000); // offered_remaining of the single order
}

#[test]
fn best_level_sums_volume_at_same_rate() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 4000, 2); // same rate (2/4000)
    let (_rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(vol, 6000, "summed offered_remaining of both orders at the rate");
}

#[test]
fn best_level_returns_best_rate_only() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1); // rate 1/3000 — best
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 3000, "most generous level");
    assert_eq!(vol, 3000, "only the best level's volume, not the 2000 level");
}

#[test]
fn best_level_skips_inactive_front() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 3000, 1); // best rate, but filled below
    book.orders.get_mut(&id).unwrap().full_fill();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // next active level
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 2000, "skips the fully-filled best level");
    assert_eq!(vol, 2000);
}

#[test]
fn snapshot_best_levels_covers_each_directed_pair() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1800);
    let snap = book.snapshot_best_levels();
    assert_eq!(snap.len(), 2);
    assert!(snap.contains_key(&(usdc(), eth())));
    assert!(snap.contains_key(&(eth(), usdc())));
}

#[test]
fn cleanup_removes_inactive() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.orders.get_mut(&id).unwrap().full_fill();
    book.cleanup_if_filled(id);
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
fn protocol_balance_saturates_at_zero() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_protocol_balance(eth(), 1);
    book.deduct_protocol_balance(eth(), 2);
    assert_eq!(book.protocol_balances[&eth()], 0);
}

#[test]
fn fifo_at_same_rate_returns_older_order_first() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let older = gen.next();
    let newer = gen.next();
    // Two same-rate orders: same offered/requested, inserted in order.
    book.add_user_order(older, usdc(), eth(), 2000, 1);
    book.add_user_order(newer, usdc(), eth(), 2000, 1);

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(
        best.id, older,
        "FIFO: oldest order at the same rate should fill first (price-time priority)"
    );
}

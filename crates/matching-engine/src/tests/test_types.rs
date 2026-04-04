use crate::types::*;
use super::{eth, usdc, make_note_id};

fn make_order(id: OrderId, offered_token: TokenId, requested_token: TokenId, offered: Amount, requested: Amount) -> Order {
    Order { id, offered_token, requested_token, offered, requested, requested_remaining: requested }
}

#[test]
fn match_self_bottleneck() {
    let mut order_a = make_order(make_note_id(0), eth(), usdc(), 100, 50);
    let mut order_b = make_order(make_note_id(1), usdc(), eth(), 200, 200);

    let _result = order_a.match_with(&mut order_b).unwrap();
    assert!(order_a.is_completely_filled(), "order_a should be fully filled");
    assert!(order_b.is_active(), "order_b should be partially filled");
}

#[test]
fn match_other_bottleneck() {
    let mut order_a = make_order(make_note_id(0), eth(), usdc(), 1000, 500);
    let mut order_b = make_order(make_note_id(1), usdc(), eth(), 50, 30);

    let _result = order_a.match_with(&mut order_b).unwrap();
    assert!(order_b.is_completely_filled(), "order_b should be fully filled");
    assert!(order_a.is_active(), "order_a should be partially filled");
}

#[test]
fn match_inactive() {
    let mut order_a = make_order(make_note_id(0), eth(), usdc(), 100, 50);
    order_a.requested_remaining = 0;
    let mut order_b = make_order(make_note_id(1), usdc(), eth(), 200, 200);
    assert!(order_a.match_with(&mut order_b).is_none());
}

#[test]
fn profitable_check() {
    let order_a = make_order(make_note_id(0), eth(), usdc(), 2000, 1);
    let good = make_order(make_note_id(1), usdc(), eth(), 1, 1600);
    let even = make_order(make_note_id(2), usdc(), eth(), 1, 2000);
    let bad = make_order(make_note_id(3), usdc(), eth(), 1, 2100);

    assert!(order_a.is_profitable_with(&good));
    assert!(!order_a.is_profitable_with(&even));
    assert!(!order_a.is_profitable_with(&bad));
}

#[test]
fn order_fill_and_remaining() {
    let mut order = make_order(make_note_id(0), eth(), usdc(), 2000, 10);
    assert!(order.is_active());
    assert!(!order.is_completely_filled());
    assert_eq!(order.offered_remaining(), 2000);
    assert_eq!(order.offered_for(5), 1000);
    assert_eq!(order.requested_filled(), 0);

    let released = order.fill(5);
    assert_eq!(released, 1000);
    assert_eq!(order.requested_remaining, 5);
    assert_eq!(order.requested_filled(), 5);
    assert!(order.is_active());

    let released = order.full_fill();
    assert_eq!(released, 1000);
    assert!(order.is_completely_filled());
    assert_eq!(order.requested_filled(), 10);
}

#[test]
fn calculate_output_via_offered_for() {
    let order = make_order(make_note_id(0), eth(), usdc(), 2000, 1);
    assert_eq!(order.offered_for(1), 2000);
    assert_eq!(order.offered_for(0), 0);

    let order2 = make_order(make_note_id(0), eth(), usdc(), 200, 10);
    assert_eq!(order2.offered_for(5), 100);

    let order3 = make_order(make_note_id(0), eth(), usdc(), 10, 200);
    assert_eq!(order3.offered_for(100), 5);
}

#[test]
fn requested_for_inverse() {
    let order = make_order(make_note_id(0), eth(), usdc(), 2000, 10);
    // offered_for(5) = 1000, so requested_for(1000) should ~ 5
    assert_eq!(order.requested_for(1000), 5);
}

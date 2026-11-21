//! Fixed-price clearing of PSWAPs whose requested and offered amounts round in opposite directions.

use super::{assets, batch_with_fee, clear, order};
use crate::clearing::{
    BatchPrice, ClearingConfig, ClearingOutcome, Order, PairAmounts, SettlementPlan,
};
use miden_protocol::{asset::FungibleAsset, crypto::rand::RandomCoin, note::Note, Word};

// Each side lists (offered, requested, minimum fill step), in arrival order.
fn book(sells: &[(u64, u64, u64)], buys: &[(u64, u64, u64)]) -> Vec<Order> {
    let (base, quote) = assets();
    let mut rng = RandomCoin::new(Word::default());
    let mut orders = Vec::new();
    for (side, offered_token, requested_token) in [(sells, base, quote), (buys, quote, base)] {
        for &(offered, requested, minimum) in side {
            orders.push(order(
                FungibleAsset::new(offered_token, offered).unwrap(),
                FungibleAsset::new(requested_token, requested).unwrap(),
                minimum,
                orders.len() as u64 + 1,
                &mut rng,
            ));
        }
    }
    orders
}

// Replay the protocol's fill, including the minimum step, and measure the assets it releases.
fn released(order: &Order, payment: FungibleAsset) -> u64 {
    assert_eq!(order.requested_asset().faucet_id(), payment.faucet_id());
    let (_, remainder) = order
        .pswap_note()
        .execute(order.note().metadata().sender(), None, Some(payment))
        .unwrap();
    order.offered_asset().amount().as_u64()
        - remainder.map_or(0, |note| note.offered_asset().amount().as_u64())
}

fn outcome(orders: &[Order], price: (u64, u64), fee_ppm: u32) -> ClearingOutcome {
    let batch = batch_with_fee(
        BatchPrice::from_ratio(price.0, price.1).unwrap(),
        orders,
        fee_ppm,
    );
    let result = clear(
        &batch,
        &ClearingConfig {
            protocol_fee_ppm: fee_ppm,
            ..Default::default()
        },
    );
    if let ClearingOutcome::Accepted(plan) = &result {
        // Conversion checks that each execution corresponds to a distinct order in this book.
        plan.to_execution_batch(&batch).unwrap();
        let (base, _) = assets();
        let mut paid = PairAmounts::default();
        let mut unlocked = PairAmounts::default();
        for fill in &plan.candidate.executions {
            let order = orders
                .iter()
                .find(|order| order.id() == fill.order_id)
                .unwrap();
            let payout = released(order, fill.payment);
            assert_eq!(
                fill.release,
                FungibleAsset::new(order.offered_asset().faucet_id(), payout).unwrap()
            );
            if fill.release.faucet_id() == base {
                unlocked.base += u128::from(payout);
                paid.quote += u128::from(fill.payment.amount().as_u64());
            } else {
                unlocked.quote += u128::from(payout);
                paid.base += u128::from(fill.payment.amount().as_u64());
            }
        }
        assert_eq!(plan.candidate.released, unlocked);
        assert_eq!(plan.candidate.paid, paid);
        let fee = &plan.accruals.realized_protocol_fee;
        let surplus = &plan.accruals.rounding_surplus;
        assert_eq!(unlocked.base, paid.base + fee.base + surplus.base);
        assert_eq!(unlocked.quote, paid.quote + fee.quote + surplus.quote);
    }
    result
}

fn accepted(orders: &[Order], price: (u64, u64), fee_ppm: u32) -> Box<SettlementPlan> {
    match outcome(orders, price, fee_ppm) {
        ClearingOutcome::Accepted(plan) => plan,
        ClearingOutcome::Skipped(reason) => panic!("expected a funded settlement, got {reason:?}"),
    }
}

fn payment(plan: &SettlementPlan, order: &Order) -> u64 {
    plan.candidate
        .executions
        .iter()
        .find(|fill| fill.order_id == order.id())
        .expect("order should participate in this settlement")
        .payment
        .amount()
        .as_u64()
}

#[test]
fn five_quote_for_three_base_can_be_funded_entirely_by_the_two_orders() {
    let orders = book(&[(10, 14, 0)], &[(5, 3, 0)]);
    let (base, quote) = assets();
    assert_eq!(
        released(&orders[0], FungibleAsset::new(quote, 5).unwrap()),
        3
    );
    assert_eq!(
        released(&orders[1], FungibleAsset::new(base, 3).unwrap()),
        5
    );
}

#[test]
fn four_quote_for_two_base_already_clears_in_full() {
    let orders = book(&[(10, 14, 0)], &[(4, 2, 0)]);
    let plan = accepted(&orders, (3, 2), 50);
    assert_eq!(payment(&plan, &orders[1]), 2);
}

#[test]
fn an_all_or_nothing_buyer_cannot_receive_three_base_from_a_two_base_book() {
    let orders = book(&[(2, 2, 0)], &[(5, 3, 3)]);
    assert!(matches!(
        outcome(&orders, (3, 2), 50),
        ClearingOutcome::Skipped(_)
    ));
}

fn remainder_after_124_then_14() -> Order {
    let root = book(&[], &[(89_131, 139, 0)]).remove(0);
    let consumer = root.note().metadata().sender();
    let token = root.requested_asset().faucet_id();
    let (_, first) = root
        .pswap_note()
        .execute(
            consumer,
            None,
            Some(FungibleAsset::new(token, 124).unwrap()),
        )
        .unwrap();
    let first = first.unwrap();
    assert_eq!(
        (
            first.offered_asset().amount().as_u64(),
            first.storage().min_requested_amount()
        ),
        (9_619, 15)
    );
    let (_, tail) = first
        .execute(consumer, None, Some(FungibleAsset::new(token, 14).unwrap()))
        .unwrap();
    let tail: Note = tail.unwrap().into();
    Order::from_note(&tail, 2).unwrap()
}

#[test]
fn paying_124_then_14_leaves_642_quote_for_one_base() {
    let tail = remainder_after_124_then_14();
    assert_eq!(tail.offered_asset().amount().as_u64(), 642);
    assert_eq!(tail.requested_asset().amount().as_u64(), 1);
    assert_eq!(released(&tail, tail.requested_asset()), 642);
}

mod integer_completion_goals {
    use super::*;

    #[test]
    #[ignore = "fixed-price clearing leaves a funded three-base order partially filled"]
    fn completes_five_quote_for_three_base() {
        let orders = book(&[(10, 14, 0)], &[(5, 3, 0)]);
        for fee_ppm in [0, 50] {
            let plan = accepted(&orders, (3, 2), fee_ppm);
            assert_eq!(payment(&plan, &orders[1]), 3);
        }
    }

    #[test]
    #[ignore = "fixed-price clearing skips a funded final unit"]
    fn completes_two_quote_for_one_base() {
        let orders = book(&[(8, 11, 0)], &[(2, 1, 0)]);
        let plan = accepted(&orders, (3, 2), 50);
        assert_eq!(payment(&plan, &orders[1]), 1);
    }

    #[test]
    #[ignore = "fixed-price clearing falls short of full payment with a nonzero fee"]
    fn completes_at_the_full_price_and_collects_the_50_ppm_fee() {
        let orders = book(&[(158_627, 100_937, 0)], &[(14_520, 22_813, 0)]);
        let plan = accepted(&orders, (7, 11), 50);
        // 14,520 quote at 7/11 buys 22,817 whole base units before the one-unit fee.
        assert_eq!(payment(&plan, &orders[1]), 22_816);
        let expected = PairAmounts { base: 1, quote: 0 };
        assert_eq!(plan.candidate.protocol_fee_target, expected);
        assert_eq!(plan.accruals.realized_protocol_fee, expected);
    }

    #[test]
    #[ignore = "fixed-price clearing leaves the buyer partially filled across two eligible sellers"]
    fn completes_the_buyer_using_both_sellers() {
        // The better-priced seller goes first and has only two of the three base units needed.
        let orders = book(&[(2, 2, 0), (10, 14, 0)], &[(5, 3, 0)]);
        for fee_ppm in [0, 50] {
            let plan = accepted(&orders, (3, 2), fee_ppm);
            assert!(payment(&plan, &orders[0]) > 0);
            assert!(payment(&plan, &orders[1]) > 0);
            assert_eq!(payment(&plan, &orders[2]), 3);
        }
    }

    #[test]
    #[ignore = "fixed-price clearing leaves the seller partially filled across two eligible buyers"]
    fn completes_the_seller_while_both_buyers_meet_their_minimums() {
        let orders = book(&[(60, 80, 0)], &[(26, 15, 8), (69, 47, 42)]);
        for fee_ppm in [0, 50] {
            let plan = accepted(&orders, (137, 100), fee_ppm);
            // A full sale of 60 base pays floor(60 * 1.37) quote. The fee rounds to zero.
            assert_eq!(payment(&plan, &orders[0]), 82);
            assert!(payment(&plan, &orders[1]) >= 8);
            assert!(payment(&plan, &orders[2]) >= 42);
        }
    }

    #[test]
    #[ignore = "fixed-price clearing skips the funded one-base remainder after two partial fills"]
    fn completes_the_remainder_after_paying_124_then_14() {
        // The public note terms determine the remainder. The counterparty is synthetic.
        let mut orders = book(&[(100, 64_101, 0)], &[]);
        orders.push(remainder_after_124_then_14());
        let plan = accepted(&orders, (1283, 2), 50);
        assert_eq!(payment(&plan, &orders[1]), 1);
    }
}

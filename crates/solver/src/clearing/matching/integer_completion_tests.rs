use super::*;
use crate::clearing::{Order, OrderSide};
use miden_protocol::{
    asset::FungibleAsset,
    crypto::rand::{FeltRng, RandomCoin},
    note::{Note, NoteType},
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    },
    Word,
};
use miden_standards::note::{PswapNote, PswapNoteStorage};

pub(super) fn book(specs: &[(bool, u64, u64, u64)]) -> Vec<Order> {
    let base = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let quote = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
    let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
        .try_into()
        .unwrap();
    let mut rng = RandomCoin::new(Word::default());
    specs
        .iter()
        .enumerate()
        .map(|(i, &(sell, offered, requested, minimum))| {
            let note: Note = PswapNote::builder()
                .sender(creator)
                .storage(
                    PswapNoteStorage::builder()
                        .min_requested_asset(
                            FungibleAsset::new(if sell { quote } else { base }, requested).unwrap(),
                        )
                        .min_fill_step(AssetAmount::new(minimum).unwrap())
                        .creator_account_id(creator)
                        .build(),
                )
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(
                    FungibleAsset::new(if sell { base } else { quote }, offered).unwrap(),
                )
                .build()
                .unwrap()
                .into();
            Order::from_note(&note, i as u64 + 1).unwrap()
        })
        .collect()
}

pub(super) fn prepared(orders: &[Order], price: BatchPrice, fee: u32) -> PairBatch<'_> {
    let base = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let mut sides = [Vec::new(), Vec::new()];
    for order in orders {
        let sell = order.offered_asset().faucet_id() == base;
        let side = if sell {
            OrderSide::SellBase
        } else {
            OrderSide::BuyBase
        };
        if let Some(order) = order.prepare_if_eligible(side, price, fee).unwrap() {
            sides[usize::from(!sell)].push(order);
        }
    }
    for side in &mut sides {
        side.sort_by_key(|o| o.order().index_key().1);
    }
    let [sells, buys] = sides;
    PairBatch::new(price, sells, buys)
}

pub(super) fn enabled(fee: u32) -> ClearingConfig {
    ClearingConfig {
        protocol_fee_ppm: fee,
        integer_completion_repair: true,
        ..Default::default()
    }
}
fn accepted(
    batch: &PairBatch<'_>,
    config: &ClearingConfig,
) -> Box<super::super::types::SettlementPlan> {
    match PairMatcher::new(batch, config).clear().unwrap() {
        ClearingOutcome::Accepted(plan) => plan,
        other => panic!("expected completion, got {other:?}"),
    }
}

#[test]
fn integer_repair_completes_original_trade() {
    let orders = book(&[(true, 10, 14, 0), (false, 5, 3, 0)]);
    let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    let plan = accepted(&batch, &enabled(50));
    assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 3);
    assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 5);
    assert_eq!(plan.candidate.paid, plan.candidate.released);
}

#[test]
fn integer_repair_completes_final_unit_on_the_same_book() {
    let orders = book(&[(true, 8, 11, 0), (false, 2, 1, 0)]);
    let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    let plan = accepted(&batch, &enabled(50));
    assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 1);
    assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 2);
}

#[test]
fn integer_repair_preserves_nonzero_50ppm_fee_and_full_price() {
    let orders = book(&[(true, 158_627, 100_937, 0), (false, 14_520, 22_813, 0)]);
    let batch = prepared(&orders, BatchPrice::from_ratio(7, 11).unwrap(), 50);
    let plan = accepted(&batch, &enabled(50));
    assert_eq!(
        plan.candidate.executions[0].payment.amount().as_u64(),
        14_519
    );
    assert_eq!(
        plan.candidate.executions[1].payment.amount().as_u64(),
        22_816
    );
    assert_eq!(plan.accruals.realized_protocol_fee.base, 1);
    assert_eq!(
        plan.accruals.realized_protocol_fee,
        plan.candidate.protocol_fee_target
    );
}

#[test]
fn integer_repair_zero_fee_and_multiple_positive_inputs() {
    let orders = book(&[(true, 2, 2, 0), (true, 10, 14, 0), (false, 5, 3, 0)]);
    for fee in [0, 50] {
        let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), fee);
        let plan = accepted(&batch, &enabled(fee));
        assert_eq!(plan.candidate.executions.len(), 3);
        assert_eq!(plan.candidate.executions[2].payment.amount().as_u64(), 3);
        assert_eq!(
            plan.accruals.realized_protocol_fee,
            plan.candidate.protocol_fee_target
        );
    }
}

#[test]
fn integer_repair_actual_124_plus_14_lineage_uses_current_tail_terms() {
    let root = book(&[(false, 89_131, 139, 0)]).remove(0);
    let consumer = root.note().metadata().sender();
    let token = root.requested_asset().faucet_id();
    let (first_payback, first) = root
        .pswap_note()
        .execute(
            consumer,
            None,
            Some(FungibleAsset::new(token, 124).unwrap()),
        )
        .unwrap();
    let first = first.unwrap();
    assert_eq!(first.offered_asset().amount().as_u64(), 9_619);
    assert_eq!(first.storage().min_requested_amount(), 15);
    let (second_payback, tail) = first
        .execute(consumer, None, Some(FungibleAsset::new(token, 14).unwrap()))
        .unwrap();
    let tail = tail.unwrap();
    assert_eq!(tail.offered_asset().amount().as_u64(), 642);
    assert_eq!(tail.storage().min_requested_amount(), 1);
    assert_eq!(first_payback.assets().iter().count(), 1);
    assert_eq!(second_payback.assets().iter().count(), 1);
    // Synthetic same-book counterparty, not a reconstruction of private
    // historical maker terms. Only the public note-model lineage is actual.
    let maker = book(&[(true, 100, 64_101, 0)]).remove(0);
    let tail_note: Note = tail.into();
    let orders = vec![maker, Order::from_note(&tail_note, 2).unwrap()];
    let batch = prepared(&orders, BatchPrice::from_ratio(1283, 2).unwrap(), 50);
    let old = PairMatcher::new(
        &batch,
        &ClearingConfig {
            protocol_fee_ppm: 50,
            ..Default::default()
        },
    )
    .clear()
    .unwrap();
    assert!(matches!(old, ClearingOutcome::Skipped(_)));
    let plan = accepted(&batch, &enabled(50));
    assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 642);
    assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 1);
    let (_, remainder) = orders[1]
        .pswap_note()
        .execute(consumer, None, Some(plan.candidate.executions[1].payment))
        .unwrap();
    assert!(remainder.is_none());
}

#[test]
fn integer_repair_disabled_and_insufficient_funds_retain_legacy() {
    let orders = book(&[(true, 10, 14, 0), (false, 5, 3, 0)]);
    let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    let old = accepted(
        &batch,
        &ClearingConfig {
            protocol_fee_ppm: 50,
            ..Default::default()
        },
    );
    assert_eq!(old.candidate.executions[0].payment.amount().as_u64(), 3);
    assert_eq!(old.candidate.executions[1].payment.amount().as_u64(), 2);
    let no_funds = book(&[(true, 10, 14, 0), (false, 4, 2, 0)]);
    let batch = prepared(&no_funds, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    let legacy = PairMatcher::new(
        &batch,
        &ClearingConfig {
            protocol_fee_ppm: 50,
            ..Default::default()
        },
    )
    .clear()
    .unwrap();
    let rejected = PairMatcher::new(&batch, &enabled(50)).clear().unwrap();
    assert_eq!(legacy_as_values(&legacy), legacy_as_values(&rejected));
}

fn legacy_as_values(outcome: &ClearingOutcome) -> String {
    format!("{outcome:?}")
}

#[test]
fn integer_repair_completes_the_quote_deficit_direction_with_minimums() {
    let orders = book(&[(true, 60, 80, 0), (false, 26, 15, 8), (false, 69, 47, 42)]);
    for fee in [0, 50] {
        let batch = prepared(&orders, BatchPrice::from_ratio(137, 100).unwrap(), fee);
        let old = accepted(
            &batch,
            &ClearingConfig {
                protocol_fee_ppm: fee,
                ..Default::default()
            },
        );
        assert_eq!(old.candidate.executions[0].payment.amount().as_u64(), 78);
        let plan = accepted(&batch, &enabled(fee));
        assert_eq!(
            plan.candidate
                .executions
                .iter()
                .map(|e| e.payment.amount().as_u64())
                .collect::<Vec<_>>(),
            vec![82, 13, 42]
        );
        assert_eq!(
            plan.candidate
                .executions
                .iter()
                .map(|e| e.release.amount().as_u64())
                .collect::<Vec<_>>(),
            vec![60, 22, 61]
        );
        assert_eq!(
            plan.accruals.rounding_surplus,
            crate::clearing::PairAmounts { base: 5, quote: 1 }
        );
    }
}

#[test]
fn aggregate_quote_still_misses_a_same_book_minimum_skip_in_both_directions() {
    use crate::matching::types::{BookLevel, RateKey};
    use crate::swap_eta::{judge, FillStatus, ProposedOrder, VerdictTerms};
    let price = BatchPrice::from_ratio(1, 1).unwrap();
    for sell in [true, false] {
        let orders = book(&[
            (sell, 1000, 995, 600),
            (sell, 200, 199, 0),
            (!sell, 500, 499, 0),
        ]);
        let batch = prepared(&orders, price, 50);
        let plan = accepted(&batch, &enabled(50));
        assert!(!plan
            .candidate
            .executions
            .iter()
            .any(|e| e.order_id == orders[0].id()));
        let probe = plan
            .candidate
            .executions
            .iter()
            .find(|e| e.order_id == orders[1].id())
            .unwrap();
        assert_eq!(probe.payment.amount().as_u64(), 200);
        let quote = judge(
            if sell {
                OrderSide::SellBase
            } else {
                OrderSide::BuyBase
            },
            price,
            VerdictTerms {
                fee_ppm: 50,
                tolerance_bps: 0,
            },
            ProposedOrder {
                offered: 200,
                requested: 199,
                min_fill_step: 0,
            },
            &[BookLevel {
                rate: RateKey::new(995, 1000),
                volume: 1000,
            }],
            &[BookLevel {
                rate: RateKey::new(499, 500),
                volume: 500,
            }],
        )
        .unwrap();
        assert_eq!(quote.status, FillStatus::None);
        // This characterizes the separate estimator limitation. The repair
        // does not change quote endpoints or make aggregate estimates exact.
    }
}

#[test]
fn integer_repair_configuration_is_explicit_and_default_off() {
    assert!(!ClearingConfig::default().integer_completion_repair);
    let old_config = "pulse_interval_ms = 5000\nfetch_interval_ms = 3000\n";
    let old: crate::config::EngineConfig = toml::from_str(old_config).unwrap();
    assert!(!old.clearing_integer_completion_repair);
    let on: crate::config::EngineConfig = toml::from_str(&format!(
        "{old_config}clearing_integer_completion_repair = true\n"
    ))
    .unwrap();
    assert!(on.clearing_integer_completion_repair);
    assert!(ClearingConfig {
        protocol_fee_ppm: 1_000_000,
        ..Default::default()
    }
    .validate()
    .is_err());
}

#[test]
fn supported_book_performance_samples() {
    use std::time::Instant;
    let mut samples = Vec::new();
    // Supported 100+100 shapes: dense, sparse/AON, extreme ratios and a
    // resource-trimming workload. Measured timings have no asserted SLO.
    for (name, price, sell, buy, cap) in [
        ("dense", (3, 2), (10, 14, 0), (5, 3, 0), 50_000),
        (
            "sparse_minimum",
            (137, 100),
            (60, 80, 70),
            (26, 15, 8),
            50_000,
        ),
        (
            "extreme_ratio",
            (1283, 2),
            (100, 64101, 0),
            (642, 1, 0),
            50_000,
        ),
        ("trimmed", (3, 2), (10, 14, 14), (5, 3, 3), 128),
    ] {
        let specs = (0..100)
            .map(|_| (true, sell.0, sell.1, sell.2))
            .chain((0..100).map(|_| (false, buy.0, buy.1, buy.2)))
            .collect::<Vec<_>>();
        let orders = book(&specs);
        let price = BatchPrice::from_ratio(price.0, price.1).unwrap();
        let batch = prepared(&orders, price, 50);
        let mut micros = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            let result = PairMatcher::new(
                &batch,
                &ClearingConfig {
                    max_total_intervals: cap,
                    ..enabled(50)
                },
            )
            .clear();
            assert!(result.is_ok());
            micros.push(start.elapsed().as_micros());
        }
        micros.sort();
        samples.push((name, micros[0], micros[10], micros[19]));
    }
    println!(
        "repair enabled debug build, 100+100 books; (shape,min_us,median_us,max_us): {samples:?}"
    );
}

use super::super::integer_completion_tests::{book, enabled, prepared};
use super::*;
use crate::clearing::Order;

fn with_repair(
    orders: &[Order],
    price: BatchPrice,
    config: &ClearingConfig,
    f: impl FnOnce(&Repair<'_, '_>),
) {
    let batch = prepared(orders, price, config.protocol_fee_ppm);
    if batch.sell_orders.is_empty() || batch.buy_orders.is_empty() {
        return;
    }
    let sm = ReachableFillMap::build(
        &batch.sell_orders,
        config.max_total_intervals,
        PairMatcher::total_offered(&batch.buy_orders).unwrap(),
    )
    .unwrap();
    let bm = ReachableFillMap::build(
        &batch.buy_orders,
        config.max_total_intervals,
        PairMatcher::total_offered(&batch.sell_orders).unwrap()
            + u128::from(AssetAmount::MAX.as_u64()),
    )
    .unwrap();
    let Some((b, s)) = sm.maximum_common_fill(&bm, price, U256::MAX).unwrap() else {
        return;
    };
    let sa = sm.allocate_by_priority(&batch.sell_orders, s).unwrap();
    let ba = bm.allocate_by_priority(&batch.buy_orders, b).unwrap();
    let mut ledger = PairLedger::default();
    let mut es = Vec::new();
    for (orders, cs) in [(&batch.sell_orders, &sa), (&batch.buy_orders, &ba)] {
        for (o, &c) in orders.iter().zip(cs) {
            if let Some(e) = o.execution(c, config.protocol_fee_ppm).unwrap() {
                ledger.record(o.side(), &e).unwrap();
                es.push(e);
            }
        }
    }
    let anchor = Anchor {
        seller_allocations: sa,
        buyer_allocations: ba,
        candidate: ledger.into_plan(es),
    };
    let mut off = *config;
    off.integer_completion_repair = false;
    let legacy = PairMatcher::new(&batch, &off).clear().unwrap();
    f(&Repair {
        batch: &batch,
        config,
        sells: &batch.sell_orders,
        buys: &batch.buy_orders,
        seller_fills: &sm,
        buyer_fills: &bm,
        anchor: &anchor,
        legacy: &legacy,
    });
}

#[test]
fn work_caps_and_failed_final_winner_never_return_partial_search_winner() {
    let orders = book(&[(true, 2, 2, 0), (true, 10, 14, 0), (false, 5, 3, 0)]);
    with_repair(
        &orders,
        BatchPrice::from_ratio(3, 2).unwrap(),
        &enabled(50),
        |r| {
            assert!(r.run().is_some());
            for limits in [
                WorkLimits {
                    orders: 2,
                    ..Default::default()
                },
                WorkLimits {
                    donors: 1,
                    ..Default::default()
                },
                WorkLimits {
                    intervals: 49_999,
                    ..Default::default()
                },
                WorkLimits {
                    candidates: 1,
                    ..Default::default()
                },
                WorkLimits {
                    executions: 3,
                    ..Default::default()
                },
                WorkLimits {
                    priority_visits: 2,
                    ..Default::default()
                },
            ] {
                assert!(r.run_with(limits, |_| Some(())).is_none());
            }
            let mut calls = 0;
            assert!(r
                .run_with(Default::default(), |_| {
                    calls += 1;
                    None
                })
                .is_none());
            assert_eq!(calls, 1);
        },
    );
}

#[test]
fn exact_threshold_declines_full_price_and_fee_cliff() {
    for (offered, requested, fee) in [
        (110, 180, 100_000),
        (110_000, 180_000, 50),
        (110_000, 180_000, 0),
    ] {
        let orders = book(&[(true, offered, requested, 0)]);
        let batch = prepared(&orders, BatchPrice::from_ratio(2, 1).unwrap(), fee);
        let order = &batch.sell_orders[0];
        let maximum = order.fill_interval().maximum;
        // Find the last partial payment below R. Full canonical payment jumps
        // above exact T=R, so the donor must be declined, even when solvent.
        let old = (maximum - U256::ONE) * U256::from(requested - 1) / U256::from(requested);
        let old = order.execution(old, fee).unwrap().unwrap();
        let missing = u128::from(offered - old.release.amount().as_u64());
        assert!(order
            .release_threshold(old.comparison_fill, missing, fee)
            .unwrap()
            .is_none());
    }
}

#[test]
fn exact_inverse_matches_exhaustive_payment_and_release_thresholds() {
    let mut checked = 0;
    for offered in 2..=15u64 {
        for requested in 1..=20u64 {
            let orders = book(&[(true, offered, requested, 0)]);
            let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 0);
            for order in &batch.sell_orders {
                let maximum = u64::try_from(order.fill_interval().maximum).unwrap();
                for old in 1..maximum {
                    let e = order.execution(U256::from(old), 0).unwrap().unwrap();
                    for missing in 1..=offered - e.release.amount().as_u64() {
                        let t =
                            ((e.release.amount().as_u64() + missing) * requested).div_ceil(offered);
                        // Enumerate comparison integers independently of inverse
                        // formula. Actual full completion uses only Cmax.
                        let expected = (old + 1..=maximum)
                            .find(|&c| {
                                let e = order.execution(U256::from(c), 0).unwrap().unwrap();
                                e.payment.amount().as_u64() == t && (t < requested || c == maximum)
                            })
                            .map(U256::from);
                        assert_eq!(
                            order
                                .release_threshold(U256::from(old), u128::from(missing), 0)
                                .unwrap(),
                            expected
                        );
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 10_000);
}

#[test]
fn already_solvent_minimum_aon_and_outside_work_bounds_preserve_exact_legacy() {
    for specs in [
        vec![(true, 10, 10, 0), (false, 10, 10, 0)],
        vec![(true, 10, 14, 14), (false, 5, 3, 3)],
        vec![(true, 10, 14, 99), (false, 5, 3, 99)],
        vec![(true, 10, 14, 0), (false, 4, 2, 0)],
    ] {
        let orders = book(&specs);
        for cap in [1, 3, 50_000, 50_001] {
            let cfg = ClearingConfig {
                max_total_intervals: cap,
                ..enabled(50)
            };
            let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
            let mut off = cfg;
            off.integer_completion_repair = false;
            let legacy = PairMatcher::new(&batch, &off).clear();
            let candidate = PairMatcher::new(&batch, &cfg).clear();
            assert_eq!(format!("{legacy:?}"), format!("{candidate:?}"));
        }
    }
}

// Independent small-book oracle: enumerate every integer comparison for the
// single positive donor, then enforce the written exact-T neighborhood and
// source execution/priority contracts. No inverse helper or repair predicate.
fn oracle(r: &Repair<'_, '_>) -> Option<SettlementPlan> {
    let a = &r.anchor.candidate;
    let db = a.paid.base.saturating_sub(a.released.base);
    let dq = a.paid.quote.saturating_sub(a.released.quote);
    let (donors, allocations, map, sell, missing) = match (db, dq) {
        (d, 0) if d > 0 => (
            r.sells,
            &r.anchor.seller_allocations,
            r.seller_fills,
            true,
            d,
        ),
        (0, d) if d > 0 => (r.buys, &r.anchor.buyer_allocations, r.buyer_fills, false, d),
        _ => return None,
    };
    let old = match r.legacy {
        ClearingOutcome::Accepted(p) => Some(p),
        _ => None,
    };
    let old_executions = old
        .into_iter()
        .flat_map(|p| &p.candidate.executions)
        .collect::<Vec<_>>();
    let extra = |es: &[OrderExecution]| {
        es.iter().any(|e| {
            let o = r
                .sells
                .iter()
                .chain(r.buys)
                .find(|o| o.order().id() == e.order_id)
                .unwrap();
            complete(e, o)
                && !old_executions
                    .iter()
                    .any(|old| old.order_id == e.order_id && complete(old, o))
        })
    };
    if !extra(&a.executions) {
        return None;
    }
    let mut candidates = Vec::new();
    for (j, o) in donors.iter().enumerate() {
        let c0 = u64::try_from(allocations[j]).unwrap();
        if c0 == 0 {
            continue;
        }
        let e0 = o
            .execution(allocations[j], r.config.protocol_fee_ppm)
            .unwrap()
            .unwrap();
        let off = u128::from(o.offered_amount().as_u64());
        let req = u128::from(o.order().requested_asset().amount().as_u64());
        let needed = u128::from(e0.release.amount().as_u64()) + missing;
        if needed > off {
            continue;
        }
        let t = (needed * req)
            .div_ceil(off)
            .max(u128::from(o.order().pswap_note().storage().min_fill_step().as_u64()).min(req))
            .max(1);
        let maximum = u64::try_from(o.fill_interval().maximum).unwrap();
        let minimum = u64::try_from(o.fill_interval().minimum).unwrap();
        let first = if t >= req {
            Some(maximum)
        } else {
            (c0 + 1..=maximum).filter(|&c| c >= minimum).find(|&c| {
                u128::from(
                    o.execution(U256::from(c), r.config.protocol_fee_ppm)
                        .unwrap()
                        .unwrap()
                        .payment
                        .amount()
                        .as_u64(),
                ) >= t
            })
        };
        let Some(mut c) = first else {
            continue;
        };
        if c <= c0 {
            continue;
        }
        let initial = o
            .execution(U256::from(c), r.config.protocol_fee_ppm)
            .unwrap()
            .unwrap();
        if u128::from(initial.payment.amount().as_u64()) >= req {
            c = maximum;
        }
        let e = o
            .execution(U256::from(c), r.config.protocol_fee_ppm)
            .unwrap()
            .unwrap();
        if u128::from(e.payment.amount().as_u64()) != t {
            continue;
        }
        let mut changed = allocations.clone();
        changed[j] = U256::from(c);
        let total = changed.iter().copied().sum();
        if map.allocate_by_priority(donors, total).ok().as_ref() != Some(&changed) {
            continue;
        }
        let (s, b) = if sell {
            (&changed, &r.anchor.buyer_allocations)
        } else {
            (&r.anchor.seller_allocations, &changed)
        };
        let mut ledger = PairLedger::default();
        let mut executions = Vec::new();
        let mut valid = true;
        for (orders, cs) in [(r.sells, s), (r.buys, b)] {
            for (o, &c) in orders.iter().zip(cs) {
                let Some(e) = o.execution(c, r.config.protocol_fee_ppm).unwrap() else {
                    continue;
                };
                let minimum = o
                    .order()
                    .pswap_note()
                    .storage()
                    .min_fill_step()
                    .as_u64()
                    .min(o.order().requested_asset().amount().as_u64());
                if c < o.fill_interval().minimum
                    || c > o.fill_interval().maximum
                    || e.payment.amount().as_u64() < minimum
                    || (complete(&e, o) && c != o.fill_interval().maximum)
                {
                    valid = false;
                }
                ledger.record(o.side(), &e).unwrap();
                executions.push(e);
            }
        }
        if !valid || !extra(&executions) {
            continue;
        }
        if old_executions.iter().any(|old| {
            !executions.iter().any(|e| {
                e.order_id == old.order_id
                    && e.comparison_fill >= old.comparison_fill
                    && e.payment.amount() >= old.payment.amount()
                    && e.release.amount() >= old.release.amount()
            })
        }) {
            continue;
        }
        let Ok(p) = ledger.into_plan(executions).finalize() else {
            continue;
        };
        if p.accruals.realized_protocol_fee != p.candidate.protocol_fee_target
            || old.is_some_and(|old| {
                p.accruals.realized_protocol_fee.base < old.accruals.realized_protocol_fee.base
                    || p.accruals.realized_protocol_fee.quote
                        < old.accruals.realized_protocol_fee.quote
            })
        {
            continue;
        }
        let btotal: U256 = b.iter().copied().sum();
        let stotal: U256 = s.iter().copied().sum();
        let coupled = r.batch.clearing_price.quote_for_base_floor(btotal).unwrap();
        let deviation = stotal.max(coupled) - stotal.min(coupled);
        let payments = p
            .candidate
            .executions
            .iter()
            .map(|e| e.payment.amount().as_u64())
            .collect::<Vec<_>>();
        let releases = p
            .candidate
            .executions
            .iter()
            .map(|e| e.release.amount().as_u64())
            .collect::<Vec<_>>();
        candidates.push((
            (
                btotal,
                std::cmp::Reverse(deviation),
                payments,
                releases,
                std::cmp::Reverse(j),
                std::cmp::Reverse(c),
            ),
            p,
        ));
    }
    let (_, winner) = candidates.into_iter().max_by(|a, b| a.0.cmp(&b.0))?;
    crate::executor::validate_completion_repair(winner.to_execution_batch(r.batch).unwrap())
        .ok()?;
    Some(winner)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(256))]
    #[test]
    fn declared_neighborhood_matches_integer_enumeration(
        specs in proptest::collection::vec((proptest::bool::ANY, 1u64..32, 1u64..32, 0u64..40), 2..7),
        quote in 1u64..8, base in 1u64..8,
        fee in proptest::sample::select(vec![0u32,50,100_000,999_999]),
    ) {
        let orders=book(&specs);
        with_repair(&orders,BatchPrice::from_ratio(quote,base).unwrap(),&enabled(fee),|r| {
            assert_eq!(r.run(),oracle(r));
            // Input container permutations must not alter source price/FIFO or
            // the deterministic winner. Priorities travel with notes.
            let mut permuted=orders.clone();permuted.reverse();
            let batch=prepared(&permuted,r.batch.clearing_price,fee);
            let actual=PairMatcher::new(r.batch,r.config).clear().unwrap();
            let permuted=PairMatcher::new(&batch,r.config).clear().unwrap();
            assert_eq!(format!("{actual:?}"),format!("{permuted:?}"));
        });
    }
}

#[test]
fn oracle_has_positive_witnesses_for_both_deficit_directions_and_real_fees() {
    for (specs, price, fee) in [
        (
            vec![(true, 10, 14, 0), (false, 5, 3, 0)],
            BatchPrice::from_ratio(3, 2).unwrap(),
            0,
        ),
        (
            vec![(true, 60, 80, 0), (false, 26, 15, 8), (false, 69, 47, 42)],
            BatchPrice::from_ratio(137, 100).unwrap(),
            50,
        ),
        (
            vec![(true, 158627, 100937, 0), (false, 14520, 22813, 0)],
            BatchPrice::from_ratio(7, 11).unwrap(),
            50,
        ),
    ] {
        with_repair(&book(&specs), price, &enabled(fee), |r| {
            assert!(oracle(r).is_some());
            assert_eq!(r.run(), oracle(r));
        });
    }
}

#[test]
fn execution_budget_includes_anchor_and_threshold_overhead() {
    let orders = book(&[(true, 10, 14, 0), (false, 5, 3, 0)]);
    with_repair(
        &orders,
        BatchPrice::from_ratio(3, 2).unwrap(),
        &enabled(50),
        |r| {
            // Two anchor slots + (three threshold slots + two candidate slots).
            assert!(r
                .run_with(
                    WorkLimits {
                        executions: 6,
                        ..Default::default()
                    },
                    |_| Some(())
                )
                .is_none());
            assert!(r
                .run_with(
                    WorkLimits {
                        executions: 7,
                        ..Default::default()
                    },
                    |_| Some(())
                )
                .is_some());
        },
    );
}

#[test]
fn source_errors_oversize_books_and_extreme_amounts_remain_safe() {
    let orders = book(&[(true, 10, 14, 0), (false, 5, 3, 0)]);
    let mut batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    batch.clearing_price = BatchPrice::new(U256::MAX, U256::ONE).unwrap();
    let off = ClearingConfig {
        protocol_fee_ppm: 50,
        ..Default::default()
    };
    let legacy = PairMatcher::new(&batch, &off).clear();
    let candidate = PairMatcher::new(&batch, &enabled(50)).clear();
    assert!(legacy.is_err());
    assert_eq!(format!("{legacy:?}"), format!("{candidate:?}"));

    let specs = (0..101)
        .map(|_| (true, 10, 14, 0))
        .chain((0..101).map(|_| (false, 5, 3, 0)))
        .collect::<Vec<_>>();
    let orders = book(&specs);
    let batch = prepared(&orders, BatchPrice::from_ratio(3, 2).unwrap(), 50);
    let on = PairMatcher::new(&batch, &enabled(50)).clear();
    let off = PairMatcher::new(&batch, &off).clear();
    assert_eq!(format!("{on:?}"), format!("{off:?}"));

    let max = AssetAmount::MAX.as_u64();
    let orders = book(&[(true, max, max - 1, 0)]);
    let batch = prepared(&orders, BatchPrice::from_ratio(1, 1).unwrap(), 0);
    let o = &batch.sell_orders[0];
    let c = o.fill_interval().maximum - U256::from(2u64);
    let threshold = o.release_threshold(c, 1, 0).unwrap();
    if let Some(c) = threshold {
        let execution = o.execution(c, 0).unwrap().unwrap();
        assert!(execution.payment.amount() <= AssetAmount::MAX);
        assert!(execution.release.amount() <= AssetAmount::MAX);
    }
}

#[test]
fn measured_real_repair_enumeration_at_the_hundred_donor_side_bound() {
    use std::time::Instant;
    let mut specs = vec![(true, 10, 14, 0), (false, 5, 3, 0)];
    // These admitted later AON orders exceed all available quote. They remain
    // in the exact source maps, but cannot become new donors/inputs. This
    // exercises a real insolvent anchor with the maximum donor-side length.
    specs.extend((0..99).map(|_| (true, 1000, 1499, 1499)));
    let orders = book(&specs);
    let config = enabled(50);
    let price = BatchPrice::from_ratio(3, 2).unwrap();
    with_repair(&orders, price, &config, |r| {
        assert_eq!(r.sells.len(), 100);
        assert_eq!(r.sells.len() + r.buys.len(), 101);
        assert!(r.anchor.candidate.paid.base > r.anchor.candidate.released.base);
        let mut durations = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            let plan = r.run().expect("must exercise actual repair enumeration");
            assert_eq!(plan.candidate.executions.len(), 2);
            assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 3);
            durations.push(start.elapsed().as_micros());
        }
        durations.sort();
        let start = Instant::now();
        let mut validated = false;
        let result = r.run_with(
            WorkLimits {
                candidates: 0,
                ..Default::default()
            },
            |_| {
                validated = true;
                Some(())
            },
        );
        assert!(result.is_none());
        assert!(!validated);
        println!("actual insolvent repair: N101,donor_side100,positive_donors1, candidates1, reserved_execution_checks205, priority_visit_bound100; min_us={} median_us={} max_us={}, cap_abort_us={}",durations[0],durations[10],durations[19],start.elapsed().as_micros());
    });
}

#[test]
fn record_anchor_fallback_and_selected_token_economics() {
    for (name, specs, price, fee) in [
        (
            "original",
            vec![(true, 10, 14, 0), (false, 5, 3, 0)],
            (3, 2),
            50,
        ),
        (
            "final_tail",
            vec![(true, 8, 11, 0), (false, 2, 1, 0)],
            (3, 2),
            50,
        ),
        (
            "nonzero_fee",
            vec![(true, 115034, 72464, 0), (false, 20353, 32302, 0)],
            (63, 100),
            50,
        ),
        (
            "quote_deficit",
            vec![(true, 60, 80, 0), (false, 26, 15, 8), (false, 69, 47, 42)],
            (137, 100),
            50,
        ),
    ] {
        let orders = book(&specs);
        with_repair(
            &orders,
            BatchPrice::from_ratio(price.0, price.1).unwrap(),
            &enabled(fee),
            |r| {
                let selected = r
                    .run()
                    .expect("economics record must represent an accepted repair");
                let legacy = match r.legacy {
                    ClearingOutcome::Accepted(p) => Some(p),
                    _ => None,
                };
                let a = &r.anchor.candidate;
                println!("ECONOMICS {name}: anchor released={:?} paid={:?} nominal_fee={:?} raw_residual=({},{}); fallback={:?}; candidate released={:?} paid={:?} nominal_fee={:?} accruals={:?}",
                a.released,a.paid,a.protocol_fee_target,
                a.released.base as i128-a.paid.base as i128,a.released.quote as i128-a.paid.quote as i128,
                legacy.map(|p|(&p.candidate.released,&p.candidate.paid,&p.candidate.protocol_fee_target,&p.accruals)),
                selected.candidate.released,selected.candidate.paid,selected.candidate.protocol_fee_target,selected.accruals);
                assert_eq!(
                    selected.accruals.realized_protocol_fee,
                    selected.candidate.protocol_fee_target
                );
            },
        );
    }
}

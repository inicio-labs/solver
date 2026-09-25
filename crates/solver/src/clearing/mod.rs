//! Exact, bounded pair clearing at one frozen price. Polaris supplies an
//! immutable pair snapshot; the executor must recheck the returned plan
//! against actual PSWAP outputs before submitting a transaction.

mod envelope;
mod math;
mod settlement;
mod types;

use std::cmp::Ordering;
use std::collections::HashSet;

use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteId};

use crate::types::{ExecutionBatch, FilledNote};

use envelope::{allocate_by_priority, find_max_targets, EnvelopeStore};
use settlement::{build_execution, effective_min_fill, finalize_candidate, PairLedger};
use types::{PreparedOrder, Ratio, ReachableInterval, Side};

pub use types::{
    AdmittedPswap, CandidatePlan, ClearingConfig, ClearingError, ClearingOutcome, ExactPrice,
    InvalidOrderReason, OrderExecution, PairAmounts, PairBatch, PrioritySeq, ReferencePrice,
    ResourceLimitKind, SettlementPlan, SkipReason, SolverAccruals, Wide,
    PPM_DENOMINATOR,
};

fn checked_mul(left: Wide, right: Wide) -> Result<Wide, ClearingError> {
    left.checked_mul(right)
        .ok_or(ClearingError::ArithmeticOverflow)
}

fn classify(order: &AdmittedPswap, batch: &PairBatch) -> Result<Side, ClearingError> {
    let offered = order.note.offered_asset().id();
    let requested = order.note.storage().min_requested_asset().id();
    if offered == batch.base && requested == batch.quote {
        Ok(Side::SellBase)
    } else if offered == batch.quote && requested == batch.base {
        Ok(Side::BuyBase)
    } else {
        Err(ClearingError::InvalidOrder {
            note_id: order.note_id,
            reason: InvalidOrderReason::UnsupportedAssetDirection,
        })
    }
}

fn is_eligible(
    side: Side,
    offered: u64,
    requested: u64,
    price: ExactPrice,
    fee_ppm: u32,
) -> Result<bool, ClearingError> {
    let c = Wide::from(PPM_DENOMINATOR);
    let f = Wide::from(fee_ppm);
    let o = Wide::from(offered);
    let r = Wide::from(requested);
    match side {
        Side::SellBase => {
            let lhs = checked_mul(checked_mul(r, price.base_units)?, c)?;
            let rhs = checked_mul(checked_mul(o, price.quote_units)?, c - f)?;
            Ok(lhs <= rhs)
        }
        Side::BuyBase => {
            let lhs = checked_mul(checked_mul(o, price.base_units)?, c)?;
            let rhs = checked_mul(checked_mul(r, price.quote_units)?, c + f)?;
            Ok(lhs >= rhs)
        }
    }
}

/// Shared eligibility check for the live admission index and the core solver.
pub(crate) fn order_is_eligible(
    order: &AdmittedPswap,
    base: miden_protocol::asset::AssetId,
    price: ExactPrice,
    fee_ppm: u32,
) -> Result<bool, ClearingError> {
    if fee_ppm >= PPM_DENOMINATOR {
        return Err(ClearingError::InvalidConfig);
    }
    let offered = order.note.offered_asset();
    is_eligible(
        if offered.id() == base {
            Side::SellBase
        } else {
            Side::BuyBase
        },
        offered.amount().as_u64(),
        order.note.storage().min_requested_asset().amount().as_u64(),
        price,
        fee_ppm,
    )
}

fn prepare_order(
    input_index: usize,
    order: &AdmittedPswap,
    side: Side,
    price: ExactPrice,
) -> Result<PreparedOrder, ClearingError> {
    let offered = *order.note.offered_asset();
    let requested = *order.note.storage().min_requested_asset();
    let min_fill = effective_min_fill(&order.note)?;
    let o = Wide::from(offered.amount().as_u64());
    let r = Wide::from(requested.amount().as_u64());
    let scale = match side {
        Side::SellBase => Ratio::new(
            checked_mul(price.quote_units, o)?,
            checked_mul(price.base_units, r)?,
        )?,
        Side::BuyBase => Ratio::new(
            checked_mul(price.base_units, o)?,
            checked_mul(price.quote_units, r)?,
        )?,
    };
    let lo = scale.scale_up_floor(min_fill)?;
    let hi = scale.scale_up_floor(requested.amount())?;
    if lo > hi || lo < Wide::from(min_fill.as_u64()) || hi < r {
        return Err(ClearingError::InternalInvariant("invalid scaled domain"));
    }
    Ok(PreparedOrder {
        input_index,
        side,
        offered,
        requested,
        min_fill,
        scale,
        domain: ReachableInterval { lo, hi },
    })
}

fn price_time_cmp(lhs: &PreparedOrder, rhs: &PreparedOrder, batch: &PairBatch) -> Ordering {
    let lhs_o = u128::from(lhs.offered.amount().as_u64());
    let lhs_r = u128::from(lhs.requested.amount().as_u64());
    let rhs_o = u128::from(rhs.offered.amount().as_u64());
    let rhs_r = u128::from(rhs.requested.amount().as_u64());
    let price_order = match lhs.side {
        Side::SellBase => (lhs_r * rhs_o).cmp(&(rhs_r * lhs_o)),
        Side::BuyBase => (rhs_o * lhs_r).cmp(&(lhs_o * rhs_r)),
    };
    price_order.then_with(|| {
        batch.orders[lhs.input_index]
            .priority_seq
            .cmp(&batch.orders[rhs.input_index].priority_seq)
    })
}

fn prepare_sides(
    batch: &PairBatch,
    config: &ClearingConfig,
) -> Result<(Vec<PreparedOrder>, Vec<PreparedOrder>), ClearingError> {
    let mut ids = HashSet::with_capacity(batch.orders.len());
    let mut sell_priority = HashSet::new();
    let mut buy_priority = HashSet::new();
    let mut sells = Vec::new();
    let mut buys = Vec::new();
    let mut sell_count = 0;
    let mut buy_count = 0;

    for (input_index, order) in batch.orders.iter().enumerate() {
        if order.priority_seq == 0 {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::MissingPriority,
            });
        }
        if !ids.insert(order.note_id) {
            return Err(ClearingError::DuplicateNoteId(order.note_id));
        }
        let offered = order.note.offered_asset();
        let requested = order.note.storage().min_requested_asset();
        if offered.amount().as_u64() == 0 || requested.amount().as_u64() == 0 {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::ZeroAmount,
            });
        }
        let side = classify(order, batch)?;
        let (priority, count) = match side {
            Side::SellBase => (&mut sell_priority, &mut sell_count),
            Side::BuyBase => (&mut buy_priority, &mut buy_count),
        };
        if !priority.insert(order.priority_seq) {
            return Err(ClearingError::DuplicatePriority(order.priority_seq));
        }
        if is_eligible(
            side,
            offered.amount().as_u64(),
            requested.amount().as_u64(),
            batch.clearing_price,
            config.protocol_fee_ppm,
        )? {
            *count += 1;
            if *count > config.max_orders_per_side {
                return Err(ClearingError::ResourceLimit(
                    ResourceLimitKind::OrdersPerSide,
                ));
            }
            let prepared = prepare_order(input_index, order, side, batch.clearing_price)?;
            match side {
                Side::SellBase => sells.push(prepared),
                Side::BuyBase => buys.push(prepared),
            }
        }
    }
    sells.sort_unstable_by(|lhs, rhs| price_time_cmp(lhs, rhs, batch));
    buys.sort_unstable_by(|lhs, rhs| price_time_cmp(lhs, rhs, batch));
    Ok((sells, buys))
}

fn validate(batch: &PairBatch, config: &ClearingConfig) -> Result<(), ClearingError> {
    if batch.base == batch.quote {
        return Err(ClearingError::InvalidConfig);
    }
    if batch.clearing_price.base_units == Wide::ZERO
        || batch.clearing_price.quote_units == Wide::ZERO
    {
        return Err(ClearingError::InvalidPrice);
    }
    if config.protocol_fee_ppm >= PPM_DENOMINATOR
        || config.max_orders_per_side == 0
        || config.max_intervals_per_row == 0
        || config.max_total_intervals == 0
    {
        return Err(ClearingError::InvalidConfig);
    }
    Ok(())
}

/// Find the maximum scaled envelope candidate at the supplied frozen price,
/// distribute it by exact price then durable ingestion FIFO, and accept only
/// when actual PSWAP payouts balance in both assets.
pub fn clear_pair(
    batch: &PairBatch,
    config: &ClearingConfig,
) -> Result<ClearingOutcome, ClearingError> {
    validate(batch, config)?;
    let (sells, buys) = match prepare_sides(batch, config) {
        Ok(sides) => sides,
        Err(ClearingError::ResourceLimit(limit)) => {
            return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit(limit)));
        }
        Err(error) => return Err(error),
    };
    if sells.is_empty() || buys.is_empty() {
        return Ok(ClearingOutcome::Skipped(SkipReason::NoEligibleCross));
    }
    let sell_envelope = match EnvelopeStore::build(&sells, config) {
        Ok(value) => value,
        Err(ClearingError::ResourceLimit(limit)) => {
            return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit(limit)));
        }
        Err(error) => return Err(error),
    };
    let buy_envelope = match EnvelopeStore::build(&buys, config) {
        Ok(value) => value,
        Err(ClearingError::ResourceLimit(limit)) => {
            return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit(limit)));
        }
        Err(error) => return Err(error),
    };
    let Some((buyer_target, seller_target)) =
        find_max_targets(&sell_envelope, &buy_envelope, batch.clearing_price)?
    else {
        return Ok(ClearingOutcome::Skipped(SkipReason::NoPositiveCross));
    };
    let sell_scaled = allocate_by_priority(&sells, &sell_envelope, seller_target)?;
    let buy_scaled = allocate_by_priority(&buys, &buy_envelope, buyer_target)?;
    let mut executions = Vec::new();
    let mut ledger = PairLedger::default();
    for (orders, allocations) in [(&sells, &sell_scaled), (&buys, &buy_scaled)] {
        for (order, &scaled) in orders.iter().zip(allocations.iter()) {
            let pswap = &batch.orders[order.input_index].note;
            if let Some(execution) = build_execution(order, pswap, scaled, config.protocol_fee_ppm)?
            {
                ledger.record(order.side, &execution)?;
                executions.push(execution);
            }
        }
    }
    let (released, paid, nominal_protocol_fee) = ledger.into_totals();
    let candidate = CandidatePlan {
        clearing_price: batch.clearing_price,
        buyer_target_a: buyer_target,
        seller_target_b: seller_target,
        executions,
        released,
        paid,
        nominal_protocol_fee,
    };
    Ok(match finalize_candidate(candidate) {
        Ok(plan) => ClearingOutcome::Accepted(plan),
        Err(reason) => ClearingOutcome::Skipped(reason),
    })
}

impl SettlementPlan {
    /// Recheck each planned PSWAP output with the pinned protocol implementation
    /// and build the existing executor's batch. `arrival_unix` is used only for
    /// settlement-time metrics; the persisted FIFO sequence controls priority.
    pub fn to_execution_batch(
        &self,
        batch: &PairBatch,
        solver_id: AccountId,
        arrival_unix: &std::collections::HashMap<NoteId, u64>,
    ) -> Result<ExecutionBatch, ClearingError> {
        let mut filled_notes = Vec::with_capacity(self.candidate.executions.len());
        for execution in &self.candidate.executions {
            let admitted =
                batch
                    .orders
                    .get(execution.input_index)
                    .ok_or(ClearingError::InternalInvariant(
                        "execution index outside input batch",
                    ))?;
            let note: Note = admitted.note.clone().into();
            if note.id() != admitted.note_id {
                return Err(ClearingError::InternalInvariant(
                    "admitted note ID mismatch",
                ));
            }
            let payment = FungibleAsset::new(
                execution.payment.faucet_id(),
                execution.payment.amount().as_u64(),
            )?;
            let (_, remainder) = admitted
                .note
                .execute(solver_id, None, Some(payment))
                .map_err(|source| ClearingError::InvalidPswap {
                    note_id: admitted.note_id,
                    source,
                })?;
            let actual_release = match remainder {
                Some(remainder) => admitted
                    .note
                    .offered_asset()
                    .amount()
                    .as_u64()
                    .checked_sub(remainder.offered_asset().amount().as_u64())
                    .ok_or(ClearingError::InternalInvariant(
                        "remainder exceeds offered",
                    ))?,
                None => admitted.note.offered_asset().amount().as_u64(),
            };
            if actual_release != execution.release.amount().as_u64() {
                return Err(ClearingError::InternalInvariant(
                    "protocol release differs from plan",
                ));
            }
            let arrival = arrival_unix.get(&admitted.note_id).copied().ok_or(
                ClearingError::InternalInvariant("missing order arrival timestamp"),
            )?;
            let mut raw_note_data = Vec::new();
            note.write_into(&mut raw_note_data);
            filled_notes.push(FilledNote {
                note_id: admitted.note_id,
                priority_seq: admitted.priority_seq,
                requested_filled: execution.payment.amount().as_u64(),
                raw_note_data,
                arrival_unix: arrival,
            });
        }
        Ok(ExecutionBatch { filled_notes, group_ends: Vec::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::{AssetAmount, AssetId, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    fn assets() -> (AccountId, AccountId) {
        (
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(),
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap(),
        )
    }

    fn order(
        offered: FungibleAsset,
        requested: FungibleAsset,
        minimum: u64,
        sequence: u64,
        rng: &mut RandomCoin,
    ) -> AdmittedPswap {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let storage = PswapNoteStorage::builder()
            .min_requested_asset(requested)
            .min_fill_step(AssetAmount::new(minimum).unwrap())
            .creator_account_id(creator)
            .build();
        let note: Note = PswapNote::builder()
            .sender(creator)
            .storage(storage)
            .serial_number(rng.draw_word())
            .note_type(NoteType::Public)
            .offered_asset(offered)
            .build()
            .unwrap()
            .into();
        AdmittedPswap::from_note(&note, sequence).unwrap()
    }

    fn batch(price: ExactPrice, orders: Vec<AdmittedPswap>) -> PairBatch {
        let (a, b) = assets();
        PairBatch {
            base: AssetId::new_fungible(a),
            quote: AssetId::new_fungible(b),
            clearing_price: price,
            orders,
        }
    }

    #[test]
    fn ingested_note_adapter_checks_identity_amounts_and_fifo() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let original = order(
            FungibleAsset::new(a, 10).unwrap(),
            FungibleAsset::new(b, 20).unwrap(),
            5,
            7,
            &mut rng,
        );
        let note: Note = original.note.clone().into();
        let mut ingested = crate::types::IngestOrder {
            note_id: original.note_id,
            priority_seq: 7,
            offered_token: a,
            requested_token: b,
            offered_amount: 10,
            requested_amount: 20,
            min_fill_step: 5,
            raw_note_data: note.to_bytes(),
        };
        assert_eq!(
            AdmittedPswap::from_ingest_order(&ingested)
                .unwrap()
                .priority_seq,
            7
        );
        let input = PairBatch::from_ingest_orders(
            AssetId::new_fungible(a),
            AssetId::new_fungible(b),
            ReferencePrice::from_decimal("2").unwrap(),
            ReferencePrice::from_decimal("1").unwrap(),
            0,
            0,
            &[ingested.clone()],
        )
        .unwrap();
        assert_eq!(input.orders.len(), 1);
        assert_eq!(input.clearing_price.quote_units, Wide::from(2u8));
        ingested.requested_amount = 21;
        assert!(matches!(
            AdmittedPswap::from_ingest_order(&ingested),
            Err(ClearingError::InvalidOrder {
                reason: InvalidOrderReason::InconsistentIngestOrder,
                ..
            })
        ));
        ingested.requested_amount = 20;
        ingested.priority_seq = 0;
        assert!(matches!(
            AdmittedPswap::from_ingest_order(&ingested),
            Err(ClearingError::InvalidOrder {
                reason: InvalidOrderReason::MissingPriority,
                ..
            })
        ));
    }

    #[test]
    fn endpoint_fee_comes_only_from_each_notes_surplus() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
            order(
                FungibleAsset::new(a, 11).unwrap(),
                FungibleAsset::new(b, 18).unwrap(),
                5,
                1,
                &mut rng,
            ),
            order(
                FungibleAsset::new(b, 22).unwrap(),
                FungibleAsset::new(a, 10).unwrap(),
                5,
                2,
                &mut rng,
            ),
        ];
        let price = ExactPrice::new(Wide::from(2u8), Wide::ONE).unwrap();
        let config = ClearingConfig {
            protocol_fee_ppm: 100_000,
            ..ClearingConfig::default()
        };
        let input = batch(price, orders);
        let ClearingOutcome::Accepted(plan) = clear_pair(&input, &config).unwrap() else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.buyer_target_a, Wide::from(11u8));
        assert_eq!(plan.candidate.seller_target_b, Wide::from(22u8));
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 20);
        assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 10);
        assert_eq!(plan.accruals.realized_protocol_fee.base, Wide::ONE);
        assert_eq!(plan.accruals.realized_protocol_fee.quote, Wide::from(2u8));
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());
        let arrivals = input
            .orders
            .iter()
            .map(|order| (order.note_id, 100))
            .collect();
        let solver_id = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        let execution_batch = plan
            .to_execution_batch(&input, solver_id, &arrivals)
            .unwrap();
        assert_eq!(execution_batch.filled_notes.len(), 2);
        assert_eq!(execution_batch.filled_notes[0].requested_filled, 20);
        assert_eq!(execution_batch.filled_notes[1].requested_filled, 10);
    }

    #[test]
    fn inherited_minimum_above_remainder_allows_full_only() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
            order(
                FungibleAsset::new(a, 2).unwrap(),
                FungibleAsset::new(b, 3).unwrap(),
                10,
                1,
                &mut rng,
            ),
            order(
                FungibleAsset::new(b, 6).unwrap(),
                FungibleAsset::new(a, 2).unwrap(),
                1,
                2,
                &mut rng,
            ),
        ];
        let price = ExactPrice::new(Wide::from(3u8), Wide::ONE).unwrap();
        let ClearingOutcome::Accepted(plan) =
            clear_pair(&batch(price, orders), &ClearingConfig::default()).unwrap()
        else {
            panic!("full-only remainder should be executable");
        };
        assert_eq!(plan.candidate.executions[0].scaled, Wide::from(6u8));
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 6);
    }

    #[test]
    fn partial_seller_uses_inverse_ceil_and_protocol_payout() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let input = batch(
            ExactPrice::new(Wide::ONE, Wide::ONE).unwrap(),
            vec![
                order(
                    FungibleAsset::new(a, 100).unwrap(),
                    FungibleAsset::new(b, 50).unwrap(),
                    20,
                    1,
                    &mut rng,
                ),
                order(
                    FungibleAsset::new(b, 80).unwrap(),
                    FungibleAsset::new(a, 60).unwrap(),
                    20,
                    2,
                    &mut rng,
                ),
            ],
        );
        let config = ClearingConfig {
            protocol_fee_ppm: 100_000,
            ..ClearingConfig::default()
        };
        let ClearingOutcome::Accepted(plan) = clear_pair(&input, &config).unwrap() else {
            panic!("expected partial seller and full buyer to settle");
        };
        assert_eq!(plan.candidate.executions[0].scaled, Wide::from(80u8));
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 40);
        assert_eq!(plan.candidate.executions[0].release.amount().as_u64(), 80);
        assert_eq!(
            plan.candidate.executions[0].nominal_protocol_fee,
            Wide::ZERO
        );
        assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 72);
        assert_eq!(
            plan.candidate.executions[1].nominal_protocol_fee,
            Wide::from(8u8)
        );
        assert_eq!(plan.accruals.realized_protocol_fee.base, Wide::from(8u8));
        assert_eq!(plan.accruals.rounding_surplus.quote, Wide::from(40u8));

        let arrivals = input
            .orders
            .iter()
            .map(|order| (order.note_id, 100))
            .collect();
        let solver_id = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        plan.to_execution_batch(&input, solver_id, &arrivals)
            .unwrap();
    }

    #[test]
    fn price_then_durable_fifo_wins_when_only_one_seller_fits() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let late = order(
            FungibleAsset::new(a, 10).unwrap(),
            FungibleAsset::new(b, 10).unwrap(),
            1,
            2,
            &mut rng,
        );
        let early = order(
            FungibleAsset::new(a, 10).unwrap(),
            FungibleAsset::new(b, 10).unwrap(),
            1,
            1,
            &mut rng,
        );
        let buyer = order(
            FungibleAsset::new(b, 10).unwrap(),
            FungibleAsset::new(a, 10).unwrap(),
            1,
            1,
            &mut rng,
        );
        let price = ExactPrice::new(Wide::ONE, Wide::ONE).unwrap();
        let ClearingOutcome::Accepted(plan) = clear_pair(
            &batch(price, vec![late, early, buyer]),
            &ClearingConfig::default(),
        )
        .unwrap() else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.executions[0].input_index, 1);
        assert_eq!(plan.candidate.executions.len(), 2);
    }

    #[test]
    fn hundred_orders_per_side_settle_with_exact_protocol_payouts() {
        let (a, b) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let mut orders = Vec::with_capacity(200);
        for sequence in 1..=100 {
            orders.push(order(
                FungibleAsset::new(a, 600).unwrap(),
                FungibleAsset::new(b, 1_000).unwrap(),
                100,
                sequence,
                &mut rng,
            ));
            orders.push(order(
                FungibleAsset::new(b, 1_200).unwrap(),
                FungibleAsset::new(a, 500).unwrap(),
                50,
                sequence,
                &mut rng,
            ));
        }
        let ingested: Vec<_> = orders
            .iter()
            .map(|admitted| {
                let note: Note = admitted.note.clone().into();
                let offered = admitted.note.offered_asset();
                let requested = admitted.note.storage().min_requested_asset();
                crate::types::IngestOrder {
                    note_id: admitted.note_id,
                    priority_seq: admitted.priority_seq,
                    offered_token: offered.faucet_id(),
                    requested_token: requested.faucet_id(),
                    offered_amount: offered.amount().as_u64(),
                    requested_amount: requested.amount().as_u64(),
                    min_fill_step: admitted.note.storage().min_fill_step().as_u64(),
                    raw_note_data: note.to_bytes(),
                }
            })
            .collect();
        let input = PairBatch::from_ingest_orders(
            AssetId::new_fungible(a),
            AssetId::new_fungible(b),
            ReferencePrice::from_decimal("2").unwrap(),
            ReferencePrice::from_decimal("1").unwrap(),
            0,
            0,
            &ingested,
        )
        .unwrap();
        let config = ClearingConfig {
            protocol_fee_ppm: 10_000,
            ..ClearingConfig::default()
        };
        let ClearingOutcome::Accepted(plan) = clear_pair(&input, &config).unwrap() else {
            panic!("expected all 200 notes to settle");
        };
        assert_eq!(plan.candidate.executions.len(), 200);
        assert_eq!(plan.candidate.buyer_target_a, Wide::from(60_000u64));
        assert_eq!(plan.candidate.seller_target_b, Wide::from(120_000u64));
        assert_eq!(plan.candidate.released.base, Wide::from(60_000u64));
        assert_eq!(plan.candidate.paid.base, Wide::from(59_400u64));
        assert_eq!(plan.candidate.released.quote, Wide::from(120_000u64));
        assert_eq!(plan.candidate.paid.quote, Wide::from(118_800u64));
        assert_eq!(plan.accruals.realized_protocol_fee.base, Wide::from(600u64));
        assert_eq!(
            plan.accruals.realized_protocol_fee.quote,
            Wide::from(1_200u64)
        );
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());

        let arrivals = input
            .orders
            .iter()
            .map(|order| (order.note_id, 100))
            .collect();
        let solver_id = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        let execution_batch = plan
            .to_execution_batch(&input, solver_id, &arrivals)
            .unwrap();
        assert_eq!(execution_batch.filled_notes.len(), 200);
    }
}

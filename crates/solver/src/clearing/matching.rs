use std::collections::HashSet;

use miden_protocol::asset::AssetId;

use crate::types::IngestOrder;

use super::envelope::ReachableFillMap;
use super::order::{MatchOrder, Order, OrderSide};
use super::settlement::PairLedger;
use super::types::{
    CandidatePlan, ClearingConfig, ClearingError, ClearingOutcome, ExactPrice, ReferencePrice,
    SkipReason,
};

#[derive(Clone, Debug)]
pub struct PairBatch {
    base: AssetId,
    quote: AssetId,
    clearing_price: ExactPrice,
    orders: Vec<Order>,
}

impl PairBatch {
    pub fn new(
        base: AssetId,
        quote: AssetId,
        clearing_price: ExactPrice,
        orders: Vec<Order>,
    ) -> Result<Self, ClearingError> {
        if base == quote {
            return Err(ClearingError::InvalidConfig);
        }
        Ok(Self {
            base,
            quote,
            clearing_price,
            orders,
        })
    }

    /// Build a pair batch from two whole-token prices in the same frozen
    /// reference snapshot and the on-chain token decimals.
    pub fn from_reference_prices(
        base: AssetId,
        quote: AssetId,
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
        orders: Vec<Order>,
    ) -> Result<Self, ClearingError> {
        Self::new(
            base,
            quote,
            ExactPrice::from_reference_prices(
                base_price,
                quote_price,
                base_decimals,
                quote_decimals,
            )?,
            orders,
        )
    }

    /// Verify serialized notes and persisted priority before constructing a
    /// batch from a frozen price snapshot.
    pub fn from_ingest_orders(
        base: AssetId,
        quote: AssetId,
        base_price: ReferencePrice,
        quote_price: ReferencePrice,
        base_decimals: u8,
        quote_decimals: u8,
        orders: &[IngestOrder],
    ) -> Result<Self, ClearingError> {
        let orders = orders
            .iter()
            .map(Order::from_ingest_order)
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_reference_prices(
            base,
            quote,
            base_price,
            quote_price,
            base_decimals,
            quote_decimals,
            orders,
        )
    }

    pub fn orders(&self) -> &[Order] {
        &self.orders
    }

    pub fn clearing_price(&self) -> ExactPrice {
        self.clearing_price
    }
}

pub struct PairMatcher<'a> {
    batch: &'a PairBatch,
    config: &'a ClearingConfig,
}

impl<'a> PairMatcher<'a> {
    pub fn new(batch: &'a PairBatch, config: &'a ClearingConfig) -> Result<Self, ClearingError> {
        config.validate()?;
        Ok(Self { batch, config })
    }

    /// Maximize comparison volume at the frozen price, allocate that volume in
    /// price-time priority, and accept only an exactly solvent settlement.
    pub fn clear(self) -> Result<ClearingOutcome, ClearingError> {
        let (sell_orders, buy_orders) = self.prepare_orders()?;
        if sell_orders.is_empty() || buy_orders.is_empty() {
            return Ok(ClearingOutcome::Skipped(SkipReason::NoEligibleCross));
        }

        let seller_fills = match ReachableFillMap::build(&sell_orders, self.config) {
            Ok(fills) => fills,
            Err(ClearingError::ResourceLimit(limit)) => {
                return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit(limit)));
            }
            Err(error) => return Err(error),
        };
        let buyer_fills = match ReachableFillMap::build(&buy_orders, self.config) {
            Ok(fills) => fills,
            Err(ClearingError::ResourceLimit(limit)) => {
                return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit(limit)));
            }
            Err(error) => return Err(error),
        };
        let Some((buyer_base_target, seller_quote_target)) =
            seller_fills.maximum_common_fill(&buyer_fills, self.batch.clearing_price)?
        else {
            return Ok(ClearingOutcome::Skipped(SkipReason::NoPositiveCross));
        };

        let seller_allocations =
            seller_fills.allocate_by_priority(&sell_orders, seller_quote_target)?;
        let buyer_allocations = buyer_fills.allocate_by_priority(&buy_orders, buyer_base_target)?;
        let mut executions = Vec::new();
        let mut ledger = PairLedger::default();
        for (orders, allocations) in [
            (&sell_orders, &seller_allocations),
            (&buy_orders, &buyer_allocations),
        ] {
            for (order, &comparison_fill) in orders.iter().zip(allocations.iter()) {
                if let Some(execution) =
                    order.execution(comparison_fill, self.config.protocol_fee_ppm)?
                {
                    ledger.record(order.side(), &execution)?;
                    executions.push(execution);
                }
            }
        }
        let (released, paid, protocol_fee_target) = ledger.totals();
        let candidate = CandidatePlan {
            clearing_price: self.batch.clearing_price,
            buyer_base_target,
            seller_quote_target,
            executions,
            released,
            paid,
            protocol_fee_target,
        };
        Ok(match candidate.finalize() {
            Ok(plan) => ClearingOutcome::Accepted(Box::new(plan)),
            Err(reason) => ClearingOutcome::Skipped(reason),
        })
    }

    fn prepare_orders(&self) -> Result<(Vec<MatchOrder<'_>>, Vec<MatchOrder<'_>>), ClearingError> {
        let mut note_ids = HashSet::with_capacity(self.batch.orders.len());
        let mut sell_priorities = HashSet::new();
        let mut buy_priorities = HashSet::new();
        let mut sell_orders = Vec::new();
        let mut buy_orders = Vec::new();

        for order in &self.batch.orders {
            if !note_ids.insert(order.id()) {
                return Err(ClearingError::DuplicateNoteId(order.id()));
            }
            let side = order.side_for(self.batch.base, self.batch.quote)?;
            let priorities = match side {
                OrderSide::SellBase => &mut sell_priorities,
                OrderSide::BuyBase => &mut buy_priorities,
            };
            if !priorities.insert(order.priority_sequence()) {
                return Err(ClearingError::DuplicatePriority(order.priority_sequence()));
            }
            if !order.is_eligible_at(
                self.batch.base,
                self.batch.quote,
                self.batch.clearing_price,
                self.config.protocol_fee_ppm,
            )? {
                continue;
            }
            let prepared =
                order.prepare(self.batch.base, self.batch.quote, self.batch.clearing_price)?;
            match side {
                OrderSide::SellBase => sell_orders.push(prepared),
                OrderSide::BuyBase => buy_orders.push(prepared),
            }
        }

        sell_orders.sort_unstable_by(MatchOrder::compare_price_time);
        buy_orders.sort_unstable_by(MatchOrder::compare_price_time);
        sell_orders.truncate(self.config.max_orders_per_side);
        buy_orders.truncate(self.config.max_orders_per_side);
        Ok((sell_orders, buy_orders))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::{
        rand::{FeltRng, RandomCoin},
        utils::Serializable,
    };
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use ruint::aliases::U256;

    use crate::clearing::{PairAmounts, ReferencePrice};

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
    ) -> Order {
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
        Order::from_note(&note, sequence).unwrap()
    }

    fn batch(price: ExactPrice, orders: Vec<Order>) -> PairBatch {
        let (base, quote) = assets();
        PairBatch::new(
            AssetId::new_fungible(base),
            AssetId::new_fungible(quote),
            price,
            orders,
        )
        .unwrap()
    }

    fn clear(batch: &PairBatch, config: &ClearingConfig) -> ClearingOutcome {
        PairMatcher::new(batch, config).unwrap().clear().unwrap()
    }

    #[test]
    fn ingested_note_adapter_checks_identity_amounts_and_fifo() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let original = order(
            FungibleAsset::new(base, 10).unwrap(),
            FungibleAsset::new(quote, 20).unwrap(),
            5,
            7,
            &mut rng,
        );
        let note: Note = original.pswap_note().clone().into();
        let mut ingested = IngestOrder {
            note_id: original.id(),
            priority_seq: 7,
            offered_token: base,
            requested_token: quote,
            offered_amount: 10,
            requested_amount: 20,
            min_fill_step: 5,
            raw_note_data: note.to_bytes(),
        };
        assert_eq!(
            Order::from_ingest_order(&ingested)
                .unwrap()
                .priority_sequence(),
            7
        );
        let input = PairBatch::from_ingest_orders(
            AssetId::new_fungible(base),
            AssetId::new_fungible(quote),
            ReferencePrice::from_decimal("2").unwrap(),
            ReferencePrice::from_decimal("1").unwrap(),
            0,
            0,
            &[ingested.clone()],
        )
        .unwrap();
        assert_eq!(input.orders().len(), 1);
        assert_eq!(input.clearing_price().quote_units, U256::from(2u8));
        ingested.requested_amount = 21;
        assert!(matches!(
            Order::from_ingest_order(&ingested),
            Err(ClearingError::InvalidOrder {
                reason: super::super::types::InvalidOrderReason::InconsistentIngestOrder,
                ..
            })
        ));
        ingested.requested_amount = 20;
        ingested.priority_seq = 0;
        assert!(matches!(
            Order::from_ingest_order(&ingested),
            Err(ClearingError::InvalidOrder {
                reason: super::super::types::InvalidOrderReason::MissingPriority,
                ..
            })
        ));
    }

    #[test]
    fn endpoint_fee_comes_only_from_each_orders_surplus() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
            order(
                FungibleAsset::new(base, 11).unwrap(),
                FungibleAsset::new(quote, 18).unwrap(),
                5,
                1,
                &mut rng,
            ),
            order(
                FungibleAsset::new(quote, 22).unwrap(),
                FungibleAsset::new(base, 10).unwrap(),
                5,
                2,
                &mut rng,
            ),
        ];
        let config = ClearingConfig {
            protocol_fee_ppm: 100_000,
            ..ClearingConfig::default()
        };
        let input = batch(ExactPrice::from_ratio(2, 1).unwrap(), orders);
        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.buyer_base_target, U256::from(11u8));
        assert_eq!(plan.candidate.seller_quote_target, U256::from(22u8));
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 20);
        assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 10);
        assert_eq!(plan.accruals.realized_protocol_fee.base, U256::ONE);
        assert_eq!(plan.accruals.realized_protocol_fee.quote, U256::from(2u8));
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());
        let arrivals = input
            .orders()
            .iter()
            .map(|order| (order.id(), 100))
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
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
            order(
                FungibleAsset::new(base, 2).unwrap(),
                FungibleAsset::new(quote, 3).unwrap(),
                10,
                1,
                &mut rng,
            ),
            order(
                FungibleAsset::new(quote, 6).unwrap(),
                FungibleAsset::new(base, 2).unwrap(),
                1,
                2,
                &mut rng,
            ),
        ];
        let input = batch(ExactPrice::from_ratio(3, 1).unwrap(), orders);
        let ClearingOutcome::Accepted(plan) = clear(&input, &ClearingConfig::default()) else {
            panic!("full-only remainder should be executable");
        };
        assert_eq!(
            plan.candidate.executions[0].comparison_fill,
            U256::from(6u8)
        );
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 6);
    }

    #[test]
    fn partial_seller_uses_inverse_ceil_and_protocol_payout() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let input = batch(
            ExactPrice::from_ratio(1, 1).unwrap(),
            vec![
                order(
                    FungibleAsset::new(base, 100).unwrap(),
                    FungibleAsset::new(quote, 50).unwrap(),
                    20,
                    1,
                    &mut rng,
                ),
                order(
                    FungibleAsset::new(quote, 80).unwrap(),
                    FungibleAsset::new(base, 60).unwrap(),
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
        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("expected partial seller and full buyer to settle");
        };
        assert_eq!(
            plan.candidate.executions[0].comparison_fill,
            U256::from(80u8)
        );
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 40);
        assert_eq!(plan.candidate.executions[0].release.amount().as_u64(), 80);
        assert_eq!(plan.candidate.executions[0].protocol_fee_target, U256::ZERO);
        assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 72);
        assert_eq!(
            plan.candidate.executions[1].protocol_fee_target,
            U256::from(8u8)
        );
        assert_eq!(plan.accruals.realized_protocol_fee.base, U256::from(8u8));
        assert_eq!(plan.accruals.rounding_surplus.quote, U256::from(40u8));

        let arrivals = input
            .orders()
            .iter()
            .map(|order| (order.id(), 100))
            .collect();
        let solver_id = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        plan.to_execution_batch(&input, solver_id, &arrivals)
            .unwrap();
    }

    #[test]
    fn price_then_fifo_wins_when_only_one_seller_fits() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let late = order(
            FungibleAsset::new(base, 10).unwrap(),
            FungibleAsset::new(quote, 10).unwrap(),
            1,
            2,
            &mut rng,
        );
        let early = order(
            FungibleAsset::new(base, 10).unwrap(),
            FungibleAsset::new(quote, 10).unwrap(),
            1,
            1,
            &mut rng,
        );
        let early_id = early.id();
        let buyer = order(
            FungibleAsset::new(quote, 10).unwrap(),
            FungibleAsset::new(base, 10).unwrap(),
            1,
            1,
            &mut rng,
        );
        let input = batch(
            ExactPrice::from_ratio(1, 1).unwrap(),
            vec![late, early, buyer],
        );
        let ClearingOutcome::Accepted(plan) = clear(&input, &ClearingConfig::default()) else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.executions[0].order_id, early_id);
        assert_eq!(plan.candidate.executions.len(), 2);
    }

    #[test]
    fn hundred_orders_per_side_settle_with_exact_protocol_payouts() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let mut orders = Vec::with_capacity(200);
        for sequence in 1..=100 {
            orders.push(order(
                FungibleAsset::new(base, 600).unwrap(),
                FungibleAsset::new(quote, 1_000).unwrap(),
                100,
                sequence,
                &mut rng,
            ));
            orders.push(order(
                FungibleAsset::new(quote, 1_200).unwrap(),
                FungibleAsset::new(base, 500).unwrap(),
                50,
                sequence,
                &mut rng,
            ));
        }
        let ingested: Vec<_> = orders
            .iter()
            .map(|order| {
                let note: Note = order.pswap_note().clone().into();
                let offered = order.offered_asset();
                let requested = order.requested_asset();
                IngestOrder {
                    note_id: order.id(),
                    priority_seq: order.priority_sequence(),
                    offered_token: offered.faucet_id(),
                    requested_token: requested.faucet_id(),
                    offered_amount: offered.amount().as_u64(),
                    requested_amount: requested.amount().as_u64(),
                    min_fill_step: order.pswap_note().storage().min_fill_step().as_u64(),
                    raw_note_data: note.to_bytes(),
                }
            })
            .collect();
        let input = PairBatch::from_ingest_orders(
            AssetId::new_fungible(base),
            AssetId::new_fungible(quote),
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
        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("expected all 200 orders to settle");
        };
        assert_eq!(plan.candidate.executions.len(), 200);
        assert_eq!(plan.candidate.buyer_base_target, U256::from(60_000u64));
        assert_eq!(plan.candidate.seller_quote_target, U256::from(120_000u64));
        assert_eq!(plan.candidate.released.base, U256::from(60_000u64));
        assert_eq!(plan.candidate.paid.base, U256::from(59_400u64));
        assert_eq!(plan.candidate.released.quote, U256::from(120_000u64));
        assert_eq!(plan.candidate.paid.quote, U256::from(118_800u64));
        assert_eq!(plan.accruals.realized_protocol_fee.base, U256::from(600u64));
        assert_eq!(
            plan.accruals.realized_protocol_fee.quote,
            U256::from(1_200u64)
        );
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());

        let arrivals = input
            .orders()
            .iter()
            .map(|order| (order.id(), 100))
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

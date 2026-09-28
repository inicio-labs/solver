use miden_protocol::asset::{AssetAmount, AssetId};

use super::config::ClearingConfig;
use super::envelope::ReachableFillMap;
use super::order::MatchOrder;
use super::settlement::PairLedger;
use super::types::{BatchPrice, ClearingError, ClearingOutcome, SkipReason};

const PRIORITY_RETRY_PARTS: usize = 5;

#[derive(Clone, Debug)]
pub struct PairBatch<'a> {
    clearing_price: BatchPrice,
    sell_orders: Vec<MatchOrder<'a>>,
    buy_orders: Vec<MatchOrder<'a>>,
}

impl<'a> PairBatch<'a> {
    /// The live book supplies both sides already price-eligible and sorted by
    /// price, then durable FIFO. The matcher does not revalidate that work.
    pub(crate) fn new(
        base: AssetId,
        quote: AssetId,
        clearing_price: BatchPrice,
        sell_orders: Vec<MatchOrder<'a>>,
        buy_orders: Vec<MatchOrder<'a>>,
    ) -> Result<Self, ClearingError> {
        if base == quote {
            return Err(ClearingError::InvalidConfig);
        }
        Ok(Self {
            clearing_price,
            sell_orders,
            buy_orders,
        })
    }

    pub(crate) fn orders(&self) -> impl Iterator<Item = &MatchOrder<'a>> {
        self.sell_orders.iter().chain(self.buy_orders.iter())
    }
}

pub struct PairMatcher<'batch, 'order> {
    batch: &'batch PairBatch<'order>,
    config: &'batch ClearingConfig,
}

impl<'batch, 'order> PairMatcher<'batch, 'order> {
    fn total_offered(orders: &[MatchOrder<'_>]) -> Result<u128, ClearingError> {
        orders.iter().try_fold(0u128, |total, order| {
            total
                .checked_add(u128::from(order.offered_amount().as_u64()))
                .ok_or(ClearingError::ArithmeticOverflow)
        })
    }

    pub(crate) fn new(batch: &'batch PairBatch<'order>, config: &'batch ClearingConfig) -> Self {
        Self { batch, config }
    }

    /// Drop a coarse block of the worst orders, then halve small prefixes.
    /// With 100 orders this tries 100, 80, 60, 40, 20, 10, 5, 2, 1.
    fn smaller_prefix(current: usize, step: usize) -> usize {
        if current > step * 2 {
            current - step
        } else {
            (current / 2).max(1)
        }
    }

    /// Maximize comparison volume at the frozen price, allocate that volume in
    /// price-time priority, and accept only an exactly solvent settlement.
    pub fn clear(self) -> Result<ClearingOutcome, ClearingError> {
        let all_sell_orders = &self.batch.sell_orders;
        let all_buy_orders = &self.batch.buy_orders;
        if all_sell_orders.is_empty() || all_buy_orders.is_empty() {
            return Ok(ClearingOutcome::Skipped(SkipReason::NoEligibleCross));
        }

        let mut sell_count = all_sell_orders.len();
        let mut buy_count = all_buy_orders.len();
        let sell_step = (sell_count / PRIORITY_RETRY_PARTS).max(1);
        let buy_step = (buy_count / PRIORITY_RETRY_PARTS).max(1);

        let (sell_orders, buy_orders, seller_fills, buyer_fills) = loop {
            let sell_orders = &all_sell_orders[..sell_count];
            let buy_orders = &all_buy_orders[..buy_count];

            // Recompute bounds after each trim. No seller can receive more
            // quote than buyers offer. The buyer bound includes one maximum
            // asset amount for floor(P * buyer_fill) rounding.
            let seller_limit = Self::total_offered(buy_orders)?;
            let buyer_limit = Self::total_offered(sell_orders)?
                .checked_add(u128::from(AssetAmount::MAX.as_u64()))
                .ok_or(ClearingError::ArithmeticOverflow)?;

            let seller_fills = match ReachableFillMap::build(
                sell_orders,
                self.config.max_total_intervals,
                seller_limit,
            ) {
                Ok(fills) => fills,
                Err(ClearingError::ResourceLimit) if sell_count > 1 => {
                    sell_count = Self::smaller_prefix(sell_count, sell_step);
                    continue;
                }
                Err(ClearingError::ResourceLimit) => {
                    return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit));
                }
                Err(error) => return Err(error),
            };
            let buyer_fills = match ReachableFillMap::build(
                buy_orders,
                self.config.max_total_intervals,
                buyer_limit,
            ) {
                Ok(fills) => fills,
                Err(ClearingError::ResourceLimit) if buy_count > 1 => {
                    buy_count = Self::smaller_prefix(buy_count, buy_step);
                    continue;
                }
                Err(ClearingError::ResourceLimit) => {
                    return Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit));
                }
                Err(error) => return Err(error),
            };
            break (sell_orders, buy_orders, seller_fills, buyer_fills);
        };
        if sell_count < all_sell_orders.len() || buy_count < all_buy_orders.len() {
            tracing::warn!(
                sellers_admitted = all_sell_orders.len(),
                sellers_used = sell_count,
                buyers_admitted = all_buy_orders.len(),
                buyers_used = buy_count,
                max_total_intervals = self.config.max_total_intervals,
                "clearing interval limit reduced the price-time candidate set"
            );
        }
        let Some((buyer_base_target, seller_quote_target)) =
            seller_fills.maximum_common_fill(&buyer_fills, self.batch.clearing_price)?
        else {
            return Ok(ClearingOutcome::Skipped(SkipReason::NoPositiveCross));
        };

        let seller_allocations =
            seller_fills.allocate_by_priority(sell_orders, seller_quote_target)?;
        let buyer_allocations = buyer_fills.allocate_by_priority(buy_orders, buyer_base_target)?;
        let mut executions = Vec::new();
        let mut ledger = PairLedger::default();
        for (orders, allocations) in [
            (sell_orders, &seller_allocations),
            (buy_orders, &buyer_allocations),
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
        let candidate = ledger.into_plan(executions);
        Ok(match candidate.finalize() {
            Ok(plan) => ClearingOutcome::Accepted(Box::new(plan)),
            Err(reason) => ClearingOutcome::Skipped(reason),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::order::{Order, OrderSide};
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use ruint::aliases::U256;
    use std::sync::Arc;

    use crate::clearing::{PairAmounts, ReferencePrice};
    use crate::matching::types::RateKey;
    use crate::types::IngestOrder;

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

    fn batch<'a>(price: BatchPrice, orders: &'a [Order]) -> PairBatch<'a> {
        let (base, quote) = assets();
        let (mut sell_orders, mut buy_orders): (Vec<_>, Vec<_>) = orders
            .iter()
            .partition(|order| order.offered_asset().faucet_id() == base);
        let priority = |order: &&Order| {
            (
                RateKey::new(
                    order.requested_asset().amount().as_u64(),
                    order.offered_asset().amount().as_u64(),
                ),
                order.priority_sequence(),
            )
        };
        sell_orders.sort_unstable_by_key(priority);
        buy_orders.sort_unstable_by_key(priority);
        let sell_orders = sell_orders
            .into_iter()
            .map(|order| {
                order
                    .prepare_if_eligible(OrderSide::SellBase, price, 0)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let buy_orders = buy_orders
            .into_iter()
            .map(|order| {
                order
                    .prepare_if_eligible(OrderSide::BuyBase, price, 0)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        PairBatch::new(
            AssetId::new_fungible(base),
            AssetId::new_fungible(quote),
            price,
            sell_orders,
            buy_orders,
        )
        .unwrap()
    }

    fn clear(batch: &PairBatch<'_>, config: &ClearingConfig) -> ClearingOutcome {
        PairMatcher::new(batch, config).clear().unwrap()
    }

    fn allocate_seller_fills(domains: &[(u64, u64)], target: u64) -> Vec<U256> {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = domains
            .iter()
            .enumerate()
            .map(|(index, &(maximum, minimum))| {
                order(
                    FungibleAsset::new(base, maximum).unwrap(),
                    FungibleAsset::new(quote, maximum).unwrap(),
                    minimum,
                    index as u64 + 1,
                    &mut rng,
                )
            })
            .collect::<Vec<_>>();
        let price = BatchPrice::from_ratio(1, 1).unwrap();
        let prepared = orders
            .iter()
            .map(|order| {
                order
                    .prepare_if_eligible(OrderSide::SellBase, price, 0)
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let map = ReachableFillMap::build(
            &prepared,
            ClearingConfig::default().max_total_intervals,
            u128::from(target),
        )
        .unwrap();
        map.allocate_by_priority(&prepared, U256::from(target))
            .unwrap()
    }

    #[test]
    fn ingested_note_adapter_preserves_native_note_and_fifo() {
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
            priority_seq: 7,
            note: Arc::new(note),
        };
        assert_eq!(
            Order::from_ingest_order(&ingested)
                .unwrap()
                .priority_sequence(),
            7
        );
        let price = BatchPrice::from_reference_prices(
            ReferencePrice::from_decimal("2").unwrap(),
            ReferencePrice::from_decimal("1").unwrap(),
            0,
            0,
        )
        .unwrap();
        let orders = vec![Order::from_ingest_order(&ingested).unwrap()];
        let input = batch(price, &orders);
        assert_eq!(input.orders().count(), 1);
        assert_eq!(input.clearing_price.quote_units, U256::from(2u8));
        assert_eq!(orders[0].id(), ingested.id());
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
        let input = batch(BatchPrice::from_ratio(2, 1).unwrap(), &orders);
        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.released.base, 11);
        assert_eq!(plan.candidate.released.quote, 22);
        assert_eq!(plan.candidate.executions[0].payment.amount().as_u64(), 20);
        assert_eq!(plan.candidate.executions[1].payment.amount().as_u64(), 10);
        assert_eq!(plan.accruals.realized_protocol_fee.base, 1);
        assert_eq!(plan.accruals.realized_protocol_fee.quote, 2);
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());
        let execution_batch = plan.to_execution_batch(&input).unwrap();
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
        let input = batch(BatchPrice::from_ratio(3, 1).unwrap(), &orders);
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
    fn zero_minimum_stays_zero_in_the_order_domain() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let seller = order(
            FungibleAsset::new(base, 10).unwrap(),
            FungibleAsset::new(quote, 10).unwrap(),
            0,
            1,
            &mut rng,
        );
        let prepared = seller
            .prepare_if_eligible(
                OrderSide::SellBase,
                BatchPrice::from_ratio(1, 1).unwrap(),
                0,
            )
            .unwrap()
            .unwrap();
        assert_eq!(prepared.fill_interval().minimum, U256::ZERO);
    }

    #[test]
    fn partial_seller_uses_inverse_ceil_and_protocol_payout() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
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
        ];
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
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
        assert_eq!(plan.accruals.realized_protocol_fee.base, 8);
        assert_eq!(plan.accruals.rounding_surplus.quote, 40);

        plan.to_execution_batch(&input).unwrap();
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
        let orders = vec![late, early, buyer];
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
        let ClearingOutcome::Accepted(plan) = clear(&input, &ClearingConfig::default()) else {
            panic!("expected a solvent settlement");
        };
        assert_eq!(plan.candidate.executions[0].order_id, early_id);
        assert_eq!(plan.candidate.executions.len(), 2);
        let settled = plan.to_execution_batch(&input).unwrap();
        assert_eq!(settled.filled_notes[0].note_id, early_id);
        assert_eq!(settled.filled_notes.len(), 2);
    }

    #[test]
    fn priority_allocation_fills_best_order_then_skips_unsupported_minimum() {
        assert_eq!(
            allocate_seller_fills(&[(8, 4), (10, 9), (4, 3)], 10),
            [U256::from(7u8), U256::ZERO, U256::from(3u8)]
        );
    }

    #[test]
    fn priority_allocation_skips_when_minimum_fits_but_later_orders_cannot_complete() {
        assert_eq!(
            allocate_seller_fills(&[(6, 4), (7, 7)], 7),
            [U256::ZERO, U256::from(7u8)]
        );
    }

    #[test]
    fn retry_prefix_keeps_best_orders_and_has_a_short_tail() {
        let mut count = 100;
        let mut tried = vec![count];
        while count > 1 {
            count = PairMatcher::smaller_prefix(count, 20);
            tried.push(count);
        }
        assert_eq!(tried, [100, 80, 60, 40, 20, 10, 5, 2, 1]);
    }

    #[test]
    fn seller_interval_limit_retries_without_removing_orders_from_the_batch() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let better = order(
            FungibleAsset::new(base, 5).unwrap(),
            FungibleAsset::new(quote, 4).unwrap(),
            4,
            1,
            &mut rng,
        );
        let better_id = better.id();
        let worse = order(
            FungibleAsset::new(base, 5).unwrap(),
            FungibleAsset::new(quote, 5).unwrap(),
            5,
            2,
            &mut rng,
        );
        let worse_id = worse.id();
        let buyer = order(
            FungibleAsset::new(quote, 5).unwrap(),
            FungibleAsset::new(base, 5).unwrap(),
            5,
            3,
            &mut rng,
        );
        let orders = [better, worse, buyer];
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
        let config = ClearingConfig {
            max_total_intervals: 3,
            ..ClearingConfig::default()
        };

        for _ in 0..2 {
            let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
                panic!("better seller should clear after trimming the worse seller");
            };
            assert_eq!(plan.candidate.executions.len(), 2);
            assert_eq!(plan.candidate.executions[0].order_id, better_id);
            assert!(plan
                .candidate
                .executions
                .iter()
                .all(|fill| fill.order_id != worse_id));
            assert_eq!(input.orders().count(), 3);
        }
    }

    #[test]
    fn buyer_interval_limit_retries_without_removing_orders_from_the_batch() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let seller = order(
            FungibleAsset::new(base, 5).unwrap(),
            FungibleAsset::new(quote, 5).unwrap(),
            5,
            1,
            &mut rng,
        );
        let better = order(
            FungibleAsset::new(quote, 6).unwrap(),
            FungibleAsset::new(base, 5).unwrap(),
            4,
            2,
            &mut rng,
        );
        let better_id = better.id();
        let worse = order(
            FungibleAsset::new(quote, 5).unwrap(),
            FungibleAsset::new(base, 5).unwrap(),
            5,
            3,
            &mut rng,
        );
        let worse_id = worse.id();
        let orders = [seller, better, worse];
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
        let config = ClearingConfig {
            max_total_intervals: 3,
            ..ClearingConfig::default()
        };

        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("better buyer should clear after trimming the worse buyer");
        };
        assert_eq!(plan.candidate.executions.len(), 2);
        assert_eq!(plan.candidate.executions[1].order_id, better_id);
        assert!(plan
            .candidate
            .executions
            .iter()
            .all(|fill| fill.order_id != worse_id));
        assert_eq!(input.orders().count(), 3);
    }

    #[test]
    fn hundred_sellers_retry_at_eighty_when_the_full_map_exceeds_budget() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let mut orders = Vec::with_capacity(101);
        for sequence in 1..=100 {
            orders.push(order(
                FungibleAsset::new(base, 5).unwrap(),
                FungibleAsset::new(quote, 5).unwrap(),
                5,
                sequence,
                &mut rng,
            ));
        }
        let eightieth_id = orders[79].id();
        let eighty_first_id = orders[80].id();
        orders.push(order(
            FungibleAsset::new(quote, 500).unwrap(),
            FungibleAsset::new(base, 500).unwrap(),
            5,
            101,
            &mut rng,
        ));
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
        let config = ClearingConfig {
            // 100 fixed-size sellers need 5,151 stored intervals; 80 need 3,321.
            max_total_intervals: 4_000,
            ..ClearingConfig::default()
        };

        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("the best 80 sellers should still clear");
        };
        assert_eq!(plan.candidate.executions.len(), 81);
        assert_eq!(plan.candidate.executions[79].order_id, eightieth_id);
        assert!(plan
            .candidate
            .executions
            .iter()
            .all(|fill| fill.order_id != eighty_first_id));
        assert_eq!(plan.candidate.released.base, 400);
        assert_eq!(plan.candidate.paid.base, 400);
        assert_eq!(input.orders().count(), 101);
    }

    #[test]
    fn one_order_that_exceeds_the_interval_budget_reports_resource_limit() {
        let (base, quote) = assets();
        let mut rng = RandomCoin::new(Word::default());
        let orders = vec![
            order(
                FungibleAsset::new(base, 5).unwrap(),
                FungibleAsset::new(quote, 5).unwrap(),
                5,
                1,
                &mut rng,
            ),
            order(
                FungibleAsset::new(quote, 5).unwrap(),
                FungibleAsset::new(base, 5).unwrap(),
                5,
                2,
                &mut rng,
            ),
        ];
        let input = batch(BatchPrice::from_ratio(1, 1).unwrap(), &orders);
        let config = ClearingConfig {
            max_total_intervals: 2,
            ..ClearingConfig::default()
        };

        assert!(matches!(
            clear(&input, &config),
            ClearingOutcome::Skipped(SkipReason::ResourceLimit)
        ));
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
                IngestOrder {
                    priority_seq: order.priority_sequence(),
                    note: Arc::new(note),
                }
            })
            .collect();
        let price = BatchPrice::from_reference_prices(
            ReferencePrice::from_decimal("2").unwrap(),
            ReferencePrice::from_decimal("1").unwrap(),
            0,
            0,
        )
        .unwrap();
        let parsed = ingested
            .iter()
            .map(Order::from_ingest_order)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let input = batch(price, &parsed);
        let config = ClearingConfig {
            protocol_fee_ppm: 10_000,
            ..ClearingConfig::default()
        };
        let ClearingOutcome::Accepted(plan) = clear(&input, &config) else {
            panic!("expected all 200 orders to settle");
        };
        assert_eq!(plan.candidate.executions.len(), 200);
        assert_eq!(plan.candidate.released.base, 60_000);
        assert_eq!(plan.candidate.paid.base, 59_400);
        assert_eq!(plan.candidate.released.quote, 120_000);
        assert_eq!(plan.candidate.paid.quote, 118_800);
        assert_eq!(plan.accruals.realized_protocol_fee.base, 600);
        assert_eq!(plan.accruals.realized_protocol_fee.quote, 1_200);
        assert_eq!(plan.accruals.rounding_surplus, PairAmounts::default());

        let execution_batch = plan.to_execution_batch(&input).unwrap();
        assert_eq!(execution_batch.filled_notes.len(), 200);
    }
}

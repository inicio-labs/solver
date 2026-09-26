use std::ops::Range;

use range_set_blaze::RangeSetBlaze;
use ruint::aliases::U256;

use super::order::{FillInterval, MatchOrder};
use super::types::{BatchPrice, ClearingError};

/// Backward map of comparison fills reachable from each order onward.
pub(crate) struct ReachableFillMap {
    intervals: Vec<FillInterval>,
    rows: Vec<Range<usize>>,
    max_total_intervals: usize,
    max_total_fill: u128,
}

impl ReachableFillMap {
    /// Build F[i] backward: each row contains every total reachable using
    /// orders i onward, including the choice to skip any of those orders.
    /// The cap sums liquidity from multiple notes and may exceed AssetAmount::MAX.
    pub(crate) fn build(
        orders: &[MatchOrder<'_>],
        max_total_intervals: usize,
        maximum_total_fill: u128,
    ) -> Result<Self, ClearingError> {
        // The extra row is F[n] = {0}: the total reachable with no orders left.
        let row_count = orders.len() + 1;
        let mut map = Self {
            intervals: Vec::new(),
            rows: Vec::with_capacity(row_count),
            max_total_intervals,
            max_total_fill: maximum_total_fill,
        };
        map.push_row(vec![FillInterval::default()])?;

        for order in orders.iter().rev() {
            map.add_order(order.fill_interval())?;
        }
        map.rows.reverse();
        Ok(map)
    }

    fn last_row(&self) -> &[FillInterval] {
        &self.intervals[self.rows.last().expect("initial row exists").clone()]
    }

    /// Form F[i] from F[i+1]: keep later totals (skip this order), or add one
    /// fill from this order's interval to each later total (use this order).
    fn add_order(&mut self, order_fills: FillInterval) -> Result<(), ClearingError> {
        let maximum = U256::from(self.max_total_fill);
        let mut reachable = RangeSetBlaze::<u128>::new();
        for &later in self.last_row() {
            for interval in [
                later,
                FillInterval {
                    minimum: later.minimum.saturating_add(order_fills.minimum),
                    maximum: later.maximum.saturating_add(order_fills.maximum),
                },
            ] {
                if interval.minimum > maximum {
                    continue;
                }
                let start = u128::try_from(interval.minimum)
                    .map_err(|_| ClearingError::ArithmeticOverflow)?;
                let end = u128::try_from(interval.maximum.min(maximum))
                    .map_err(|_| ClearingError::ArithmeticOverflow)?;
                reachable.ranges_insert(start..=end);
            }
        }
        let row = reachable.ranges().map(FillInterval::from).collect();
        self.push_row(row)
    }

    fn push_row(&mut self, mut reachable: Vec<FillInterval>) -> Result<(), ClearingError> {
        let end = self
            .intervals
            .len()
            .checked_add(reachable.len())
            .ok_or(ClearingError::ResourceLimit)?;
        if end > self.max_total_intervals {
            return Err(ClearingError::ResourceLimit);
        }
        let start = self.intervals.len();
        self.intervals.append(&mut reachable);
        self.rows.push(start..end);
        Ok(())
    }

    fn fills_from(&self, order_index: usize) -> Result<&[FillInterval], ClearingError> {
        let range = self
            .rows
            .get(order_index)
            .ok_or(ClearingError::InternalInvariant("missing reachable row"))?
            .clone();
        self.intervals
            .get(range)
            .ok_or(ClearingError::InternalInvariant("invalid reachable row"))
    }

    /// Find the largest buyer fill whose price-converted amount is reachable
    /// by the seller. Integer buyer fills can leave gaps after conversion.
    pub(crate) fn maximum_common_fill(
        &self,
        buyer_fills: &Self,
        price: BatchPrice,
    ) -> Result<Option<(U256, U256)>, ClearingError> {
        let seller_intervals = self.fills_from(0)?;
        let buyer_intervals = buyer_fills.fills_from(0)?;
        let mut seller_count = seller_intervals.len();
        let mut buyer_count = buyer_intervals.len();

        // Both rows are sorted. Walk from their largest intervals so the first
        // valid buyer fill is the maximum-volume match.
        while seller_count > 0 && buyer_count > 0 {
            let seller = seller_intervals[seller_count - 1];
            let buyer = buyer_intervals[buyer_count - 1];
            let buyer_minimum_quote = price.quote_for_base_floor(buyer.minimum)?;
            let buyer_maximum_quote = price.quote_for_base_floor(buyer.maximum)?;

            if buyer_minimum_quote > seller.maximum {
                buyer_count -= 1;
                continue;
            }
            if buyer_maximum_quote < seller.minimum {
                seller_count -= 1;
                continue;
            }

            // Find the largest actual buyer fill whose floor(P * fill) is at
            // most the seller's maximum. Converted endpoints alone are not
            // enough: at P=2, buyer fills [1,2] produce {2,4}, not [2,4].
            let mut low = buyer.minimum;
            let mut high = buyer.maximum;
            while low < high {
                let middle = low + (high - low).div_ceil(U256::from(2u8));
                if price.quote_for_base_floor(middle)? <= seller.maximum {
                    low = middle;
                } else {
                    high = middle - U256::ONE;
                }
            }
            let seller_quote = price.quote_for_base_floor(low)?;
            if seller_quote >= seller.minimum && seller_quote > U256::ZERO {
                return Ok(Some((low, seller_quote)));
            }

            // No buyer fill in this interval lands inside the seller interval.
            // Any lower buyer fill is also below it, so try a lower seller row.
            seller_count -= 1;
        }
        Ok(None)
    }

    /// Allocate a fixed target in price-time order. A better-ranked order gets
    /// its largest fill that the later orders can still complete, or is skipped.
    pub(crate) fn allocate_by_priority(
        &self,
        orders: &[MatchOrder<'_>],
        target: U256,
    ) -> Result<Vec<U256>, ClearingError> {
        if first_reachable(self.fills_from(0)?, target, target).is_none() {
            return Err(ClearingError::InternalInvariant("target is not reachable"));
        }
        let mut remaining = target;
        let mut allocations = Vec::with_capacity(orders.len());
        for (index, order) in orders.iter().enumerate() {
            if remaining == U256::ZERO {
                allocations.resize(orders.len(), U256::ZERO);
                break;
            }
            let order_fills = order.fill_interval();
            if remaining < order_fills.minimum {
                allocations.push(U256::ZERO);
                continue;
            }

            // A smaller feasible remainder means a larger fill for this order.
            // F[i+1] includes every combination of later fills and skips.
            let minimum_remainder = remaining.saturating_sub(order_fills.maximum);
            let maximum_remainder = remaining - order_fills.minimum;
            let later_fills = self.fills_from(index + 1)?;
            if let Some(remainder) =
                first_reachable(later_fills, minimum_remainder, maximum_remainder)
            {
                allocations.push(remaining - remainder);
                remaining = remainder;
            } else {
                // Because remaining is reachable in F[i], the skip branch
                // must be feasible when the fill branch is not.
                allocations.push(U256::ZERO);
            }
        }
        if remaining != U256::ZERO {
            return Err(ClearingError::InternalInvariant(
                "priority allocation did not reach target",
            ));
        }
        Ok(allocations)
    }
}

/// Find the smallest reachable remainder in a range. The rows are sorted, so
/// binary search locates the first possible interval without scanning values.
fn first_reachable(intervals: &[FillInterval], minimum: U256, maximum: U256) -> Option<U256> {
    if minimum > maximum {
        return None;
    }
    let index = intervals.partition_point(|interval| interval.maximum < minimum);
    let interval = intervals.get(index)?;
    let value = minimum.max(interval.minimum);
    (value <= maximum).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::asset::AssetAmount;

    #[test]
    fn aggregate_cap_can_exceed_one_asset_amount() {
        let amount = u128::from(AssetAmount::MAX.as_u64());
        let mut map = ReachableFillMap {
            intervals: vec![FillInterval {
                minimum: U256::ZERO,
                maximum: U256::ZERO,
            }],
            rows: vec![0..1],
            max_total_intervals: usize::MAX,
            max_total_fill: amount * 2,
        };
        let order = FillInterval {
            minimum: U256::from(amount),
            maximum: U256::from(amount),
        };

        map.add_order(order).unwrap();
        map.add_order(order).unwrap();

        assert_eq!(
            map.last_row().last().unwrap().maximum,
            U256::from(amount * 2)
        );
    }

    #[test]
    fn sparse_union_keeps_real_gaps() {
        let later = [
            FillInterval {
                minimum: U256::ZERO,
                maximum: U256::ZERO,
            },
            FillInterval {
                minimum: U256::from(7u8),
                maximum: U256::from(8u8),
            },
        ];
        let mut map = ReachableFillMap {
            intervals: later.to_vec(),
            rows: vec![0..later.len()],
            max_total_intervals: usize::MAX,
            max_total_fill: 20,
        };
        map.add_order(FillInterval {
            minimum: U256::from(2u8),
            maximum: U256::from(3u8),
        })
        .unwrap();
        assert_eq!(
            map.last_row()
                .iter()
                .map(|interval| (interval.minimum, interval.maximum))
                .collect::<Vec<_>>(),
            vec![
                (U256::ZERO, U256::ZERO),
                (U256::from(2u8), U256::from(3u8)),
                (U256::from(7u8), U256::from(11u8)),
            ]
        );
    }

    #[test]
    fn range_set_rows_match_exhaustive_fills_after_clipping() {
        let cap = 12u128;
        let mut map = ReachableFillMap {
            intervals: vec![FillInterval {
                minimum: U256::ZERO,
                maximum: U256::ZERO,
            }],
            rows: vec![0..1],
            max_total_intervals: usize::MAX,
            max_total_fill: cap,
        };
        let mut expected = std::collections::BTreeSet::from([0u128]);
        for (minimum, maximum) in [(4u128, 6u128), (9, 10), (3, 4)] {
            map.add_order(FillInterval {
                minimum: U256::from(minimum),
                maximum: U256::from(maximum),
            })
            .unwrap();
            let previous = expected.clone();
            for existing in previous {
                for fill in minimum..=maximum {
                    if existing + fill <= cap {
                        expected.insert(existing + fill);
                    }
                }
            }
            let actual: std::collections::BTreeSet<_> = map
                .last_row()
                .iter()
                .flat_map(|interval| {
                    u128::try_from(interval.minimum).unwrap()
                        ..=u128::try_from(interval.maximum).unwrap()
                })
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn wide_order_range_is_clipped_before_entering_the_range_set() {
        let mut map = ReachableFillMap {
            intervals: vec![FillInterval {
                minimum: U256::ZERO,
                maximum: U256::ZERO,
            }],
            rows: vec![0..1],
            max_total_intervals: usize::MAX,
            max_total_fill: 5,
        };
        map.add_order(FillInterval {
            minimum: U256::from(2u8),
            maximum: U256::MAX,
        })
        .unwrap();
        assert_eq!(
            map.last_row()
                .iter()
                .map(|interval| (interval.minimum, interval.maximum))
                .collect::<Vec<_>>(),
            [(U256::ZERO, U256::ZERO), (U256::from(2u8), U256::from(5u8))]
        );
    }

    fn map_from_mask(mask: u16) -> ReachableFillMap {
        let mut intervals: Vec<FillInterval> = Vec::new();
        for value in 0..16u8 {
            if mask & (1u16 << value) == 0 {
                continue;
            }
            let value = U256::from(value);
            if let Some(last) = intervals.last_mut() {
                if last.maximum + U256::ONE == value {
                    last.maximum = value;
                    continue;
                }
            }
            intervals.push(FillInterval {
                minimum: value,
                maximum: value,
            });
        }
        let end = intervals.len();
        ReachableFillMap {
            intervals,
            rows: vec![0..end],
            max_total_intervals: usize::MAX,
            max_total_fill: u128::MAX,
        }
    }

    #[test]
    fn buyer_conversion_checks_reachable_values_not_just_interval_endpoints() {
        let buyers = map_from_mask((1 << 1) | (1 << 2));
        let price = BatchPrice::from_ratio(2, 1).unwrap();

        // Buyer fills [1,2] convert to {2,4}; 3 lies between the endpoints
        // but is not a possible converted fill.
        let seller_three = map_from_mask(1 << 3);
        assert_eq!(
            seller_three.maximum_common_fill(&buyers, price).unwrap(),
            None
        );

        let seller_two_or_three = map_from_mask((1 << 2) | (1 << 3));
        assert_eq!(
            seller_two_or_three
                .maximum_common_fill(&buyers, price)
                .unwrap(),
            Some((U256::from(1u8), U256::from(2u8)))
        );
    }

    #[test]
    fn two_pointer_cross_matches_small_exhaustive_books() {
        for seller_mask in (1u16..=u16::MAX).step_by(997) {
            for buyer_mask in (1u16..=u16::MAX).step_by(1139) {
                let sellers = map_from_mask(seller_mask | 1);
                let buyers = map_from_mask(buyer_mask | 1);
                for quote_units in 1..=4u8 {
                    for base_units in 1..=4u8 {
                        let price =
                            BatchPrice::from_ratio(u64::from(quote_units), u64::from(base_units))
                                .unwrap();
                        let actual = sellers.maximum_common_fill(&buyers, price).unwrap();
                        let expected = (1..16u8).rev().find_map(|buyer_base| {
                            if buyer_mask & (1u16 << buyer_base) == 0 {
                                return None;
                            }
                            let seller_quote = u16::from(buyer_base) * u16::from(quote_units)
                                / u16::from(base_units);
                            (seller_quote > 0
                                && seller_quote < 16
                                && seller_mask & (1u16 << seller_quote) != 0)
                                .then_some((U256::from(buyer_base), U256::from(seller_quote)))
                        });
                        assert_eq!(
                            actual, expected,
                            "sell={seller_mask:#x} buy={buyer_mask:#x} P={quote_units}/{base_units}"
                        );
                    }
                }
            }
        }
    }
}

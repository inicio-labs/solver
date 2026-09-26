use std::ops::Range;

use range_set_blaze::RangeSetBlaze;
use ruint::aliases::U256;

use super::config::ClearingConfig;
use super::order::{FillInterval, MatchOrder};
use super::types::{BatchPrice, ClearingError, ResourceLimitKind};

/// Backward map of comparison fills reachable from each order onward.
pub(crate) struct ReachableFillMap {
    intervals: Vec<FillInterval>,
    rows: Vec<Range<usize>>,
}

impl ReachableFillMap {
    pub(crate) fn build(
        orders: &[MatchOrder<'_>],
        config: &ClearingConfig,
        maximum_fill: u128,
    ) -> Result<Self, ClearingError> {
        let suffix_count = orders
            .len()
            .checked_add(1)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        let mut map = Self {
            intervals: Vec::with_capacity(config.max_total_intervals.min(suffix_count)),
            rows: Vec::with_capacity(suffix_count),
        };
        let zero = FillInterval {
            minimum: U256::ZERO,
            maximum: U256::ZERO,
        };
        map.push_row(vec![zero], config.max_total_intervals)?;

        for order in orders.iter().rev() {
            map.add_order(order.fill_interval(), maximum_fill, config)?;
        }
        map.rows.reverse();
        Ok(map)
    }

    fn last_row(&self) -> &[FillInterval] {
        &self.intervals[self.rows.last().expect("initial row exists").clone()]
    }

    fn add_order(
        &mut self,
        order_fills: FillInterval,
        maximum_fill: u128,
        config: &ClearingConfig,
    ) -> Result<(), ClearingError> {
        let maximum = U256::from(maximum_fill);
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
        if reachable.ranges_len() > config.max_intervals_per_row {
            return Err(ClearingError::ResourceLimit(
                ResourceLimitKind::IntervalsPerRow,
            ));
        }
        let row = reachable
            .ranges()
            .map(|range| FillInterval {
                minimum: U256::from(*range.start()),
                maximum: U256::from(*range.end()),
            })
            .collect();
        self.push_row(row, config.max_total_intervals)
    }

    fn push_row(
        &mut self,
        mut reachable: Vec<FillInterval>,
        maximum_total: usize,
    ) -> Result<(), ClearingError> {
        let end = self.intervals.len().checked_add(reachable.len()).ok_or(
            ClearingError::ResourceLimit(ResourceLimitKind::TotalIntervals),
        )?;
        if end > maximum_total {
            return Err(ClearingError::ResourceLimit(
                ResourceLimitKind::TotalIntervals,
            ));
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
            .ok_or(ClearingError::InternalInvariant("missing reachable suffix"))?
            .clone();
        self.intervals
            .get(range)
            .ok_or(ClearingError::InternalInvariant("invalid reachable suffix"))
    }

    /// Return the greatest reachable buyer-base target whose one-time floor at
    /// the clearing price is reachable by the seller-quote map.
    pub(crate) fn maximum_common_fill(
        &self,
        buyer_fills: &Self,
        price: BatchPrice,
    ) -> Result<Option<(U256, U256)>, ClearingError> {
        let seller_intervals = self.fills_from(0)?;
        let buyer_intervals = buyer_fills.fills_from(0)?;
        let mut seller_index = 0;
        let mut buyer_index = 0;
        let mut best = None;

        while let (Some(seller), Some(buyer)) = (
            seller_intervals.get(seller_index),
            buyer_intervals.get(buyer_index),
        ) {
            // a <= floor(P*q) <= b iff ceil(a/P) <= q <= ceil((b+1)/P)-1.
            let lower = price.base_for_quote_ceil(seller.minimum)?;
            let after_maximum = seller
                .maximum
                .checked_add(U256::ONE)
                .ok_or(ClearingError::ArithmeticOverflow)?;
            let upper_exclusive = price.base_for_quote_ceil(after_maximum)?;
            if upper_exclusive > U256::ZERO {
                let upper = upper_exclusive - U256::ONE;
                let overlap_minimum = lower.max(buyer.minimum);
                let overlap_maximum = upper.min(buyer.maximum);
                if overlap_minimum <= overlap_maximum
                    && price.quote_for_base_floor(overlap_maximum)? > U256::ZERO
                {
                    best = Some(overlap_maximum);
                }
            }
            if upper_exclusive == U256::ZERO || upper_exclusive - U256::ONE < buyer.maximum {
                seller_index += 1;
            } else {
                buyer_index += 1;
            }
        }

        match best {
            Some(buyer_base_target) => Ok(Some((
                buyer_base_target,
                price.quote_for_base_floor(buyer_base_target)?,
            ))),
            None => Ok(None),
        }
    }

    /// Select the lexicographically largest per-order fill vector for a fixed
    /// target. Each order receives as much as suffix feasibility permits.
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
            let later_fills = self.fills_from(index + 1)?;
            let order_fills = order.fill_interval();
            let selected = if remaining >= order_fills.minimum {
                let minimum_remainder = if remaining > order_fills.maximum {
                    remaining - order_fills.maximum
                } else {
                    U256::ZERO
                };
                let maximum_remainder = remaining - order_fills.minimum;
                first_reachable(later_fills, minimum_remainder, maximum_remainder)
                    .map(|remainder| remaining - remainder)
            } else {
                None
            };
            let fill = if let Some(fill) = selected {
                fill
            } else if first_reachable(later_fills, remaining, remaining).is_some() {
                U256::ZERO
            } else {
                return Err(ClearingError::InternalInvariant(
                    "no feasible priority allocation",
                ));
            };
            remaining -= fill;
            allocations.push(fill);
        }
        if remaining != U256::ZERO {
            return Err(ClearingError::InternalInvariant(
                "priority allocation did not reach target",
            ));
        }
        Ok(allocations)
    }
}

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
        };
        map.add_order(
            FillInterval {
                minimum: U256::from(2u8),
                maximum: U256::from(3u8),
            },
            20,
            &ClearingConfig {
                max_intervals_per_row: 10,
                ..ClearingConfig::default()
            },
        )
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
        let config = ClearingConfig::default();
        let mut map = ReachableFillMap {
            intervals: vec![FillInterval {
                minimum: U256::ZERO,
                maximum: U256::ZERO,
            }],
            rows: vec![0..1],
        };
        let cap = 12u128;
        let mut expected = std::collections::BTreeSet::from([0u128]);
        for (minimum, maximum) in [(4u128, 6u128), (9, 10), (3, 4)] {
            map.add_order(
                FillInterval {
                    minimum: U256::from(minimum),
                    maximum: U256::from(maximum),
                },
                cap,
                &config,
            )
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
        };
        map.add_order(
            FillInterval {
                minimum: U256::from(2u8),
                maximum: U256::MAX,
            },
            5,
            &ClearingConfig::default(),
        )
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
        }
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

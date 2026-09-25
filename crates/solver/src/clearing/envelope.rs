use std::ops::Range;

use super::types::{
    ClearingConfig, ClearingError, ExactPrice, PreparedOrder, ReachableInterval, ResourceLimitKind,
    Wide,
};

pub(crate) struct EnvelopeStore {
    intervals: Vec<ReachableInterval>,
    rows: Vec<Range<usize>>,
}

impl EnvelopeStore {
    pub(crate) fn build(
        orders: &[PreparedOrder],
        config: &ClearingConfig,
    ) -> Result<Self, ClearingError> {
        let row_count = orders
            .len()
            .checked_add(1)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        let mut store = Self {
            intervals: Vec::with_capacity(config.max_total_intervals.min(row_count)),
            rows: Vec::with_capacity(row_count),
        };
        let zero = [ReachableInterval {
            lo: Wide::ZERO,
            hi: Wide::ZERO,
        }];
        store.push_row(&zero, config.max_total_intervals)?;
        let mut next_row = zero.to_vec();

        for order in orders.iter().rev() {
            let mut row = Vec::with_capacity(next_row.len().saturating_mul(2));
            merge_shifted(
                &next_row,
                order.domain,
                &mut row,
                config.max_intervals_per_row,
            )?;
            store.push_row(&row, config.max_total_intervals)?;
            next_row = row;
        }
        store.rows.reverse();
        Ok(store)
    }

    fn push_row(
        &mut self,
        row: &[ReachableInterval],
        max_total: usize,
    ) -> Result<(), ClearingError> {
        let end =
            self.intervals
                .len()
                .checked_add(row.len())
                .ok_or(ClearingError::ResourceLimit(
                    ResourceLimitKind::TotalIntervals,
                ))?;
        if end > max_total {
            return Err(ClearingError::ResourceLimit(
                ResourceLimitKind::TotalIntervals,
            ));
        }
        let start = self.intervals.len();
        self.intervals.extend_from_slice(row);
        self.rows.push(start..end);
        Ok(())
    }

    pub(crate) fn row(&self, index: usize) -> Result<&[ReachableInterval], ClearingError> {
        let range = self
            .rows
            .get(index)
            .ok_or(ClearingError::InternalInvariant("missing envelope row"))?
            .clone();
        self.intervals
            .get(range)
            .ok_or(ClearingError::InternalInvariant("invalid envelope row"))
    }
}

fn append_merged(
    output: &mut Vec<ReachableInterval>,
    next: ReachableInterval,
    max_intervals: usize,
) -> Result<(), ClearingError> {
    if let Some(last) = output.last_mut() {
        let adjacent_or_overlap = last.hi >= next.lo
            || last
                .hi
                .checked_add(Wide::ONE)
                .is_none_or(|after| after >= next.lo);
        if adjacent_or_overlap {
            last.hi = last.hi.max(next.hi);
            return Ok(());
        }
    }
    if output.len() >= max_intervals {
        return Err(ClearingError::ResourceLimit(
            ResourceLimitKind::IntervalsPerRow,
        ));
    }
    output.push(next);
    Ok(())
}

/// Linear two-stream merge of F_(i+1) and F_(i+1) + [L_i,H_i]. No sort and no
/// per-unit expansion. Output is sorted, disjoint, and adjacency-coalesced.
fn merge_shifted(
    suffix: &[ReachableInterval],
    domain: ReachableInterval,
    output: &mut Vec<ReachableInterval>,
    max_intervals: usize,
) -> Result<(), ClearingError> {
    let mut skip = 0;
    let mut take = 0;
    while skip < suffix.len() || take < suffix.len() {
        let choose_skip = if skip >= suffix.len() {
            false
        } else if take >= suffix.len() {
            true
        } else {
            let take_lo = suffix[take]
                .lo
                .checked_add(domain.lo)
                .ok_or(ClearingError::ArithmeticOverflow)?;
            suffix[skip].lo <= take_lo
        };
        let next = if choose_skip {
            let interval = suffix[skip];
            skip += 1;
            interval
        } else {
            let interval = ReachableInterval {
                lo: suffix[take]
                    .lo
                    .checked_add(domain.lo)
                    .ok_or(ClearingError::ArithmeticOverflow)?,
                hi: suffix[take]
                    .hi
                    .checked_add(domain.hi)
                    .ok_or(ClearingError::ArithmeticOverflow)?,
            };
            take += 1;
            interval
        };
        append_merged(output, next, max_intervals)?;
    }
    Ok(())
}

fn first_reachable(row: &[ReachableInterval], lower: Wide, upper: Wide) -> Option<Wide> {
    if lower > upper {
        return None;
    }
    let index = row.partition_point(|interval| interval.hi < lower);
    let interval = row.get(index)?;
    let value = lower.max(interval.lo);
    (value <= upper).then_some(value)
}

/// Return the greatest reachable buyer A target whose one-time floor at P is
/// reachable on the seller B side.
pub(crate) fn find_max_targets(
    sell: &EnvelopeStore,
    buy: &EnvelopeStore,
    price: ExactPrice,
) -> Result<Option<(Wide, Wide)>, ClearingError> {
    let sellers = sell.row(0)?;
    let buyers = buy.row(0)?;
    let mut sell_index = 0;
    let mut buy_index = 0;
    let mut best = None;
    while let (Some(seller), Some(buyer)) = (sellers.get(sell_index), buyers.get(buy_index)) {
        // a <= floor(P*q) <= b iff ceil(a/P) <= q <= ceil((b+1)/P)-1.
        let lower = price.base_for_quote_ceil(seller.lo)?;
        let past_hi = seller
            .hi
            .checked_add(Wide::ONE)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        let upper_exclusive = price.base_for_quote_ceil(past_hi)?;
        if upper_exclusive > Wide::ZERO {
            let upper = upper_exclusive - Wide::ONE;
            let lo = lower.max(buyer.lo);
            let hi = upper.min(buyer.hi);
            if lo <= hi && price.quote_for_base_floor(hi)? > Wide::ZERO {
                best = Some(hi);
            }
        }
        if upper_exclusive == Wide::ZERO || upper_exclusive - Wide::ONE < buyer.hi {
            sell_index += 1;
        } else {
            buy_index += 1;
        }
    }
    match best {
        Some(buyer_target) => Ok(Some((
            buyer_target,
            price.quote_for_base_floor(buyer_target)?,
        ))),
        None => Ok(None),
    }
}

/// Select the lexicographically largest per-order scaled fill vector at an
/// already-fixed side target. A better price/time order is filled as much as
/// suffix feasibility permits; a skipped order does not stop later orders.
pub(crate) fn allocate_by_priority(
    orders: &[PreparedOrder],
    envelope: &EnvelopeStore,
    target: Wide,
) -> Result<Vec<Wide>, ClearingError> {
    if first_reachable(envelope.row(0)?, target, target).is_none() {
        return Err(ClearingError::InternalInvariant("target outside envelope"));
    }
    let mut remaining = target;
    let mut allocations = Vec::with_capacity(orders.len());
    for (index, order) in orders.iter().enumerate() {
        let suffix = envelope.row(index + 1)?;
        let chosen = if remaining >= order.domain.lo {
            let lower = if remaining > order.domain.hi {
                remaining - order.domain.hi
            } else {
                Wide::ZERO
            };
            let upper = remaining - order.domain.lo;
            first_reachable(suffix, lower, upper).map(|remainder| remaining - remainder)
        } else {
            None
        };
        let fill = if let Some(fill) = chosen {
            fill
        } else if first_reachable(suffix, remaining, remaining).is_some() {
            Wide::ZERO
        } else {
            return Err(ClearingError::InternalInvariant(
                "no feasible priority allocation",
            ));
        };
        remaining -= fill;
        allocations.push(fill);
    }
    if remaining != Wide::ZERO {
        return Err(ClearingError::InternalInvariant(
            "priority allocation did not reach target",
        ));
    }
    Ok(allocations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_union_keeps_real_gaps() {
        let mut row = Vec::new();
        let suffix = [
            ReachableInterval {
                lo: Wide::ZERO,
                hi: Wide::ZERO,
            },
            ReachableInterval {
                lo: Wide::from(7u8),
                hi: Wide::from(8u8),
            },
        ];
        merge_shifted(
            &suffix,
            ReachableInterval {
                lo: Wide::from(2u8),
                hi: Wide::from(3u8),
            },
            &mut row,
            10,
        )
        .unwrap();
        assert_eq!(
            row.iter().map(|x| (x.lo, x.hi)).collect::<Vec<_>>(),
            vec![
                (Wide::ZERO, Wide::ZERO),
                (Wide::from(2u8), Wide::from(3u8)),
                (Wide::from(7u8), Wide::from(11u8)),
            ]
        );
    }

    fn store_from_mask(mask: u16) -> EnvelopeStore {
        let mut intervals: Vec<ReachableInterval> = Vec::new();
        for value in 0..16u8 {
            if mask & (1u16 << value) == 0 {
                continue;
            }
            let value = Wide::from(value);
            if let Some(last) = intervals.last_mut() {
                if last.hi + Wide::ONE == value {
                    last.hi = value;
                    continue;
                }
            }
            intervals.push(ReachableInterval {
                lo: value,
                hi: value,
            });
        }
        let end = intervals.len();
        EnvelopeStore {
            intervals,
            rows: vec![0..end],
        }
    }

    #[test]
    fn two_pointer_cross_matches_small_exhaustive_books() {
        for sell_mask in (1u16..=u16::MAX).step_by(997) {
            for buy_mask in (1u16..=u16::MAX).step_by(1139) {
                let sell = store_from_mask(sell_mask | 1);
                let buy = store_from_mask(buy_mask | 1);
                for numerator in 1..=4u8 {
                    for denominator in 1..=4u8 {
                        let price = ExactPrice::new(Wide::from(numerator), Wide::from(denominator))
                            .unwrap();
                        let actual = find_max_targets(&sell, &buy, price).unwrap();
                        let expected = (1..16u8).rev().find_map(|q| {
                            if buy_mask & (1u16 << q) == 0 {
                                return None;
                            }
                            let t = u16::from(q) * u16::from(numerator) / u16::from(denominator);
                            (t > 0 && t < 16 && sell_mask & (1u16 << t) != 0)
                                .then_some((Wide::from(q), Wide::from(t)))
                        });
                        assert_eq!(
                            actual, expected,
                            "sell={sell_mask:#x} buy={buy_mask:#x} P={numerator}/{denominator}"
                        );
                    }
                }
            }
        }
    }
}

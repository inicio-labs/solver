//! The bounded clearing price (ADR 0007): inside a band around the Binance
//! mid, the price that trades the most volume, and among those the one
//! closest to the mid.
//!
//! One walk pairs the pair's sells, cheapest first, with its buys, highest
//! first, straight from the book's index. A maker whose own bid and ask clash
//! first has the older order of every clashing pair sat out (rule B), so
//! orders that will never trade cannot set the price. Admission still applies
//! ADR 0005 at the price this walk returns.

use std::cmp::Ordering;

use miden_protocol::note::NoteId;
use ruint::aliases::U256;

use super::ClearingBook;
use crate::clearing::{checked_mul, BatchPrice, ClearingError, OrderSide, PPM_DENOMINATOR};
use crate::maker::MakerId;
use crate::types::TokenId;

const BPS_DENOMINATOR: u32 = 10_000;

/// Why the walk stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalkStop {
    SideExhausted,
    OrderCap,
    SellAboveBand,
    BuyBelowBand,
    NoLongerCrossing,
}

/// The walk's price and what produced it.
#[derive(Clone, Debug)]
pub(crate) struct BoundedPrice {
    pub(crate) price: BatchPrice,
    /// The limits of the last sell and buy the walk paired: every price
    /// between them trades the walk's volume.
    pub(crate) sell_limit: BatchPrice,
    pub(crate) buy_limit: BatchPrice,
    /// Quote units at the mid, scaled by the mid's base units.
    pub(crate) volume: U256,
    pub(crate) stop: WalkStop,
    /// The order whose limit is the price; `None` when the price is the mid.
    pub(crate) price_setter: Option<NoteId>,
    /// Maker orders that rule B sat out while pricing.
    pub(crate) sit_outs: Vec<NoteId>,
}

/// One order as the walk sees it: its fee-inclusive limit and what is left
/// of its size.
#[derive(Clone, Copy)]
struct Leg {
    id: NoteId,
    limit: BatchPrice,
    remaining: U256,
}

struct Walk {
    base: TokenId,
    quote: TokenId,
    mid: BatchPrice,
    low: BatchPrice,
    high: BatchPrice,
    fee_ppm: u32,
    /// Rule B per maker the walk has met: a pair has a handful of makers.
    decided: Vec<(MakerId, Vec<NoteId>)>,
}

impl ClearingBook {
    /// The pair's bounded price at `mid` with a band of `band_bps` each side,
    /// or `None` when nothing crosses inside the band. At most
    /// `max_orders_per_side` orders count on each side; sat-out orders take no
    /// slot.
    pub(in crate::matcher) fn bounded_price(
        &self,
        base: TokenId,
        quote: TokenId,
        mid: BatchPrice,
        band_bps: u16,
        fee_ppm: u32,
        max_orders_per_side: usize,
    ) -> Result<Option<BoundedPrice>, ClearingError> {
        let band = u32::from(band_bps);
        // Config caps the band far lower; this keeps the band's bottom above
        // zero for any caller.
        if band >= BPS_DENOMINATOR {
            return Err(ClearingError::InvalidConfig);
        }
        let mut walk = Walk {
            base,
            quote,
            mid,
            low: scaled(mid, BPS_DENOMINATOR - band)?,
            high: scaled(mid, BPS_DENOMINATOR + band)?,
            fee_ppm,
            decided: Vec::new(),
        };
        let mut sells = self
            .pairs
            .get(&(base, quote))
            .into_iter()
            .flat_map(|index| index.values());
        let mut buys = self
            .pairs
            .get(&(quote, base))
            .into_iter()
            .flat_map(|index| index.values());
        let (mut sell, mut buy) = (None, None);
        let (mut sells_counted, mut buys_counted) = (0usize, 0usize);
        let mut volume = U256::ZERO;
        let mut last = None;
        let stop = loop {
            if sell.is_none() {
                sell = self.next_leg(&mut sells, OrderSide::SellBase, &mut walk)?;
                sells_counted += usize::from(sell.is_some());
            }
            if buy.is_none() {
                buy = self.next_leg(&mut buys, OrderSide::BuyBase, &mut walk)?;
                buys_counted += usize::from(buy.is_some());
            }
            let (Some(s), Some(b)) = (sell, buy) else {
                break WalkStop::SideExhausted;
            };
            if sells_counted > max_orders_per_side || buys_counted > max_orders_per_side {
                break WalkStop::OrderCap;
            }
            // Both sides sort best first, so nothing later is in the band either.
            if compare(s.limit, walk.high)? == Ordering::Greater {
                break WalkStop::SellAboveBand;
            }
            if compare(b.limit, walk.low)? == Ordering::Less {
                break WalkStop::BuyBelowBand;
            }
            if compare(s.limit, b.limit)? == Ordering::Greater {
                break WalkStop::NoLongerCrossing;
            }
            let matched = s.remaining.min(b.remaining);
            volume = volume
                .checked_add(matched)
                .ok_or(ClearingError::ArithmeticOverflow)?;
            last = Some((s, b));
            sell = (s.remaining > matched).then_some(Leg {
                remaining: s.remaining - matched,
                ..s
            });
            buy = (b.remaining > matched).then_some(Leg {
                remaining: b.remaining - matched,
                ..b
            });
        };
        let Some((s, b)) = last else {
            return Ok(None);
        };
        // Max volume holds on [s.limit, b.limit]; the mid, or the end nearest it.
        let (price, price_setter) = if compare(mid, s.limit)? == Ordering::Less {
            (s.limit, Some(s.id))
        } else if compare(mid, b.limit)? == Ordering::Greater {
            (b.limit, Some(b.id))
        } else {
            (mid, None)
        };
        Ok(Some(BoundedPrice {
            price,
            sell_limit: s.limit,
            buy_limit: b.limit,
            volume,
            stop,
            price_setter,
            sit_outs: walk.decided.into_iter().flat_map(|(_, out)| out).collect(),
        }))
    }

    /// The next order of `ids` that rule B keeps, as a leg of `side`.
    fn next_leg<'a>(
        &self,
        ids: &mut impl Iterator<Item = &'a NoteId>,
        side: OrderSide,
        walk: &mut Walk,
    ) -> Result<Option<Leg>, ClearingError> {
        for &id in ids {
            let order = self
                .orders
                .get(&id)
                .ok_or(ClearingError::InternalInvariant("missing indexed order"))?;
            if let Some(tag) = order.maker_tag() {
                let at = match walk
                    .decided
                    .iter()
                    .position(|(maker_id, _)| *maker_id == tag.maker_id)
                {
                    Some(at) => at,
                    None => {
                        let out = self.rule_b(tag.maker_id, walk)?;
                        walk.decided.push((tag.maker_id, out));
                        walk.decided.len() - 1
                    }
                };
                // A sit-out takes no slot: the next order in line moves up.
                if walk.decided[at].1.contains(&id) {
                    continue;
                }
            }
            let offered = order.offered_asset().amount().as_u64();
            let requested = order.requested_asset().amount().as_u64();
            // Sizes in quote at the mid without dividing: a sell's base times
            // the mid's quote units, a buy's quote times its base units.
            let (limit, size) = match side {
                OrderSide::SellBase => (
                    sell_limit(offered, requested, walk.fee_ppm)?,
                    checked_mul(offered, walk.mid.quote_units)?,
                ),
                OrderSide::BuyBase => (
                    buy_limit(offered, requested, walk.fee_ppm)?,
                    checked_mul(offered, walk.mid.base_units)?,
                ),
            };
            return Ok(Some(Leg {
                id,
                limit,
                remaining: size,
            }));
        }
        Ok(None)
    }

    /// Rule B for one maker. Among its active orders on the pair that could
    /// trade inside the band (bids at or above its bottom, asks at or below
    /// its top), the older order of every bid and ask that clash (bid at or
    /// above ask) sits out. A newer order counts as newer even when it sits
    /// out itself, so at any single price this is ADR 0005's decision.
    fn rule_b(&self, maker_id: MakerId, walk: &Walk) -> Result<Vec<NoteId>, ClearingError> {
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        for id in self.makers.orders_on_pair(maker_id, walk.base, walk.quote) {
            // Inactive orders are in flight, not in the book.
            let Some(order) = self.orders.get(&id).filter(|order| order.is_active()) else {
                continue;
            };
            let Some(tag) = order.maker_tag() else {
                continue;
            };
            let (offered, requested) = (order.offered_asset(), order.requested_asset());
            let tokens = (offered.faucet_id(), requested.faucet_id());
            let amounts = (offered.amount().as_u64(), requested.amount().as_u64());
            if tokens == (walk.base, walk.quote) {
                let limit = sell_limit(amounts.0, amounts.1, walk.fee_ppm)?;
                if compare(limit, walk.high)? != Ordering::Greater {
                    asks.push((limit, tag.root_seq, id));
                }
            } else if tokens == (walk.quote, walk.base) {
                let limit = buy_limit(amounts.0, amounts.1, walk.fee_ppm)?;
                if compare(limit, walk.low)? != Ordering::Less {
                    bids.push((limit, tag.root_seq, id));
                }
            }
        }
        let (Some(top_bid), Some(low_ask)) = (
            extreme(bids.iter().map(|bid| bid.0), Ordering::Greater)?,
            extreme(asks.iter().map(|ask| ask.0), Ordering::Less)?,
        ) else {
            return Ok(Vec::new());
        };
        // The common case: the maker's quotes don't cross, nothing to sort.
        if compare(top_bid, low_ask)? == Ordering::Less {
            return Ok(Vec::new());
        }
        // Only orders in the clash zone can clash with anything.
        let mut zone = Vec::new();
        for (limit, seq, id) in bids {
            if compare(limit, low_ask)? != Ordering::Less {
                zone.push((limit, seq, OrderSide::BuyBase, id));
            }
        }
        for (limit, seq, id) in asks {
            if compare(limit, top_bid)? != Ordering::Greater {
                zone.push((limit, seq, OrderSide::SellBase, id));
            }
        }
        // Newest first. A bid and an ask never share a sequence (one per
        // submit); if one ever did, the ask counts as newer, as in ADR 0005.
        zone.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| (a.2 == OrderSide::BuyBase).cmp(&(b.2 == OrderSide::BuyBase)))
        });
        let (mut lowest_newer_ask, mut highest_newer_bid) = (None, None);
        let mut sit_out = Vec::new();
        for (limit, _, side, id) in zone {
            match side {
                OrderSide::BuyBase => {
                    if let Some(ask) = lowest_newer_ask {
                        if compare(limit, ask)? != Ordering::Less {
                            sit_out.push(id);
                        }
                    }
                    highest_newer_bid = extreme(
                        [highest_newer_bid, Some(limit)].into_iter().flatten(),
                        Ordering::Greater,
                    )?;
                }
                OrderSide::SellBase => {
                    if let Some(bid) = highest_newer_bid {
                        if compare(limit, bid)? != Ordering::Greater {
                            sit_out.push(id);
                        }
                    }
                    lowest_newer_ask = extreme(
                        [lowest_newer_ask, Some(limit)].into_iter().flatten(),
                        Ordering::Less,
                    )?;
                }
            }
        }
        Ok(sit_out)
    }
}

/// `a` against `b`, exactly: `a.quote / a.base` against `b.quote / b.base`.
fn compare(a: BatchPrice, b: BatchPrice) -> Result<Ordering, ClearingError> {
    Ok(checked_mul(a.quote_units, b.base_units)?.cmp(&checked_mul(b.quote_units, a.base_units)?))
}

/// The highest (`Ordering::Greater`) or lowest (`Ordering::Less`) of `prices`.
fn extreme(
    prices: impl IntoIterator<Item = BatchPrice>,
    keep: Ordering,
) -> Result<Option<BatchPrice>, ClearingError> {
    let mut best: Option<BatchPrice> = None;
    for price in prices {
        best = match best {
            Some(current) if compare(price, current)? != keep => Some(current),
            _ => Some(price),
        };
    }
    Ok(best)
}

/// `price × factor / 10_000`.
fn scaled(price: BatchPrice, factor: u32) -> Result<BatchPrice, ClearingError> {
    BatchPrice::new(
        checked_mul(price.quote_units, factor)?,
        checked_mul(price.base_units, BPS_DENOMINATOR)?,
    )
}

/// The lowest price at which a sell offering `offered` base for `requested`
/// quote is eligible after the fee: `eligible_units`' threshold, exactly.
pub(super) fn sell_limit(
    offered: u64,
    requested: u64,
    fee_ppm: u32,
) -> Result<BatchPrice, ClearingError> {
    BatchPrice::new(
        checked_mul(requested, PPM_DENOMINATOR)?,
        checked_mul(offered, PPM_DENOMINATOR - fee_ppm)?,
    )
}

/// The highest price at which a buy offering `offered` quote for `requested`
/// base is eligible after the fee.
pub(super) fn buy_limit(
    offered: u64,
    requested: u64,
    fee_ppm: u32,
) -> Result<BatchPrice, ClearingError> {
    BatchPrice::new(
        checked_mul(offered, PPM_DENOMINATOR)?,
        checked_mul(requested, PPM_DENOMINATOR + fee_ppm)?,
    )
}

#[cfg(test)]
mod tests;

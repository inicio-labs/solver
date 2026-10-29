//! The bounded clearing price walk (ADR 0007), shared by the matcher, which
//! walks its orders with self-trade prevention, and the price API, which
//! walks its mirror of the book's levels.
//!
//! Inside a band around the mid, the price that trades the most volume, and
//! among those the one closest to the mid. The walk pairs sells, cheapest
//! first, with buys, highest first, while they cross inside the band; the
//! limits of the last pair bound every price that trades that volume.

use std::cmp::Ordering;

use ruint::aliases::U256;

use super::config::PPM_DENOMINATOR;
use super::math::checked_mul;
use super::order::OrderSide;
use super::types::{BatchPrice, ClearingError};

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

/// The band around the mid that a batch price may move in.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Band {
    pub(crate) mid: BatchPrice,
    pub(crate) low: BatchPrice,
    pub(crate) high: BatchPrice,
}

impl Band {
    /// `band_bps` either side of `mid`. Config caps the band far lower; a
    /// band of 100% or more is refused, since its bottom would be zero.
    pub(crate) fn new(mid: BatchPrice, band_bps: u16) -> Result<Self, ClearingError> {
        let band = u32::from(band_bps);
        if band >= BPS_DENOMINATOR {
            return Err(ClearingError::InvalidConfig);
        }
        Ok(Self {
            mid,
            low: scaled(mid, BPS_DENOMINATOR - band)?,
            high: scaled(mid, BPS_DENOMINATOR + band)?,
        })
    }
}

/// One order, or one level of orders at a single rate, as the walk sees it:
/// its fee-inclusive limit and what is left of its size.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Leg<T> {
    pub(crate) tag: T,
    pub(crate) limit: BatchPrice,
    /// Quote units at the mid, scaled by the mid's base units, so that no
    /// division rounds a small order away.
    pub(crate) remaining: U256,
}

impl<T> Leg<T> {
    /// Sells at the rate of `offered` base for `requested` quote, `volume`
    /// base of them in all (an order's own offer, or a level's sum).
    pub(crate) fn sell(
        tag: T,
        offered: u64,
        requested: u64,
        volume: u64,
        fee_ppm: u32,
        mid: BatchPrice,
    ) -> Result<Self, ClearingError> {
        Ok(Self {
            tag,
            limit: sell_limit(offered, requested, fee_ppm)?,
            remaining: checked_mul(volume, mid.quote_units)?,
        })
    }

    /// Buys at the rate of `offered` quote for `requested` base, `volume`
    /// quote of them in all.
    pub(crate) fn buy(
        tag: T,
        offered: u64,
        requested: u64,
        volume: u64,
        fee_ppm: u32,
        mid: BatchPrice,
    ) -> Result<Self, ClearingError> {
        Ok(Self {
            tag,
            limit: buy_limit(offered, requested, fee_ppm)?,
            remaining: checked_mul(volume, mid.base_units)?,
        })
    }
}

/// Where the walk takes its legs from: each side best first, sells by
/// ascending limit and buys by descending limit.
pub(crate) trait Legs<T> {
    /// The next leg on `side`, or `None` once the side is exhausted.
    fn next(&mut self, side: OrderSide) -> Result<Option<Leg<T>>, ClearingError>;
}

/// The walk's price and what produced it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Walked<T> {
    pub(crate) price: BatchPrice,
    /// The limits of the last sell and buy the walk paired: every price
    /// between them trades the walk's volume.
    pub(crate) sell_limit: BatchPrice,
    pub(crate) buy_limit: BatchPrice,
    pub(crate) volume: U256,
    pub(crate) stop: WalkStop,
    /// The leg whose limit is the price; `None` when the price is the mid.
    pub(crate) price_setter: Option<T>,
}

/// The bounded price of the legs inside `band`, or `None` when nothing
/// crosses there. At most `max_orders_per_side` legs count on each side.
pub(crate) fn walk<T: Copy>(
    band: &Band,
    max_orders_per_side: usize,
    legs: &mut impl Legs<T>,
) -> Result<Option<Walked<T>>, ClearingError> {
    let (mut sell, mut buy) = (None, None);
    let (mut sells_counted, mut buys_counted) = (0usize, 0usize);
    let mut volume = U256::ZERO;
    let mut last = None;
    let stop = loop {
        if sell.is_none() {
            sell = legs.next(OrderSide::SellBase)?;
            sells_counted += usize::from(sell.is_some());
        }
        if buy.is_none() {
            buy = legs.next(OrderSide::BuyBase)?;
            buys_counted += usize::from(buy.is_some());
        }
        let (Some(s), Some(b)) = (sell, buy) else {
            break WalkStop::SideExhausted;
        };
        if sells_counted > max_orders_per_side || buys_counted > max_orders_per_side {
            break WalkStop::OrderCap;
        }
        // Both sides sort best first, so nothing later is in the band either.
        if compare(s.limit, band.high)? == Ordering::Greater {
            break WalkStop::SellAboveBand;
        }
        if compare(b.limit, band.low)? == Ordering::Less {
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
    let (price, price_setter) = if compare(band.mid, s.limit)? == Ordering::Less {
        (s.limit, Some(s.tag))
    } else if compare(band.mid, b.limit)? == Ordering::Greater {
        (b.limit, Some(b.tag))
    } else {
        (band.mid, None)
    };
    Ok(Some(Walked {
        price,
        sell_limit: s.limit,
        buy_limit: b.limit,
        volume,
        stop,
        price_setter,
    }))
}

/// `a` against `b`, exactly: `a.quote / a.base` against `b.quote / b.base`.
pub(crate) fn compare(a: BatchPrice, b: BatchPrice) -> Result<Ordering, ClearingError> {
    Ok(checked_mul(a.quote_units, b.base_units)?.cmp(&checked_mul(b.quote_units, a.base_units)?))
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
pub(crate) fn sell_limit(
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
pub(crate) fn buy_limit(
    offered: u64,
    requested: u64,
    fee_ppm: u32,
) -> Result<BatchPrice, ClearingError> {
    BatchPrice::new(
        checked_mul(offered, PPM_DENOMINATOR)?,
        checked_mul(requested, PPM_DENOMINATOR + fee_ppm)?,
    )
}

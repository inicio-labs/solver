//! The bounded clearing price (ADR 0007) over the live book: the shared walk
//! ([`crate::clearing::bounded`]) fed with the pair's orders, cheapest sell
//! and highest buy first, straight from the book's index.
//!
//! With self-trade prevention on, a maker whose own bid and ask clash first
//! has the older order of every clashing pair sat out (rule B), so orders
//! that will never trade cannot set the price. Admission still applies ADR
//! 0005 at the price the walk returns.

use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap};

use miden_protocol::note::NoteId;
use ruint::aliases::U256;

use super::ClearingBook;
pub(crate) use crate::clearing::bounded::WalkStop;
use crate::clearing::bounded::{self, buy_limit, compare, sell_limit, Band, Leg, Legs};
use crate::clearing::{BatchPrice, ClearingError, OrderKey, OrderSide};
use crate::maker::MakerId;
use crate::types::TokenId;

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

/// One side of the pair's index, best first.
type Ids<'a> = std::iter::Flatten<std::option::IntoIter<btree_map::Values<'a, OrderKey, NoteId>>>;

/// The pair's orders as the walk's legs, with rule B decided per maker the
/// first time the walk meets one of its orders.
struct BookLegs<'a> {
    book: &'a ClearingBook,
    sells: Ids<'a>,
    buys: Ids<'a>,
    base: TokenId,
    quote: TokenId,
    band: Band,
    fee_ppm: u32,
    self_trade_prevention: bool,
    /// Rule B per maker the walk has met: a pair has a handful of makers.
    decided: Vec<(MakerId, Vec<NoteId>)>,
}

impl ClearingBook {
    /// The pair's bounded price at `mid` with a band of `band_bps` each side,
    /// or `None` when nothing crosses inside the band. At most
    /// `max_orders_per_side` orders count on each side; sat-out orders take no
    /// slot. Without `self_trade_prevention` nothing sits out, as admission
    /// then skips ADR 0005.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::matcher) fn bounded_price(
        &self,
        base: TokenId,
        quote: TokenId,
        mid: BatchPrice,
        band_bps: u16,
        fee_ppm: u32,
        max_orders_per_side: usize,
        self_trade_prevention: bool,
    ) -> Result<Option<BoundedPrice>, ClearingError> {
        let band = Band::new(mid, band_bps)?;
        let ids = |pair| {
            self.pairs
                .get(&pair)
                .map(BTreeMap::values)
                .into_iter()
                .flatten()
        };
        let mut legs = BookLegs {
            book: self,
            sells: ids((base, quote)),
            buys: ids((quote, base)),
            base,
            quote,
            band,
            fee_ppm,
            self_trade_prevention,
            decided: Vec::new(),
        };
        let Some(walked) = bounded::walk(&band, max_orders_per_side, &mut legs)? else {
            return Ok(None);
        };
        Ok(Some(BoundedPrice {
            price: walked.price,
            sell_limit: walked.sell_limit,
            buy_limit: walked.buy_limit,
            volume: walked.volume,
            stop: walked.stop,
            price_setter: walked.price_setter,
            sit_outs: legs.decided.into_iter().flat_map(|(_, out)| out).collect(),
        }))
    }
}

impl Legs<NoteId> for BookLegs<'_> {
    fn next(&mut self, side: OrderSide) -> Result<Option<Leg<NoteId>>, ClearingError> {
        loop {
            let ids = match side {
                OrderSide::SellBase => &mut self.sells,
                OrderSide::BuyBase => &mut self.buys,
            };
            let Some(&id) = ids.next() else {
                return Ok(None);
            };
            let order = self
                .book
                .orders
                .get(&id)
                .ok_or(ClearingError::InternalInvariant("missing indexed order"))?;
            if let Some(tag) = order.maker_tag().filter(|_| self.self_trade_prevention) {
                let at = match self
                    .decided
                    .iter()
                    .position(|(maker_id, _)| *maker_id == tag.maker_id)
                {
                    Some(at) => at,
                    None => {
                        let out = self.rule_b(tag.maker_id)?;
                        self.decided.push((tag.maker_id, out));
                        self.decided.len() - 1
                    }
                };
                // A sit-out takes no slot: the next order in line moves up.
                if self.decided[at].1.contains(&id) {
                    continue;
                }
            }
            let offered = order.offered_asset().amount().as_u64();
            let requested = order.requested_asset().amount().as_u64();
            let (fee, mid) = (self.fee_ppm, self.band.mid);
            return Ok(Some(match side {
                OrderSide::SellBase => Leg::sell(id, offered, requested, offered, fee, mid)?,
                OrderSide::BuyBase => Leg::buy(id, offered, requested, offered, fee, mid)?,
            }));
        }
    }
}

impl BookLegs<'_> {
    /// Rule B for one maker. Among its active orders on the pair that could
    /// trade inside the band (bids at or above its bottom, asks at or below
    /// its top), the older order of every bid and ask that clash (bid at or
    /// above ask) sits out. A newer order counts as newer even when it sits
    /// out itself, so at any single price this is ADR 0005's decision.
    fn rule_b(&self, maker_id: MakerId) -> Result<Vec<NoteId>, ClearingError> {
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        for id in self
            .book
            .makers
            .orders_on_pair(maker_id, self.base, self.quote)
        {
            // Inactive orders are in flight, not in the book.
            let Some(order) = self.book.orders.get(&id).filter(|order| order.is_active()) else {
                continue;
            };
            let Some(tag) = order.maker_tag() else {
                continue;
            };
            let (offered, requested) = (order.offered_asset(), order.requested_asset());
            let tokens = (offered.faucet_id(), requested.faucet_id());
            let amounts = (offered.amount().as_u64(), requested.amount().as_u64());
            if tokens == (self.base, self.quote) {
                let limit = sell_limit(amounts.0, amounts.1, self.fee_ppm)?;
                if compare(limit, self.band.high)? != Ordering::Greater {
                    asks.push((limit, tag.root_seq, id));
                }
            } else if tokens == (self.quote, self.base) {
                let limit = buy_limit(amounts.0, amounts.1, self.fee_ppm)?;
                if compare(limit, self.band.low)? != Ordering::Less {
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

#[cfg(test)]
mod tests;

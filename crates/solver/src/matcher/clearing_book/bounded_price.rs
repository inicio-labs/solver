//! The bounded clearing price (ADR 0007) over the live book: the shared walk
//! ([`crate::clearing::bounded`]) fed with the pair's orders, cheapest sell
//! and highest buy first, straight from the book's index.
//!
//! A maker's own crossing orders are cancelled at submit (ADR 0009), so the
//! walk does not look at makers. A pair that rounding leaves slightly crossed
//! on a nearly filled remainder can set the price: a known limitation (#93).

use std::collections::{btree_map, BTreeMap};

use miden_protocol::note::NoteId;
use ruint::aliases::U256;

use super::ClearingBook;
pub(crate) use crate::clearing::bounded::WalkStop;
use crate::clearing::bounded::{self, Band, Leg, Legs};
use crate::clearing::{BatchPrice, ClearingError, OrderKey, OrderSide};
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
}

/// One side of the pair's index, best first.
type Ids<'a> = std::iter::Flatten<std::option::IntoIter<btree_map::Values<'a, OrderKey, NoteId>>>;

/// The pair's orders as the walk's legs, best first.
struct BookLegs<'a> {
    book: &'a ClearingBook,
    sells: Ids<'a>,
    buys: Ids<'a>,
    band: Band,
    fee_ppm: u32,
}

impl ClearingBook {
    /// The pair's bounded price at `mid` with a band of `band_bps` each side,
    /// or `None` when nothing crosses inside the band. At most
    /// `max_orders_per_side` orders count on each side.
    pub(in crate::matcher) fn bounded_price(
        &self,
        base: TokenId,
        quote: TokenId,
        mid: BatchPrice,
        band_bps: u16,
        fee_ppm: u32,
        max_orders_per_side: usize,
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
            band,
            fee_ppm,
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
        }))
    }
}

impl Legs<NoteId> for BookLegs<'_> {
    fn next(&mut self, side: OrderSide) -> Result<Option<Leg<NoteId>>, ClearingError> {
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
        let offered = order.offered_asset().amount().as_u64();
        let requested = order.requested_asset().amount().as_u64();
        let (fee, mid) = (self.fee_ppm, self.band.mid);
        Ok(Some(match side {
            OrderSide::SellBase => Leg::sell(id, offered, requested, offered, fee, mid)?,
            OrderSide::BuyBase => Leg::buy(id, offered, requested, offered, fee, mid)?,
        }))
    }
}

#[cfg(test)]
mod tests;

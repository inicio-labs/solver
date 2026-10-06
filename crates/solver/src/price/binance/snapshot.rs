//! The publisher's merged quote state and the snapshot it publishes.
//!
//! Both readers offer observations for the same symbols. Per symbol, the
//! highest Binance update ID wins; an equal or lower ID changes nothing, so a
//! duplicate never renews a quote's age. A newer identifiable quote that
//! failed validation makes the symbol invalid, and an older good quote cannot
//! revive it. An equal ID with different content is reported as a conflict,
//! which is how a divergence between the two endpoints would show.
//!
//! A snapshot carries the TTL, so the matcher and the price API apply the
//! same freshness rule: a quote is fresh while `now - received_at < ttl`.
//! Callers read `now` after taking the snapshot; a receipt time after `now`
//! is reported as [`PriceUnavailable::FutureReceipt`] rather than treated as
//! fresh.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::market::{Markets, Orientation, Symbol, Valuation};
use super::ticker::QuoteRejection;
use crate::clearing::ReferencePrice;
use crate::types::TokenId;

/// One reader's parsed `bookTicker` update for a subscribed symbol.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Observation {
    /// Index into [`Markets::symbols`].
    pub(crate) symbol: usize,
    pub(crate) update_id: u64,
    /// Monotonic local time the socket delivered the frame.
    pub(crate) received_at: Instant,
    /// The exact midpoint, or why this quote is invalid.
    pub(crate) mid: Result<ReferencePrice, QuoteRejection>,
}

/// A validated quote: Binance's `quote per base` midpoint for one symbol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Quote {
    pub(crate) update_id: u64,
    pub(crate) mid: ReferencePrice,
    pub(crate) received_at: Instant,
}

/// The published state of one subscribed symbol. `Valid` says the newest
/// update passed validation, not that it is fresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SymbolQuote {
    /// No update since the feed started.
    Missing,
    Valid(Quote),
    /// The newest identifiable update failed validation.
    Invalid {
        update_id: u64,
    },
}

impl SymbolQuote {
    /// The Binance update ID behind this state.
    fn update_id(&self) -> Option<u64> {
        match self {
            Self::Missing => None,
            Self::Valid(quote) => Some(quote.update_id),
            Self::Invalid { update_id } => Some(*update_id),
        }
    }
}

/// Why no price is available.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum PriceUnavailable {
    /// No confirmed Binance market prices this pair or token.
    NoMarket,
    /// The market has had no update yet.
    NoQuote,
    /// The newest update failed validation.
    Invalid,
    /// The quote is at least the TTL old.
    Stale,
    /// The receipt time is after the caller's clock reading.
    FutureReceipt,
}

/// What offering an observation did to the published state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Offer {
    /// The symbol's state changed.
    Accepted,
    /// An equal or lower update ID, or an unknown symbol: nothing changed.
    NotNewer,
    /// The same update ID as the published state with different content;
    /// nothing changed, but the two sources disagree.
    Conflict,
}

/// A wallet valuation: one whole token in the valuation quote asset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Valued {
    pub(crate) price: ReferencePrice,
    /// When the quote was received; `None` for the quote asset itself.
    pub(crate) received_at: Option<Instant>,
    /// Whether the quote is younger than the TTL.
    pub(crate) fresh: bool,
}

/// Latest merged quote per subscribed symbol, with the markets they price and
/// the TTL that decides freshness. Consumers clone the `Arc` once per use.
/// The default snapshot has no markets: everything is unavailable.
#[derive(Clone, Debug, Default)]
pub struct PriceSnapshot {
    markets: Arc<Markets>,
    quotes: Vec<SymbolQuote>,
    ttl: Duration,
}

impl PriceSnapshot {
    /// An empty book for `markets`, whose quotes are fresh while younger
    /// than `ttl`.
    pub(super) fn new(markets: Arc<Markets>, ttl: Duration) -> Self {
        let quotes = vec![SymbolQuote::Missing; markets.symbols().len()];
        Self {
            markets,
            quotes,
            ttl,
        }
    }

    /// Apply one observation. Only the publisher calls this.
    pub(super) fn offer(&mut self, observation: &Observation) -> Offer {
        let Some(quote) = self.quotes.get_mut(observation.symbol) else {
            return Offer::NotNewer;
        };
        match quote.update_id() {
            Some(seen) if observation.update_id < seen => return Offer::NotNewer,
            Some(seen) if observation.update_id == seen => {
                let same = match (&*quote, &observation.mid) {
                    (SymbolQuote::Valid(published), Ok(mid)) => published.mid == *mid,
                    (SymbolQuote::Invalid { .. }, Err(_)) => true,
                    _ => false,
                };
                return if same {
                    Offer::NotNewer
                } else {
                    Offer::Conflict
                };
            }
            _ => {}
        }
        *quote = match observation.mid {
            Ok(mid) => SymbolQuote::Valid(Quote {
                update_id: observation.update_id,
                mid,
                received_at: observation.received_at,
            }),
            Err(_) => SymbolQuote::Invalid {
                update_id: observation.update_id,
            },
        };
        Offer::Accepted
    }

    pub(crate) fn markets(&self) -> &Markets {
        &self.markets
    }

    /// Every subscribed symbol with its current state.
    pub(crate) fn quotes(&self) -> impl Iterator<Item = (&Symbol, &SymbolQuote)> {
        self.markets.symbols().iter().zip(&self.quotes)
    }

    /// The valid quote of `symbol` and whether its age is below the TTL.
    fn quote(&self, symbol: usize, now: Instant) -> Result<(Quote, bool), PriceUnavailable> {
        match self
            .quotes
            .get(symbol)
            .copied()
            .unwrap_or(SymbolQuote::Missing)
        {
            SymbolQuote::Missing => Err(PriceUnavailable::NoQuote),
            SymbolQuote::Invalid { .. } => Err(PriceUnavailable::Invalid),
            SymbolQuote::Valid(quote) => {
                let age = now
                    .checked_duration_since(quote.received_at)
                    .ok_or(PriceUnavailable::FutureReceipt)?;
                Ok((quote, age < self.ttl))
            }
        }
    }

    /// Whole `quote` tokens per whole `base` token for a confirmed clearing
    /// pair, as configured, while its quote is fresh at `now`.
    pub(crate) fn pair_price(
        &self,
        base: TokenId,
        quote: TokenId,
        now: Instant,
    ) -> Result<ReferencePrice, PriceUnavailable> {
        let source = self
            .markets
            .pair(base, quote)
            .ok_or(PriceUnavailable::NoMarket)?;
        let (latest, fresh) = self.quote(source.symbol, now)?;
        if !fresh {
            return Err(PriceUnavailable::Stale);
        }
        Ok(match source.orientation {
            Orientation::Direct => latest.mid,
            Orientation::Reverse => latest.mid.reciprocal(),
        })
    }

    /// Like [`Self::pair_price`] for a clearing pair configured in either
    /// direction: whole `requested` tokens per whole `offered` token.
    pub(crate) fn market_price(
        &self,
        offered: TokenId,
        requested: TokenId,
        now: Instant,
    ) -> Result<ReferencePrice, PriceUnavailable> {
        match self.pair_price(offered, requested, now) {
            Err(PriceUnavailable::NoMarket) => self
                .pair_price(requested, offered, now)
                .map(ReferencePrice::reciprocal),
            result => result,
        }
    }

    /// One whole `token` in the valuation quote asset, under the same TTL as
    /// clearing. A stale quote is still returned, marked not fresh; callers
    /// decide whether to serve it.
    pub(crate) fn valuation(
        &self,
        token: TokenId,
        now: Instant,
    ) -> Result<Valued, PriceUnavailable> {
        match self
            .markets
            .valuation(token)
            .ok_or(PriceUnavailable::NoMarket)?
        {
            Valuation::Unit => Ok(Valued {
                price: ReferencePrice::ONE,
                received_at: None,
                fresh: true,
            }),
            Valuation::Market(symbol) => {
                let (quote, fresh) = self.quote(symbol, now)?;
                Ok(Valued {
                    price: quote.mid,
                    received_at: Some(quote.received_at),
                    fresh,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::price::binance::test_support::{btc, eth, price, reversed_eth_markets, usdt};
    use crate::price::binance::ticker::{parse_frame, Frame, QuoteLimits};

    const TTL: Duration = Duration::from_secs(2);

    /// USDT/ETH configured with USDT as base, priced by ETHUSDT (reversed).
    fn book() -> PriceSnapshot {
        PriceSnapshot::new(Arc::new(reversed_eth_markets()), TTL)
    }

    fn observe(update_id: u64, received_at: Instant, mid: Option<&str>) -> Observation {
        Observation {
            symbol: 0,
            update_id,
            received_at,
            mid: mid.map(price).ok_or(QuoteRejection::Crossed),
        }
    }

    #[test]
    fn highest_update_id_wins() {
        let mut book = book();
        let start = Instant::now();
        assert_eq!(
            book.offer(&observe(10, start, Some("2000"))),
            Offer::Accepted
        );
        // Equal or lower IDs from the other reader change nothing, including age.
        let later = start + Duration::from_secs(1);
        assert_eq!(
            book.offer(&observe(10, later, Some("2000"))),
            Offer::NotNewer
        );
        assert_eq!(
            book.offer(&observe(9, later, Some("1999"))),
            Offer::NotNewer
        );
        let (_, quote) = book.quotes().next().unwrap();
        assert_eq!(
            *quote,
            SymbolQuote::Valid(Quote {
                update_id: 10,
                mid: price("2000"),
                received_at: start,
            })
        );
        // Unknown symbol indexes are ignored.
        assert_eq!(
            book.offer(&Observation {
                symbol: 7,
                ..observe(11, later, Some("1"))
            }),
            Offer::NotNewer
        );
    }

    /// The same update ID with different content is a conflict between the
    /// two endpoints; the published state stays as it was.
    #[test]
    fn same_id_with_different_content_is_a_conflict() {
        let mut book = book();
        let now = Instant::now();
        book.offer(&observe(10, now, Some("2000")));
        assert_eq!(book.offer(&observe(10, now, Some("2001"))), Offer::Conflict);
        assert_eq!(book.offer(&observe(10, now, None)), Offer::Conflict);
        assert_eq!(
            book.pair_price(usdt(), eth(), now),
            Ok(price("2000").reciprocal())
        );
        book.offer(&observe(11, now, None));
        assert_eq!(book.offer(&observe(11, now, Some("2000"))), Offer::Conflict);
        assert_eq!(book.offer(&observe(11, now, None)), Offer::NotNewer);
    }

    #[test]
    fn newer_invalid_quote_blocks_older_good_quotes() {
        let mut book = book();
        let now = Instant::now();
        assert_eq!(book.offer(&observe(10, now, Some("2000"))), Offer::Accepted);
        assert_eq!(book.offer(&observe(11, now, None)), Offer::Accepted);
        assert_eq!(book.offer(&observe(10, now, Some("2000"))), Offer::NotNewer);
        assert_eq!(
            book.pair_price(usdt(), eth(), now),
            Err(PriceUnavailable::Invalid)
        );
        // Only a newer valid quote restores the pair.
        assert_eq!(book.offer(&observe(12, now, Some("2500"))), Offer::Accepted);
        assert_eq!(
            book.pair_price(usdt(), eth(), now),
            Ok(price("2500").reciprocal())
        );
    }

    #[test]
    fn ttl_boundary_is_stale() {
        let mut snapshot = book();
        let received = Instant::now();
        snapshot.offer(&observe(1, received, Some("2000")));
        let just_inside = received + TTL - Duration::from_nanos(1);
        assert!(snapshot.pair_price(usdt(), eth(), just_inside).is_ok());
        assert_eq!(
            snapshot.pair_price(usdt(), eth(), received + TTL),
            Err(PriceUnavailable::Stale)
        );
        // A receipt after the reading is never usable.
        let before = received.checked_sub(Duration::from_millis(1)).unwrap();
        assert_eq!(
            snapshot.pair_price(usdt(), eth(), before),
            Err(PriceUnavailable::FutureReceipt)
        );
        // Valuation uses the same TTL and still reports a stale quote, marked
        // as such.
        assert!(snapshot.valuation(eth(), just_inside).unwrap().fresh);
        let valued = snapshot.valuation(eth(), received + TTL).unwrap();
        assert_eq!(valued.price, price("2000"));
        assert!(!valued.fresh);
        assert_eq!(valued.received_at, Some(received));
    }

    #[test]
    fn lookups_follow_confirmed_markets() {
        let mut snapshot = book();
        let now = Instant::now();
        assert_eq!(
            snapshot.pair_price(usdt(), eth(), now),
            Err(PriceUnavailable::NoQuote)
        );
        snapshot.offer(&observe(1, now, Some("2000")));
        // The matcher asks only for the configured direction.
        assert_eq!(
            snapshot.pair_price(eth(), usdt(), now),
            Err(PriceUnavailable::NoMarket)
        );
        // Swap guidance accepts either direction: requested per offered.
        assert_eq!(snapshot.market_price(eth(), usdt(), now), Ok(price("2000")));
        assert_eq!(
            snapshot.market_price(usdt(), eth(), now),
            Ok(price("2000").reciprocal())
        );
        assert_eq!(
            snapshot.market_price(eth(), btc(), now),
            Err(PriceUnavailable::NoMarket)
        );
        assert_eq!(
            snapshot.valuation(usdt(), now),
            Ok(Valued {
                price: ReferencePrice::ONE,
                received_at: None,
                fresh: true
            })
        );
        assert_eq!(
            snapshot.valuation(btc(), now),
            Err(PriceUnavailable::NoMarket)
        );
        // The default snapshot prices nothing.
        assert_eq!(
            PriceSnapshot::default().valuation(usdt(), now),
            Err(PriceUnavailable::NoMarket)
        );
    }

    /// A reversed pair is priced at `1 / mid`, not at the midpoint of the
    /// reciprocal bid and ask.
    #[test]
    fn reversed_pair_uses_the_reciprocal_of_the_midpoint() {
        let mut book = book();
        let now = Instant::now();
        let frame = r#"{"stream":"ethusdt@bookTicker","data":{"u":1,"s":"ETHUSDT","b":"2000","B":"1","a":"2002","A":"1"}}"#;
        let limits = QuoteLimits {
            max_spread_bps: 100,
            min_notional: None,
        };
        let Ok(Frame::Quote(observation)) = parse_frame(frame, book.markets(), limits, now) else {
            panic!("a quote frame");
        };
        book.offer(&observation);
        let usdt_in_eth = book.pair_price(usdt(), eth(), now).unwrap();
        assert_eq!(usdt_in_eth, price("2001").reciprocal());
        let averaged_reciprocals =
            ReferencePrice::midpoint(price("2002").reciprocal(), price("2000").reciprocal())
                .unwrap();
        assert_ne!(usdt_in_eth, averaged_reciprocals);
    }
}

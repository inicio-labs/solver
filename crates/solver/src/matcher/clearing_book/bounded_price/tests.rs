use std::cmp::Ordering;

use miden_protocol::crypto::rand::RandomCoin;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};
use miden_protocol::Word;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rust_decimal::Decimal;

use super::super::tests::{fixture, tagged};
use super::*;
use crate::clearing::bounded::{buy_limit, compare, sell_limit};
use crate::maker::MakerId;

fn base() -> TokenId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
}

fn quote() -> TokenId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
}

/// Two quote units per base unit.
fn mid() -> BatchPrice {
    BatchPrice::from_ratio(2, 1).unwrap()
}

/// The mid moved by `tenths` tenths of a basis point.
fn at(tenths: i64) -> BatchPrice {
    BatchPrice::from_ratio(2 * (100_000 + tenths) as u64, 100_000).unwrap()
}

/// A book built order by order. Limits are tenths of a basis point from the
/// mid; sizes are lots of 100,000 base units.
struct Book {
    book: ClearingBook,
    rng: RandomCoin,
    priority: u64,
}

impl Book {
    fn new() -> Self {
        Self {
            book: ClearingBook::default(),
            rng: RandomCoin::new(Word::default()),
            priority: 0,
        }
    }

    /// `_name` labels the order for the reader only.
    fn add(
        &mut self,
        _name: &'static str,
        buy: bool,
        tenths: i64,
        lots: u64,
        maker: Option<(MakerId, u64)>,
    ) -> NoteId {
        self.priority += 1;
        let base_units = 100_000 * lots;
        let quote_units = 2 * (100_000 + tenths) as u64 * lots;
        let order = if buy {
            fixture(true, quote_units, base_units, self.priority, &mut self.rng)
        } else {
            fixture(false, base_units, quote_units, self.priority, &mut self.rng)
        };
        let order = match maker {
            Some((maker_id, root_seq)) => tagged(order, maker_id, root_seq),
            None => order,
        };
        self.book.insert_or_skip(&order);
        order.id()
    }

    fn user(&mut self, name: &'static str, buy: bool, tenths: i64, lots: u64) -> NoteId {
        self.add(name, buy, tenths, lots, None)
    }

    fn maker(
        &mut self,
        name: &'static str,
        maker_id: MakerId,
        root_seq: u64,
        buy: bool,
        tenths: i64,
        lots: u64,
    ) -> NoteId {
        self.add(name, buy, tenths, lots, Some((maker_id, root_seq)))
    }

    fn walk(&self, band_bps: u16) -> Option<BoundedPrice> {
        self.walk_with(band_bps, 0, usize::MAX)
    }

    fn walk_with(&self, band_bps: u16, fee_ppm: u32, cap: usize) -> Option<BoundedPrice> {
        self.book
            .bounded_price(base(), quote(), mid(), band_bps, fee_ppm, cap)
            .unwrap()
    }
}

const BUY: bool = true;
const SELL: bool = false;
const LOT: u64 = 100_000;

#[test]
fn users_meeting_around_the_mid_clear_at_the_mid() {
    let mut book = Book::new();
    book.user("seller", SELL, -100, 1);
    book.user("buyer", BUY, 100, 1);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, mid());
    assert_eq!(walk.price_setter, None);
    assert_eq!(walk.volume, U256::from(2 * LOT));
}

#[test]
fn more_buyers_clear_at_the_makers_ask() {
    let mut book = Book::new();
    let ask = book.maker("ask", 7, 2, SELL, 65, 5);
    book.maker("bid", 7, 1, BUY, -65, 5);
    book.user("seller", SELL, -10, 1);
    book.user("buyer", BUY, 400, 2);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, at(65));
    assert_eq!(walk.price_setter, Some(ask));
}

#[test]
fn more_sellers_clear_at_the_makers_bid() {
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 65, 5);
    let bid = book.maker("bid", 7, 2, BUY, -65, 5);
    book.user("seller", SELL, -400, 2);
    book.user("buyer", BUY, 10, 1);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, at(-65));
    assert_eq!(walk.price_setter, Some(bid));
}

#[test]
fn a_book_that_does_not_cross_has_no_price() {
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 65, 5);
    book.maker("bid", 7, 2, BUY, -65, 5);
    let walk = book.book.bounded_price(base(), quote(), mid(), 30, 0, 100);
    assert!(walk.unwrap().is_none());
    assert!(Book::new().walk(30).is_none(), "an empty book has no price");
}

#[test]
fn the_band_bounds_the_price() {
    // A maker quote outside the band never trades or sets the price.
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 400, 5);
    book.user("buyer", BUY, 500, 1);
    assert_eq!(book.walk(30).map(|walk| walk.stop), None);

    // Exactly at the band's top it can, and the price is that top.
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 300, 5);
    book.user("buyer", BUY, 500, 1);
    assert_eq!(book.walk(30).unwrap().price, at(300));

    // A hair above it, it cannot.
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 301, 5);
    book.user("buyer", BUY, 500, 1);
    assert!(book.walk(30).is_none());
}

#[test]
fn the_fee_counts_toward_the_band() {
    // +25 bps raw is about +35 bps once a 0.1% fee is added: outside 30 bps.
    let mut book = Book::new();
    book.maker("ask", 7, 1, SELL, 250, 5);
    book.user("buyer", BUY, 600, 1);
    assert!(book.walk_with(30, 0, usize::MAX).is_some());
    assert!(book.walk_with(30, 1_000, usize::MAX).is_none());
}

/// A maker's own crossed pair counts like any other orders: the walk does
/// not look at makers. Self-trade prevention at submit cancels such a pair
/// before it reaches the book (ADR 0009), except a pair that rounding leaves
/// slightly crossed on a nearly filled remainder, which can set the price:
/// the known limitation in #93.
#[test]
fn a_makers_crossed_pair_counts_like_any_other_orders() {
    let mut book = Book::new();
    book.maker("Y ask +25 #1", 1, 1, SELL, 250, 1);
    book.maker("Y bid +26 #2", 1, 2, BUY, 260, 1);
    book.maker("Y ask +8 #3", 1, 3, SELL, 80, 3);
    book.user("users", BUY, 400, 3);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, at(250));
}

/// Orders in flight are off the index and do not count.
#[test]
fn an_order_in_flight_is_ignored() {
    let mut book = Book::new();
    let in_flight = book.user("bid in flight", BUY, 200, 1);
    book.maker("ask", 1, 2, SELL, 10, 1);
    book.user("buyer", BUY, 20, 1);
    book.book.deactivate(in_flight);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, at(10));
}

#[test]
fn the_order_cap_stops_the_walk() {
    let mut book = Book::new();
    for _ in 0..3 {
        book.user("seller", SELL, -10, 1);
    }
    book.user("buyer", BUY, 10, 5);
    let walk = book.walk_with(30, 0, 2).unwrap();
    assert_eq!(walk.stop, WalkStop::OrderCap);
    assert_eq!(walk.volume, U256::from(2 * 2 * LOT));
}

/// ETH (18 decimals) against USDT (6): one USDT unit buys 4·10^8 wei at
/// 2500 USDT per ETH. Sizes are compared without dividing, so the smallest
/// orders still pair.
#[test]
fn eighteen_and_six_decimals_pair_their_smallest_orders() {
    let eth_usdt = BatchPrice::from_whole_price(Decimal::from(2500), 18, 6).unwrap();
    let mut rng = RandomCoin::new(Word::default());
    let mut book = ClearingBook::default();
    book.insert_or_skip(&fixture(false, 400_000_000, 1, 1, &mut rng));
    book.insert_or_skip(&fixture(true, 1, 400_000_000, 2, &mut rng));
    let walk = book
        .bounded_price(base(), quote(), eth_usdt, 30, 0, 100)
        .unwrap()
        .unwrap();
    assert_eq!(walk.price, eth_usdt);
    assert!(walk.volume > U256::ZERO);
}

#[test]
fn a_band_wider_than_the_price_is_an_error_not_a_panic() {
    let mut book = Book::new();
    book.user("seller", SELL, -10, 1);
    book.user("buyer", BUY, 10, 1);
    assert!(book.walk_with(9_999, 0, 100).is_some());
    assert!(matches!(
        book.book
            .bounded_price(base(), quote(), mid(), 10_000, 0, 100),
        Err(ClearingError::InvalidConfig)
    ));
}

#[test]
fn an_overflow_is_an_error_not_a_panic() {
    let mut book = Book::new();
    book.user("seller", SELL, -10, 1);
    book.user("buyer", BUY, 10, 1);
    let huge = BatchPrice::new(U256::from(1u8) << 250, U256::from(1u8)).unwrap();
    assert!(matches!(
        book.book.bounded_price(base(), quote(), huge, 30, 0, 100),
        Err(ClearingError::ArithmeticOverflow)
    ));
}

/// The fee-inclusive limits are exactly `eligible_units`' thresholds.
#[test]
fn limits_are_the_eligibility_thresholds() {
    let fee = 1_000;
    let (offered, requested) = (100_000, 200_650);
    let sell = sell_limit(offered, requested, fee).unwrap();
    let buy = buy_limit(requested, offered, fee).unwrap();
    let eligible = |side, price| {
        let (offered, requested) = match side {
            OrderSide::SellBase => (offered, requested),
            OrderSide::BuyBase => (requested, offered),
        };
        crate::clearing::eligible_units(side, price, fee, offered, requested)
            .unwrap()
            .is_some()
    };
    let nudge = |price: BatchPrice, up: bool| {
        let (q, b) = (
            price.quote_units * U256::from(1_000_000u32),
            price.base_units * U256::from(1_000_000u32),
        );
        BatchPrice::new(
            if up {
                q + U256::from(1u8)
            } else {
                q - U256::from(1u8)
            },
            b,
        )
        .unwrap()
    };
    assert!(eligible(OrderSide::SellBase, sell));
    assert!(!eligible(OrderSide::SellBase, nudge(sell, false)));
    assert!(eligible(OrderSide::BuyBase, buy));
    assert!(!eligible(OrderSide::BuyBase, nudge(buy, true)));
}

/// Random books against a brute-force oracle: every candidate price inside
/// the band, its volume, the most volume, then the price closest to the mid.
#[test]
fn the_walk_matches_a_brute_force_oracle() {
    let mut rng = StdRng::seed_from_u64(7);
    let mut off_mid = 0;
    for round in 0..2_000 {
        let mut book = Book::new();
        let mut orders = Vec::new();
        for _ in 0..rng.random_range(0..6) {
            let (buy, tenths, lots) = (
                rng.random_bool(0.5),
                rng.random_range(-600..600),
                rng.random_range(1..5),
            );
            orders.push((
                book.user("user", buy, tenths, lots),
                buy,
                tenths,
                lots,
                None,
            ));
        }
        for maker_id in 1..=rng.random_range(0..3) {
            for root_seq in 1..=rng.random_range(1..6) {
                let (buy, tenths, lots) = (
                    rng.random_bool(0.5),
                    rng.random_range(-500..500),
                    rng.random_range(1..4),
                );
                let id = book.maker("maker", maker_id, root_seq, buy, tenths, lots);
                orders.push((id, buy, tenths, lots, Some((maker_id, root_seq))));
            }
        }
        // Some orders are in flight: off the index, invisible to both.
        orders.retain(|order| {
            let in_flight = rng.random_bool(0.1);
            if in_flight {
                book.book.deactivate(order.0);
            }
            !in_flight
        });
        let band = 30;
        let (low, high) = (at(-300), at(300));
        let le = |a: BatchPrice, b: BatchPrice| compare(a, b).unwrap() != Ordering::Greater;
        // In quote at the mid: a sell's base times 2, a buy's quote.
        let size = |buy: bool, tenths: i64, lots: u64| {
            if buy {
                2 * (100_000 + tenths) as u64 * lots
            } else {
                2 * LOT * lots
            }
        };
        let volume = |price: BatchPrice| {
            let side = |buy: bool| -> u64 {
                orders
                    .iter()
                    .filter(|order| order.1 == buy)
                    .filter(|order| {
                        if buy {
                            le(price, at(order.2))
                        } else {
                            le(at(order.2), price)
                        }
                    })
                    .map(|order| size(order.1, order.2, order.3))
                    .sum()
            };
            side(true).min(side(false))
        };
        let mut candidates = vec![low, high, mid()];
        candidates.extend(
            orders
                .iter()
                .map(|order| at(order.2))
                .filter(|&p| le(low, p) && le(p, high)),
        );
        let best = candidates.iter().map(|&p| volume(p)).max().unwrap();
        let expected = (best > 0).then(|| {
            if volume(mid()) == best {
                return mid();
            }
            let at_best = candidates.iter().copied().filter(|&p| volume(p) == best);
            let above =
                at_best
                    .clone()
                    .filter(|&p| le(mid(), p))
                    .reduce(|a, b| if le(a, b) { a } else { b });
            above.unwrap_or_else(|| at_best.reduce(|a, b| if le(a, b) { b } else { a }).unwrap())
        });
        let walk = book.walk(band);
        assert_eq!(
            walk.as_ref().map(|walk| walk.price),
            expected,
            "round {round}"
        );
        if let Some(walk) = walk {
            assert_eq!(walk.volume, U256::from(best), "round {round}");
            off_mid += usize::from(walk.price != mid());
        }
    }
    // The books exercise what matters: prices off the mid.
    assert!(off_mid > 200, "{off_mid} off the mid");
}

/// The price API walks the matcher's book as levels, by the same rules: both
/// find the same price on random books, makers' crossed orders included.
/// Grouping equal-rate orders into a level changes nothing.
#[test]
fn the_price_api_walk_on_levels_matches_the_matcher() {
    use crate::price::{OrderMarket, PairRules};
    use crate::swap_eta::{expected_price, Probe};

    let mut rng = StdRng::seed_from_u64(11);
    let (mut off_mid, mut rounds) = (0, 0);
    for round in 0..1_000 {
        let mut book = Book::new();
        for _ in 0..rng.random_range(0..10) {
            let buy = rng.random_bool(0.5);
            // A few distinct prices, so equal-rate orders share a level.
            let tenths = 50 * rng.random_range(-8..8);
            if rng.random_bool(0.4) {
                let maker_id = rng.random_range(1..3);
                book.maker(
                    "maker",
                    maker_id,
                    round * 100 + rounds,
                    buy,
                    tenths,
                    rng.random_range(1..4),
                );
            } else {
                book.user("user", buy, tenths, rng.random_range(1..4));
            }
            rounds += 1;
        }
        let levels = book.book.levels();
        let side = |pair| levels.get(&pair).map_or(&[][..], Vec::as_slice);
        let market = OrderMarket {
            side: OrderSide::SellBase,
            price: mid(),
            market: rust_decimal::Decimal::from(2),
            received_at: std::time::Instant::now(),
            offered_decimals: 0,
            requested_decimals: 0,
            // No reference size: the walk adds nothing to the levels.
            rules: PairRules {
                band_bps: 30,
                quote_ttl: None,
                reference_amount: None,
            },
        };
        let api = expected_price(
            &market,
            0,
            side((base(), quote())),
            side((quote(), base())),
            Probe::Reference,
        )
        .unwrap();
        let matcher = book.walk(30).map_or(mid(), |walk| walk.price);
        assert_eq!(api, matcher, "round {round}");
        off_mid += usize::from(api != mid());
    }
    assert!(off_mid > 100, "only {off_mid} books priced off the mid");
}

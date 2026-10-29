use std::collections::{HashMap, HashSet};

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
    names: HashMap<NoteId, &'static str>,
}

impl Book {
    fn new() -> Self {
        Self {
            book: ClearingBook::default(),
            rng: RandomCoin::new(Word::default()),
            priority: 0,
            names: HashMap::new(),
        }
    }

    fn add(
        &mut self,
        name: &'static str,
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
        self.names.insert(order.id(), name);
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
            .bounded_price(base(), quote(), mid(), band_bps, fee_ppm, cap, true)
            .unwrap()
    }

    /// The walk with self-trade prevention switched off, as admission is
    /// when `engine.maker_self_trade_prevention = false`.
    fn walk_without_stp(&self, band_bps: u16) -> Option<BoundedPrice> {
        self.book
            .bounded_price(base(), quote(), mid(), band_bps, 0, usize::MAX, false)
            .unwrap()
    }

    fn named(&self, ids: &[NoteId]) -> Vec<&'static str> {
        let mut names: Vec<_> = ids.iter().map(|id| self.names[id]).collect();
        names.sort_unstable();
        names
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
    assert!(
        walk.sit_outs.is_empty(),
        "a two-sided quote sits nothing out"
    );
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
    let walk = book
        .book
        .bounded_price(base(), quote(), mid(), 30, 0, 100, true);
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

/// The example walked through with Vaibhav: maker M's old bid at +12 bps
/// clashes with its newer ask at +6.5, so the old bid sits out; its new bid
/// at -6.5 clashes with nothing and stays.
#[test]
fn rule_b_sits_out_the_stale_bid_and_keeps_the_quote() {
    let mut book = Book::new();
    book.user("A sells", SELL, -40, 2);
    let ask = book.maker("M ask #3", 1, 3, SELL, 65, 4);
    book.user("B buys", BUY, 400, 4);
    book.maker("M old bid #1", 1, 1, BUY, 120, 3);
    book.maker("M new bid #2", 1, 2, BUY, -65, 3);
    let walk = book.walk(30).unwrap();
    assert_eq!(book.named(&walk.sit_outs), ["M old bid #1"]);
    assert_eq!(walk.price, at(65));
    assert_eq!(walk.price_setter, Some(ask));
    // All of B's 4 lots at its +40 bps limit, in quote units.
    assert_eq!(walk.volume, U256::from(2 * 100_400 * 4u64));
    assert_eq!(walk.stop, WalkStop::NoLongerCrossing);
}

/// Without rule B a maker's own tiny crossed pair (ask +25, bid +26) would
/// set the price for every buyer at +25. With it, the price stays at the
/// real ask, +8.
#[test]
fn a_makers_own_crossed_pair_cannot_pump_the_price() {
    let mut book = Book::new();
    book.maker("Y ask +25 #1", 1, 1, SELL, 250, 1);
    book.maker("Y bid +26 #2", 1, 2, BUY, 260, 1);
    book.maker("Y ask +8 #3", 1, 3, SELL, 80, 3);
    book.user("users", BUY, 400, 3);
    let walk = book.walk(30).unwrap();
    assert_eq!(walk.price, at(80));
    assert_eq!(book.named(&walk.sit_outs), ["Y ask +25 #1", "Y bid +26 #2"]);
}

/// With self-trade prevention switched off, the walk benches nothing, as
/// admission then skips ADR 0005: the same book prices at the crossed pair.
#[test]
fn without_self_trade_prevention_nothing_sits_out() {
    let mut book = Book::new();
    book.maker("Y ask +25 #1", 1, 1, SELL, 250, 1);
    book.maker("Y bid +26 #2", 1, 2, BUY, 260, 1);
    book.maker("Y ask +8 #3", 1, 3, SELL, 80, 3);
    book.user("users", BUY, 400, 3);
    let walk = book.walk_without_stp(30).unwrap();
    assert!(walk.sit_outs.is_empty());
    assert_eq!(walk.price, at(250));
}

/// Pragma's B1 → S1 → B2 → B3, every bid above the ask: B2 and B3 trade,
/// B1 and S1 sit out (the same as ADR 0005 at one price).
#[test]
fn b1_s1_b2_b3() {
    let mut book = Book::new();
    book.maker("B1", 1, 1, BUY, 20, 1);
    book.maker("S1", 1, 2, SELL, 10, 1);
    book.maker("B2", 1, 3, BUY, 30, 1);
    book.maker("B3", 1, 4, BUY, 40, 1);
    book.user("seller", SELL, -50, 5);
    let walk = book.walk(30).unwrap();
    assert_eq!(book.named(&walk.sit_outs), ["B1", "S1"]);
    // The seller below the mid meets the kept bids above it: the mid.
    assert_eq!(walk.price, mid());
}

#[test]
fn users_and_other_makers_never_sit_out() {
    let mut book = Book::new();
    book.maker("X bid", 1, 1, BUY, 20, 1);
    book.maker("Y ask", 2, 1, SELL, 10, 1);
    book.user("user bid", BUY, 20, 1);
    book.user("user ask", SELL, 10, 1);
    let walk = book.walk(30).unwrap();
    assert!(walk.sit_outs.is_empty());
    assert_eq!(walk.volume, U256::from(2 * 2 * LOT));
}

/// A clash only on paper: the newer ask is above the band, so it can never
/// trade there, and the older bid above it stays.
#[test]
fn a_clash_outside_the_band_benches_nothing() {
    let mut book = Book::new();
    book.maker("bid", 1, 1, BUY, 450, 1);
    book.maker("ask above the band", 1, 2, SELL, 400, 1);
    book.user("seller", SELL, -50, 1);
    let walk = book.walk(30).unwrap();
    assert!(walk.sit_outs.is_empty());
    assert_eq!(walk.price, mid());
}

/// Orders in flight are off the index and do not count for rule B either.
#[test]
fn an_order_in_flight_is_ignored() {
    let mut book = Book::new();
    let stale = book.maker("old bid", 1, 1, BUY, 20, 1);
    book.maker("ask", 1, 2, SELL, 10, 1);
    book.user("buyer", BUY, 20, 1);
    book.book.deactivate(stale);
    let walk = book.walk(30).unwrap();
    assert!(walk.sit_outs.is_empty());
    assert_eq!(walk.price, at(10));
}

/// Sat-out orders take no slot: with room for two orders a side, five stale
/// bids do not crowd out the users.
#[test]
fn sit_outs_take_no_slot() {
    let mut book = Book::new();
    for seq in 1..=5 {
        book.maker("stale bid", 1, seq, BUY, 20, 1);
    }
    book.maker("new ask", 1, 6, SELL, 10, 1);
    book.user("seller", SELL, -10, 1);
    book.user("buyer", BUY, 5, 1);
    let walk = book.walk_with(30, 0, 2).unwrap();
    assert_eq!(walk.sit_outs.len(), 5);
    assert_eq!(walk.price, mid());
    assert_eq!(walk.volume, U256::from(2 * LOT));
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

/// A bid and an ask never share a sequence; if they did, the ask counts as
/// newer, as in ADR 0005.
#[test]
fn an_equal_sequence_counts_the_ask_as_newer() {
    let mut book = Book::new();
    book.maker("bid", 1, 1, BUY, 20, 1);
    book.maker("ask", 1, 1, SELL, 10, 1);
    book.user("buyer", BUY, 50, 1);
    let walk = book.walk(30).unwrap();
    assert_eq!(book.named(&walk.sit_outs), ["bid"]);
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
        .bounded_price(base(), quote(), eth_usdt, 30, 0, 100, true)
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
            .bounded_price(base(), quote(), mid(), 10_000, 0, 100, true),
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
        book.book
            .bounded_price(base(), quote(), huge, 30, 0, 100, true),
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
/// the band, its volume with rule B applied pair by pair, the most volume,
/// then the price closest to the mid.
#[test]
fn the_walk_matches_a_brute_force_oracle() {
    let mut rng = StdRng::seed_from_u64(7);
    let (mut off_mid, mut with_sit_outs) = (0, 0);
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
        // Rule B, pair by pair.
        let mut sat_out = HashSet::new();
        for bid in &orders {
            for ask in &orders {
                let (Some((maker_b, seq_b)), Some((maker_a, seq_a))) = (bid.4, ask.4) else {
                    continue;
                };
                if !(bid.1 && !ask.1 && maker_b == maker_a) {
                    continue;
                }
                let (bid_limit, ask_limit) = (at(bid.2), at(ask.2));
                let in_play = le(low, bid_limit) && le(ask_limit, high);
                if in_play && le(ask_limit, bid_limit) {
                    sat_out.insert(if seq_b > seq_a { ask.0 } else { bid.0 });
                }
            }
        }
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
                    .filter(|order| order.1 == buy && !sat_out.contains(&order.0))
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
            assert!(
                walk.sit_outs.iter().all(|id| sat_out.contains(id)),
                "round {round}"
            );
            off_mid += usize::from(walk.price != mid());
            with_sit_outs += usize::from(!walk.sit_outs.is_empty());
        }
    }
    // The books exercise what matters: prices off the mid, and rule B.
    assert!(
        off_mid > 200 && with_sit_outs > 100,
        "{off_mid} off the mid, {with_sit_outs} with sit-outs"
    );
}

/// The price API walks the matcher's book as levels, without self-trade
/// prevention. With prevention off here too, both find the same price on
/// random books: grouping equal-rate orders into a level changes nothing.
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
        let matcher = book.walk_without_stp(30).map_or(mid(), |walk| walk.price);
        assert_eq!(api, matcher, "round {round}");
        off_mid += usize::from(api != mid());
    }
    assert!(off_mid > 100, "only {off_mid} books priced off the mid");
}

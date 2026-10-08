//! Swap quotes and time estimates for the public `/v1/swap-eta` endpoint.
//!
//! Three pieces, all pure/self-contained and unit-tested here:
//!  * [`SettlementStats`] — an **in-memory, ephemeral** rolling window of recent
//!    settlement durations per directed pair (no DB storage). The executor owns
//!    one, records into it on each successful settlement, and publishes it over a
//!    `watch` channel; the price-API thread reads it to compute a 24h median,
//!    and whether settlements run right now (`settling`).
//!  * [`DepthBook`] — the price API's mirror of the matcher's resting levels,
//!    built from the [`DepthChange`]s the matcher sends as orders enter and
//!    leave its book, so no depth work runs on the matcher.
//!  * [`quote`] — what the matcher would do now with the order a wallet is
//!    about to sign: where its price sits against the clearing price, and how
//!    much of it the live book can fill.

use std::collections::{btree_map, BTreeMap, HashMap, VecDeque};

use ruint::aliases::U256;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::clearing::{
    checked_mul, eligible_units, mul_div_floor, ppm_floor, BatchPrice, ClearingError, OrderSide,
    PPM_DENOMINATOR,
};
use crate::matching::types::{Amount, BookLevel, RateKey, SwapBookSnapshot};
use crate::types::{TokenId, UnixSecs};

/// Retention window for settlement samples (24h).
pub const WINDOW_SECS: u64 = 24 * 60 * 60;
/// Hard per-pair cap on retained samples — a memory bound for a hot pair.
///
/// NOTE: this makes `median24h_seconds` **approximate** for any pair that sees
/// more than `MAX_SAMPLES_PER_PAIR` settlements inside the 24h window: once the
/// cap is hit, the oldest-but-still-fresh sample is dropped, so the reported
/// median is the median of the most recent `MAX_SAMPLES_PER_PAIR`, not of the
/// full window. Accepted trade-off: bounds memory for hot pairs.
const MAX_SAMPLES_PER_PAIR: usize = 1000;

#[derive(Clone, Copy, Debug)]
struct Sample {
    at_unix: UnixSecs,
    duration_secs: u64,
}

/// In-memory rolling window of recent settlement durations, keyed by directed
/// `(offered, requested)` pair. Ephemeral — empty after restart, rebuilds as
/// settlements happen. `Clone` so the executor can publish an `Arc<Self>` snapshot.
#[derive(Clone, Debug, Default)]
pub struct SettlementStats {
    by_pair: HashMap<(TokenId, TokenId), VecDeque<Sample>>,
    /// Whether the executor takes batches: `false` before it starts and while
    /// it is in verification mode (no fee headroom, node or database down).
    pub settling: bool,
}

impl SettlementStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a `pair` order settled in `duration_secs`, observed at
    /// `now_unix`. Prunes samples older than [`WINDOW_SECS`] and caps per-pair
    /// length at [`MAX_SAMPLES_PER_PAIR`].
    pub fn record(&mut self, pair: (TokenId, TokenId), now_unix: UnixSecs, duration_secs: u64) {
        let q = self.by_pair.entry(pair).or_default();
        q.push_back(Sample {
            at_unix: now_unix,
            duration_secs,
        });

        let cutoff = now_unix.saturating_sub(WINDOW_SECS);
        while q.front().map_or(false, |s| s.at_unix < cutoff) {
            q.pop_front();
        }
        while q.len() > MAX_SAMPLES_PER_PAIR {
            q.pop_front();
        }
    }

    /// Median settlement seconds for `pair` over the last window as of
    /// `now_unix`. `None` if there are no fresh samples for the pair.
    pub fn median_secs(&self, pair: (TokenId, TokenId), now_unix: UnixSecs) -> Option<u64> {
        let q = self.by_pair.get(&pair)?;
        let cutoff = now_unix.saturating_sub(WINDOW_SECS);
        let mut durs: Vec<u64> = q
            .iter()
            .filter(|s| s.at_unix >= cutoff)
            .map(|s| s.duration_secs)
            .collect();
        median_of(&mut durs)
    }
}

/// Median of `v` (sorts it in place). `None` if empty. Even-length → mean of the
/// two middle values (floored).
pub fn median_of(v: &mut [u64]) -> Option<u64> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        // u128 to avoid overflow on the sum of two large durations.
        ((v[n / 2 - 1] as u128 + v[n / 2] as u128) / 2) as u64
    })
}

/// One change to the matcher's active book, sent as it happens.
#[derive(Clone, Copy, Debug)]
pub enum DepthChange {
    /// An order entered the active book of directed `pair` at `rate`.
    Added {
        pair: (TokenId, TokenId),
        rate: RateKey,
        volume: Amount,
    },
    /// An order left it.
    Removed {
        pair: (TokenId, TokenId),
        rate: RateKey,
        volume: Amount,
    },
}

/// The matcher's resting levels, rebuilt from its [`DepthChange`]s on the
/// price API's thread.
#[derive(Debug, Default)]
pub struct DepthBook {
    levels: HashMap<(TokenId, TokenId), BTreeMap<RateKey, u128>>,
}

impl DepthBook {
    pub fn apply(&mut self, change: DepthChange) {
        match change {
            DepthChange::Added { pair, rate, volume } => {
                *self
                    .levels
                    .entry(pair)
                    .or_default()
                    .entry(rate)
                    .or_default() += u128::from(volume);
            }
            DepthChange::Removed { pair, rate, volume } => {
                let Some(levels) = self.levels.get_mut(&pair) else {
                    return;
                };
                if let btree_map::Entry::Occupied(mut level) = levels.entry(rate) {
                    *level.get_mut() = level.get().saturating_sub(u128::from(volume));
                    if *level.get() == 0 {
                        level.remove();
                    }
                }
                if levels.is_empty() {
                    self.levels.remove(&pair);
                }
            }
        }
    }

    pub fn snapshot(&self) -> SwapBookSnapshot {
        self.levels
            .iter()
            .map(|(&pair, levels)| {
                let levels = levels
                    .iter()
                    .map(|(&rate, &volume)| BookLevel {
                        rate,
                        volume: Amount::try_from(volume).unwrap_or(Amount::MAX),
                    })
                    .collect();
                (pair, levels)
            })
            .collect()
    }

    /// Apply every change already queued.
    #[cfg(test)]
    pub(crate) fn drain(
        &mut self,
        changes: &mut tokio::sync::mpsc::UnboundedReceiver<DepthChange>,
    ) {
        while let Ok(change) = changes.try_recv() {
            self.apply(change);
        }
    }
}

/// Basis points in one.
const BPS: u64 = 10_000;

/// Where an order's price sits against the price the matcher clears at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceBand {
    /// Clears at the current price.
    AtMarket,
    /// Clears once the price moves at most the tolerance the order's way.
    Tolerated,
    /// Further from the market than the tolerance.
    OffMarket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FillStatus {
    Full,
    Partial,
    None,
}

/// Why a quote says [`FillStatus::None`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoFillReason {
    /// The order is [`PriceBand::OffMarket`].
    Price,
    /// Priced well enough, but the book holds nothing for it.
    Liquidity,
    /// The pair has no clearing market.
    NoMarket,
    /// The pair's market has no fresh price right now.
    NoPrice,
}

/// The settings every quote is evaluated with.
#[derive(Clone, Copy, Debug)]
pub struct QuoteTerms {
    /// The clearing fee, exactly as the matcher charges it.
    pub fee_ppm: u32,
    /// How far (bps) the price may still have to move the order's way.
    pub tolerance_bps: u64,
}

/// The order the wallet is about to sign.
#[derive(Clone, Copy, Debug)]
pub struct QuoteOrder {
    pub offered: u64,
    pub requested: u64,
    /// The note's smallest partial fill, if the wallet sets one.
    pub min_fill_step: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub band: PriceBand,
    pub status: FillStatus,
    /// Set only when `status` is [`FillStatus::None`].
    pub reason: Option<NoFillReason>,
    /// How much of the order the book fills: all of it, part of it, or zero.
    pub fillable_offered: u64,
    pub fillable_requested: u64,
    /// How much of the offered token the book takes at this order's price,
    /// not capped by the order's size. `None` off market.
    pub available_offered: Option<u64>,
    /// What a full fill pays at the current price: the order's market value
    /// minus the fee, never less than `requested`.
    pub expected_requested: u64,
    /// The fee on a full fill at the current price.
    pub fee: u64,
}

/// What the matcher would do now with `order` on `side` of its clearing
/// pair, at `price` (that pair's exact clearing price), against the resting
/// levels on its own side and the opposite side, best first.
///
/// Mirrors one clearing tick: the order is eligible after the fee
/// ([`eligible_units`]); opposite levels eligible at the same price supply it;
/// same-side levels at a better or equal rate are served first (the new order
/// joins its rate last). An order outside the price but within the tolerance
/// is evaluated at the tolerance edge, the worst price it would clear at.
/// Ignored: external liquidity routing and the per-side order cap.
pub(crate) fn quote(
    side: OrderSide,
    price: BatchPrice,
    terms: QuoteTerms,
    order: QuoteOrder,
    same_side: &[BookLevel],
    opposite: &[BookLevel],
) -> Result<Quote, ClearingError> {
    let QuoteOrder {
        offered,
        requested,
        min_fill_step,
    } = order;
    let fee_ppm = terms.fee_ppm;

    // A full fill pays the market value minus the fee, at least the ask.
    let worth = value(side, price, offered)?;
    let fee = ppm_floor(worth, fee_ppm)?;
    let expected_requested = to_u64((worth - fee).max(U256::from(requested)))?;
    let fee = to_u64(fee)?;

    let eligible_at = |at| eligible_units(side, at, fee_ppm, offered, requested);
    let tolerance_edge = shifted(side, price, BPS + terms.tolerance_bps, BPS)?;
    let (band, clear_at) = if eligible_at(price)?.is_some() {
        (PriceBand::AtMarket, price)
    } else if eligible_at(tolerance_edge)?.is_some() {
        (PriceBand::Tolerated, tolerance_edge)
    } else {
        (PriceBand::OffMarket, price)
    };
    let quote = Quote {
        band,
        status: FillStatus::None,
        reason: Some(NoFillReason::Price),
        fillable_offered: 0,
        fillable_requested: 0,
        available_offered: None,
        expected_requested,
        fee,
    };
    if band == PriceBand::OffMarket {
        return Ok(quote);
    }

    // Both sides in requested units at `clear_at`.
    let mut supply = U256::ZERO;
    for level in opposite {
        let (offered, requested) = (level.rate.offered, level.rate.requested);
        if eligible_units(side.opposite(), clear_at, fee_ppm, offered, requested)?.is_none() {
            // Eligibility is monotone in the book's rate order.
            break;
        }
        supply += U256::from(level.volume);
    }
    let rate = RateKey::new(requested, offered);
    let mut ahead = U256::ZERO;
    for level in same_side.iter().take_while(|level| level.rate <= rate) {
        ahead += value(side, clear_at, level.volume)?;
    }
    let worth = value(side, clear_at, offered)?;
    let available = supply.saturating_sub(ahead);
    // The same amount in the offered token, at this order's own worth.
    let available_offered = u64::try_from(mul_div_floor(offered, available, worth)?).ok();
    let quote = Quote {
        available_offered: Some(available_offered.unwrap_or(u64::MAX)),
        ..quote
    };
    let fill = available.min(worth);
    if fill == worth {
        return Ok(Quote {
            status: FillStatus::Full,
            reason: None,
            fillable_offered: offered,
            fillable_requested: requested,
            ..quote
        });
    }
    // A partial fill pays the note's own ratio, as PSWAP computes it.
    let fillable_requested = to_u64(mul_div_floor(requested, fill, worth)?)?;
    let fillable_offered = to_u64(mul_div_floor(offered, fillable_requested, requested)?)?;
    let minimum = min_fill_step.unwrap_or(1).clamp(1, requested);
    if fillable_requested < minimum || fillable_offered == 0 {
        return Ok(Quote {
            reason: Some(NoFillReason::Liquidity),
            ..quote
        });
    }
    Ok(Quote {
        status: FillStatus::Partial,
        reason: None,
        fillable_offered,
        fillable_requested,
        ..quote
    })
}

/// Whole requested tokens per whole offered token that a full fill pays at the
/// `market` mid: the mid after the fee, as the order's side is charged it.
pub(crate) fn fill_price(side: OrderSide, market: Decimal, fee_ppm: u32) -> Option<Decimal> {
    let ppm = Decimal::from(PPM_DENOMINATOR);
    let fee = Decimal::from(fee_ppm);
    match side {
        OrderSide::SellBase => market.checked_mul((ppm - fee).checked_div(ppm)?),
        OrderSide::BuyBase => market.checked_mul(ppm.checked_div(ppm + fee)?),
    }
}

/// `price` with the order's rate (requested per offered) scaled by `num / den`:
/// above one moves it the order's way, below one against it.
fn shifted(
    side: OrderSide,
    price: BatchPrice,
    num: u64,
    den: u64,
) -> Result<BatchPrice, ClearingError> {
    let (quote_factor, base_factor) = match side {
        OrderSide::SellBase => (num, den),
        OrderSide::BuyBase => (den, num),
    };
    BatchPrice::new(
        checked_mul(price.quote_units, quote_factor)?,
        checked_mul(price.base_units, base_factor)?,
    )
}

/// What `offered` is worth at `price`, in requested units, rounded down.
fn value(side: OrderSide, price: BatchPrice, offered: u64) -> Result<U256, ClearingError> {
    match side {
        OrderSide::SellBase => mul_div_floor(offered, price.quote_units, price.base_units),
        OrderSide::BuyBase => mul_div_floor(offered, price.base_units, price.quote_units),
    }
}

fn to_u64(value: U256) -> Result<u64, ClearingError> {
    u64::try_from(value).map_err(|_| ClearingError::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    fn tok_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }
    fn tok_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }
    fn level(requested: u64, offered: u64, volume: u64) -> BookLevel {
        BookLevel {
            rate: RateKey::new(requested, offered),
            volume,
        }
    }

    // ── median ────────────────────────────────────────────────────────────
    #[test]
    fn median_odd_even_empty() {
        assert_eq!(median_of(&mut []), None);
        assert_eq!(median_of(&mut [7]), Some(7));
        assert_eq!(median_of(&mut [3, 1, 2]), Some(2));
        assert_eq!(median_of(&mut [4, 1, 3, 2]), Some(2)); // (2+3)/2 = 2 (floored)
        assert_eq!(median_of(&mut [10, 20]), Some(15));
    }

    // ── SettlementStats ───────────────────────────────────────────────────
    #[test]
    fn stats_records_and_medians_per_pair() {
        let mut s = SettlementStats::new();
        let now = 1_000_000u64;
        for d in [10u64, 30, 20] {
            s.record((tok_a(), tok_b()), now, d);
        }
        // A different direction is tracked separately.
        s.record((tok_b(), tok_a()), now, 99);
        assert_eq!(s.median_secs((tok_a(), tok_b()), now), Some(20));
        assert_eq!(s.median_secs((tok_b(), tok_a()), now), Some(99));
        assert_eq!(s.median_secs((tok_a(), tok_a()), now), None); // never recorded
    }

    #[test]
    fn stats_prunes_stale_samples() {
        let mut s = SettlementStats::new();
        let pair = (tok_a(), tok_b());
        // An old sample and a fresh one; querying at `now` drops the old.
        s.record(pair, 100, 5); // very old
        let now = 100 + WINDOW_SECS + 10;
        s.record(pair, now, 50); // fresh — this record() call prunes the old one
        assert_eq!(s.median_secs(pair, now), Some(50));
    }

    // ── DepthBook ─────────────────────────────────────────────────────────
    #[test]
    fn depth_book_merges_equal_rates_and_drops_empty_levels() {
        let pair = (tok_a(), tok_b());
        let added = |requested, offered, volume| DepthChange::Added {
            pair,
            rate: RateKey::new(requested, offered),
            volume,
        };
        let removed = |requested, offered, volume| DepthChange::Removed {
            pair,
            rate: RateKey::new(requested, offered),
            volume,
        };
        let mut depth = DepthBook::default();
        depth.apply(added(4, 2, 100)); // 2 B per A
        depth.apply(added(2, 1, 50)); // the same rate
        depth.apply(added(1, 1, 7)); // better
        let levels: Vec<_> = depth.snapshot()[&pair]
            .iter()
            .map(|level| (level.rate.requested, level.rate.offered, level.volume))
            .collect();
        assert_eq!(levels, [(1, 1, 7), (4, 2, 150)]);

        depth.apply(removed(4, 2, 100));
        assert_eq!(depth.snapshot()[&pair][1].volume, 50);
        depth.apply(removed(2, 1, 50));
        depth.apply(removed(1, 1, 7));
        assert!(depth.snapshot().is_empty());
        // A change for a level the mirror never saw changes nothing.
        depth.apply(removed(3, 1, 5));
        assert!(depth.snapshot().is_empty());
    }

    // ── quote ─────────────────────────────────────────────────────────────
    // A/B pair: base A, quote B, 2 B units per A unit. Fee 0.1%, tolerance
    // 0.5%: a seller's fill price is 1.998 B per A, the edge of the
    // tolerance 2.00799.
    const TERMS: QuoteTerms = QuoteTerms {
        fee_ppm: 1_000,
        tolerance_bps: 50,
    };

    fn two() -> BatchPrice {
        BatchPrice::from_ratio(2, 1).unwrap()
    }

    fn order(offered: u64, requested: u64) -> QuoteOrder {
        QuoteOrder {
            offered,
            requested,
            min_fill_step: None,
        }
    }

    /// Sell 1_000_000 A units for `requested` B against `same` and `opposite`.
    fn sell(requested: u64, same: &[BookLevel], opposite: &[BookLevel]) -> Quote {
        let order = order(1_000_000, requested);
        quote(OrderSide::SellBase, two(), TERMS, order, same, opposite).unwrap()
    }

    /// A buyer of A paying 2.14 B per A: eligible at 2 and at 2.01.
    fn deep_buyers(volume: u64) -> Vec<BookLevel> {
        vec![level(1_400_000, 3_000_000, volume)]
    }

    #[test]
    fn at_market_order_with_depth_fills_fully_at_market_minus_fee() {
        let q = sell(1_990_000, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::AtMarket);
        assert_eq!(q.status, FillStatus::Full);
        assert_eq!(q.reason, None);
        assert_eq!(
            (q.fillable_offered, q.fillable_requested),
            (1_000_000, 1_990_000)
        );
        // The book takes 3_000_000 B: 1_500_000 A, more than this order.
        assert_eq!(q.available_offered, Some(1_500_000));
        // Worth 2_000_000 B; the 0.1% fee leaves 1_998_000, above the ask.
        assert_eq!(q.expected_requested, 1_998_000);
        assert_eq!(q.fee, 2_000);
    }

    #[test]
    fn thin_book_fills_part_at_the_notes_own_ratio() {
        // 1_000_000 B against an order worth 2_000_000 B: half of it.
        let q = sell(1_990_000, &[], &deep_buyers(1_000_000));
        assert_eq!(q.status, FillStatus::Partial);
        assert_eq!(
            (q.fillable_offered, q.fillable_requested),
            (500_000, 995_000)
        );
        assert_eq!(q.available_offered, Some(500_000));
    }

    #[test]
    fn same_side_orders_ahead_are_served_first() {
        let better = level(1_900_000, 1_000_000, 1_000_000); // worth 2_000_000 B
        let equal = level(1_990_000, 1_000_000, 250_000); // earlier at our rate
        let worse = level(1_995_000, 1_000_000, 9_000_000); // behind us
        let q = sell(1_990_000, &[better, equal, worse], &deep_buyers(3_000_000));
        // 3_000_000 − 2_000_000 − 500_000 leaves 500_000 B of 2_000_000.
        assert_eq!(q.status, FillStatus::Partial);
        assert_eq!(
            (q.fillable_offered, q.fillable_requested),
            (250_000, 497_500)
        );
        assert_eq!(q.available_offered, Some(250_000));
    }

    #[test]
    fn nothing_left_or_nothing_eligible_is_none_for_liquidity() {
        for opposite in [vec![], vec![level(1_000_000, 1_000_000, 50_000_000)]] {
            let q = sell(1_990_000, &[], &opposite);
            assert_eq!(q.band, PriceBand::AtMarket);
            assert_eq!(q.status, FillStatus::None);
            assert_eq!(q.reason, Some(NoFillReason::Liquidity));
            assert_eq!((q.fillable_offered, q.fillable_requested), (0, 0));
            assert_eq!(q.available_offered, Some(0));
        }
    }

    #[test]
    fn partial_below_the_min_fill_step_is_none() {
        let order = QuoteOrder {
            min_fill_step: Some(1_000_000),
            ..order(1_000_000, 1_990_000)
        };
        let q = quote(
            OrderSide::SellBase,
            two(),
            TERMS,
            order,
            &[],
            &deep_buyers(1_000_000),
        )
        .unwrap();
        assert_eq!(q.status, FillStatus::None);
        assert_eq!(q.reason, Some(NoFillReason::Liquidity));
    }

    #[test]
    fn ask_within_the_tolerance_is_tolerated_and_judged_at_the_edge() {
        // 2.005 B per A: above the 1.998 fill price, below the 2.00799 edge.
        let q = sell(2_005_000, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::Tolerated);
        assert_eq!(q.status, FillStatus::Full);
        // Today it is worth less than it asks: a fill pays the ask.
        assert_eq!(q.expected_requested, 2_005_000);
        // At the edge (2.01) a buyer must pay at least 2.01201 B per A.
        let thin_buyer = level(1_000_000, 2_011_000, 50_000_000);
        let q = sell(2_005_000, &[], &[thin_buyer]);
        assert_eq!(q.reason, Some(NoFillReason::Liquidity));
        // The same buyer fills an at-market order.
        let q = sell(1_990_000, &[], &[thin_buyer]);
        assert_eq!(q.status, FillStatus::Full);
    }

    #[test]
    fn ask_past_the_tolerance_is_off_market() {
        assert_eq!(sell(2_007_990, &[], &[]).band, PriceBand::Tolerated);
        let q = sell(2_007_991, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::OffMarket);
        assert_eq!(q.status, FillStatus::None);
        assert_eq!(q.reason, Some(NoFillReason::Price));
        assert_eq!(q.available_offered, None);
    }

    #[test]
    fn buyer_side_uses_the_buyers_fee_rule() {
        // Offer 2_000_000 B for A at 2 B per A: at most 2e6 / 2.002 = 999_000 A.
        let sellers = [level(1_900_000, 1_000_000, 1_000_000)];
        let at = |requested| {
            let order = order(2_000_000, requested);
            quote(OrderSide::BuyBase, two(), TERMS, order, &[], &sellers).unwrap()
        };
        let q = at(999_000);
        assert_eq!(q.band, PriceBand::AtMarket);
        assert_eq!(q.status, FillStatus::Full);
        assert_eq!(q.expected_requested, 999_000);
        assert_eq!(at(999_001).band, PriceBand::Tolerated);
    }

    #[test]
    fn docs_example_sells_one_eth_for_usdt() {
        // docs/price-api.md: ETH 18 decimals, USDT 6, mid 2500, fee 0.1%; the
        // wallet asks the fill price less 0.5% slippage: 2485.0125 USDT.
        let price = BatchPrice::from_whole_price(Decimal::from(2500), 18, 6).unwrap();
        let eth = 1_000_000_000_000_000_000;
        let q = quote(
            OrderSide::SellBase,
            price,
            TERMS,
            order(eth, 2_485_012_500),
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(q.band, PriceBand::AtMarket);
        assert_eq!(q.expected_requested, 2_497_500_000);
        assert_eq!(q.fee, 2_500_000);
    }

    #[test]
    fn fill_price_follows_the_side_and_fee() {
        let fill = fill_price(OrderSide::SellBase, Decimal::from(2500), 1_000).unwrap();
        assert_eq!(fill, Decimal::new(24975, 1));
        let fill = fill_price(OrderSide::BuyBase, Decimal::TWO, 1_000).unwrap();
        assert_eq!(fill.round_dp(9), Decimal::new(1998001998, 9));
    }
}

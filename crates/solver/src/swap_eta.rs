//! Swap quotes and time estimates for the public `/v1/swap-eta` endpoint.
//!
//! Two pieces, both pure/self-contained and unit-tested here:
//!  * [`SettlementStats`] — an **in-memory, ephemeral** rolling window of recent
//!    settlement durations per directed pair (no DB storage). The executor owns
//!    one, records into it on each successful settlement, and publishes it over a
//!    `watch` channel; the price-API thread reads it to compute a 24h median,
//!    and whether settlements run right now (`settling`).
//!  * [`DepthBook`] — the price API's mirror of the matcher's resting levels,
//!    built from the [`DepthChange`]s the matcher sends as orders enter and
//!    leave its book, so no depth work runs on the matcher.
//!  * [`quote`] — what the matcher would do with a prospective order now: where
//!    its price sits against the clearing price, how much of it the live book
//!    can fill, and the price we suggest instead.

use std::collections::{btree_map, BTreeMap, HashMap, VecDeque};

use ruint::aliases::U256;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::clearing::{
    checked_mul, eligible_units, mul_div_ceil, mul_div_floor, ppm_floor, BatchPrice, ClearingError,
    OrderSide, PPM_DENOMINATOR,
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
    /// How far (bps) the suggested price sits below the fill price.
    pub buffer_bps: u64,
}

/// A prospective order. A missing amount is filled in at the suggested price.
#[derive(Clone, Copy, Debug, Default)]
pub struct QuoteOrder {
    pub offered: Option<u64>,
    pub requested: Option<u64>,
    /// The note's smallest partial fill, if the wallet sets one.
    pub min_fill_step: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub offered: u64,
    pub requested: u64,
    pub band: PriceBand,
    pub status: FillStatus,
    /// Set only when `status` is [`FillStatus::None`].
    pub reason: Option<NoFillReason>,
    /// How much of the order the book fills: all of it, part of it, or zero.
    pub fillable_offered: u64,
    pub fillable_requested: u64,
    /// What a full fill pays at the current price: the order's market value
    /// minus the fee, never less than `requested`.
    pub expected_requested: u64,
    /// The fee on a full fill at the current price.
    pub fee: u64,
    /// The most `offered` can request and still clear at a price `buffer_bps`
    /// worse than now.
    pub suggested_requested: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum QuoteError {
    #[error("give `offered_amount`, `requested_amount` or both")]
    NoAmount,
    #[error("the amount is too small to price")]
    TooSmall,
    #[error("the amounts cannot be priced: {0}")]
    Clearing(#[from] ClearingError),
}

/// What the matcher would do now with an order on `side` of its clearing
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
) -> Result<Quote, QuoteError> {
    let fee_ppm = terms.fee_ppm;
    let suggested_price = shifted(side, price, BPS.saturating_sub(terms.buffer_bps), BPS)?;
    let (offered, requested) = match (order.offered, order.requested) {
        (Some(offered), Some(requested)) => (offered, requested),
        (Some(offered), None) => (
            offered,
            to_u64(max_requested(side, suggested_price, fee_ppm, offered)?)?,
        ),
        (None, Some(requested)) => (
            to_u64(min_offered(side, suggested_price, fee_ppm, requested)?)?,
            requested,
        ),
        (None, None) => return Err(QuoteError::NoAmount),
    };
    if offered == 0 || requested == 0 {
        return Err(QuoteError::TooSmall);
    }
    let suggested_requested = to_u64(max_requested(side, suggested_price, fee_ppm, offered)?)?;

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
        offered,
        requested,
        band,
        status: FillStatus::None,
        reason: Some(NoFillReason::Price),
        fillable_offered: 0,
        fillable_requested: 0,
        expected_requested,
        fee,
        suggested_requested,
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
    let fill = supply.saturating_sub(ahead).min(worth);
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
    let minimum = order.min_fill_step.unwrap_or(1).clamp(1, requested);
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

/// The fill price `buffer_bps` worse for the order: the price we suggest.
pub(crate) fn suggested_price(fill_price: Decimal, buffer_bps: u64) -> Option<Decimal> {
    let bps = Decimal::from(BPS);
    fill_price.checked_mul((bps - Decimal::from(buffer_bps)).checked_div(bps)?)
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

/// The most an order offering `offered` can request and still clear at
/// `price`: [`eligible_units`] solved for the requested amount.
fn max_requested(
    side: OrderSide,
    price: BatchPrice,
    fee_ppm: u32,
    offered: u64,
) -> Result<U256, ClearingError> {
    match side {
        OrderSide::SellBase => mul_div_floor(
            checked_mul(offered, price.quote_units)?,
            PPM_DENOMINATOR - fee_ppm,
            checked_mul(price.base_units, PPM_DENOMINATOR)?,
        ),
        OrderSide::BuyBase => mul_div_floor(
            checked_mul(offered, price.base_units)?,
            PPM_DENOMINATOR,
            checked_mul(price.quote_units, PPM_DENOMINATOR + fee_ppm)?,
        ),
    }
}

/// The least an order requesting `requested` must offer to clear at `price`:
/// [`eligible_units`] solved for the offered amount.
fn min_offered(
    side: OrderSide,
    price: BatchPrice,
    fee_ppm: u32,
    requested: u64,
) -> Result<U256, ClearingError> {
    match side {
        OrderSide::SellBase => mul_div_ceil(
            checked_mul(requested, price.base_units)?,
            PPM_DENOMINATOR,
            checked_mul(price.quote_units, PPM_DENOMINATOR - fee_ppm)?,
        ),
        OrderSide::BuyBase => mul_div_ceil(
            checked_mul(requested, price.quote_units)?,
            PPM_DENOMINATOR + fee_ppm,
            checked_mul(price.base_units, PPM_DENOMINATOR)?,
        ),
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
    // 0.5%, buffer 0.2%: a seller's fill price is 1.998 B per A, the edge of
    // the tolerance 2.00799, and the suggested price 1.994004.
    const TERMS: QuoteTerms = QuoteTerms {
        fee_ppm: 1_000,
        tolerance_bps: 50,
        buffer_bps: 20,
    };

    fn two() -> BatchPrice {
        BatchPrice::from_ratio(2, 1).unwrap()
    }

    fn order(offered: Option<u64>, requested: Option<u64>) -> QuoteOrder {
        QuoteOrder {
            offered,
            requested,
            min_fill_step: None,
        }
    }

    /// Sell 1 A unit-million for `requested` B against `same` and `opposite`.
    fn sell(requested: u64, same: &[BookLevel], opposite: &[BookLevel]) -> Quote {
        let order = order(Some(1_000_000), Some(requested));
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
        // Worth 2_000_000 B; the 0.1% fee leaves 1_998_000, above the ask.
        assert_eq!(q.expected_requested, 1_998_000);
        assert_eq!(q.fee, 2_000);
        assert_eq!(q.suggested_requested, 1_994_004);
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
    }

    #[test]
    fn nothing_left_or_nothing_eligible_is_none_for_liquidity() {
        for opposite in [vec![], vec![level(1_000_000, 1_000_000, 50_000_000)]] {
            let q = sell(1_990_000, &[], &opposite);
            assert_eq!(q.band, PriceBand::AtMarket);
            assert_eq!(q.status, FillStatus::None);
            assert_eq!(q.reason, Some(NoFillReason::Liquidity));
            assert_eq!((q.fillable_offered, q.fillable_requested), (0, 0));
        }
    }

    #[test]
    fn partial_below_the_min_fill_step_is_none() {
        let order = QuoteOrder {
            min_fill_step: Some(1_000_000),
            ..order(Some(1_000_000), Some(1_990_000))
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
    fn ask_past_the_tolerance_is_off_market_with_a_suggestion() {
        assert_eq!(sell(2_007_990, &[], &[]).band, PriceBand::Tolerated);
        let q = sell(2_007_991, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::OffMarket);
        assert_eq!(q.status, FillStatus::None);
        assert_eq!(q.reason, Some(NoFillReason::Price));
        assert_eq!(q.suggested_requested, 1_994_004);
    }

    #[test]
    fn a_missing_amount_is_filled_in_at_the_suggested_price() {
        let book = deep_buyers(3_000_000);
        let exact_in = order(Some(1_000_000), None);
        let q = quote(OrderSide::SellBase, two(), TERMS, exact_in, &[], &book).unwrap();
        assert_eq!((q.offered, q.requested), (1_000_000, 1_994_004));
        assert_eq!(q.band, PriceBand::AtMarket);
        let exact_out = order(None, Some(1_994_004));
        let q = quote(OrderSide::SellBase, two(), TERMS, exact_out, &[], &book).unwrap();
        assert_eq!((q.offered, q.requested), (1_000_000, 1_994_004));
        let neither = order(None, None);
        assert!(matches!(
            quote(OrderSide::SellBase, two(), TERMS, neither, &[], &book),
            Err(QuoteError::NoAmount)
        ));
        let dust = order(Some(1), None); // 1 B unit is worth half an A unit
        assert!(matches!(
            quote(OrderSide::BuyBase, two(), TERMS, dust, &[], &book),
            Err(QuoteError::TooSmall)
        ));
    }

    #[test]
    fn buyer_side_uses_the_buyers_fee_rule() {
        // Offer 2_000_000 B for A at 2 B per A: at most 2e6 / 2.002 = 999_000 A.
        let sellers = [level(1_900_000, 1_000_000, 1_000_000)];
        let at = |requested| {
            let order = order(Some(2_000_000), Some(requested));
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
        // docs/price-api.md: ETH 18 decimals, USDT 6, mid 2500, fee 0.1%.
        let price = BatchPrice::from_whole_price(Decimal::from(2500), 18, 6).unwrap();
        let eth = 1_000_000_000_000_000_000;
        let order = order(Some(eth), None);
        let q = quote(OrderSide::SellBase, price, TERMS, order, &[], &[]).unwrap();
        assert_eq!(q.requested, 2_492_505_000);
        assert_eq!(q.suggested_requested, 2_492_505_000);
        assert_eq!(q.expected_requested, 2_497_500_000);
        assert_eq!(q.fee, 2_500_000);
        assert_eq!(q.band, PriceBand::AtMarket);
    }

    #[test]
    fn display_prices_follow_the_fee_and_buffer() {
        let market = Decimal::from(2500);
        let fill = fill_price(OrderSide::SellBase, market, 1_000).unwrap();
        assert_eq!(fill, Decimal::new(24975, 1));
        assert_eq!(suggested_price(fill, 20).unwrap(), Decimal::new(2492505, 3));
        let fill = fill_price(OrderSide::BuyBase, Decimal::TWO, 1_000).unwrap();
        assert_eq!(fill.round_dp(9), Decimal::new(1998001998, 9));
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn side() -> impl Strategy<Value = OrderSide> {
            prop_oneof![Just(OrderSide::SellBase), Just(OrderSide::BuyBase)]
        }

        proptest! {
            /// The closed forms are exactly the boundary of the matcher's rule.
            #[test]
            fn closed_forms_match_eligibility(
                side in side(),
                quote_units in 1u64..1_000_000,
                base_units in 1u64..1_000_000,
                fee_ppm in 0u32..100_000,
                amount in 1u64..1_000_000_000,
            ) {
                let price = BatchPrice::from_ratio(quote_units, base_units).unwrap();
                let eligible = |offered: u64, requested: u64| {
                    eligible_units(side, price, fee_ppm, offered, requested).unwrap().is_some()
                };
                let max = to_u64(max_requested(side, price, fee_ppm, amount).unwrap()).unwrap();
                prop_assert!(max == 0 || eligible(amount, max));
                prop_assert!(!eligible(amount, max + 1));
                let min = to_u64(min_offered(side, price, fee_ppm, amount).unwrap()).unwrap();
                prop_assert!(eligible(min, amount));
                prop_assert!(min == 1 || !eligible(min - 1, amount));
            }
        }
    }
}

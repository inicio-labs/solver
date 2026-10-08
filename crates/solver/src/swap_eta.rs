//! Swap pricing and quotes for the public `/v2/pair-price` and `/v2/swap-eta`
//! endpoints, and the frozen `/v1/swap-eta` checks. All pure/self-contained
//! and unit-tested here:
//!  * [`SettlementStats`] — an **in-memory, ephemeral** rolling window of recent
//!    settlement durations per directed pair (no DB storage). The executor owns
//!    one, records into it on each successful settlement, and publishes it over a
//!    `watch` channel; the price-API thread reads it to compute a 24h median,
//!    and whether settlements run right now (`settling`).
//!  * [`DepthBook`] — the price API's mirror of the matcher's resting levels,
//!    built from the [`DepthChange`]s the matcher sends as orders enter and
//!    leave its book, so no depth work runs on the matcher.
//!  * [`fill_price`] — the best price an ask can name and still fill now.
//!  * [`quote`] — what the matcher would do now with the order a wallet is
//!    about to sign: where its price sits against the clearing price, and how
//!    much of it the live book can fill.

use std::collections::{btree_map, BTreeMap, HashMap, VecDeque};

use ruint::aliases::U256;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::clearing::{
    checked_mul, eligible_units, mul_div_ceil, mul_div_floor, ppm_floor, BatchPrice, ClearingError,
    OrderSide, PPM_DENOMINATOR,
};
use crate::matching::types::{Amount, BookLevel, RateKey, SwapBookSnapshot};
use crate::price::PricePrecision;
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
pub(crate) struct DepthBook {
    levels: HashMap<(TokenId, TokenId), BTreeMap<RateKey, u128>>,
}

impl DepthBook {
    pub(crate) fn apply(&mut self, change: DepthChange) {
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
                // The mirror sees every change from the matcher's first one,
                // so removing more than it holds means the two have drifted.
                let level =
                    self.levels
                        .get_mut(&pair)
                        .and_then(|levels| match levels.entry(rate) {
                            btree_map::Entry::Occupied(level) => Some(level),
                            btree_map::Entry::Vacant(_) => None,
                        });
                let Some(mut level) = level else {
                    tracing::warn!(?pair, "depth mirror: removal for an unknown level");
                    return;
                };
                let held = *level.get();
                if held < u128::from(volume) {
                    tracing::warn!(?pair, held, volume, "depth mirror: level underflow");
                }
                *level.get_mut() = held.saturating_sub(u128::from(volume));
                if *level.get() == 0 {
                    level.remove();
                }
                if self.levels.get(&pair).is_some_and(BTreeMap::is_empty) {
                    self.levels.remove(&pair);
                }
            }
        }
    }

    /// Apply every change already queued.
    pub(crate) fn drain(&mut self, changes: &mut mpsc::UnboundedReceiver<DepthChange>) {
        while let Ok(change) = changes.try_recv() {
            self.apply(change);
        }
    }

    pub(crate) fn snapshot(&self) -> SwapBookSnapshot {
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
}

/// Where an order's price sits against the price the matcher clears at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PriceBand {
    /// Clears at the current price.
    AtMarket,
    /// Clears once the price moves at most the tolerance the order's way.
    Tolerated,
    /// Further from the market than the tolerance.
    OffMarket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FillStatus {
    Full,
    Partial,
    None,
}

/// Why a quote says [`FillStatus::None`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NoFillReason {
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
pub(crate) struct QuoteTerms {
    /// The clearing fee, exactly as the matcher charges it.
    pub fee_ppm: u32,
    /// How far (bps) the price may still have to move the order's way.
    pub tolerance_bps: u64,
}

/// The order the wallet is about to sign.
#[derive(Clone, Copy, Debug)]
pub(crate) struct QuoteOrder {
    pub offered: u64,
    pub requested: u64,
    /// The note's smallest partial fill, in requested units, if it has one.
    pub min_fill_step: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Quote {
    pub band: PriceBand,
    pub status: FillStatus,
    /// How much of the order the book fills: all of it, part of it, or zero.
    pub fillable_offered: u64,
    pub fillable_requested: u64,
    /// How much of the offered token the book takes at this order's price,
    /// not capped by the order's size. `None` off market.
    pub available_offered: Option<u64>,
    /// What a full fill pays at today's price, as the matcher settles it.
    pub expected_requested: u64,
    /// The fee on that full fill.
    pub fee: u64,
}

impl Quote {
    /// Why nothing fills, when nothing does.
    pub(crate) fn reason(&self) -> Option<NoFillReason> {
        match (self.status, self.band) {
            (FillStatus::None, PriceBand::OffMarket) => Some(NoFillReason::Price),
            (FillStatus::None, _) => Some(NoFillReason::Liquidity),
            _ => None,
        }
    }
}

/// What the matcher would do now with `order` on `side` of its clearing
/// pair, at `price` (that pair's exact clearing price), against the resting
/// levels on its own side and the opposite side, best first.
///
/// Mirrors one clearing tick: the order is eligible after the fee
/// ([`eligible_units`]); opposite levels eligible at the same price supply it;
/// same-side levels at a better or equal rate are served first (the new order
/// joins its rate last). A tolerated order is judged at its break-even price,
/// the first price it would clear at. Not modelled: external liquidity
/// routing, the per-side order cap, and the opposite orders' own minimum
/// fills (an all-or-nothing order counts in full).
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

    // A full fill today, as `MatchOrder::execution` settles it: the market
    // value less the fee, and the fee never exceeds the surplus over the ask.
    let market_worth = value(side, price, offered)?;
    let fee =
        ppm_floor(market_worth, fee_ppm)?.min(market_worth.saturating_sub(U256::from(requested)));
    let expected_requested = to_u64((market_worth - fee).max(U256::from(requested)))?;
    let fee = to_u64(fee)?;

    let eligible_at = |at| eligible_units(side, at, fee_ppm, offered, requested);
    let band = if eligible_at(price)?.is_some() {
        PriceBand::AtMarket
    } else if eligible_at(tolerance_edge(side, price, terms.tolerance_bps)?)?.is_some() {
        PriceBand::Tolerated
    } else {
        PriceBand::OffMarket
    };
    let unfilled = Quote {
        band,
        status: FillStatus::None,
        fillable_offered: 0,
        fillable_requested: 0,
        available_offered: None,
        expected_requested,
        fee,
    };
    if band == PriceBand::OffMarket {
        return Ok(unfilled);
    }
    let clear_at = match band {
        PriceBand::AtMarket => price,
        _ => break_even(side, fee_ppm, offered, requested)?,
    };
    let (comparison_units, payment_units) = eligible_at(clear_at)?.ok_or(
        ClearingError::InternalInvariant("quoted order not eligible at its clearing price"),
    )?;

    // Both sides in requested units at `clear_at`.
    let mut supply = U256::ZERO;
    for level in opposite {
        let opposite_side = side.opposite();
        let (level_offered, level_requested) = (level.rate.offered, level.rate.requested);
        if eligible_units(
            opposite_side,
            clear_at,
            fee_ppm,
            level_offered,
            level_requested,
        )?
        .is_none()
        {
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
    let available_offered =
        u64::try_from(mul_div_floor(offered, available, worth)?).unwrap_or(u64::MAX);

    let (status, fillable_offered, fillable_requested) = if available >= worth {
        (FillStatus::Full, offered, requested)
    } else {
        // A partial fill pays the note's own ratio, rounded up as the matcher
        // does, and must reach the note's minimum.
        let paid = to_u64(mul_div_ceil(available, payment_units, comparison_units)?)?;
        let released = to_u64(mul_div_floor(offered, paid, requested)?)?;
        let minimum = min_fill_step.unwrap_or(1).min(requested).max(1);
        if paid < minimum || released == 0 {
            (FillStatus::None, 0, 0)
        } else {
            (FillStatus::Partial, released, paid)
        }
    };
    Ok(Quote {
        status,
        fillable_offered,
        fillable_requested,
        available_offered: Some(available_offered),
        ..unfilled
    })
}

/// The best price an ask can name and still fill now, in whole requested
/// tokens per whole offered token: the `market` mid after the fee as the
/// order's side is charged it, rounded toward zero at 18 places so an ask
/// built from it never overshoots.
pub(crate) fn fill_price(side: OrderSide, market: Decimal, fee_ppm: u32) -> Option<Decimal> {
    let ppm = Decimal::from(PPM_DENOMINATOR);
    let fee = Decimal::from(fee_ppm);
    let fill = match side {
        OrderSide::SellBase => market.checked_mul((ppm - fee).checked_div(ppm)?),
        OrderSide::BuyBase => market.checked_mul(ppm.checked_div(ppm + fee)?),
    }?;
    Some(
        fill.round_dp_with_strategy(18, RoundingStrategy::ToZero)
            .normalize(),
    )
}

/// `price` moved `tolerance_bps` the order's way: the order's own rate
/// (requested per offered) grows by that much.
fn tolerance_edge(
    side: OrderSide,
    price: BatchPrice,
    tolerance_bps: u64,
) -> Result<BatchPrice, ClearingError> {
    const BPS: u64 = 10_000;
    let (quote_factor, base_factor) = match side {
        OrderSide::SellBase => (BPS + tolerance_bps, BPS),
        OrderSide::BuyBase => (BPS, BPS + tolerance_bps),
    };
    BatchPrice::new(
        checked_mul(price.quote_units, quote_factor)?,
        checked_mul(price.base_units, base_factor)?,
    )
}

/// The price at which an order just becomes eligible after the fee:
/// [`eligible_units`]'s boundary solved for the price.
fn break_even(
    side: OrderSide,
    fee_ppm: u32,
    offered: u64,
    requested: u64,
) -> Result<BatchPrice, ClearingError> {
    match side {
        // requested × 10^6 = offered × price × (10^6 − fee)
        OrderSide::SellBase => BatchPrice::new(
            checked_mul(requested, PPM_DENOMINATOR)?,
            checked_mul(offered, PPM_DENOMINATOR - fee_ppm)?,
        ),
        // offered × 10^6 = requested × price × (10^6 + fee)
        OrderSide::BuyBase => BatchPrice::new(
            checked_mul(offered, PPM_DENOMINATOR)?,
            checked_mul(requested, PPM_DENOMINATOR + fee_ppm)?,
        ),
    }
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

// ── v1 (frozen) ──────────────────────────────────────────────────────────────
// `/v1/swap-eta` keeps its original answers for wallets built against it;
// new wallets use `/v2` and [`quote`].

/// Can an order offering `offered_a` of token A and requesting `requested_b` of
/// token B fill against `best` — the top level of the **opposite** pair (B→A)?
///
/// Crossing is strict (`>`), mirroring
/// [`crate::matching::types::Order::is_profitable_with`]; additionally the top
/// level must hold enough volume (`best.volume >= requested_b`).
pub(crate) fn eval_can_fill(offered_a: u64, requested_b: u64, best: Option<BookLevel>) -> bool {
    let Some(best) = best else {
        return false;
    };
    // Cross iff  offered_a * best.offered  >  requested_b * best.requested.
    let cross = (offered_a as u128) * (best.rate.offered as u128)
        > (requested_b as u128) * (best.rate.requested as u128);
    cross && best.volume >= requested_b
}

/// Is the order priced worse than the market (off-market)?
///
/// `market` is the pair's Binance midpoint in whole B per whole A. Returns
/// `(off_market, market_price)`. `off_market = Some(true)` when the order asks
/// for more B than its A is worth at `market`, by more than `tol_bps`.
/// `off_market` is `None` without a market price or either token's decimals;
/// `market_price` (B per A, exact up to 18 decimal places, like the price
/// API's `full` precision) is present whenever `market` is.
pub(crate) fn eval_off_market(
    offered_a: u64,
    d_a: Option<u8>,
    requested_b: u64,
    d_b: Option<u8>,
    market: Option<Decimal>,
    tol_bps: u64,
) -> (Option<bool>, Option<String>) {
    let Some(market) = market else {
        return (None, None);
    };
    let market_price = Some(PricePrecision::Full.format(market));
    let (Some(d_a), Some(d_b)) = (d_a, d_b) else {
        return (None, market_price);
    };
    // On overflow → unknown (conservative).
    let off = BatchPrice::from_whole_price(market, d_a, d_b)
        .and_then(|price| price.exceeds(offered_a, requested_b, tol_bps))
        .ok();
    (off, market_price)
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
        assert_eq!(q.reason(), None);
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
    fn buyers_ahead_are_served_first_too() {
        // Offer 2_000_000 B for 990_000 A (worth 1_000_000 A at 2 B per A).
        // A buyer at a better rate takes 1_000_000 A of the 1_500_000 A sold.
        let ahead = level(900_000, 2_000_000, 2_000_000);
        let sellers = [level(1_900_000, 1_000_000, 1_500_000)];
        let order = order(2_000_000, 990_000);
        let q = quote(OrderSide::BuyBase, two(), TERMS, order, &[ahead], &sellers).unwrap();
        assert_eq!(q.status, FillStatus::Partial);
        assert_eq!(
            (q.fillable_offered, q.fillable_requested),
            (1_000_000, 495_000)
        );
        assert_eq!(q.available_offered, Some(1_000_000));
    }

    #[test]
    fn nothing_left_or_nothing_eligible_is_none_for_liquidity() {
        for opposite in [vec![], vec![level(1_000_000, 1_000_000, 50_000_000)]] {
            let q = sell(1_990_000, &[], &opposite);
            assert_eq!(q.band, PriceBand::AtMarket);
            assert_eq!(q.status, FillStatus::None);
            assert_eq!(q.reason(), Some(NoFillReason::Liquidity));
            assert_eq!((q.fillable_offered, q.fillable_requested), (0, 0));
            assert_eq!(q.available_offered, Some(0));
        }
    }

    #[test]
    fn a_partial_fill_must_reach_the_min_fill_step() {
        let with_step = |min_fill_step, buyers| {
            let order = QuoteOrder {
                min_fill_step: Some(min_fill_step),
                ..order(1_000_000, 1_990_000)
            };
            quote(
                OrderSide::SellBase,
                two(),
                TERMS,
                order,
                &[],
                &deep_buyers(buyers),
            )
            .unwrap()
        };
        let q = with_step(1_000_000, 1_000_000);
        assert_eq!(q.reason(), Some(NoFillReason::Liquidity));
        // A step above the whole order means all or nothing.
        assert_eq!(with_step(5_000_000, 1_000_000).status, FillStatus::None);
        assert_eq!(with_step(5_000_000, 3_000_000).status, FillStatus::Full);
    }

    #[test]
    fn a_tolerated_order_is_judged_at_its_break_even_price() {
        // 2.005 B per A: above the 1.998 fill price, below the 2.00799 edge.
        // It starts to fill at 2.005 / 0.999 = 2.007007 B per A.
        let q = sell(2_005_000, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::Tolerated);
        assert_eq!(q.status, FillStatus::Full);
        // Today it is worth less than it asks: no surplus, so no fee, and a
        // fill pays the ask.
        assert_eq!(q.expected_requested, 2_005_000);
        assert_eq!(q.fee, 0);
        // A buyer at 2.011 B per A still buys at 2.007007 (up to 2.008991)…
        let buyer_at = |pays| level(1_000_000, pays, 1_000_000);
        let q = sell(2_005_000, &[], &[buyer_at(2_011_000)]);
        assert_eq!(q.status, FillStatus::Partial);
        assert_eq!(
            (q.fillable_offered, q.fillable_requested),
            (498_254, 999_000)
        );
        assert_eq!(q.available_offered, Some(498_254));
        // …one at 2.0075 (up to 2.005494) does not.
        let q = sell(2_005_000, &[], &[buyer_at(2_007_500)]);
        assert_eq!(q.reason(), Some(NoFillReason::Liquidity));
    }

    #[test]
    fn ask_past_the_tolerance_is_off_market() {
        assert_eq!(sell(2_007_990, &[], &[]).band, PriceBand::Tolerated);
        let q = sell(2_007_991, &[], &deep_buyers(3_000_000));
        assert_eq!(q.band, PriceBand::OffMarket);
        assert_eq!(q.status, FillStatus::None);
        assert_eq!(q.reason(), Some(NoFillReason::Price));
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
        // Worth 1_000_000 A; the fee is capped at the 1_000 A surplus.
        assert_eq!((q.expected_requested, q.fee), (999_000, 1_000));
        assert_eq!(at(999_001).band, PriceBand::Tolerated);
        // The buyer's rule is looser than 1 − fee: the fee stops at the surplus.
        let order = order(4_000_000, 1_998_001);
        let q = quote(OrderSide::BuyBase, two(), TERMS, order, &[], &sellers).unwrap();
        assert_eq!(q.band, PriceBand::AtMarket);
        assert_eq!((q.expected_requested, q.fee), (1_998_001, 1_999));
    }

    #[test]
    fn docs_example_sells_one_eth_for_usdt() {
        // docs/price-api.md: ETH 18 decimals, USDT 6, mid 2500, fee 0.1%; the
        // wallet asks the fill price less 0.5% slippage: 2485.0125 USDT.
        let price = BatchPrice::from_whole_price(Decimal::from(2500), 18, 6).unwrap();
        let eth = 1_000_000_000_000_000_000;
        let order = order(eth, 2_485_012_500);
        let q = quote(OrderSide::SellBase, price, TERMS, order, &[], &[]).unwrap();
        assert_eq!(q.band, PriceBand::AtMarket);
        assert_eq!(q.expected_requested, 2_497_500_000);
        assert_eq!(q.fee, 2_500_000);
    }

    #[test]
    fn an_ask_built_from_the_fill_price_is_at_market() {
        // Buying ETH (18 decimals) with 10_000 USDT (6), mid 2500: the fill
        // price 0.0003996003996003996… is cut, not rounded up, at 18 places.
        let price = BatchPrice::from_whole_price(Decimal::from(2500), 18, 6).unwrap();
        let fill = fill_price(
            OrderSide::BuyBase,
            Decimal::ONE / Decimal::from(2500),
            1_000,
        )
        .unwrap();
        assert_eq!(fill.to_string(), "0.000399600399600399");
        let usdt = 10_000_000_000u64;
        let eth = (Decimal::from(usdt) * fill * Decimal::from(10u64.pow(12))).floor();
        let order = order(usdt, eth.try_into().unwrap());
        let q = quote(OrderSide::BuyBase, price, TERMS, order, &[], &[]).unwrap();
        assert_eq!(q.band, PriceBand::AtMarket);
    }

    #[test]
    fn fill_price_follows_the_side_and_fee() {
        let fill = fill_price(OrderSide::SellBase, Decimal::from(2500), 1_000).unwrap();
        assert_eq!(fill, Decimal::new(24975, 1));
        let fill = fill_price(OrderSide::BuyBase, Decimal::TWO, 1_000).unwrap();
        assert_eq!(fill.to_string(), "1.998001998001998001");
    }

    // ── v1: eval_can_fill ─────────────────────────────────────────────────────
    #[test]
    fn can_fill_crosses_with_enough_volume() {
        // user offers 100 A, wants 200 B; opposite best offers 300 B for 100 A.
        // cross: 100*300 > 200*100 → 30000 > 20000 ✓. volume 300 >= 200 ✓.
        assert!(eval_can_fill(100, 200, Some(level(100, 300, 300))));
    }

    #[test]
    fn can_fill_crosses_but_thin_volume() {
        // Same rate cross, but only 50 B available < 200 requested → not fillable.
        assert!(!eval_can_fill(100, 200, Some(level(100, 300, 50))));
    }

    #[test]
    fn can_fill_no_cross() {
        // Opposite best gives only 1.5 B per A (offers 150 B for 100 A); user wants
        // 2 B per A → 100*150 > 200*100? 15000 > 20000? no → doesn't cross.
        assert!(!eval_can_fill(100, 200, Some(level(100, 150, 1000))));
    }

    #[test]
    fn can_fill_no_book_entry() {
        assert!(!eval_can_fill(100, 200, None));
    }

    // ── eval_off_market (asymmetric decimals) ─────────────────────────────
    // 1 A (8 decimals) = 2 B (6 decimals) at the market midpoint.
    fn market_two() -> Option<Decimal> {
        Some(Decimal::TWO)
    }

    #[test]
    fn off_market_fair_note_is_false() {
        // offer 1 A (1e8), request 2 B (2e6): exactly the market.
        let (off, mkt) =
            eval_off_market(100_000_000, Some(8), 2_000_000, Some(6), market_two(), 50);
        assert_eq!(off, Some(false));
        assert_eq!(mkt.as_deref(), Some("2"));
    }

    #[test]
    fn off_market_greedy_note_is_true() {
        // offer 1 A, request 4 B → asks twice the market → off-market.
        let (off, mkt) =
            eval_off_market(100_000_000, Some(8), 4_000_000, Some(6), market_two(), 50);
        assert_eq!(off, Some(true));
        assert_eq!(mkt.as_deref(), Some("2"));
    }

    #[test]
    fn off_market_tolerance_is_exact() {
        // 2.01 B for 1 A is exactly 50 bps above the market: still within.
        let (off, _) = eval_off_market(100_000_000, Some(8), 2_010_000, Some(6), market_two(), 50);
        assert_eq!(off, Some(false));
        let (off, _) = eval_off_market(100_000_000, Some(8), 2_010_001, Some(6), market_two(), 50);
        assert_eq!(off, Some(true));
    }

    #[test]
    fn off_market_generous_note_is_false() {
        // offer 1 A, request 1 B → gives more than it asks.
        let (off, _) = eval_off_market(100_000_000, Some(8), 1_000_000, Some(6), market_two(), 50);
        assert_eq!(off, Some(false));
    }

    #[test]
    fn off_market_unpriced_is_none() {
        let (off, mkt) = eval_off_market(1, Some(8), 1, Some(6), None, 50);
        assert_eq!(off, None);
        assert_eq!(mkt, None);
    }

    #[test]
    fn off_market_missing_decimals_keeps_market_price() {
        // Market known, decimals unknown → flag unknown but market price present.
        let (off, mkt) = eval_off_market(1, None, 1, Some(6), market_two(), 50);
        assert_eq!(off, None);
        assert_eq!(mkt.as_deref(), Some("2"));
    }

    #[test]
    fn market_price_is_rounded_to_eighteen_places() {
        let third = Some(Decimal::ONE / Decimal::from(3));
        let (_, mkt) = eval_off_market(1, Some(0), 1, Some(0), third, 0);
        assert_eq!(mkt.as_deref(), Some("0.333333333333333333"));
    }
}

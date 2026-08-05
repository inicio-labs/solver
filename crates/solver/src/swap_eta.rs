//! Swap time-estimation support for the public `/v1/swap-eta` endpoint.
//!
//! Two pieces, both pure/self-contained and unit-tested here:
//!  * [`SettlementStats`] — an **in-memory, ephemeral** rolling window of recent
//!    settlement durations per directed pair (no DB storage). The executor owns
//!    one, records into it on each successful settlement, and publishes it over a
//!    `watch` channel; the price-API thread reads it to compute a 24h median.
//!  * The pure predicates behind the endpoint: [`eval_can_fill`] (does the order
//!    cross the top of the opposite book?) and [`eval_off_market`] (is the order
//!    priced worse than the real-time oracle?), plus small decimal formatters.

use std::collections::{HashMap, VecDeque};

use crate::matching::types::BestLevel;
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
        q.push_back(Sample { at_unix: now_unix, duration_secs });

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
        let mut durs: Vec<u64> =
            q.iter().filter(|s| s.at_unix >= cutoff).map(|s| s.duration_secs).collect();
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

/// Can an order offering `offered_a` of token A and requesting `requested_b` of
/// token B fill against `best` — the top level of the **opposite** pair (B→A)?
///
/// Crossing is strict (`>`), mirroring
/// [`crate::matching::types::Order::is_profitable_with`]; additionally the top
/// level must hold enough volume (`best.volume >= requested_b`).
pub fn eval_can_fill(offered_a: u64, requested_b: u64, best: Option<BestLevel>) -> bool {
    let Some(best) = best else {
        return false;
    };
    // Cross iff  offered_a * best.offered  >  requested_b * best.requested.
    let cross = (offered_a as u128) * (best.rate.offered as u128)
        > (requested_b as u128) * (best.rate.requested as u128);
    cross && best.volume >= requested_b
}

/// Is the order priced worse than the real-time oracle (off-market)?
///
/// Returns `(off_market, market_price)`. `off_market = Some(true)` when the order
/// demands more USD value than it offers by more than `tol_bps` (the user is
/// selling above / buying below market). `off_market` is `None` when a price or a
/// decimals value is missing (can't judge); `market_price` (requested-per-offered,
/// B per A, at oracle mid = `usd_a / usd_b`) is present whenever both prices are.
pub fn eval_off_market(
    offered_a: u64,
    d_a: Option<u8>,
    usd_a: Option<f64>,
    requested_b: u64,
    d_b: Option<u8>,
    usd_b: Option<f64>,
    tol_bps: u64,
) -> (Option<bool>, Option<String>) {
    let (Some(usd_a), Some(usd_b)) = (usd_a, usd_b) else {
        return (None, None);
    };
    if !(usd_a > 0.0) || !(usd_b > 0.0) {
        return (None, None);
    }
    let market_price = Some(fmt_f64(usd_a / usd_b));

    // off_market needs decimals to compare USD value exactly; without them we
    // still return market_price but leave the flag unknown.
    let (Some(d_a), Some(d_b)) = (d_a, d_b) else {
        return (None, market_price);
    };
    // The off-market comparison quantises USD prices to whole cents (integer math
    // keeps the ratio comparison exact). ACCEPTED LIMITATION: an asset priced
    // below ~$0.005 rounds to 0 cents, so we can't judge it and return the flag
    // as unknown (`None`) rather than guess. `market_price` above is still exact.
    // Revisit with lossless fixed-point if sub-cent assets get listed.
    let cents_a = (usd_a * 100.0).round() as u128;
    let cents_b = (usd_b * 100.0).round() as u128;
    if cents_a == 0 || cents_b == 0 {
        return (None, market_price);
    }

    // Common-denominator USD scaling (reduced by 10^min to bound magnitude),
    // mirroring the router's `scaled_usd`. On overflow → unknown (conservative).
    let off = (|| {
        let m = d_a.min(d_b);
        let offered_usd = (offered_a as u128)
            .checked_mul(cents_a)?
            .checked_mul(10u128.checked_pow((d_b - m) as u32)?)?;
        let requested_usd = (requested_b as u128)
            .checked_mul(cents_b)?
            .checked_mul(10u128.checked_pow((d_a - m) as u32)?)?;
        let lhs = requested_usd.checked_mul(10_000)?;
        let rhs = offered_usd.checked_mul(10_000u128.checked_add(tol_bps as u128)?)?;
        Some(lhs > rhs)
    })();

    (off, market_price)
}

/// Format an `f64` as a trimmed fixed-precision decimal (no exponent).
fn fmt_f64(x: f64) -> String {
    let s = format!("{x:.8}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::types::RateKey;
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
    fn best(requested: u64, offered: u64, volume: u64) -> BestLevel {
        BestLevel { rate: RateKey::new(requested, offered), volume }
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

    // ── eval_can_fill ─────────────────────────────────────────────────────
    #[test]
    fn can_fill_crosses_with_enough_volume() {
        // user offers 100 A, wants 200 B; opposite best offers 300 B for 100 A.
        // cross: 100*300 > 200*100 → 30000 > 20000 ✓. volume 300 >= 200 ✓.
        assert!(eval_can_fill(100, 200, Some(best(100, 300, 300))));
    }

    #[test]
    fn can_fill_crosses_but_thin_volume() {
        // Same rate cross, but only 50 B available < 200 requested → not fillable.
        assert!(!eval_can_fill(100, 200, Some(best(100, 300, 50))));
    }

    #[test]
    fn can_fill_no_cross() {
        // Opposite best gives only 1.5 B per A (offers 150 B for 100 A); user wants
        // 2 B per A → 100*150 > 200*100? 15000 > 20000? no → doesn't cross.
        assert!(!eval_can_fill(100, 200, Some(best(100, 150, 1000))));
    }

    #[test]
    fn can_fill_no_book_entry() {
        assert!(!eval_can_fill(100, 200, None));
    }

    // ── eval_off_market (asymmetric decimals) ─────────────────────────────
    // A = $2 / 8-dec, B = $1 / 6-dec.
    #[test]
    fn off_market_fair_note_is_false() {
        // offer 1 A (1e8), request 2 B (2e6): $2 for $2 → fair.
        let (off, mkt) =
            eval_off_market(100_000_000, Some(8), Some(2.0), 2_000_000, Some(6), Some(1.0), 50);
        assert_eq!(off, Some(false));
        assert_eq!(mkt.as_deref(), Some("2")); // usd_a/usd_b = 2/1
    }

    #[test]
    fn off_market_greedy_note_is_true() {
        // offer 1 A ($2), request 4 B (4e6, $4) → demands more value → off-market.
        let (off, mkt) =
            eval_off_market(100_000_000, Some(8), Some(2.0), 4_000_000, Some(6), Some(1.0), 50);
        assert_eq!(off, Some(true));
        assert_eq!(mkt.as_deref(), Some("2"));
    }

    #[test]
    fn off_market_generous_note_is_false() {
        // offer 1 A ($2), request 1 B (1e6, $1) → gives more than it asks.
        let (off, _) =
            eval_off_market(100_000_000, Some(8), Some(2.0), 1_000_000, Some(6), Some(1.0), 50);
        assert_eq!(off, Some(false));
    }

    #[test]
    fn off_market_unpriced_is_none() {
        let (off, mkt) =
            eval_off_market(1, Some(8), None, 1, Some(6), Some(1.0), 50);
        assert_eq!(off, None);
        assert_eq!(mkt, None);
    }

    #[test]
    fn off_market_missing_decimals_keeps_market_price() {
        // Prices known, decimals unknown → flag unknown but market price present.
        let (off, mkt) =
            eval_off_market(1, None, Some(2.0), 1, Some(6), Some(1.0), 50);
        assert_eq!(off, None);
        assert_eq!(mkt.as_deref(), Some("2"));
    }

}

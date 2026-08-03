//! Pure note-selection for external liquidity routing.
//!
//! Decides which unmatched notes to hand to which DEX given the DEXes' standing
//! quotes. Referentially transparent — no I/O, no websocket, no `OrderBook`
//! mutation — so it is exhaustively unit-tested. This is the **#1 correctness
//! surface**: a note's terms and a DEX's price live in *different* unit bases
//! (raw base units vs cents-per-whole-token), so every comparison is done in
//! exact `u128` integer arithmetic with explicit `10^decimals` normalisation.
//! A naive raw-rate-vs-price compare mis-routes by `10^(d_off−d_req)` (e.g. 100×
//! for 6-dec vs 8-dec tokens).
//!
//! NOTE on bases: the internal matcher (`price_feed::is_order_profitable`,
//! `three_edge_cycle`) is decimals-*blind* — it multiplies raw amount × cents,
//! which is only correct because the live devnet tokens share 8 decimals. This
//! module is deliberately decimals-*correct*; for equal decimals the two agree.

use crate::matching::price_feed::UsdCents;
use crate::matching::types::{Amount, DexId, OrderId, TokenId};
use std::collections::{HashMap, HashSet};

/// A pair as the NOTE orients it: `(offered_token, requested_token)`.
pub type Pair = (TokenId, TokenId);

/// Token decimals (immutable on-chain faucet property).
pub type Decimals = HashMap<TokenId, u8>;

/// A DEX's standing quote for one registered pair (one orientation). A DEX that
/// wants both directions of a pair posts two quotes.
#[derive(Clone, Debug)]
pub struct Quote {
    pub dex: DexId,
    /// The pair this quote applies to, `(offered_token, requested_token)` in the
    /// orientation of the notes it can fill (i.e. `offered_token` is what the DEX
    /// *receives* on consuming the note). The router builds this by **flipping**
    /// the DEX's filler-centric SDK quote (it gives `offered`, wants `requested`),
    /// since a note it fills offers what the DEX wants.
    pub pair: Pair,
    /// Price as an exact rational `price_num / price_den` = requested-token per
    /// offered-token, in **base units** (NOT per whole token). Taken straight
    /// from the SDK quote's base-unit amounts after the flip: `price_num =
    /// sdk.offered.amount`, `price_den = sdk.requested.amount`. Because both the
    /// note's terms and this price are base-unit, the willingness gate is a plain
    /// cross with no decimals; decimals enter only the oracle gates. Both > 0.
    pub price_num: u128,
    pub price_den: u128,
    /// Max requested-token quantity (base units) the DEX will take on this pair
    /// (= the SDK quote's `offered.amount` — the most it will give).
    pub quantity: Amount,
    /// Wall-clock unix-millis after which the quote is stale.
    pub expires_at: u64,
}

/// One unmatched note selected to hand to a DEX.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pick {
    pub dex: DexId,
    pub note_id: OrderId,
    /// requested-token amount to fill — the note's full `requested` in v1
    /// (whole-note handover; partial-note handover is a later refinement).
    pub fill: Amount,
    pub pair: Pair,
}

/// Minimal view of a residual note the selector needs (built from `Order`).
#[derive(Clone, Debug)]
pub struct NoteView {
    pub id: OrderId,
    pub offered_token: TokenId,
    pub offered: Amount,
    pub requested_token: TokenId,
    pub requested: Amount,
}

impl NoteView {
    pub fn pair(&self) -> Pair {
        (self.offered_token, self.requested_token)
    }
}

fn pow10(n: u8) -> Option<u128> {
    10u128.checked_pow(n as u32)
}

/// Scale offered/requested USD values to a shared denominator, reduced by the
/// common `10^min(d_off,d_req)` factor to bound magnitude. Returns
/// `(offered_scaled, requested_scaled)` in identical units, or `None` on
/// overflow (treated as not-exportable — conservative).
fn scaled_usd(
    off_raw: Amount,
    c_off: UsdCents,
    d_off: u8,
    req_raw: Amount,
    c_req: UsdCents,
    d_req: u8,
) -> Option<(u128, u128)> {
    let m = d_off.min(d_req);
    let off = (off_raw as u128)
        .checked_mul(c_off as u128)?
        .checked_mul(pow10(d_req - m)?)?;
    let req = (req_raw as u128)
        .checked_mul(c_req as u128)?
        .checked_mul(pow10(d_off - m)?)?;
    Some((off, req))
}

/// Decide whether `note` is exportable to the DEX behind `quote`, returning the
/// USD surplus `offered − requested` (in the quote-pair's reduced units, so it
/// is comparable for ordering *within* a pair) — or `None` if not exportable.
///
/// Gates (all must hold):
///   1. data: both tokens priced AND both decimals known;
///   2. oracle-edge vs MID: note generous to the consumer by ≥ `min_edge_bps`
///      *at oracle mid* (NOT the DEX's quote — so an in-band manipulated quote
///      can't move the export decision);
///   3. DEX-willingness: note rate is on the profitable side of the quote;
///   4. off-market guard: reject if the quote deviates from mid by > `max_dev_bps`.
fn export_surplus(
    note: &NoteView,
    quote: &Quote,
    price_cents: &impl Fn(TokenId) -> Option<UsdCents>,
    decimals: &Decimals,
    min_edge_bps: u64,
    max_dev_bps: u64,
) -> Option<u128> {
    // (1) data gate
    let c_off = price_cents(note.offered_token)?;
    let c_req = price_cents(note.requested_token)?;
    let d_off = *decimals.get(&note.offered_token)?;
    let d_req = *decimals.get(&note.requested_token)?;
    if c_off == 0 || c_req == 0 || quote.price_den == 0 || quote.price_num == 0 {
        return None;
    }

    let (off, req) = scaled_usd(note.offered, c_off, d_off, note.requested, c_req, d_req)?;

    // (2) oracle-edge: off >= req * (10000 + edge) / 10000
    let lhs = off.checked_mul(10_000)?;
    let rhs = req.checked_mul(10_000u128.checked_add(min_edge_bps as u128)?)?;
    if lhs < rhs {
        return None;
    }

    // (4) off-market guard (oracle check — this is where decimals enter): reject
    //     if the quote's base-unit rate deviates from oracle mid by > max_dev_bps.
    //     Oracle mid (requested-base per offered-base) = c_off·10^d_req /
    //     (c_req·10^d_off); the quote rate is price_num/price_den. Cross-multiplied
    //     and reduced by the common 10^m factor (m = min(d_off,d_req)):
    //       reject iff |num·c_req·10^(d_off-m) − c_off·den·10^(d_req-m)|·10000
    //                    > dev · c_off · den · 10^(d_req-m)
    //     OVERFLOW: price_num/price_den are base-unit `FungibleAsset` amounts, so
    //     each is ≤ `AssetAmount::MAX` (2^63−2^31), enforced on the wire by
    //     `AssetAmount::read_from`. But unlike gate 3 these products ALSO carry a
    //     `UsdCents` (u64, not amount-bounded) and a 10^(Δdecimals) factor, which
    //     are not covered by that bound — so they CAN exceed u128 for extreme
    //     tokens (high price × large size × wide decimal gap). `checked_mul` then
    //     yields `None` and the `?` treats the note as not-exportable — fail-safe,
    //     never a mis-fill or panic (see `no_panic_on_overflow_inputs`).
    let m = d_off.min(d_req);
    let q_side = quote
        .price_num
        .checked_mul(c_req as u128)?
        .checked_mul(pow10(d_off - m)?)?;
    let mid_side = (c_off as u128)
        .checked_mul(quote.price_den)?
        .checked_mul(pow10(d_req - m)?)?;
    let dev_lhs = q_side.abs_diff(mid_side).checked_mul(10_000)?;
    let dev_rhs = (max_dev_bps as u128)
        .checked_mul(c_off as u128)?
        .checked_mul(quote.price_den)?
        .checked_mul(pow10(d_req - m)?)?;
    if dev_lhs > dev_rhs {
        return None;
    }

    // (3) DEX-willingness (base-unit cross — NO decimals): the note's base-unit
    //     rate (requested/offered) must be at or below the quote's base-unit rate.
    //       reject iff requested·price_den > price_num·offered
    //     All four factors are `FungibleAsset` amounts, each ≤ `AssetAmount::MAX`
    //     (2^63−2^31), so both products are < 2^126: this cross provably CANNOT
    //     overflow u128 — the `checked_mul` here is belt-and-suspenders only.
    let w_lhs = (note.requested as u128).checked_mul(quote.price_den)?;
    let w_rhs = quote.price_num.checked_mul(note.offered as u128)?;
    if w_lhs > w_rhs {
        return None;
    }

    Some(off.saturating_sub(req))
}

/// Select which notes to hand to which DEX.
///
/// For each fresh quote, gathers the eligible residual notes for that pair
/// (Export gates), orders them **marginal-eligible-first** (smallest USD surplus
/// first — give away the least-generous, retain the best for internal crossing),
/// and accumulates up to `quote.quantity − reserved[(dex,pair)]`. Each note is
/// handed to at most one DEX (`used` set). Pure: no mutation of inputs.
#[allow(clippy::too_many_arguments)]
pub fn select_notes(
    candidates: &[NoteView],
    quotes: &[Quote],
    now: u64,
    price_cents: &impl Fn(TokenId) -> Option<UsdCents>,
    decimals: &Decimals,
    reserved: &HashMap<(DexId, Pair), Amount>,
    // `blocked`: (note, dex) pairs not to offer — e.g. a note that already
    // no-showed at that DEX (don't immediately re-offer it to the same one).
    blocked: &HashSet<(OrderId, DexId)>,
    min_edge_bps: u64,
    max_dev_bps: u64,
) -> Vec<Pick> {
    let mut picks = Vec::new();
    let mut used: HashSet<OrderId> = HashSet::new();

    for quote in quotes {
        if quote.expires_at <= now {
            continue; // stale
        }
        let already = *reserved.get(&(quote.dex, quote.pair)).unwrap_or(&0);
        let budget = quote.quantity.saturating_sub(already) as u128;
        if budget == 0 {
            continue;
        }

        // Eligible notes for this quote's pair, with their surplus (same pair =>
        // same unit scale => surplus is comparable here).
        let mut eligible: Vec<(u128, &NoteView)> = candidates
            .iter()
            .filter(|n| {
                n.pair() == quote.pair
                    && !used.contains(&n.id)
                    && !blocked.contains(&(n.id, quote.dex))
            })
            .filter_map(|n| {
                export_surplus(n, quote, price_cents, decimals, min_edge_bps, max_dev_bps)
                    .map(|s| (s, n))
            })
            .collect();
        // Marginal-first: smallest surplus given away first. `sort_by_key` is
        // stable, so equal-surplus notes keep input (book FIFO) order.
        eligible.sort_by_key(|(s, _)| *s);

        let mut taken: u128 = 0;
        for (_surplus, note) in eligible {
            let fill = note.requested as u128;
            if taken + fill > budget {
                continue; // would exceed the DEX's quoted quantity; a smaller note may still fit
            }
            taken += fill;
            used.insert(note.id);
            picks.push(Pick {
                dex: quote.dex,
                note_id: note.id,
                fill: note.requested,
                pair: quote.pair,
            });
        }
    }

    picks
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::note::NoteId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    };

    fn imiden() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
    }
    fn iusdt() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
    }
    fn ibtc() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2.try_into().unwrap()
    }
    fn nid(seed: u64) -> NoteId {
        NoteId::try_from_hex(&format!("0x{seed:064x}")).unwrap()
    }

    // Oracle: IMIDEN = $2.00 (200c), IUSDT = $1.00 (100c), IBTC = $100.00 (10000c).
    fn mid(t: TokenId) -> Option<UsdCents> {
        if t == imiden() {
            Some(200)
        } else if t == iusdt() {
            Some(100)
        } else if t == ibtc() {
            Some(10_000)
        } else {
            None
        }
    }

    /// decimals: IMIDEN 8, IUSDT **6**, IBTC 8 — asymmetric on purpose.
    fn decimals() -> Decimals {
        let mut d = Decimals::new();
        d.insert(imiden(), 8);
        d.insert(iusdt(), 6);
        d.insert(ibtc(), 8);
        d
    }

    /// A quote priced exactly at oracle mid for the IMIDEN/IUSDT pair. Mid is 2
    /// IUSDT per **whole** IMIDEN; in **base units** (IMIDEN 8-dec, IUSDT 6-dec)
    /// that is `2e6 / 1e8 = 1/50` requested-base per offered-base. At mid the
    /// off-market deviation is 0 and any at-or-below-mid note rate passes
    /// willingness — so this isolates the oracle-edge gate (gate 2) on that pair.
    fn open_quote(dex: DexId, pair: Pair, quantity: Amount) -> Quote {
        Quote { dex, pair, price_num: 1, price_den: 50, quantity, expires_at: u64::MAX }
    }

    // --- the 100× decimals trap ---

    #[test]
    fn parity_note_not_exported_decimals_correct() {
        // Offer 1 IMIDEN (1e8 @ $2 = $2) for 2 IUSDT (2e6 @ $1 = $2): parity.
        let note = NoteView {
            id: nid(1),
            offered_token: imiden(),
            offered: 100_000_000, // 1e8, 8-dec
            requested_token: iusdt(),
            requested: 2_000_000, // 2e6, 6-dec
        };
        // margin 0 → not exported for any positive edge (decimals-correct).
        let s = export_surplus(&note, &open_quote(1, note.pair(), u64::MAX), &mid, &decimals(), 1, 100_000);
        assert_eq!(s, None, "a fair note must not be exported");
        // And exactly at edge 0 it is borderline-exportable with zero surplus.
        let s0 = export_surplus(&note, &open_quote(1, note.pair(), u64::MAX), &mid, &decimals(), 0, 100_000);
        assert_eq!(s0, Some(0), "at edge 0, parity clears with zero surplus");
    }

    #[test]
    fn generous_note_exported_stingy_retained() {
        let pair = (imiden(), iusdt());
        // Generous: offer 1.1 IMIDEN ($2.20) for 2 IUSDT ($2.00) → +10%.
        let generous = NoteView {
            id: nid(2),
            offered_token: imiden(),
            offered: 110_000_000,
            requested_token: iusdt(),
            requested: 2_000_000,
        };
        // Stingy: offer 1.005 IMIDEN ($2.01) for 2 IUSDT ($2.00) → +0.5%.
        let stingy = NoteView {
            id: nid(3),
            offered_token: imiden(),
            offered: 100_500_000,
            requested_token: iusdt(),
            requested: 2_000_000,
        };
        let q = open_quote(1, pair, u64::MAX);
        // At a 1% (100bps) edge: generous exports, stingy is retained.
        assert!(export_surplus(&generous, &q, &mid, &decimals(), 100, 100_000).is_some());
        assert_eq!(export_surplus(&stingy, &q, &mid, &decimals(), 100, 100_000), None);
    }

    // --- ordering + quantity cap ---

    #[test]
    fn marginal_first_and_quantity_cap() {
        let pair = (imiden(), iusdt());
        // Two eligible notes, each requesting 2e6 IUSDT; budget only fits one.
        // 'a' is more generous (bigger surplus) than 'b'; marginal-first picks b.
        let a = NoteView { id: nid(10), offered_token: imiden(), offered: 150_000_000, requested_token: iusdt(), requested: 2_000_000 }; // $3.00
        let b = NoteView { id: nid(11), offered_token: imiden(), offered: 120_000_000, requested_token: iusdt(), requested: 2_000_000 }; // $2.40
        let cands = vec![a.clone(), b.clone()];
        let quotes = vec![open_quote(7, pair, 2_000_000)]; // room for exactly one note
        let picks = select_notes(&cands, &quotes, 0, &mid, &decimals(), &HashMap::new(), &HashSet::new(), 100, 100_000);
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, b.id, "least-generous eligible note handed over first");
        assert_eq!(picks[0].fill, 2_000_000);
        assert_eq!(picks[0].dex, 7);
    }

    #[test]
    fn reserved_reduces_budget() {
        let pair = (imiden(), iusdt());
        let note = NoteView { id: nid(20), offered_token: imiden(), offered: 130_000_000, requested_token: iusdt(), requested: 2_000_000 };
        let quotes = vec![open_quote(7, pair, 2_000_000)];
        let mut reserved = HashMap::new();
        reserved.insert((7u64, pair), 1_000_000); // half already committed → budget 1e6 < note's 2e6
        let picks = select_notes(&[note], &quotes, 0, &mid, &decimals(), &reserved, &HashSet::new(), 100, 100_000);
        assert!(picks.is_empty(), "note exceeds remaining quote budget");
    }

    #[test]
    fn stale_quote_skipped() {
        let pair = (imiden(), iusdt());
        let note = NoteView { id: nid(30), offered_token: imiden(), offered: 130_000_000, requested_token: iusdt(), requested: 2_000_000 };
        let mut q = open_quote(7, pair, u64::MAX);
        q.expires_at = 1_000;
        // now == expires_at → stale (strict >).
        let picks = select_notes(&[note], &[q], 1_000, &mid, &decimals(), &HashMap::new(), &HashSet::new(), 100, 100_000);
        assert!(picks.is_empty());
    }

    #[test]
    fn note_handed_to_at_most_one_dex() {
        let pair = (imiden(), iusdt());
        let note = NoteView { id: nid(40), offered_token: imiden(), offered: 130_000_000, requested_token: iusdt(), requested: 2_000_000 };
        let quotes = vec![open_quote(1, pair, u64::MAX), open_quote(2, pair, u64::MAX)];
        let picks = select_notes(&[note], &quotes, 0, &mid, &decimals(), &HashMap::new(), &HashSet::new(), 100, 100_000);
        assert_eq!(picks.len(), 1, "a note goes to exactly one DEX even if many quote for it");
    }

    // --- gates 3 & 4 ---

    #[test]
    fn off_market_quote_rejected() {
        let pair = (imiden(), iusdt());
        // Generous note (would export under a sane quote)...
        let note = NoteView { id: nid(50), offered_token: imiden(), offered: 130_000_000, requested_token: iusdt(), requested: 2_000_000 };
        // ...but the quote's implied rate is wildly off oracle mid. Base mid = 1/50
        // (requested-base per offered-base); a quote of 1/1 is 50× that → reject at 100bps.
        let q = Quote { dex: 1, pair, price_num: 1, price_den: 1, quantity: u64::MAX, expires_at: u64::MAX };
        let picks = select_notes(&[note], &[q], 0, &mid, &decimals(), &HashMap::new(), &HashSet::new(), 100, 100);
        assert!(picks.is_empty(), "off-market quote must not pull notes");
    }

    #[test]
    fn dex_unwilling_when_quote_below_note_rate() {
        let pair = (imiden(), iusdt());
        // Note base rate (requested/offered) = 2e6/130e6 ≈ 0.0154.
        let note = NoteView { id: nid(60), offered_token: imiden(), offered: 130_000_000, requested_token: iusdt(), requested: 2_000_000 };
        // DEX quotes only 1 IUSDT per whole IMIDEN = 1e6/1e8 = 0.01 base — below the
        // note's rate → unwilling. (0.01 vs mid 0.02 is 50% off; the wide band lets it
        // through so we isolate the willingness gate.)
        let q = Quote { dex: 1, pair, price_num: 1, price_den: 100, quantity: u64::MAX, expires_at: u64::MAX };
        let s = export_surplus(&note, &q, &mid, &decimals(), 100, 100_000);
        assert_eq!(s, None, "DEX quote below the note rate → not willing");
    }

    #[test]
    fn willingness_is_base_unit_exact_no_decimals() {
        // Regression: gate 3 is a plain base-unit cross (requested·den ≤ num·offered)
        // with NO decimal scaling, even under asymmetric decimals (the "100× trap":
        // IMIDEN 8-dec vs IUSDT 6-dec). Isolate it with a generous note (clears
        // gates 2 & 4 at a wide band) and move the quote by ONE base unit.
        let pair = (imiden(), iusdt());
        // Generous note: 1.5 IMIDEN ($3) offered for 2 IUSDT ($2). Its base rate
        // (requested/offered) = 2e6 / 1.5e8.
        let note = NoteView { id: nid(1), offered_token: imiden(), offered: 150_000_000, requested_token: iusdt(), requested: 2_000_000 };
        // Quote exactly at the note's base rate → willing (borderline).
        let at = Quote { dex: 1, pair, price_num: 2_000_000, price_den: 150_000_000, quantity: u64::MAX, expires_at: u64::MAX };
        assert!(export_surplus(&note, &at, &mid, &decimals(), 0, 100_000).is_some());
        // One base unit stingier (den +1 → rate just below the note's) → unwilling.
        let below = Quote { dex: 1, pair, price_num: 2_000_000, price_den: 150_000_001, quantity: u64::MAX, expires_at: u64::MAX };
        assert_eq!(export_surplus(&note, &below, &mid, &decimals(), 0, 100_000), None);
    }

    // --- overflow / extreme inputs ---

    #[test]
    fn no_panic_on_overflow_inputs() {
        // Astronomical amounts/prices/decimals must not panic — the checked math
        // returns None and the note is conservatively skipped (never mis-routed).
        let pair = (imiden(), iusdt());
        let note = NoteView {
            id: nid(1),
            offered_token: imiden(),
            offered: u64::MAX,
            requested_token: iusdt(),
            requested: u64::MAX,
        };
        let mut d = Decimals::new();
        d.insert(imiden(), 18);
        d.insert(iusdt(), 18);
        let price = |t: TokenId| {
            if t == imiden() || t == iusdt() {
                Some(u64::MAX)
            } else {
                None
            }
        };
        let q = Quote { dex: 1, pair, price_num: u128::MAX, price_den: 1, quantity: u64::MAX, expires_at: u64::MAX };
        let picks = select_notes(
            &[note], &[q], 0, &price, &d, &HashMap::new(), &HashSet::new(), 0, u64::MAX,
        );
        assert!(picks.is_empty(), "overflow inputs are conservatively skipped, not panicked");
    }

    #[test]
    fn extreme_decimals_0_and_18() {
        // 0-dec offered vs 18-dec requested — the 10^decimals normalisation must
        // still rank correctly. 1 unit of O ($2) for a parity amount of R ($2).
        let o = imiden();
        let r = iusdt();
        let mut d = Decimals::new();
        d.insert(o, 0); // whole units
        d.insert(r, 18);
        let price = |t: TokenId| if t == o { Some(200) } else if t == r { Some(100) } else { None };
        // 1 O = $2.  2*10^18 base-units of R = 2 whole R = $2 → parity.
        let parity = NoteView { id: nid(1), offered_token: o, offered: 1, requested_token: r, requested: 2_000_000_000_000_000_000 };
        // base mid = c_off·10^d_req/(c_req·10^d_off) = 200·10^18/100 = 2·10^18 (R-base per O-base).
        let q = Quote { dex: 1, pair: (o, r), price_num: 2_000_000_000_000_000_000, price_den: 1, quantity: u64::MAX, expires_at: u64::MAX };
        assert_eq!(
            export_surplus(&parity, &q, &price, &d, 1, 1_000_000),
            None,
            "parity across 0/18 decimals → not exported at a positive edge"
        );
        // Generous: 1 O ($2) for 1 whole R ($1) → +100% generous → exported.
        let generous = NoteView { id: nid(2), offered_token: o, offered: 1, requested_token: r, requested: 1_000_000_000_000_000_000 };
        assert!(export_surplus(&generous, &q, &price, &d, 1, 1_000_000).is_some());
    }

    // --- property-based: invariants over random books, quotes, decimals ---

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]
        /// Over random notes/quotes/decimals/prices, `select_notes` must: never
        /// panic, hand each note to ≤1 DEX, never exceed a quote's quantity, fill
        /// the full requested amount, and only export notes that genuinely clear
        /// the oracle edge (no value leak). Bounds keep the reference recomputation
        /// overflow-free; overflow safety itself is covered by the test above.
        #[test]
        fn prop_select_notes_invariants(
            decs in [0u8..=12, 0u8..=12, 0u8..=12],
            prices in [1u64..=10_000u64, 1u64..=10_000u64, 1u64..=10_000u64],
            raw_notes in prop::collection::vec(
                (0usize..3, 0usize..3, 1u64..=1_000_000_000u64, 1u64..=1_000_000_000u64),
                0..8,
            ),
            raw_quotes in prop::collection::vec(
                (1u64..=3u64, 0usize..3, 0usize..3, 1u128..=1_000_000u128, 1u128..=1_000u128, 1u64..=1_000_000_000_000u64),
                0..5,
            ),
            edge_bps in 0u64..=1_000u64,
        ) {
            let toks = [imiden(), iusdt(), ibtc()];
            let mut decimals = Decimals::new();
            for i in 0..3 { decimals.insert(toks[i], decs[i]); }
            let price_of = |t: TokenId| toks.iter().position(|x| *x == t).map(|i| prices[i]);

            let candidates: Vec<NoteView> = raw_notes.iter().enumerate()
                .filter(|(_, (i, j, _, _))| i != j)
                .map(|(k, (i, j, o, r))| NoteView {
                    id: nid(k as u64 + 1),
                    offered_token: toks[*i], offered: *o,
                    requested_token: toks[*j], requested: *r,
                })
                .collect();

            let mut seen = HashSet::new();
            let quotes: Vec<Quote> = raw_quotes.iter()
                .filter(|(_, i, j, _, _, _)| i != j)
                .filter_map(|(dex, i, j, num, den, qty)| {
                    let pair = (toks[*i], toks[*j]);
                    if !seen.insert((*dex, pair)) { return None; }
                    Some(Quote { dex: *dex, pair, price_num: *num, price_den: *den, quantity: *qty, expires_at: u64::MAX })
                })
                .collect();

            // Wide deviation band → off-market rarely binds (tested separately).
            let picks = select_notes(&candidates, &quotes, 0, &price_of, &decimals,
                &HashMap::new(), &HashSet::new(), edge_bps, u64::MAX);

            // (1) each note handed to at most one DEX.
            let mut once = HashSet::new();
            for p in &picks { prop_assert!(once.insert(p.note_id), "note routed to >1 DEX"); }

            // (2) per (dex,pair), Σ fill ≤ that quote's quantity.
            let mut sums: HashMap<(DexId, Pair), u128> = HashMap::new();
            for p in &picks { *sums.entry((p.dex, p.pair)).or_default() += p.fill as u128; }
            for ((dex, pair), sum) in &sums {
                let q = quotes.iter().find(|q| q.dex == *dex && q.pair == *pair).unwrap();
                prop_assert!(*sum <= q.quantity as u128, "over-allocated a quote's quantity");
            }

            // (3) every picked note fills its full requested amount AND genuinely
            //     clears the oracle edge (recomputed independently — no value leak).
            for p in &picks {
                let n = candidates.iter().find(|n| n.id == p.note_id).unwrap();
                prop_assert_eq!(p.fill, n.requested);
                let (c_off, c_req) = (price_of(n.offered_token).unwrap(), price_of(n.requested_token).unwrap());
                let (d_off, d_req) = (decimals[&n.offered_token], decimals[&n.requested_token]);
                let m = d_off.min(d_req);
                let off = (n.offered as u128) * (c_off as u128) * 10u128.pow((d_req - m) as u32);
                let req = (n.requested as u128) * (c_req as u128) * 10u128.pow((d_off - m) as u32);
                prop_assert!(off * 10_000 >= req * (10_000 + edge_bps as u128), "exported a note below the edge threshold");
            }
        }
    }

}

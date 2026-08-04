//! Pure note-selection for external liquidity routing.
//!
//! Decides which unmatched notes to hand to which DEX given the DEXes' standing
//! quotes. Referentially transparent — no I/O, no websocket, no `OrderBook`
//! mutation — so it is exhaustively unit-tested.
//!
//! A PSWAP note carries its rate **fixed on-chain**: whoever consumes it pays
//! exactly the note's `requested` for its `offered`, so the maker's price is
//! guaranteed regardless of which DEX fills it. Selection is therefore a pure
//! **willingness** check — does the DEX's standing quote accept the note's rate?
//! Both the note's terms and the quote's price live in the *same two tokens'
//! base units*, so the check is an exact integer cross with **no decimals and no
//! oracle** (the solver is a matchmaker here, not a price authority — it does not
//! second-guess a fixed on-chain rate against an off-chain oracle).

use crate::matching::types::{Amount, DexId, OrderId, TokenId};
use std::collections::{HashMap, HashSet};

/// A pair as the NOTE orients it: `(offered_token, requested_token)`.
pub type Pair = (TokenId, TokenId);

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
    /// offered-token, in **base units** (taken straight from the SDK quote's
    /// base-unit amounts after the flip: `price_num = sdk.offered.amount`,
    /// `price_den = sdk.requested.amount`). Because both the note's terms and this
    /// price are base-unit, willingness is a plain integer cross — no decimals.
    /// Both > 0.
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

/// Is the DEX behind `quote` willing to fill `note` at the note's fixed rate?
///
/// Base-unit cross, **no decimals**: the note demands `requested/offered`; the DEX
/// accepts anything at or below `price_num/price_den`. Willing iff
///   `note.requested · price_den ≤ price_num · note.offered`.
/// Both the note amounts and the quote price are base units of the same two
/// tokens, so decimals cancel.
///
/// All four factors are `FungibleAsset` amounts, each ≤ `AssetAmount::MAX`
/// (2^63−2^31, wire-enforced), so both products are < 2^126 and cannot overflow
/// u128; the `checked_mul` is belt-only (an impossible overflow ⇒ "not willing").
fn dex_is_willing(note: &NoteView, quote: &Quote) -> bool {
    if quote.price_num == 0 || quote.price_den == 0 {
        return false;
    }
    let (Some(lhs), Some(rhs)) = (
        (note.requested as u128).checked_mul(quote.price_den),
        quote.price_num.checked_mul(note.offered as u128),
    ) else {
        return false;
    };
    lhs <= rhs
}

/// Select which notes to hand to which DEX.
///
/// For each fresh quote, walk the residual notes for that pair (book order) and
/// take each one the DEX is willing to fill, accumulating up to
/// `quote.quantity − reserved[(dex,pair)]`. Each note is handed to at most one DEX
/// (`used` set). Pure: no mutation of inputs.
pub fn select_notes(
    candidates: &[NoteView],
    quotes: &[Quote],
    now: u64,
    reserved: &HashMap<(DexId, Pair), Amount>,
    // `blocked`: (note, dex) pairs not to offer — e.g. a note that already
    // no-showed at that DEX (don't immediately re-offer it to the same one).
    blocked: &HashSet<(OrderId, DexId)>,
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

        let mut taken: u128 = 0;
        for note in candidates {
            if note.pair() != quote.pair
                || used.contains(&note.id)
                || blocked.contains(&(note.id, quote.dex))
                || !dex_is_willing(note, quote)
            {
                continue;
            }
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

    /// A quote with an explicit base-unit rate + quantity, never stale.
    fn quote(dex: DexId, pair: Pair, price_num: u128, price_den: u128, quantity: Amount) -> Quote {
        Quote { dex, pair, price_num, price_den, quantity, expires_at: u64::MAX }
    }

    fn note(id: u64, offered_token: TokenId, offered: Amount, requested_token: TokenId, requested: Amount) -> NoteView {
        NoteView { id: nid(id), offered_token, offered, requested_token, requested }
    }

    // --- willingness (the only gate) ---

    #[test]
    fn willing_when_note_rate_at_or_below_quote() {
        let pair = (imiden(), iusdt());
        // Note offers 1.1 IMIDEN for 2 IUSDT (base: 110e6 for 2e6).
        let n = note(1, imiden(), 110_000_000, iusdt(), 2_000_000);
        // Quote accepts up to 1/50 (requested-base per offered-base): willing.
        let picks = select_notes(&[n], &[quote(7, pair, 1, 50, u64::MAX)], 0, &HashMap::new(), &HashSet::new());
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, nid(1));
        assert_eq!(picks[0].fill, 2_000_000);
        assert_eq!(picks[0].dex, 7);
    }

    #[test]
    fn unwilling_when_quote_rate_below_note() {
        let pair = (imiden(), iusdt());
        // Note base rate requested/offered = 2e6/130e6 ≈ 0.0154.
        let n = note(2, imiden(), 130_000_000, iusdt(), 2_000_000);
        // DEX accepts only 1/100 = 0.01 base — below the note's rate → unwilling.
        let picks = select_notes(&[n], &[quote(1, pair, 1, 100, u64::MAX)], 0, &HashMap::new(), &HashSet::new());
        assert!(picks.is_empty(), "DEX quote below the note rate → not willing");
    }

    #[test]
    fn willingness_is_base_unit_exact_no_decimals() {
        // The cross is a plain base-unit comparison (requested·den ≤ num·offered)
        // with NO decimal scaling — even though IMIDEN is 8-dec and IUSDT 6-dec
        // (the "100× trap" is irrelevant here because both sides are base units).
        let pair = (imiden(), iusdt());
        let n = note(1, imiden(), 150_000_000, iusdt(), 2_000_000);
        // Quote exactly at the note's base rate → willing (borderline).
        let at = quote(1, pair, 2_000_000, 150_000_000, u64::MAX);
        assert_eq!(select_notes(&[n.clone()], &[at], 0, &HashMap::new(), &HashSet::new()).len(), 1);
        // One base unit stingier (den +1 → rate just below the note's) → unwilling.
        let below = quote(1, pair, 2_000_000, 150_000_001, u64::MAX);
        assert!(select_notes(&[n], &[below], 0, &HashMap::new(), &HashSet::new()).is_empty());
    }

    #[test]
    fn zero_price_quote_is_not_willing() {
        let pair = (imiden(), iusdt());
        let n = note(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        assert!(select_notes(&[n.clone()], &[quote(1, pair, 0, 50, u64::MAX)], 0, &HashMap::new(), &HashSet::new()).is_empty());
        assert!(select_notes(&[n], &[quote(1, pair, 1, 0, u64::MAX)], 0, &HashMap::new(), &HashSet::new()).is_empty());
    }

    #[test]
    fn pair_must_match() {
        // A note on (IMIDEN, IUSDT) is never offered to an (IMIDEN, IBTC) quote.
        let n = note(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        let q = quote(1, (imiden(), ibtc()), 1, 50, u64::MAX);
        assert!(select_notes(&[n], &[q], 0, &HashMap::new(), &HashSet::new()).is_empty());
    }

    // --- quantity cap / reservations / mechanics ---

    #[test]
    fn quantity_cap_limits_picks() {
        let pair = (imiden(), iusdt());
        // Two willing notes, each requesting 2e6 IUSDT; budget fits exactly one.
        let a = note(10, imiden(), 150_000_000, iusdt(), 2_000_000);
        let b = note(11, imiden(), 120_000_000, iusdt(), 2_000_000);
        let picks = select_notes(&[a, b], &[quote(7, pair, 1, 50, 2_000_000)], 0, &HashMap::new(), &HashSet::new());
        assert_eq!(picks.len(), 1, "budget fits exactly one note");
        assert_eq!(picks[0].note_id, nid(10), "first willing note in book order is taken");
        assert_eq!(picks[0].fill, 2_000_000);
    }

    #[test]
    fn reserved_reduces_budget() {
        let pair = (imiden(), iusdt());
        let n = note(20, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut reserved = HashMap::new();
        reserved.insert((7u64, pair), 1_000_000); // half committed → budget 1e6 < note's 2e6
        let picks = select_notes(&[n], &[quote(7, pair, 1, 50, 2_000_000)], 0, &reserved, &HashSet::new());
        assert!(picks.is_empty(), "note exceeds remaining quote budget");
    }

    #[test]
    fn stale_quote_skipped() {
        let pair = (imiden(), iusdt());
        let n = note(30, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut q = quote(7, pair, 1, 50, u64::MAX);
        q.expires_at = 1_000;
        // now == expires_at → stale (strict >).
        assert!(select_notes(&[n], &[q], 1_000, &HashMap::new(), &HashSet::new()).is_empty());
    }

    #[test]
    fn blocked_pair_skipped() {
        let pair = (imiden(), iusdt());
        let n = note(35, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut blocked = HashSet::new();
        blocked.insert((nid(35), 7u64)); // don't re-offer this note to DEX 7
        let picks = select_notes(&[n], &[quote(7, pair, 1, 50, u64::MAX)], 0, &HashMap::new(), &blocked);
        assert!(picks.is_empty(), "a blocked (note, dex) is not re-offered");
    }

    #[test]
    fn note_handed_to_at_most_one_dex() {
        let pair = (imiden(), iusdt());
        let n = note(40, imiden(), 130_000_000, iusdt(), 2_000_000);
        let quotes = vec![quote(1, pair, 1, 50, u64::MAX), quote(2, pair, 1, 50, u64::MAX)];
        let picks = select_notes(&[n], &quotes, 0, &HashMap::new(), &HashSet::new());
        assert_eq!(picks.len(), 1, "a note goes to exactly one DEX even if many quote for it");
    }

    // --- overflow / extreme inputs ---

    #[test]
    fn no_panic_on_extreme_amounts() {
        // Astronomical amounts/prices must not panic — the checked cross returns
        // "not willing" on the (impossible, given AssetAmount::MAX) overflow.
        let pair = (imiden(), iusdt());
        let n = note(1, imiden(), u64::MAX, iusdt(), u64::MAX);
        let q = quote(1, pair, u128::MAX, 1, u64::MAX);
        let picks = select_notes(&[n], &[q], 0, &HashMap::new(), &HashSet::new());
        assert!(picks.is_empty(), "overflow inputs are conservatively skipped, not panicked");
    }

    // --- property-based: invariants over random books + quotes ---

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]
        /// Over random notes/quotes, `select_notes` must: never panic, hand each
        /// note to ≤1 DEX, never exceed a quote's quantity, fill the full requested
        /// amount, only export notes whose pair matches, and only export notes the
        /// DEX is genuinely willing to fill (recomputed independently — base-unit
        /// cross). Bounds keep the reference recomputation overflow-free; overflow
        /// safety itself is covered by `no_panic_on_extreme_amounts`.
        #[test]
        fn prop_select_notes_invariants(
            raw_notes in prop::collection::vec(
                (0usize..3, 0usize..3, 1u64..=1_000_000_000u64, 1u64..=1_000_000_000u64),
                0..8,
            ),
            raw_quotes in prop::collection::vec(
                (1u64..=3u64, 0usize..3, 0usize..3, 1u128..=1_000_000u128, 1u128..=1_000u128, 1u64..=1_000_000_000_000u64),
                0..5,
            ),
        ) {
            let toks = [imiden(), iusdt(), ibtc()];

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

            let picks = select_notes(&candidates, &quotes, 0, &HashMap::new(), &HashSet::new());

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

            // (3) every pick fills the full requested amount, matches the quote's
            //     pair, and is one the DEX is genuinely willing to fill.
            for p in &picks {
                let n = candidates.iter().find(|n| n.id == p.note_id).unwrap();
                prop_assert_eq!(p.fill, n.requested);
                prop_assert_eq!(n.pair(), p.pair, "pick pair matches note");
                let q = quotes.iter().find(|q| q.dex == p.dex && q.pair == p.pair).unwrap();
                prop_assert!(
                    (n.requested as u128) * q.price_den <= q.price_num * (n.offered as u128),
                    "exported a note the DEX is not willing to fill"
                );
            }
        }
    }
}

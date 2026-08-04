//! Pure note-selection for external liquidity routing.
//!
//! A PSWAP note's rate is fixed on-chain, so selection is a base-unit
//! **willingness** check — does the DEX's quote accept the note's rate? — with no
//! oracle and no decimals. Referentially transparent, so it is unit-tested here.
//!
//! Selection runs **per pair** over two rate-sorted inputs: the pair's notes (from
//! the book's `pair_index`, ascending `requested/offered` — best first) and the
//! pair's quotes (from the router, descending `supply/demand` — most generous
//! first). That is exactly the shape a future uniform-price batch clearing wants;
//! see [`match_pair`].

use crate::matching::types::{Amount, DexId, Order, OrderId, TokenId};
use std::collections::HashMap;

/// A pair as the NOTE orients it: `(offered_token, requested_token)`.
pub type Pair = (TokenId, TokenId);

/// A price as a base-unit ratio — `requested` of the note's requested-token per
/// `offered` of its offered-token (R per O). In v1 each note settles at its own
/// on-chain rate, so a pick carries the note's own rate; a future uniform-price
/// clearing would overwrite it with the pair's clearing rate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    pub requested: Amount,
    pub offered: Amount,
}

/// A DEX's standing quote for one pair, note-oriented (the router flips the DEX's
/// filler-centric SDK quote). `supply`/`demand` are its two base-unit amounts: it
/// supplies up to `supply` to receive `demand`; their ratio is the rate and
/// `supply` is the capacity. Both > 0.
#[derive(Clone, Debug)]
pub struct Quote {
    pub dex: DexId,
    pub pair: Pair,
    pub supply: Amount,
    pub demand: Amount,
    pub expires_at: u64,
}

/// One unmatched note selected to hand to a DEX (its full `requested` in v1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pick {
    pub dex: DexId,
    pub note_id: OrderId,
    pub fill: Amount,
    pub pair: Pair,
    /// The rate this fill settles at. v1: the note's own on-chain rate. A future
    /// uniform-price clearing sets it to the pair's clearing rate.
    pub price: Rate,
}

/// The result of matching one pair. `clearing` is a reserved seam for the future
/// uniform-price batch clearing — v1 leaves it `None` (each pick settles at its
/// own note rate); v2 sets it to the pair's single clearing rate.
#[derive(Clone, Debug, Default)]
pub struct PairMatch {
    pub picks: Vec<Pick>,
    pub clearing: Option<Rate>,
}

/// The willingness cross for a **non-degenerate** quote (`supply`, `demand` > 0):
/// `requested·demand ≤ supply·offered`, i.e. note rate ≤ quote rate. Every factor
/// is a `u64`, so each product is `< 2^128` and the cross can't overflow u128.
fn crosses(note: &Order, quote: &Quote) -> bool {
    (note.requested as u128) * (quote.demand as u128) <= (quote.supply as u128) * (note.offered as u128)
}

/// Match one pair's rate-sorted notes against its rate-sorted quotes.
///
/// **Preconditions** (guaranteed by the producers): `notes` ascending by
/// `requested/offered` (best/cheapest first, as `pair_index` stores them);
/// `quotes` descending by `supply/demand` (most generous first, as the router
/// publishes them). All quotes share `pair`.
///
/// v1 policy: greedy whole-note assignment. Each note goes to the **tightest**
/// crossing quote that still has room — the crossing quote with the *smallest*
/// rate (closest to the note's), so the more generous quotes stay free for the
/// notes that need them. A note larger than every quote's remaining capacity is
/// left unrouted (no splitting in v1).
///
/// v2 seam: the same two sorted inputs are what a uniform-price clearing consumes;
/// its per-pair clearing rate would fill `PairMatch::clearing` and each `Pick::price`.
fn match_pair(pair: Pair, notes: &[Order], quotes: &[Quote], time: u64) -> PairMatch {
    // Base units each quote can still supply this batch.
    let mut remaining: Vec<u128> = quotes.iter().map(|q| q.supply as u128).collect();
    let mut picks = Vec::new();

    for note in notes {
        let need = note.requested as u128;
        // Quotes descend by rate, so the crossing quotes are a prefix; the tightest
        // with room is the highest-index crossing quote whose capacity fits the note.
        let mut chosen: Option<usize> = None;
        for (i, q) in quotes.iter().enumerate() {
            if q.expires_at <= time || q.supply == 0 || q.demand == 0 {
                continue; // stale or degenerate — skip (the router pre-filters both)
            }
            if !crosses(note, q) {
                break; // sorted by rate: no tighter quote crosses either
            }
            if remaining[i] >= need {
                chosen = Some(i); // crosses + has room → tightest seen so far
            }
        }
        if let Some(i) = chosen {
            remaining[i] -= need;
            picks.push(Pick {
                dex: quotes[i].dex,
                note_id: note.id,
                fill: note.requested,
                pair,
                price: Rate { requested: note.requested, offered: note.offered },
            });
        }
    }

    PairMatch { picks, clearing: None }
}

/// Select which residual notes to hand to which DEX, per pair. Both maps are
/// rate-sorted by their producers (see [`match_pair`]); a note goes to at most one
/// DEX. Pure.
pub fn select_notes(
    notes_by_pair: &HashMap<Pair, Vec<Order>>,
    quotes_by_pair: &HashMap<Pair, Vec<Quote>>,
    time: u64,
) -> Vec<Pick> {
    let mut picks = Vec::new();
    for (pair, quotes) in quotes_by_pair {
        let notes = notes_by_pair.get(pair).map(Vec::as_slice).unwrap_or(&[]);
        picks.append(&mut match_pair(*pair, notes, quotes, time).picks);
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
    use std::collections::HashSet;

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
    fn order(id: u64, ot: TokenId, offered: Amount, rt: TokenId, requested: Amount) -> Order {
        Order { id: nid(id), offered_token: ot, requested_token: rt, offered, requested, requested_remaining: requested }
    }
    fn quote(dex: DexId, pair: Pair, supply: Amount, demand: Amount) -> Quote {
        Quote { dex, pair, supply, demand, expires_at: u64::MAX }
    }
    fn pair() -> Pair {
        (imiden(), iusdt())
    }

    /// Group + rate-sort exactly like the producers (`pair_index` for notes, the
    /// router snapshot for quotes), then run `select_notes`. Keeps the tests
    /// expressed as flat slices while exercising the real per-pair path.
    fn sel(notes: &[Order], quotes: &[Quote], time: u64) -> Vec<Pick> {
        let mut notes_by_pair: HashMap<Pair, Vec<Order>> = HashMap::new();
        for n in notes {
            notes_by_pair.entry((n.offered_token, n.requested_token)).or_default().push(n.clone());
        }
        for list in notes_by_pair.values_mut() {
            // ascending requested/offered (best/cheapest first)
            list.sort_by(|a, b| {
                (a.requested as u128 * b.offered as u128).cmp(&(b.requested as u128 * a.offered as u128))
            });
        }
        let mut quotes_by_pair: HashMap<Pair, Vec<Quote>> = HashMap::new();
        for q in quotes {
            quotes_by_pair.entry(q.pair).or_default().push(q.clone());
        }
        for list in quotes_by_pair.values_mut() {
            // descending supply/demand (most generous first)
            list.sort_by(|a, b| {
                (b.supply as u128 * a.demand as u128).cmp(&(a.supply as u128 * b.demand as u128))
            });
        }
        select_notes(&notes_by_pair, &quotes_by_pair, time)
    }

    #[test]
    fn willing_when_note_rate_at_or_below_quote() {
        // Note offers 110e6 imiden for 2e6 iusdt; DEX gives 10e6 iusdt at rate
        // 10e6/500e6 = 1/50 → willing (and 10e6 capacity fits the 2e6 fill).
        let n = order(1, imiden(), 110_000_000, iusdt(), 2_000_000);
        let picks = sel(&[n], &[quote(7, pair(), 10_000_000, 500_000_000)], 0);
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, nid(1));
        assert_eq!(picks[0].fill, 2_000_000);
        assert_eq!(picks[0].dex, 7);
    }

    #[test]
    fn unwilling_when_quote_rate_below_note() {
        // Quote rate 700e6/10e6 = 70 demand-per-supply exceeds the note's 130/2 = 65.
        let n = order(2, imiden(), 130_000_000, iusdt(), 2_000_000);
        let picks = sel(&[n], &[quote(1, pair(), 10_000_000, 700_000_000)], 0);
        assert!(picks.is_empty());
    }

    #[test]
    fn willingness_is_base_unit_exact_no_decimals() {
        // Plain base-unit cross even across 8-dec IMIDEN / 6-dec IUSDT: at the
        // note's exact rate → willing; one base unit stingier → not.
        let n = order(1, imiden(), 150_000_000, iusdt(), 2_000_000);
        assert_eq!(sel(&[n.clone()], &[quote(1, pair(), 2_000_000, 150_000_000)], 0).len(), 1);
        assert!(sel(&[n], &[quote(1, pair(), 2_000_000, 150_000_001)], 0).is_empty());
    }

    #[test]
    fn zero_quote_is_not_willing() {
        let n = order(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        assert!(sel(&[n.clone()], &[quote(1, pair(), 0, 500_000_000)], 0).is_empty());
        assert!(sel(&[n], &[quote(1, pair(), 10_000_000, 0)], 0).is_empty());
    }

    #[test]
    fn pair_must_match() {
        let n = order(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        let q = quote(1, (imiden(), ibtc()), 10_000_000, 500_000_000);
        assert!(sel(&[n], &[q], 0).is_empty());
    }

    #[test]
    fn supply_caps_the_batch() {
        // Two willing 2e6 notes; supply=2e6 fits exactly one (the cheaper note first).
        let a = order(10, imiden(), 150_000_000, iusdt(), 2_000_000);
        let b = order(11, imiden(), 120_000_000, iusdt(), 2_000_000);
        let picks = sel(&[a, b], &[quote(7, pair(), 2_000_000, 100_000_000)], 0);
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, nid(10));
        assert_eq!(picks[0].fill, 2_000_000);
    }

    #[test]
    fn stale_quote_skipped() {
        let n = order(30, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut q = quote(7, pair(), 10_000_000, 500_000_000);
        q.expires_at = 1_000; // now == expires_at → stale (strict >)
        assert!(sel(&[n], &[q], 1_000).is_empty());
    }

    #[test]
    fn note_handed_to_at_most_one_dex() {
        let n = order(40, imiden(), 130_000_000, iusdt(), 2_000_000);
        let quotes = vec![quote(1, pair(), 10_000_000, 500_000_000), quote(2, pair(), 10_000_000, 500_000_000)];
        assert_eq!(sel(&[n], &quotes, 0).len(), 1);
    }

    #[test]
    fn note_goes_to_tightest_crossing_quote() {
        // Note rate = 2e6/100e6 = 0.02. Two willing quotes, both with room:
        //   dex 1: supply/demand 10e6/300e6 = 0.0333 (generous, further above)
        //   dex 2: supply/demand 10e6/450e6 = 0.0222 (tighter, closer to 0.02)
        // The note should take the TIGHTEST (dex 2), leaving dex 1 free.
        let n = order(1, imiden(), 100_000_000, iusdt(), 2_000_000);
        let quotes = vec![quote(1, pair(), 10_000_000, 300_000_000), quote(2, pair(), 10_000_000, 450_000_000)];
        let picks = sel(&[n], &quotes, 0);
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].dex, 2, "assigned to the tightest crossing quote");
    }

    #[test]
    fn pick_carries_the_notes_own_rate_as_price() {
        // v1 seam: price == the note's own on-chain rate (requested/offered).
        let n = order(1, imiden(), 100_000_000, iusdt(), 2_000_000);
        let picks = sel(&[n], &[quote(7, pair(), 10_000_000, 300_000_000)], 0);
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].price, Rate { requested: 2_000_000, offered: 100_000_000 });
    }

    #[test]
    fn no_panic_on_extreme_amounts() {
        // u64·u64 < 2^128 so the cross is overflow-free even at the max.
        let n = order(1, imiden(), u64::MAX, iusdt(), u64::MAX);
        let picks = sel(&[n], &[quote(1, pair(), u64::MAX, u64::MAX)], 0);
        assert_eq!(picks.len(), 1);
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]
        #[test]
        fn prop_select_notes_invariants(
            raw_notes in prop::collection::vec((0usize..3, 0usize..3, 1u64..=1_000_000_000u64, 1u64..=1_000_000_000u64), 0..8),
            raw_quotes in prop::collection::vec((1u64..=3u64, 0usize..3, 0usize..3, 1u64..=1_000_000_000u64, 1u64..=1_000_000_000u64), 0..5),
        ) {
            let toks = [imiden(), iusdt(), ibtc()];
            let candidates: Vec<Order> = raw_notes.iter().enumerate()
                .filter(|(_, (i, j, _, _))| i != j)
                .map(|(k, (i, j, o, r))| order(k as u64 + 1, toks[*i], *o, toks[*j], *r))
                .collect();

            let mut seen = HashSet::new();
            let quotes: Vec<Quote> = raw_quotes.iter()
                .filter(|(_, i, j, _, _)| i != j)
                .filter_map(|(dex, i, j, supply, demand)| {
                    let p = (toks[*i], toks[*j]);
                    if !seen.insert((*dex, p)) { return None; }
                    Some(quote(*dex, p, *supply, *demand))
                })
                .collect();

            let picks = sel(&candidates, &quotes, 0);

            let mut once = HashSet::new();
            for p in &picks { prop_assert!(once.insert(p.note_id), "note routed to >1 DEX"); }

            let mut sums: HashMap<(DexId, Pair), u128> = HashMap::new();
            for p in &picks { *sums.entry((p.dex, p.pair)).or_default() += p.fill as u128; }
            for ((dex, p), sum) in &sums {
                let q = quotes.iter().find(|q| q.dex == *dex && q.pair == *p).unwrap();
                prop_assert!(*sum <= q.supply as u128, "over-allocated a quote's supply");
            }

            for p in &picks {
                let n = candidates.iter().find(|n| n.id == p.note_id).unwrap();
                prop_assert_eq!(p.fill, n.requested);
                prop_assert_eq!((n.offered_token, n.requested_token), p.pair);
                let q = quotes.iter().find(|q| q.dex == p.dex && q.pair == p.pair).unwrap();
                prop_assert!(
                    (n.requested as u128) * (q.demand as u128) <= (q.supply as u128) * (n.offered as u128),
                    "exported a note the DEX is not willing to fill"
                );
            }
        }
    }
}

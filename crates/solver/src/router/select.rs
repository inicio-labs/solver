//! Pure note-selection for external liquidity routing.
//!
//! A PSWAP note's rate is fixed on-chain, so selection is a base-unit
//! **willingness** check — does the DEX's quote accept the note's rate? — with no
//! oracle and no decimals. Referentially transparent, so it is unit-tested here.

use crate::matching::types::{Amount, DexId, Order, OrderId, TokenId};
use std::collections::HashSet;

/// A pair as the NOTE orients it: `(offered_token, requested_token)`.
pub type Pair = (TokenId, TokenId);

/// A DEX's standing quote for one pair, note-oriented (the router flips the DEX's
/// filler-centric SDK quote). `give`/`want` are its two base-unit amounts: it
/// gives up to `give` to receive `want`; their ratio is the rate and `give` is the
/// capacity. Both > 0.
#[derive(Clone, Debug)]
pub struct Quote {
    pub dex: DexId,
    pub pair: Pair,
    pub give: Amount,
    pub want: Amount,
    pub expires_at: u64,
}

/// One unmatched note selected to hand to a DEX (its full `requested` in v1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pick {
    pub dex: DexId,
    pub note_id: OrderId,
    pub fill: Amount,
    pub pair: Pair,
}

/// Willing iff the note's rate is at or below the quote's:
/// `requested·want ≤ give·offered`. All four are `FungibleAsset` amounts (u64), so
/// each product is `< 2^128` and the cross can't overflow u128.
fn dex_is_willing(note: &Order, quote: &Quote) -> bool {
    quote.give != 0
        && quote.want != 0
        && (note.requested as u128) * (quote.want as u128) <= (quote.give as u128) * (note.offered as u128)
}

/// Select which residual notes to hand to which DEX. Each fresh quote takes the
/// notes it's willing to fill, in book order, up to its `give`. A note goes to at
/// most one DEX. Pure.
pub fn select_notes(
    candidates: &[Order],
    quotes: &[Quote],
    now: u64,
    // `blocked`: (note, dex) pairs not to offer — e.g. a note that no-showed at
    // that DEX (don't immediately re-offer it there).
    blocked: &HashSet<(OrderId, DexId)>,
) -> Vec<Pick> {
    let mut picks = Vec::new();
    let mut used: HashSet<OrderId> = HashSet::new();

    for quote in quotes {
        if quote.expires_at <= now {
            continue;
        }
        let budget = quote.give as u128;
        let mut taken: u128 = 0;
        for note in candidates {
            if (note.offered_token, note.requested_token) != quote.pair
                || used.contains(&note.id)
                || blocked.contains(&(note.id, quote.dex))
                || !dex_is_willing(note, quote)
            {
                continue;
            }
            let fill = note.requested as u128;
            if taken + fill > budget {
                continue; // a smaller note may still fit
            }
            taken += fill;
            used.insert(note.id);
            picks.push(Pick { dex: quote.dex, note_id: note.id, fill: note.requested, pair: quote.pair });
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
    use std::collections::HashMap;

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
    fn quote(dex: DexId, pair: Pair, give: Amount, want: Amount) -> Quote {
        Quote { dex, pair, give, want, expires_at: u64::MAX }
    }
    fn pair() -> Pair {
        (imiden(), iusdt())
    }

    #[test]
    fn willing_when_note_rate_at_or_below_quote() {
        // Note offers 110e6 imiden for 2e6 iusdt; DEX gives 10e6 iusdt at rate
        // 10e6/500e6 = 1/50 → willing (and 10e6 capacity fits the 2e6 fill).
        let n = order(1, imiden(), 110_000_000, iusdt(), 2_000_000);
        let picks = select_notes(&[n], &[quote(7, pair(), 10_000_000, 500_000_000)], 0, &HashSet::new());
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, nid(1));
        assert_eq!(picks[0].fill, 2_000_000);
        assert_eq!(picks[0].dex, 7);
    }

    #[test]
    fn unwilling_when_quote_rate_below_note() {
        // Quote rate 700e6/10e6 = 70 want-per-give exceeds the note's 130/2 = 65.
        let n = order(2, imiden(), 130_000_000, iusdt(), 2_000_000);
        let picks = select_notes(&[n], &[quote(1, pair(), 10_000_000, 700_000_000)], 0, &HashSet::new());
        assert!(picks.is_empty());
    }

    #[test]
    fn willingness_is_base_unit_exact_no_decimals() {
        // Plain base-unit cross even across 8-dec IMIDEN / 6-dec IUSDT: at the
        // note's exact rate → willing; one base unit stingier → not.
        let n = order(1, imiden(), 150_000_000, iusdt(), 2_000_000);
        assert_eq!(select_notes(&[n.clone()], &[quote(1, pair(), 2_000_000, 150_000_000)], 0, &HashSet::new()).len(), 1);
        assert!(select_notes(&[n], &[quote(1, pair(), 2_000_000, 150_000_001)], 0, &HashSet::new()).is_empty());
    }

    #[test]
    fn zero_quote_is_not_willing() {
        let n = order(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        assert!(select_notes(&[n.clone()], &[quote(1, pair(), 0, 500_000_000)], 0, &HashSet::new()).is_empty());
        assert!(select_notes(&[n], &[quote(1, pair(), 10_000_000, 0)], 0, &HashSet::new()).is_empty());
    }

    #[test]
    fn pair_must_match() {
        let n = order(1, imiden(), 130_000_000, iusdt(), 2_000_000);
        let q = quote(1, (imiden(), ibtc()), 10_000_000, 500_000_000);
        assert!(select_notes(&[n], &[q], 0, &HashSet::new()).is_empty());
    }

    #[test]
    fn give_caps_the_batch() {
        // Two willing 2e6 notes; give=2e6 fits exactly one.
        let a = order(10, imiden(), 150_000_000, iusdt(), 2_000_000);
        let b = order(11, imiden(), 120_000_000, iusdt(), 2_000_000);
        let picks = select_notes(&[a, b], &[quote(7, pair(), 2_000_000, 100_000_000)], 0, &HashSet::new());
        assert_eq!(picks.len(), 1);
        assert_eq!(picks[0].note_id, nid(10));
        assert_eq!(picks[0].fill, 2_000_000);
    }

    #[test]
    fn stale_quote_skipped() {
        let n = order(30, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut q = quote(7, pair(), 10_000_000, 500_000_000);
        q.expires_at = 1_000; // now == expires_at → stale (strict >)
        assert!(select_notes(&[n], &[q], 1_000, &HashSet::new()).is_empty());
    }

    #[test]
    fn blocked_pair_skipped() {
        let n = order(35, imiden(), 130_000_000, iusdt(), 2_000_000);
        let mut blocked = HashSet::new();
        blocked.insert((nid(35), 7u64));
        assert!(select_notes(&[n], &[quote(7, pair(), 10_000_000, 500_000_000)], 0, &blocked).is_empty());
    }

    #[test]
    fn note_handed_to_at_most_one_dex() {
        let n = order(40, imiden(), 130_000_000, iusdt(), 2_000_000);
        let quotes = vec![quote(1, pair(), 10_000_000, 500_000_000), quote(2, pair(), 10_000_000, 500_000_000)];
        assert_eq!(select_notes(&[n], &quotes, 0, &HashSet::new()).len(), 1);
    }

    #[test]
    fn no_panic_on_extreme_amounts() {
        // u64·u64 < 2^128 so the cross is overflow-free even at the max.
        let n = order(1, imiden(), u64::MAX, iusdt(), u64::MAX);
        let picks = select_notes(&[n], &[quote(1, pair(), u64::MAX, u64::MAX)], 0, &HashSet::new());
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
                .filter_map(|(dex, i, j, give, want)| {
                    let p = (toks[*i], toks[*j]);
                    if !seen.insert((*dex, p)) { return None; }
                    Some(quote(*dex, p, *give, *want))
                })
                .collect();

            let picks = select_notes(&candidates, &quotes, 0, &HashSet::new());

            let mut once = HashSet::new();
            for p in &picks { prop_assert!(once.insert(p.note_id), "note routed to >1 DEX"); }

            let mut sums: HashMap<(DexId, Pair), u128> = HashMap::new();
            for p in &picks { *sums.entry((p.dex, p.pair)).or_default() += p.fill as u128; }
            for ((dex, p), sum) in &sums {
                let q = quotes.iter().find(|q| q.dex == *dex && q.pair == *p).unwrap();
                prop_assert!(*sum <= q.give as u128, "over-allocated a quote's give");
            }

            for p in &picks {
                let n = candidates.iter().find(|n| n.id == p.note_id).unwrap();
                prop_assert_eq!(p.fill, n.requested);
                prop_assert_eq!((n.offered_token, n.requested_token), p.pair);
                let q = quotes.iter().find(|q| q.dex == p.dex && q.pair == p.pair).unwrap();
                prop_assert!(
                    (n.requested as u128) * (q.want as u128) <= (q.give as u128) * (n.offered as u128),
                    "exported a note the DEX is not willing to fill"
                );
            }
        }
    }
}

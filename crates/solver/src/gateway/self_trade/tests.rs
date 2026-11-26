//! [`decide`] against a replay of the maker's commands in seq order.
//!
//! A small model of the store runs intake rounds as ADR 0009 describes
//! them: cancels, then claims, then one read per (maker, market) of the open
//! lineages and of every lineage newer than the round's lowest claimed seq,
//! then the decision, then the stops. Orders may also fill between rounds,
//! which takes them out of the open set for good (an expired order stays
//! open for this rule, ADR 0009, so the model has no expiry). After every
//! round no two open orders of a maker cross, and the decision kept its
//! contract: it cancelled only open orders, each naming the first newer
//! crossing order the round read, and let exactly the claims still open
//! go live. At the end, every lineage that did not finish is stopped exactly
//! when a replay in seq order stops it: its maker cancelled it, a cutoff
//! covers it, or a newer crossing order of its maker was submitted.

use std::collections::{BTreeSet, HashMap, HashSet};

use rand::rngs::StdRng;
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};

use super::{decide, Decision, Lineage, Loss};
use crate::maker::{LineageId, MakerId};

const SELL: &[u8] = b"sell";
const BUY: &[u8] = b"buy";

/// A maker's order on `market`: a sell at `price` offers 100 for `price`,
/// a buy at `price` offers `price` for 100, so a bid crosses an ask when
/// its price is at or above it.
fn quote(maker_id: MakerId, market: u8, seq: u64, sell: bool, price: u64) -> Lineage {
    let (direction, offered, requested) = if sell {
        (SELL, 100, price)
    } else {
        (BUY, price, 100)
    };
    Lineage {
        lineage_id: format!("{maker_id}/{seq}").into_bytes(),
        maker_id,
        market: vec![market],
        direction: direction.to_vec(),
        root_seq: seq,
        offered,
        requested,
    }
}

#[test]
fn crossing_is_symmetric_and_needs_one_maker_one_pair_two_sides() {
    let ask = quote(1, 0, 1, true, 100);
    let bid = quote(1, 0, 2, false, 100);
    assert!(ask.crosses(&bid) && bid.crosses(&ask), "equal prices cross");
    assert!(!quote(1, 0, 3, false, 99).crosses(&ask));
    assert!(quote(1, 0, 3, false, 101).crosses(&ask));
    assert!(!quote(1, 0, 3, true, 90).crosses(&ask), "same side");
    assert!(!quote(2, 0, 3, false, 120).crosses(&ask), "other maker");
    assert!(!quote(1, 1, 3, false, 120).crosses(&ask), "other pair");
    // The largest amounts a fungible asset holds do not overflow.
    let max = (1 << 63) - (1 << 31);
    let big = Lineage {
        offered: max,
        requested: max,
        ..ask.clone()
    };
    let other = Lineage {
        direction: BUY.to_vec(),
        offered: max,
        requested: max - 1,
        ..ask
    };
    assert!(big.crosses(&other));
    // Exact at that size: in f64, max - 1 rounds to max.
    let below = Lineage {
        offered: max - 1,
        requested: max,
        ..other.clone()
    };
    assert!(
        !big.crosses(&below),
        "one unit below the ask does not cross"
    );
}

/// Crossing, written another way: the bid's price (what it offers per unit
/// it asks) is at or above the ask's, compared as fractions by Euclid's
/// algorithm, with no product that could overflow.
fn bid_at_or_above_ask(bid: &Lineage, ask: &Lineage) -> bool {
    // n1/d1 >= n2/d2, all positive.
    fn at_least(n1: u64, d1: u64, n2: u64, d2: u64) -> bool {
        let (q1, r1, q2, r2) = (n1 / d1, n1 % d1, n2 / d2, n2 % d2);
        if q1 != q2 {
            return q1 > q2;
        }
        match (r1, r2) {
            (_, 0) => true,
            (0, _) => false,
            // r1/d1 >= r2/d2 exactly when d2/r2 >= d1/r1.
            _ => at_least(d2, r2, d1, r1),
        }
    }
    at_least(bid.offered, bid.requested, ask.requested, ask.offered)
}

#[test]
fn crossing_agrees_with_a_price_comparison_on_random_sizes() {
    let mut rng = StdRng::seed_from_u64(9);
    let max = (1_u64 << 63) - (1 << 31);
    for run in 0..20_000 {
        let size = |rng: &mut StdRng| match rng.random_range(0..3) {
            0 => rng.random_range(1..=1_000),
            1 => rng.random_range(1..=1_000_000_000),
            _ => rng.random_range(1..=max),
        };
        let ask = Lineage {
            offered: size(&mut rng),
            requested: size(&mut rng),
            ..quote(1, 0, 1, true, 1)
        };
        // Every third bid sits exactly at the ask's price, scaled, or one
        // unit either side of it.
        let bid = if run % 3 == 0 {
            let scale = rng.random_range(1..=1_000);
            let (requested, offered) = (ask.offered, ask.requested);
            let fits = offered.checked_mul(scale).zip(requested.checked_mul(scale));
            let (offered, requested) = fits.unwrap_or((offered, requested));
            let nudge: i64 = rng.random_range(-1..=1);
            Lineage {
                offered: offered.saturating_add_signed(nudge).max(1),
                requested,
                ..quote(1, 0, 2, false, 1)
            }
        } else {
            Lineage {
                offered: size(&mut rng),
                requested: size(&mut rng),
                ..quote(1, 0, 2, false, 1)
            }
        };
        let expected = bid_at_or_above_ask(&bid, &ask);
        assert_eq!(bid.crosses(&ask), expected, "{bid:?} {ask:?}");
        assert_eq!(ask.crosses(&bid), expected, "symmetric: {bid:?} {ask:?}");
    }
}

#[test]
fn the_first_newer_crossing_order_is_named_and_winners_keep_arrival_order() {
    let old_ask = quote(1, 0, 1, true, 100);
    let bid = quote(1, 0, 5, false, 101);
    let higher_bid = quote(1, 0, 3, false, 102);
    let other_ask = quote(1, 0, 4, true, 200);
    let claimed = [bid.clone(), higher_bid.clone(), other_ask.clone()];
    let open: HashSet<_> = [&old_ask, &bid, &higher_bid, &other_ask]
        .iter()
        .map(|l| l.lineage_id.clone())
        .collect();
    let decision = decide(&claimed, std::slice::from_ref(&old_ask), &open);
    // Both bids cross the old ask; the first newer one (seq 3) is named,
    // although the other arrived first.
    assert_eq!(
        decision.losers.into_iter().collect::<Vec<_>>(),
        [(
            old_ask.lineage_id.clone(),
            Loss {
                maker_id: 1,
                caused_by: higher_bid.lineage_id.clone()
            }
        )]
    );
    assert_eq!(
        decision.winners,
        [
            bid.lineage_id.clone(),
            higher_bid.lineage_id.clone(),
            other_ask.lineage_id.clone()
        ]
    );
}

#[test]
fn an_entering_loser_names_the_first_newer_crossing_order_it_read() {
    let late_ask = quote(1, 0, 2, true, 100);
    let newer: Vec<Lineage> = (3..=9).map(|seq| quote(1, 0, seq, false, 101)).collect();
    let mut open: HashSet<_> = newer.iter().map(|l| l.lineage_id.clone()).collect();
    open.insert(late_ask.lineage_id.clone());
    // Each call iterates a fresh hash map: the answer must not depend on
    // its order.
    for _ in 0..20 {
        let decision = decide(std::slice::from_ref(&late_ask), &newer, &open);
        assert_eq!(
            decision.losers.get(&late_ask.lineage_id),
            Some(&Loss {
                maker_id: 1,
                caused_by: newer[0].lineage_id.clone()
            })
        );
        assert!(decision.winners.is_empty());
    }
}

/// A closed order is never cancelled, and a closed submit never goes live,
/// though it still counts as the newer order.
#[test]
fn a_closed_order_neither_loses_nor_goes_live() {
    let filled_bid = quote(1, 0, 1, false, 101);
    let dead_ask = quote(1, 0, 2, true, 100);
    let newer_bid = quote(1, 0, 3, false, 101);
    let open = HashSet::from([newer_bid.lineage_id.clone()]);
    let decision = decide(
        std::slice::from_ref(&dead_ask),
        &[filled_bid, newer_bid],
        &open,
    );
    assert_eq!(decision, Decision::default());
}

/// A maker command as intake receives it.
#[derive(Clone, Debug)]
enum Cmd {
    Submit(Lineage),
    CancelOrder {
        maker_id: MakerId,
        seq: u64,
        target: LineageId,
    },
    CancelAll {
        maker_id: MakerId,
        seq: u64,
        market: Option<u8>,
        sells: Option<bool>,
    },
}

impl Cmd {
    fn key(&self) -> (MakerId, u64) {
        match self {
            Cmd::Submit(lineage) => (lineage.maker_id, lineage.root_seq),
            Cmd::CancelOrder { maker_id, seq, .. } | Cmd::CancelAll { maker_id, seq, .. } => {
                (*maker_id, *seq)
            }
        }
    }

    fn is_submit(&self) -> bool {
        matches!(self, Cmd::Submit(_))
    }
}

/// Ways to break the rule or the round, each of which the checks must
/// catch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Break {
    /// A newer order counts only while it is open.
    NewerOnlyWhileOpen,
    /// A submit its maker had cancelled cancels nothing.
    DeadSubmitCancelsNothing,
    /// A submit that loses cancels nothing.
    LoserCancelsNothing,
    /// Equal prices do not cross.
    StrictlyCrossed,
    /// The round reads before it claims its own submits.
    ReadBeforeClaims,
    /// The newer read starts at the round's highest claimed seq.
    NewerAboveHighestClaim,
}

/// The store as the round sees it.
#[derive(Default)]
struct Store {
    seen: HashSet<(MakerId, u64)>,
    claims: HashMap<LineageId, Lineage>,
    stops: HashSet<LineageId>,
    cutoffs: Vec<(MakerId, Option<u8>, Option<bool>, u64)>,
    finished: HashSet<LineageId>,
    maker_cancelled: HashSet<LineageId>,
    /// Rounds whose decision broke its contract with the caller: a loser
    /// that was not open, a loser not naming the first newer crossing order
    /// the round read, or winners other than the claims still open after the
    /// stops, in arrival order.
    broken_contracts: usize,
}

impl Store {
    fn cut(&self, lineage: &Lineage) -> bool {
        self.cutoffs
            .iter()
            .any(|(maker_id, market, sells, cutoff)| {
                *maker_id == lineage.maker_id
                    && lineage.root_seq < *cutoff
                    && market.is_none_or(|market| lineage.market == [market])
                    && sells.is_none_or(|sells| (lineage.direction == SELL) == sells)
            })
    }

    fn dead(&self, lineage: &Lineage) -> bool {
        self.stops.contains(&lineage.lineage_id) || self.cut(lineage)
    }

    fn open(&self, lineage: &Lineage) -> bool {
        !self.dead(lineage) && !self.finished.contains(&lineage.lineage_id)
    }

    fn open_ids(&self) -> BTreeSet<LineageId> {
        self.claims
            .values()
            .filter(|l| self.open(l))
            .map(|l| l.lineage_id.clone())
            .collect()
    }

    fn write_cancel(&mut self, cmd: &Cmd) {
        if !self.seen.insert(cmd.key()) {
            return;
        }
        match cmd {
            Cmd::CancelOrder {
                maker_id: _,
                target,
                ..
            } => {
                self.stops.insert(target.clone());
                self.maker_cancelled.insert(target.clone());
            }
            Cmd::CancelAll {
                maker_id,
                seq,
                market,
                sells,
            } => self.cutoffs.push((*maker_id, *market, *sells, *seq)),
            Cmd::Submit(_) => unreachable!(),
        }
    }

    /// The claim: a replay or an already claimed lineage takes no part.
    fn claim(&mut self, lineage: &Lineage) -> bool {
        if !self.seen.insert((lineage.maker_id, lineage.root_seq)) {
            return false;
        }
        if self.claims.contains_key(&lineage.lineage_id) {
            return false;
        }
        self.claims
            .insert(lineage.lineage_id.clone(), lineage.clone());
        true
    }

    /// The round's read, per (maker, market) of its claims: the open
    /// lineages, and every lineage newer than its lowest claim.
    fn read(
        &self,
        claimed: &[Lineage],
        broken: Option<Break>,
    ) -> (Vec<Lineage>, HashSet<LineageId>) {
        let mut lowest: HashMap<(MakerId, Vec<u8>), u64> = HashMap::new();
        for lineage in claimed {
            let key = (lineage.maker_id, lineage.market.clone());
            let entry = lowest.entry(key).or_insert(lineage.root_seq);
            *entry = if broken == Some(Break::NewerAboveHighestClaim) {
                (*entry).max(lineage.root_seq)
            } else {
                (*entry).min(lineage.root_seq)
            };
        }
        let mut known = Vec::new();
        let mut open = HashSet::new();
        for lineage in self.claims.values() {
            let Some(lowest) = lowest.get(&(lineage.maker_id, lineage.market.clone())) else {
                continue;
            };
            let is_open = self.open(lineage);
            if is_open {
                open.insert(lineage.lineage_id.clone());
            }
            if is_open || lineage.root_seq > *lowest {
                known.push(lineage.clone());
            }
        }
        (known, open)
    }

    /// One intake round, as intake writes it.
    fn round(&mut self, commands: &[Cmd], broken: Option<Break>) {
        let (cancels, submits): (Vec<&Cmd>, Vec<&Cmd>) =
            commands.iter().partition(|cmd| !cmd.is_submit());
        let mut claimed = Vec::new();
        cancels.iter().for_each(|cmd| self.write_cancel(cmd));
        for cmd in &submits {
            if let Cmd::Submit(lineage) = cmd {
                if self.claim(lineage) {
                    claimed.push(lineage.clone());
                }
            }
        }
        if claimed.is_empty() {
            return;
        }
        let (mut known, mut open) = self.read(&claimed, broken);
        if broken == Some(Break::ReadBeforeClaims) {
            // The read misses the round's own claims: none is open.
            let ids: HashSet<_> = claimed.iter().map(|l| l.lineage_id.clone()).collect();
            known.retain(|l| !ids.contains(&l.lineage_id));
            open.retain(|id| !ids.contains(id));
        }
        let mut entering = claimed.clone();
        match broken {
            Some(Break::NewerOnlyWhileOpen) => known.retain(|l| open.contains(&l.lineage_id)),
            Some(Break::DeadSubmitCancelsNothing) => {
                entering.retain(|l| open.contains(&l.lineage_id))
            }
            Some(Break::StrictlyCrossed) => {
                // ox*oy >= rx*ry on these amounts is ox*oy > rx*ry on the
                // originals (all amounts are far below the factor).
                let strict = |l: &Lineage| Lineage {
                    offered: l.offered * 1_000_000,
                    requested: l.requested * 1_000_000 + 1,
                    ..l.clone()
                };
                known = known.iter().map(strict).collect();
                entering = entering.iter().map(strict).collect();
            }
            _ => {}
        }
        let mut decision = decide(&entering, &known, &open);
        if broken == Some(Break::LoserCancelsNothing) {
            let lost: BTreeSet<_> = decision.losers.keys().cloned().collect();
            decision
                .losers
                .retain(|_, loss| !lost.contains(&loss.caused_by));
        }
        if decision.losers.keys().any(|loser| !open.contains(loser)) {
            self.broken_contracts += 1;
        }
        // Each loser names the lowest-seq newer order that crosses it among
        // those the round compared it with: every one for a claim, the
        // claims for a lineage only read.
        let claim_ids: HashSet<&LineageId> = entering.iter().map(|l| &l.lineage_id).collect();
        let pool: HashMap<&LineageId, &Lineage> = known
            .iter()
            .chain(&entering)
            .map(|l| (&l.lineage_id, l))
            .collect();
        for (loser, loss) in &decision.losers {
            let Some(lost) = pool.get(loser) else {
                self.broken_contracts += 1;
                continue;
            };
            let first = pool
                .values()
                .filter(|c| c.root_seq > lost.root_seq && c.crosses(lost))
                .filter(|c| claim_ids.contains(loser) || claim_ids.contains(&c.lineage_id))
                .min_by_key(|c| c.root_seq);
            if first.map(|c| &c.lineage_id) != Some(&loss.caused_by) {
                self.broken_contracts += 1;
            }
        }
        for (loser, _) in decision.losers {
            open.remove(&loser);
            self.stops.insert(loser);
        }
        let still_open: Vec<LineageId> = claimed
            .iter()
            .filter(|l| self.open(l))
            .map(|l| l.lineage_id.clone())
            .collect();
        if decision.winners != still_open {
            self.broken_contracts += 1;
        }
    }

    /// No two open orders of a maker cross.
    fn crossed_pairs(&self) -> usize {
        let open: Vec<&Lineage> = self.claims.values().filter(|l| self.open(l)).collect();
        let mut pairs = 0;
        for (index, a) in open.iter().enumerate() {
            pairs += open[index + 1..].iter().filter(|b| a.crosses(b)).count();
        }
        pairs
    }

    /// Lineages that did not finish whose stop differs from a seq-order
    /// replay of every command that arrived.
    fn differences(&self) -> Vec<LineageId> {
        self.claims
            .values()
            .filter(|l| !self.finished.contains(&l.lineage_id))
            .filter(|l| {
                let replay = self.maker_cancelled.contains(&l.lineage_id)
                    || self.cut(l)
                    || self
                        .claims
                        .values()
                        .any(|newer| newer.root_seq > l.root_seq && newer.crosses(l));
                replay != self.dead(l)
            })
            .map(|l| l.lineage_id.clone())
            .collect()
    }
}

/// The commands of up to two makers on up to two pairs, each maker's in seq
/// order with gaps: submits around one price, targeted cancels of earlier
/// submits, and cancel-alls of every scope.
fn commands(rng: &mut StdRng) -> Vec<Cmd> {
    let mut all = Vec::new();
    for maker_id in 1..=rng.random_range(1..=2) {
        let mut seq = 0;
        let mut submitted: Vec<LineageId> = Vec::new();
        for _ in 0..rng.random_range(2..=12) {
            seq += rng.random_range(1..=3);
            let roll = rng.random_range(0..100);
            if roll < 78 || submitted.is_empty() {
                let lineage = quote(
                    maker_id,
                    rng.random_range(0..2),
                    seq,
                    rng.random_bool(0.5),
                    rng.random_range(97..=103),
                );
                submitted.push(lineage.lineage_id.clone());
                all.push(Cmd::Submit(lineage));
            } else if roll < 92 {
                let target = submitted[rng.random_range(0..submitted.len())].clone();
                all.push(Cmd::CancelOrder {
                    maker_id,
                    seq,
                    target,
                });
            } else {
                all.push(Cmd::CancelAll {
                    maker_id,
                    seq,
                    market: rng.random_bool(0.5).then(|| rng.random_range(0..2)),
                    sells: rng.random_bool(0.5).then(|| rng.random_bool(0.5)),
                });
            }
        }
    }
    all
}

/// Shuffle the arrival order: in order, a few swaps, or fully; and resend a
/// few submits later (exact retries).
fn arrival(rng: &mut StdRng, mut commands: Vec<Cmd>) -> Vec<Cmd> {
    match rng.random_range(0..3) {
        0 => {}
        1 => {
            for _ in 0..commands.len() / 2 {
                let index = rng.random_range(0..commands.len().saturating_sub(1).max(1));
                if index + 1 < commands.len() {
                    commands.swap(index, index + 1);
                }
            }
        }
        _ => commands.shuffle(rng),
    }
    for _ in 0..rng.random_range(0..3) {
        let index = rng.random_range(0..commands.len());
        if commands[index].is_submit() {
            let at = rng.random_range(index..=commands.len());
            commands.insert(at, commands[index].clone());
        }
    }
    commands
}

/// Run `commands` in rounds of random size, filling a random open order
/// between rounds now and then. Returns the store and whether a round left
/// a crossed pair open.
fn run(rng: &mut StdRng, commands: &[Cmd], broken: Option<Break>) -> (Store, bool) {
    let mut store = Store::default();
    let mut crossed = false;
    let mut rest = commands;
    while !rest.is_empty() {
        let size = rng.random_range(1..=rest.len().min(4));
        store.round(&rest[..size], broken);
        rest = &rest[size..];
        crossed |= store.crossed_pairs() > 0;
        if rng.random_bool(0.2) {
            let open: Vec<LineageId> = store.open_ids().into_iter().collect();
            if let Some(id) = open.choose(rng) {
                store.finished.insert(id.clone());
            }
        }
    }
    (store, crossed)
}

/// Random runs; returns each failing seed with why it failed: a round left
/// a crossed pair open, a round broke the decision's contract, or the end
/// differs from the replay.
fn failures(runs: u64, broken: Option<Break>) -> Vec<(u64, &'static str)> {
    let mut failed = Vec::new();
    for seed in 0..runs {
        let mut rng = StdRng::seed_from_u64(seed);
        let generated = commands(&mut rng);
        let commands = arrival(&mut rng, generated);
        let (store, crossed) = run(&mut rng, &commands, broken);
        if crossed {
            failed.push((seed, "crossed"));
        } else if store.broken_contracts > 0 {
            failed.push((seed, "contract"));
        } else if !store.differences().is_empty() {
            failed.push((seed, "differs"));
        }
    }
    failed
}

#[test]
fn random_rounds_equal_a_replay_in_seq_order() {
    assert_eq!(failures(20_000, None).first(), None);
}

#[test]
fn every_break_of_the_rule_or_the_round_is_caught() {
    for broken in [
        Break::NewerOnlyWhileOpen,
        Break::DeadSubmitCancelsNothing,
        Break::LoserCancelsNothing,
        Break::StrictlyCrossed,
        Break::ReadBeforeClaims,
        Break::NewerAboveHighestClaim,
    ] {
        assert!(
            !failures(5_000, Some(broken)).is_empty(),
            "{broken:?} was not caught"
        );
    }
}

/// Every order of arrival and every split into rounds of `commands`, with
/// nothing finishing: the stops always equal the replay, no round leaves a
/// crossed pair, and the open set is the same every time.
fn every_arrangement(commands: &[Cmd]) -> BTreeSet<LineageId> {
    fn permutations(items: &[Cmd]) -> Vec<Vec<Cmd>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut all = Vec::new();
        for index in 0..items.len() {
            let mut rest = items.to_vec();
            let first = rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, first.clone());
                all.push(tail);
            }
        }
        all
    }
    let mut open_sets: BTreeSet<BTreeSet<LineageId>> = BTreeSet::new();
    for order in permutations(commands) {
        for splits in 0..1u32 << (order.len() - 1) {
            let mut store = Store::default();
            let mut round = Vec::new();
            for (index, cmd) in order.iter().enumerate() {
                round.push(cmd.clone());
                if index + 1 == order.len() || splits & (1 << index) != 0 {
                    store.round(&round, None);
                    round.clear();
                    assert_eq!(store.crossed_pairs(), 0, "{order:?} split {splits:b}");
                }
            }
            assert_eq!(
                store.differences(),
                Vec::<LineageId>::new(),
                "{order:?} split {splits:b}"
            );
            assert_eq!(store.broken_contracts, 0, "{order:?} split {splits:b}");
            open_sets.insert(store.open_ids());
        }
    }
    assert_eq!(
        open_sets.len(),
        1,
        "the result depends on arrival: {open_sets:?}"
    );
    open_sets.pop_first().unwrap()
}

fn ids(lineages: &[&Lineage]) -> BTreeSet<LineageId> {
    lineages.iter().map(|l| l.lineage_id.clone()).collect()
}

/// The cases ADR 0009's rule and tables describe, each in every arrival
/// order and every split into rounds.
#[test]
fn the_documented_cases_hold_in_every_arrangement() {
    let ask = quote(1, 0, 10, true, 100);
    let bid = quote(1, 0, 11, false, 101);
    // An older ask and a newer crossing bid, in either order or one round:
    // only the bid stays.
    assert_eq!(
        every_arrangement(&[Cmd::Submit(ask.clone()), Cmd::Submit(bid.clone())]),
        ids(&[&bid])
    );
    // The newer bid, cancelled by its maker, still cancels the ask.
    let cancel_n = Cmd::CancelOrder {
        maker_id: 1,
        seq: 12,
        target: bid.lineage_id.clone(),
    };
    assert_eq!(
        every_arrangement(&[Cmd::Submit(ask.clone()), Cmd::Submit(bid.clone()), cancel_n]),
        ids(&[])
    );
    // A chain, ask 100 (10), bid 101 (11), ask 100.5 (12): only the last
    // ask stays.
    let last_ask = quote(1, 0, 12, true, 100);
    let last_ask = Lineage {
        offered: 200,
        requested: 201,
        ..last_ask
    };
    assert_eq!(
        every_arrangement(&[
            Cmd::Submit(ask.clone()),
            Cmd::Submit(bid.clone()),
            Cmd::Submit(last_ask.clone()),
        ]),
        ids(&[&last_ask])
    );
    // A submit that crosses several older orders cancels them all.
    let ask2 = quote(1, 0, 8, true, 99);
    assert_eq!(
        every_arrangement(&[
            Cmd::Submit(ask2.clone()),
            Cmd::Submit(ask.clone()),
            Cmd::Submit(bid.clone()),
        ]),
        ids(&[&bid])
    );
    // A submit that crosses an older and a newer order: it and the older
    // one go.
    let newer_ask = quote(1, 0, 13, true, 100);
    assert_eq!(
        every_arrangement(&[
            Cmd::Submit(ask.clone()),
            Cmd::Submit(bid.clone()),
            Cmd::Submit(newer_ask.clone()),
        ]),
        ids(&[&newer_ask])
    );
    // A cancel-all of sells covers the late ask, which still cancels the
    // older crossing bid; a sell above the cancel-all's seq stays.
    let old_bid = quote(1, 0, 9, false, 100);
    let cancel_all = Cmd::CancelAll {
        maker_id: 1,
        seq: 11,
        market: None,
        sells: Some(true),
    };
    let fresh = quote(1, 0, 12, true, 110);
    assert_eq!(
        every_arrangement(&[
            Cmd::Submit(old_bid.clone()),
            Cmd::Submit(ask.clone()),
            cancel_all,
            Cmd::Submit(fresh.clone()),
        ]),
        ids(&[&fresh])
    );
    // Equal prices cross; the same side, another pair or another maker
    // never do.
    let same_side = quote(1, 0, 11, true, 90);
    let other_pair = quote(1, 1, 12, false, 120);
    let other_maker = quote(2, 0, 13, false, 120);
    let equal = quote(1, 0, 14, false, 100);
    assert_eq!(
        every_arrangement(&[
            Cmd::Submit(ask.clone()),
            Cmd::Submit(same_side.clone()),
            Cmd::Submit(other_pair.clone()),
            Cmd::Submit(other_maker.clone()),
        ]),
        ids(&[&ask, &same_side, &other_pair, &other_maker])
    );
    assert_eq!(
        every_arrangement(&[Cmd::Submit(ask.clone()), Cmd::Submit(equal.clone())]),
        ids(&[&equal])
    );
}

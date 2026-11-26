//! Self-trade prevention at submit (ADR 0009): which of a maker's crossing
//! orders are cancelled. Pure, with no database: an intake round reads the
//! maker's orders, [`decide`] picks the losers, the round writes them.
//!
//! The rule: for one maker on one pair, when a bid and an ask cross at the
//! prices the maker submitted (bid ≥ ask), the one with the lower seq is
//! cancelled if it is open. The higher one counts in any state. Which orders
//! are cancelled is what a replay of the maker's commands in seq order
//! gives, whatever order they arrive in and however they are split into
//! rounds; which newer order a stop names may differ (ADR 0009, "Limits",
//! 8).

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::maker::{LineageId, MakerId};

/// One of a maker's orders as the check sees it: the lineage it claimed and
/// the amounts it was submitted at. A remainder keeps both, so it is never
/// compared again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lineage {
    pub lineage_id: LineageId,
    pub maker_id: MakerId,
    pub market: Vec<u8>,
    pub direction: Vec<u8>,
    pub root_seq: u64,
    pub offered: u64,
    pub requested: u64,
}

impl Lineage {
    /// Whether two orders of one maker cross: opposite directions of one
    /// pair, at prices where the bid is at or above the ask. With X offering
    /// `ox` for `rx` and Y offering `oy` for `ry` the other way, that is
    /// `ox * oy >= rx * ry`; no code needs to know which side is the bid.
    pub fn crosses(&self, other: &Lineage) -> bool {
        self.maker_id == other.maker_id
            && self.market == other.market
            && self.direction != other.direction
            && u128::from(self.offered) * u128::from(other.offered)
                >= u128::from(self.requested) * u128::from(other.requested)
    }
}

/// A lineage the check cancels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loss {
    pub maker_id: MakerId,
    /// The newer crossing order that cancelled it: if several did, the
    /// first (lowest seq) of those this round read. One that arrives later
    /// does not change it (ADR 0009, "Limits", 8).
    pub caused_by: LineageId,
}

/// What one round decided.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Decision {
    /// Every lineage to cancel, with the order that cancelled it.
    pub losers: BTreeMap<LineageId, Loss>,
    /// The claimed submits that are open and did not lose, in arrival
    /// order: the ones that may go live.
    pub winners: Vec<LineageId>,
}

/// Decide one round. `claimed` holds the round's newly claimed submits in
/// arrival order (not a replay, an `AlreadyRegistered` or a conflict).
/// `known` holds the lineages read for each (maker, pair) the claims name:
/// the open ones and those newer than that group's lowest claimed seq, in
/// any state. `known` may include the claims; the claimed copy is used.
/// `open` names the lineages that are open: claimed or live and neither
/// stopped nor cut off by their maker, nor finished. A claimed submit its
/// maker had already cancelled is not open: it never goes live, but still
/// cancels its older crossing orders.
///
/// For each claimed submit E and each crossing lineage C of its maker:
/// whichever has the lower seq is cancelled, if it is open. Two lineages
/// both in `known` are never compared: an earlier round decided them, or
/// the switch was off then. Seqs are unique per maker, so the two never tie.
pub fn decide(claimed: &[Lineage], known: &[Lineage], open: &HashSet<LineageId>) -> Decision {
    let mut by_id: HashMap<&LineageId, &Lineage> = HashMap::new();
    for lineage in known.iter().chain(claimed) {
        by_id.insert(&lineage.lineage_id, lineage);
    }
    // Each loser, and the lowest-seq newer crossing order that beat it.
    let mut causes: BTreeMap<&LineageId, &Lineage> = BTreeMap::new();
    for entering in claimed {
        // A lineage never crosses itself: it has one direction.
        for other in by_id.values().filter(|other| entering.crosses(other)) {
            let (loser, winner) = if other.root_seq < entering.root_seq {
                (*other, entering)
            } else {
                (entering, *other)
            };
            if !open.contains(&loser.lineage_id) {
                continue;
            }
            causes
                .entry(&loser.lineage_id)
                .and_modify(|cause| {
                    if winner.root_seq < cause.root_seq {
                        *cause = winner;
                    }
                })
                .or_insert(winner);
        }
    }
    // A loser and its cause cross, so they share a maker.
    let losers: BTreeMap<LineageId, Loss> = causes
        .into_iter()
        .map(|(loser, cause)| {
            let loss = Loss {
                maker_id: cause.maker_id,
                caused_by: cause.lineage_id.clone(),
            };
            (loser.clone(), loss)
        })
        .collect();
    let winners = claimed
        .iter()
        .filter(|lineage| open.contains(&lineage.lineage_id))
        .filter(|lineage| !losers.contains_key(&lineage.lineage_id))
        .map(|lineage| lineage.lineage_id.clone())
        .collect();
    Decision { losers, winners }
}

#[cfg(test)]
mod tests;

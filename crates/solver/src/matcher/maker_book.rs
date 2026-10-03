//! Maker attribution of book entries and the cancellation barriers they obey
//! (ADR 0003).
//!
//! An optimization, not the guarantee: reservation re-checks every input
//! against `live_orders` under the maker control lock. This keeps the matcher
//! from spending proofs on orders a maker has cancelled, and keeps maker
//! orders away from the RFQ router.

use std::collections::{HashMap, HashSet};

use miden_protocol::note::NoteId;

use crate::maker::{CutoffScope, LineageId, MakerFact, MakerId, MakerTag};
use crate::types::{BookOrder, OrderKeys};

/// How long a fact is kept for book updates that arrive after it. A book
/// update committed before the fact can still be queued behind it on
/// `book_tx`, which the matcher drains every tick; a minute is far longer.
const RECENT_FACT_TTL_MS: u64 = 60_000;

struct Entry {
    lineage_id: LineageId,
    market: Vec<u8>,
    direction: Vec<u8>,
    tag: Option<MakerTag>,
}

/// Facts kept for book updates delivered after them.
struct Recent {
    tag: Option<MakerTag>,
    stopped_by: HashSet<MakerId>,
    at_ms: u64,
}

#[derive(Default)]
pub(crate) struct MakerBook {
    entries: HashMap<NoteId, Entry>,
    by_lineage: HashMap<LineageId, HashSet<NoteId>>,
    by_maker: HashMap<MakerId, HashSet<NoteId>>,
    /// Every cutoff (hydrated at startup): few per maker, kept forever.
    cutoffs: HashMap<MakerId, HashMap<CutoffScope, u64>>,
    recent: HashMap<LineageId, Recent>,
}

impl MakerBook {
    /// Record a book entry. `false` when a cutoff or stop bars it, so it must
    /// stay out of the book. An entry whose keys cannot be computed is not a
    /// maker order (the book rejects such a note anyway).
    pub(super) fn admit(&mut self, order: &BookOrder) -> bool {
        let id = order.id();
        self.forget(id);
        let Ok(keys) = OrderKeys::from_note(&order.note) else {
            return true;
        };
        let recent = self.recent.get(&keys.lineage_id);
        let tag = order.maker.or_else(|| recent.and_then(|recent| recent.tag));
        if let Some(tag) = tag {
            let stopped = recent.is_some_and(|recent| recent.stopped_by.contains(&tag.maker_id));
            if stopped || self.below_cutoff(tag, &keys.market, &keys.direction) {
                return false;
            }
            self.by_maker.entry(tag.maker_id).or_default().insert(id);
        }
        self.by_lineage
            .entry(keys.lineage_id.clone())
            .or_default()
            .insert(id);
        self.entries.insert(
            id,
            Entry {
                lineage_id: keys.lineage_id,
                market: keys.market,
                direction: keys.direction,
                tag,
            },
        );
        true
    }

    pub(super) fn forget(&mut self, id: NoteId) {
        let Some(entry) = self.entries.remove(&id) else {
            return;
        };
        remove_index(&mut self.by_lineage, &entry.lineage_id, id);
        if let Some(tag) = entry.tag {
            remove_index(&mut self.by_maker, &tag.maker_id, id);
        }
    }

    /// Whether the RFQ router must skip this entry.
    pub(super) fn is_maker_order(&self, id: &NoteId) -> bool {
        self.entries
            .get(id)
            .is_some_and(|entry| entry.tag.is_some())
    }

    pub(super) fn raise_cutoff(&mut self, maker_id: MakerId, scope: CutoffScope, cutoff: u64) {
        let barrier = self
            .cutoffs
            .entry(maker_id)
            .or_default()
            .entry(scope)
            .or_default();
        *barrier = (*barrier).max(cutoff);
    }

    /// Apply a fact from the maker control lane; returns the entries it
    /// stops, which the caller removes from the book.
    pub(super) fn apply(&mut self, fact: MakerFact, now_ms: u64) -> Vec<NoteId> {
        match fact {
            MakerFact::CutoffRaised {
                maker_id,
                scope,
                cutoff,
            } => {
                self.raise_cutoff(maker_id, scope, cutoff);
                self.entries_of_maker(maker_id)
                    .filter(|(_, entry, tag)| {
                        self.below_cutoff(*tag, &entry.market, &entry.direction)
                    })
                    .map(|(id, ..)| id)
                    .collect()
            }
            MakerFact::LineageStopped {
                maker_id,
                lineage_id,
            } => {
                let stopped = self
                    .entries_of_lineage(&lineage_id)
                    .filter(|(_, entry)| entry.tag.is_some_and(|tag| tag.maker_id == maker_id))
                    .map(|(id, _)| id)
                    .collect();
                self.remember(lineage_id, now_ms)
                    .stopped_by
                    .insert(maker_id);
                stopped
            }
            MakerFact::LineageAttributed { lineage_id, tag } => {
                // Tag entries that arrived first (a public note ingested
                // before its maker submitted it).
                let ids: Vec<NoteId> = self
                    .entries_of_lineage(&lineage_id)
                    .filter(|(_, entry)| entry.tag.is_none())
                    .map(|(id, _)| id)
                    .collect();
                let mut stopped = Vec::new();
                for id in ids {
                    let Some(entry) = self.entries.get_mut(&id) else {
                        continue;
                    };
                    entry.tag = Some(tag);
                    self.by_maker.entry(tag.maker_id).or_default().insert(id);
                    let entry = &self.entries[&id];
                    if self.below_cutoff(tag, &entry.market, &entry.direction) {
                        stopped.push(id);
                    }
                }
                self.remember(lineage_id, now_ms).tag = Some(tag);
                stopped
            }
        }
    }

    /// Drop facts old enough that no earlier book update can still arrive.
    pub(super) fn expire(&mut self, now_ms: u64) {
        self.recent
            .retain(|_, recent| now_ms.saturating_sub(recent.at_ms) < RECENT_FACT_TTL_MS);
    }

    fn remember(&mut self, lineage_id: LineageId, now_ms: u64) -> &mut Recent {
        let recent = self.recent.entry(lineage_id).or_insert_with(|| Recent {
            tag: None,
            stopped_by: HashSet::new(),
            at_ms: now_ms,
        });
        recent.at_ms = now_ms;
        recent
    }

    fn below_cutoff(&self, tag: MakerTag, market: &[u8], direction: &[u8]) -> bool {
        self.cutoffs.get(&tag.maker_id).is_some_and(|cutoffs| {
            cutoffs
                .iter()
                .any(|(scope, &cutoff)| tag.root_seq < cutoff && scope.covers(market, direction))
        })
    }

    fn entries_of_maker(
        &self,
        maker_id: MakerId,
    ) -> impl Iterator<Item = (NoteId, &Entry, MakerTag)> + '_ {
        self.by_maker
            .get(&maker_id)
            .into_iter()
            .flatten()
            .filter_map(|id| {
                let entry = self.entries.get(id)?;
                Some((*id, entry, entry.tag?))
            })
    }

    fn entries_of_lineage<'a>(
        &'a self,
        lineage_id: &LineageId,
    ) -> impl Iterator<Item = (NoteId, &'a Entry)> + 'a {
        self.by_lineage
            .get(lineage_id)
            .into_iter()
            .flatten()
            .filter_map(|id| Some((*id, self.entries.get(id)?)))
    }
}

fn remove_index<K: std::hash::Hash + Eq>(
    index: &mut HashMap<K, HashSet<NoteId>>,
    key: &K,
    id: NoteId,
) {
    if let Some(ids) = index.get_mut(key) {
        ids.remove(&id);
        if ids.is_empty() {
            index.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TokenId;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use std::sync::Arc;

    const ALPHA: MakerId = 1;
    const BETA: MakerId = 2;

    fn tokens() -> (TokenId, TokenId) {
        (
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
        )
    }

    fn order(serial: u32, sell_x: bool, maker: Option<(MakerId, u64)>) -> BookOrder {
        let (x, y) = tokens();
        let (offered, requested) = if sell_x { (x, y) } else { (y, x) };
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let note: Note = PswapNote::builder()
            .sender(creator)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 10).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .serial_number(Word::from([serial, 0, 0, 0]))
            .note_type(NoteType::Public)
            .offered_asset(FungibleAsset::new(offered, 10).unwrap())
            .build()
            .unwrap()
            .into();
        BookOrder {
            priority_seq: u64::from(serial),
            arrival_unix: 1,
            note: Arc::new(note),
            maker: maker.map(|(maker_id, root_seq)| MakerTag { maker_id, root_seq }),
        }
    }

    fn lineage(order: &BookOrder) -> LineageId {
        OrderKeys::from_note(&order.note).unwrap().lineage_id
    }

    fn cutoff(maker_id: MakerId, scope: CutoffScope, cutoff: u64) -> MakerFact {
        MakerFact::CutoffRaised {
            maker_id,
            scope,
            cutoff,
        }
    }

    #[test]
    fn a_cutoff_stops_its_makers_older_orders_in_scope_only() {
        let (x, y) = tokens();
        let mut book = MakerBook::default();
        let old = order(1, true, Some((ALPHA, 5)));
        let new = order(2, true, Some((ALPHA, 20)));
        let other_side = order(3, false, Some((ALPHA, 5)));
        let other_maker = order(4, true, Some((BETA, 5)));
        let public = order(5, true, None);
        for order in [&old, &new, &other_side, &other_maker, &public] {
            assert!(book.admit(order));
        }
        let stopped = book.apply(cutoff(ALPHA, CutoffScope::direction(x, y), 10), 0);
        assert_eq!(stopped, vec![old.id()]);
        let stopped = book.apply(cutoff(ALPHA, CutoffScope::market(x, y), 10), 0);
        assert_eq!(stopped.len(), 2, "the market scope adds the other side");
        assert!(stopped.contains(&other_side.id()));
        // A later Active update below the barrier stays out.
        assert!(!book.admit(&order(6, false, Some((ALPHA, 9)))));
        assert!(book.admit(&order(7, false, Some((ALPHA, 10)))));
    }

    #[test]
    fn a_stop_binds_its_lineage_and_late_updates_for_a_while() {
        let mut book = MakerBook::default();
        let quote = order(1, true, Some((ALPHA, 5)));
        assert!(book.admit(&quote));
        let stop = |maker_id| MakerFact::LineageStopped {
            maker_id,
            lineage_id: lineage(&quote),
        };
        assert!(
            book.apply(stop(BETA), 0).is_empty(),
            "only its owner can stop it"
        );
        assert_eq!(book.apply(stop(ALPHA), 0), vec![quote.id()]);
        book.forget(quote.id());
        // An Active update committed before the stop but delivered after it.
        assert!(!book.admit(&quote));
        book.expire(RECENT_FACT_TTL_MS);
        assert!(book.admit(&quote), "reservation still re-checks it");
    }

    #[test]
    fn an_attribution_tags_entries_that_arrived_first() {
        let (x, y) = tokens();
        let mut book = MakerBook::default();
        let early = order(1, true, None);
        assert!(book.admit(&early));
        assert!(!book.is_maker_order(&early.id()));
        let attribute = |root_seq| MakerFact::LineageAttributed {
            lineage_id: lineage(&early),
            tag: MakerTag {
                maker_id: ALPHA,
                root_seq,
            },
        };
        assert!(book.apply(attribute(5), 0).is_empty());
        assert!(book.is_maker_order(&early.id()), "never routed from now on");

        // An untagged copy delivered after the attribution is tagged too.
        let late = order(1, true, None);
        book.forget(early.id());
        assert!(book.admit(&late));
        assert!(book.is_maker_order(&late.id()));

        // A delayed submit below an existing cutoff is dropped at once.
        let mut other = MakerBook::default();
        other.raise_cutoff(ALPHA, CutoffScope::market(x, y), 10);
        assert!(other.admit(&early));
        assert_eq!(other.apply(attribute(5), 0), vec![early.id()]);
    }

    #[test]
    fn cutoffs_only_rise() {
        let mut book = MakerBook::default();
        book.raise_cutoff(ALPHA, CutoffScope::all(), 10);
        book.raise_cutoff(ALPHA, CutoffScope::all(), 8);
        assert!(!book.admit(&order(1, true, Some((ALPHA, 9)))));
    }
}

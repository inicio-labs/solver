//! Maker attribution of book entries and the cancellation barriers they obey
//! (ADR 0003).
//!
//! An optimization, not the guarantee: reservation re-checks every input
//! against `live_orders` under the maker control lock. This keeps the matcher
//! from spending proofs on orders a maker has cancelled, and keeps maker
//! orders away from the RFQ router.

use std::collections::{HashMap, HashSet};

use miden_protocol::note::NoteId;
use miden_standards::note::PswapNote;

use crate::maker::{CutoffScope, LineageId, MakerId, MakerTag, MakerUpdate};
use crate::types::{BookOrder, OrderKeys, TokenId};

struct Entry {
    lineage_id: LineageId,
    offered: TokenId,
    requested: TokenId,
    tag: Option<MakerTag>,
}

#[derive(Default)]
pub(crate) struct MakerIndex {
    entries: HashMap<NoteId, Entry>,
    by_lineage: HashMap<LineageId, HashSet<NoteId>>,
    by_maker: HashMap<MakerId, HashSet<NoteId>>,
    /// Every cutoff (hydrated at startup): few per maker, kept forever.
    cutoffs: HashMap<MakerId, HashMap<CutoffScope, u64>>,
}

impl MakerIndex {
    /// Record a book entry. `false` when a cutoff or stop bars it, so it must
    /// stay out of the book. An entry whose keys cannot be computed is not a
    /// maker order (the book rejects such a note anyway).
    pub(super) fn admit(&mut self, order: &BookOrder) -> bool {
        let id = order.id();
        self.forget(id);
        let Ok(pswap) = PswapNote::try_from(order.note.as_ref()) else {
            return true;
        };
        let lineage_id = OrderKeys::lineage_id_from_pswap(&pswap);
        let offered = pswap.offered_asset().faucet_id();
        let requested = pswap.storage().requested_faucet_id();
        let tag = order.maker;
        if let Some(tag) = tag {
            if self.below_cutoff(tag, offered, requested) {
                return false;
            }
            self.by_maker.entry(tag.maker_id).or_default().insert(id);
        }
        self.by_lineage
            .entry(lineage_id.clone())
            .or_default()
            .insert(id);
        self.entries.insert(
            id,
            Entry {
                lineage_id,
                offered,
                requested,
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

    /// Maker metadata to copy onto the single order model after admission.
    pub(super) fn tag(&self, id: &NoteId) -> Option<MakerTag> {
        self.entries.get(id).and_then(|entry| entry.tag)
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

    /// Apply a committed maker update; return entries to remove from the book.
    pub(super) fn apply(&mut self, update: MakerUpdate) -> Vec<NoteId> {
        match update {
            MakerUpdate::CutoffRaised {
                maker_id,
                scope,
                cutoff,
            } => {
                self.raise_cutoff(maker_id, scope, cutoff);
                self.entries_of_maker(maker_id)
                    .filter(|(_, entry, tag)| {
                        self.below_cutoff(*tag, entry.offered, entry.requested)
                    })
                    .map(|(id, ..)| id)
                    .collect()
            }
            MakerUpdate::LineageCancelled {
                maker_id,
                lineage_id,
            } => {
                let cancelled = self
                    .entries_of_lineage(&lineage_id)
                    .filter(|(_, entry)| entry.tag.is_some_and(|tag| tag.maker_id == maker_id))
                    .map(|(id, _)| id)
                    .collect();
                cancelled
            }
            MakerUpdate::OrdersAttributed {
                order_ids,
                tag,
                cancelled,
            } => {
                let mut cancelled_ids = Vec::new();
                for id in order_ids {
                    let Some(entry) = self.entries.get_mut(&id) else {
                        continue;
                    };
                    if entry.tag.is_some() {
                        continue;
                    }
                    entry.tag = Some(tag);
                    self.by_maker.entry(tag.maker_id).or_default().insert(id);
                    let entry = &self.entries[&id];
                    if cancelled || self.below_cutoff(tag, entry.offered, entry.requested) {
                        cancelled_ids.push(id);
                    }
                }
                cancelled_ids
            }
        }
    }

    fn below_cutoff(&self, tag: MakerTag, offered: TokenId, requested: TokenId) -> bool {
        self.cutoffs.get(&tag.maker_id).is_some_and(|cutoffs| {
            cutoffs.iter().any(|(scope, &cutoff)| {
                tag.root_seq < cutoff && scope.covers_pair(offered, requested)
            })
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

    fn cutoff(maker_id: MakerId, scope: CutoffScope, cutoff: u64) -> MakerUpdate {
        MakerUpdate::CutoffRaised {
            maker_id,
            scope,
            cutoff,
        }
    }

    #[test]
    fn a_cutoff_stops_its_makers_older_orders_in_scope_only() {
        let (x, y) = tokens();
        let mut book = MakerIndex::default();
        let old = order(1, true, Some((ALPHA, 5)));
        let new = order(2, true, Some((ALPHA, 20)));
        let other_side = order(3, false, Some((ALPHA, 5)));
        let other_maker = order(4, true, Some((BETA, 5)));
        let public = order(5, true, None);
        for order in [&old, &new, &other_side, &other_maker, &public] {
            assert!(book.admit(order));
        }
        let cancelled = book.apply(cutoff(ALPHA, CutoffScope::direction(x, y), 10));
        assert_eq!(cancelled, vec![old.id()]);
        let cancelled = book.apply(cutoff(ALPHA, CutoffScope::market(x, y), 10));
        assert_eq!(cancelled.len(), 2, "the market scope adds the other side");
        assert!(cancelled.contains(&other_side.id()));
        // A later Active update below the barrier stays out.
        assert!(!book.admit(&order(6, false, Some((ALPHA, 9)))));
        assert!(book.admit(&order(7, false, Some((ALPHA, 10)))));
    }

    #[test]
    fn a_stop_removes_owned_entries() {
        let mut book = MakerIndex::default();
        let quote = order(1, true, Some((ALPHA, 5)));
        assert!(book.admit(&quote));
        let stop = |maker_id| MakerUpdate::LineageCancelled {
            maker_id,
            lineage_id: lineage(&quote),
        };
        assert!(
            book.apply(stop(BETA)).is_empty(),
            "only its owner can stop it"
        );
        assert_eq!(book.apply(stop(ALPHA)), vec![quote.id()]);
        book.forget(quote.id());
        // The ordered update stream delivers earlier activations before this
        // cancellation; later activations have already passed live_orders.
    }

    #[test]
    fn an_attribution_tags_entries_that_arrived_first() {
        let (x, y) = tokens();
        let mut book = MakerIndex::default();
        let early = order(1, true, None);
        assert!(book.admit(&early));
        assert!(book.tag(&early.id()).is_none());
        let attribute = |root_seq| MakerUpdate::OrdersAttributed {
            order_ids: vec![early.id()],
            tag: MakerTag {
                maker_id: ALPHA,
                root_seq,
            },
            cancelled: false,
        };
        assert!(book.apply(attribute(5)).is_empty());
        assert!(book.tag(&early.id()).is_some(), "never routed from now on");

        // A delayed submit below an existing cutoff is dropped at once.
        let mut other = MakerIndex::default();
        other.raise_cutoff(ALPHA, CutoffScope::market(x, y), 10);
        assert!(other.admit(&early));
        assert_eq!(other.apply(attribute(5)), vec![early.id()]);

        // A cancellation committed before the claim is carried by that
        // claim, so an already-booked public order is removed at attribution.
        let mut cancelled = MakerIndex::default();
        assert!(cancelled.admit(&early));
        assert_eq!(
            cancelled.apply(MakerUpdate::OrdersAttributed {
                order_ids: vec![early.id()],
                tag: MakerTag {
                    maker_id: ALPHA,
                    root_seq: 5
                },
                cancelled: true,
            },),
            vec![early.id()]
        );
    }

    #[test]
    fn cutoffs_only_rise() {
        let mut book = MakerIndex::default();
        book.raise_cutoff(ALPHA, CutoffScope::all(), 10);
        book.raise_cutoff(ALPHA, CutoffScope::all(), 8);
        assert!(!book.admit(&order(1, true, Some((ALPHA, 9)))));
    }
}

//! The matcher's book as order-by-order batches, and the copy built from
//! them.
//!
//! As orders enter and leave its matchable index, the matcher only notes
//! their IDs. When a step is finished (a committed book update applied, a
//! tick ended, the startup load done) it sends one [`BookBatch`]: the current
//! state of every order touched since the last batch, live with its fields or
//! gone, and a check value (digest) for each pair touched. A batch is a state
//! the matcher really had, never half of one.
//!
//! [`BookCopy`] is the receiving side's copy: today the price API's depth
//! mirror, later the quote host. The matcher keeps one too (without price
//! levels), so both sides compute the digest with the same code.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use miden_protocol::note::NoteId;
use tokio::sync::mpsc;

use crate::clearing::Order;
use crate::matching::types::{Amount, BookLevel, RateKey, SwapBookSnapshot};
use crate::types::TokenId;

/// A directed pair: what an order offers, what it requests.
pub(crate) type Pair = (TokenId, TokenId);

/// What clearing reads from a live order: its pair, its price and amounts
/// (`rate`), its place in the queue at that price, and its smallest fill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BookEntry {
    pub(crate) id: NoteId,
    pub(crate) pair: Pair,
    /// `requested` / `offered`: the order's own amounts; lower is better.
    pub(crate) rate: RateKey,
    pub(crate) priority: u64,
    pub(crate) min_fill_step: u64,
}

impl BookEntry {
    /// The order's fields, read the way the matcher indexes it.
    pub(crate) fn of(order: &Order) -> Self {
        let (pair, key) = order.index_key();
        Self {
            id: order.id(),
            pair,
            rate: key.rate,
            priority: order.priority_sequence(),
            min_fill_step: order.pswap_note().storage().min_fill_step().as_u64(),
        }
    }

    /// The order's share of its pair's digest. Stable across builds: the
    /// first 16 bytes of a blake3 hash over every field's fixed-size bytes
    /// (note ID, both faucet IDs, then the four numbers little-endian).
    fn hash(&self) -> u128 {
        const LEN: usize = 32 + 15 + 15 + 4 * 8;
        let mut bytes = [0u8; LEN];
        let numbers = [
            self.rate.requested,
            self.rate.offered,
            self.priority,
            self.min_fill_step,
        ];
        bytes[..32].copy_from_slice(&self.id.as_bytes());
        bytes[32..47].copy_from_slice(&<[u8; 15]>::from(self.pair.0));
        bytes[47..62].copy_from_slice(&<[u8; 15]>::from(self.pair.1));
        for (i, number) in numbers.iter().enumerate() {
            let at = 62 + 8 * i;
            bytes[at..at + 8].copy_from_slice(&number.to_le_bytes());
        }
        let hash = blake3::hash(&bytes);
        let mut first = [0u8; 16];
        first.copy_from_slice(&hash.as_bytes()[..16]);
        u128::from_le_bytes(first)
    }
}

/// One touched order's state at the end of a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderState {
    /// In the matchable index now, with these fields.
    Live(BookEntry),
    /// Was in it during the step and is not at its end (filled, picked for
    /// a settlement, cancelled, expired). An order the index never held is
    /// never sent.
    Gone(NoteId),
}

/// Everything one finished step changed in the matcher's book.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BookBatch {
    pub(crate) orders: Vec<OrderState>,
    /// The digest of every pair the step touched, as the matcher holds it.
    pub(crate) digests: Vec<(Pair, u128)>,
}

/// Order states the matcher has sent and the receiving copy has applied
/// (published once its quotes can see them); their difference is the copy's
/// backlog.
static SENT: AtomicU64 = AtomicU64::new(0);
static APPLIED: AtomicU64 = AtomicU64::new(0);

/// Order states sent and not yet applied, read at scrape time: up to one
/// round's worth (the price API's mirror applies them every 250 ms) while the
/// copy keeps up, growing while it falls behind, stalls or has stopped.
pub(crate) fn backlog() -> u64 {
    // Applied first: it never runs ahead of sent, so the later read of sent
    // is at least as large.
    let applied = APPLIED.load(Ordering::Relaxed);
    SENT.load(Ordering::Relaxed).saturating_sub(applied)
}

/// Batches after which at least one pair's digest differed from the
/// matcher's. Any is a bug: the in-process channel loses nothing. Nothing
/// repairs the copy, so a pair out of step counts again with every batch
/// that touches it (logged once, when it goes out of step).
static DIGEST_MISMATCHES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn digest_mismatches() -> u64 {
    DIGEST_MISMATCHES.load(Ordering::Relaxed)
}

/// What applying batches did: the pairs they touched, and how many batches
/// left a pair with a digest other than the matcher's (each one a bug).
#[derive(Debug, Default)]
pub(crate) struct Applied {
    pub(crate) touched: HashSet<Pair>,
    pub(crate) mismatched: usize,
}

impl Applied {
    fn merge(&mut self, other: Applied) {
        self.touched.extend(other.touched);
        self.mismatched += other.mismatched;
    }
}

/// A copy of the matcher's live orders, kept up to date by [`BookBatch`]es:
/// every live order, each pair's digest, and (when asked for) each pair's
/// price levels with their total offered volume.
#[derive(Debug, Default)]
pub(crate) struct BookCopy {
    /// Each live order with its hash, so removing it costs no rehash.
    entries: HashMap<NoteId, (BookEntry, u128)>,
    /// Each held pair's order count and digest (the wrapping sum of its
    /// orders' hashes); a pair leaves when its last order does.
    pairs: HashMap<Pair, (usize, u128)>,
    /// `None` on the matcher's side, which needs no levels.
    levels: Option<HashMap<Pair, BTreeMap<RateKey, u128>>>,
    applied: u64,
    /// Pairs whose digest differed after the last batch touching them, so
    /// each goes out of step (and back) in the log once.
    out_of_step: HashSet<Pair>,
}

impl BookCopy {
    /// A copy that also keeps price levels, for quoting.
    pub(crate) fn with_levels() -> Self {
        Self {
            levels: Some(HashMap::new()),
            ..Self::default()
        }
    }

    pub(crate) fn get(&self, id: &NoteId) -> Option<&BookEntry> {
        self.entries.get(id).map(|(entry, _)| entry)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The digest of `pair` (0 when it holds no order).
    pub(crate) fn digest(&self, pair: Pair) -> u128 {
        self.pairs.get(&pair).map_or(0, |(_, digest)| *digest)
    }

    /// Hold `entry`, replacing an older one with its ID. The pairs touched.
    pub(crate) fn insert(&mut self, entry: BookEntry) -> [Option<Pair>; 2] {
        let old = self.remove(&entry.id);
        let hash = entry.hash();
        let (count, digest) = self.pairs.entry(entry.pair).or_default();
        *count += 1;
        *digest = digest.wrapping_add(hash);
        if let Some(levels) = &mut self.levels {
            *levels
                .entry(entry.pair)
                .or_default()
                .entry(entry.rate)
                .or_default() += u128::from(entry.rate.offered);
        }
        self.entries.insert(entry.id, (entry, hash));
        [old, Some(entry.pair)]
    }

    /// Drop the entry with `id`, if held. Its pair.
    pub(crate) fn remove(&mut self, id: &NoteId) -> Option<Pair> {
        let (entry, hash) = self.entries.remove(id)?;
        if let Some((count, digest)) = self.pairs.get_mut(&entry.pair) {
            *count -= 1;
            *digest = digest.wrapping_sub(hash);
            if *count == 0 {
                self.pairs.remove(&entry.pair);
            }
        }
        if let Some(levels) = &mut self.levels {
            if let Some(pair_levels) = levels.get_mut(&entry.pair) {
                if let Some(held) = pair_levels.get_mut(&entry.rate) {
                    *held = held.saturating_sub(u128::from(entry.rate.offered));
                    if *held == 0 {
                        pair_levels.remove(&entry.rate);
                    }
                }
                if pair_levels.is_empty() {
                    levels.remove(&entry.pair);
                }
            }
        }
        Some(entry.pair)
    }

    /// Apply `batch`. A pair whose digest then differs from the batch's, or
    /// that the batch changed without giving a digest for, is out of step:
    /// the batch is counted and reported, and the pair logged when it goes
    /// out of step and when it is back.
    pub(crate) fn apply(&mut self, batch: &BookBatch) -> Applied {
        let mut touched = HashSet::new();
        for state in &batch.orders {
            match *state {
                OrderState::Live(entry) => touched.extend(self.insert(entry).into_iter().flatten()),
                OrderState::Gone(id) => touched.extend(self.remove(&id)),
            }
        }
        self.applied = self.applied.saturating_add(batch.orders.len() as u64);
        // Every pair this copy changed must come with the matcher's digest,
        // not only the pairs the batch lists.
        let sent: HashMap<Pair, u128> = batch.digests.iter().copied().collect();
        touched.extend(sent.keys().copied());
        let mut mismatched = 0;
        for &pair in &touched {
            if sent.get(&pair) != Some(&self.digest(pair)) {
                mismatched = 1;
                if self.out_of_step.insert(pair) {
                    tracing::error!(?pair, "book copy differs from the matcher's book");
                }
            } else if self.out_of_step.remove(&pair) {
                tracing::warn!(?pair, "book copy matches the matcher's book again");
            }
        }
        DIGEST_MISMATCHES.fetch_add(mismatched as u64, Ordering::Relaxed);
        Applied {
            touched,
            mismatched,
        }
    }

    /// Apply every batch already queued.
    pub(crate) fn drain(&mut self, batches: &mut mpsc::UnboundedReceiver<BookBatch>) -> Applied {
        let mut applied = Applied::default();
        while let Ok(batch) = batches.try_recv() {
            applied.merge(self.apply(&batch));
        }
        applied
    }

    /// Publish that this copy's quotes now see every order state it applied.
    pub(crate) fn publish_applied(&self) {
        APPLIED.store(self.applied, Ordering::Relaxed);
    }

    /// Every non-empty pair's levels, best first (empty without levels).
    pub(crate) fn snapshot(&self) -> SwapBookSnapshot {
        let Some(levels) = &self.levels else {
            return SwapBookSnapshot::default();
        };
        levels
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

/// The matcher's side: its own copy (no levels), the orders touched since the
/// last batch, and where batches go.
#[derive(Debug)]
pub(crate) struct BookFeed {
    copy: BookCopy,
    touched_orders: HashSet<NoteId>,
    touched_pairs: HashSet<Pair>,
    tx: mpsc::UnboundedSender<BookBatch>,
}

impl BookFeed {
    pub(crate) fn new(tx: mpsc::UnboundedSender<BookBatch>) -> Self {
        Self {
            copy: BookCopy::default(),
            touched_orders: HashSet::new(),
            touched_pairs: HashSet::new(),
            tx,
        }
    }

    /// `order` entered the matchable index.
    pub(crate) fn live(&mut self, order: &Order) {
        self.live_entry(BookEntry::of(order));
    }

    /// An order with these fields entered the matchable index.
    pub(crate) fn live_entry(&mut self, entry: BookEntry) {
        self.touched_pairs
            .extend(self.copy.insert(entry).into_iter().flatten());
        self.touched_orders.insert(entry.id);
    }

    /// The order with `id` left it.
    pub(crate) fn gone(&mut self, id: NoteId) {
        self.touched_pairs.extend(self.copy.remove(&id));
        self.touched_orders.insert(id);
    }

    /// End of a step: send what it changed, if anything. Sending never
    /// blocks; a closed receiver is ignored (and shows as a growing backlog).
    pub(crate) fn flush(&mut self) {
        if self.touched_orders.is_empty() {
            return;
        }
        let orders: Vec<OrderState> = self
            .touched_orders
            .drain()
            .map(|id| match self.copy.get(&id) {
                Some(entry) => OrderState::Live(*entry),
                None => OrderState::Gone(id),
            })
            .collect();
        let digests = self
            .touched_pairs
            .drain()
            .map(|pair| (pair, self.copy.digest(pair)))
            .collect();
        // A large step (the startup load) leaves no large sets behind.
        self.touched_orders.shrink_to(SHRINK_TO);
        self.touched_pairs.shrink_to(SHRINK_TO);
        SENT.fetch_add(orders.len() as u64, Ordering::Relaxed);
        let _ = self.tx.send(BookBatch { orders, digests });
    }
}

/// Capacity the touched sets keep between steps.
const SHRINK_TO: usize = 1024;

#[cfg(test)]
mod tests;

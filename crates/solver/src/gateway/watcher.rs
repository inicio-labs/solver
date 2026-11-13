//! Tracks maker notes with a dedicated Miden client. The SDK owns note
//! inclusion, nullifier sync and its chain cursor; PostgreSQL owns whether an
//! order is live and which maker events have been committed.
//!
//! A round can be repeated after any failure. The SDK store may advance before
//! the PostgreSQL write; pending and live rows are reconciled against the SDK
//! on every round, so a restart or failed write does not lose a transition.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{NoteFile, NoteSyncHint};
use miden_client::rpc::domain::note::CommittedNote;
use miden_client::rpc::NodeRpcClient;
use miden_client::store::{InputNoteRecord, NoteFilter};
use miden_client::sync::{NoteUpdateAction, OnNoteReceived, StateSync, StateSyncInput};
use miden_client::{Client, ClientError};
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::{Note, NoteId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::proto::OrderState;
use crate::db::maker_db;
use crate::db::{postgres_db, DbPool};
use crate::ingest::{ChainError, ChainResult};
use crate::types::{now_millis, now_unix, BookUpdate, OrderId};

// Published after each successful round, for `/metrics`. `READ_UNIX_MS` is
// set when the watcher starts, so a watcher that never completes a round
// still reports a growing age.
static SYNCED_BLOCK: AtomicU64 = AtomicU64::new(0);
static READ_UNIX_MS: AtomicU64 = AtomicU64::new(0);
static PENDING_LINEAGES: AtomicU64 = AtomicU64::new(0);
static OLDEST_PENDING_MS: AtomicU64 = AtomicU64::new(0);
static UNCONFIRMED: AtomicU64 = AtomicU64::new(0);
static OLDEST_UNCONFIRMED_MS: AtomicU64 = AtomicU64::new(0);
static WAITING: AtomicU64 = AtomicU64::new(0);
static UNOBSERVED: AtomicU64 = AtomicU64::new(0);

/// What the maker-note watcher last saw, for `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WatcherGauges {
    /// The block the last successful round synced to; 0 before the first.
    pub synced_block: u64,
    /// Seconds since that round started reading the chain, or since the
    /// watcher started.
    pub read_age_secs: f64,
    /// Submitted orders whose note is not resolved yet.
    pub pending_lineages: u64,
    /// Age of the oldest of them, by the database clock.
    pub oldest_pending_age_secs: f64,
    /// Of those, orders that went live at accept (ADR 0008).
    pub unconfirmed: u64,
    pub oldest_unconfirmed_age_secs: f64,
    /// Of those, orders out of the book until their note lands.
    pub waiting: u64,
    /// Watched notes the last round could not read.
    pub unobserved: u64,
}

/// `None` when no watcher runs in this process (gateway disabled).
pub(crate) fn watcher_gauges() -> Option<WatcherGauges> {
    let read_ms = READ_UNIX_MS.load(Ordering::Relaxed);
    if read_ms == 0 {
        return None;
    }
    Some(WatcherGauges {
        synced_block: SYNCED_BLOCK.load(Ordering::Relaxed),
        read_age_secs: now_millis().saturating_sub(read_ms) as f64 / 1000.0,
        pending_lineages: PENDING_LINEAGES.load(Ordering::Relaxed),
        oldest_pending_age_secs: OLDEST_PENDING_MS.load(Ordering::Relaxed) as f64 / 1000.0,
        unconfirmed: UNCONFIRMED.load(Ordering::Relaxed),
        oldest_unconfirmed_age_secs: OLDEST_UNCONFIRMED_MS.load(Ordering::Relaxed) as f64 / 1000.0,
        waiting: WAITING.load(Ordering::Relaxed),
        unobserved: UNOBSERVED.load(Ordering::Relaxed),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteObservation {
    Pending,
    Committed,
    Consumed,
    Mismatched,
}

/// Check a set of exact notes without deciding whether their orders may trade.
#[async_trait(?Send)]
pub trait MakerTracker {
    /// Observe `notes`. A note new to the tracker that is in `import_from`
    /// has its history searched from that block: an order that went live at
    /// accept (ADR 0008), whose note may have landed while a lagging node
    /// still answered that it didn't. A note the tracker could not read this
    /// round is left out of the result.
    async fn observe_from(
        &mut self,
        notes: &[Note],
        import_from: &HashMap<NoteId, BlockNumber>,
    ) -> ChainResult<HashMap<NoteId, NoteObservation>>;

    async fn observe(&mut self, notes: &[Note]) -> ChainResult<HashMap<NoteId, NoteObservation>> {
        self.observe_from(notes, &HashMap::new()).await
    }

    /// One exact lookup of `note` and its nullifier on the node.
    async fn lookup(&mut self, note: &Note) -> ChainResult<NoteObservation>;

    /// The block the last `observe` synced to, and when it started reading
    /// the chain.
    fn last_sync(&self) -> Option<SyncRead> {
        None
    }
}

/// A tracker's chain read: the block it synced to, and when it started
/// reading, by the wall clock (for `/metrics`) and by a monotonic clock (for
/// freshness, which a wall-clock step must not fake).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncRead {
    pub block: BlockNumber,
    pub read_unix_ms: u64,
    pub read_at: std::time::Instant,
}

/// The watcher's latest synced block and when it was read from the chain,
/// shared with intake: an order goes live at accept only while this is fresh
/// (ADR 0008, D7).
#[derive(Clone, Default)]
pub struct SyncedHeight(Arc<Mutex<Option<(BlockNumber, std::time::Instant)>>>);

impl SyncedHeight {
    /// The longest a block read may be old for an order to go live at accept.
    pub const MAX_AGE: Duration = Duration::from_secs(10);

    fn publish(&self, read: SyncRead) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some((read.block, read.read_at));
    }

    /// The block, if it was read from the chain less than `max_age` ago.
    pub fn fresh(&self, max_age: Duration) -> Option<BlockNumber> {
        let (block, read_at) = (*self.0.lock().unwrap_or_else(|e| e.into_inner()))?;
        (read_at.elapsed() < max_age).then_some(block)
    }
}

/// At most this many cap lookups per round; the rest wait for the next one,
/// so a backlog of missing notes cannot stall the round.
const MAX_CAP_LOOKUPS: usize = 16;

/// When the watcher cancels an order that went live at accept whose note
/// never arrived (ADR 0008, D6): both must have passed since accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnconfirmedCap {
    pub secs: u64,
    pub blocks: u32,
}

impl Default for UnconfirmedCap {
    fn default() -> Self {
        Self {
            secs: 60,
            blocks: 20,
        }
    }
}

impl UnconfirmedCap {
    /// Whether the time and block parts of the cap have passed for
    /// `submission`, with the watcher synced to `synced`.
    fn due(&self, submission: &maker_db::PendingSubmission, synced: Option<BlockNumber>) -> bool {
        let (Some(accept_block), Some(synced)) = (submission.accept_block, synced) else {
            return false;
        };
        submission.age_secs >= self.secs as f64
            && u64::from(synced.as_u32())
                >= u64::from(accept_block.as_u32()) + u64::from(self.blocks)
    }
}

/// Blocks before an order's accept block from which its note's history is
/// searched, in case the watcher's block lagged the maker's transaction.
const IMPORT_MARGIN_BLOCKS: u32 = 10;

/// Only exact notes supplied by PostgreSQL may enter the maker client store.
struct MakerNoteScreen(HashSet<NoteId>);

#[async_trait(?Send)]
impl OnNoteReceived for MakerNoteScreen {
    async fn on_note_received(
        &self,
        committed: CommittedNote,
        _public: Option<InputNoteRecord>,
    ) -> Result<NoteUpdateAction, ClientError> {
        Ok(if self.0.contains(committed.note_id()) {
            NoteUpdateAction::Commit(committed)
        } else {
            NoteUpdateAction::Discard
        })
    }
}

/// The SDK's client store is dedicated to maker notes, separate from ingest.
/// An exact-ID RPC fallback handles the exceptional case in which an expected
/// record with the same details commitment resolved to another note's metadata.
pub struct SdkTracker {
    client: Client<FilesystemKeyStore>,
    rpc: Arc<dyn NodeRpcClient>,
    // Recheck exact identity and historic spends when a note enters the watch
    // set, including after restart or a period excluded from scoped sync.
    tracked: HashSet<NoteId>,
    last_sync: Option<SyncRead>,
}

impl SdkTracker {
    pub fn new(client: Client<FilesystemKeyStore>, rpc: Arc<dyn NodeRpcClient>) -> Self {
        Self {
            client,
            rpc,
            tracked: HashSet::new(),
            last_sync: None,
        }
    }

    /// Build sync input from the authoritative set, never all SQLite records.
    /// This also excludes unrelated records left by earlier watcher versions.
    async fn maker_sync_input(&self, notes: &[Note]) -> ChainResult<StateSyncInput> {
        let allowed: HashSet<_> = notes.iter().map(Note::id).collect();
        let input_notes: Vec<_> = self
            .client
            .get_input_notes(NoteFilter::DetailsCommitments(
                notes.iter().map(Note::details_commitment).collect(),
            ))
            .await?
            .into_iter()
            .filter(|record| {
                !record.is_consumed() && record.id().is_none_or(|id| allowed.contains(&id))
            })
            .collect();
        let pending: BTreeSet<_> = input_notes
            .iter()
            .filter(|record| !record.is_committed())
            .map(|record| record.details_commitment())
            .collect();
        Ok(StateSyncInput {
            accounts: Vec::new(),
            note_tags: notes
                .iter()
                .filter(|note| pending.contains(&note.details_commitment()))
                .map(|note| note.metadata().tag())
                .collect(),
            input_notes,
            output_notes: Vec::new(),
            uncommitted_transactions: Vec::new(),
        })
    }

    async fn sync_maker_notes(&mut self, notes: &[Note]) -> ChainResult<()> {
        let input = self.maker_sync_input(notes).await?;
        let allowed = notes.iter().map(Note::id).collect();
        let sync = StateSync::new(
            self.rpc.clone(),
            Arc::new(MakerNoteScreen(allowed)),
            None,
            self.client.get_validator_config().await?,
        );
        let (started_ms, started_at) = (now_millis(), std::time::Instant::now());
        let mut update = sync
            .fetch_state(self.client.get_sync_height().await?, input)
            .await?;
        sync.derive_state_updates(&mut update).await?;
        sync.fetch_nullifiers(&mut update).await?;
        self.client.apply_chain_updates(&sync, update).await?;
        // The cap's blocks, intake's freshness and `/metrics` read this. A
        // failed height read leaves the last value and never fails the round.
        if let Ok(synced) = self.client.get_sync_height().await {
            self.last_sync = Some(SyncRead {
                block: synced,
                read_unix_ms: started_ms,
                read_at: started_at,
            });
        }
        Ok(())
    }

    async fn exact_lookup(&self, note: &Note) -> ChainResult<NoteObservation> {
        let Some(found) = self
            .rpc
            .get_notes_by_id(&[note.id()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(NoteObservation::Pending);
        };
        // A note ID commits to the metadata, so the note under our ID is
        // ours, however the node renders its metadata (a withheld
        // attachment, for one).
        if found.id() != note.id() {
            return Ok(NoteObservation::Pending);
        }
        let spent = self
            .rpc
            .get_nullifier_commit_heights(BTreeSet::from([note.nullifier()]), BlockNumber::GENESIS)
            .await?;
        Ok(
            if spent.get(&note.nullifier()).copied().flatten().is_some() {
                NoteObservation::Consumed
            } else {
                NoteObservation::Committed
            },
        )
    }
}

#[async_trait(?Send)]
impl MakerTracker for SdkTracker {
    fn last_sync(&self) -> Option<SyncRead> {
        self.last_sync
    }

    async fn lookup(&mut self, note: &Note) -> ChainResult<NoteObservation> {
        let found = self.exact_lookup(note).await?;
        if found != NoteObservation::Pending {
            // Its record is still an expected note: import it again next
            // round, so the store holds it committed and sees it spent.
            self.tracked.remove(&note.id());
        }
        Ok(found)
    }

    async fn observe_from(
        &mut self,
        notes: &[Note],
        import_from: &HashMap<NoteId, BlockNumber>,
    ) -> ChainResult<HashMap<NoteId, NoteObservation>> {
        self.client.ensure_genesis_in_place().await?;
        self.client.ensure_rpc_limits_in_place().await?;
        // Capture before lookup: a note committed during import must still be
        // covered by the following forward sync.
        let cursor = self.client.get_sync_height().await?;
        let commitments: Vec<_> = notes.iter().map(Note::details_commitment).collect();
        let mut count_by_commitment = BTreeMap::new();
        for commitment in &commitments {
            *count_by_commitment.entry(*commitment).or_insert(0_usize) += 1;
        }
        // Each stored record's ID by details commitment (none while expected).
        let known: BTreeMap<_, _> = self
            .client
            .get_input_notes(NoteFilter::DetailsCommitments(commitments.clone()))
            .await?
            .iter()
            .map(|record| (record.details_commitment(), record.id()))
            .collect();
        let mut queued = BTreeSet::new();
        let new_notes: Vec<_> = notes
            .iter()
            .filter(|note| {
                let commitment = note.details_commitment();
                // A record of another note with the same details can't be
                // replaced: the exact lookup below decides this one.
                let held_by_other =
                    matches!(known.get(&commitment), Some(Some(id)) if *id != note.id());
                (!known.contains_key(&commitment) || !self.tracked.contains(&note.id()))
                    && !held_by_other
                    && queued.insert(commitment)
            })
            .collect();
        // Decided per note: one note's failure never fails the round.
        let mut observed = HashMap::with_capacity(notes.len());
        let mut unreadable = HashSet::new();
        if !new_notes.is_empty() {
            let limit = self.rpc.get_rpc_limits().await?.note_ids_limit.max(1) as usize;
            for batch in new_notes.chunks(limit) {
                let ids: Vec<_> = batch.iter().map(|note| note.id()).collect();
                let found = match self.rpc.get_notes_by_id(&ids).await {
                    Ok(found) => found,
                    // One note that fails to decode fails the whole batch:
                    // look each one up alone.
                    Err(error) if ids.len() > 1 => {
                        tracing::warn!(%error, notes = ids.len(), "maker note batch lookup failed; looking notes up one by one");
                        let mut found = Vec::with_capacity(ids.len());
                        for id in &ids {
                            match self.rpc.get_notes_by_id(&[*id]).await {
                                Ok(note) => found.extend(note),
                                Err(error) => {
                                    tracing::warn!(note_id = %id, %error, "maker note lookup failed; skipped this round");
                                    unreadable.insert(*id);
                                }
                            }
                        }
                        found
                    }
                    Err(error) => {
                        tracing::warn!(note_id = %ids[0], %error, "maker note lookup failed; skipped this round");
                        unreadable.insert(ids[0]);
                        Vec::new()
                    }
                };
                let found: HashMap<_, _> =
                    found.into_iter().map(|note| (note.id(), note)).collect();
                let mut imports = Vec::with_capacity(batch.len());
                for note in batch {
                    if unreadable.contains(&note.id()) {
                        continue;
                    }
                    // Found by our ID: it is our note (the ID commits to the
                    // metadata), so it is imported with our own metadata.
                    let file = if let Some(found) = found.get(&note.id()) {
                        NoteFile::Committed {
                            note: (*note).clone(),
                            proof: found.inclusion_proof().clone(),
                        }
                    } else {
                        // Forward sync starts at the captured cursor and
                        // covers commitments racing this lookup. A note that
                        // went live at accept is searched from its accept
                        // block too.
                        let after = import_from.get(&note.id()).map_or(cursor.child(), |block| {
                            BlockNumber::from(block.as_u32().saturating_sub(IMPORT_MARGIN_BLOCKS))
                                .min(cursor.child())
                        });
                        NoteFile::ExpectedNote {
                            details: (*note).clone().into(),
                            sync_hint: NoteSyncHint::new(after, note.metadata().tag()),
                        }
                    };
                    imports.push((note.id(), file));
                }
                if imports.is_empty() {
                    continue;
                }
                let files: Vec<_> = imports.iter().map(|(_, file)| file.clone()).collect();
                if let Err(error) = self.client.import_notes(&files).await {
                    // One note's import can fail the batch: import each
                    // alone, and skip the ones that still fail this round.
                    tracing::warn!(%error, notes = imports.len(), "maker note import failed; importing notes one by one");
                    for (id, file) in imports {
                        if let Err(error) = self.client.import_notes(&[file]).await {
                            tracing::warn!(note_id = %id, %error, "maker note import failed; skipped this round");
                            unreadable.insert(id);
                        }
                    }
                }
            }
        }
        let skip =
            |note: &&Note| !unreadable.contains(&note.id()) && !observed.contains_key(&note.id());

        let synced: Vec<Note> = notes.iter().filter(skip).cloned().collect();
        self.sync_maker_notes(&synced).await?;
        let records: BTreeMap<_, _> = self
            .client
            .get_input_notes(NoteFilter::DetailsCommitments(commitments))
            .await?
            .into_iter()
            .map(|record| (record.details_commitment(), record))
            .collect();
        for note in &synced {
            // The SDK stores only one expected record per details commitment.
            // If two submitted IDs share it, use exact SDK RPC lookup for both
            // rather than treating either record as the other's commitment.
            let state = if count_by_commitment[&note.details_commitment()] > 1 {
                self.exact_lookup(note).await
            } else {
                match records.get(&note.details_commitment()) {
                    None => Err(ChainError::MissingSyncedNote(note.id())),
                    Some(record)
                        if record.id() == Some(note.id())
                            && record.metadata() == Some(note.metadata()) =>
                    {
                        Ok(if record.is_consumed() {
                            NoteObservation::Consumed
                        } else if record.is_committed() {
                            NoteObservation::Committed
                        } else {
                            NoteObservation::Pending
                        })
                    }
                    Some(record) if record.id().is_some() => self.exact_lookup(note).await,
                    Some(_) => Ok(NoteObservation::Pending),
                }
            };
            match state {
                Ok(state) => {
                    observed.insert(note.id(), state);
                }
                Err(error) => {
                    tracing::warn!(note_id = %note.id(), %error, "maker note not observed this round");
                }
            }
        }
        self.tracked = notes
            .iter()
            .map(Note::id)
            .filter(|id| !unreadable.contains(id))
            .collect();
        Ok(observed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Db(#[from] crate::db::DbError),
}

pub struct Watcher<T> {
    pool: DbPool,
    tracker: T,
    book_tx: mpsc::Sender<BookUpdate>,
    // PostgreSQL supplies the authoritative set; this cache avoids decoding
    // every unchanged live note on every round. It is rebuilt after restart.
    watched: HashMap<Vec<u8>, Note>,
    height: SyncedHeight,
    cap: UnconfirmedCap,
    /// Where this round's cap lookups start among the pending submissions,
    /// moved on every round so notes whose lookups keep failing can't hold
    /// the per-round budget.
    lookup_start: usize,
}

impl<T: MakerTracker> Watcher<T> {
    pub fn new(pool: DbPool, tracker: T, book_tx: mpsc::Sender<BookUpdate>) -> Self {
        Self {
            pool,
            tracker,
            book_tx,
            watched: HashMap::new(),
            height: SyncedHeight::default(),
            cap: UnconfirmedCap::default(),
            lookup_start: 0,
        }
    }

    pub fn with_cap(mut self, cap: UnconfirmedCap) -> Self {
        self.cap = cap;
        self
    }

    /// Where this watcher publishes its latest synced block, for intake.
    pub fn synced_height(&self) -> SyncedHeight {
        self.height.clone()
    }

    /// Reconcile pending submissions and previously live maker orders against
    /// the SDK, then commit every resulting order and event change together.
    ///
    /// An order that went live at accept (ADR 0008) whose note is still not
    /// found once the cap's time and blocks have passed gets one exact lookup.
    /// Its answer counts as this round's observation; if the note is still
    /// not on chain, the order is cancelled.
    pub async fn round(&mut self) -> Result<(), WatchError> {
        let mut pending = self.pool.read(maker_db::pending_submissions_tx).await?;
        if !pending.is_empty() {
            let start = self.lookup_start % pending.len();
            pending.rotate_left(start);
            self.lookup_start = self.lookup_start.wrapping_add(MAX_CAP_LOOKUPS);
        }
        let watched_ids = self.pool.read(maker_db::watched_maker_orders_tx).await?;
        let wanted: HashSet<_> = watched_ids.iter().cloned().collect();
        let new_ids: Vec<_> = watched_ids
            .into_iter()
            .filter(|id| !self.watched.contains_key(id))
            .collect();
        let new_notes = self
            .pool
            .read(move |conn| maker_db::order_notes_tx(conn, &new_ids))
            .await?;
        self.watched.retain(|id, _| wanted.contains(id));
        for note in new_notes {
            self.watched.insert(note.id().to_bytes().to_vec(), note);
        }

        let notes: BTreeMap<NoteId, Note> = pending
            .iter()
            .map(|submission| (submission.note.id(), submission.note.clone()))
            .chain(self.watched.values().map(|note| (note.id(), note.clone())))
            .collect();
        let import_from: HashMap<NoteId, BlockNumber> = pending
            .iter()
            .filter_map(|s| Some((s.note.id(), s.accept_block?)))
            .collect();
        let observations = self
            .tracker
            .observe_from(&notes.into_values().collect::<Vec<_>>(), &import_from)
            .await?;
        let unobserved = pending
            .iter()
            .map(|submission| &submission.note)
            .chain(self.watched.values())
            .filter(|note| !observations.contains_key(&note.id()))
            .count();
        UNOBSERVED.store(unobserved as u64, Ordering::Relaxed);
        if unobserved > 0 {
            tracing::warn!(unobserved, "maker notes without an observation this round");
        }
        let read = self.tracker.last_sync();
        // Publish the block now: the cap's lookups and the write below must
        // not age it for intake.
        if let Some(read) = read {
            self.height.publish(read);
        }
        let synced = read.map(|read| read.block);

        let mut activations = Vec::new();
        let mut rejections = Vec::new();
        let mut capped = Vec::new();
        let mut looked_up = Vec::new();
        let mut lookups = 0;
        for submission in pending {
            let observed = observations.get(&submission.note.id()).copied();
            // An unread note gets no decision, unless the cap is due: its
            // exact lookup decides then, so it can't stay live forever.
            let observation = if matches!(observed, None | Some(NoteObservation::Pending))
                && self.cap.due(&submission, synced)
            {
                if lookups == MAX_CAP_LOOKUPS {
                    continue;
                }
                lookups += 1;
                match self.tracker.lookup(&submission.note).await {
                    Ok(NoteObservation::Pending) => {
                        capped.push(submission);
                        continue;
                    }
                    Ok(found) => {
                        looked_up.push(submission.lineage_id.clone());
                        found
                    }
                    Err(error) => {
                        tracing::warn!(note_id = %submission.note.id(), %error, "cap lookup failed; next round");
                        continue;
                    }
                }
            } else if let Some(observed) = observed {
                observed
            } else {
                continue;
            };
            match observation {
                NoteObservation::Committed => activations.push(submission),
                NoteObservation::Consumed => rejections.push((
                    submission,
                    OrderState::Unavailable,
                    "note spent before it became live",
                )),
                NoteObservation::Mismatched => rejections.push((
                    submission,
                    OrderState::Rejected,
                    "committed note differs from the submitted note",
                )),
                NoteObservation::Pending => {}
            }
        }
        let spent_ids: Vec<Vec<u8>> = self
            .watched
            .iter()
            .filter(|(_, note)| observations.get(&note.id()) == Some(&NoteObservation::Consumed))
            .map(|(id, _)| id.clone())
            .collect();

        if !activations.is_empty()
            || !rejections.is_empty()
            || !capped.is_empty()
            || !spent_ids.is_empty()
        {
            let removed = spent_ids
                .iter()
                .map(|id| Ok(OrderId::read_from_bytes(id)?))
                .collect::<Result<Vec<_>, crate::db::DbError>>()?;
            let writes = (
                activations.clone(),
                rejections.clone(),
                capped.clone(),
                spent_ids.clone(),
            );
            let done = Arc::new(Mutex::new(maker_db::WatcherWrites::default()));
            let done_in = done.clone();
            self.pool
                .write_book(&self.book_tx, move |conn| {
                    let (activations, rejections, capped, spent_ids) = writes;
                    maker_db::prelock_watcher_tx(
                        conn,
                        &activations,
                        &rejections,
                        &capped,
                        &spent_ids,
                    )?;
                    let rejected = maker_db::reject_tx(conn, &rejections)?;
                    let (stopped, cap_cancels) = maker_db::cap_unconfirmed_tx(conn, &capped)?;
                    postgres_db::mark_orders_onchain_nullified_tx(conn, &spent_ids)?;
                    let activated = maker_db::activate_tx(conn, &activations, now_unix())?;
                    let mut update = activated.update;
                    update.removed.extend(removed);
                    update.removed.extend(rejected);
                    update.removed.extend(stopped);
                    *done_in.lock().unwrap_or_else(|e| e.into_inner()) = maker_db::WatcherWrites {
                        returned: activated.returned_makers,
                        cap_cancels,
                        activated: activated.unconfirmed,
                    };
                    Ok(update)
                })
                .await?;
            let done = std::mem::take(&mut *done.lock().unwrap_or_else(|e| e.into_inner()));
            for submission in activations
                .iter()
                .filter(|s| done.activated.contains(&s.lineage_id))
            {
                let via = if looked_up.contains(&submission.lineage_id) {
                    "lookup"
                } else {
                    "watcher"
                };
                super::metrics::count_activation(submission.maker_id, via);
            }
            for maker_id in done.returned {
                super::metrics::count(super::metrics::WAITING_RETURNED, maker_id);
            }
            for maker_id in &done.cap_cancels {
                super::metrics::count(super::metrics::CAP_CANCELS, *maker_id);
            }
            tracing::info!(
                activated = activations.len(),
                rejected = rejections.len(),
                capped = done.cap_cancels.len(),
                spent = spent_ids.len(),
                "maker notes verified"
            );
        }
        for id in spent_ids {
            self.watched.remove(&id);
        }
        self.publish_gauges().await;
        Ok(())
    }

    /// Publish what this successful round saw for `/metrics`. A failed count
    /// leaves the previous values; it never fails the round.
    async fn publish_gauges(&self) {
        if let Some(read) = self.tracker.last_sync() {
            SYNCED_BLOCK.store(u64::from(read.block.as_u32()), Ordering::Relaxed);
            READ_UNIX_MS.store(read.read_unix_ms, Ordering::Relaxed);
        }
        match self.pool.read(maker_db::pending_lineage_stats_tx).await {
            Ok(stats) => {
                PENDING_LINEAGES.store(u64::try_from(stats.count).unwrap_or(0), Ordering::Relaxed);
                let millis = |secs: Option<f64>| (secs.unwrap_or(0.0).max(0.0) * 1000.0) as u64;
                OLDEST_PENDING_MS.store(millis(stats.oldest_age_secs), Ordering::Relaxed);
                UNCONFIRMED.store(
                    u64::try_from(stats.unconfirmed).unwrap_or(0),
                    Ordering::Relaxed,
                );
                OLDEST_UNCONFIRMED_MS
                    .store(millis(stats.oldest_unconfirmed_age_secs), Ordering::Relaxed);
                WAITING.store(u64::try_from(stats.waiting).unwrap_or(0), Ordering::Relaxed);
            }
            Err(error) => tracing::debug!(%error, "pending lineage count failed"),
        }
    }
}

pub async fn run_watcher<T: MakerTracker>(
    mut watcher: Watcher<T>,
    interval: Duration,
    cancel: CancellationToken,
) {
    // Until a round succeeds, the read age counts from now, and no block is
    // reported.
    READ_UNIX_MS.store(now_millis(), Ordering::Relaxed);
    SYNCED_BLOCK.store(0, Ordering::Relaxed);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {
                if let Err(error) = watcher.round().await {
                    tracing::warn!(%error, "maker-note watcher round failed; retrying");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::maker_db::read_events_tx;
    use crate::db::postgres_models::NewOrderRow;
    use crate::db::postgres_test::TestDb;
    use crate::gateway::proto;
    use crate::maker::{CommandHeader, CutoffScope, MakerCommand, MakerId};
    use miden_client::builder::ClientBuilder;
    use miden_client::testing::mock::MockRpcApi;
    use miden_client_sqlite_store::ClientBuilderSqliteExt;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::{Note, NoteMetadata, NoteType, Nullifier};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use miden_testing::MockChain;
    use prost::Message;
    use std::sync::Mutex;

    #[derive(Default)]
    struct ChainState {
        tip: u32,
        notes: HashMap<NoteId, (u32, NoteMetadata)>,
        spent: HashMap<Nullifier, u32>,
        down: bool,
        /// Notes the tracker cannot read: no observation, no lookup answer.
        unreadable: HashSet<NoteId>,
        /// Notes a lagging round misses although they landed; the exact
        /// lookup still finds them.
        hidden: HashSet<NoteId>,
        synced: Option<SyncRead>,
        lookups: usize,
        /// The exact lookup fails, as on an RPC error.
        lookup_fails: bool,
        /// Notes whose exact lookup always fails.
        failing_lookups: HashSet<NoteId>,
    }

    #[derive(Default, Clone)]
    struct FakeChain(Arc<Mutex<ChainState>>);

    impl FakeChain {
        fn state(&self) -> std::sync::MutexGuard<'_, ChainState> {
            self.0.lock().unwrap()
        }

        fn commit(&self, note: &Note, block: u32) {
            let mut state = self.state();
            state.tip = state.tip.max(block);
            state.notes.insert(note.id(), (block, *note.metadata()));
        }

        fn spend(&self, note: &Note, block: u32) {
            let mut state = self.state();
            state.tip = state.tip.max(block);
            state.spent.insert(note.nullifier(), block);
        }
    }

    fn up(state: &ChainState) -> ChainResult<()> {
        if state.down {
            return Err(ChainError::Test("node down"));
        }
        Ok(())
    }

    fn observed(state: &ChainState, note: &Note) -> NoteObservation {
        match state.notes.get(&note.id()) {
            None => NoteObservation::Pending,
            Some((_, metadata)) if metadata != note.metadata() => NoteObservation::Mismatched,
            Some(_) if state.spent.contains_key(&note.nullifier()) => NoteObservation::Consumed,
            Some(_) => NoteObservation::Committed,
        }
    }

    #[async_trait(?Send)]
    impl MakerTracker for FakeChain {
        async fn observe_from(
            &mut self,
            notes: &[Note],
            _import_from: &HashMap<NoteId, BlockNumber>,
        ) -> ChainResult<HashMap<NoteId, NoteObservation>> {
            let mut state = self.state();
            up(&state)?;
            let observations = notes
                .iter()
                .filter(|note| !state.unreadable.contains(&note.id()))
                .map(|note| {
                    let observation = if state.hidden.contains(&note.id()) {
                        NoteObservation::Pending
                    } else {
                        observed(&state, note)
                    };
                    (note.id(), observation)
                })
                .collect();
            state.synced = Some(SyncRead {
                block: BlockNumber::from(state.tip),
                read_unix_ms: now_millis(),
                read_at: std::time::Instant::now(),
            });
            Ok(observations)
        }

        async fn lookup(&mut self, note: &Note) -> ChainResult<NoteObservation> {
            let mut state = self.state();
            up(&state)?;
            state.lookups += 1;
            if state.lookup_fails || state.failing_lookups.contains(&note.id()) {
                return Err(ChainError::Test("lookup failed"));
            }
            Ok(observed(&state, note))
        }

        fn last_sync(&self) -> Option<SyncRead> {
            self.state().synced
        }
    }

    fn note(serial: u32, note_type: NoteType) -> Note {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        note_for_creator(serial, note_type, creator)
    }

    fn note_for_creator(
        serial: u32,
        note_type: NoteType,
        creator: miden_protocol::account::AccountId,
    ) -> Note {
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        PswapNote::builder()
            .sender(creator)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 100).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .serial_number(Word::from([serial, 0, 0, 0]))
            .note_type(note_type)
            .offered_asset(FungibleAsset::new(offered, 50).unwrap())
            .build()
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn sdk_tracker_recovers_note_committed_before_submission() {
        let committed = note(700, NoteType::Private);
        let mut builder = MockChain::builder();
        builder.add_output_note(miden_protocol::transaction::RawOutputNote::Full(
            committed.clone(),
        ));
        let rpc = Arc::new(MockRpcApi::new(builder.build().unwrap()));
        let store_dir = tempfile::tempdir().unwrap();
        let store_path = store_dir.path().join("maker.sqlite3");
        let client = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(store_path.clone())
            .build()
            .await
            .unwrap();
        let mut tracker = SdkTracker::new(client, rpc.clone());
        assert_eq!(tracker.last_sync(), None);

        let first = tracker
            .observe(std::slice::from_ref(&committed))
            .await
            .unwrap();
        assert_eq!(first[&committed.id()], NoteObservation::Committed);
        // The sync it made is what `/metrics` reports.
        let read = tracker.last_sync().unwrap();
        assert_eq!(read.block, tracker.client.get_sync_height().await.unwrap());
        assert!(read.read_unix_ms > 0);

        drop(tracker);
        let reopened = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(store_path)
            .build()
            .await
            .unwrap();
        let mut tracker = SdkTracker::new(reopened, rpc);
        let after_restart = tracker
            .observe(std::slice::from_ref(&committed))
            .await
            .unwrap();
        assert_eq!(after_restart[&committed.id()], NoteObservation::Committed);
    }

    #[tokio::test]
    async fn sdk_tracker_filters_same_tag_notes_and_legacy_store_records() {
        use miden_protocol::transaction::RawOutputNote;
        use miden_testing::Auth;
        let mut builder = MockChain::builder();
        let asset =
            FungibleAsset::new(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(), 100).unwrap();
        let sender = builder
            .add_existing_wallet_with_assets(Auth::IncrNonce, [asset.into()])
            .unwrap();
        let wanted = note_for_creator(701, NoteType::Public, sender.id());
        let unrelated = note_for_creator(702, NoteType::Public, sender.id());
        assert_eq!(wanted.metadata().tag(), unrelated.metadata().tag());
        let spawn = builder.add_spawn_note([&wanted, &unrelated]).unwrap();
        let chain = builder.build().unwrap();
        let executed = chain
            .build_transaction(sender)
            .authenticated_input_note(spawn.id())
            .expected_output_note(RawOutputNote::Full(wanted.clone()))
            .expected_output_note(RawOutputNote::Full(unrelated.clone()))
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let rpc = Arc::new(MockRpcApi::new(chain));
        let dir = tempfile::tempdir().unwrap();
        let client = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(dir.path().join("maker.sqlite3"))
            .build()
            .await
            .unwrap();
        let mut tracker = SdkTracker::new(client, rpc.clone());
        assert_eq!(
            tracker.observe(std::slice::from_ref(&spawn)).await.unwrap()[&spawn.id()],
            NoteObservation::Committed
        );
        assert_eq!(
            tracker
                .observe(std::slice::from_ref(&wanted))
                .await
                .unwrap()[&wanted.id()],
            NoteObservation::Pending
        );
        rpc.mock_chain
            .write()
            .add_pending_executed_transaction(&executed)
            .unwrap();
        rpc.prove_block();
        assert_eq!(
            tracker
                .observe(std::slice::from_ref(&wanted))
                .await
                .unwrap()[&wanted.id()],
            NoteObservation::Committed
        );
        let stored = tracker
            .client
            .get_input_notes(NoteFilter::All)
            .await
            .unwrap();
        assert_eq!(
            stored.len(),
            2,
            "only the explicitly imported notes are stored"
        );
        assert!(
            stored
                .iter()
                .all(|record| record.id() != Some(unrelated.id())),
            "same-tag public notes must not enter the maker store"
        );
        assert!(stored.iter().any(|record| record.id() == Some(wanted.id())));
        // The spawn was consumed while excluded and the cursor already passed
        // its spend block. Re-entering the set must recover that historic spend.
        assert_eq!(
            tracker.observe(std::slice::from_ref(&spawn)).await.unwrap()[&spawn.id()],
            NoteObservation::Consumed
        );

        // Simulate a record retained by the earlier watcher. It must not be
        // loaded into subsequent nullifier sync, including an empty watch set.
        let proof = rpc
            .get_notes_by_id(&[unrelated.id()])
            .await
            .unwrap()
            .remove(0)
            .inclusion_proof()
            .clone();
        tracker
            .client
            .import_notes(&[NoteFile::Committed {
                note: unrelated,
                proof,
            }])
            .await
            .unwrap();
        let input = tracker
            .maker_sync_input(std::slice::from_ref(&wanted))
            .await
            .unwrap();
        assert_eq!(input.input_notes.len(), 1);
        assert_eq!(input.input_notes[0].id(), Some(wanted.id()));
        assert!(
            input.note_tags.is_empty(),
            "committed notes only need nullifier tracking"
        );
        let empty = tracker.maker_sync_input(&[]).await.unwrap();
        assert!(empty.input_notes.is_empty());
        assert!(empty.note_tags.is_empty());
        tracker.observe(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn sdk_tracker_new_pending_notes_start_after_the_synced_cursor() {
        use miden_client::store::InputNoteState;
        let rpc = Arc::new(MockRpcApi::new(MockChain::new()));
        rpc.prove_block();
        let dir = tempfile::tempdir().unwrap();
        let client = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(dir.path().join("maker.sqlite3"))
            .build()
            .await
            .unwrap();
        let mut tracker = SdkTracker::new(client, rpc.clone());
        tracker.observe(&[]).await.unwrap();
        let cursor = tracker.client.get_sync_height().await.unwrap();
        assert!(cursor > BlockNumber::GENESIS);
        let pending = note(703, NoteType::Private);
        let calls = rpc.get_notes_by_id_call_count();
        assert_eq!(
            tracker
                .observe(std::slice::from_ref(&pending))
                .await
                .unwrap()[&pending.id()],
            NoteObservation::Pending
        );
        assert_eq!(
            rpc.get_notes_by_id_call_count(),
            calls + 1,
            "new notes get one initial exact-ID lookup"
        );
        let records = tracker
            .client
            .get_input_notes(NoteFilter::All)
            .await
            .unwrap();
        let InputNoteState::Expected(state) = records[0].state() else {
            panic!("expected pending note")
        };
        assert_eq!(state.after_block_num, cursor.child());
        tracker
            .observe(std::slice::from_ref(&pending))
            .await
            .unwrap();
        assert_eq!(
            rpc.get_notes_by_id_call_count(),
            calls + 1,
            "pending retries use forward sync"
        );
    }

    struct Fixture {
        db: TestDb,
        chain: FakeChain,
        watcher: Watcher<FakeChain>,
        book: mpsc::Receiver<BookUpdate>,
        maker_id: MakerId,
    }

    async fn fixture() -> Fixture {
        let db = TestDb::new().await.unwrap();
        let maker_id = db
            .pool
            .write(|conn| Ok(maker_db::create_maker_tx(conn, "alpha")?.unwrap()))
            .await
            .unwrap();
        let chain = FakeChain::default();
        let (book_tx, book) = mpsc::channel(8);
        let watcher = Watcher::new(db.pool.clone(), chain.clone(), book_tx);
        Fixture {
            db,
            chain,
            watcher,
            book,
            maker_id,
        }
    }

    impl Fixture {
        async fn command(&self, request_id: &str, seq: u64, command: MakerCommand) {
            let header = CommandHeader::new(self.maker_id, request_id.into(), seq).unwrap();
            self.db
                .pool
                .write(move |conn| maker_db::execute_command_tx(conn, &header, &command))
                .await
                .unwrap();
        }

        async fn submit(&self, seq: u64, note: &Note) {
            let command = MakerCommand::submit(note.clone()).unwrap();
            self.command(&format!("s{seq}"), seq, command).await;
        }

        /// The OrderStatus events so far: (note ID, state, reason).
        async fn statuses(&self) -> Vec<(Vec<u8>, proto::OrderState, String)> {
            let maker_id = self.maker_id;
            self.db
                .pool
                .read(move |conn| read_events_tx(conn, maker_id, 0, 100))
                .await
                .unwrap()
                .into_iter()
                .map(|event| {
                    let body = proto::EventBody::decode(event.payload.as_slice()).unwrap();
                    match body.kind.unwrap() {
                        proto::event_body::Kind::OrderStatus(status) => {
                            let state = status.state();
                            (status.note_id, state, status.reason)
                        }
                        other => panic!("unexpected event {other:?}"),
                    }
                })
                .collect()
        }

        async fn status(&self, note: &Note) -> String {
            let id = note.id().to_bytes().to_vec();
            self.db
                .pool
                .read(move |conn| {
                    use crate::db::postgres_schema::orders;
                    use diesel::prelude::*;
                    Ok(orders::table
                        .find(id)
                        .select(orders::status)
                        .first::<String>(conn)?)
                })
                .await
                .unwrap()
        }
    }

    fn id(note: &Note) -> Vec<u8> {
        note.id().to_bytes().to_vec()
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_note_committed_before_its_submit_goes_live_once() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.chain.commit(&order, 5);
        f.submit(1, &order).await;
        f.watcher.round().await.unwrap();

        let update = f.book.try_recv().unwrap();
        assert_eq!(update.active.len(), 1);
        assert_eq!(update.active[0].id(), order.id());
        assert_eq!(
            update.active[0]
                .maker
                .map(|tag| (tag.maker_id, tag.root_seq)),
            Some((f.maker_id, 1)),
            "the book entry carries its maker"
        );
        assert_eq!(
            f.statuses().await,
            [(id(&order), proto::OrderState::Live, String::new())]
        );
        assert_eq!(f.status(&order).await, "active");

        f.watcher.round().await.unwrap();
        assert!(f.book.try_recv().is_err());
        assert_eq!(f.statuses().await.len(), 1, "Live is reported once");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_note_committed_later_is_found_by_its_tag() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.chain.state().tip = 5;
        f.submit(1, &order).await;
        f.watcher.round().await.unwrap();
        assert!(f.book.try_recv().is_err());

        // Another note with the same tag commits too; only ours counts.
        f.chain.commit(&note(2, NoteType::Private), 7);
        f.chain.commit(&order, 7);
        f.watcher.round().await.unwrap();
        assert_eq!(f.book.try_recv().unwrap().active[0].id(), order.id());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_differing_or_spent_note_is_rejected_with_its_reason() {
        let mut f = fixture().await;
        let (differs, spent) = (note(1, NoteType::Private), note(2, NoteType::Private));
        f.submit(1, &differs).await;
        f.submit(2, &spent).await;
        // The chain holds a note with this ID but other metadata.
        let other = note(1, NoteType::Public);
        f.chain
            .state()
            .notes
            .insert(differs.id(), (4, *other.metadata()));
        f.chain.commit(&spent, 4);
        f.chain.spend(&spent, 5);
        f.watcher.round().await.unwrap();

        assert!(f.book.try_recv().is_err(), "nothing goes live");
        let mut statuses = f.statuses().await;
        statuses.sort();
        let mut expected = vec![
            (
                id(&differs),
                proto::OrderState::Rejected,
                "committed note differs from the submitted note".to_string(),
            ),
            (
                id(&spent),
                proto::OrderState::Unavailable,
                "note spent before it became live".to_string(),
            ),
        ];
        expected.sort();
        assert_eq!(statuses, expected);
        let pending =
            f.db.pool
                .read(maker_db::pending_submissions_tx)
                .await
                .unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_cancel_before_activation_keeps_the_order_out_of_the_book() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.submit(1, &order).await;
        f.command(
            "c2",
            2,
            MakerCommand::CancelAll {
                scope: CutoffScope::all(),
            },
        )
        .await;
        f.chain.commit(&order, 3);
        f.watcher.round().await.unwrap();

        assert!(f.book.try_recv().is_err());
        assert!(f.statuses().await.is_empty(), "a cancel has no event");
        assert_eq!(f.status(&order).await, "stopped");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_public_maker_note_ingested_first_becomes_a_maker_order() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Public);
        let row = NewOrderRow::ingested(&order, 1).unwrap();
        f.db.pool
            .write(move |conn| {
                postgres_db::insert_orders_batch_tx(conn, &[row], 1)?;
                Ok(())
            })
            .await
            .unwrap();
        f.submit(1, &order).await;
        f.chain.commit(&order, 2);
        f.watcher.round().await.unwrap();

        // Ingest put the order in the book, and the submit's attribution
        // tagged it there, so activation sends nothing again.
        assert!(f.book.try_recv().is_err());
        assert_eq!(
            f.statuses().await,
            [(id(&order), proto::OrderState::Live, String::new())]
        );
        assert_eq!(f.status(&order).await, "active");
        let tagged =
            f.db.pool
                .read(postgres_db::load_live_orders_tx)
                .await
                .unwrap();
        assert!(tagged[0].maker.is_some(), "now a maker order");
    }

    /// A public maker note can trade before the watcher activates its claim:
    /// ingest stores it and `live_orders` admits it. When our own settlement
    /// consumes it in that window, the settlement events report the fill, so
    /// the watcher waits while the settlement is in flight, then activates
    /// the claim without reporting the note spent.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_pending_note_filled_by_our_settlement_is_not_reported_spent() {
        use crate::db::postgres_models::{SettlementAttemptRow, SettlementInputRow};
        use crate::db::postgres_schema::maker_lineages;
        use diesel::prelude::*;

        let mut f = fixture().await;
        let order = note(1, NoteType::Public);
        f.submit(1, &order).await;
        let row = NewOrderRow::ingested(&order, 1).unwrap();
        let tx_id = vec![7; 32];
        let attempt = SettlementAttemptRow {
            tx_id: tx_id.clone(),
            tx_result: vec![7],
            status: "prepared".into(),
        };
        let input = SettlementInputRow {
            tx_id: tx_id.clone(),
            parent_note_id: id(&order),
            child_note_id: None,
            child_note_data: None,
            fill_amount: Some(100),
        };
        f.db.pool
            .write(move |conn| {
                postgres_db::insert_orders_batch_tx(conn, &[row], 1)?;
                postgres_db::prepare_settlement_tx(conn, &attempt, &[input], None)?;
                Ok(())
            })
            .await
            .unwrap();
        // Our transaction lands before the watcher's next round.
        f.chain.commit(&order, 2);
        f.chain.spend(&order, 3);

        let maker_id = f.maker_id;
        let pool = f.db.pool.clone();
        let state = || {
            pool.read(|conn| {
                Ok(maker_lineages::table
                    .select(maker_lineages::state)
                    .first::<String>(conn)?)
            })
        };
        let events = || {
            pool.read(move |conn| {
                Ok(read_events_tx(conn, maker_id, 0, 100)?
                    .into_iter()
                    .map(|event| {
                        proto::EventBody::decode(event.payload.as_slice())
                            .unwrap()
                            .kind
                            .unwrap()
                    })
                    .collect::<Vec<_>>())
            })
        };
        let statuses = |events: &[proto::event_body::Kind]| {
            events
                .iter()
                .filter(|kind| matches!(kind, proto::event_body::Kind::OrderStatus(_)))
                .count()
        };

        // While the settlement is in flight the claim waits.
        f.watcher.round().await.unwrap();
        assert_eq!(state().await.unwrap(), "pending");
        assert_eq!(statuses(&events().await.unwrap()), 0);

        f.db.pool
            .write(move |conn| {
                postgres_db::confirm_settlement_tx(
                    conn,
                    &tx_id,
                    &HashSet::new(),
                    BlockNumber::from(3),
                    postgres_db::test_consumer(),
                    None,
                )?;
                Ok(())
            })
            .await
            .unwrap();
        f.watcher.round().await.unwrap();
        assert_eq!(state().await.unwrap(), "activated");
        let events = events().await.unwrap();
        assert!(
            events.iter().any(|kind| matches!(
                kind,
                proto::event_body::Kind::SettlementResolved(resolved) if resolved.committed
            )),
            "the fill is reported: {events:?}"
        );
        assert_eq!(
            statuses(&events),
            0,
            "a note our settlement filled is not reported unavailable: {events:?}"
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_live_order_spent_elsewhere_is_retired_and_reported() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.submit(1, &order).await;
        f.chain.commit(&order, 2);
        f.watcher.round().await.unwrap();
        f.book.try_recv().unwrap();
        // Next round starts watching it; then the maker reclaims it.
        f.watcher.round().await.unwrap();
        f.chain.spend(&order, 6);
        f.watcher.round().await.unwrap();

        let update = f.book.try_recv().unwrap();
        assert_eq!(update.removed, [order.id()]);
        assert_eq!(f.status(&order).await, "onchain_nullified");
        let statuses = f.statuses().await;
        assert_eq!(
            statuses.last().unwrap(),
            &(
                id(&order),
                proto::OrderState::Unavailable,
                "note spent outside this solver".to_string()
            )
        );
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_failed_round_changes_nothing_and_is_repeated_whole() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.submit(1, &order).await;
        f.chain.commit(&order, 2);
        f.chain.state().down = true;
        assert!(f.watcher.round().await.is_err());
        assert!(f.book.try_recv().is_err());

        f.chain.state().down = false;
        f.watcher.round().await.unwrap();
        assert_eq!(f.book.try_recv().unwrap().active[0].id(), order.id());
    }

    impl Fixture {
        /// Submit with live at accept on, the watcher's block at `block`.
        async fn accept(&self, seq: u64, note: &Note, block: u32) {
            let header = CommandHeader::new(self.maker_id, format!("a{seq}"), seq).unwrap();
            let command = MakerCommand::submit(note.clone()).unwrap();
            let outcome = self
                .db
                .pool
                .write(move |conn| {
                    use crate::db::postgres_schema::sync_state;
                    use diesel::prelude::*;
                    let epoch: i64 = sync_state::table
                        .find(1_i16)
                        .select(sync_state::owner_epoch)
                        .first(conn)?;
                    let mut gate =
                        maker_db::AcceptGate::new(epoch, move || Some(BlockNumber::from(block)));
                    maker_db::execute_command_with_tx(conn, &header, &command, Some(&mut gate))
                })
                .await
                .unwrap();
            assert!(outcome.live.is_some(), "live at accept");
        }

        /// Make every pending submission `secs` older.
        async fn age(&self, secs: u32) {
            self.db
                .pool
                .write(move |conn| {
                    use diesel::connection::SimpleConnection;
                    conn.batch_execute(&format!(
                        "UPDATE maker_lineages SET created_at = created_at - interval '{secs} seconds'"
                    ))?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        async fn set_status(&self, note: &Note, status: &'static str) {
            let id = note.id().to_bytes().to_vec();
            self.db
                .pool
                .write(move |conn| {
                    use crate::db::postgres_schema::orders;
                    use diesel::prelude::*;
                    diesel::update(orders::table.find(id))
                        .set(orders::status.eq(status))
                        .execute(conn)?;
                    Ok(())
                })
                .await
                .unwrap();
        }
    }

    /// The cap needs its time AND its blocks since accept, then a lookup that
    /// still misses the note.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_cap_waits_for_time_and_blocks_then_cancels() {
        let mut f = fixture().await;
        f.watcher = Watcher::new(
            f.db.pool.clone(),
            f.chain.clone(),
            f.watcher.book_tx.clone(),
        )
        .with_cap(UnconfirmedCap {
            secs: 60,
            blocks: 20,
        });
        let order = note(1, NoteType::Private);
        f.chain.state().tip = 10;
        f.accept(1, &order, 10).await;

        // Blocks passed, time not.
        f.chain.state().tip = 40;
        f.watcher.round().await.unwrap();
        assert_eq!(f.status(&order).await, "active");
        // Time passed, blocks not.
        f.chain.state().tip = 29;
        f.age(61).await;
        f.watcher.round().await.unwrap();
        assert_eq!(f.status(&order).await, "active");
        assert_eq!(f.chain.state().lookups, 0);
        // Both: one lookup, still missing, so the order is cancelled.
        f.chain.state().tip = 30;
        f.watcher.round().await.unwrap();
        assert_eq!(f.chain.state().lookups, 1);
        assert_eq!(f.status(&order).await, "stopped");
        let update = f.book.try_recv().unwrap();
        assert_eq!(update.removed, [order.id()]);
        assert_eq!(
            f.statuses().await,
            [
                (id(&order), proto::OrderState::Live, String::new()),
                (
                    id(&order),
                    proto::OrderState::Cancelled,
                    maker_db::NOTE_NOT_RECEIVED.to_owned()
                ),
            ]
        );
        // Closed: neither pending nor watched any more.
        f.watcher.round().await.unwrap();
        assert_eq!(f.chain.state().lookups, 1);
        assert_eq!(f.statuses().await.len(), 2);
    }

    /// A round that missed a landed note (a lagging node) does not cap it:
    /// the exact lookup finds it and counts as the round's observation.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_cap_lookup_finds_a_note_the_round_missed() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.accept(1, &order, 0).await;
        f.set_status(&order, "waiting").await;
        f.chain.commit(&order, 5);
        f.chain.state().hidden.insert(order.id());
        f.chain.state().tip = 100;
        f.age(3600).await;
        f.watcher.round().await.unwrap();

        assert_eq!(f.chain.state().lookups, 1);
        assert_eq!(f.status(&order).await, "active");
        let update = f.book.try_recv().unwrap();
        assert_eq!(update.active[0].id(), order.id(), "back from waiting");
        assert!(update.active[0].maker.is_some());
        assert_eq!(f.statuses().await.len(), 1, "LIVE only at accept");
    }

    /// One note the tracker cannot read gets no decision; the others do.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn one_unreadable_note_does_not_stop_the_round() {
        let mut f = fixture().await;
        let (bad, good) = (note(1, NoteType::Private), note(2, NoteType::Private));
        f.accept(1, &bad, 0).await;
        f.accept(2, &good, 0).await;
        f.set_status(&good, "waiting").await;
        f.chain.commit(&good, 3);
        f.chain.state().unreadable.insert(bad.id());
        f.chain.state().tip = 100;
        f.watcher.round().await.unwrap();

        assert_eq!(f.status(&good).await, "active");
        // Unread, and the cap not due yet: no decision, no lookup.
        assert_eq!(f.status(&bad).await, "active");
        assert_eq!(f.chain.state().lookups, 0);
    }

    /// A waiting order comes back once its note lands; an active one is
    /// activated in place and not sent again.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn landed_notes_activate_unconfirmed_orders_quietly() {
        let mut f = fixture().await;
        let (waiting, active) = (note(1, NoteType::Private), note(2, NoteType::Private));
        f.accept(1, &waiting, 0).await;
        f.accept(2, &active, 0).await;
        f.set_status(&waiting, "waiting").await;
        f.chain.commit(&waiting, 2);
        f.chain.commit(&active, 2);
        f.watcher.round().await.unwrap();

        let update = f.book.try_recv().unwrap();
        let sent: Vec<_> = update.active.iter().map(|order| order.id()).collect();
        assert_eq!(sent, [waiting.id()]);
        assert_eq!(f.status(&waiting).await, "active");
        assert_eq!(f.statuses().await.len(), 2, "LIVE only at accept");
        // Activated: now watched like any live maker order.
        f.chain.spend(&active, 3);
        f.watcher.round().await.unwrap();
        assert_eq!(f.status(&active).await, "onchain_nullified");
        assert_eq!(
            f.statuses().await.last().unwrap(),
            &(
                id(&active),
                proto::OrderState::Unavailable,
                "note spent outside this solver".to_owned()
            )
        );
    }

    /// The watcher publishes its block for intake, fresh only while recent.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_synced_height_is_fresh_only_after_a_recent_round() {
        let mut f = fixture().await;
        let height = f.watcher.synced_height();
        assert_eq!(height.fresh(SyncedHeight::MAX_AGE), None);
        f.chain.state().tip = 12;
        f.watcher.round().await.unwrap();
        assert_eq!(
            height.fresh(SyncedHeight::MAX_AGE),
            Some(BlockNumber::from(12))
        );
        assert_eq!(height.fresh(Duration::ZERO), None);
        // A failed round publishes nothing new.
        f.chain.state().down = true;
        f.chain.state().tip = 13;
        assert!(f.watcher.round().await.is_err());
        assert_eq!(
            height.fresh(SyncedHeight::MAX_AGE),
            Some(BlockNumber::from(12))
        );
    }

    /// A spawn note committed by a maker wallet, and the wallet's executed
    /// transaction that spends it, on a mock chain.
    async fn spawned_note() -> (
        Note,
        miden_protocol::transaction::ExecutedTransaction,
        Arc<MockRpcApi>,
    ) {
        use miden_protocol::transaction::RawOutputNote;
        use miden_testing::Auth;
        let mut builder = MockChain::builder();
        let asset =
            FungibleAsset::new(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(), 100).unwrap();
        let sender = builder
            .add_existing_wallet_with_assets(Auth::IncrNonce, [asset.into()])
            .unwrap();
        let out = note_for_creator(720, NoteType::Public, sender.id());
        let spawn = builder.add_spawn_note([&out]).unwrap();
        let chain = builder.build().unwrap();
        let spend = chain
            .build_transaction(sender)
            .authenticated_input_note(spawn.id())
            .expected_output_note(RawOutputNote::Full(out))
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let rpc = Arc::new(MockRpcApi::new(chain));
        rpc.prove_block();
        (spawn, spend, rpc)
    }

    async fn tracker(rpc: &Arc<MockRpcApi>, dir: &tempfile::TempDir) -> SdkTracker {
        let client = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(dir.path().join("maker.sqlite3"))
            .build()
            .await
            .unwrap();
        SdkTracker::new(client, rpc.clone())
    }

    /// The cap's lookup finds a note an earlier round missed (a lagging
    /// node). The next round imports it again, so the store holds it
    /// committed and later sees it spent.
    #[tokio::test]
    async fn sdk_tracker_resolves_a_note_its_cap_lookup_found() {
        let (spawn, spend, rpc) = spawned_note().await;
        let dir = tempfile::tempdir().unwrap();
        let mut tracker = tracker(&rpc, &dir).await;
        tracker.observe(&[]).await.unwrap();
        let cursor = tracker.client.get_sync_height().await.unwrap();
        // What a round leaves when the node answered "not found".
        tracker
            .client
            .import_notes(&[NoteFile::ExpectedNote {
                details: spawn.clone().into(),
                sync_hint: NoteSyncHint::new(cursor.child(), spawn.metadata().tag()),
            }])
            .await
            .unwrap();
        tracker.tracked.insert(spawn.id());
        assert_eq!(seen(&mut tracker, &spawn).await, NoteObservation::Pending);
        assert_eq!(
            tracker.lookup(&spawn).await.unwrap(),
            NoteObservation::Committed
        );
        assert_eq!(seen(&mut tracker, &spawn).await, NoteObservation::Committed);
        // The maker spends it: the next round sees it.
        rpc.mock_chain
            .write()
            .add_pending_executed_transaction(&spend)
            .unwrap();
        rpc.prove_block();
        assert_eq!(seen(&mut tracker, &spawn).await, NoteObservation::Consumed);
    }

    async fn seen(tracker: &mut SdkTracker, note: &Note) -> NoteObservation {
        tracker.observe(std::slice::from_ref(note)).await.unwrap()[&note.id()]
    }

    /// A stored record of another note with the same details can't be
    /// replaced by an import: the new note is decided by an exact lookup, and
    /// the round goes on for every other note.
    #[tokio::test]
    async fn sdk_tracker_decides_a_note_sharing_details_with_a_record_by_lookup() {
        use miden_protocol::transaction::RawOutputNote;
        let old = note(730, NoteType::Public);
        let new = note(730, NoteType::Private);
        assert_eq!(old.details_commitment(), new.details_commitment());
        assert_ne!(old.id(), new.id());
        let good = note(731, NoteType::Private);
        let mut builder = MockChain::builder();
        for committed in [&old, &new, &good] {
            builder.add_output_note(RawOutputNote::Full(committed.clone()));
        }
        let rpc = Arc::new(MockRpcApi::new(builder.build().unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let mut tracker = tracker(&rpc, &dir).await;
        assert_eq!(
            tracker.observe(std::slice::from_ref(&old)).await.unwrap()[&old.id()],
            NoteObservation::Committed
        );
        for _ in 0..2 {
            let seen = tracker.observe(&[new.clone(), good.clone()]).await.unwrap();
            assert_eq!(seen[&new.id()], NoteObservation::Committed);
            assert_eq!(seen[&good.id()], NoteObservation::Committed);
        }
    }

    /// A node error on the cap's lookup cancels nothing; the next round
    /// tries again.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_failed_cap_lookup_cancels_nothing() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.accept(1, &order, 0).await;
        f.chain.state().tip = 100;
        f.age(3600).await;
        f.chain.state().lookup_fails = true;
        f.watcher.round().await.unwrap();
        assert_eq!(f.chain.state().lookups, 1);
        assert_eq!(f.status(&order).await, "active");
        assert_eq!(f.statuses().await.len(), 1, "LIVE only");

        f.chain.state().lookup_fails = false;
        f.watcher.round().await.unwrap();
        assert_eq!(f.status(&order).await, "stopped");
    }

    /// An unconfirmed order whose note the tracker can never read is still
    /// decided once the cap is due, by its exact lookup.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_cap_decides_an_unreadable_note() {
        let mut f = fixture().await;
        let (missing, landed) = (note(1, NoteType::Private), note(2, NoteType::Private));
        f.accept(1, &missing, 0).await;
        f.accept(2, &landed, 0).await;
        f.chain.commit(&landed, 3);
        for order in [&missing, &landed] {
            f.chain.state().unreadable.insert(order.id());
        }
        f.chain.state().tip = 100;
        // Not due yet: no decision for either.
        f.watcher.round().await.unwrap();
        assert_eq!(f.chain.state().lookups, 0);
        assert_eq!(
            crate::gateway::watcher_gauges().map(|g| g.unobserved >= 2),
            Some(true)
        );
        f.age(3600).await;
        f.watcher.round().await.unwrap();
        assert_eq!(f.chain.state().lookups, 2);
        assert_eq!(f.status(&missing).await, "stopped");
        assert_eq!(f.status(&landed).await, "active");
    }

    /// The cap tells a maker nothing about an order it cancelled itself.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_cap_sends_nothing_after_a_cancel_all() {
        let mut f = fixture().await;
        let order = note(1, NoteType::Private);
        f.accept(1, &order, 0).await;
        f.command(
            "c2",
            2,
            MakerCommand::CancelAll {
                scope: CutoffScope::all(),
            },
        )
        .await;
        f.chain.state().tip = 100;
        f.age(3600).await;
        f.watcher.round().await.unwrap();
        assert_eq!(f.statuses().await.len(), 1, "LIVE only, no CANCELLED");
    }

    /// Notes whose cap lookups keep failing can't take the whole per-round
    /// lookup budget: the next round starts its lookups further on.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn failing_cap_lookups_do_not_hold_the_budget() {
        let mut f = fixture().await;
        let orders: Vec<_> = (1..=MAX_CAP_LOOKUPS as u32 + 1)
            .map(|serial| note(serial, NoteType::Private))
            .collect();
        for (seq, order) in orders.iter().enumerate() {
            f.accept(seq as u64 + 1, order, 0).await;
        }
        let (stuck, last) = orders.split_at(MAX_CAP_LOOKUPS);
        f.chain
            .state()
            .failing_lookups
            .extend(stuck.iter().map(Note::id));
        f.chain.state().tip = 100;
        f.age(3600).await;
        for _ in 0..2 {
            f.watcher.round().await.unwrap();
        }
        assert_eq!(f.status(&last[0]).await, "stopped");
        for order in stuck {
            assert_eq!(f.status(order).await, "active");
        }
    }
}

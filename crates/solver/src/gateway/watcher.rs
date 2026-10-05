//! Tracks maker notes with a dedicated Miden client. The SDK owns note
//! inclusion, nullifier sync and its chain cursor; PostgreSQL owns whether an
//! order is live and which maker events have been committed.
//!
//! A round can be repeated after any failure. The SDK store may advance before
//! the PostgreSQL write; pending and live rows are reconciled against the SDK
//! on every round, so a restart or failed write does not lose a transition.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteFile;
use miden_client::rpc::NodeRpcClient;
use miden_client::store::NoteFilter;
use miden_client::Client;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::{Note, NoteId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::proto::OrderState;
use crate::db::maker_db;
use crate::db::{postgres_db, DbPool};
use crate::ingest::{ChainError, ChainResult};
use crate::types::{now_unix, BookUpdate, OrderId};

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
    async fn observe(&mut self, notes: &[Note]) -> ChainResult<HashMap<NoteId, NoteObservation>>;
}

/// The SDK's client store is dedicated to maker notes, separate from ingest.
/// An exact-ID RPC fallback handles the exceptional case in which an expected
/// record with the same details commitment resolved to another note's metadata.
pub struct SdkTracker {
    client: Client<FilesystemKeyStore>,
    rpc: Arc<dyn NodeRpcClient>,
}

impl SdkTracker {
    pub fn new(client: Client<FilesystemKeyStore>, rpc: Arc<dyn NodeRpcClient>) -> Self {
        Self { client, rpc }
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
        if found.id() != note.id() || found.metadata() != note.metadata() {
            return Ok(NoteObservation::Mismatched);
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
    async fn observe(&mut self, notes: &[Note]) -> ChainResult<HashMap<NoteId, NoteObservation>> {
        if notes.is_empty() {
            self.client.sync_chain().await?;
            return Ok(HashMap::new());
        }

        // Reconcile from PostgreSQL, including notes committed before the
        // gateway received their submission. Import only details absent from
        // the SDK store; importing an ExpectedNote performs historical sync.
        let commitments: Vec<_> = notes.iter().map(Note::details_commitment).collect();
        let mut count_by_commitment = BTreeMap::new();
        for commitment in &commitments {
            *count_by_commitment.entry(*commitment).or_insert(0_usize) += 1;
        }
        let known: BTreeSet<_> = self
            .client
            .get_input_notes(NoteFilter::DetailsCommitments(commitments.clone()))
            .await?
            .iter()
            .map(|record| record.details_commitment())
            .collect();
        let mut to_import = Vec::new();
        let mut queued = BTreeSet::new();
        for note in notes {
            let commitment = note.details_commitment();
            if !known.contains(&commitment) && queued.insert(commitment) {
                to_import.push(NoteFile::from(note.clone()));
            }
        }
        if !to_import.is_empty() {
            self.client.import_notes(&to_import).await?;
        }

        // The SDK persists its chain cursor and note transitions. It also
        // checks nullifiers for every tracked unspent note.
        self.client.sync_chain().await?;
        let records: BTreeMap<_, _> = self
            .client
            .get_input_notes(NoteFilter::DetailsCommitments(commitments))
            .await?
            .into_iter()
            .map(|record| (record.details_commitment(), record))
            .collect();
        let mut observed = HashMap::with_capacity(notes.len());
        for note in notes {
            // The SDK stores only one expected record per details commitment.
            // If two submitted IDs share it, use exact SDK RPC lookup for both
            // rather than treating either record as the other's commitment.
            if count_by_commitment[&note.details_commitment()] > 1 {
                observed.insert(note.id(), self.exact_lookup(note).await?);
                continue;
            }
            let record = records
                .get(&note.details_commitment())
                .ok_or(ChainError::MissingSyncedNote(note.id()))?;
            let state =
                if record.id() == Some(note.id()) && record.metadata() == Some(note.metadata()) {
                    if record.is_consumed() {
                        NoteObservation::Consumed
                    } else if record.is_committed() {
                        NoteObservation::Committed
                    } else {
                        NoteObservation::Pending
                    }
                } else if record.id().is_some() {
                    self.exact_lookup(note).await?
                } else {
                    NoteObservation::Pending
                };
            observed.insert(note.id(), state);
        }
        Ok(observed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Db(#[from] crate::db::DbError),
    #[error("tracker omitted note {0}")]
    MissingObservation(NoteId),
}

pub struct Watcher<T> {
    pool: DbPool,
    tracker: T,
    book_tx: mpsc::Sender<BookUpdate>,
    // PostgreSQL supplies the authoritative set; this cache avoids decoding
    // every unchanged live note on every round. It is rebuilt after restart.
    watched: HashMap<Vec<u8>, Note>,
}

impl<T: MakerTracker> Watcher<T> {
    pub fn new(pool: DbPool, tracker: T, book_tx: mpsc::Sender<BookUpdate>) -> Self {
        Self {
            pool,
            tracker,
            book_tx,
            watched: HashMap::new(),
        }
    }

    /// Reconcile pending submissions and previously live maker orders against
    /// the SDK, then commit every resulting order and event change together.
    pub async fn round(&mut self) -> Result<(), WatchError> {
        let pending = self.pool.read(maker_db::pending_submissions_tx).await?;
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
        let observations = self
            .tracker
            .observe(&notes.into_values().collect::<Vec<_>>())
            .await?;
        for note in pending
            .iter()
            .map(|submission| &submission.note)
            .chain(self.watched.values())
        {
            if !observations.contains_key(&note.id()) {
                return Err(WatchError::MissingObservation(note.id()));
            }
        }

        let mut activations = Vec::new();
        let mut rejections = Vec::new();
        for submission in pending {
            match observations[&submission.note.id()] {
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

        if !activations.is_empty() || !rejections.is_empty() || !spent_ids.is_empty() {
            let removed = spent_ids
                .iter()
                .map(|id| Ok(OrderId::read_from_bytes(id)?))
                .collect::<Result<Vec<_>, crate::db::DbError>>()?;
            let writes = (activations.clone(), rejections.clone(), spent_ids.clone());
            self.pool
                .write_book(&self.book_tx, move |conn| {
                    let (activations, rejections, spent_ids) = writes;
                    maker_db::reject_tx(conn, &rejections)?;
                    postgres_db::mark_orders_onchain_nullified_tx(conn, &spent_ids)?;
                    let mut update = maker_db::activate_tx(conn, &activations, now_unix())?;
                    update.removed.extend(removed);
                    Ok(update)
                })
                .await?;
            tracing::info!(
                activated = activations.len(),
                rejected = rejections.len(),
                spent = spent_ids.len(),
                "maker notes verified"
            );
        }
        for id in spent_ids {
            self.watched.remove(&id);
        }
        Ok(())
    }
}

pub async fn run_watcher<T: MakerTracker>(
    mut watcher: Watcher<T>,
    interval: Duration,
    cancel: CancellationToken,
) {
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

    #[async_trait(?Send)]
    impl MakerTracker for FakeChain {
        async fn observe(
            &mut self,
            notes: &[Note],
        ) -> ChainResult<HashMap<NoteId, NoteObservation>> {
            let state = self.state();
            up(&state)?;
            Ok(notes
                .iter()
                .map(|note| {
                    let observation = match state.notes.get(&note.id()) {
                        None => NoteObservation::Pending,
                        Some((_, metadata)) if metadata != note.metadata() => {
                            NoteObservation::Mismatched
                        }
                        Some(_) if state.spent.contains_key(&note.nullifier()) => {
                            NoteObservation::Consumed
                        }
                        Some(_) => NoteObservation::Committed,
                    };
                    (note.id(), observation)
                })
                .collect())
        }
    }

    fn note(serial: u32, note_type: NoteType) -> Note {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
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

        let first = tracker.observe(&[committed.clone()]).await.unwrap();
        assert_eq!(first[&committed.id()], NoteObservation::Committed);

        drop(tracker);
        let reopened = ClientBuilder::new()
            .rpc(rpc.clone())
            .sqlite_store(store_path)
            .build()
            .await
            .unwrap();
        let mut tracker = SdkTracker::new(reopened, rpc);
        let after_restart = tracker.observe(&[committed.clone()]).await.unwrap();
        assert_eq!(after_restart[&committed.id()], NoteObservation::Committed);
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

        let update = f.book.try_recv().unwrap();
        assert_eq!(update.active[0].id(), order.id());
        assert!(
            update.active[0].maker.is_some(),
            "now tagged as a maker order"
        );
        assert_eq!(f.statuses().await.len(), 1);
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
}

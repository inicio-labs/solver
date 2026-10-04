//! The maker-note watcher (ADR 0003). It makes accepted submits Live once
//! their notes are verified committed and unspent, and retires maker orders
//! whose notes are spent elsewhere. It uses node RPC only: maker notes are
//! never imported into the ingest client, so no private note reaches the
//! book without the gateway's checks.
//!
//! Each round:
//! 1. new submissions are looked up by note ID once (catches notes committed
//!    before their data arrived);
//! 2. older pending ones are found through their note tags over the blocks
//!    committed since the last round, never by re-querying every ID;
//! 3. a found note must match what was submitted and be unspent;
//! 4. watched maker orders are checked for spends in the new blocks (a newly
//!    watched one over its whole history);
//! 5. one `write_book` commits every activation, rejection and spend, with
//!    their OrderStatus events and the book update.
//!
//! A round that fails changes nothing and is repeated whole. The node is
//! trusted for inclusion, as the ingest and executor paths already trust it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use miden_client::rpc::domain::note::FetchedNote;
use miden_client::rpc::{NodeRpcClient, RpcLimits};
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::{NoteId, NoteMetadata, NoteTag, Nullifier};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::proto::OrderState;
use crate::db::maker_db::{self, PendingSubmission};
use crate::db::{postgres_db, DbPool};
use crate::ingest::{ChainError, ChainResult};
use crate::types::{now_unix, BookUpdate, OrderId};

/// What the watcher needs from the chain.
#[async_trait]
pub trait MakerChain: Send + Sync {
    /// The committed chain tip.
    async fn tip(&mut self) -> ChainResult<BlockNumber>;
    /// The committed notes among `ids`, with their committed metadata.
    async fn committed(&mut self, ids: &[NoteId]) -> ChainResult<HashMap<NoteId, NoteMetadata>>;
    /// Notes committed in `from..=to` carrying one of `tags`.
    async fn tagged(
        &mut self,
        from: BlockNumber,
        to: BlockNumber,
        tags: &BTreeSet<NoteTag>,
    ) -> ChainResult<HashMap<NoteId, NoteMetadata>>;
    /// Which of `nullifiers` were spent in `from..=to`.
    async fn spent(
        &mut self,
        nullifiers: &[Nullifier],
        from: BlockNumber,
        to: BlockNumber,
    ) -> ChainResult<HashSet<Nullifier>>;
}

/// [`MakerChain`] over the solver's node RPC, in requests sized to the
/// node's published limits.
pub struct RpcChain {
    rpc: Arc<dyn NodeRpcClient>,
    limits: Option<RpcLimits>,
}

impl RpcChain {
    pub fn new(rpc: Arc<dyn NodeRpcClient>) -> Self {
        Self { rpc, limits: None }
    }

    async fn limits(&mut self) -> ChainResult<RpcLimits> {
        if let Some(limits) = self.limits {
            return Ok(limits);
        }
        let limits = self.rpc.get_rpc_limits().await?;
        self.limits = Some(limits);
        Ok(limits)
    }
}

fn chunk_size(limit: u32) -> usize {
    usize::try_from(limit).unwrap_or(usize::MAX).max(1)
}

#[async_trait]
impl MakerChain for RpcChain {
    async fn tip(&mut self) -> ChainResult<BlockNumber> {
        let (header, _) = self.rpc.get_block_header_by_number(None, false).await?;
        Ok(header.block_num())
    }

    async fn committed(&mut self, ids: &[NoteId]) -> ChainResult<HashMap<NoteId, NoteMetadata>> {
        let size = chunk_size(self.limits().await?.note_ids_limit);
        let mut found = HashMap::new();
        for chunk in ids.chunks(size) {
            for note in self.rpc.get_notes_by_id(chunk).await? {
                let id = match &note {
                    FetchedNote::Private(id, ..) => *id,
                    FetchedNote::Public(note, _) => note.id(),
                };
                found.insert(id, *note.metadata());
            }
        }
        Ok(found)
    }

    async fn tagged(
        &mut self,
        from: BlockNumber,
        to: BlockNumber,
        tags: &BTreeSet<NoteTag>,
    ) -> ChainResult<HashMap<NoteId, NoteMetadata>> {
        let size = chunk_size(self.limits().await?.note_tags_limit);
        let tags: Vec<NoteTag> = tags.iter().copied().collect();
        let mut found = HashMap::new();
        for chunk in tags.chunks(size) {
            let chunk: BTreeSet<NoteTag> = chunk.iter().copied().collect();
            for block in self.rpc.sync_notes(from, to, &chunk).await? {
                for (id, note) in block.notes {
                    found.insert(id, *note.metadata());
                }
            }
        }
        Ok(found)
    }

    async fn spent(
        &mut self,
        nullifiers: &[Nullifier],
        from: BlockNumber,
        to: BlockNumber,
    ) -> ChainResult<HashSet<Nullifier>> {
        let size = chunk_size(self.limits().await?.nullifiers_limit);
        let wanted: HashSet<Nullifier> = nullifiers.iter().copied().collect();
        let prefixes: Vec<u16> = wanted
            .iter()
            .map(Nullifier::prefix)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut spent = HashSet::new();
        for chunk in prefixes.chunks(size) {
            for update in self.rpc.sync_nullifiers(chunk, from, to).await? {
                if wanted.contains(&update.nullifier) {
                    spent.insert(update.nullifier);
                }
            }
        }
        Ok(spent)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Db(#[from] crate::db::DbError),
}

/// The watcher's memory; all of it is rebuilt from the database and chain
/// after a restart.
pub struct Watcher<C> {
    pool: DbPool,
    chain: C,
    book_tx: mpsc::Sender<BookUpdate>,
    /// The last block whose tags and nullifiers were scanned.
    cursor: Option<BlockNumber>,
    /// Pending notes already looked up by ID; later found by tag.
    looked_up: HashSet<NoteId>,
    /// Maker orders watched for spends, by stored note ID.
    watched: HashMap<Vec<u8>, Nullifier>,
}

impl<C: MakerChain> Watcher<C> {
    pub fn new(pool: DbPool, chain: C, book_tx: mpsc::Sender<BookUpdate>) -> Self {
        Self {
            pool,
            chain,
            book_tx,
            cursor: None,
            looked_up: HashSet::new(),
            watched: HashMap::new(),
        }
    }

    /// One round. Nothing is remembered unless its write committed.
    pub async fn round(&mut self) -> Result<(), WatchError> {
        let tip = self.chain.tip().await?;
        // Blocks not scanned yet; `None` when the tip has not moved.
        let fresh = match self.cursor {
            Some(cursor) if tip <= cursor => None,
            Some(cursor) => Some(cursor.child()),
            None => Some(tip),
        };

        // 1–2: find committed submissions.
        let pending = self.pool.read(maker_db::pending_submissions_tx).await?;
        let new_ids: Vec<NoteId> = pending
            .iter()
            .map(|submission| submission.note.id())
            .filter(|id| !self.looked_up.contains(id))
            .collect();
        let mut committed = self.chain.committed(&new_ids).await?;
        if let (Some(from), Some(_)) = (fresh, self.cursor) {
            let tags: BTreeSet<NoteTag> = pending
                .iter()
                .filter(|submission| self.looked_up.contains(&submission.note.id()))
                .map(|submission| submission.note.metadata().tag())
                .collect();
            if !tags.is_empty() {
                // Other notes share these tags; only submitted IDs count.
                committed.extend(self.chain.tagged(from, tip, &tags).await?);
            }
        }

        // 3: a found note must be the submitted one, and unspent.
        let mut found = Vec::new();
        let mut rejections = Vec::new();
        for submission in &pending {
            match committed.get(&submission.note.id()) {
                None => {}
                Some(metadata) if metadata != submission.note.metadata() => rejections.push((
                    submission.clone(),
                    OrderState::Rejected,
                    "committed note differs from the submitted note",
                )),
                Some(_) => found.push(submission.clone()),
            }
        }
        let nullifiers: Vec<Nullifier> = found.iter().map(|s| s.note.nullifier()).collect();
        let spent_before = self
            .chain
            .spent(&nullifiers, BlockNumber::GENESIS, tip)
            .await?;
        let (spent, activations): (Vec<PendingSubmission>, Vec<PendingSubmission>) = found
            .into_iter()
            .partition(|submission| spent_before.contains(&submission.note.nullifier()));
        rejections.extend(spent.into_iter().map(|submission| {
            (
                submission,
                OrderState::Unavailable,
                "note spent before it became live",
            )
        }));

        // 4: maker orders spent elsewhere.
        let watched_ids = self.pool.read(maker_db::watched_maker_orders_tx).await?;
        let newly_watched: Vec<Vec<u8>> = watched_ids
            .iter()
            .filter(|id| !self.watched.contains_key(*id))
            .cloned()
            .collect();
        let new_notes = self
            .pool
            .read(move |conn| maker_db::order_notes_tx(conn, &newly_watched))
            .await?;
        let mut watched: HashMap<Vec<u8>, Nullifier> = watched_ids
            .iter()
            .filter_map(|id| Some((id.clone(), *self.watched.get(id)?)))
            .collect();
        let new_nullifiers: Vec<Nullifier> =
            new_notes.iter().map(|note| note.nullifier()).collect();
        let mut spent_orders = self
            .chain
            .spent(&new_nullifiers, BlockNumber::GENESIS, tip)
            .await?;
        if let (Some(from), Some(_)) = (fresh, self.cursor) {
            let known: Vec<Nullifier> = watched.values().copied().collect();
            spent_orders.extend(self.chain.spent(&known, from, tip).await?);
        }
        for note in &new_notes {
            watched.insert(note.id().to_bytes().to_vec(), note.nullifier());
        }
        let spent_ids: Vec<Vec<u8>> = watched
            .iter()
            .filter(|(_, nullifier)| spent_orders.contains(nullifier))
            .map(|(id, _)| id.clone())
            .collect();

        // 5: one commit for everything found.
        if !activations.is_empty() || !rejections.is_empty() || !spent_ids.is_empty() {
            let removed = spent_ids
                .iter()
                .map(|id| Ok(OrderId::read_from_bytes(id)?))
                .collect::<Result<Vec<_>, crate::db::DbError>>()?;
            let writes = (activations.clone(), rejections.clone(), spent_ids.clone());
            self.pool
                .write_book(&self.book_tx, move |conn| {
                    let (activations, rejections, spent_ids) = writes;
                    maker_db::prelock_watcher_tx(conn, &activations, &rejections, &spent_ids)?;
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

        // The round committed: remember what it learned.
        for id in &spent_ids {
            watched.remove(id);
        }
        self.watched = watched;
        self.looked_up = pending
            .iter()
            .map(|submission| submission.note.id())
            .filter(|id| !committed.contains_key(id))
            .collect();
        if fresh.is_some() {
            self.cursor = Some(tip);
        }
        Ok(())
    }
}

/// Run rounds every `interval` until `cancel` fires. A failed round is
/// logged and repeated whole on the next tick.
pub async fn run_watcher<C: MakerChain>(
    mut watcher: Watcher<C>,
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
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use prost::Message;
    use std::sync::Mutex;

    #[derive(Default)]
    struct ChainState {
        tip: u32,
        notes: HashMap<NoteId, (u32, NoteMetadata)>,
        spent: HashMap<Nullifier, u32>,
        down: bool,
        looked_up_by_id: Vec<NoteId>,
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

    #[async_trait]
    impl MakerChain for FakeChain {
        async fn tip(&mut self) -> ChainResult<BlockNumber> {
            let state = self.state();
            up(&state)?;
            Ok(state.tip.into())
        }

        async fn committed(
            &mut self,
            ids: &[NoteId],
        ) -> ChainResult<HashMap<NoteId, NoteMetadata>> {
            let mut state = self.state();
            up(&state)?;
            state.looked_up_by_id.extend(ids);
            Ok(ids
                .iter()
                .filter_map(|id| Some((*id, state.notes.get(id)?.1)))
                .collect())
        }

        async fn tagged(
            &mut self,
            from: BlockNumber,
            to: BlockNumber,
            tags: &BTreeSet<NoteTag>,
        ) -> ChainResult<HashMap<NoteId, NoteMetadata>> {
            let state = self.state();
            up(&state)?;
            Ok(state
                .notes
                .iter()
                .filter(|(_, (block, metadata))| {
                    (from.as_u32()..=to.as_u32()).contains(block) && tags.contains(&metadata.tag())
                })
                .map(|(id, (_, metadata))| (*id, *metadata))
                .collect())
        }

        async fn spent(
            &mut self,
            nullifiers: &[Nullifier],
            from: BlockNumber,
            to: BlockNumber,
        ) -> ChainResult<HashSet<Nullifier>> {
            let state = self.state();
            up(&state)?;
            Ok(nullifiers
                .iter()
                .filter(|nullifier| {
                    state
                        .spent
                        .get(nullifier)
                        .is_some_and(|block| (from.as_u32()..=to.as_u32()).contains(block))
                })
                .copied()
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
        assert_eq!(
            f.chain.state().looked_up_by_id,
            [order.id()],
            "looked up by ID once, then found by tag"
        );
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

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use diesel::{Connection, SqliteConnection};
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::rpc::{NodeRpcClient, RpcError};
use miden_client::{Client, ClientError};
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::db::models::{NoteRow, OrderRow};
use crate::db::{self, DbPool};
use crate::types::Order as PipelineOrder;
use crate::types::{BookOrder, BookUpdate, OrderStatus, TokenId};

/// Result of a sync_state call — newly received notes plus IDs of notes
/// whose nullifier was just observed on-chain. The matcher uses the
/// latter to surgically drop zombie orders from its in-memory book.
pub struct SyncResult {
    pub block_num: u64,
    pub new_notes: Vec<Note>,
    pub consumed_notes: Vec<NoteId>,
}

/// Trait abstracting the Miden Node RPC client.
///
/// No `Send` bound: the production adapter wraps `Client<FilesystemKeyStore>`
/// which has `Arc<dyn Trait>` fields without `Send + Sync` bounds upstream,
/// making the whole `Client` `!Send`. All tasks that own a `MidenClient`
/// run on a single-threaded `LocalSet`, so cross-thread migration is
/// forbidden by construction.
#[async_trait(?Send)]
pub trait MidenClient {
    /// Register note tags for a trading pair (both directions).
    /// Must be called before sync_state to receive notes for this pair.
    async fn subscribe_pair(&mut self, offered: TokenId, requested: TokenId) -> Result<()>;

    /// Sync client state with the Miden Node.
    /// Returns the new block number, newly received notes, and IDs of notes
    /// whose nullifiers just appeared on-chain.
    async fn sync_state(&mut self) -> Result<SyncResult>;

    /// Included notes retained by the client, including notes discovered before
    /// a crash interrupted persistence into the solver database.
    async fn stored_notes(&mut self) -> Result<Vec<Note>>;

    /// Given a slice of notes the solver currently believes are matchable,
    /// return the subset whose nullifiers are already on-chain. Used by the
    /// executor after a non-RPC submit error to identify which input notes
    /// are zombies vs. which are still legitimately active.
    async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>>;

    /// Whether a known settlement output has an inclusion record on-chain.
    async fn note_is_included(&mut self, note_id: NoteId) -> Result<bool>;

    /// Fetch a public fungible faucet's on-chain metadata `(decimals, ticker)`
    /// by id. Returns `None` if the account isn't a public faucet / doesn't
    /// exist. Keyless — no signing, no pre-tracking.
    async fn fetch_token_metadata(&mut self, faucet_id: TokenId) -> Result<Option<(u8, String)>>;

    /// The chain tip's fee parameters `(fee faucet, verification base fee)`, or
    /// `None` when this client can't report them — mocks, which skip the
    /// executor's fee pre-flight.
    async fn fee_parameters(&mut self) -> Result<Option<(TokenId, u32)>> {
        Ok(None)
    }
}

/// Run the note ingestion loop.
///
/// Each tick: sync → fetch new notes by ID → filter PSWAP → atomic DB insert → send to channel.
/// On a successful tick the `last_sync_unix_seconds` atomic is bumped so the
/// observability `/readyz` endpoint can detect a stalled ingest.
#[allow(clippy::too_many_arguments)]
pub async fn run_ingest(
    client: Arc<Mutex<dyn MidenClient>>,
    pool: DbPool,
    book_tx: mpsc::Sender<BookUpdate>,
    interval: Duration,
    cancel: CancellationToken,
    last_sync_unix_seconds: Arc<AtomicI64>,
    solver_id: AccountId,
) {
    loop {
        let sync = match client.lock().await.sync_state().await {
            Ok(sync) => Some(sync),
            Err(error) if is_rpc_error(&error) => {
                tracing::warn!(%error, "ingest RPC failed; retrying next tick");
                None
            }
            Err(error) => {
                tracing::error!(%error, "ingest client failed; stopping pipeline");
                cancel.cancel();
                return;
            }
        };

        if let Some(sync) = sync {
            // Sync may already have advanced the client's durable cursor. A DB
            // or matcher-channel failure now needs coordinated restart so boot
            // can replay the client's stored discoveries without losing them.
            if let Err(error) = pool
                .update_book(&book_tx, |conn| sync.persist(conn, solver_id))
                .await
            {
                tracing::error!(%error, "persisting ingest update failed; stopping pipeline");
                cancel.cancel();
                return;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            last_sync_unix_seconds.store(now, Ordering::Relaxed);
        }
        // The idle wait stays cancellable (and stays at the end so the first
        // tick fires immediately, not after `interval`). This is where a
        // shutdown almost always lands, since the loop is asleep most of the
        // time — so cancellation is still effectively instant in practice.
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    tracing::info!("ingest cancelled, shutting down");
}

fn is_rpc_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<ClientError>()
        .is_some_and(|error| matches!(error, ClientError::RpcError(_)))
        || error.downcast_ref::<RpcError>().is_some()
}

impl SyncResult {
    /// Replay the client's durable discoveries, not only the next sync's delta.
    /// RPC checks are bounded and happen before the SQLite transaction.
    pub async fn recover(
        client: &mut dyn MidenClient,
        pool: &DbPool,
        solver_id: AccountId,
    ) -> Result<()> {
        let mut sync = client.sync_state().await?;
        sync.new_notes = client.stored_notes().await?;
        for notes in sync
            .new_notes
            .chunks(miden_protocol::MAX_INPUT_NOTES_PER_TX)
        {
            sync.consumed_notes
                .extend(client.check_consumed_notes(notes).await?);
        }
        sync.persist(&mut *pool.write_conn()?, solver_id)?;
        Ok(())
    }

    /// Persist one observed chain update as one transaction. Removals win over
    /// additions when a sync both discovers and consumes the same remainder.
    fn persist(self, conn: &mut SqliteConnection, solver_id: AccountId) -> Result<BookUpdate> {
        conn.transaction(|conn| self.persist_notes(conn, solver_id))
    }

    fn persist_notes(
        self,
        conn: &mut SqliteConnection,
        solver_id: AccountId,
    ) -> Result<BookUpdate> {
        let SyncResult {
            block_num,
            new_notes,
            consumed_notes,
        } = self;

        // An included expected remainder confirms its whole settlement. Do this
        // before processing nullifiers so our own parent becomes Executed rather
        // than being mistaken for an externally consumed order.
        let mut expected_children = HashSet::new();
        let mut update = BookUpdate::default();
        for note in &new_notes {
            if let Some(outcome) =
                db::confirm_expected_remainder(conn, note.id().to_bytes().as_slice())?
            {
                expected_children.insert(note.id());
                update.removed.extend(outcome.removed);
                update.active.extend(outcome.active);
            }
        }

        // Parse ordinary PSWAP notes after recognizing linked remainders.
        let mut db_notes = Vec::new();
        let mut db_orders = Vec::new();
        let mut ingest_orders = Vec::new();

        for note in &new_notes {
            if expected_children.contains(&note.id()) {
                continue;
            }
            if note.recipient().script().root() != PswapNote::script_root() {
                continue;
            }

            let order = match PipelineOrder::from_note(note) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(note_id = %note.id(), error = %e, "skipping unparseable PSWAP note");
                    continue;
                }
            };

            // A note naming the solver as its creator takes PSWAP's reclaim branch when
            // the solver consumes it: the offered asset lands in the solver's vault and
            // no payback note is created, so the batch's expected outputs never appear
            // and the whole batch fails — every tick, at a fee each time.
            if order.creator_id == solver_id {
                tracing::warn!(note_id = %note.id(), "skipping PSWAP note whose creator is the solver account");
                continue;
            }

            let note_id_bytes = note.id().to_bytes().to_vec();

            let mut raw_data = Vec::new();
            note.write_into(&mut raw_data);

            db_notes.push(NoteRow {
                note_id: note_id_bytes.clone(),
                account_id: order.creator_id.to_bytes().to_vec(),
                raw_data,
            });

            let arrival_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            db_orders.push(OrderRow {
                note_id: note_id_bytes,
                account_id: order.creator_id.to_bytes().to_vec(),
                requested_asset: order.requested_faucet_id.to_bytes().to_vec(),
                requested_amount: order.requested_amount as i64,
                offered_asset: order.offered_faucet_id.to_bytes().to_vec(),
                offered_amount: order.offered_amount as i64,
                timestamp: arrival_unix as i64,
                status: OrderStatus::Active.as_str().to_string(),
                priority_seq: 0, // assigned by the DB trigger on first insert
            });

            ingest_orders.push(BookOrder {
                priority_seq: 0, // replaced with the persisted sequence after insert
                arrival_unix,
                note: Arc::new(note.clone()),
            });
        }

        // Insert notes, orders and the cursor together. The returned set contains
        //    is the orders that were *actually* inserted this call — i.e. seen
        //    for the first time. Duplicates (already-known note_ids) are excluded
        //    by the `orders` primary key. This is the durable, bounded dedup that
        //    replaces the old in-memory `seen_notes` HashSet: a note enters the
        //    matcher channel exactly once, at first commit, even across restarts.
        let inserted = db::insert_notes_batch(conn, &db_notes, &db_orders, block_num)?;

        // Only newly inserted ordinary orders need an activation event.
        for mut order in ingest_orders {
            if let Some(&priority_seq) = inserted.get(order.id().to_bytes().as_slice()) {
                order.priority_seq = priority_seq;
                update.active.push(order);
            }
        }
        let consumed: HashSet<_> = consumed_notes.into_iter().collect();
        let consumed_bytes: Vec<_> = consumed.iter().map(|id| id.to_bytes().to_vec()).collect();
        db::mark_orders_onchain_nullified(conn, &consumed_bytes)?;
        update
            .active
            .retain(|order| !consumed.contains(&order.id()));
        update.removed.extend(consumed);
        Ok(update)
    }
}

/// Adapter that wraps the real `miden_client::Client` behind our `MidenClient`
/// trait abstraction. The same trait is implemented by `MockMidenClient` for
/// tests; this adapter makes the typed Client interchangeable in production.
///
/// Locking strategy: we hold `Arc<Mutex<Client>>` so the executor (which needs
/// the typed Client for `submit_new_transaction`) and ingest/admin (which go
/// through this adapter) can both share the same underlying instance without
/// fighting over ownership.
///
/// Note discovery (post the keyless-ingest / keystore-executor split):
/// PSWAPs come from a single `SyncSummary` source —
///   * `new_public_notes` ∪ `new_private_notes` — notes the screener
///     inserted into the input-notes table on this sync (tag-discovered,
///     not previously tracked).
///
/// The **ingest client is keyless and tracks no accounts**, so it has no
/// `output_notes` table and its `committed_notes` never carries the
/// solver's own notes. Solver-produced **remainder PSWAPs** (from partial
/// fills) are `Public` and tag-matched, so the ingest client re-discovers
/// them here via `new_public_notes` on the sync after the executor's
/// settle commits — exactly like any externally-created PSWAP. (The old
/// single-client model needed a second `committed_notes` pass because the
/// solver client owned the account and its remainder surfaced as its own
/// committed output note; that pass is dead post-split and was removed.)
/// The **executor client** subscribes no tags, so its `sync_state`
/// discovers nothing — it runs only to keep the chain tip / solver
/// account fresh; its returned notes are discarded by the sync task.
///
/// Dedup: the adapter is intentionally stateless. `new_public_notes` /
/// `new_private_notes` are edge-triggered (a note appears on exactly one
/// sync — the one whose block range covers its inclusion block — and
/// never again, since ranges advance and are non-overlapping). Any
/// residual double-emit is absorbed durably downstream: `ingest_once`
/// forwards an order to the matcher only when `insert_notes_batch`
/// reports it as newly inserted (the `orders` primary key is the dedup
/// authority, which also survives restarts).
pub(crate) struct MidenClientAdapter {
    pub(crate) client: Arc<Mutex<Client<FilesystemKeyStore>>>,
    /// Standalone RPC handle for the same node, used by
    /// `check_consumed_notes` for the nullifier existence query. Held
    /// separately so we never reach the `Client`'s internal RPC via the
    /// `#[cfg(feature = "testing")]` `Client::test_rpc_api()` accessor —
    /// that test helper previously forced the production build to enable
    /// miden-client's `testing` feature.
    pub(crate) rpc: Arc<dyn NodeRpcClient>,
}

#[async_trait(?Send)]
impl MidenClient for MidenClientAdapter {
    async fn stored_notes(&mut self) -> Result<Vec<Note>> {
        let mut records = self
            .client
            .lock()
            .await
            .get_input_notes(miden_client::store::NoteFilter::All)
            .await?;
        // Existing orders keep their persisted FIFO. Recover previously missed
        // discoveries in client arrival order, with a stable tie-break on restart.
        records.sort_by_key(|record| (record.created_at(), record.id()));
        records
            .iter()
            .filter(|record| {
                (record.is_authenticated() || record.is_consumed())
                    && record.details().recipient().script().root() == PswapNote::script_root()
            })
            .map(|record| record.try_into().map_err(anyhow::Error::from))
            .collect()
    }

    async fn note_is_included(&mut self, note_id: NoteId) -> Result<bool> {
        match self.rpc.get_note_by_id(note_id).await {
            Ok(_) => Ok(true),
            Err(RpcError::NoteNotFound(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn subscribe_pair(&mut self, offered: TokenId, requested: TokenId) -> Result<()> {
        // PSWAP discovery tags only depend on faucet IDs; the amounts in the
        // FungibleAsset args to `create_tag` are placeholders.
        let offered_asset = FungibleAsset::new(offered, 1)
            .map_err(|e| anyhow!("invalid offered asset for tag: {e}"))?;
        let requested_asset = FungibleAsset::new(requested, 1)
            .map_err(|e| anyhow!("invalid requested asset for tag: {e}"))?;
        let tag = PswapNote::create_tag(NoteType::Public, &offered_asset, &requested_asset);

        let mut client = self.client.lock().await;
        client
            .add_note_tag(tag)
            .await
            .map_err(|e| anyhow!("add_note_tag failed: {e}"))?;
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(block_num, new_pub, new_priv))]
    async fn sync_state(&mut self) -> Result<SyncResult> {
        let mut client = self.client.lock().await;
        let summary = client.sync_state().await.context("sync_state failed")?;

        // Populate span fields so structured logs carry sync stats.
        let span = tracing::Span::current();
        span.record("block_num", summary.block_num.as_u64());
        span.record("new_pub", summary.new_public_notes.len());
        span.record("new_priv", summary.new_private_notes.len());

        let mut new_notes: Vec<Note> = Vec::new();

        // Notes the Client inserted into its input-notes table on this sync
        // (tag-discovered, not previously tracked). This is the *only* PSWAP
        // discovery path — see the struct doc for why the old
        // `committed_notes` second pass is dead post-split.
        for note_id in summary
            .new_public_notes
            .iter()
            .chain(summary.new_private_notes.iter())
        {
            match client.get_input_note(*note_id).await {
                Ok(Some(record)) => match (&record).try_into() {
                    Ok(note) => new_notes.push(note),
                    Err(e) => return Err(anyhow!("cannot recover synced note {note_id}: {e}")),
                },
                Ok(None) => return Err(anyhow!("synced note {note_id} missing from client store")),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(SyncResult {
            block_num: summary.block_num.as_u64(),
            new_notes,
            consumed_notes: summary.consumed_notes.clone(),
        })
    }

    async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>> {
        if notes.is_empty() {
            return Ok(HashSet::new());
        }

        // Map nullifier → NoteId so we can recover the IDs from the RPC response.
        let mut nullifier_to_id = HashMap::new();
        let mut nullifiers = BTreeSet::new();
        for note in notes {
            let nullifier = note.nullifier();
            nullifier_to_id.insert(nullifier, note.id());
            nullifiers.insert(nullifier);
        }

        // GENESIS is intentional, not a placeholder: this is an "ever
        // consumed?" existence check, so it must scan the full nullifier
        // history. (The node serves this from its nullifier set; the
        // `from` block only bounds the *response* range, not correctness.)
        // Uses the dedicated `self.rpc` handle — NOT `client.test_rpc_api()`
        // — so production no longer depends on miden-client's `testing`
        // feature. No `client` lock is taken: this query goes straight to
        // the node, independent of `Client` state.
        let heights = self
            .rpc
            .get_nullifier_commit_heights(nullifiers, BlockNumber::GENESIS)
            .await
            .context("get_nullifier_commit_heights failed")?;

        let mut consumed = HashSet::new();
        for (nullifier, maybe_height) in heights {
            if maybe_height.is_some() {
                if let Some(id) = nullifier_to_id.get(&nullifier) {
                    consumed.insert(*id);
                }
            }
        }
        Ok(consumed)
    }

    async fn fetch_token_metadata(&mut self, faucet_id: TokenId) -> Result<Option<(u8, String)>> {
        let client = self.client.lock().await;
        let meta = client
            .fetch_remote_token_metadata(faucet_id)
            .await
            .map_err(|e| anyhow!("fetch_remote_token_metadata failed: {e}"))?;
        Ok(meta.map(|m| (m.decimals, m.symbol)))
    }

    async fn fee_parameters(&mut self) -> Result<Option<(TokenId, u32)>> {
        let (header, _) = self
            .rpc
            .get_block_header_by_number(None, false)
            .await
            .map_err(|e| anyhow!("fetch chain-tip header: {e}"))?;
        let fees = header.fee_parameters();
        let config = self
            .client
            .lock()
            .await
            .get_protocol_config(header.protocol_config_commitment())
            .await
            .map_err(|e| anyhow!("load protocol config for chain tip: {e}"))?;
        Ok(Some((
            config.fee_asset_id().faucet_id(),
            fees.verification_base_fee(),
        )))
    }
}

/// Spawn the keyless **ingest** OS thread: own `current_thread` runtime +
/// `LocalSet`; builds the ingest client on-thread (so the `!Send` `Client`
/// never crosses a thread boundary), then runs subscribe-relay + ingest.
///
/// Returns the joinable thread handle plus a `oneshot::Receiver` that yields
/// `Ok(())` once the client is built and the tasks are spawned, or the
/// build/subscribe error — so a startup failure surfaces at the caller's
/// readiness gate instead of dying silently in a detached thread.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_ingest_thread(
    factory: Arc<dyn ClientFactory>,
    db_pool: DbPool,
    cancel: CancellationToken,
    book_tx: mpsc::Sender<BookUpdate>,
    subscribe_rx: mpsc::Receiver<(TokenId, TokenId)>,
    ingest_interval: Duration,
    last_sync: Arc<AtomicI64>,
    solver_id: AccountId,
    clearing_bootstrap: oneshot::Sender<crate::matcher::ClearingBootstrap>,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ingest_ready_tx, ingest_ready_rx) = oneshot::channel::<Result<()>>();
    let ingest_factory = factory;
    let ingest_db = db_pool;
    let ingest_cancel = cancel;
    let ingest_book_tx = book_tx;
    let ingest_subscribe_rx = subscribe_rx;
    let ingest_last_sync: Arc<AtomicI64> = last_sync;
    let ingest_thread = thread::Builder::new()
        .name("ingest-client".into())
        .spawn(move || {
            crate::start::run_on_local_runtime("ingest-client", async move {
                let client = match ingest_factory.build_ingest().await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = ingest_ready_tx.send(Err(e.context("build_ingest")));
                        return;
                    }
                };
                let rpc = match ingest_factory.rpc() {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = ingest_ready_tx.send(Err(e.context("build ingest rpc")));
                        return;
                    }
                };
                let adapter: Arc<Mutex<dyn MidenClient>> =
                    Arc::new(Mutex::new(MidenClientAdapter {
                        client: Arc::new(Mutex::new(client)),
                        rpc,
                    }));
                let mut h = match crate::pipeline::spawn_ingest_tasks(
                    adapter,
                    ingest_db,
                    ingest_book_tx,
                    ingest_subscribe_rx,
                    ingest_interval,
                    ingest_cancel.clone(),
                    ingest_last_sync,
                    solver_id,
                    clearing_bootstrap,
                )
                .await
                {
                    Ok(h) => h,
                    Err(e) => {
                        let _ = ingest_ready_tx.send(Err(e.context("spawn_ingest_tasks")));
                        return;
                    }
                };
                let _ = ingest_ready_tx.send(Ok(()));
                // If a task exits *unexpectedly* (not via cancel) the main
                // coordination loop has no other signal — `book_tx` keeps
                // other live senders, so its `book_rx` never closes and the
                // matcher would silently run a stale book. Propagate a global
                // shutdown (`ingest_cancel` is a clone of the root token).
                tokio::select! {
                    _ = ingest_cancel.cancelled() => {}
                    _ = &mut h.ingest_handle => {
                        tracing::error!("ingest task exited unexpectedly; triggering shutdown");
                        ingest_cancel.cancel();
                    }
                    _ = &mut h.subscribe_handle => {
                        tracing::error!("subscribe-relay task exited unexpectedly; triggering shutdown");
                        ingest_cancel.cancel();
                    }
                }
                // Drain inside the runtime: abort + await both tasks so their
                // `Client` Arc refs are dropped *here* (runtime still entered).
                // Otherwise `LocalSet::drop` after `block_on` returns would
                // force-drop the `!Send` Client with no runtime context, whose
                // Drop then panics ("panic in a destructor during cleanup").
                //
                // The `is_finished()` guard is load-bearing: if the `select!`
                // above ended via a `&mut h.*_handle` arm, that handle was
                // already polled to completion there. A `JoinHandle` is a
                // one-shot future — awaiting it again panics with "JoinHandle
                // polled after completion". So only `.await` the handles the
                // `select!` did NOT already drive to completion; `abort()` on a
                // finished task is a harmless no-op.
                h.ingest_handle.abort();
                h.subscribe_handle.abort();
                if !h.ingest_handle.is_finished() {
                    let _ = h.ingest_handle.await;
                }
                if !h.subscribe_handle.is_finished() {
                    let _ = h.subscribe_handle.await;
                }
            });
        })
        .context("spawn ingest thread")?;
    Ok((ingest_thread, ingest_ready_rx))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::{asset::AssetAmount, Word};
    use miden_standards::note::PswapNoteStorage;

    fn fixture() -> (Note, Note, AccountId) {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let solver = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        let parent: Note = PswapNote::builder()
            .sender(creator)
            .serial_number(Word::default())
            .note_type(NoteType::Public)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 10).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .offered_asset(
                FungibleAsset::new(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(), 10)
                    .unwrap(),
            )
            .build()
            .unwrap()
            .into();
        let (_, child) = PswapNote::try_from(&parent)
            .unwrap()
            .execute(
                solver,
                None,
                Some(FungibleAsset::new(requested, 5).unwrap()),
            )
            .unwrap();
        (parent, child.unwrap().into(), solver)
    }

    fn prepare(pool: &DbPool, parent: &Note, child: &Note, solver: AccountId) -> Vec<u8> {
        let mut conn = pool.write_conn().unwrap();
        SyncResult {
            block_num: 1,
            new_notes: vec![parent.clone()],
            consumed_notes: vec![],
        }
        .persist(&mut conn, solver)
        .unwrap();
        let tx_id = vec![7; 32];
        db::prepare_settlement(
            &mut conn,
            &db::models::SettlementAttemptRow {
                tx_id: tx_id.clone(),
                tx_result: vec![1],
                status: "prepared".into(),
            },
            &[db::models::SettlementInputRow {
                tx_id: tx_id.clone(),
                parent_note_id: parent.id().to_bytes().to_vec(),
                payback_note_id: vec![8; 32],
                child_note_id: Some(child.id().to_bytes().to_vec()),
                child_note_data: Some(child.to_bytes()),
            }],
        )
        .unwrap();
        tx_id
    }

    #[tokio::test]
    async fn startup_recovers_client_notes_missing_from_solver_database() {
        let pool = db::init_db(":memory:", 1).unwrap();
        let (parent, _, solver) = fixture();
        let mut client = MockMidenClient::new();
        client.add_notes(vec![parent.clone()], 10);
        // Client sync committed, then the process died before solver persistence.
        client.sync_state().await.unwrap();
        assert!(client.sync_state().await.unwrap().new_notes.is_empty());
        SyncResult::recover(&mut client, &pool, solver)
            .await
            .unwrap();
        let first = db::get_active_orders(&mut pool.write_conn().unwrap()).unwrap();
        assert_eq!(first.len(), 1);
        SyncResult::recover(&mut client, &pool, solver)
            .await
            .unwrap();
        let second = db::get_active_orders(&mut pool.write_conn().unwrap()).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].priority_seq, first[0].priority_seq);
        client.mark_consumed_silent(vec![parent.id()]);
        SyncResult::recover(&mut client, &pool, solver)
            .await
            .unwrap();
        assert!(db::get_active_orders(&mut pool.write_conn().unwrap())
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn startup_remainder_keeps_parent_priority() {
        let pool = db::init_db(":memory:", 1).unwrap();
        let (parent, child, solver) = fixture();
        prepare(&pool, &parent, &child, solver);
        let mut client = MockMidenClient::new();
        client.add_notes(vec![child.clone()], 10);
        client.sync_state().await.unwrap();
        SyncResult::recover(&mut client, &pool, solver)
            .await
            .unwrap();
        let live = db::get_active_orders(&mut pool.write_conn().unwrap()).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].note_id, child.id().to_bytes());
        assert_eq!(live[0].priority_seq, 1);
    }

    #[test]
    fn consumed_remainder_is_not_activated_in_same_sync() {
        let pool = db::init_db(":memory:", 1).unwrap();
        let (parent, child, solver) = fixture();
        prepare(&pool, &parent, &child, solver);
        let update = SyncResult {
            block_num: 10,
            new_notes: vec![child.clone()],
            consumed_notes: vec![parent.id(), child.id()],
        }
        .persist(&mut pool.write_conn().unwrap(), solver)
        .unwrap();
        assert!(update.active.is_empty());
        assert!(update.removed.contains(&child.id()));
        assert!(db::get_active_orders(&mut pool.write_conn().unwrap())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn sync_error_rolls_back_remainder_confirmation() {
        let pool = db::init_db(":memory:", 1).unwrap();
        let (parent, child, solver) = fixture();
        prepare(&pool, &parent, &child, solver);
        let mut conn = pool.write_conn().unwrap();
        conn.batch_execute(
            "CREATE TRIGGER reject_cursor BEFORE UPDATE ON sync_state
            BEGIN SELECT RAISE(ABORT, 'injected cursor error'); END;",
        )
        .unwrap();
        assert!(SyncResult {
            block_num: 10,
            new_notes: vec![child],
            consumed_notes: vec![]
        }
        .persist(&mut conn, solver)
        .is_err());
        assert_eq!(db::unresolved_settlements(&mut conn).unwrap().len(), 1);
        assert!(db::get_active_orders(&mut conn).unwrap().is_empty());
    }

    /// Mock MidenClient for testing.
    pub struct MockMidenClient {
        notes: Vec<Note>,
        stored: Vec<Note>,
        block: u64,
        /// Pre-staged consumed-note IDs returned by the next sync_state call.
        /// Drained on each sync so a single push delivers once.
        pending_consumed: Vec<NoteId>,
        /// Set of note IDs that should be reported as consumed by
        /// `check_consumed_notes`. Persistent — represents on-chain state.
        consumed_set: HashSet<NoteId>,
        sync_rpc_failures: usize,
        pub fail_consumed_check: bool,
        /// Canned on-chain metadata returned by `fetch_token_metadata`
        /// (`None` = the faucet has no public metadata).
        token_metadata: Option<(u8, String)>,
    }

    impl MockMidenClient {
        pub fn new() -> Self {
            Self {
                notes: Vec::new(),
                stored: Vec::new(),
                block: 0,
                pending_consumed: Vec::new(),
                consumed_set: HashSet::new(),
                sync_rpc_failures: 0,
                fail_consumed_check: false,
                token_metadata: None,
            }
        }

        /// Stage the `(decimals, ticker)` that `fetch_token_metadata` returns.
        pub fn set_token_metadata(&mut self, decimals: u8, ticker: &str) {
            self.token_metadata = Some((decimals, ticker.to_string()));
        }

        pub fn fail_next_syncs(&mut self, count: usize) {
            self.sync_rpc_failures = count;
        }

        pub fn add_notes(&mut self, notes: Vec<Note>, block: u64) {
            self.stored.extend(notes.clone());
            self.notes.extend(notes);
            self.block = block;
        }

        /// Stage NoteIds to be returned by the next `sync_state` call as
        /// `consumed_notes`. Also marks them as consumed for any later
        /// `check_consumed_notes` calls.
        pub fn add_consumed(&mut self, note_ids: Vec<NoteId>) {
            for id in &note_ids {
                self.consumed_set.insert(*id);
            }
            self.pending_consumed.extend(note_ids);
        }

        /// Mark IDs as consumed for `check_consumed_notes` without
        /// surfacing them via `sync_state`. Useful for tests of the
        /// executor's classify path where the discovery path is bypassed.
        pub fn mark_consumed_silent(&mut self, note_ids: Vec<NoteId>) {
            for id in note_ids {
                self.consumed_set.insert(id);
            }
        }
    }

    #[async_trait(?Send)]
    impl MidenClient for MockMidenClient {
        async fn stored_notes(&mut self) -> Result<Vec<Note>> {
            Ok(self.stored.clone())
        }
        async fn note_is_included(&mut self, _note_id: NoteId) -> Result<bool> {
            Ok(false)
        }

        async fn subscribe_pair(&mut self, _offered: TokenId, _requested: TokenId) -> Result<()> {
            Ok(())
        }

        async fn sync_state(&mut self) -> Result<SyncResult> {
            if self.sync_rpc_failures > 0 {
                self.sync_rpc_failures -= 1;
                return Err(ClientError::RpcError(RpcError::InvalidNodeEndpoint(
                    "mock transient failure".to_string(),
                ))
                .into());
            }
            let consumed = std::mem::take(&mut self.pending_consumed);
            Ok(SyncResult {
                block_num: self.block,
                new_notes: std::mem::take(&mut self.notes),
                consumed_notes: consumed,
            })
        }

        async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>> {
            anyhow::ensure!(!self.fail_consumed_check, "mock nullifier RPC unavailable");
            Ok(notes
                .iter()
                .map(|n| n.id())
                .filter(|id| self.consumed_set.contains(id))
                .collect())
        }

        async fn fetch_token_metadata(
            &mut self,
            _faucet_id: TokenId,
        ) -> Result<Option<(u8, String)>> {
            Ok(self.token_metadata.clone())
        }
    }

    #[tokio::test]
    async fn transient_ingest_rpc_failure_retries_without_cancelling_pipeline() {
        let pool = db::init_db(":memory:", 1).unwrap();
        let (_, _, solver) = fixture();
        let mut mock = MockMidenClient::new();
        mock.add_notes(Vec::new(), 7);
        mock.fail_next_syncs(1);
        let client: Arc<Mutex<dyn MidenClient>> = Arc::new(Mutex::new(mock));
        let (book_tx, _book_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let last_sync = Arc::new(AtomicI64::new(0));

        let ingest = run_ingest(
            client,
            pool.clone(),
            book_tx,
            Duration::from_millis(1),
            cancel.clone(),
            last_sync.clone(),
            solver,
        );
        tokio::pin!(ingest);

        let observed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if db::get_last_fetched_block(&mut pool.write_conn().unwrap()).unwrap() == 7 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        tokio::select! {
            _ = &mut ingest => panic!("ingest stopped after a transient RPC failure"),
            result = observed => result.unwrap(),
        }

        assert!(!cancel.is_cancelled());
        assert!(last_sync.load(Ordering::Relaxed) > 0);
        cancel.cancel();
        ingest.await;
    }
}

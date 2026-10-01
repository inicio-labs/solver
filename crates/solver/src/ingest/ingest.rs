use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use diesel::pg::PgConnection;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::rpc::{NodeRpcClient, RpcError};
use miden_client::{Client, ClientError};
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteId};
use miden_protocol::transaction::TransactionId;
use miden_standards::note::PswapNote;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::client_factory::ClientFactory;
use crate::db::postgres_models::NewOrderRow;
use crate::db::{self, DbPool};
use crate::types::Order as PipelineOrder;
use crate::types::{BookOrder, BookUpdate, TokenId};

/// Notes per startup-recovery write transaction. Keeps each transaction well
/// inside the writer's statement timeout however much history the client has.
const RECOVERY_BATCH_NOTES: usize = 1_000;
/// Consecutive failed persists of one sync result before ingest gives up.
const INGEST_PERSIST_ATTEMPTS: u32 = 5;

/// Result of a sync_state call — newly received notes plus IDs of notes
/// whose nullifier was just observed on-chain. The matcher uses the
/// latter to surgically drop zombie orders from its in-memory book.
#[derive(Clone)]
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

    /// Whether the node has committed transaction `tx_id` of `account_id` in
    /// blocks `from..=to`. This is the only evidence a settlement landed:
    /// note IDs cannot prove it, because another filler consuming the same
    /// parent with the same amount produces identical payback/remainder IDs.
    async fn transaction_committed(
        &mut self,
        account_id: AccountId,
        tx_id: TransactionId,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<bool>;

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
    // Sync advances the client's durable cursor before the database sees the
    // result, so a failed persist keeps that result and retries it before the
    // next sync; the write is idempotent. Only a fatal writer or persistent
    // failure stops the pipeline, where boot replays the client's stored notes.
    let mut unpersisted: Option<(SyncResult, u32)> = None;
    loop {
        let sync = match unpersisted.take() {
            Some(retry) => Some(retry),
            None => match client.lock().await.sync_state().await {
                Ok(sync) => Some((sync, 0)),
                Err(error) if is_rpc_error(&error) => {
                    tracing::warn!(%error, "ingest RPC failed; retrying next tick");
                    None
                }
                Err(error) => {
                    tracing::error!(%error, "ingest client failed; stopping pipeline");
                    cancel.cancel();
                    return;
                }
            },
        };

        if let Some((sync, failures)) = sync {
            let retry = sync.clone();
            match pool
                .write_book(&book_tx, move |conn| {
                    sync.persist_postgres_tx(conn, solver_id)
                })
                .await
            {
                Ok(()) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    last_sync_unix_seconds.store(now, Ordering::Relaxed);
                }
                Err(error)
                    if !pool.fatal_token().is_cancelled()
                        && failures + 1 < INGEST_PERSIST_ATTEMPTS =>
                {
                    tracing::warn!(%error, attempt = failures + 1, "persisting ingest update failed; retrying next tick");
                    unpersisted = Some((retry, failures + 1));
                }
                Err(error) => {
                    tracing::error!(%error, "persisting ingest update failed; stopping pipeline");
                    cancel.cancel();
                    return;
                }
            }
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
    /// Replays the keyless client's durable note discoveries into PostgreSQL
    /// before matcher hydration. The Miden client itself never crosses the
    /// blocking-worker boundary; only the owned sync result does.
    ///
    /// The database cursor is the anchor: it only advances in the final write,
    /// after every missing note has been inserted in bounded batches. A crash
    /// part-way leaves the anchor in place and the next boot skips notes that
    /// already landed, so recovery cost tracks what is missing, not history.
    pub async fn recover_postgres(
        client: &mut dyn MidenClient,
        pool: &DbPool,
        solver_id: AccountId,
    ) -> Result<()> {
        let sync = client.sync_state().await?;
        let stored = client.stored_notes().await?;
        let mut consumed_notes = sync.consumed_notes;
        for note_chunk in stored.chunks(miden_protocol::MAX_INPUT_NOTES_PER_TX) {
            consumed_notes.extend(client.check_consumed_notes(note_chunk).await?);
        }

        let stored_ids: Vec<_> = stored
            .iter()
            .map(|note| note.id().to_bytes().to_vec())
            .collect();
        let (anchor, existing) = pool
            .read(move |conn| {
                Ok((
                    db::postgres_db::get_last_fetched_block_tx(conn)?,
                    db::postgres_db::existing_note_ids_tx(conn, &stored_ids)?,
                ))
            })
            .await?;
        let missing: Vec<Note> = stored
            .into_iter()
            .filter(|note| !existing.contains(note.id().to_bytes().as_slice()))
            .collect();
        tracing::info!(
            anchor,
            target_block = sync.block_num,
            missing = missing.len(),
            "replaying client notes missing from PostgreSQL"
        );

        for batch in missing.chunks(RECOVERY_BATCH_NOTES) {
            let batch = SyncResult {
                block_num: anchor,
                new_notes: batch.to_vec(),
                consumed_notes: Vec::new(),
            };
            pool.write(move |conn| batch.persist_postgres_tx(conn, solver_id))
                .await?;
        }
        let finish = SyncResult {
            block_num: sync.block_num.max(anchor),
            new_notes: Vec::new(),
            consumed_notes,
        };
        pool.write(move |conn| finish.persist_postgres_tx(conn, solver_id))
            .await?;
        Ok(())
    }

    /// PostgreSQL version of the ingest transition. `PgPool::write_book` owns
    /// the single transaction; this helper performs no nested transaction.
    pub(crate) fn persist_postgres_tx(
        self,
        conn: &mut PgConnection,
        solver_id: AccountId,
    ) -> Result<BookUpdate> {
        let SyncResult {
            block_num,
            new_notes,
            consumed_notes,
        } = self;
        // A remainder our settlement expects is ingested with its parent's
        // FIFO slot. The settlement is not confirmed here: an identical note
        // can come from another filler, so only our transaction ID decides.
        let (expected_children, remainders) =
            db::postgres_db::ingest_expected_remainders_tx(conn, &new_notes)?;
        let mut update = BookUpdate {
            removed: Vec::new(),
            active: remainders,
        };

        let mut order_rows = Vec::new();
        let mut book_orders = Vec::new();
        for note in &new_notes {
            if expected_children.contains(note.id().to_bytes().as_slice())
                || note.recipient().script().root() != PswapNote::script_root()
            {
                continue;
            }
            let order = match PipelineOrder::from_note(note) {
                Ok(order) => order,
                Err(error) => {
                    tracing::warn!(note_id = %note.id(), %error, "skipping unparseable PSWAP note");
                    continue;
                }
            };
            if order.creator_id == solver_id {
                tracing::warn!(note_id = %note.id(), "skipping PSWAP note whose creator is the solver account");
                continue;
            }
            let arrival_unix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            // Parsed as a valid PSWAP order just above.
            order_rows.push(NewOrderRow {
                note_id: note.id().to_bytes().to_vec(),
                raw_data: note.to_bytes(),
                arrival_unix: i64::try_from(arrival_unix)?,
            });
            book_orders.push(BookOrder {
                priority_seq: 0,
                arrival_unix,
                note: Arc::new(note.clone()),
            });
        }

        let inserted = db::postgres_db::insert_orders_batch_tx(conn, &order_rows, block_num)?;
        for mut order in book_orders {
            if let Some(&priority_seq) = inserted.get(order.id().to_bytes().as_slice()) {
                order.priority_seq = priority_seq;
                update.active.push(order);
            }
        }
        let consumed: HashSet<_> = consumed_notes.into_iter().collect();
        let consumed_bytes: Vec<_> = consumed.iter().map(|id| id.to_bytes().to_vec()).collect();
        db::postgres_db::mark_orders_onchain_nullified_tx(conn, &consumed_bytes)?;
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

    async fn transaction_committed(
        &mut self,
        account_id: AccountId,
        tx_id: TransactionId,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<bool> {
        if from > to {
            return Ok(false);
        }
        let records = self
            .rpc
            .sync_transactions(from, to, vec![account_id])
            .await?;
        Ok(records
            .iter()
            .any(|record| record.transaction_header.id() == tx_id))
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
    use crate::db::postgres_test::TestDb;
    use crate::types::OrderStatus;
    use diesel::connection::SimpleConnection;
    use diesel::prelude::*;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::{asset::AssetAmount, Word};
    use miden_standards::note::PswapNoteStorage;

    struct PostgresFixture {
        conn: PgConnection,
        name: String,
    }

    impl PostgresFixture {
        fn new() -> Result<Self> {
            let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
            let mut conn = db::postgres_migrations::connect(&url)?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let name = format!("solver_ingest_{}_{}", std::process::id(), nonce);
            conn.batch_execute(&format!("CREATE SCHEMA {name}; SET search_path TO {name}"))?;
            db::postgres_migrations::migrate(&mut conn)?;
            Ok(Self { conn, name })
        }
    }

    impl Drop for PostgresFixture {
        fn drop(&mut self) {
            let _ = self.conn.batch_execute("ROLLBACK");
            let _ = self.conn.batch_execute(&format!(
                "SET search_path TO public; DROP SCHEMA {} CASCADE",
                self.name
            ));
        }
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn postgres_ingest_never_confirms_and_consumed_remainder_stays_retired() -> Result<()> {
        use db::postgres_schema::orders;

        let mut pg_fixture = PostgresFixture::new()?;
        let conn = &mut pg_fixture.conn;
        let (parent, child, solver) = fixture();
        let first = conn.transaction::<_, anyhow::Error, _>(|conn| {
            SyncResult {
                block_num: 1,
                new_notes: vec![parent.clone()],
                consumed_notes: Vec::new(),
            }
            .persist_postgres_tx(conn, solver)
        })?;
        assert_eq!(first.active.len(), 1);
        assert_eq!(first.active[0].id(), parent.id());
        let stored_note: Vec<u8> = orders::table
            .find(parent.id().to_bytes().to_vec())
            .select(orders::raw_data)
            .first(conn)?;
        assert_eq!(stored_note, parent.to_bytes());

        let duplicate = conn.transaction::<_, anyhow::Error, _>(|conn| {
            SyncResult {
                block_num: 2,
                new_notes: vec![parent.clone()],
                consumed_notes: Vec::new(),
            }
            .persist_postgres_tx(conn, solver)
        })?;
        assert!(duplicate.is_empty());

        let priority: i64 = orders::table
            .find(parent.id().to_bytes().to_vec())
            .select(orders::priority_seq)
            .first(conn)?;
        let tx_id = vec![7; 32];
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            db::postgres_db::prepare_settlement_tx(
                conn,
                &db::postgres_models::SettlementAttemptRow {
                    tx_id: tx_id.clone(),
                    tx_result: vec![1],
                    status: "prepared".into(),
                },
                &[db::postgres_models::SettlementInputRow {
                    tx_id: tx_id.clone(),
                    parent_note_id: parent.id().to_bytes().to_vec(),
                    child_note_id: Some(child.id().to_bytes().to_vec()),
                    child_note_data: Some(child.to_bytes()),
                }],
            )
        })?;
        let unresolved = db::postgres_db::load_unresolved_attempts_tx(conn)?;
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].parents.len(), 1);
        assert_eq!(unresolved[0].parents[0].id(), parent.id());
        let update = conn.transaction::<_, anyhow::Error, _>(|conn| {
            SyncResult {
                block_num: 3,
                new_notes: vec![child.clone()],
                consumed_notes: vec![child.id(), parent.id()],
            }
            .persist_postgres_tx(conn, solver)
        })?;
        // Ingest cannot tell our remainder from an identical one created by
        // another filler. It stores the remainder with the parent's FIFO slot
        // (retired here, since it was consumed in the same sync) but leaves
        // the settlement unconfirmed and the reserved parent Settling.
        assert!(update.active.is_empty());
        let parent_status: String = orders::table
            .find(parent.id().to_bytes().to_vec())
            .select(orders::status)
            .first(conn)?;
        assert_eq!(parent_status, OrderStatus::Settling.as_str());
        let ingested_child: (String, i64) = orders::table
            .find(child.id().to_bytes().to_vec())
            .select((orders::status, orders::priority_seq))
            .first(conn)?;
        assert_eq!(ingested_child.0, OrderStatus::OnchainNullified.as_str());
        assert_eq!(ingested_child.1, priority);
        assert_eq!(db::postgres_db::load_unresolved_attempts_tx(conn)?.len(), 1);

        // The node confirmed our transaction ID. The remainder is already
        // retired, so confirmation retires the parent and activates nothing.
        let consumed_child: HashSet<_> = [child.id()].into_iter().collect();
        let update = conn.transaction::<_, anyhow::Error, _>(|conn| {
            db::postgres_db::confirm_settlement_tx(conn, &tx_id, &consumed_child)
        })?;
        assert!(update.active.is_empty());
        assert!(update.removed.contains(&parent.id()));
        assert!(update.removed.contains(&child.id()));
        let child_row: (String, i64) = orders::table
            .find(child.id().to_bytes().to_vec())
            .select((orders::status, orders::priority_seq))
            .first(conn)?;
        assert_eq!(child_row.0, OrderStatus::OnchainNullified.as_str());
        assert_eq!(child_row.1, priority);
        assert!(db::postgres_db::load_unresolved_attempts_tx(conn)?.is_empty());
        Ok(())
    }

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

    async fn prepare(pool: &DbPool, parent: &Note, child: &Note, solver: AccountId) -> Vec<u8> {
        let first_parent = parent.clone();
        let parent = parent.clone();
        let child = child.clone();
        pool.write(move |conn| {
            SyncResult {
                block_num: 1,
                new_notes: vec![first_parent],
                consumed_notes: vec![],
            }
            .persist_postgres_tx(conn, solver)
        })
        .await
        .unwrap();
        let tx_id = vec![7; 32];
        let durable_tx_id = tx_id.clone();
        pool.write(move |conn| {
            db::postgres_db::prepare_settlement_tx(
                conn,
                &db::postgres_models::SettlementAttemptRow {
                    tx_id: durable_tx_id.clone(),
                    tx_result: vec![1],
                    status: "prepared".into(),
                },
                &[db::postgres_models::SettlementInputRow {
                    tx_id: durable_tx_id,
                    parent_note_id: parent.id().to_bytes().to_vec(),
                    child_note_id: Some(child.id().to_bytes().to_vec()),
                    child_note_data: Some(child.to_bytes()),
                }],
            )
        })
        .await
        .unwrap();
        tx_id
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn startup_recovers_client_notes_missing_from_solver_database() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let (parent, _, solver) = fixture();
        let mut client = MockMidenClient::new();
        client.add_notes(vec![parent.clone()], 10);
        // Client sync committed, then the process died before solver persistence.
        client.sync_state().await.unwrap();
        assert!(client.sync_state().await.unwrap().new_notes.is_empty());
        SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .unwrap();
        let first = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .unwrap();
        let second = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].priority_seq, first[0].priority_seq);
        client.mark_consumed_silent(vec![parent.id()]);
        SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .unwrap();
        assert!(pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn startup_remainder_keeps_parent_priority_without_confirming() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let (parent, child, solver) = fixture();
        let tx_id = prepare(pool, &parent, &child, solver).await;
        let mut client = MockMidenClient::new();
        client.add_notes(vec![child.clone()], 10);
        client.sync_state().await.unwrap();
        SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .unwrap();
        // The remainder is live on chain whoever created it: it is ingested
        // with its parent's FIFO slot, but our settlement stays unconfirmed.
        let live = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].note_id, child.id().to_bytes());
        assert_eq!(live[0].priority_seq, 1);
        assert_eq!(
            pool.read(db::postgres_db::load_unresolved_attempts_tx)
                .await
                .unwrap()
                .len(),
            1
        );

        // The executor confirms once the node reports our transaction ID; the
        // already-ingested remainder is not announced to the matcher again.
        let update = pool
            .write(move |conn| {
                db::postgres_db::confirm_settlement_tx(conn, &tx_id, &HashSet::new())
            })
            .await
            .unwrap();
        assert!(update.active.is_empty());
        assert!(update.removed.contains(&parent.id()));
        let live = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].note_id, child.id().to_bytes());
        assert_eq!(live[0].priority_seq, 1);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn consumed_remainder_is_not_activated_in_same_sync() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let (parent, child, solver) = fixture();
        prepare(pool, &parent, &child, solver).await;
        let child_id = child.id();
        let update = SyncResult {
            block_num: 10,
            new_notes: vec![child.clone()],
            consumed_notes: vec![parent.id(), child.id()],
        };
        let update = pool
            .write(move |conn| update.persist_postgres_tx(conn, solver))
            .await
            .unwrap();
        assert!(update.active.is_empty());
        assert!(update.removed.contains(&child_id));
        assert!(pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn sync_error_rolls_back_remainder_confirmation() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let (parent, child, solver) = fixture();
        prepare(pool, &parent, &child, solver).await;
        pool.write(|conn| { conn.batch_execute("ALTER TABLE sync_state ADD CONSTRAINT reject_cursor CHECK (last_fetched_block < 10)")?; Ok(()) }).await.unwrap();
        let sync = SyncResult {
            block_num: 10,
            new_notes: vec![child],
            consumed_notes: vec![],
        };
        assert!(pool
            .write(move |conn| sync.persist_postgres_tx(conn, solver))
            .await
            .is_err());
        assert_eq!(
            pool.read(db::postgres_db::load_unresolved_attempts_tx)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap()
            .is_empty());
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
        async fn transaction_committed(
            &mut self,
            _account_id: AccountId,
            _tx_id: TransactionId,
            _from: BlockNumber,
            _to: BlockNumber,
        ) -> Result<bool> {
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
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn transient_ingest_rpc_failure_retries_without_cancelling_pipeline() {
        let test_db = TestDb::new().await.unwrap();
        let pool = test_db.pool.clone();
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
                if pool
                    .read(db::postgres_db::get_last_fetched_block_tx)
                    .await
                    .unwrap()
                    == 7
                {
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

    fn order_notes(count: usize, seed: u32) -> Vec<Note> {
        use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
        let creator: AccountId = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let mut rng = RandomCoin::new(Word::from([seed, 0, 0, 0]));
        (0..count)
            .map(|_| {
                PswapNote::builder()
                    .sender(creator)
                    .serial_number(rng.draw_word())
                    .note_type(NoteType::Public)
                    .storage(
                        PswapNoteStorage::builder()
                            .min_requested_asset(FungibleAsset::new(requested, 10).unwrap())
                            .min_fill_step(AssetAmount::new(1).unwrap())
                            .creator_account_id(creator)
                            .build(),
                    )
                    .offered_asset(FungibleAsset::new(offered, 10).unwrap())
                    .build()
                    .unwrap()
                    .into()
            })
            .collect()
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn startup_recovery_batches_from_the_anchor_and_resumes_after_a_crash() {
        let test_db = TestDb::new().await.unwrap();
        let pool = &test_db.pool;
        let (_, _, solver) = fixture();
        let notes = order_notes(2_500, 1);

        // A previous boot persisted the first batch, then crashed: those rows
        // exist but the anchor (sync cursor) never advanced.
        let first_batch = notes[..RECOVERY_BATCH_NOTES].to_vec();
        pool.write(move |conn| {
            SyncResult {
                block_num: 0,
                new_notes: first_batch,
                consumed_notes: Vec::new(),
            }
            .persist_postgres_tx(conn, solver)
            .map(|_| ())
        })
        .await
        .unwrap();
        let before: HashMap<Vec<u8>, i64> = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.note_id, row.priority_seq))
            .collect();
        assert_eq!(before.len(), RECOVERY_BATCH_NOTES);

        let mut client = MockMidenClient::new();
        client.add_notes(notes.clone(), 50);
        client.sync_state().await.unwrap();

        // The final write fails: every missing note lands in its own batch,
        // but the anchor must not move until the replay is complete.
        pool.write(|conn| {
            conn.batch_execute(
                "ALTER TABLE sync_state ADD CONSTRAINT block_recovery CHECK (last_fetched_block < 50)",
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .is_err());
        assert_eq!(
            pool.read(db::postgres_db::get_last_fetched_block_tx)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pool.read(db::postgres_db::get_active_orders_tx)
                .await
                .unwrap()
                .len(),
            2_500
        );

        // The next boot finishes without duplicating or reprioritising rows.
        pool.write(|conn| {
            conn.batch_execute("ALTER TABLE sync_state DROP CONSTRAINT block_recovery")?;
            Ok(())
        })
        .await
        .unwrap();
        SyncResult::recover_postgres(&mut client, pool, solver)
            .await
            .unwrap();
        assert_eq!(
            pool.read(db::postgres_db::get_last_fetched_block_tx)
                .await
                .unwrap(),
            50
        );
        let after = pool
            .read(db::postgres_db::get_active_orders_tx)
            .await
            .unwrap();
        assert_eq!(after.len(), 2_500);
        for row in after {
            if let Some(&priority) = before.get(&row.note_id) {
                assert_eq!(row.priority_seq, priority, "replay kept the FIFO slot");
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn failed_ingest_persist_retries_the_same_sync_result() {
        let test_db = TestDb::new().await.unwrap();
        let pool = test_db.pool.clone();
        let (_, _, solver) = fixture();
        let note = order_notes(1, 2).remove(0);
        let note_id = note.id().to_bytes().to_vec();

        // The client hands out this note exactly once.
        let mut mock = MockMidenClient::new();
        mock.add_notes(vec![note], 7);
        let client: Arc<Mutex<dyn MidenClient>> = Arc::new(Mutex::new(mock));
        pool.write(|conn| {
            conn.batch_execute(
                "ALTER TABLE sync_state ADD CONSTRAINT block_ingest CHECK (last_fetched_block < 5)",
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let (book_tx, mut book_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let last_sync = Arc::new(AtomicI64::new(0));
        let ingest = run_ingest(
            client,
            pool.clone(),
            book_tx,
            Duration::from_millis(200),
            cancel.clone(),
            last_sync,
            solver,
        );
        tokio::pin!(ingest);

        let recovered = async {
            // Let the first persist fail, then clear the fault.
            let started = std::time::Instant::now();
            while pool.telemetry_snapshot().write_errors == 0 {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "persist never failed"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            pool.write(|conn| {
                conn.batch_execute("ALTER TABLE sync_state DROP CONSTRAINT block_ingest")?;
                Ok(())
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(5), book_rx.recv())
                .await
                .expect("the held sync result was never persisted")
                .expect("book channel closed")
        };
        let update = tokio::select! {
            _ = &mut ingest => panic!("ingest stopped after one failed persist"),
            update = recovered => update,
        };

        assert_eq!(update.active.len(), 1);
        assert_eq!(update.active[0].id().to_bytes(), note_id);
        assert_eq!(
            pool.read(db::postgres_db::get_last_fetched_block_tx)
                .await
                .unwrap(),
            7
        );
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        ingest.await;
    }
}

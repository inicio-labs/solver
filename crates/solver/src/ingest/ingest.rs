use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::rpc::NodeRpcClient;
use miden_client::Client;
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
use crate::types::{IngestOrder, OrderStatus, TokenId};

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

    /// Given a slice of notes the solver currently believes are matchable,
    /// return the subset whose nullifiers are already on-chain. Used by the
    /// executor after a non-RPC submit error to identify which input notes
    /// are zombies vs. which are still legitimately active.
    async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>>;

    /// Fetch a public fungible faucet's on-chain metadata `(decimals, ticker)`
    /// by id. Returns `None` if the account isn't a public faucet / doesn't
    /// exist. Keyless — no signing, no pre-tracking.
    async fn fetch_token_metadata(&mut self, faucet_id: TokenId) -> Result<Option<(u8, String)>>;
}

/// Run the note ingestion loop.
///
/// Each tick: sync → fetch new notes by ID → filter PSWAP → atomic DB insert → send to channel.
/// On a successful tick the `last_sync_unix_seconds` atomic is bumped so the
/// observability `/readyz` endpoint can detect a stalled ingest.
pub async fn run_ingest(
    client: Arc<Mutex<dyn MidenClient>>,
    pool: DbPool,
    order_tx: mpsc::Sender<IngestOrder>,
    consumed_tx: mpsc::Sender<NoteId>,
    interval: Duration,
    cancel: CancellationToken,
    last_sync_unix_seconds: Arc<AtomicI64>,
) {
    loop {
        // Finish-the-unit shutdown: `ingest_once` is deliberately NOT wrapped
        // in `select!`, so a started iteration always runs to completion and
        // the consumed-notes DB write + channel sends for this sync are atomic
        // (no reliance on `ingest_once` being drop-safe). Cancellation is
        // observed *between* iterations by the bottom `select!`, which breaks
        // on `cancel` whether it fired during the preceding `ingest_once` or
        // during the idle wait. Worst-case shutdown delay is one `ingest_once`,
        // bounded by the per-call RPC timeout (`rpc.timeout_ms`).
        match ingest_once(&client, &pool, &order_tx, &consumed_tx).await {
            Ok(()) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                last_sync_unix_seconds.store(now, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!(error = %e, "ingest iteration failed");
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

/// Single iteration of the ingest loop.
#[tracing::instrument(skip_all)]
async fn ingest_once(
    client: &Arc<Mutex<dyn MidenClient>>,
    pool: &DbPool,
    order_tx: &mpsc::Sender<IngestOrder>,
    consumed_tx: &mpsc::Sender<NoteId>,
) -> Result<()> {
    let SyncResult {
        block_num,
        new_notes,
        consumed_notes,
    } = {
        let mut c = client.lock().await;
        c.sync_state().await?
    };

    // Handle consumed notes BEFORE the new-notes path. DB write happens
    // first so a crash between write and channel send leaves the DB
    // authoritative; matcher's next-boot hydration sees terminal state.
    if !consumed_notes.is_empty() {
        let consumed_bytes: Vec<Vec<u8>> = consumed_notes
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect();
        {
            let mut conn = pool.write_conn()?;
            let updated = db::mark_orders_onchain_nullified(&mut conn, &consumed_bytes)?;
            if updated > 0 {
                tracing::warn!(
                    count = updated,
                    "ingest marked orders OnchainNullified after sync"
                );
            }
        }
        for note_id in consumed_notes {
            // Best-effort send. Matcher channel-closed means the matcher
            // shut down; we keep ingesting (other tasks may still be live)
            // but the in-memory book stops being maintained.
            let _ = consumed_tx.send(note_id).await;
        }
    }

    // 3. Filter PSWAP notes and parse into DB records + channel messages
    let mut db_notes = Vec::new();
    let mut db_orders = Vec::new();
    let mut ingest_orders = Vec::new();

    for note in &new_notes {
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

        let note_id_bytes = note.id().to_bytes().to_vec();

        let mut raw_data = Vec::new();
        note.write_into(&mut raw_data);

        db_notes.push(NoteRow {
            note_id: note_id_bytes.clone(),
            account_id: order.creator_id.to_bytes().to_vec(),
            raw_data: raw_data.clone(),
        });

        db_orders.push(OrderRow {
            note_id: note_id_bytes,
            account_id: order.creator_id.to_bytes().to_vec(),
            requested_asset: order.requested_faucet_id.to_bytes().to_vec(),
            requested_amount: order.requested_amount as i64,
            offered_asset: order.offered_faucet_id.to_bytes().to_vec(),
            offered_amount: order.offered_amount as i64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            status: OrderStatus::Active.as_str().to_string(),
        });

        ingest_orders.push(IngestOrder {
            note_id: note.id(),
            offered_token: order.offered_faucet_id,
            requested_token: order.requested_faucet_id,
            offered_amount: order.offered_amount,
            requested_amount: order.requested_amount,
            raw_note_data: raw_data,
        });
    }

    if db_notes.is_empty() {
        return Ok(());
    }

    // 4. Atomic DB insert (notes + orders + block number). The returned set
    //    is the orders that were *actually* inserted this call — i.e. seen
    //    for the first time. Duplicates (already-known note_ids) are excluded
    //    by the `orders` primary key. This is the durable, bounded dedup that
    //    replaces the old in-memory `seen_notes` HashSet: a note enters the
    //    matcher channel exactly once, at first commit, even across restarts.
    let inserted = {
        let mut conn = pool.write_conn()?;
        db::insert_notes_batch(&mut conn, &db_notes, &db_orders, block_num)?
    };

    // 5. Forward only first-seen orders to the matcher. Backpressure via the
    //    bounded channel; an error means the matcher has shut down.
    let mut forwarded = 0usize;
    for order in ingest_orders {
        if inserted.contains(order.note_id.to_bytes().as_slice()) {
            order_tx
                .send(order)
                .await
                .map_err(|_| anyhow::anyhow!("matcher channel closed"))?;
            forwarded += 1;
        }
    }
    tracing::info!(
        persisted = inserted.len(),
        forwarded,
        block = block_num,
        "ingested PSWAP orders"
    );

    Ok(())
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
        let summary = client
            .sync_state()
            .await
            .map_err(|e| anyhow!("sync_state failed: {e}"))?;

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
                    Err(e) => {
                        tracing::error!(%note_id, error = %e, "InputNoteRecord → Note conversion failed")
                    }
                },
                Ok(None) => {
                    tracing::warn!(%note_id, "note reported by sync but not in input-notes store");
                }
                Err(e) => {
                    tracing::error!(%note_id, error = %e, "get_input_note failed");
                }
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
            .map_err(|e| anyhow!("get_nullifier_commit_heights failed: {e}"))?;

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
    order_tx: mpsc::Sender<IngestOrder>,
    consumed_tx: mpsc::Sender<NoteId>,
    subscribe_rx: mpsc::Receiver<(TokenId, TokenId)>,
    ingest_interval: Duration,
    last_sync: Arc<AtomicI64>,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ingest_ready_tx, ingest_ready_rx) = oneshot::channel::<Result<()>>();
    let ingest_factory = factory;
    let ingest_db = db_pool;
    let ingest_cancel = cancel;
    let ingest_order_tx = order_tx;
    let ingest_consumed_tx = consumed_tx;
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
                    ingest_order_tx,
                    ingest_consumed_tx,
                    ingest_subscribe_rx,
                    ingest_interval,
                    ingest_cancel.clone(),
                    ingest_last_sync,
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
                // coordination loop has no other signal — `order_tx` keeps
                // other live senders, so its `order_rx` never closes and the
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

    /// Mock MidenClient for testing.
    pub struct MockMidenClient {
        notes: Vec<Note>,
        block: u64,
        /// Pre-staged consumed-note IDs returned by the next sync_state call.
        /// Drained on each sync so a single push delivers once.
        pending_consumed: Vec<NoteId>,
        /// Set of note IDs that should be reported as consumed by
        /// `check_consumed_notes`. Persistent — represents on-chain state.
        consumed_set: HashSet<NoteId>,
        /// Canned on-chain metadata returned by `fetch_token_metadata`
        /// (`None` = the faucet has no public metadata).
        token_metadata: Option<(u8, String)>,
    }

    impl MockMidenClient {
        pub fn new() -> Self {
            Self {
                notes: Vec::new(),
                block: 0,
                pending_consumed: Vec::new(),
                consumed_set: HashSet::new(),
                token_metadata: None,
            }
        }

        /// Stage the `(decimals, ticker)` that `fetch_token_metadata` returns.
        pub fn set_token_metadata(&mut self, decimals: u8, ticker: &str) {
            self.token_metadata = Some((decimals, ticker.to_string()));
        }

        pub fn add_notes(&mut self, notes: Vec<Note>, block: u64) {
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
        async fn subscribe_pair(&mut self, _offered: TokenId, _requested: TokenId) -> Result<()> {
            Ok(())
        }

        async fn sync_state(&mut self) -> Result<SyncResult> {
            let consumed = std::mem::take(&mut self.pending_consumed);
            Ok(SyncResult {
                block_num: self.block,
                new_notes: self.notes.clone(),
                consumed_notes: consumed,
            })
        }

        async fn check_consumed_notes(&mut self, notes: &[Note]) -> Result<HashSet<NoteId>> {
            Ok(notes
                .iter()
                .map(|n| n.id())
                .filter(|id| self.consumed_set.contains(id))
                .collect())
        }

        async fn fetch_token_metadata(&mut self, _faucet_id: TokenId) -> Result<Option<(u8, String)>> {
            Ok(self.token_metadata.clone())
        }
    }
}

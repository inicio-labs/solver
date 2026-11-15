//! A node-like wrapper around `MockRpcApi`, for the maker gateway tests.
//!
//! `MockRpcApi` queues every submitted transaction and only fails later, by
//! panicking in `prove_block`, when one spends a note that is unknown or
//! already spent. A real Miden node refuses such a submission at once: a note
//! already spent in a block (code 8), a note a waiting transaction already
//! spends (code 2, "nullifiers already exist"), an unauthenticated note
//! nobody created (code 2, "unauthenticated input notes are unknown"). This
//! wrapper does the same, in the node's order. Every other method forwards to
//! the mock unchanged; only `submit_proven_transaction` differs.
//!
//! Not modelled: duplicate output notes, account state, expiry, and a node
//! that goes away.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use miden_client::rpc::domain::account::{AccountDetails, AccountProof, GetAccountRequest};
use miden_client::rpc::domain::account_vault::AccountVaultInfo;
use miden_client::rpc::domain::note::{FetchedNote, ResolvedSyncNotesBlock, SyncNotesBlock};
use miden_client::rpc::domain::nullifier::NullifierUpdate;
use miden_client::rpc::domain::storage_map::StorageMapInfo;
use miden_client::rpc::domain::sync::{ChainMmrInfo, SyncTarget};
use miden_client::rpc::domain::transaction::TransactionRecord;
use miden_client::rpc::encryption::{AttestedTransactionEncryptionKey, SealedTransactionInputs};
use miden_client::rpc::{
    AddTransactionError, EndpointError, GrpcError, NetworkNoteStatusInfo, NodeRpcClient,
    NoteContentFetch, RpcEndpoint, RpcError, RpcLimits, RpcStatusInfo,
};
use miden_client::store::InputNoteRecord;
use miden_client::testing::mock::MockRpcApi;
use miden_protocol::account::{Account, AccountId};
use miden_protocol::address::NetworkId;
use miden_protocol::batch::{ProposedBatch, ProvenBatch};
use miden_protocol::block::{BlockHeader, BlockNumber, SignedBlock};
use miden_protocol::crypto::merkle::mmr::MmrProof;
use miden_protocol::note::{NoteId, NoteScript, NoteTag, Nullifier};
use miden_protocol::transaction::{ProvenTransaction, TransactionId};
use miden_protocol::vm::ExecutionProof;
use miden_protocol::Word;

/// A code-2 refusal in the shape miden-client 0.17.5 reports for a node's
/// mempool conflicts. The text is built to match the node's (`{:?}` of the
/// IDs); it is not a captured sample.
fn state_conflict(cause: String) -> RpcError {
    RpcError::RequestError {
        endpoint: RpcEndpoint::SubmitProvenTx,
        error_kind: GrpcError::InvalidArgument,
        endpoint_error: Some(EndpointError::AddTransaction(
            AddTransactionError::StateConflict {
                message: format!(
                    "transaction conflicts with current mempool state\ncaused by: {cause}"
                ),
            },
        )),
        source: None,
    }
}

/// Refusal for unauthenticated input notes the node does not know.
pub fn unknown_notes_error(ids: &[NoteId]) -> RpcError {
    state_conflict(format!("unauthenticated input notes are unknown: {ids:?}"))
}

/// Refusal for notes a waiting transaction already spends.
pub fn spent_notes_error(nullifiers: &[Nullifier]) -> RpcError {
    state_conflict(format!("nullifiers already exist: {nullifiers:?}"))
}

/// Refusal for notes already spent in a block (code 8).
pub fn committed_spend_error(nullifiers: &[Nullifier]) -> RpcError {
    RpcError::RequestError {
        endpoint: RpcEndpoint::SubmitProvenTx,
        error_kind: GrpcError::InvalidArgument,
        endpoint_error: Some(EndpointError::AddTransaction(
            AddTransactionError::AuthenticationFailed {
                message: format!(
                    "failed to authenticate transaction\ncaused by: nullifiers already exist: \
                     {nullifiers:?}"
                ),
            },
        )),
        source: None,
    }
}

/// The notes a transaction creates and spends.
struct Effects {
    outputs: Vec<NoteId>,
    nullifiers: Vec<Nullifier>,
}

/// What the wrapper knows about transactions submitted through it.
#[derive(Default)]
struct State {
    /// Accepted and not yet seen in a block: the wrapper's "mempool".
    waiting: BTreeMap<TransactionId, Effects>,
    /// Every transaction seen in a block, and how many blocks were read for
    /// it.
    included: BTreeSet<TransactionId>,
    scanned: usize,
    /// Nullifiers the blocks record as spent. A note created and spent in one
    /// block is erased: the block leaves its nullifier out, so a later spend
    /// of it is refused as unknown, as a real node does.
    spent: BTreeSet<Nullifier>,
}

pub struct NodeLikeRpc {
    inner: Arc<MockRpcApi>,
    state: Mutex<State>,
}

impl NodeLikeRpc {
    pub fn new(inner: Arc<MockRpcApi>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            state: Mutex::new(State::default()),
        })
    }

    /// Commit every queued transaction in one block. A note created and
    /// spent in that block was erased and is unknown again.
    pub fn prove_block(&self) {
        self.inner.prove_block();
        self.settle();
    }

    /// Move the transactions the chain now holds out of the mempool. This is
    /// decided by the blocks' own transaction IDs, not by when it runs: the
    /// solver submits from another thread, so a submission can reach the mock
    /// after it has drained its queue for a block and before this call. It
    /// stays in the mempool until a block really holds it.
    fn settle(&self) {
        let mut state = self.state.lock().unwrap();
        let (included, spent): (Vec<TransactionId>, Vec<Nullifier>) = {
            let chain = self.inner.mock_chain.read();
            let blocks = chain.proven_blocks();
            let new = &blocks[state.scanned.min(blocks.len())..];
            state.scanned = blocks.len();
            (
                new.iter()
                    .flat_map(|block| block.body().transactions().as_slice())
                    .map(|transaction| transaction.id())
                    .collect(),
                new.iter()
                    .flat_map(|block| block.body().created_nullifiers())
                    .copied()
                    .collect(),
            )
        };
        state.spent.extend(spent);
        for id in included {
            state.waiting.remove(&id);
            state.included.insert(id);
        }
    }
}

#[async_trait::async_trait]
impl NodeRpcClient for NodeLikeRpc {
    async fn submit_proven_transaction(
        &self,
        proven_transaction: &ProvenTransaction,
        sealed_transaction_inputs: SealedTransactionInputs,
    ) -> Result<BlockNumber, RpcError> {
        // A block proven behind this wrapper's back still counts.
        self.settle();
        // The node checks spent notes, on chain then in its mempool, before
        // unknown notes.
        let inputs = proven_transaction.input_notes();
        let nullifiers: Vec<Nullifier> = inputs.iter().map(|note| note.nullifier()).collect();
        // Only unauthenticated inputs carry a header.
        let unauthenticated: Vec<NoteId> = inputs
            .iter()
            .filter_map(|note| note.header().map(|header| header.id()))
            .collect();
        {
            // Read the chain under the same lock as the mempool, in the order
            // `settle` takes them, so a block proven between the two reads
            // can't hide a note from both.
            let state = self.state.lock().unwrap();
            let off_chain: Vec<NoteId> = {
                let chain = self.inner.mock_chain.read();
                unauthenticated
                    .into_iter()
                    .filter(|id| !chain.committed_notes().contains_key(id))
                    .collect()
            };
            let committed: Vec<Nullifier> = nullifiers
                .iter()
                .copied()
                .filter(|nullifier| state.spent.contains(nullifier))
                .collect();
            if !committed.is_empty() {
                return Err(committed_spend_error(&committed));
            }
            let waiting = |nullifier: &Nullifier| {
                state
                    .waiting
                    .values()
                    .any(|effects| effects.nullifiers.contains(nullifier))
            };
            let spent: Vec<Nullifier> = nullifiers
                .iter()
                .copied()
                .filter(|nullifier| waiting(nullifier))
                .collect();
            if !spent.is_empty() {
                return Err(spent_notes_error(&spent));
            }
            let unknown: Vec<NoteId> = off_chain
                .into_iter()
                .filter(|id| {
                    !state
                        .waiting
                        .values()
                        .any(|effects| effects.outputs.contains(id))
                })
                .collect();
            if !unknown.is_empty() {
                return Err(unknown_notes_error(&unknown));
            }
        }
        let accepted = self
            .inner
            .submit_proven_transaction(proven_transaction, sealed_transaction_inputs)
            .await?;
        let id = proven_transaction.id();
        let mut state = self.state.lock().unwrap();
        // A block that took it before this line ran already recorded what it
        // spent.
        if !state.included.contains(&id) {
            let outputs = proven_transaction
                .output_notes()
                .iter()
                .map(|note| note.id())
                .collect();
            state.waiting.insert(
                id,
                Effects {
                    outputs,
                    nullifiers,
                },
            );
        }
        Ok(accepted)
    }

    async fn set_genesis_commitment(&self, commitment: Word) -> Result<(), RpcError> {
        self.inner.set_genesis_commitment(commitment).await
    }

    fn has_genesis_commitment(&self) -> Option<Word> {
        self.inner.has_genesis_commitment()
    }

    async fn get_transaction_encryption_key(
        &self,
    ) -> Result<AttestedTransactionEncryptionKey, RpcError> {
        self.inner.get_transaction_encryption_key().await
    }

    async fn submit_proven_batch(
        &self,
        proven_batch: &ProvenBatch,
        proposed_batch: &ProposedBatch,
        transaction_inputs: Vec<SealedTransactionInputs>,
    ) -> Result<BlockNumber, RpcError> {
        self.inner
            .submit_proven_batch(proven_batch, proposed_batch, transaction_inputs)
            .await
    }

    async fn get_block_header_by_number(
        &self,
        block_num: Option<BlockNumber>,
        include_mmr_proof: bool,
    ) -> Result<(BlockHeader, Option<MmrProof>), RpcError> {
        self.inner
            .get_block_header_by_number(block_num, include_mmr_proof)
            .await
    }

    async fn get_block_by_number(
        &self,
        block_num: BlockNumber,
        include_proof: bool,
    ) -> Result<(SignedBlock, Option<ExecutionProof>), RpcError> {
        self.inner
            .get_block_by_number(block_num, include_proof)
            .await
    }

    async fn get_notes_by_id(&self, note_ids: &[NoteId]) -> Result<Vec<FetchedNote>, RpcError> {
        self.inner.get_notes_by_id(note_ids).await
    }

    async fn sync_chain_mmr(
        &self,
        current_block_height: BlockNumber,
        upper_bound: SyncTarget,
    ) -> Result<ChainMmrInfo, RpcError> {
        self.inner
            .sync_chain_mmr(current_block_height, upper_bound)
            .await
    }

    async fn get_account_details(
        &self,
        account_id: AccountId,
    ) -> Result<Option<Account>, RpcError> {
        self.inner.get_account_details(account_id).await
    }

    async fn sync_notes(
        &self,
        block_from: BlockNumber,
        block_to: BlockNumber,
        note_tags: &BTreeSet<NoteTag>,
    ) -> Result<Vec<SyncNotesBlock>, RpcError> {
        self.inner.sync_notes(block_from, block_to, note_tags).await
    }

    async fn sync_notes_with_content(
        &self,
        block_from: BlockNumber,
        block_to: BlockNumber,
        note_tags: &BTreeSet<NoteTag>,
        fetch: NoteContentFetch,
    ) -> Result<Vec<ResolvedSyncNotesBlock>, RpcError> {
        self.inner
            .sync_notes_with_content(block_from, block_to, note_tags, fetch)
            .await
    }

    async fn sync_nullifiers(
        &self,
        prefix: &[u16],
        block_from: BlockNumber,
        block_to: BlockNumber,
    ) -> Result<Vec<NullifierUpdate>, RpcError> {
        self.inner
            .sync_nullifiers(prefix, block_from, block_to)
            .await
    }

    async fn get_account(
        &self,
        account_id: AccountId,
        request: GetAccountRequest,
    ) -> Result<(BlockNumber, AccountProof), RpcError> {
        self.inner.get_account(account_id, request).await
    }

    async fn register_account(
        &self,
        invitation_code: &str,
        account_id: AccountId,
    ) -> Result<(), RpcError> {
        self.inner
            .register_account(invitation_code, account_id)
            .await
    }

    async fn is_account_allowed(&self, account_id: AccountId) -> Result<bool, RpcError> {
        self.inner.is_account_allowed(account_id).await
    }

    async fn is_invitation_code_valid(&self, invitation_code: &str) -> Result<bool, RpcError> {
        self.inner.is_invitation_code_valid(invitation_code).await
    }

    async fn resolve_oversize_vault(
        &self,
        account_id: AccountId,
        block_to: BlockNumber,
        details: &mut AccountDetails,
    ) -> Result<(), RpcError> {
        self.inner
            .resolve_oversize_vault(account_id, block_to, details)
            .await
    }

    async fn resolve_oversize_storage_maps(
        &self,
        account_id: AccountId,
        block_to: BlockNumber,
        details: &mut AccountDetails,
    ) -> Result<(), RpcError> {
        self.inner
            .resolve_oversize_storage_maps(account_id, block_to, details)
            .await
    }

    async fn get_nullifier_commit_heights(
        &self,
        requested_nullifiers: BTreeSet<Nullifier>,
        block_from: BlockNumber,
    ) -> Result<BTreeMap<Nullifier, Option<BlockNumber>>, RpcError> {
        self.inner
            .get_nullifier_commit_heights(requested_nullifiers, block_from)
            .await
    }

    async fn get_public_note_records(
        &self,
        note_ids: &[NoteId],
        current_timestamp: Option<u64>,
    ) -> Result<Vec<InputNoteRecord>, RpcError> {
        self.inner
            .get_public_note_records(note_ids, current_timestamp)
            .await
    }

    async fn get_block_header_with_proof(
        &self,
        block_num: BlockNumber,
    ) -> Result<(BlockHeader, MmrProof), RpcError> {
        self.inner.get_block_header_with_proof(block_num).await
    }

    async fn get_note_by_id(&self, note_id: NoteId) -> Result<FetchedNote, RpcError> {
        self.inner.get_note_by_id(note_id).await
    }

    async fn get_note_script_by_root(&self, root: Word) -> Result<Option<NoteScript>, RpcError> {
        self.inner.get_note_script_by_root(root).await
    }

    async fn sync_storage_maps(
        &self,
        block_from: BlockNumber,
        block_to: BlockNumber,
        account_id: AccountId,
    ) -> Result<StorageMapInfo, RpcError> {
        self.inner
            .sync_storage_maps(block_from, block_to, account_id)
            .await
    }

    async fn sync_account_vault(
        &self,
        block_from: BlockNumber,
        block_to: BlockNumber,
        account_id: AccountId,
    ) -> Result<AccountVaultInfo, RpcError> {
        self.inner
            .sync_account_vault(block_from, block_to, account_id)
            .await
    }

    async fn sync_transactions(
        &self,
        block_from: BlockNumber,
        block_to: BlockNumber,
        account_ids: Vec<AccountId>,
    ) -> Result<Vec<TransactionRecord>, RpcError> {
        self.inner
            .sync_transactions(block_from, block_to, account_ids)
            .await
    }

    async fn get_network_id(&self) -> Result<NetworkId, RpcError> {
        self.inner.get_network_id().await
    }

    async fn get_rpc_limits(&self) -> Result<RpcLimits, RpcError> {
        self.inner.get_rpc_limits().await
    }

    fn has_rpc_limits(&self) -> Option<RpcLimits> {
        self.inner.has_rpc_limits()
    }

    async fn set_rpc_limits(&self, limits: RpcLimits) {
        self.inner.set_rpc_limits(limits).await
    }

    async fn get_status_unversioned(&self) -> Result<RpcStatusInfo, RpcError> {
        self.inner.get_status_unversioned().await
    }

    async fn get_network_note_status(
        &self,
        note_id: NoteId,
    ) -> Result<NetworkNoteStatusInfo, RpcError> {
        self.inner.get_network_note_status(note_id).await
    }
}

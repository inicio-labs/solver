//! The Miden side: the maker's wallet account, its order notes and its
//! incoming notes. Built on miden-client 0.17.5.
//!
//! Testnet rules this module follows:
//! - Sync before building a transaction. A transaction built on a stale block
//!   expires early: one that moves USDCx (the fee token) expires 20 blocks,
//!   about 60 s, after the block it was built on.
//! - Prove remotely with a timeout under that window (config: 45 s).
//! - Every transaction pays a fee in USDCx; keep some in the account.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use miden_client::account::component::BasicWallet;
use miden_client::account::{AccountBuilder, AccountBuilderSchemaCommitmentExt, AccountType};
use miden_client::auth::{AuthSchemeId, AuthSecretKey, AuthSingleSig};
use miden_client::block::BlockNumber;
use miden_client::builder::ClientBuilder;
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::note::NoteType;
use miden_client::rpc::{Endpoint, EndpointError, GrpcClient, NodeRpcClient, RegisterAccountError, VerifyingRpcClient};
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_client::{Client, RemoteTransactionProver};
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use miden_protocol::Felt;
use miden_standards::account::auth::Approver;
use miden_standards::note::{NoteConsumptionStatus, PswapNote};

use crate::config::Config;

pub type MidenClient = Client<FilesystemKeyStore>;

/// Client with the sqlite store, the keystore and the remote prover.
pub async fn client(cfg: &Config) -> Result<(MidenClient, Arc<FilesystemKeyStore>)> {
    let endpoint = Endpoint::try_from(cfg.network.rpc.as_str()).map_err(|e| anyhow!("rpc endpoint: {e}"))?;
    let rpc: Arc<dyn NodeRpcClient> =
        Arc::new(VerifyingRpcClient::new(GrpcClient::new(&endpoint, cfg.network.rpc_timeout_ms)));
    if let Some(dir) = cfg.wallet.store.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::create_dir_all(&cfg.wallet.keystore)?;
    let keystore = Arc::new(FilesystemKeyStore::new(cfg.wallet.keystore.clone()).context("open keystore")?);
    let mut builder = ClientBuilder::new()
        .rpc(rpc)
        .sqlite_store(cfg.wallet.store.clone())
        .authenticator(keystore.clone());
    if let Some(url) = &cfg.network.prover {
        // miden-client's default is 10 s, too short for many transactions.
        let prover = RemoteTransactionProver::new(url.clone())
            .with_timeout(Duration::from_millis(cfg.network.prover_timeout_ms));
        builder = builder.prover(Arc::new(prover));
    }
    let client = builder.build().await.context("build miden client")?;
    Ok((client, keystore))
}

/// Create a new public wallet account (Falcon key in the keystore). It is
/// deployed on chain by its first transaction.
pub async fn create_account(client: &mut MidenClient, keystore: &FilesystemKeyStore) -> Result<AccountId> {
    let key = AuthSecretKey::new_falcon512_poseidon2();
    let auth = AuthSingleSig::new(Approver::new(key.public_key().to_commitment(), AuthSchemeId::Falcon512Poseidon2));
    let account = AccountBuilder::new(rand::random())
        .account_type(AccountType::Public)
        .with_component(auth)
        .with_component(BasicWallet)
        .build_with_schema_commitment()
        .context("build wallet account")?;
    let id = account.id();
    keystore.add_key(&key, id).await.context("add key to keystore")?;
    client.add_account(&account, false).await.context("add account to store")?;
    Ok(id)
}

/// Register the account with the network. The testnet funds a new
/// registration once with a small USDCx note (10,000 units, 0.01 USDCx) so the
/// account can pay its first fees; `claim` then moves it into the vault.
pub async fn register(cfg: &Config, client: &mut MidenClient, id: AccountId) -> Result<()> {
    client.sync_state().await.context("sync")?;
    let (genesis, _) = client
        .get_block_header_by_num(BlockNumber::GENESIS)
        .await?
        .ok_or_else(|| anyhow!("genesis header not in the store"))?;
    let endpoint = Endpoint::try_from(cfg.network.rpc.as_str()).map_err(|e| anyhow!("rpc endpoint: {e}"))?;
    let node = GrpcClient::new(&endpoint, cfg.network.rpc_timeout_ms);
    node.set_genesis_commitment(genesis.commitment()).await.context("set genesis commitment")?;
    match node.register_account("", id).await {
        Ok(()) => println!("registered {}: a USDCx fee note is on its way; run `maker claim` in a minute", id.to_hex()),
        Err(e)
            if matches!(
                e.endpoint_error(),
                Some(EndpointError::RegisterAccount(RegisterAccountError::AlreadyRegistered))
            ) =>
        {
            println!("{} is already registered (a registration is funded only once)", id.to_hex())
        }
        Err(e) => bail!("register {}: {e}", id.to_hex()),
    }
    Ok(())
}

pub async fn balance(client: &mut MidenClient, id: AccountId, faucet: AccountId) -> Result<u64> {
    let amount = client.account_reader(id).get_balance(faucet).await.map_err(|e| anyhow!("balance: {e}"))?;
    Ok(u64::from(amount))
}

/// Consume incoming notes (faucet mints, USDCx, paybacks from fills) one at
/// a time. Only notes this account can spend now are fetched: your 32-bit
/// note tag also matches many other accounts' notes, which you can't consume.
/// One note that still fails must not block the rest, so failures are skipped.
/// Your own order notes are never consumed here; see `reclaim`.
pub async fn claim(client: &mut MidenClient, id: AccountId) -> Result<(usize, usize)> {
    client.sync_state().await.context("sync")?;
    let mut notes = Vec::new();
    for (record, statuses) in client.get_consumable_notes(Some(id)).await? {
        let spendable = statuses.iter().any(|(account, status)| {
            *account == id
                && matches!(status, NoteConsumptionStatus::Consumable | NoteConsumptionStatus::ConsumableWithAuthorization)
        });
        let note: Result<Note, _> = record.try_into();
        let Ok(note) = note else { continue };
        if !spendable || note.recipient().script().root() == PswapNote::script_root() {
            continue;
        }
        notes.push(note);
    }
    let (mut claimed, mut skipped) = (0, 0);
    for note in notes {
        let note_id = note.id().to_hex();
        client.sync_state().await.context("sync")?;
        let result = match TransactionRequestBuilder::new().build_consume_notes(vec![note]) {
            Ok(request) => client.submit_new_transaction(id, request).await.map(|_| ()).map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match result {
            Ok(()) => claimed += 1,
            Err(e) => {
                skipped += 1;
                eprintln!("skip note {note_id}: {}", e.chars().take(160).collect::<String>());
            }
        }
    }
    Ok((claimed, skipped))
}

/// Create an order: a PSWAP note offering `offered` for `requested`, created
/// and committed by the maker's own account. Private by default (its terms
/// stay off chain until it fills); paybacks public, so the wallet finds them
/// by syncing. Returns the note to submit to the gateway.
pub async fn create_order(
    client: &mut MidenClient,
    id: AccountId,
    offered: FungibleAsset,
    requested: FungibleAsset,
    public_note: bool,
) -> Result<Note> {
    client.sync_state().await.context("sync")?;
    let note_type = if public_note { NoteType::Public } else { NoteType::Private };
    let request = TransactionRequestBuilder::new()
        .build_pswap_create(&PswapTransactionData::new(id, offered, requested), note_type, NoteType::Public, None, client.rng())
        .map_err(|e| anyhow!(e).context("build the PSWAP note"))?;
    let note = request
        .expected_output_own_notes()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("the request creates no note"))?;
    client
        .submit_new_transaction(id, request)
        .await
        .map_err(|e| anyhow!(e).context("commit the PSWAP note"))?;
    Ok(note)
}

/// Take back what is left in your own order note. Cancel first and wait for
/// any pending settlement to resolve (guide, section 10). After a partial
/// fill, reclaim the rebuilt remainder instead of the original note.
pub async fn reclaim(client: &mut MidenClient, id: AccountId, note: Note) -> Result<()> {
    client.sync_state().await.context("sync")?;
    let request = TransactionRequestBuilder::new().build_consume_notes(vec![note])?;
    client.submit_new_transaction(id, request).await.map_err(|e| anyhow!(e).context("reclaim"))?;
    Ok(())
}

/// The 47-byte lineage id: one id for an order and all its remainders.
/// creator account id (15) || serial[0..3] (3 x 8, big-endian) || serial[3] - depth (8).
pub fn lineage_id(note: &Note) -> Result<Vec<u8>> {
    let pswap = PswapNote::try_from(note).map_err(|e| anyhow!("not a PSWAP note: {e:?}"))?;
    let serial = pswap.serial_number();
    let root_s3 = serial[3] - Felt::from(pswap.parent_depth());
    let mut id = pswap.storage().creator_account_id().to_bytes();
    for element in [serial[0], serial[1], serial[2], root_s3] {
        id.extend_from_slice(&element.as_canonical_u64().to_be_bytes());
    }
    Ok(id)
}

pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

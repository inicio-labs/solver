//! On-chain account + note helpers, adapted from miden-client v0.15.2's own
//! canonical test helpers (`rust-client/src/test_utils/common.rs`,
//! `miden-bench/src/deploy.rs`). Everything here drives a live devnet client.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use miden_client::account::component::{
    BasicWallet,
    BurnPolicyConfig,
    FungibleFaucet,
    MintPolicyConfig,
    PolicyRegistration,
    TokenPolicyManager,
};
use miden_client::account::{AccountBuilder, AccountBuilderSchemaCommitmentExt, AccountType};
use miden_client::auth::{AuthSchemeId, AuthSecretKey, AuthSingleSig};
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::note::Note;
use miden_client::store::{NoteFilter, TransactionFilter};
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder, TransactionStatus};
use miden_client::Client;
use miden_protocol::account::AccountId;
use miden_protocol::asset::{AssetAmount, FungibleAsset, TokenSymbol};
use miden_protocol::note::NoteType;
use miden_protocol::transaction::TransactionId;
use miden_standards::account::faucets::TokenName;
use rand::RngCore;

type DevClient = Client<FilesystemKeyStore>;

/// Falcon512 auth scheme used for all accounts the harness creates.
const SCHEME: AuthSchemeId = AuthSchemeId::Falcon512Poseidon2;

/// Create + register a new **public** basic wallet account (key → keystore,
/// account → client store). The account is deployed lazily on its first
/// transaction (e.g. its first note consume).
pub async fn create_wallet(
    client: &mut DevClient,
    keystore: &FilesystemKeyStore,
) -> Result<AccountId> {
    let key = AuthSecretKey::new_falcon512_poseidon2();
    let auth = AuthSingleSig::new(key.public_key().to_commitment(), SCHEME);

    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);

    let account = AccountBuilder::new(seed)
        .account_type(AccountType::Public)
        .with_auth_component(auth)
        .with_component(BasicWallet)
        .build_with_schema_commitment()
        .context("build wallet account")?;

    let id = account.id();
    keystore.add_key(&key, id).await.context("add wallet key to keystore")?;
    client.add_account(&account, false).await.context("add wallet to client store")?;
    Ok(id)
}

/// Create + register a new **public** fungible faucet. Mint/burn-only token
/// policy (NO transfer policies — registering those would force callback flags
/// and break `FungibleAsset::new`-built mints; see the upstream note). Deployed
/// lazily on its first mint.
pub async fn create_faucet(
    client: &mut DevClient,
    keystore: &FilesystemKeyStore,
    symbol: &str,
    decimals: u8,
    max_supply: u64,
) -> Result<AccountId> {
    let key = AuthSecretKey::new_falcon512_poseidon2();
    let auth = AuthSingleSig::new(key.public_key().to_commitment(), SCHEME);

    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);

    let symbol = TokenSymbol::new(symbol).context("invalid token symbol")?;
    let name = TokenName::new(&symbol.to_string()).context("invalid token name")?;
    let faucet = FungibleFaucet::builder()
        .name(name)
        .symbol(symbol)
        .decimals(decimals)
        .max_supply(AssetAmount::new(max_supply).context("invalid max supply")?)
        .build()
        .context("build faucet component")?;

    let policy = TokenPolicyManager::new()
        .with_mint_policy(MintPolicyConfig::AllowAll, PolicyRegistration::Active)
        .context("mint policy")?
        .with_burn_policy(BurnPolicyConfig::AllowAll, PolicyRegistration::Active)
        .context("burn policy")?;

    let account = AccountBuilder::new(seed)
        .account_type(AccountType::Public)
        .with_auth_component(auth)
        .with_component(faucet)
        .with_components(policy)
        .build_with_schema_commitment()
        .context("build faucet account")?;

    let id = account.id();
    keystore.add_key(&key, id).await.context("add faucet key to keystore")?;
    client.add_account(&account, false).await.context("add faucet to client store")?;
    Ok(id)
}

/// Mint `amount` base units of `faucet_id`'s asset to `target_id` as a public
/// note. Returns the submitted tx id and the expected output note (so the
/// target can consume it). Does NOT wait for commit.
pub async fn mint(
    client: &mut DevClient,
    faucet_id: AccountId,
    target_id: AccountId,
    amount: u64,
) -> Result<(TransactionId, Note)> {
    let asset = FungibleAsset::new(faucet_id, amount).context("build fungible asset")?;
    let request = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(asset, target_id, NoteType::Public, client.rng())
        .context("build mint request")?;
    let note = request
        .expected_output_own_notes()
        .pop()
        .context("mint request produced no output note")?;
    let tx_id = client
        .submit_new_transaction(faucet_id, request)
        .await
        .context("submit mint transaction")?;
    Ok((tx_id, note))
}

/// Create a **public** PSWAP note: `creator_id` offers `offered` and requests
/// `requested`. Public so the solver's keyless ingest discovers it on-chain.
/// Returns the submitted tx id and the created PSWAP note. Does NOT wait.
pub async fn create_pswap(
    client: &mut DevClient,
    creator_id: AccountId,
    offered: FungibleAsset,
    requested: FungibleAsset,
) -> Result<(TransactionId, Note)> {
    let data = PswapTransactionData::new(creator_id, offered, requested);
    let request = TransactionRequestBuilder::new()
        .build_pswap_create(&data, NoteType::Public, NoteType::Public, None, client.rng())
        .context("build pswap-create request")?;
    let note = request
        .expected_output_own_notes()
        .into_iter()
        .next()
        .context("pswap-create produced no output note")?;
    let tx_id = client
        .submit_new_transaction(creator_id, request)
        .await
        .context("submit pswap-create transaction")?;
    Ok((tx_id, note))
}

/// Consume `notes` into `account_id` (crediting their assets to its vault).
/// Returns the submitted tx id. Does NOT wait for commit.
pub async fn consume(
    client: &mut DevClient,
    account_id: AccountId,
    notes: Vec<Note>,
) -> Result<TransactionId> {
    let request = TransactionRequestBuilder::new()
        .build_consume_notes(notes)
        .context("build consume request")?;
    client
        .submit_new_transaction(account_id, request)
        .await
        .context("submit consume transaction")
}

/// Fetch all currently-committed (spendable) input notes the client tracks for
/// the accounts it owns, as consumable [`Note`]s.
pub async fn committed_input_notes(client: &DevClient) -> Result<Vec<Note>> {
    let mut notes = Vec::new();
    for record in client.get_input_notes(NoteFilter::Committed).await? {
        notes.push(record.try_into().context("input-note record -> Note")?);
    }
    Ok(notes)
}

/// On-chain balance of `faucet_id`'s asset in `account_id`'s vault.
pub async fn balance(client: &DevClient, account_id: AccountId, faucet_id: AccountId) -> Result<u64> {
    client
        .account_reader(account_id)
        .get_balance(faucet_id)
        .await
        .with_context(|| format!("read balance of {faucet_id} in {account_id}"))
}

/// Sync the client and block until `tx_id` is committed (or discarded → error).
pub async fn wait_for_tx(client: &mut DevClient, tx_id: TransactionId) -> Result<()> {
    loop {
        client.sync_state().await.context("sync while waiting for tx")?;
        let tracked = client
            .get_transactions(TransactionFilter::Ids(vec![tx_id]))
            .await
            .with_context(|| format!("get transaction {tx_id}"))?
            .pop()
            .with_context(|| format!("transaction {tx_id} not tracked"))?;
        match tracked.status {
            TransactionStatus::Committed { .. } => return Ok(()),
            TransactionStatus::Pending => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            TransactionStatus::Discarded(cause) => {
                bail!("transaction {tx_id} was discarded: {cause:?}");
            }
        }
    }
}

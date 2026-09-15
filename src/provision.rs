//! Operator subcommands that stand up the solver account on a Miden 0.16 network:
//! `provision-account` creates it and `fund-account` deploys and funds it.
//!
//! On 0.16 every transaction pays a fee in the native asset from the account's own
//! vault, so a brand-new account can't deploy with an empty one. The flow is:
//! `provision-account` → send native tokens to the printed account as a PUBLIC note
//! (e.g. from the testnet faucet) → `fund-account`, whose single transaction
//! consumes those notes and so deploys and funds the account at once.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use miden_client::account::component::BasicWallet;
use miden_client::account::{AccountBuilder, AccountBuilderSchemaCommitmentExt, AccountType};
use miden_client::auth::AuthSchemeId;
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::note::Note;
use miden_client::transaction::TransactionRequestBuilder;
use miden_protocol::account::auth::AuthSecretKey;
use miden_protocol::account::AccountId;
use miden_standards::account::auth::{Approver, AuthSingleSig};
use solver::config::SolverConfig;
use solver::ClientFactory;

use crate::client_factory::ProdClientFactory;

/// How long `fund-account` waits for its transaction to show in the balance.
const FUND_POLL_INTERVAL: Duration = Duration::from_secs(2);
const FUND_POLL_ATTEMPTS: u32 = 30;

/// Create a public solver wallet (`BasicWallet` + single-sig Falcon auth) in the
/// executor's store and keystore, and print its id. `BasicWallet` also exposes
/// `note_creator::create_note`, which the 0.16 PSWAP script calls to emit a
/// remainder note from the solver's context on a partial fill.
pub async fn provision_account(config: &SolverConfig) -> Result<()> {
    let factory = ProdClientFactory::from_config(config);
    let mut client = factory.build_executor().await?;
    client.sync_state().await.context("sync with the node")?;

    let key = AuthSecretKey::new_falcon512_poseidon2();
    let auth = AuthSingleSig::new(Approver::new(
        key.public_key().to_commitment(),
        AuthSchemeId::Falcon512Poseidon2,
    ));
    let account = AccountBuilder::new(rand::random())
        .account_type(AccountType::Public)
        .with_component(auth)
        .with_component(BasicWallet)
        .build_with_schema_commitment()
        .context("build solver account")?;

    let keystore = FilesystemKeyStore::new(PathBuf::from(&config.solver.keystore_path))
        .context("open keystore")?;
    keystore.add_key(&key, account.id()).await.context("add solver key to keystore")?;
    client
        .add_account(&account, false)
        .await
        .context("add solver account to the executor store")?;

    let id = account.id();
    let address = match client.network_id().await {
        Ok(network) => id.to_bech32(network),
        Err(e) => format!("(unavailable: {e})"),
    };
    println!("solver account created");
    println!("  account_id = \"{}\"   # set this as [solver] account_id", id.to_hex());
    println!("  address    = {address}");
    println!("next: send native fee tokens to it as a PUBLIC note, then run `solver-bin fund-account`");
    Ok(())
}

/// Consume every note waiting for the solver account (the first such transaction
/// also deploys it), then wait until the fee-asset balance shows up.
pub async fn fund_account(config: &SolverConfig) -> Result<()> {
    let solver_id = AccountId::from_hex(&config.solver.account_id)
        .with_context(|| format!("invalid solver account_id {:?}", config.solver.account_id))?;
    let factory = ProdClientFactory::from_config(config);
    let mut client = factory.build_executor().await?;
    client.sync_state().await.context("sync with the node")?;

    let notes: Vec<Note> = client
        .get_consumable_notes(Some(solver_id))
        .await
        .context("list consumable notes")?
        .into_iter()
        .map(|(record, _)| TryInto::<Note>::try_into(record))
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow!("convert note record: {e}"))?;
    if notes.is_empty() {
        bail!(
            "no consumable notes for {} yet: send it native fee tokens as a PUBLIC note, \
             wait for the next block, and retry",
            solver_id.to_hex()
        );
    }

    let count = notes.len();
    let request = TransactionRequestBuilder::new()
        .build_consume_notes(notes)
        .context("build consume request")?;
    let tx_id = client
        .submit_new_transaction(solver_id, request)
        .await
        .context("submit consume transaction")?;
    println!("submitted {tx_id}: consuming {count} note(s) into {}", solver_id.to_hex());

    let (header, _) = factory
        .rpc()?
        .get_block_header_by_number(None, false)
        .await
        .context("fetch chain-tip header")?;
    let fee_faucet = header.fee_parameters().fee_faucet_id();
    for _ in 0..FUND_POLL_ATTEMPTS {
        tokio::time::sleep(FUND_POLL_INTERVAL).await;
        client.sync_state().await.context("sync with the node")?;
        let balance = client
            .account_reader(solver_id)
            .get_balance(fee_faucet)
            .await
            .context("read fee-asset balance")?;
        if balance.as_u64() > 0 {
            println!("fee-asset balance: {} (fee faucet {})", balance.as_u64(), fee_faucet.to_hex());
            return Ok(());
        }
    }
    bail!("transaction {tx_id} not reflected after polling; check the explorer and re-run `solver-bin fund-account`")
}

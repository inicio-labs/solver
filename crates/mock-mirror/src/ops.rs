//! Account operations for driving an end-to-end test from a user wallet:
//! claim minted funds, create a PSWAP order, read balances. Mirrors the proven
//! e2e helpers (`build_consume_notes` / `build_pswap_create`).

use anyhow::{anyhow, Result};
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::store::NoteFilter;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_client::Client;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;

type C = Client<FilesystemKeyStore>;

/// Sync, then consume committed **P2ID** input notes into `account` (claim mint
/// notes / trade proceeds into the spendable vault). Skips PSWAP notes — those
/// are swap orders, not plain payments. Returns the count consumed.
pub async fn claim(client: &mut C, account: AccountId) -> Result<usize> {
    client.sync_state().await.map_err(|e| anyhow!("sync_state: {e}"))?;
    let mut notes: Vec<Note> = Vec::new();
    for record in client
        .get_input_notes(NoteFilter::Committed)
        .await
        .map_err(|e| anyhow!("get_input_notes: {e}"))?
    {
        let note: Note = record.try_into().map_err(|_| anyhow!("input-note record -> Note"))?;
        if note.recipient().script().root() == PswapNote::script_root() {
            continue;
        }
        notes.push(note);
    }
    if notes.is_empty() {
        return Ok(0);
    }
    eprintln!("DIAG: {} candidate note(s) to consume", notes.len());
    let mut ok = 0usize;
    for note in notes {
        let nid = format!("{}", note.id());
        // Same resilient path as the mirror's auto-claim: retry transient
        // prover/RPC failures with backoff, skip deterministic ones.
        match crate::mirror::submit_with_backoff(client, account, "claim", || {
            TransactionRequestBuilder::new()
                .build_consume_notes(vec![note.clone()])
                .map_err(|e| anyhow!("build_consume: {e}"))
        })
        .await
        {
            Ok(()) => { ok += 1; eprintln!("CONSUMED {nid}"); }
            Err(e) => { eprintln!("FAILNOTE {nid}: {e}"); }
        }
    }
    Ok(ok)
}

/// Create a **public** PSWAP from `account` (so the solver's keyless ingest
/// discovers it): offer `offered`, request `requested`. `payback` controls the
/// note type of the asset returned to the maker (Public or Private).
pub async fn create_pswap(
    client: &mut C,
    account: AccountId,
    offered: FungibleAsset,
    requested: FungibleAsset,
    payback: NoteType,
) -> Result<()> {
    let data = PswapTransactionData::new(account, offered, requested);
    let request = TransactionRequestBuilder::new()
        .build_pswap_create(&data, NoteType::Public, payback, None, client.rng())
        .map_err(|e| anyhow!("build pswap-create: {e}"))?;
    client
        .submit_new_transaction(account, request)
        .await
        .map_err(|e| anyhow!("submit pswap-create: {e}"))?;
    Ok(())
}

/// Sync and print `account`'s vault balance for each faucet.
pub async fn balances(client: &mut C, account: AccountId, faucets: &[AccountId]) -> Result<()> {
    client.sync_state().await.map_err(|e| anyhow!("sync_state: {e}"))?;
    for f in faucets {
        let bal = client
            .account_reader(account)
            .get_balance(*f)
            .await
            .map_err(|e| anyhow!("get_balance: {e}"))?;
        println!("{}  balance {}", f.to_hex(), bal);
    }
    Ok(())
}

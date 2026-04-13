use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_client::{
    keystore::FilesystemKeyStore, store::NoteFilter, transaction::TransactionRequestBuilder,
    Client, Felt, Word,
};
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    crypto::utils::{Deserializable, SliceReader},
    note::{Note, NoteDetails},
};
use consume_script::ConsumeAssetScript;
use miden_standards::note::PswapNote;
use tokio::sync::mpsc;

use crate::types::{ExecutionBatch, FilledNote, TokenId, Amount};

/// Run the executor loop: listen for ExecutionBatches and submit Miden transactions.
pub async fn run_executor(
    client: &mut Client<FilesystemKeyStore>,
    solver_id: AccountId,
    mut exec_rx: mpsc::Receiver<ExecutionBatch>,
) {
    while let Some(batch) = exec_rx.recv().await {
        if batch.filled_notes.is_empty() {
            continue;
        }

        match execute_batch(client, solver_id, &batch).await {
            Ok(_) => println!("[executor] batch executed successfully ({} notes)", batch.filled_notes.len()),
            Err(e) => eprintln!("[executor] batch execution failed: {e}"),
        }
    }

    println!("[executor] channel closed, shutting down");
}

/// Execute a batch of filled notes in a single Miden transaction.
///
/// For each FilledNote:
///   1. Deserialize raw bytes → Note → PswapNote
///   2. Call pswap.execute(solver_id, None, Some(fill_asset)) to get output notes
///   3. Collect input notes, expected outputs, and per-token supply/demand
///
/// Surplus (supply - demand) goes into the solver's vault via ConsumeAssetScript.
async fn execute_batch(
    client: &mut Client<FilesystemKeyStore>,
    solver_id: AccountId,
    batch: &ExecutionBatch,
) -> Result<()> {
    let mut input_notes: Vec<(Note, Option<Word>)> = Vec::new();
    let mut expected_future_notes = Vec::new();
    let mut expected_output_recipients = Vec::new();

    // Track supply (what enters the tx) and demand (what leaves) per token
    let mut supply: HashMap<TokenId, u64> = HashMap::new();
    let mut demand: HashMap<TokenId, u64> = HashMap::new();

    for filled in &batch.filled_notes {
        // Deserialize raw note data back to Note
        let note = Note::read_from(&mut SliceReader::new(&filled.raw_note_data))
            .context("failed to deserialize note from raw data")?;

        // Parse as PswapNote
        let pswap = PswapNote::try_from(&note)
            .map_err(|e| anyhow::anyhow!("failed to parse PswapNote: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_token = offered_asset.faucet_id();
        let requested_token = pswap.storage().requested_faucet_id();

        // Supply: the offered asset enters the transaction
        *supply.entry(offered_token).or_default() += offered_asset.amount();

        // Build fill asset (inflight — from another note in the same tx)
        let fill_asset = FungibleAsset::new(requested_token, filled.requested_filled)
            .map_err(|e| anyhow::anyhow!("failed to create fill asset: {}", e))?;

        // Note args: [input_amount=0, 0, inflight_amount=fill, 0]
        let note_args = Word::from([
            Felt::ZERO,
            Felt::ZERO,
            Felt::new(filled.requested_filled),
            Felt::ZERO,
        ]);
        input_notes.push((note.clone(), Some(note_args)));

        // Execute the PSWAP note to get output notes
        let (p2id, remainder) = pswap
            .execute(solver_id, None, Some(fill_asset))
            .map_err(|e| anyhow::anyhow!("pswap execute failed: {}", e))?;

        // P2ID note sends the requested asset to the order creator → demand
        *demand.entry(requested_token).or_default() += note_asset_amount(&p2id);
        let p2id_tag = p2id.metadata().tag();
        expected_output_recipients.push(p2id.recipient().clone());
        expected_future_notes.push((NoteDetails::from(p2id), p2id_tag));

        // Remainder note (if partial fill) sends leftover offered asset back → demand
        if let Some(rem_pswap) = remainder {
            let rem_note = Note::from(rem_pswap);
            *demand.entry(offered_token).or_default() += note_asset_amount(&rem_note);
            let rem_tag = rem_note.metadata().tag();
            expected_output_recipients.push(rem_note.recipient().clone());
            expected_future_notes.push((NoteDetails::from(rem_note), rem_tag));
        }
    }

    // Verify input notes have full details in local store
    verify_note_details(client, &input_notes).await?;

    // Compute surplus per token (supply - demand)
    let mut surplus_assets: Vec<Asset> = Vec::new();
    let all_tokens: Vec<TokenId> = supply.keys().chain(demand.keys()).copied().collect();
    for token in all_tokens {
        let s = supply.get(&token).copied().unwrap_or(0);
        let d = demand.get(&token).copied().unwrap_or(0);
        let surplus = s.saturating_sub(d);
        if surplus > 0 {
            surplus_assets.push(Asset::Fungible(
                FungibleAsset::new(token, surplus)
                    .map_err(|e| anyhow::anyhow!("surplus asset: {}", e))?,
            ));
        }
    }

    // Prepare ConsumeAssetScript for surplus
    let consume_data = if !surplus_assets.is_empty() {
        Some(ConsumeAssetScript::prepare(&surplus_assets))
    } else {
        None
    };

    // Sync state to ensure note details are available
    client.sync_state().await?;

    // Build transaction
    let mut builder = TransactionRequestBuilder::new()
        .input_notes(input_notes)
        .expected_future_notes(expected_future_notes)
        .expected_output_recipients(expected_output_recipients);

    if let Some(data) = consume_data {
        builder = builder
            .custom_script(ConsumeAssetScript::tx_script())
            .script_arg(data.commitment_arg)
            .extend_advice_map([data.advice_map_entry]);
    }

    let request = builder
        .build()
        .context("failed to build transaction request")?;

    client
        .submit_new_transaction(solver_id, request)
        .await
        .map_err(|e| anyhow::anyhow!("failed to submit transaction: {}", e))?;

    Ok(())
}

/// Verify all input notes have metadata and inclusion proofs in the local store.
async fn verify_note_details(
    client: &Client<FilesystemKeyStore>,
    input_notes: &[(Note, Option<Word>)],
) -> Result<()> {
    let note_ids: Vec<_> = input_notes.iter().map(|(n, _)| n.id()).collect();

    let records = client
        .get_input_notes(NoteFilter::List(note_ids.clone()))
        .await
        .context("failed to query note records")?;

    for id in &note_ids {
        let record = records
            .iter()
            .find(|r| r.id() == *id)
            .ok_or_else(|| anyhow::anyhow!("note {} not found in local store", id))?;

        if record.metadata().is_none() {
            anyhow::bail!("note {} missing metadata — sync state first", id);
        }

        if !record.is_authenticated() {
            anyhow::bail!("note {} has no inclusion proof — not yet committed on-chain", id);
        }
    }

    Ok(())
}

/// Extract the amount of the first fungible asset from a note.
fn note_asset_amount(note: &Note) -> u64 {
    note.assets()
        .iter()
        .next()
        .and_then(|a| match a {
            Asset::Fungible(fa) => Some(fa.amount()),
            _ => None,
        })
        .unwrap_or(0)
}

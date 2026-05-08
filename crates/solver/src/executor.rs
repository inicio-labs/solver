use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use consume_script::ConsumeAssetScript;
use miden_client::{
    keystore::FilesystemKeyStore, transaction::TransactionRequestBuilder, Client,
};
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    crypto::utils::{Deserializable, SliceReader},
    note::{Note, NoteDetails},
};
use miden_standards::note::PswapNote;
use tokio::sync::mpsc;

use crate::types::{ExecutionBatch, TokenId};

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

async fn execute_batch(
    client: &mut Client<FilesystemKeyStore>,
    solver_id: AccountId,
    batch: &ExecutionBatch,
) -> Result<()> {
    let mut input_notes = Vec::new();
    let mut expected_future_notes = Vec::new();
    let mut expected_output_recipients = Vec::new();

    // Net flow per token: positive = surplus staying with solver, negative = insolvent
    let mut flow: HashMap<TokenId, i64> = HashMap::new();

    for filled in &batch.filled_notes {
        let note = Note::read_from(&mut SliceReader::new(&filled.raw_note_data))
            .context("failed to deserialize note from raw data")?;

        let pswap = PswapNote::try_from(&note)
            .map_err(|e| anyhow!("failed to parse PswapNote: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_token = offered_asset.faucet_id();
        let requested_token = pswap.storage().requested_faucet_id();

        *flow.entry(offered_token).or_default() += offered_asset.amount() as i64;

        let fill_asset = FungibleAsset::new(requested_token, filled.requested_filled)
            .map_err(|e| anyhow!("failed to create fill asset: {}", e))?;

        let note_args = PswapNote::create_args(0, filled.requested_filled)
            .map_err(|e| anyhow!("failed to create note args: {}", e))?;

        input_notes.push((note.clone(), Some(note_args)));

        let (p2id, remainder) = pswap
            .execute(solver_id, None, Some(fill_asset))
            .map_err(|e| anyhow!("pswap execute failed: {}", e))?;

        *flow.entry(requested_token).or_default() -= note_asset_amount(&p2id) as i64;
        let p2id_tag = p2id.metadata().tag();
        expected_output_recipients.push(p2id.recipient().clone());
        expected_future_notes.push((NoteDetails::from(p2id), p2id_tag));

        if let Some(rem_pswap) = remainder {
            let rem_note = Note::from(rem_pswap);
            *flow.entry(offered_token).or_default() -= note_asset_amount(&rem_note) as i64;
            let rem_tag = rem_note.metadata().tag();
            expected_output_recipients.push(rem_note.recipient().clone());
            expected_future_notes.push((NoteDetails::from(rem_note), rem_tag));
        }
    }

    // Negative flow means we owe more than we have — batch is insolvent
    let mut surplus_assets: Vec<Asset> = Vec::new();
    for (token, net) in &flow {
        if *net < 0 {
            bail!("insolvent batch: token {:?} has deficit of {}", token, net.abs());
        }
        if *net > 0 {
            surplus_assets.push(Asset::Fungible(
                FungibleAsset::new(*token, *net as u64)
                    .map_err(|e| anyhow!("surplus asset: {}", e))?,
            ));
        }
    }

    let consume_data = if !surplus_assets.is_empty() {
        Some(ConsumeAssetScript::prepare(&surplus_assets))
    } else {
        None
    };

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

    let request = builder.build().context("failed to build transaction request")?;

    client
        .submit_new_transaction(solver_id, request)
        .await
        .map_err(|e| anyhow!("failed to submit transaction: {}", e))?;

    Ok(())
}

fn note_asset_amount(note: &Note) -> u64 {
    note.assets().iter_fungible().next().map(|a| a.amount()).unwrap_or(0)
}

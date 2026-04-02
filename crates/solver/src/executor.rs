use std::time::Duration;

use anyhow::{Context, Result};
use miden_client::{
    keystore::FilesystemKeyStore, store::NoteFilter, transaction::TransactionRequestBuilder,
    Client, Felt, Word,
};
use miden_core::FieldElement;
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    note::{Note, NoteDetails},
};
use consume_script::ConsumeAssetScript;
use miden_standards::note::PswapNote;

use crate::order::Order;

pub struct MatchResult {
    pub total_x: u64,
    pub total_y: u64,
    pub surplus_x: u64,
    pub surplus_y: u64,
}

pub struct Executor;

impl Executor {
    /// Execute matched orders.
    ///
    /// Tracks supply/demand for each asset:
    ///   - supply: note assets entering the transaction
    ///   - demand: assets leaving via P2ID notes and remainder notes
    ///   - surplus = supply - demand → solver's profit
    ///
    /// Surplus is collected via `ConsumeAssetScript`: a custom tx-script that
    /// moves surplus assets directly into the solver's vault (no P2ID notes needed).
    ///
    /// Note arg layout: [input_amount=0, inflight_amount=fill, surplus_amount=0, tag=0]
    pub async fn execute_simple_match(
        client: &mut Client<FilesystemKeyStore>,
        solver_id: AccountId,
        asks: &[Order], // offer X, want Y
        bids: &[Order], // offer Y, want X
    ) -> Result<MatchResult> {
        println!("Testing:: Executor::execute_simple_match");
        println!(
            "Testing:: asks={:?}",
            asks.iter().map(|a| a.offered_amount).collect::<Vec<_>>()
        );
        println!(
            "Testing:: asks={:?}",
            asks.iter().map(|a| a.requested_amount).collect::<Vec<_>>()
        );
        println!(
            "Testing:: asks={:?}",
            asks.iter().map(|a| a.fill_amount).collect::<Vec<_>>()
        );

        println!(
            "Testing:: bids={:?}",
            bids.iter().map(|b| b.offered_amount).collect::<Vec<_>>()
        );
        println!(
            "Testing:: bids={:?}",
            bids.iter().map(|b| b.requested_amount).collect::<Vec<_>>()
        );
        println!(
            "Testing:: bids={:?}",
            bids.iter().map(|b| b.fill_amount).collect::<Vec<_>>()
        );

        println!("Testing:: bids={:?}", bids);

        let x_faucet = asks[0].offered_faucet_id;
        let y_faucet = bids[0].offered_faucet_id;

        // ── Verify all input notes have full details in the local store ──
        // Public notes received via tag tracking must have metadata and an inclusion
        // proof before the prover can add their details to the advice map.
        {
            let note_ids: Vec<_> = asks
                .iter()
                .chain(bids.iter())
                .map(|o| o.note.id())
                .collect();

            let records = client
                .get_input_notes(NoteFilter::List(note_ids.clone()))
                .await
                .context("Failed to query note records for detail check")?;

            for id in &note_ids {
                let record = records.iter().find(|r| r.id() == *id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Note {} not found in local store — call sync_state() before executing",
                        id
                    )
                })?;

                if record.metadata().is_none() {
                    anyhow::bail!(
                        "Note {} is missing metadata — public note details not yet available (call sync_state() again)",
                        id
                    );
                }

                if !record.is_authenticated() {
                    anyhow::bail!(
                        "Note {} has no inclusion proof — not yet committed on-chain",
                        id
                    );
                }
            }
        }

        let mut total_supply_x: u64 = 0;
        let mut total_supply_y: u64 = 0;
        let mut total_demand_x: u64 = 0;
        let mut total_demand_y: u64 = 0;

        let mut expected_future_notes = Vec::new();
        let mut expected_output_recipients = Vec::new();
        let mut final_input_notes = Vec::new();

        // ── Process asks (offer X, want Y) ──
        for order in asks.iter() {
            total_supply_x += order.offered_amount;
            println!(
                "[ask] note={} offered={} requested={} fill={}",
                order.note.id(),
                order.offered_amount,
                order.requested_amount,
                order.fill_amount,
            );
            final_input_notes.push((
                order.note.clone(),
                Some(Word::from([
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::new(order.fill_amount),
                    Felt::ZERO,
                ])),
            ));

            let pswap = PswapNote::try_from(&order.note)
                .map_err(|e| anyhow::anyhow!("ask parse: {}", e))?;
            let fill_asset = FungibleAsset::new(
                pswap.storage().requested_faucet_id(),
                order.fill_amount,
            ).map_err(|e| anyhow::anyhow!("ask fill asset: {}", e))?;
            let (p2id, remainder_pswap) = pswap.execute(solver_id, None, Some(fill_asset))
                .map_err(|e| anyhow::anyhow!("ask output: {}", e))?;
            let remainder = remainder_pswap.map(Note::from);

            // P2ID carries Y (requested asset) → demand_y
            total_demand_y += note_asset_amount(&p2id);
            let p2id_tag = p2id.metadata().tag();
            println!(
                "  [ask] p2id recipient={:?} tag={:?} asset={}",
                p2id.recipient().digest(),
                p2id_tag,
                note_asset_amount(&p2id),
            );
            expected_output_recipients.push(p2id.recipient().clone());
            expected_future_notes.push((NoteDetails::from(p2id), p2id_tag));

            // Remainder carries leftover X (offered asset) → demand_x
            if let Some(rem) = remainder {
                total_demand_x += note_asset_amount(&rem);
                let rem_tag = rem.metadata().tag();
                println!(
                    "  [ask] remainder recipient={:?} tag={:?} asset={}",
                    rem.recipient().digest(),
                    rem_tag,
                    note_asset_amount(&rem),
                );
                expected_output_recipients.push(rem.recipient().clone());
                expected_future_notes.push((NoteDetails::from(rem), rem_tag));
            } else {
                println!("  [ask] no remainder (fully filled)");
            }
        }

        // ── Process bids (offer Y, want X) ──
        for order in bids.iter() {
            total_supply_y += order.offered_amount;
            println!(
                "[bid] note={} offered={} requested={} fill={}",
                order.note.id(),
                order.offered_amount,
                order.requested_amount,
                order.fill_amount,
            );
            final_input_notes.push((
                order.note.clone(),
                Some(Word::from([
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::new(order.fill_amount),
                    Felt::ZERO,
                ])),
            ));

            let pswap = PswapNote::try_from(&order.note)
                .map_err(|e| anyhow::anyhow!("bid parse: {}", e))?;
            let fill_asset = FungibleAsset::new(
                pswap.storage().requested_faucet_id(),
                order.fill_amount,
            ).map_err(|e| anyhow::anyhow!("bid fill asset: {}", e))?;
            let (p2id, remainder_pswap) = pswap.execute(solver_id, None, Some(fill_asset))
                .map_err(|e| anyhow::anyhow!("bid output: {}", e))?;
            let remainder = remainder_pswap.map(Note::from);

            // P2ID carries X (requested asset) → demand_x
            total_demand_x += note_asset_amount(&p2id);
            let p2id_tag = p2id.metadata().tag();
            println!(
                "  [bid] p2id recipient={:?} tag={:?} asset={}",
                p2id.recipient().digest(),
                p2id_tag,
                note_asset_amount(&p2id),
            );
            expected_output_recipients.push(p2id.recipient().clone());
            expected_future_notes.push((NoteDetails::from(p2id), p2id_tag));

            // Remainder carries leftover Y (offered asset) → demand_y
            if let Some(rem) = remainder {
                total_demand_y += note_asset_amount(&rem);
                let rem_tag = rem.metadata().tag();
                println!(
                    "  [bid] remainder recipient={:?} tag={:?} asset={}",
                    rem.recipient().digest(),
                    rem_tag,
                    note_asset_amount(&rem),
                );
                expected_output_recipients.push(rem.recipient().clone());
                expected_future_notes.push((NoteDetails::from(rem), rem_tag));
            } else {
                println!("  [bid] no remainder (fully filled)");
            }
        }

        println!(
            "total_supply_x={}, total_demand_x={}",
            total_supply_x, total_demand_x
        );
        println!(
            "total_supply_y={}, total_demand_y={}",
            total_supply_y, total_demand_y
        );

        // ── Compute surplus ──
        let surplus_x = total_supply_x.saturating_sub(total_demand_x);
        let surplus_y = total_supply_y.saturating_sub(total_demand_y);

        println!("Surplus: x={}, y={}", surplus_x, surplus_y);

        // ── Prepare ConsumeAssetScript for solver's surplus ──
        // Surplus goes directly into the solver's vault — no output P2ID notes.
        let mut surplus_assets: Vec<Asset> = Vec::new();
        if surplus_x > 0 {
            surplus_assets.push(Asset::Fungible(
                FungibleAsset::new(x_faucet, surplus_x)
                    .map_err(|e| anyhow::anyhow!("surplus X asset: {}", e))?,
            ));
        }
        if surplus_y > 0 {
            surplus_assets.push(Asset::Fungible(
                FungibleAsset::new(y_faucet, surplus_y)
                    .map_err(|e| anyhow::anyhow!("surplus Y asset: {}", e))?,
            ));
        }

        let consume_data = if !surplus_assets.is_empty() {
            Some(ConsumeAssetScript::prepare(&surplus_assets))
        } else {
            None
        };

        // ── Sync state to ensure note details are available ──
        client.sync_state().await?;

        // ── Build and submit transaction ──
        let mut builder = TransactionRequestBuilder::new()
            .input_notes(final_input_notes.clone())
            .expected_future_notes(expected_future_notes.clone())
            .expected_output_recipients(expected_output_recipients.clone());

        if let Some(data) = consume_data {
            builder = builder
                .custom_script(ConsumeAssetScript::tx_script())
                .script_arg(data.commitment_arg)
                .extend_advice_map([data.advice_map_entry]);
        }

        println!("Print all the notes that are being sent to the transaction");
        for note in final_input_notes {
            println!("note={:?}", note.0.assets().iter().next().unwrap());
        }

        println!("Print all the notes that are expected to be output");
        for note in expected_future_notes {
            println!("note={:?}", note.0.assets().iter().next().unwrap());
        }

        println!("Print all the recipients that are expected to be output");
        for recipient in expected_output_recipients {
            println!("recipient={:?}", recipient.digest());
            println!("recipient={:?}", recipient.digest().to_hex());
        }

        tokio::time::sleep(Duration::from_secs(10)).await;

        let request = builder
            .build()
            .context("Failed to build transaction request")?;

        let tx_id = client
            .submit_new_transaction(solver_id, request)
            .await
            .map_err(|e| {
                eprintln!("submit_new_transaction failed: {:?}", e);
                anyhow::anyhow!("Failed to submit match transaction error: {}", e)
            })
            .context("Failed to submit match transaction")?;

        tokio::time::sleep(Duration::from_secs(100)).await;

        println!(
            "Match executed: {} asks + {} bids, surplus_x={}, surplus_y={}, tx={:?}",
            asks.len(),
            bids.len(),
            surplus_x,
            surplus_y,
            tx_id,
        );

        Ok(MatchResult {
            total_x: total_supply_x,
            total_y: total_supply_y,
            surplus_x,
            surplus_y,
        })
    }
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

use anyhow::{Context, Result};
use miden_client::{
    keystore::FilesystemKeyStore,
    transaction::TransactionRequestBuilder,
    Client, Felt, Word,
};
use miden_core::FieldElement;
use miden_protocol::{
    account::AccountId,
    asset::{Asset, FungibleAsset},
    note::{
        NoteAssets, NoteAttachment, NoteAttachmentScheme, NoteDetails, NoteMetadata, NoteTag,
        NoteType, Note,
    },
};
use miden_standards::note::utils::build_p2id_recipient;
use miden_swapp::PswapNote;

use crate::matcher::MatchGroup;

pub struct Executor;

impl Executor {
    /// Execute a match group by consuming all notes in a single transaction.
    ///
    /// The solver consumes all notes simultaneously:
    /// - Side A notes release X, which flows to Side B
    /// - Side B notes release Y, which flows to Side A
    /// - Any surplus (spread) goes to the solver via P2ID notes
    ///
    /// Waterfall surplus distribution ensures each note's args are correct:
    /// surplus is assigned to the first notes that have excess output beyond demand.
    pub async fn execute_match_group(
        client: &mut Client<FilesystemKeyStore>,
        solver_id: AccountId,
        group: &MatchGroup,
    ) -> Result<()> {
        let solver_p2id_tag_felt = if group.surplus_x > 0 || group.surplus_y > 0 {
            let tag = NoteTag::with_account_target(solver_id);
            Felt::new(u32::from(tag) as u64)
        } else {
            Felt::ZERO
        };

        let mut input_notes = Vec::new();
        let mut expected_future_notes = Vec::new();
        let mut expected_output_recipients = Vec::new();

        // --- Process Side A (releasing X) ---
        // Waterfall: distribute demand_x across side_a output_amounts, remainder is surplus per note
        let mut remaining_x_demand = group.demand_x;
        for filled in &group.side_a {
            let contribution = std::cmp::min(filled.output_amount, remaining_x_demand);
            let surplus = filled.output_amount - contribution;
            remaining_x_demand -= contribution;

            let note = &filled.order.note;

            // Note args: [consumer_tag, surplus, inflight (fill_amount = Y received), input]
            let args = Word::from([
                if surplus > 0 { solver_p2id_tag_felt } else { Felt::ZERO },
                Felt::new(surplus),
                Felt::new(filled.fill_amount),
                Felt::ZERO,
            ]);
            input_notes.push((note.clone(), Some(args)));

            // P2ID for creator
            let (p2id, remainder) = PswapNote::create_output_notes(
                note,
                solver_id,
                0,
                filled.fill_amount,
            )
            .map_err(|e| anyhow::anyhow!("Failed to create output notes for side A: {}", e))?;

            let p2id_tag = p2id.metadata().tag();
            expected_future_notes.push((NoteDetails::from(&p2id), p2id_tag));

            let inputs = note.recipient().inputs();
            let (_, creator, _, _) = PswapNote::parse_inputs(inputs.values())
                .map_err(|e| anyhow::anyhow!("Failed to parse note inputs: {}", e))?;
            let serial = note.recipient().serial_num();
            let p2id_serial = Word::from([
                serial[0] + Felt::new(1),
                serial[1] + Felt::new(1),
                serial[2] + Felt::new(1),
                serial[3] + Felt::new(1),
            ]);
            let recipient = build_p2id_recipient(creator, p2id_serial)
                .map_err(|e| anyhow::anyhow!("Failed to build P2ID recipient: {}", e))?;
            expected_output_recipients.push(recipient);

            // Spread note for solver
            if surplus > 0 {
                let (spread_note, spread_recipient) = Self::build_solver_spread_note(
                    note,
                    solver_id,
                    filled.order.offered_faucet_id,
                    surplus,
                )?;
                let spread_tag = spread_note.metadata().tag();
                expected_future_notes.push((NoteDetails::from(&spread_note), spread_tag));
                expected_output_recipients.push(spread_recipient);
            }

            // Remainder note if partial fill
            if let Some(ref rem) = remainder {
                let rem_tag = rem.metadata().tag();
                expected_future_notes.push((NoteDetails::from(rem), rem_tag));
            }
        }

        // --- Process Side B (releasing Y) ---
        // Waterfall: distribute demand_y across side_b output_amounts, remainder is surplus per note
        let mut remaining_y_demand = group.demand_y;
        for filled in &group.side_b {
            let contribution = std::cmp::min(filled.output_amount, remaining_y_demand);
            let surplus = filled.output_amount - contribution;
            remaining_y_demand -= contribution;

            let note = &filled.order.note;

            // Note args: [consumer_tag, surplus, inflight (fill_amount = X received), input]
            let args = Word::from([
                if surplus > 0 { solver_p2id_tag_felt } else { Felt::ZERO },
                Felt::new(surplus),
                Felt::new(filled.fill_amount),
                Felt::ZERO,
            ]);
            input_notes.push((note.clone(), Some(args)));

            // P2ID for creator
            let (p2id, remainder) = PswapNote::create_output_notes(
                note,
                solver_id,
                0,
                filled.fill_amount,
            )
            .map_err(|e| anyhow::anyhow!("Failed to create output notes for side B: {}", e))?;

            let p2id_tag = p2id.metadata().tag();
            expected_future_notes.push((NoteDetails::from(&p2id), p2id_tag));

            let inputs = note.recipient().inputs();
            let (_, creator, _, _) = PswapNote::parse_inputs(inputs.values())
                .map_err(|e| anyhow::anyhow!("Failed to parse note inputs: {}", e))?;
            let serial = note.recipient().serial_num();
            let p2id_serial = Word::from([
                serial[0] + Felt::new(1),
                serial[1] + Felt::new(1),
                serial[2] + Felt::new(1),
                serial[3] + Felt::new(1),
            ]);
            let recipient = build_p2id_recipient(creator, p2id_serial)
                .map_err(|e| anyhow::anyhow!("Failed to build P2ID recipient: {}", e))?;
            expected_output_recipients.push(recipient);

            // Spread note for solver
            if surplus > 0 {
                let (spread_note, spread_recipient) = Self::build_solver_spread_note(
                    note,
                    solver_id,
                    filled.order.offered_faucet_id,
                    surplus,
                )?;
                let spread_tag = spread_note.metadata().tag();
                expected_future_notes.push((NoteDetails::from(&spread_note), spread_tag));
                expected_output_recipients.push(spread_recipient);
            }

            // Remainder note if partial fill
            if let Some(ref rem) = remainder {
                let rem_tag = rem.metadata().tag();
                expected_future_notes.push((NoteDetails::from(rem), rem_tag));
            }
        }

        // Build the transaction
        let request = TransactionRequestBuilder::new()
            .input_notes(input_notes)
            .expected_future_notes(expected_future_notes)
            .expected_output_recipients(expected_output_recipients)
            .build()
            .context("Failed to build transaction request")?;

        // Submit
        let tx_id = client
            .submit_new_transaction(solver_id, request)
            .await
            .context("Failed to submit match group transaction")?;

        let n_notes = group.side_a.len() + group.side_b.len();
        println!(
            "Match group executed: {} notes ({}A + {}B), total_x={}, total_y={}, surplus_x={}, surplus_y={}, tx={:?}",
            n_notes, group.side_a.len(), group.side_b.len(),
            group.total_x, group.total_y, group.surplus_x, group.surplus_y, tx_id,
        );

        Ok(())
    }

    /// Build a P2ID spread note for the solver from a swap note's surplus.
    fn build_solver_spread_note(
        _source_note: &Note,
        solver_id: AccountId,
        surplus_faucet_id: AccountId,
        surplus_amount: u64,
    ) -> Result<(Note, miden_protocol::note::NoteRecipient)> {
        // Generate a random serial number for the solver's spread P2ID note
        let mut rng = rand::rng();
        let solver_p2id_serial = Word::from([
            Felt::new(rand::Rng::random::<u64>(&mut rng)),
            Felt::new(rand::Rng::random::<u64>(&mut rng)),
            Felt::new(rand::Rng::random::<u64>(&mut rng)),
            Felt::new(rand::Rng::random::<u64>(&mut rng)),
        ]);

        let solver_p2id_recipient = build_p2id_recipient(solver_id, solver_p2id_serial)
            .map_err(|e| anyhow::anyhow!("Failed to build solver P2ID recipient: {}", e))?;

        let solver_p2id_tag = NoteTag::with_account_target(solver_id);
        let surplus_asset = Asset::Fungible(
            FungibleAsset::new(surplus_faucet_id, surplus_amount)
                .map_err(|e| anyhow::anyhow!("Failed to create surplus asset: {}", e))?,
        );
        let spread_note_assets = NoteAssets::new(vec![surplus_asset])
            .map_err(|e| anyhow::anyhow!("Failed to create spread note assets: {}", e))?;

        let aux_word = Word::from([Felt::new(surplus_amount), Felt::ZERO, Felt::ZERO, Felt::ZERO]);
        let attachment = NoteAttachment::new_word(NoteAttachmentScheme::none(), aux_word);

        let spread_metadata = NoteMetadata::new(solver_id, NoteType::Public, solver_p2id_tag)
            .with_attachment(attachment);

        let spread_note = Note::new(
            spread_note_assets,
            spread_metadata,
            solver_p2id_recipient.clone(),
        );

        Ok((spread_note, solver_p2id_recipient))
    }
}

use std::collections::BTreeMap;

use consume_script::{ConsumeAssetData, ConsumeAssetScript};
use miden_protocol::account::auth::AuthScheme;
use miden_protocol::asset::{Asset, FungibleAsset};
use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
use miden_protocol::note::NoteType;
use miden_protocol::transaction::RawOutputNote;
use miden_protocol::note::Note;
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};
use miden_testing::{Auth, MockChain};

const BASIC_AUTH: Auth = Auth::BasicAuth {
    auth_scheme: AuthScheme::Falcon512Poseidon2,
};

/// Test: Solver consumes two cross-matching PSWAP notes and receives surplus via
/// the consume-asset-script.
///
/// Setup:
///   - Alice offers 100 USDC, requests 10 ETH (rate: 10 USDC/ETH)
///   - Bob offers 10 ETH, requests 80 USDC (rate: 8 USDC/ETH)
///   - Surplus: 100 - 80 = 20 USDC goes to solver
///
/// The solver (Charlie) consumes both notes in one transaction:
///   - Alice's PSWAP note produces: P2ID(10 ETH → Alice)
///   - Bob's PSWAP note produces: P2ID(80 USDC → Bob)
///   - Consume-asset-script receives 20 USDC surplus into solver's vault
#[tokio::test]
async fn consume_asset_script_captures_surplus() -> anyhow::Result<()> {
    let mut builder = MockChain::builder();

    let usdc_faucet = builder.add_existing_basic_faucet(BASIC_AUTH, "USDC", 10_000, Some(200))?;
    let eth_faucet = builder.add_existing_basic_faucet(BASIC_AUTH, "ETH", 10_000, Some(100))?;

    // Alice: has 100 USDC, will offer them
    let alice = builder.add_existing_wallet_with_assets(
        BASIC_AUTH,
        [FungibleAsset::new(usdc_faucet.id(), 100)?.into()],
    )?;
    // Bob: has 10 ETH, will offer them
    let bob = builder.add_existing_wallet_with_assets(
        BASIC_AUTH,
        [FungibleAsset::new(eth_faucet.id(), 10)?.into()],
    )?;
    // Solver (Charlie): empty wallet, will receive surplus
    let solver = builder.add_existing_wallet_with_assets(BASIC_AUTH, [])?;

    let mut rng = RandomCoin::new(Word::default());

    // Alice's PSWAP note: offers 100 USDC, requests 10 ETH
    let alice_storage = PswapNoteStorage::builder()
        .requested_asset(FungibleAsset::new(eth_faucet.id(), 10)?)
        .creator_account_id(alice.id())
        .build();
    let alice_note: Note = PswapNote::builder()
        .sender(alice.id())
        .storage(alice_storage)
        .serial_number(rng.draw_word())
        .note_type(NoteType::Public)
        .offered_asset(FungibleAsset::new(usdc_faucet.id(), 100)?)
        .build()?
        .into();
    builder.add_output_note(RawOutputNote::Full(alice_note.clone()));

    // Bob's PSWAP note: offers 10 ETH, requests 80 USDC
    let bob_storage = PswapNoteStorage::builder()
        .requested_asset(FungibleAsset::new(usdc_faucet.id(), 80)?)
        .creator_account_id(bob.id())
        .build();
    let bob_note: Note = PswapNote::builder()
        .sender(bob.id())
        .storage(bob_storage)
        .serial_number(rng.draw_word())
        .note_type(NoteType::Public)
        .offered_asset(FungibleAsset::new(eth_faucet.id(), 10)?)
        .build()?
        .into();
    builder.add_output_note(RawOutputNote::Full(bob_note.clone()));

    let mock_chain = builder.build()?;

    // Note args: cross-swap via inflight amounts
    // Alice's note: account_fill=0, note_fill=10 ETH (from Bob's note)
    // Bob's note: account_fill=0, note_fill=80 USDC (from Alice's note)
    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(
        alice_note.id(),
        PswapNote::create_args(0, 10)?,
    );
    note_args_map.insert(
        bob_note.id(),
        PswapNote::create_args(0, 80)?,
    );

    // Compute expected output notes
    let alice_pswap = PswapNote::try_from(&alice_note)?;
    let (alice_p2id, _) = alice_pswap.execute(
        solver.id(),
        None,
        Some(FungibleAsset::new(eth_faucet.id(), 10)?),
    )?;

    let bob_pswap = PswapNote::try_from(&bob_note)?;
    let (bob_p2id, _) = bob_pswap.execute(
        solver.id(),
        None,
        Some(FungibleAsset::new(usdc_faucet.id(), 80)?),
    )?;

    // Surplus: 100 USDC offered by Alice - 80 USDC sent to Bob = 20 USDC for solver
    let surplus_asset = Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 20)?);
    let consume_data: ConsumeAssetData = ConsumeAssetScript::prepare(&[surplus_asset]);
    let tx_script = ConsumeAssetScript::tx_script();

    let tx_context = mock_chain
        .build_tx_context(solver.id(), &[alice_note.id(), bob_note.id()], &[])?
        .tx_script(tx_script)
        .tx_script_args(consume_data.commitment_arg)
        .extend_advice_map([consume_data.advice_map_entry])
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(vec![
            RawOutputNote::Full(alice_p2id),
            RawOutputNote::Full(bob_p2id),
        ])
        .build()?;

    let executed_transaction = tx_context.execute().await?;

    // Verify output: 2 P2ID notes
    let output_notes = executed_transaction.output_notes();
    assert_eq!(output_notes.num_notes(), 2, "Expected 2 P2ID output notes");

    // Verify solver's vault delta: should receive 20 USDC surplus
    let vault_delta = executed_transaction.account_delta().vault();
    let added: Vec<Asset> = vault_delta.added_assets().collect();

    let usdc_added: u64 = added
        .iter()
        .filter_map(|a| match a {
            Asset::Fungible(f) if f.faucet_id() == usdc_faucet.id() => Some(u64::from(f.amount())),
            _ => None,
        })
        .sum();
    assert_eq!(usdc_added, 20, "Solver should receive 20 USDC surplus");

    Ok(())
}

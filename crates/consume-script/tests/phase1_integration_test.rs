use std::collections::BTreeMap;

use consume_script::ConsumeAssetScript;
use matching_engine::engine::MatchingEngine;
use matching_engine::order_book::OrderBook;
use matching_engine::price_feed::SimpleMapFeed;
use matching_engine::types::*;
use miden_protocol::account::auth::AuthScheme;
use miden_protocol::asset::{Asset, FungibleAsset};
use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
use miden_protocol::note::{Note, NoteAssets, NoteType};
use miden_protocol::transaction::RawOutputNote;
use miden_protocol::{Felt, Word, ZERO};
use miden_standards::note::{PswapNote, PswapNoteStorage};
use miden_testing::{Auth, MockChain};

const BASIC_AUTH: Auth = Auth::BasicAuth {
    auth_scheme: AuthScheme::Falcon512Poseidon2,
};

const USDC_TOKEN: TokenId = 0;
const ETH_TOKEN: TokenId = 1;


fn create_pswap_note(
    sender_id: miden_protocol::account::AccountId,
    offered_asset: Asset,
    requested_asset: Asset,
    rng: &mut impl FeltRng,
) -> Note {
    let storage = PswapNoteStorage::builder()
        .requested_asset_key(requested_asset.to_key_word())
        .requested_asset_value(requested_asset.to_value_word())
        .creator_account_id(sender_id)
        .build();
    PswapNote::builder()
        .sender(sender_id)
        .storage(storage)
        .serial_number(rng.draw_word())
        .note_type(NoteType::Public)
        .assets(NoteAssets::new(vec![offered_asset]).unwrap())
        .build()
        .unwrap()
        .into()
}

/// Full pipeline: multiple PSWAP notes → matching engine → cross-swap execution with surplus.
///
/// The PSWAP note script only puts `input_offered_out` into the consumer vault (not inflight).
/// Surplus is captured via the consume-asset-script tx script.
///
/// Setup:
///   Alice1: offers 2000 USDC, requests 1 ETH
///   Alice2: offers 4000 USDC, requests 2 ETH
///   Bob1:   offers 1 ETH, requests 1600 USDC
///   Bob2:   offers 2 ETH, requests 3200 USDC
///
///   Cross-swap via inflight: all notes fully filled
///   Surplus: 6000 USDC in - 4800 USDC out = 1200 USDC for solver
#[tokio::test]
async fn phase1_match_and_execute_multiple_orders() -> anyhow::Result<()> {
    let mut builder = MockChain::builder();

    let usdc_faucet = builder.add_existing_basic_faucet(BASIC_AUTH, "USDC", 100_000, Some(20_000))?;
    let eth_faucet = builder.add_existing_basic_faucet(BASIC_AUTH, "ETH", 100_000, Some(100))?;

    let alice = builder.add_existing_wallet_with_assets(
        BASIC_AUTH,
        [FungibleAsset::new(usdc_faucet.id(), 6000)?.into()],
    )?;
    let bob = builder.add_existing_wallet_with_assets(
        BASIC_AUTH,
        [FungibleAsset::new(eth_faucet.id(), 3)?.into()],
    )?;
    let solver = builder.add_existing_wallet_with_assets(BASIC_AUTH, [])?;

    let mut rng = RandomCoin::new(Word::default());

    let alice_note_1 = create_pswap_note(
        alice.id(),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 2000)?),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 1)?),
        &mut rng,
    );
    builder.add_output_note(RawOutputNote::Full(alice_note_1.clone()));

    let alice_note_2 = create_pswap_note(
        alice.id(),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 4000)?),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 2)?),
        &mut rng,
    );
    builder.add_output_note(RawOutputNote::Full(alice_note_2.clone()));

    let bob_note_1 = create_pswap_note(
        bob.id(),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 1)?),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 1600)?),
        &mut rng,
    );
    builder.add_output_note(RawOutputNote::Full(bob_note_1.clone()));

    let bob_note_2 = create_pswap_note(
        bob.id(),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 2)?),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 3200)?),
        &mut rng,
    );
    builder.add_output_note(RawOutputNote::Full(bob_note_2.clone()));

    let mock_chain = builder.build()?;

    // ── Run matching engine ──
    let mut feed = SimpleMapFeed::new();
    feed.set_price(USDC_TOKEN, 1);
    feed.set_price(ETH_TOKEN, 2000);

    let mut book = OrderBook::new(feed, Config::default());

    // Alice1: sell 2000 USDC, buy 1 ETH
    book.add_user_order(USDC_TOKEN, ETH_TOKEN, 2000, 1)
        .expect("alice1 should be accepted");

    // Alice2: sell 4000 USDC, buy 2 ETH
    book.add_user_order(USDC_TOKEN, ETH_TOKEN, 4000, 2)
        .expect("alice2 should be accepted");

    // Bob1: sell 1 ETH, buy 1600 USDC
    book.add_user_order(ETH_TOKEN, USDC_TOKEN, 1, 1600)
        .expect("bob1 should be accepted");

    // Bob2: sell 2 ETH, buy 3200 USDC
    book.add_user_order(ETH_TOKEN, USDC_TOKEN, 2, 3200)
        .expect("bob2 should be accepted");

    let mut engine = MatchingEngine::new(book);
    let batch = engine.run();

    assert!(batch.cycles_executed >= 2, "should find at least 2 cycles, got {}", batch.cycles_executed);
    assert_eq!(batch.remaining_orders, 0, "all orders should be matched");

    // ── Map fills to PSWAP note args ──
    // All fills via inflight (solver has no initial assets)
    // PSWAP note script: inflight assets flow between notes, input=0 means vault receives nothing
    // Surplus must be captured via consume-asset-script
    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(alice_note_1.id(), Word::from([Felt::new(0), Felt::new(1), ZERO, ZERO]));
    note_args_map.insert(alice_note_2.id(), Word::from([Felt::new(0), Felt::new(2), ZERO, ZERO]));
    note_args_map.insert(bob_note_1.id(), Word::from([Felt::new(0), Felt::new(1600), ZERO, ZERO]));
    note_args_map.insert(bob_note_2.id(), Word::from([Felt::new(0), Felt::new(3200), ZERO, ZERO]));

    // Compute expected output notes
    let alice_pswap_1 = PswapNote::try_from(&alice_note_1)?;
    let (alice_p2id_1, _) = alice_pswap_1.execute(solver.id(), 0, 1)?;

    let alice_pswap_2 = PswapNote::try_from(&alice_note_2)?;
    let (alice_p2id_2, _) = alice_pswap_2.execute(solver.id(), 0, 2)?;

    let bob_pswap_1 = PswapNote::try_from(&bob_note_1)?;
    let (bob_p2id_1, _) = bob_pswap_1.execute(solver.id(), 0, 1600)?;

    let bob_pswap_2 = PswapNote::try_from(&bob_note_2)?;
    let (bob_p2id_2, _) = bob_pswap_2.execute(solver.id(), 0, 3200)?;

    // ── Prepare consume-asset-script for surplus ──
    // Surplus: 6000 USDC in - 4800 USDC out = 1200 USDC
    let surplus_asset = Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 1200)?);
    let consume_data = ConsumeAssetScript::prepare(&[surplus_asset]);
    let tx_script = ConsumeAssetScript::tx_script();

    // ── Execute transaction ──
    let input_note_ids = [
        alice_note_1.id(),
        alice_note_2.id(),
        bob_note_1.id(),
        bob_note_2.id(),
    ];

    let tx_context = mock_chain
        .build_tx_context(solver.id(), &input_note_ids, &[])?
        .tx_script(tx_script)
        .tx_script_args(consume_data.commitment_arg)
        .extend_advice_map([consume_data.advice_map_entry])
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(vec![
            RawOutputNote::Full(alice_p2id_1),
            RawOutputNote::Full(alice_p2id_2),
            RawOutputNote::Full(bob_p2id_1),
            RawOutputNote::Full(bob_p2id_2),
        ])
        .build()?;

    let executed_transaction = tx_context.execute().await?;

    // ── Verify ──
    let output_notes = executed_transaction.output_notes();
    assert_eq!(output_notes.num_notes(), 4, "expected 4 P2ID output notes");

    // Solver vault: +1200 USDC surplus (from consume-asset-script)
    let vault_delta = executed_transaction.account_delta().vault();
    let added: Vec<Asset> = vault_delta.added_assets().collect();

    let usdc_surplus: u64 = added
        .iter()
        .filter_map(|a| match a {
            Asset::Fungible(f) if f.faucet_id() == usdc_faucet.id() => Some(f.amount()),
            _ => None,
        })
        .sum();
    assert_eq!(usdc_surplus, 1200, "solver should receive 1200 USDC surplus");

    let eth_surplus: u64 = added
        .iter()
        .filter_map(|a| match a {
            Asset::Fungible(f) if f.faucet_id() == eth_faucet.id() => Some(f.amount()),
            _ => None,
        })
        .sum();
    assert_eq!(eth_surplus, 0, "no ETH surplus expected");

    Ok(())
}

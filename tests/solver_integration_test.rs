use std::collections::BTreeMap;

use miden_client::{
    account::component::BasicWallet as StdBasicWallet,
    note::{Note, NoteAssets, NoteMetadata, NoteTag, NoteType},
    transaction::OutputNote,
    Felt, Word,
};
use miden_core::FieldElement;
use miden_crypto::rand::RpoRandomCoin;
use miden_protocol::{
    account::{AccountBuilder, AccountId, AccountStorageMode, AccountType},
    asset::{Asset, FungibleAsset},
    note::{NoteAttachment, NoteAttachmentScheme},
    ZERO,
};
use miden_standards::note::utils::build_p2id_recipient;
use miden_swapp::{BasicWallet, PswapNote};
use miden_testing::{Auth, MockChain};

use solver::matcher::Matcher;
use solver::order::Order;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn compute_p2id_tag(account_id: AccountId) -> NoteTag {
    NoteTag::with_account_target(account_id)
}

fn compute_p2id_tag_felt(account_id: AccountId) -> Felt {
    Felt::new(u32::from(compute_p2id_tag(account_id)) as u64)
}

// ---------------------------------------------------------------------------
// Test 1: Note parsing + matching (no MockChain execution)
// ---------------------------------------------------------------------------

/// Verifies that the solver can:
/// 1. Parse pswap notes into Orders
/// 2. Run the Matcher to find valid cross-swap matches
#[test]
fn test_solver_note_parsing_and_matching() {
    let mut rng = RpoRandomCoin::new(Word::default());

    // Create faucet IDs
    let usdc_faucet = AccountId::dummy(
        [0xAA; 15],
        miden_protocol::account::AccountIdVersion::Version0,
        AccountType::FungibleFaucet,
        AccountStorageMode::Public,
    );
    let eth_faucet = AccountId::dummy(
        [0xBB; 15],
        miden_protocol::account::AccountIdVersion::Version0,
        AccountType::FungibleFaucet,
        AccountStorageMode::Public,
    );

    // Create user IDs
    let alice_id = AccountId::dummy(
        [1; 15],
        miden_protocol::account::AccountIdVersion::Version0,
        AccountType::RegularAccountImmutableCode,
        AccountStorageMode::Public,
    );
    let bob_id = AccountId::dummy(
        [2; 15],
        miden_protocol::account::AccountIdVersion::Version0,
        AccountType::RegularAccountImmutableCode,
        AccountStorageMode::Public,
    );

    // Alice: offers 50 USDC, wants 25 ETH
    let alice_note = PswapNote::create(
        alice_id,
        Asset::Fungible(FungibleAsset::new(usdc_faucet, 50).unwrap()),
        Asset::Fungible(FungibleAsset::new(eth_faucet, 25).unwrap()),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )
    .unwrap();

    // Bob: offers 25 ETH, wants 50 USDC
    let bob_note = PswapNote::create(
        bob_id,
        Asset::Fungible(FungibleAsset::new(eth_faucet, 25).unwrap()),
        Asset::Fungible(FungibleAsset::new(usdc_faucet, 50).unwrap()),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )
    .unwrap();

    // --- Solver parses notes into Orders ---
    let alice_order = Order::from_note(&alice_note).expect("Failed to parse Alice's note");
    let bob_order = Order::from_note(&bob_note).expect("Failed to parse Bob's note");

    // Verify Alice's order
    assert_eq!(alice_order.offered_faucet_id, usdc_faucet);
    assert_eq!(alice_order.offered_amount, 50);
    assert_eq!(alice_order.requested_faucet_id, eth_faucet);
    assert_eq!(alice_order.requested_amount, 25);
    assert_eq!(alice_order.creator_id, alice_id);

    // Verify Bob's order
    assert_eq!(bob_order.offered_faucet_id, eth_faucet);
    assert_eq!(bob_order.offered_amount, 25);
    assert_eq!(bob_order.requested_faucet_id, usdc_faucet);
    assert_eq!(bob_order.requested_amount, 50);
    assert_eq!(bob_order.creator_id, bob_id);

    // --- Solver runs Matcher ---
    let group = Matcher::run(vec![alice_order, bob_order], usdc_faucet, eth_faucet)
        .expect("Should match");
    assert_eq!(group.side_a.len(), 1, "Should have 1 side A order");
    assert_eq!(group.side_b.len(), 1, "Should have 1 side B order");

    // fill amounts: USDC flowing from Alice (side A) to Bob (side B)
    assert_eq!(group.side_a[0].output_amount, 50, "All 50 USDC should flow from Alice to Bob");
    assert_eq!(group.side_b[0].output_amount, 25, "All 25 ETH should flow from Bob to Alice");

    // Verify no spread (perfect match)
    let inflight_to_a = group.side_a[0].fill_amount; // 25 ETH
    let inflight_to_b = group.side_b[0].fill_amount; // 50 USDC

    assert_eq!(inflight_to_a, 25, "Alice receives 25 ETH via inflight");
    assert_eq!(inflight_to_b, 50, "Bob receives 50 USDC via inflight");
    assert_eq!(group.surplus_x, 0, "No USDC surplus (no spread)");
    assert_eq!(group.surplus_y, 0, "No ETH surplus (no spread)");

    println!("Note parsing and matching test passed!");
}

// ---------------------------------------------------------------------------
// Test 2: Full pipeline with MockChain execution (no spread)
// ---------------------------------------------------------------------------

/// End-to-end test:
/// 1. Create Alice (50 USDC) and Bob (25 ETH) with tokens
/// 2. They create pswap notes (Alice: 50 USDC -> 25 ETH, Bob: 25 ETH -> 50 USDC)
/// 3. Send notes to MockChain
/// 4. Create Solver with custom BasicWallet from miden-swapp
/// 5. Solver reads notes, parses Orders, runs Matcher
/// 6. Execute cross-swap on MockChain
/// 7. Verify: P2ID notes created, solver vault unchanged
#[tokio::test]
async fn test_solver_full_pipeline_cross_swap() -> anyhow::Result<()> {
    println!("=== Test: Solver Full Pipeline Cross-Swap ===");
    let mut builder = MockChain::builder();

    // --- Create faucets ---
    let usdc_faucet =
        builder.add_existing_basic_faucet(Auth::BasicAuth, "USDC", 1000, Some(100))?;
    let eth_faucet = builder.add_existing_basic_faucet(Auth::BasicAuth, "ETH", 1000, Some(50))?;
    println!("USDC Faucet: {:?}", usdc_faucet.id());
    println!("ETH Faucet: {:?}", eth_faucet.id());

    // --- Create Alice with 50 USDC ---
    let alice = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(usdc_faucet.id(), 50)?.into()],
    )?;
    println!("Alice: {:?} (50 USDC)", alice.id());

    // --- Create Bob with 25 ETH ---
    let bob = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(eth_faucet.id(), 25)?.into()],
    )?;
    println!("Bob: {:?} (25 ETH)", bob.id());

    // --- Create Solver account (custom + standard BasicWallet, 0 assets) ---
    let solver_account = AccountBuilder::new([42u8; 32])
        .account_type(AccountType::RegularAccountImmutableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_component(BasicWallet::component())
        .with_component(StdBasicWallet)
        .with_auth_component(miden_standards::account::auth::NoAuth::new())
        .build_existing()?;
    let solver_id = solver_account.id();
    builder.add_account(solver_account.clone())?;
    println!("Solver: {:?} (0 assets, custom + std BasicWallet)", solver_id);

    // --- Create pswap notes ---
    let mut rng = RpoRandomCoin::new(Word::default());

    // Alice's note: offers 50 USDC, wants 25 ETH
    let alice_note = PswapNote::create(
        alice.id(),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 50)?),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 25)?),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )?;
    println!("Alice's note: {:?} (50 USDC -> 25 ETH)", alice_note.id());

    // Bob's note: offers 25 ETH, wants 50 USDC
    let bob_note = PswapNote::create(
        bob.id(),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 25)?),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 50)?),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )?;
    println!("Bob's note: {:?} (25 ETH -> 50 USDC)", bob_note.id());

    // --- Send notes to MockChain ---
    builder.add_output_note(OutputNote::Full(alice_note.clone()));
    builder.add_output_note(OutputNote::Full(bob_note.clone()));
    let mock_chain = builder.build()?;
    println!("MockChain built with 2 swap notes");

    // ===== SOLVER PIPELINE =====

    // Step 1: Parse notes into Orders
    let alice_order = Order::from_note(&alice_note)?;
    let bob_order = Order::from_note(&bob_note)?;
    println!(
        "Parsed orders: Alice({}USDC->{}ETH), Bob({}ETH->{}USDC)",
        alice_order.offered_amount,
        alice_order.requested_amount,
        bob_order.offered_amount,
        bob_order.requested_amount,
    );

    // Step 2: Run Matcher
    let faucet_x = usdc_faucet.id();
    let faucet_y = eth_faucet.id();
    let group = Matcher::run(vec![alice_order, bob_order], faucet_x, faucet_y)
        .expect("Should match");
    assert_eq!(group.side_a.len(), 1, "Should have 1 side A order");
    assert_eq!(group.side_b.len(), 1, "Should have 1 side B order");
    println!("Match: total_x={} USDC, total_y={} ETH", group.total_x, group.total_y);

    // Step 3: Compute note args (executor logic)
    let inflight_to_a = group.side_a[0].fill_amount; // 25 ETH
    let inflight_to_b = group.side_b[0].fill_amount; // 50 USDC
    assert_eq!(group.surplus_x, 0, "No surplus (perfect match)");
    assert_eq!(group.surplus_y, 0, "No surplus (perfect match)");

    // Note args: [consumer_tag, surplus, inflight, input]
    let alice_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_a), // 25 ETH inflight
        Felt::ZERO,
    ]);
    let bob_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_b), // 50 USDC inflight
        Felt::ZERO,
    ]);

    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(group.side_a[0].order.note.id(), alice_note_args);
    note_args_map.insert(group.side_b[0].order.note.id(), bob_note_args);

    // Step 4: Build expected output notes
    let (alice_p2id, alice_remainder) =
        PswapNote::create_output_notes(&group.side_a[0].order.note, solver_id, 0, inflight_to_a)?;
    assert!(alice_remainder.is_none(), "Full fill -> no remainder for Alice");

    let (bob_p2id, bob_remainder) =
        PswapNote::create_output_notes(&group.side_b[0].order.note, solver_id, 0, inflight_to_b)?;
    assert!(bob_remainder.is_none(), "Full fill -> no remainder for Bob");

    // Step 5: Execute on MockChain
    let tx_context = mock_chain
        .build_tx_context(
            solver_id,
            &[group.side_a[0].order.note.id(), group.side_b[0].order.note.id()],
            &[],
        )?
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(vec![
            OutputNote::Full(alice_p2id),
            OutputNote::Full(bob_p2id),
        ])
        .build()?;

    let executed_tx = tx_context.execute().await?;
    println!("Transaction executed successfully!");
    println!(
        "Cycle count: {:?}",
        executed_tx.measurements().note_execution
    );

    // Step 6: Verify results
    let output_notes = executed_tx.output_notes();
    assert_eq!(output_notes.num_notes(), 2, "Expected 2 P2ID notes");

    let mut alice_p2id_found = false;
    let mut bob_p2id_found = false;

    for idx in 0..output_notes.num_notes() {
        let note = output_notes.get_note(idx);
        let assets = note.assets().unwrap();
        if assets.num_assets() == 1 {
            let asset = assets.iter().next().unwrap();
            if let Asset::Fungible(f) = asset {
                if f.faucet_id() == eth_faucet.id() && f.amount() == 25 {
                    alice_p2id_found = true;
                    println!("  Alice's P2ID: 25 ETH");
                } else if f.faucet_id() == usdc_faucet.id() && f.amount() == 50 {
                    bob_p2id_found = true;
                    println!("  Bob's P2ID: 50 USDC");
                }
            }
        }
    }

    assert!(alice_p2id_found, "Alice's P2ID note (25 ETH) not found");
    assert!(bob_p2id_found, "Bob's P2ID note (50 USDC) not found");

    // Verify solver's vault is unchanged (facilitator only)
    let vault_delta = executed_tx.account_delta().vault();
    let added: Vec<Asset> = vault_delta.added_assets().collect();
    let removed: Vec<Asset> = vault_delta.removed_assets().collect();
    assert_eq!(added.len(), 0, "Solver should not receive assets");
    assert_eq!(removed.len(), 0, "Solver should not spend assets");
    println!("  Solver vault unchanged (0 added, 0 removed)");

    println!("\nSolver full pipeline cross-swap test passed!");
    println!("  - Alice sent 50 USDC, received 25 ETH (via P2ID)");
    println!("  - Bob sent 25 ETH, received 50 USDC (via P2ID)");
    println!("  - Solver facilitated with 0 assets");

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3: Full pipeline with spread (solver earns profit)
// ---------------------------------------------------------------------------

/// Alice offers 100 USDC for 50 ETH (willing to pay 2 USDC/ETH)
/// Bob offers 50 ETH for 80 USDC (willing to accept 1.6 USDC/ETH)
/// Solver matches them and earns 20 USDC spread.
#[tokio::test]
async fn test_solver_cross_swap_with_spread() -> anyhow::Result<()> {
    println!("=== Test: Solver Cross-Swap With Spread ===");
    let mut builder = MockChain::builder();

    // --- Create faucets ---
    let usdc_faucet =
        builder.add_existing_basic_faucet(Auth::BasicAuth, "USDC", 1000, Some(200))?;
    let eth_faucet = builder.add_existing_basic_faucet(Auth::BasicAuth, "ETH", 1000, Some(100))?;

    // --- Create Alice (100 USDC) and Bob (50 ETH) ---
    let alice = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(usdc_faucet.id(), 100)?.into()],
    )?;
    println!("Alice: {:?} (100 USDC)", alice.id());

    let bob = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(eth_faucet.id(), 50)?.into()],
    )?;
    println!("Bob: {:?} (50 ETH)", bob.id());

    // --- Create Solver (custom + standard BasicWallet, matching miden-swapp pattern) ---
    let solver_account = AccountBuilder::new([42u8; 32])
        .account_type(AccountType::RegularAccountImmutableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_component(BasicWallet::component())
        .with_component(StdBasicWallet)
        .with_auth_component(miden_standards::account::auth::NoAuth::new())
        .build_existing()?;
    let solver_id = solver_account.id();
    builder.add_account(solver_account.clone())?;
    println!("Solver: {:?}", solver_id);

    // --- Create pswap notes ---
    let mut rng = RpoRandomCoin::new(Word::default());

    // Alice: offers 100 USDC, wants 50 ETH (price: 2 USDC/ETH)
    let alice_note = PswapNote::create(
        alice.id(),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 100)?),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 50)?),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )?;
    println!("Alice's note: 100 USDC -> 50 ETH");

    // Bob: offers 50 ETH, wants 80 USDC (price: 1.6 USDC/ETH)
    let bob_note = PswapNote::create(
        bob.id(),
        Asset::Fungible(FungibleAsset::new(eth_faucet.id(), 50)?),
        Asset::Fungible(FungibleAsset::new(usdc_faucet.id(), 80)?),
        NoteType::Public,
        NoteAttachment::default(),
        &mut rng,
    )?;
    println!("Bob's note: 50 ETH -> 80 USDC");

    // --- Send to MockChain ---
    builder.add_output_note(OutputNote::Full(alice_note.clone()));
    builder.add_output_note(OutputNote::Full(bob_note.clone()));
    let mock_chain = builder.build()?;

    // ===== SOLVER PIPELINE =====

    // Parse orders
    let alice_order = Order::from_note(&alice_note)?;
    let bob_order = Order::from_note(&bob_note)?;

    // Run Matcher
    let faucet_x = usdc_faucet.id();
    let faucet_y = eth_faucet.id();
    let group = Matcher::run(vec![alice_order, bob_order], faucet_x, faucet_y)
        .expect("Should match");
    assert_eq!(group.side_a.len(), 1, "Should have 1 side A order");
    assert_eq!(group.side_b.len(), 1, "Should have 1 side B order");
    println!("Match: total_x={} USDC, total_y={} ETH", group.total_x, group.total_y);

    // Compute fills and spread
    let inflight_to_a = group.side_a[0].fill_amount; // 50 ETH
    let inflight_to_b = group.side_b[0].fill_amount; // 80 USDC
    let surplus_x = group.surplus_x; // USDC spread for solver
    let surplus_y = group.surplus_y;

    println!(
        "inflight_to_alice={} ETH, inflight_to_bob={} USDC",
        inflight_to_a, inflight_to_b
    );
    println!(
        "surplus_x={} USDC (solver profit), surplus_y={} ETH",
        surplus_x, surplus_y
    );

    assert_eq!(inflight_to_a, 50, "Alice gets 50 ETH");
    assert_eq!(inflight_to_b, 80, "Bob gets 80 USDC");
    assert_eq!(surplus_x, 20, "Solver earns 20 USDC spread");
    assert_eq!(surplus_y, 0, "No ETH surplus");

    // Note args: [consumer_tag, surplus, inflight, input]
    let solver_tag_felt = compute_p2id_tag_felt(solver_id);
    let alice_note_args = Word::from([
        solver_tag_felt,              // solver's P2ID tag (for surplus)
        Felt::new(surplus_x),         // 20 USDC surplus -> solver
        Felt::new(inflight_to_a),     // 50 ETH inflight
        Felt::ZERO,                   // 0 direct input
    ]);
    let bob_note_args = Word::from([
        Felt::ZERO,                   // no surplus from Bob's note
        Felt::ZERO,
        Felt::new(inflight_to_b),     // 80 USDC inflight
        Felt::ZERO,
    ]);

    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(group.side_a[0].order.note.id(), alice_note_args);
    note_args_map.insert(group.side_b[0].order.note.id(), bob_note_args);

    // Build expected output notes
    // P2ID for Alice (50 ETH)
    let (alice_p2id, alice_rem) =
        PswapNote::create_output_notes(&group.side_a[0].order.note, solver_id, 0, inflight_to_a)?;
    assert!(alice_rem.is_none(), "Alice is fully filled");

    // P2ID for Bob (80 USDC)
    let (bob_p2id, bob_rem) =
        PswapNote::create_output_notes(&group.side_b[0].order.note, solver_id, 0, inflight_to_b)?;
    assert!(bob_rem.is_none(), "Bob is fully filled");

    // Spread P2ID for Solver (20 USDC from Alice's note)
    let solver_spread_serial = Word::from([
        alice_note.recipient().serial_num()[0] + Felt::new(2),
        alice_note.recipient().serial_num()[1] + Felt::new(2),
        alice_note.recipient().serial_num()[2] + Felt::new(2),
        alice_note.recipient().serial_num()[3] + Felt::new(2),
    ]);
    let solver_spread_recipient = build_p2id_recipient(solver_id, solver_spread_serial)?;
    let solver_spread_tag = compute_p2id_tag(solver_id);
    let solver_spread_asset = FungibleAsset::new(usdc_faucet.id(), surplus_x)?;
    let solver_spread_assets = NoteAssets::new(vec![solver_spread_asset.into()])?;

    let aux_word = Word::from([Felt::new(surplus_x), ZERO, ZERO, ZERO]);
    let attachment = NoteAttachment::new_word(NoteAttachmentScheme::none(), aux_word);
    let solver_spread_metadata = NoteMetadata::new(solver_id, NoteType::Public, solver_spread_tag)
        .with_attachment(attachment);
    let solver_spread_note =
        Note::new(solver_spread_assets, solver_spread_metadata, solver_spread_recipient);

    // Execute on MockChain
    let tx_context = mock_chain
        .build_tx_context(
            solver_id,
            &[group.side_a[0].order.note.id(), group.side_b[0].order.note.id()],
            &[],
        )?
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(vec![
            OutputNote::Full(alice_p2id),
            OutputNote::Full(bob_p2id),
            OutputNote::Full(solver_spread_note),
        ])
        .build()?;

    let executed_tx = tx_context.execute().await?;
    println!("Transaction executed successfully!");
    println!(
        "Cycle count: {:?}",
        executed_tx.measurements().note_execution
    );

    // Verify output notes
    let output_notes = executed_tx.output_notes();
    assert_eq!(
        output_notes.num_notes(),
        3,
        "Expected 3 P2ID notes (Alice, Bob, Solver spread)"
    );

    let mut alice_p2id_found = false;
    let mut bob_p2id_found = false;
    let mut solver_spread_found = false;

    for idx in 0..output_notes.num_notes() {
        let note = output_notes.get_note(idx);
        let assets = note.assets().unwrap();
        if assets.num_assets() == 1 {
            let asset = assets.iter().next().unwrap();
            if let Asset::Fungible(f) = asset {
                if f.faucet_id() == eth_faucet.id() && f.amount() == 50 {
                    alice_p2id_found = true;
                    println!("  Alice's P2ID: 50 ETH");
                } else if f.faucet_id() == usdc_faucet.id() && f.amount() == 80 {
                    bob_p2id_found = true;
                    println!("  Bob's P2ID: 80 USDC");
                } else if f.faucet_id() == usdc_faucet.id() && f.amount() == 20 {
                    solver_spread_found = true;
                    println!("  Solver's spread: 20 USDC");
                }
            }
        }
    }

    assert!(alice_p2id_found, "Alice's P2ID note (50 ETH) not found");
    assert!(bob_p2id_found, "Bob's P2ID note (80 USDC) not found");
    assert!(solver_spread_found, "Solver's spread note (20 USDC) not found");

    println!("\nSolver cross-swap with spread test passed!");
    println!("  - Alice: 100 USDC -> 50 ETH");
    println!("  - Bob: 50 ETH -> 80 USDC");
    println!("  - Solver earned 20 USDC spread");

    Ok(())
}

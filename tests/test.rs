#![cfg(feature = "client")]

use std::collections::BTreeMap;

use miden_client::{
    account::component::BasicWallet as StdBasicWallet, note::NoteType, transaction::OutputNote,
    Felt, Word,
};
use miden_core::FieldElement;
use miden_crypto::rand::RpoRandomCoin;
use miden_protocol::{
    account::{AccountBuilder, AccountId, AccountStorageMode, AccountType},
    asset::{Asset, FungibleAsset},
};
use consume_script::ConsumeAssetScript;
use miden_standards::note::{PswapNote, PswapNoteStorage};
use miden_testing::{Auth, MockChain};

use solver::order::Order;
use solver::simple_matcher::SimpleMatcher;

fn create_pswap_note(
    sender: AccountId,
    offered_faucet: AccountId,
    offered_amount: u64,
    requested_faucet: AccountId,
    requested_amount: u64,
    rng: &mut RpoRandomCoin,
) -> miden_protocol::note::Note {
    use miden_protocol::crypto::rand::FeltRng;
    let offered_asset = FungibleAsset::new(offered_faucet, offered_amount).unwrap();
    let requested_asset = FungibleAsset::new(requested_faucet, requested_amount).unwrap();
    let serial_number = rng.draw_word();

    let storage = PswapNoteStorage::builder()
        .requested_asset(requested_asset)
        .creator_account_id(sender)
        .build();

    let pswap = PswapNote::builder()
        .sender(sender)
        .storage(storage)
        .serial_number(serial_number)
        .note_type(NoteType::Public)
        .offered_asset(offered_asset)
        .build()
        .expect("Failed to create PSWAP note");

    miden_protocol::note::Note::from(pswap)
}

fn execute_pswap_note(
    note: &miden_protocol::note::Note,
    consumer_id: AccountId,
    fill_amount: u64,
) -> anyhow::Result<(miden_protocol::note::Note, Option<miden_protocol::note::Note>)> {
    let pswap = PswapNote::try_from(note)
        .map_err(|e| anyhow::anyhow!("parse: {}", e))?;
    let fill_asset = FungibleAsset::new(
        pswap.storage().requested_faucet_id(),
        fill_amount,
    ).map_err(|e| anyhow::anyhow!("fill asset: {}", e))?;
    let (payback, remainder_pswap) = pswap.execute(consumer_id, None, Some(fill_asset))
        .map_err(|e| anyhow::anyhow!("execute: {}", e))?;
    Ok((payback, remainder_pswap.map(miden_protocol::note::Note::from)))
}

/// Computes how much offered asset is released for a given fill of the requested asset.
fn calculate_output_amount(offered: u64, requested: u64, input: u64) -> u64 {
    if input == requested { return offered; }
    if input == 0 { return 0; }
    const P: u64 = 100_000;
    if offered > requested {
        let ratio = (offered * P) / requested;
        (input * ratio) / P
    } else {
        let ratio = (requested * P) / offered;
        (input * P) / ratio
    }
}

// ---------------------------------------------------------------------------
// Test 1: Note parsing + matching (no MockChain execution)
// ---------------------------------------------------------------------------

/// Verifies that the solver can:
/// 1. Parse pswap notes into Orders
/// 2. Run the SimpleMatcher to find valid cross-swap matches
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
    let alice_note = create_pswap_note(alice_id, usdc_faucet, 50, eth_faucet, 25, &mut rng);

    // Bob: offers 25 ETH, wants 50 USDC
    let bob_note = create_pswap_note(bob_id, eth_faucet, 25, usdc_faucet, 50, &mut rng);

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

    // --- Solver runs SimpleMatcher ---
    // X = USDC (base). Alice offers USDC = ask. Bob offers ETH = bid.
    let (bids, asks) = SimpleMatcher::run(vec![bob_order], vec![alice_order]);

    assert_eq!(asks.len(), 1);
    assert_eq!(bids.len(), 1);

    // Both should be fully filled (exact match, same price)
    assert_eq!(asks[0].fill_amount, 25, "Alice receives 25 ETH");
    assert_eq!(bids[0].fill_amount, 50, "Bob receives 50 USDC");

    // Verify output amounts (offered asset released)
    // Alice fully filled → releases all 50 USDC
    assert_eq!(
        calculate_output_amount(
            asks[0].offered_amount,
            asks[0].requested_amount,
            asks[0].fill_amount
        ),
        50,
        "Alice releases 50 USDC"
    );
    // Bob fully filled → releases all 25 ETH
    assert_eq!(
        calculate_output_amount(
            bids[0].offered_amount,
            bids[0].requested_amount,
            bids[0].fill_amount
        ),
        25,
        "Bob releases 25 ETH"
    );

    // Verify no spread (perfect match)
    let total_x: u64 = 50; // USDC released by asks
    let total_y: u64 = 25; // ETH released by bids
    let demand_x: u64 = bids[0].fill_amount; // 50 USDC
    let demand_y: u64 = asks[0].fill_amount; // 25 ETH
    assert_eq!(total_x - demand_x, 0, "No USDC surplus");
    assert_eq!(total_y - demand_y, 0, "No ETH surplus");

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
/// 5. Solver reads notes, parses Orders, runs SimpleMatcher
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
        .with_component(StdBasicWallet)
        .with_auth_component(miden_standards::account::auth::NoAuth::new())
        .build_existing()?;
    let solver_id = solver_account.id();
    builder.add_account(solver_account.clone())?;
    println!(
        "Solver: {:?} (0 assets, custom + std BasicWallet)",
        solver_id
    );

    // --- Create pswap notes ---
    let mut rng = RpoRandomCoin::new(Word::default());

    // Alice's note: offers 50 USDC, wants 25 ETH
    let alice_note = create_pswap_note(alice.id(), usdc_faucet.id(), 50, eth_faucet.id(), 25, &mut rng);
    println!("Alice's note: {:?} (50 USDC -> 25 ETH)", alice_note.id());

    // Bob's note: offers 25 ETH, wants 50 USDC
    let bob_note = create_pswap_note(bob.id(), eth_faucet.id(), 25, usdc_faucet.id(), 50, &mut rng);
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

    // Step 2: Run SimpleMatcher (X = USDC: Alice=ask, Bob=bid)
    let (bids, asks) = SimpleMatcher::run(vec![bob_order], vec![alice_order]);
    assert_ne!(asks[0].fill_amount, 0, "Alice should be filled");
    assert_ne!(bids[0].fill_amount, 0, "Bob should be filled");

    let inflight_to_alice = asks[0].fill_amount; // 25 ETH
    let inflight_to_bob = bids[0].fill_amount; // 50 USDC
    assert_eq!(inflight_to_alice, 25, "Alice gets 25 ETH");
    assert_eq!(inflight_to_bob, 50, "Bob gets 50 USDC");
    println!(
        "Match: Alice gets {} ETH, Bob gets {} USDC",
        inflight_to_alice, inflight_to_bob
    );

    // Step 3: Compute note args (no surplus in perfect match)
    // Note args: [consumer_tag, surplus, inflight, input]
    let alice_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_alice), // 25 ETH inflight
        Felt::ZERO,
    ]);
    let bob_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_bob), // 50 USDC inflight
        Felt::ZERO,
    ]);

    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(asks[0].note.id(), alice_note_args);
    note_args_map.insert(bids[0].note.id(), bob_note_args);

    // Step 4: Build expected output notes
    let (alice_p2id, alice_remainder) =
        execute_pswap_note(&asks[0].note, solver_id, inflight_to_alice)?;
    assert!(
        alice_remainder.is_none(),
        "Full fill -> no remainder for Alice"
    );

    let (bob_p2id, bob_remainder) =
        execute_pswap_note(&bids[0].note, solver_id, inflight_to_bob)?;
    assert!(bob_remainder.is_none(), "Full fill -> no remainder for Bob");

    // Step 5: Execute on MockChain
    let tx_context = mock_chain
        .build_tx_context(solver_id, &[asks[0].note.id(), bids[0].note.id()], &[])?
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
///
/// SimpleMatcher trades 80 USDC (Bob's demand):
/// - Alice: sells 80 of 100 USDC, receives 40 ETH (partial fill)
/// - Bob: gets 80 USDC (full fill), releases 50 ETH
/// - Solver earns 10 ETH spread (50 released - 40 to Alice)
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
        .with_component(StdBasicWallet)
        .with_auth_component(miden_standards::account::auth::NoAuth::new())
        .build_existing()?;
    let solver_id = solver_account.id();
    builder.add_account(solver_account.clone())?;
    println!("Solver: {:?}", solver_id);

    // --- Create pswap notes ---
    let mut rng = RpoRandomCoin::new(Word::default());

    // Alice: offers 100 USDC, wants 50 ETH (price: 2 USDC/ETH)
    let alice_note = create_pswap_note(alice.id(), usdc_faucet.id(), 100, eth_faucet.id(), 50, &mut rng);
    println!("Alice's note: 100 USDC -> 50 ETH");

    // Bob: offers 50 ETH, wants 80 USDC (price: 1.6 USDC/ETH)
    let bob_note = create_pswap_note(bob.id(), eth_faucet.id(), 50, usdc_faucet.id(), 80, &mut rng);
    println!("Bob's note: 50 ETH -> 80 USDC");

    // --- Send to MockChain ---
    builder.add_output_note(OutputNote::Full(alice_note.clone()));
    builder.add_output_note(OutputNote::Full(bob_note.clone()));
    let mock_chain = builder.build()?;

    // ===== SOLVER PIPELINE =====

    // Parse orders
    let alice_order = Order::from_note(&alice_note)?;
    let bob_order = Order::from_note(&bob_note)?;

    // Run SimpleMatcher (X = USDC: Alice=ask, Bob=bid)
    let (bids, asks) = SimpleMatcher::run(vec![bob_order], vec![alice_order]);
    assert_ne!(asks[0].fill_amount, 0, "Alice should be filled");
    assert_ne!(bids[0].fill_amount, 0, "Bob should be filled");

    // SimpleMatcher trades 80 USDC (Bob's demand = min of bid qty and ask qty)
    let inflight_to_alice = asks[0].fill_amount; // ETH Alice receives
    let inflight_to_bob = bids[0].fill_amount; // USDC Bob receives

    println!(
        "inflight_to_alice={} ETH, inflight_to_bob={} USDC",
        inflight_to_alice, inflight_to_bob
    );

    assert_eq!(
        inflight_to_alice, 40,
        "Alice gets 40 ETH (partial: 80/100 USDC sold)"
    );
    assert_eq!(inflight_to_bob, 80, "Bob gets 80 USDC (full fill)");

    // Compute output amounts and surplus
    // Alice: partial fill → calculate_output_amount(100, 50, 40) = 80 USDC released
    let alice_output = calculate_output_amount(
        asks[0].offered_amount,
        asks[0].requested_amount,
        inflight_to_alice,
    );
    // Bob: full fill → offered_amount = 50 ETH released
    let bob_output = bids[0].offered_amount;

    let total_x = alice_output; // 80 USDC
    let total_y = bob_output; // 50 ETH
    let demand_x = inflight_to_bob; // 80 USDC
    let demand_y = inflight_to_alice; // 40 ETH
    let surplus_x = total_x - demand_x; // 0
    let surplus_y = total_y - demand_y; // 10 ETH

    println!("total_x={} USDC, total_y={} ETH", total_x, total_y);
    println!("surplus_x={} USDC, surplus_y={} ETH", surplus_x, surplus_y);

    assert_eq!(surplus_x, 0, "No USDC surplus");
    assert_eq!(surplus_y, 10, "Solver earns 10 ETH spread");

    // --- Build note args ---
    // Note arg layout (Word reversed on-chain): [unused, unused, inflight_amount, input_amount]
    // On-chain: arg[0]=input_amount (last), arg[1]=inflight_amount (second-to-last)
    let alice_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_alice), // 40 ETH inflight
        Felt::ZERO,
    ]);
    let bob_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_bob), // 80 USDC inflight
        Felt::ZERO,
    ]);

    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(asks[0].note.id(), alice_note_args);
    note_args_map.insert(bids[0].note.id(), bob_note_args);

    // --- Build expected output notes ---
    // Alice: P2ID (40 ETH) + remainder (partial fill: 20 USDC remaining)
    let (alice_p2id, alice_remainder) =
        execute_pswap_note(&asks[0].note, solver_id, inflight_to_alice)?;
    assert!(
        alice_remainder.is_some(),
        "Alice has partial fill → remainder note"
    );

    // Bob: P2ID (80 USDC), no remainder (full fill)
    let (bob_p2id, bob_remainder) =
        execute_pswap_note(&bids[0].note, solver_id, inflight_to_bob)?;
    assert!(bob_remainder.is_none(), "Bob is fully filled");

    // Solver spread: 10 ETH via ConsumeAssetScript — goes directly into solver's vault
    let surplus_assets = vec![Asset::Fungible(FungibleAsset::new(
        eth_faucet.id(),
        surplus_y,
    )?)];
    let consume_data = ConsumeAssetScript::prepare(&surplus_assets);

    // Collect expected output notes (no solver spread note — it lands in vault)
    let mut expected_notes = vec![OutputNote::Full(alice_p2id), OutputNote::Full(bob_p2id)];
    if let Some(rem) = alice_remainder {
        expected_notes.push(OutputNote::Full(rem));
    }

    // Execute on MockChain with ConsumeAssetScript for surplus
    let tx_context = mock_chain
        .build_tx_context(solver_id, &[asks[0].note.id(), bids[0].note.id()], &[])?
        .tx_script(ConsumeAssetScript::tx_script())
        .tx_script_args(consume_data.commitment_arg)
        .extend_advice_map([consume_data.advice_map_entry])
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(expected_notes)
        .build()?;

    let executed_tx = tx_context.execute().await?;
    println!("Transaction executed successfully!");
    println!(
        "Cycle count: {:?}",
        executed_tx.measurements().note_execution
    );

    // Verify output notes
    // Spread (10 ETH) goes directly into the solver's vault — not an output note
    let output_notes = executed_tx.output_notes();
    assert_eq!(
        output_notes.num_notes(),
        3,
        "Expected 3 notes: Alice P2ID, Alice remainder, Bob P2ID"
    );

    let mut alice_p2id_found = false;
    let mut bob_p2id_found = false;

    for idx in 0..output_notes.num_notes() {
        let note = output_notes.get_note(idx);
        let assets = note.assets().unwrap();
        if assets.num_assets() == 1 {
            let asset = assets.iter().next().unwrap();
            if let Asset::Fungible(f) = asset {
                if f.faucet_id() == eth_faucet.id() && f.amount() == 40 {
                    alice_p2id_found = true;
                    println!("  Alice's P2ID: 40 ETH");
                } else if f.faucet_id() == usdc_faucet.id() && f.amount() == 80 {
                    bob_p2id_found = true;
                    println!("  Bob's P2ID: 80 USDC");
                }
            }
        }
    }

    assert!(alice_p2id_found, "Alice's P2ID note (40 ETH) not found");
    assert!(bob_p2id_found, "Bob's P2ID note (80 USDC) not found");

    println!("\nSolver cross-swap with spread test passed!");
    println!("  - Alice: 80 of 100 USDC sold -> 40 ETH (partial fill)");
    println!("  - Bob: 50 ETH -> 80 USDC (full fill)");
    println!("  - Solver earned 10 ETH spread");
    println!("  - Alice has 20 USDC remaining in remainder note");

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 4: Full pipeline with spread — solver earns 6 USDC
// ---------------------------------------------------------------------------

/// Alice offers 20 USDC for 10 ETH (willing to pay 2.0 USDC/ETH)
/// Bob offers 10 ETH for 14 USDC (willing to accept 1.4 USDC/ETH)
///
/// Both orders fully fill (trade_qty = 10 ETH):
/// - Alice: gets 10 ETH (full fill), gives 20 USDC
/// - Bob: gets 14 USDC (full fill), gives 10 ETH
/// - Solver earns 6 USDC spread (20 USDC in - 14 USDC out)
///
/// Note: amounts chosen so calculate_output_amount has no precision loss.
#[tokio::test]
async fn test_solver_cross_swap_with_6_usdc_spread() -> anyhow::Result<()> {
    println!("=== Test: Solver Cross-Swap With 6 USDC Spread ===");
    let mut builder = MockChain::builder();

    // --- Create faucets ---
    let usdc_faucet =
        builder.add_existing_basic_faucet(Auth::BasicAuth, "USDC", 1000, Some(200))?;
    let eth_faucet = builder.add_existing_basic_faucet(Auth::BasicAuth, "ETH", 1000, Some(100))?;

    // --- Create Alice (20 USDC) and Bob (10 ETH) ---
    let alice = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(usdc_faucet.id(), 20)?.into()],
    )?;
    println!("Alice: {:?} (20 USDC)", alice.id());

    let bob = builder.add_existing_wallet_with_assets(
        Auth::BasicAuth,
        [FungibleAsset::new(eth_faucet.id(), 10)?.into()],
    )?;
    println!("Bob: {:?} (10 ETH)", bob.id());

    // --- Create Solver (custom + standard BasicWallet) ---
    let solver_account = AccountBuilder::new([42u8; 32])
        .account_type(AccountType::RegularAccountImmutableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_component(StdBasicWallet)
        .with_auth_component(miden_standards::account::auth::NoAuth::new())
        .build_existing()?;
    let solver_id = solver_account.id();
    builder.add_account(solver_account.clone())?;
    println!("Solver: {:?}", solver_id);

    // --- Create pswap notes ---
    let mut rng = RpoRandomCoin::new(Word::default());

    // Alice: offers 20 USDC, wants 10 ETH (price: 2.0 USDC/ETH)
    let alice_note = create_pswap_note(alice.id(), usdc_faucet.id(), 20, eth_faucet.id(), 10, &mut rng);
    println!("Alice's note: 20 USDC -> 10 ETH");

    // Bob: offers 10 ETH, wants 14 USDC (price: 1.4 USDC/ETH)
    let bob_note = create_pswap_note(bob.id(), eth_faucet.id(), 10, usdc_faucet.id(), 14, &mut rng);
    println!("Bob's note: 10 ETH -> 14 USDC");

    // --- Send to MockChain ---
    builder.add_output_note(OutputNote::Full(alice_note.clone()));
    builder.add_output_note(OutputNote::Full(bob_note.clone()));
    let mock_chain = builder.build()?;

    // ===== SOLVER PIPELINE =====

    // Parse orders
    let alice_order = Order::from_note(&alice_note)?;
    let bob_order = Order::from_note(&bob_note)?;

    // Run SimpleMatcher
    // X = ETH (base): Bob offers ETH = ask, Alice offers USDC = bid
    let (bids, asks) = SimpleMatcher::run(vec![alice_order], vec![bob_order]);
    assert_ne!(asks[0].fill_amount, 0, "Bob (ask) should be filled");
    assert_ne!(bids[0].fill_amount, 0, "Alice (bid) should be filled");

    let inflight_to_alice = bids[0].fill_amount; // ETH Alice receives
    let inflight_to_bob = asks[0].fill_amount; // USDC Bob receives

    println!(
        "inflight_to_alice={} ETH, inflight_to_bob={} USDC",
        inflight_to_alice, inflight_to_bob
    );

    assert_eq!(inflight_to_alice, 10, "Alice gets 10 ETH (full fill)");
    assert_eq!(inflight_to_bob, 14, "Bob gets 14 USDC (full fill)");

    // Compute surplus
    let total_x = 10u64; // ETH from Bob's note
    let total_y = 20u64; // USDC from Alice's note
    let demand_x = inflight_to_alice; // 10 ETH to Alice
    let demand_y = inflight_to_bob; // 14 USDC to Bob
    let surplus_x = total_x - demand_x; // 0 ETH
    let surplus_y = total_y - demand_y; // 6 USDC

    println!("total_x={} ETH, total_y={} USDC", total_x, total_y);
    println!("surplus_x={} ETH, surplus_y={} USDC", surplus_x, surplus_y);

    assert_eq!(surplus_x, 0, "No ETH surplus");
    assert_eq!(surplus_y, 6, "Solver earns 6 USDC spread");

    // --- Build note args ---
    let alice_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_alice), // 10 ETH inflight
        Felt::ZERO,
    ]);
    let bob_note_args = Word::from([
        Felt::ZERO,
        Felt::ZERO,
        Felt::new(inflight_to_bob), // 14 USDC inflight
        Felt::ZERO,
    ]);

    let mut note_args_map = BTreeMap::new();
    note_args_map.insert(bids[0].note.id(), alice_note_args);
    note_args_map.insert(asks[0].note.id(), bob_note_args);

    // --- Build expected output notes ---
    // Alice (bid): P2ID (20 ETH), no remainder (full fill)
    let (alice_p2id, alice_remainder) =
        execute_pswap_note(&bids[0].note, solver_id, inflight_to_alice)?;
    assert!(alice_remainder.is_none(), "Alice is fully filled → no remainder");

    // Bob (ask): P2ID (17 USDC), no remainder (full fill)
    let (bob_p2id, bob_remainder) =
        execute_pswap_note(&asks[0].note, solver_id, inflight_to_bob)?;
    assert!(bob_remainder.is_none(), "Bob is fully filled → no remainder");

    // Solver spread: 6 USDC via ConsumeAssetScript
    let surplus_assets = vec![Asset::Fungible(FungibleAsset::new(
        usdc_faucet.id(),
        surplus_y,
    )?)];
    let consume_data = ConsumeAssetScript::prepare(&surplus_assets);

    let expected_notes = vec![OutputNote::Full(alice_p2id), OutputNote::Full(bob_p2id)];

    // Execute on MockChain
    let tx_context = mock_chain
        .build_tx_context(solver_id, &[bids[0].note.id(), asks[0].note.id()], &[])?
        .tx_script(ConsumeAssetScript::tx_script())
        .tx_script_args(consume_data.commitment_arg)
        .extend_advice_map([consume_data.advice_map_entry])
        .extend_note_args(note_args_map)
        .extend_expected_output_notes(expected_notes)
        .build()?;

    let executed_tx = tx_context.execute().await?;
    println!("Transaction executed successfully!");
    println!(
        "Cycle count: {:?}",
        executed_tx.measurements().note_execution
    );

    // Verify output notes (2 P2ID notes, surplus goes to solver vault)
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
                println!(
                    "  Output note[{}]: faucet={:?} amount={}",
                    idx,
                    f.faucet_id(),
                    f.amount()
                );
                if f.faucet_id() == eth_faucet.id() && f.amount() == 10 {
                    alice_p2id_found = true;
                    println!("  → Alice's P2ID: 10 ETH");
                } else if f.faucet_id() == usdc_faucet.id() && f.amount() == 14 {
                    bob_p2id_found = true;
                    println!("  → Bob's P2ID: 14 USDC");
                }
            }
        }
    }

    assert!(alice_p2id_found, "Alice's P2ID note (10 ETH) not found");
    assert!(bob_p2id_found, "Bob's P2ID note (14 USDC) not found");

    // Verify solver vault received 6 USDC surplus
    let vault_delta = executed_tx.account_delta().vault();
    let added: Vec<Asset> = vault_delta.added_assets().collect();
    assert_eq!(added.len(), 1, "Solver should receive 1 asset (6 USDC)");
    if let Asset::Fungible(f) = &added[0] {
        assert_eq!(f.faucet_id(), usdc_faucet.id(), "Surplus asset is USDC");
        assert_eq!(f.amount(), 6, "Solver earns 6 USDC");
    } else {
        panic!("Expected fungible surplus asset");
    }
    let removed: Vec<Asset> = vault_delta.removed_assets().collect();
    assert_eq!(removed.len(), 0, "Solver should not spend assets");

    println!("\nSolver cross-swap with 6 USDC spread test passed!");
    println!("  - Alice: 20 USDC -> 10 ETH (full fill)");
    println!("  - Bob: 10 ETH -> 14 USDC (full fill)");
    println!("  - Solver earned 6 USDC spread");

    Ok(())
}

#[path = "testnet_state.rs"]
mod testnet_state;

use anyhow::{Context, Result};
use miden_client::{
    keystore::FilesystemKeyStore,
    note::{Note, NoteType},
    transaction::{OutputNote, TransactionRequestBuilder},
    Client,
};
use miden_crypto::rand::RpoRandomCoin;
use miden_protocol::{
    account::AccountId,
    asset::FungibleAsset,
    Word, ZERO,
};
use miden_standards::note::{PswapNote, PswapNoteStorage};
use rand::Rng;
use testnet_state::{build_testnet_client, TestState};
use tokio::time::Duration;

const USER_KEYSTORE: &str = "./test_keystore";
const USER_STORE: &str = "./test_store.sqlite3";

/// Interval between individual order creation (target: 1 order per 2 seconds).
const ORDER_INTERVAL_SECS: u64 = 2;

/// How often to sync state and consume received notes (every N rounds).
const CONSUME_EVERY_N: u64 = 10;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Swap Activity (fast mode) ===\n");

    // Load state
    let state = TestState::load()?;
    let faucet_usdt = state.faucet_usdt()?;
    let faucet_eth = state.faucet_eth()?;
    let alice_id = state.alice()?;
    let bob_id = state.bob()?;

    println!("USDT faucet: {}", state.faucet_usdt_id);
    println!("ETH  faucet: {}", state.faucet_eth_id);
    println!("Alice:       {}", state.alice_id);
    println!("Bob:         {}\n", state.bob_id);

    // Initialize client (same store as setup)
    let (mut client, _keystore) = build_testnet_client(USER_KEYSTORE, USER_STORE).await?;
    client.sync_state().await?;
    println!("Client connected.\n");

    // Register PSWAP note tags so we can discover notes
    let dummy_usdt = FungibleAsset::new(faucet_usdt, 1).unwrap();
    let dummy_eth = FungibleAsset::new(faucet_eth, 1).unwrap();
    let tag_usdt_eth = PswapNote::create_tag(NoteType::Public, &dummy_usdt, &dummy_eth);
    let tag_eth_usdt = PswapNote::create_tag(NoteType::Public, &dummy_eth, &dummy_usdt);
    client.add_note_tag(tag_usdt_eth).await?;
    client.add_note_tag(tag_eth_usdt).await?;
    println!("Registered PSWAP note tags.");
    println!("Creating orders every {} seconds...\n", ORDER_INTERVAL_SECS);

    let mut rng = rand::rng();
    let mut round = 0u64;

    // Initial sync
    println!("Syncing state...");
    client.sync_state().await?;
    println!("Ready.\n");

    loop {
        round += 1;

        // Alternate between Alice and Bob each round for ~1 order per 2s
        let is_alice_turn = round % 2 == 1;

        // Generate orders with intentional spread to ensure price crossing.
        // Mid-market is ~2 USDT per ETH.
        //
        // Matcher prices (ETH-per-USDT):
        //   ask_price = eth_wanted / usdt_offered   (lower = cheaper seller)
        //   bid_price = eth_offered / usdt_wanted   (higher = aggressive buyer)
        // Match when bid_price >= ask_price.
        //
        // Alice (ask/seller): generous — offers extra USDT per ETH → low ask_price
        // Bob (bid/buyer): aggressive — offers extra ETH per USDT → high bid_price
        let base_eth: u64 = 20 + rng.random_range(0..=10); // 20-30 ETH

        if is_alice_turn {
            // Alice: offer USDT, want ETH
            // She's a cheap seller: offers 2.2-2.7x USDT per ETH wanted
            let alice_usdt = base_eth * 2 + rng.random_range(5..=20);
            match create_pswap_note(
                &mut client,
                alice_id,
                faucet_usdt,
                alice_usdt,
                faucet_eth,
                base_eth,
            )
            .await
            {
                Ok(note_id) => println!(
                    "[{}] Alice: offer {} USDT for {} ETH  (ask_price={:.3}) ({})",
                    round,
                    alice_usdt,
                    base_eth,
                    base_eth as f64 / alice_usdt as f64,
                    note_id
                ),
                Err(e) => eprintln!("[{}] Alice failed: {}", round, e.to_string()),
            }
        } else {
            // Bob: offer ETH, want USDT
            // He's an aggressive buyer: wants only 1.5-1.9x USDT per ETH offered
            let bob_usdt = base_eth + rng.random_range(10..=25);
            let bob_eth = base_eth;
            match create_pswap_note(
                &mut client,
                bob_id,
                faucet_eth,
                bob_eth,
                faucet_usdt,
                bob_usdt,
            )
            .await
            {
                Ok(note_id) => println!(
                    "[{}] Bob:   offer {} ETH for {} USDT  (bid_price={:.3}) ({})",
                    round,
                    bob_eth,
                    bob_usdt,
                    bob_eth as f64 / bob_usdt as f64,
                    note_id
                ),
                Err(e) => eprintln!("[{}] Bob   failed: {}", round, e.to_string()),
            }
        }

        // Periodically sync and consume notes
        //  if round % CONSUME_EVERY_N == 0 {
        client.sync_state().await.ok();

        match consume_available_notes(&mut client, alice_id).await {
            Ok(n) if n > 0 => println!("  Alice consumed {} note(s)", n),
            _ => {}
        }
        match consume_available_notes(&mut client, bob_id).await {
            Ok(n) if n > 0 => println!("  Bob   consumed {} note(s)", n),
            _ => {}
        }
        // }

        tokio::time::sleep(Duration::from_secs(ORDER_INTERVAL_SECS)).await;
    }
}

/// Create and publish a PSWAP note.
async fn create_pswap_note(
    client: &mut Client<FilesystemKeyStore>,
    creator_id: AccountId,
    offered_faucet: AccountId,
    offered_amount: u64,
    requested_faucet: AccountId,
    requested_amount: u64,
) -> Result<String> {
    let offered = FungibleAsset::new(offered_faucet, offered_amount)
        .map_err(|e| anyhow::anyhow!("Invalid offered asset: {}", e))?;
    let requested = FungibleAsset::new(requested_faucet, requested_amount)
        .map_err(|e| anyhow::anyhow!("Invalid requested asset: {}", e))?;

    let mut rng = rand::rng();
    let seed = Word::from([
        miden_core::Felt::new(rng.random::<u64>()),
        miden_core::Felt::new(rng.random::<u64>()),
        miden_core::Felt::new(rng.random::<u64>()),
        miden_core::Felt::new(rng.random::<u64>()),
    ]);
    let mut coin = RpoRandomCoin::new(seed);
    use miden_protocol::crypto::rand::FeltRng;
    let serial_number = coin.draw_word();

    let storage = PswapNoteStorage::builder()
        .requested_asset(requested)
        .creator_account_id(creator_id)
        .build();

    let pswap = PswapNote::builder()
        .sender(creator_id)
        .storage(storage)
        .serial_number(serial_number)
        .note_type(NoteType::Public)
        .offered_asset(offered)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create PSWAP note: {}", e))?;

    let note = miden_protocol::note::Note::from(pswap);

    let note_id = note.id().to_hex();

    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![OutputNote::Full(note)])
        .build()
        .context("Failed to build publish request")?;

    client
        .submit_new_transaction(creator_id, req)
        .await
        .context("Failed to publish PSWAP note")?;

    Ok(note_id)
}

/// Consume any available notes for the given account. Returns count consumed.
async fn consume_available_notes(
    client: &mut Client<FilesystemKeyStore>,
    account_id: AccountId,
) -> Result<usize> {
    let consumable = client.get_consumable_notes(Some(account_id)).await?;
    let notes: Vec<_> = consumable
        .into_iter()
        .filter_map(|(record, _)| {
            let note: Result<Note, _> = record.try_into();
            note.ok().map(|n| (n, None))
        })
        .collect();

    if notes.is_empty() {
        return Ok(0);
    }

    let count = notes.len();
    let req = TransactionRequestBuilder::new()
        .input_notes(notes)
        .build()
        .context("Failed to build consume request")?;

    client
        .submit_new_transaction(account_id, req)
        .await
        .context("Failed to consume notes")?;

    Ok(count)
}

#[path = "testnet_state.rs"]
mod testnet_state;

use anyhow::{Context, Result};
use miden_client::{
    account::component::{BasicFungibleFaucet, BasicWallet},
    auth::AuthSecretKey,
    note::Note,
    note::NoteType,
    transaction::TransactionRequestBuilder,
    Felt,
};
use miden_protocol::{
    account::{AccountBuilder, AccountStorageMode, AccountType},
    asset::{FungibleAsset, TokenSymbol},
};
use miden_standards::account::auth::AuthFalcon512Rpo;
use rand::RngCore;
use testnet_state::{build_testnet_client, TestState};
use tokio::time::Duration;

const USER_KEYSTORE: &str = "./test_keystore";
const USER_STORE: &str = "./test_store.sqlite3";

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Solver Testnet Setup ===\n");

    // ── Step 1: Initialize client ──────────────────────────────────
    println!("[1/5] Initializing client...");
    let (mut client, keystore) = build_testnet_client(USER_KEYSTORE, USER_STORE).await?;

    let sync = client.sync_state().await?;
    println!("Connected to testnet. Latest block: {}\n", sync.block_num);

    // ── Step 2: Deploy two faucets ─────────────────────────────────
    println!("[2/5] Creating faucets...");

    // Faucet 1: USDT
    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);
    let key_f1 = AuthSecretKey::new_falcon512_rpo();
    let faucet_usdt = AccountBuilder::new(seed)
        .account_type(AccountType::FungibleFaucet)
        .storage_mode(AccountStorageMode::Public)
        .with_auth_component(AuthFalcon512Rpo::new(key_f1.public_key().to_commitment()))
        .with_component(
            BasicFungibleFaucet::new(
                TokenSymbol::new("USDT").unwrap(),
                8,
                Felt::new(1_000_000_00),
            )
            .unwrap(),
        )
        .build()
        .unwrap();
    client.add_account(&faucet_usdt, false).await?;
    keystore.add_key(&key_f1).unwrap();
    println!("  USDT faucet: {}", faucet_usdt.id().to_hex());

    // Faucet 2: ETH
    client.rng().fill_bytes(&mut seed);
    let key_f2 = AuthSecretKey::new_falcon512_rpo();
    let faucet_eth = AccountBuilder::new(seed)
        .account_type(AccountType::FungibleFaucet)
        .storage_mode(AccountStorageMode::Public)
        .with_auth_component(AuthFalcon512Rpo::new(key_f2.public_key().to_commitment()))
        .with_component(
            BasicFungibleFaucet::new(TokenSymbol::new("ETH").unwrap(), 8, Felt::new(1_000_000_00))
                .unwrap(),
        )
        .build()
        .unwrap();
    client.add_account(&faucet_eth, false).await?;
    keystore.add_key(&key_f2).unwrap();
    println!("  ETH  faucet: {}", faucet_eth.id().to_hex());

    client.sync_state().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Step 3: Create Alice and Bob ───────────────────────────────
    println!("\n[3/5] Creating accounts...");

    // Alice
    client.rng().fill_bytes(&mut seed);
    let key_alice = AuthSecretKey::new_falcon512_rpo();
    let alice = AccountBuilder::new(seed)
        .account_type(AccountType::RegularAccountUpdatableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_auth_component(AuthFalcon512Rpo::new(
            key_alice.public_key().to_commitment(),
        ))
        .with_component(BasicWallet)
        .build()
        .unwrap();
    client.add_account(&alice, false).await?;
    keystore.add_key(&key_alice).unwrap();
    println!("  Alice: {}", alice.id().to_hex());

    // Bob
    client.rng().fill_bytes(&mut seed);
    let key_bob = AuthSecretKey::new_falcon512_rpo();
    let bob = AccountBuilder::new(seed)
        .account_type(AccountType::RegularAccountUpdatableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_auth_component(AuthFalcon512Rpo::new(key_bob.public_key().to_commitment()))
        .with_component(BasicWallet)
        .build()
        .unwrap();
    client.add_account(&bob, false).await?;
    keystore.add_key(&key_bob).unwrap();
    println!("  Bob:   {}", bob.id().to_hex());

    client.sync_state().await?;

    // ── Step 4: Mint tokens ────────────────────────────────────────
    println!("\n[4/5] Minting tokens...");

    // Mint 1000 USDT to Alice
    let mint_usdt = 100000000u64;
    let usdt_asset = FungibleAsset::new(faucet_usdt.id(), mint_usdt).unwrap();
    let mint_req = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(usdt_asset, alice.id(), NoteType::Public, client.rng())
        .unwrap();
    let tx = client
        .submit_new_transaction(faucet_usdt.id(), mint_req)
        .await?;
    println!("  Minted {} USDT to Alice. TX: {:?}", mint_usdt, tx);

    // Mint 1000 ETH to Bob
    let mint_eth = 100000000u64;
    let eth_asset = FungibleAsset::new(faucet_eth.id(), mint_eth).unwrap();
    let mint_req = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(eth_asset, bob.id(), NoteType::Public, client.rng())
        .unwrap();
    let tx = client
        .submit_new_transaction(faucet_eth.id(), mint_req)
        .await?;
    println!("  Minted {} ETH to Bob. TX: {:?}", mint_eth, tx);

    client.sync_state().await?;

    // ── Step 5: Consume minted notes ───────────────────────────────
    println!("\n[5/5] Consuming minted notes...");

    // Alice consumes USDT
    consume_notes_loop(&mut client, alice.id(), "Alice").await?;

    // Bob consumes ETH
    consume_notes_loop(&mut client, bob.id(), "Bob").await?;

    client.sync_state().await?;
    println!("Waiting for confirmations...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    client.sync_state().await?;

    // ── Save state ─────────────────────────────────────────────────
    let state = TestState::new(faucet_usdt.id(), faucet_eth.id(), alice.id(), bob.id());
    state.save()?;

    println!("\n=== Setup Complete ===");
    println!("Run `cargo run --bin swap_activity` to start creating swap notes.");
    println!("Run `cargo run --bin solver_runner` to start the solver.");

    Ok(())
}

async fn consume_notes_loop(
    client: &mut miden_client::Client<miden_client::keystore::FilesystemKeyStore>,
    account_id: miden_protocol::account::AccountId,
    name: &str,
) -> Result<()> {
    loop {
        client.sync_state().await?;
        let consumable = client.get_consumable_notes(Some(account_id)).await?;
        let notes: Vec<_> = consumable
            .into_iter()
            .filter_map(|(record, _)| {
                let note: Result<Note, _> = record.try_into();
                note.ok().map(|n| (n, None))
            })
            .collect();

        if !notes.is_empty() {
            println!("  {} consuming {} note(s)", name, notes.len());
            let req = TransactionRequestBuilder::new()
                .input_notes(notes)
                .build()
                .unwrap();
            let tx = client
                .submit_new_transaction(account_id, req)
                .await
                .with_context(|| format!("Failed to consume notes for {}", name))?;
            println!("  {} consumed notes. TX: {:?}", name, tx);
            return Ok(());
        }
        println!("  Waiting for {}'s notes...", name);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

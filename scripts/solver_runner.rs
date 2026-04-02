#[path = "testnet_state.rs"]
mod testnet_state;

use anyhow::Result;
use miden_client::account::component::BasicWallet;
use miden_client::auth::AuthSecretKey;
use miden_protocol::account::{AccountBuilder, AccountStorageMode, AccountType};
use miden_standards::account::auth::AuthFalcon512Rpo;
use miden_client::account::component::BasicWallet as SwapWallet;
use rand::RngCore;
use testnet_state::{build_testnet_client, TestState};
use tokio::sync::broadcast;

use solver::events::SolverEvent;
use solver::order::AssetPair;
use solver_bin::ws_server;

const SOLVER_KEYSTORE: &str = "./solver_keystore";
const SOLVER_STORE: &str = "./solver_store.sqlite3";
const FETCH_INTERVAL_MS: u64 = 3000;
const PULSE_INTERVAL_MS: u64 = 5000;
const WS_PORT: u16 = 3001;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Solver Runner ===\n");

    // Load test state for faucet IDs
    let state = TestState::load()?;
    let faucet_usdt = state.faucet_usdt()?;
    let faucet_eth = state.faucet_eth()?;

    println!("USDT faucet: {}", state.faucet_usdt_id);
    println!("ETH  faucet: {}", state.faucet_eth_id);

    // ── Create solver client (separate store/keystore) ─────────────
    println!("\n[1/3] Initializing solver client...");
    let (mut client, keystore) = build_testnet_client(SOLVER_KEYSTORE, SOLVER_STORE).await?;
    let sync = client.sync_state().await?;
    println!("Connected. Latest block: {}", sync.block_num);

    // ── Create solver account ──────────────────────────────────────
    println!("\n[2/3] Creating solver account...");
    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);

    let key_solver = AuthSecretKey::new_falcon512_rpo();
    let solver_account = AccountBuilder::new(seed)
        .account_type(AccountType::RegularAccountUpdatableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_auth_component(AuthFalcon512Rpo::new(
            key_solver.public_key().to_commitment(),
        ))
        .with_component(SwapWallet::component())
        .with_component(BasicWallet)
        .build()
        .unwrap();

    client.add_account(&solver_account, false).await?;
    keystore.add_key(&key_solver).unwrap();
    let solver_id = solver_account.id();
    println!("Solver account: {}", solver_id.to_hex());

    client.sync_state().await?;

    // ── Start WebSocket server ────────────────────────────────────
    println!("\n[3/3] Starting WebSocket server...");
    let (event_tx, _) = broadcast::channel::<SolverEvent>(256);
    let router = ws_server::build_router(event_tx.clone());
    let addr = format!("0.0.0.0:{}", WS_PORT);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("WebSocket server listening on ws://0.0.0.0:{}/ws", WS_PORT);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("WebSocket server error: {}", e);
        }
    });

    // ── Start solver ────────────────────────────────────────────────
    let pair = AssetPair::new(faucet_usdt, faucet_eth);

    println!("\nSolver started. Monitoring USDT/ETH pair.");
    println!(
        "Fetch every {}ms, pulse every {}ms\n",
        FETCH_INTERVAL_MS, PULSE_INTERVAL_MS
    );

    solver::start(
        &mut client,
        solver_id,
        vec![pair],
        FETCH_INTERVAL_MS,
        PULSE_INTERVAL_MS,
        Some(event_tx),
    )
    .await
}

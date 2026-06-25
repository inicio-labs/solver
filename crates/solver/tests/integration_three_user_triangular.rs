//! Integration test: three users, three tokens, triangular (3-edge cycle) matching.
//!
//! Scenario:
//!   * Alice offers 10 USDC, wants 10 ETH    (edge USDC → ETH)
//!   * Bob   offers 10 ETH,  wants 10 SOL    (edge ETH  → SOL)
//!   * Charlie offers 11 SOL, wants 10 USDC  (edge SOL  → USDC)
//!
//! Profitability:
//!   offered_product   = 10 * 10 * 11 = 1100
//!   requested_product = 10 * 10 * 10 = 1000
//!   1100 > 1000 ⇒ profitable cycle.
//!
//! No pair has a reciprocal counter-order, so direct (pairwise) matching can't
//! clear any of these orders. Only the 3-edge cycle matcher can — making this
//! the right scenario to exercise the `triangular_enabled` toggle.
//!
//! Two sub-tests share the setup pattern but run independently:
//!   1. `triangular_disabled_yields_no_matches` — `triangular_enabled = false`,
//!      expects no execution txs to land on chain.
//!   2. `triangular_enabled_clears_cycle` — `triangular_enabled = true`,
//!      expects 3 paybacks to land and the solver to keep the 1 SOL surplus.

mod common;

use std::sync::Arc;

use anyhow::Result;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{
    insert_new_fungible_faucet, insert_new_wallet, mint_and_consume,
};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_client::Client;
use miden_client::keystore::FilesystemKeyStore;
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{
    AssetPairConfig, EngineConfig, RpcConfig, SolverAccountConfig, SolverConfig,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, temp_paths, vault_balance, wait_for_realtime, MockClientFactory,
};

// L2: solver clients run on their own OS-thread runtimes; tests run on real
// time and poll observable chain state instead of stepping virtual time.

/// Shared scaffold for both sub-tests. Returns the populated user client,
/// faucets, user accounts, and the rpc + solver-side temp dir. After this
/// returns, the chain has block 4 with the 3 PSWAP notes committed and the
/// user client has NOT been synced past block 3 (so the solver sees them
/// fresh on its first sync).
async fn setup_chain_with_three_pswaps() -> Result<TriangularSetup> {
    let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

    // User Client (test driver): faucets + alice/bob/charlie.
    let (user_temp, user_keystore_path, user_store_path) = temp_paths()?;
    let mut user_client =
        build_test_client(rpc.clone(), user_keystore_path.clone(), user_store_path).await?;
    user_client
        .ensure_genesis_in_place()
        .await
        .map_err(|e| anyhow::anyhow!("user genesis: {e}"))?;
    let user_keystore = FilesystemKeyStore::new(user_keystore_path.clone())
        .map_err(|e| anyhow::anyhow!("user FilesystemKeyStore::new: {e}"))?;

    let scheme = AuthSchemeId::Falcon512Poseidon2;
    let mode = AccountType::Public;

    let (usdc, _) = insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
    let (eth, _) = insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
    let (sol, _) = insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;

    let (alice, _) = insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
    let (bob, _) = insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
    let (charlie, _) = insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;

    // Fund: alice/charlie need offer-side tokens; bob needs ETH.
    mint_and_consume(&mut user_client, alice.id(), usdc.id(), NoteType::Public).await;
    rpc.prove_block();
    user_client
        .sync_state()
        .await
        .map_err(|e| anyhow::anyhow!("sync after alice mint: {e}"))?;

    mint_and_consume(&mut user_client, bob.id(), eth.id(), NoteType::Public).await;
    rpc.prove_block();
    user_client
        .sync_state()
        .await
        .map_err(|e| anyhow::anyhow!("sync after bob mint: {e}"))?;

    mint_and_consume(&mut user_client, charlie.id(), sol.id(), NoteType::Public).await;
    rpc.prove_block();
    user_client
        .sync_state()
        .await
        .map_err(|e| anyhow::anyhow!("sync after charlie mint: {e}"))?;

    // 3 PSWAP-creation txs forming the cycle USDC → ETH → SOL → USDC.
    submit_pswap(
        &mut user_client,
        alice.id(),
        FungibleAsset::new(usdc.id(), 10)?,
        FungibleAsset::new(eth.id(), 10)?,
    )
    .await?;
    submit_pswap(
        &mut user_client,
        bob.id(),
        FungibleAsset::new(eth.id(), 10)?,
        FungibleAsset::new(sol.id(), 10)?,
    )
    .await?;
    submit_pswap(
        &mut user_client,
        charlie.id(),
        FungibleAsset::new(sol.id(), 11)?,
        FungibleAsset::new(usdc.id(), 10)?,
    )
    .await?;
    rpc.prove_block();
    // No user_client.sync_state here — let the solver see the PSWAPs first.

    Ok(TriangularSetup {
        rpc,
        _user_client: user_client,
        _user_temp: user_temp,
        usdc_id: usdc.id(),
        eth_id: eth.id(),
        sol_id: sol.id(),
        alice_id: alice.id(),
        bob_id: bob.id(),
        charlie_id: charlie.id(),
    })
}

async fn submit_pswap(
    client: &mut Client<FilesystemKeyStore>,
    creator: AccountId,
    offered: FungibleAsset,
    requested: FungibleAsset,
) -> Result<()> {
    let request = TransactionRequestBuilder::new()
        .build_pswap_create(
            &PswapTransactionData::new(creator, offered, requested),
            NoteType::Public,
            NoteType::Public,
            None,
            client.rng(),
        )
        .map_err(|e| anyhow::anyhow!("build_pswap_create: {e}"))?;
    Box::pin(client.submit_new_transaction(creator, request))
        .await
        .map_err(|e| anyhow::anyhow!("submit pswap: {e}"))?;
    Ok(())
}

struct TriangularSetup {
    rpc: Arc<MockRpcApi>,
    _user_client: Client<FilesystemKeyStore>,
    _user_temp: TempDir,
    usdc_id: AccountId,
    eth_id: AccountId,
    sol_id: AccountId,
    alice_id: AccountId,
    bob_id: AccountId,
    charlie_id: AccountId,
}

fn build_solver_config(
    solver_account_id: AccountId,
    solver_temp: &TempDir,
    solver_keystore_path: &std::path::Path,
    usdc_id: AccountId,
    eth_id: AccountId,
    sol_id: AccountId,
    triangular_enabled: bool,
) -> SolverConfig {
    let solver_db = solver_temp.path().join("solver.sqlite3");
    SolverConfig {
        rpc: RpcConfig {
            endpoint: "http://unused".into(),
            timeout_ms: 1_000,
            prover_endpoint: None,
        },
        solver: SolverAccountConfig {
            account_id: solver_account_id.to_hex(),
            keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
            app_db_path: solver_db.to_string_lossy().into_owned(),
            executor_store_path: solver_temp
                .path()
                .join("executor_store.sqlite3")
                .to_string_lossy()
                .into_owned(),
            ingest_store_path: solver_temp
                .path()
                .join("ingest_store.sqlite3")
                .to_string_lossy()
                .into_owned(),
            read_pool_size: 2,
        },
        // Three pairs so each token gets registered for tag subscription.
        pairs: vec![
            AssetPairConfig {
                name: "USDC-ETH".into(),
                asset_x_faucet_id: usdc_id.to_hex(),
                asset_x_external_symbol: None,
                asset_y_faucet_id: eth_id.to_hex(),
                asset_y_external_symbol: None,
            },
            AssetPairConfig {
                name: "ETH-SOL".into(),
                asset_x_faucet_id: eth_id.to_hex(),
                asset_x_external_symbol: None,
                asset_y_faucet_id: sol_id.to_hex(),
                asset_y_external_symbol: None,
            },
            AssetPairConfig {
                name: "SOL-USDC".into(),
                asset_x_faucet_id: sol_id.to_hex(),
                asset_x_external_symbol: None,
                asset_y_faucet_id: usdc_id.to_hex(),
                asset_y_external_symbol: None,
            },
        ],
        engine: EngineConfig {
            pulse_interval_ms: 200,
            fetch_interval_ms: 100,
            price_interval_ms: 60_000,
            triangular_enabled,
            admin_port: 0,
            debug_mode: false,
            obs_port: 0,
            readiness_freshness_secs: 60,
            price_api_base_url: None,
            price_query_port: 8080,
            price_query_bind: "127.0.0.1".to_string(),
            price_query_max_inflight: 128,
            price_query_max_batch: 50,
            price_query_timeout_ms: 3000,
            price_precision: "full".to_string(),
            price_vs_currency: "usd".to_string(),
            price_staleness_secs: 30,
            router_enabled: false,
            router_bind: "127.0.0.1".to_string(),
            router_port: 0,
            router_max_connections: 64,
            router_max_msg_bytes: 16384,
            router_quote_ttl_ms: 20_000,
            router_inflight_ttl_ms: 30_000,
            router_min_export_edge_bps: 50,
            router_quote_max_deviation_bps: 200,
        },
    }
}

/// Provision the solver wallet on disk via a throwaway executor client (the
/// same store + keystore the L2 factory will reload from), then drop it.
/// Mirrors a production operator provisioning the account before start.
async fn provision_solver(
    rpc: &Arc<MockRpcApi>,
    solver_keystore_path: &std::path::Path,
    solver_store_path: &std::path::Path,
) -> Result<AccountId> {
    let mut c = build_test_client(
        rpc.clone(),
        solver_keystore_path.to_path_buf(),
        solver_store_path.to_path_buf(),
    )
    .await?;
    c.ensure_genesis_in_place()
        .await
        .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
    let ks = FilesystemKeyStore::new(solver_keystore_path.to_path_buf())
        .map_err(|e| anyhow::anyhow!("solver FilesystemKeyStore::new: {e}"))?;
    let (acct, _) = insert_new_wallet(
        &mut c,
        AccountType::Public,
        &ks,
        AuthSchemeId::Falcon512Poseidon2,
    )
    .await?;
    Ok(acct.id())
}

#[tokio::test]
async fn triangular_enabled_clears_cycle() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let setup = setup_chain_with_three_pswaps().await?;
            let rpc = setup.rpc.clone();
            let initial_committed = rpc.mock_chain.read().committed_notes().len();

            // Solver account provisioned on disk; the L2 factory rebuilds the
            // ingest + executor clients on their own threads from these paths.
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id =
                provision_solver(&rpc, &solver_keystore_path, &solver_store_path).await?;
            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store: solver_ingest_store,
                executor_store: solver_store_path.clone(),
                keystore: solver_keystore_path.clone(),
            });

            let config = build_solver_config(
                solver_id,
                &solver_temp,
                &solver_keystore_path,
                setup.usdc_id,
                setup.eth_id,
                setup.sol_id,
                /* triangular_enabled */ true,
            );

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            // Inject prices (CoinGecko unreachable in tests). Uniform 100¢ for
            // all three legs reproduces the relative valuation the cycle math
            // was validated under (profitability is scale-invariant under a
            // uniform price; the SOL-denominated surplus is unchanged).
            let price_map: std::collections::HashMap<_, u64> = [
                (setup.usdc_id, 100),
                (setup.eth_id, 100),
                (setup.sol_id, 100),
            ]
            .into_iter()
            .collect();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(
                    factory,
                    move |_sm, _key| {
                        Ok(Box::new(solver::price::MockPriceClient::new(price_map))
                            as Box<dyn solver::price::PriceClient + Send + Sync>)
                    },
                    solver_id,
                    config,
                    solver_cancel,
                )
                .await
            });

            let sol_id = setup.sol_id;
            let wait_result = wait_for_realtime(
                &rpc,
                2000,
                std::time::Duration::from_millis(100),
                |chain| {
                    vault_balance(chain, solver_id, sol_id) >= 1
                        && chain.committed_notes().len() >= initial_committed + 3
                },
            )
            .await;

            // Verdict without `?`/`assert!` so cleanup always runs (an early
            // return/panic would orphan the ingest + executor OS threads).
            let verdict: Result<()> = (|| {
                wait_result?;
                let chain_ro = rpc.mock_chain.read();
                let sol = vault_balance(&chain_ro, solver_id, sol_id);
                if sol != 1 {
                    anyhow::bail!("solver should keep 1 SOL surplus (11−10), got {sol}");
                }
                let grown = chain_ro.committed_notes().len() - initial_committed;
                if grown < 3 {
                    anyhow::bail!("expected ≥3 new payback notes, got {grown}");
                }
                Ok(())
            })();

            if let Err(e) = &verdict {
                let chain_ro = rpc.mock_chain.read();
                println!(
                    "[test] FAILED: {e}\n[test] block_num={} solver_sol={} committed={} (was {})",
                    chain_ro.latest_block_header().block_num().as_u64(),
                    vault_balance(&chain_ro, solver_id, sol_id),
                    chain_ro.committed_notes().len(),
                    initial_committed,
                );
            }

            cancel.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                .await;
            drop(solver_temp);
            // Reference setup's _user_temp and _user_client to keep them alive
            // for the test's duration — they go out of scope here.
            let _ = setup;
            verdict
        })
        .await
}

#[tokio::test]
async fn triangular_disabled_yields_no_matches() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let setup = setup_chain_with_three_pswaps().await?;
            let rpc = setup.rpc.clone();
            let initial_committed = rpc.mock_chain.read().committed_notes().len();

            // Solver account provisioned on disk; L2 factory rebuilds clients
            // on their own threads from these paths.
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id =
                provision_solver(&rpc, &solver_keystore_path, &solver_store_path).await?;
            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store: solver_ingest_store,
                executor_store: solver_store_path.clone(),
                keystore: solver_keystore_path.clone(),
            });

            let config = build_solver_config(
                solver_id,
                &solver_temp,
                &solver_keystore_path,
                setup.usdc_id,
                setup.eth_id,
                setup.sol_id,
                /* triangular_enabled */ false,
            );

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            // Inject prices (CoinGecko unreachable in tests). Uniform 100¢ for
            // all three legs reproduces the relative valuation the cycle math
            // was validated under (profitability is scale-invariant under a
            // uniform price; the SOL-denominated surplus is unchanged).
            let price_map: std::collections::HashMap<_, u64> = [
                (setup.usdc_id, 100),
                (setup.eth_id, 100),
                (setup.sol_id, 100),
            ]
            .into_iter()
            .collect();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(
                    factory,
                    move |_sm, _key| {
                        Ok(Box::new(solver::price::MockPriceClient::new(price_map))
                            as Box<dyn solver::price::PriceClient + Send + Sync>)
                    },
                    solver_id,
                    config,
                    solver_cancel,
                )
                .await
            });

            // Real-time drive: give the pipeline ample wall-clock time to
            // ingest the 3 PSWAPs and run many matcher pulses. With triangular
            // disabled and no pairwise reciprocal counter-orders, the matcher
            // must never emit a batch — committed_notes stays flat.
            for _ in 0..80 {
                rpc.prove_block();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            // Verdict without `assert!` so cleanup always runs (a panicking
            // assert would orphan the ingest + executor OS threads).
            let verdict: Result<()> = {
                let chain_ro = rpc.mock_chain.read();
                let grown = chain_ro.committed_notes().len() - initial_committed;
                let alice_eth = vault_balance(&chain_ro, setup.alice_id, setup.eth_id);
                let bob_sol = vault_balance(&chain_ro, setup.bob_id, setup.sol_id);
                let charlie_usdc = vault_balance(&chain_ro, setup.charlie_id, setup.usdc_id);
                drop(chain_ro);
                if grown != 0 {
                    Err(anyhow::anyhow!(
                        "no matches expected (triangular disabled, no pairwise counter-orders); \
                         committed_notes grew by {grown}"
                    ))
                } else if alice_eth != 0 || bob_sol != 0 || charlie_usdc != 0 {
                    Err(anyhow::anyhow!(
                        "no payouts expected; alice_eth={alice_eth} bob_sol={bob_sol} \
                         charlie_usdc={charlie_usdc}"
                    ))
                } else {
                    Ok(())
                }
            };

            cancel.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                .await;
            drop(solver_temp);
            let _ = setup;
            verdict
        })
        .await
}

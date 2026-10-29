//! Bounded clearing price (ADR 0007) end to end on the mock chain.
//!
//! One pair, USDC/ETH, priced by Binance at 1. Bob offers 998 ETH for
//! 1,000 USDC: an ask 20 bps above the mid, so at the mid he is never
//! eligible. Alice offers 1,000 USDC for 990 ETH, a wide limit.
//!
//! Phase 1, `band_bps = 0`: the solver runs on the book and settles nothing,
//! exactly as before this feature.
//!
//! Phase 2, `band_bps = 30`: the solver restarts with the band. The walk
//! prices the batch at Bob's limit (0.998 ETH per USDC, 20 bps below the mid
//! in base terms), both orders fill fully, and the settlement commits on
//! chain: Alice's payback is 998 ETH (more than the 990 she asked, since
//! every full fill pays the batch price) and Bob's is his 1,000 USDC.

mod common;

use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, count_orders, temp_paths, vault_balance, wait_for_submitted_settlement,
    MockClientFactory, PgSchema,
};

/// Fungible amounts of `faucet` in the chain's committed notes, largest
/// first: the payback P2IDs once a settlement commits.
fn committed_amounts(chain: &MockChain, faucet: AccountId) -> Vec<u64> {
    let mut amounts: Vec<u64> = chain
        .committed_notes()
        .values()
        .filter_map(|note| note.note())
        .flat_map(|note| note.assets().iter().filter_map(|asset| asset.as_fungible()))
        .filter(|asset| asset.faucet_id() == faucet)
        .map(|asset| asset.amount().as_u64())
        .collect();
    amounts.sort_unstable_by(|a, b| b.cmp(a));
    amounts
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_band_settles_at_the_makers_ask_where_the_mid_never_trades() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let pg = PgSchema::new().await?;
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            let (user_temp, user_keystore_path, user_store_path) = temp_paths()?;
            let mut user_client = TestClient::new(
                build_test_client(rpc.clone(), user_keystore_path.clone(), user_store_path).await?,
            );
            user_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow::anyhow!("user genesis: {e}"))?;
            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let mode = AccountType::Public;
            let (usdc, _) = user_client
                .insert_account(AccountSetup::faucet(mode).auth_scheme(scheme))
                .await?;
            let (eth, _) = user_client
                .insert_account(AccountSetup::faucet(mode).auth_scheme(scheme))
                .await?;
            let (alice, _) = user_client
                .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                .await?;
            let (bob, _) = user_client
                .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                .await?;
            rpc.prove_block();
            user_client.sync_state().await?;

            // 1,000 of each: Alice holds USDC, Bob holds ETH.
            user_client
                .mint_and_consume(alice.id(), usdc.id(), NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("user sync after alice mint: {e}"))?;
            user_client
                .mint_and_consume(bob.id(), eth.id(), NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("user sync after bob mint: {e}"))?;

            // Alice sells 1,000 USDC for at least 990 ETH (0.99 ETH per USDC).
            // Bob buys USDC with 998 ETH, wanting 1,000 USDC (0.998 ETH per
            // USDC): 20 bps above the mid, which is 1.
            let alice_request = TransactionRequestBuilder::new()
                .build_pswap_create(
                    &PswapTransactionData::new(
                        alice.id(),
                        FungibleAsset::new(usdc.id(), 1_000)?,
                        FungibleAsset::new(eth.id(), 990)?,
                    ),
                    NoteType::Public,
                    NoteType::Public,
                    None,
                    user_client.rng(),
                )
                .map_err(|e| anyhow::anyhow!("alice build_pswap_create: {e}"))?;
            Box::pin(user_client.submit_new_transaction(alice.id(), alice_request))
                .await
                .map_err(|e| anyhow::anyhow!("alice submit pswap: {e}"))?;
            let bob_request = TransactionRequestBuilder::new()
                .build_pswap_create(
                    &PswapTransactionData::new(
                        bob.id(),
                        FungibleAsset::new(eth.id(), 998)?,
                        FungibleAsset::new(usdc.id(), 1_000)?,
                    ),
                    NoteType::Public,
                    NoteType::Public,
                    None,
                    user_client.rng(),
                )
                .map_err(|e| anyhow::anyhow!("bob build_pswap_create: {e}"))?;
            Box::pin(user_client.submit_new_transaction(bob.id(), bob_request))
                .await
                .map_err(|e| anyhow::anyhow!("bob submit pswap: {e}"))?;
            rpc.prove_block();

            // The solver account, provisioned by a throwaway client on the
            // stores the factory reuses.
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id = {
                let mut solver_client = TestClient::new(
                    build_test_client(
                        rpc.clone(),
                        solver_keystore_path.clone(),
                        solver_store_path.clone(),
                    )
                    .await?,
                );
                solver_client
                    .ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
                let (solver_account, _) = solver_client
                    .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                    .await?;
                solver_account.id()
            };
            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let executor_store_path = solver_store_path.to_string_lossy().into_owned();
            let ingest_store_path = solver_ingest_store.to_string_lossy().into_owned();
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store: solver_ingest_store,
                executor_store: solver_store_path,
                keystore: solver_keystore_path.clone(),
            });

            // ETHUSDC at 1 prices the USDC/ETH pair (USDC base) at 1 ETH per
            // USDC; the other two symbols value the tokens at startup.
            let binance = common::BinanceStub::start(&[
                ("ETHUSDC", "ETH", "USDC", "1"),
                ("ETHUSDT", "ETH", "USDT", "1"),
                ("USDCUSDT", "USDC", "USDT", "1"),
            ]);
            let config = |band_bps: u16| {
                let mut pair =
                    common::pair_config("USDC-ETH", usdc.id(), "USDC", eth.id(), "ETH", "ETHUSDC");
                pair.band_bps = band_bps;
                pair.reference_amount = Some("10".into());
                SolverConfig {
                    rpc: RpcConfig {
                        endpoint: "http://unused".into(),
                        timeout_ms: 1_000,
                        prover_endpoint: None,
                        prover_timeout_ms: 45_000,
                    },
                    solver: SolverAccountConfig {
                        account_id: solver_id.to_hex(),
                        keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                        executor_store_path: executor_store_path.clone(),
                        ingest_store_path: ingest_store_path.clone(),
                        read_pool_size: 2,
                    },
                    pairs: vec![pair],
                    engine: common::engine_config(),
                    binance: binance.config(),
                }
            };

            let (alice_id, bob_id, eth_id, usdc_id) = (alice.id(), bob.id(), eth.id(), usdc.id());
            let initial_committed = rpc.mock_chain.read().committed_notes().len();

            // Phase 1: no band. Both orders go live and nothing settles, since
            // Bob is not eligible at the mid.
            let cancel = CancellationToken::new();
            let mut solver_handle = {
                let (factory, config, cancel) = (factory.clone(), config(0), cancel.clone());
                tokio::task::spawn_local(async move {
                    solver::start(factory, solver_id, config, cancel).await
                })
            };
            let phase_1: Result<()> = async {
                let started = std::time::Instant::now();
                while count_orders(&pg.url, "active").await < 2 {
                    if solver_handle.is_finished() {
                        anyhow::bail!("solver stopped before both orders went live");
                    }
                    if started.elapsed() > std::time::Duration::from_secs(120) {
                        anyhow::bail!("both orders did not go live within 120 s");
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                // Fifteen matcher ticks at the mid.
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                if count_orders(&pg.url, "active").await != 2 {
                    anyhow::bail!("an order left the book at the mid without a band");
                }
                Ok(())
            }
            .await;
            cancel.cancel();
            let stopped =
                tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle).await;
            phase_1?;
            stopped.context("the solver without a band did not stop")???;

            // Phase 2: a 30 bps band. The same stores and schema, as a restart.
            let cancel = CancellationToken::new();
            let mut solver_handle = {
                let (factory, config, cancel) = (factory.clone(), config(30), cancel.clone());
                tokio::task::spawn_local(async move {
                    solver::start(factory, solver_id, config, cancel).await
                })
            };
            let phase_2: Result<()> = async {
                wait_for_submitted_settlement(&pg.url, std::time::Duration::from_secs(900), || {
                    solver_handle.is_finished()
                })
                .await?;
                rpc.prove_block();
                let chain = rpc.mock_chain.read();
                let grown = chain.committed_notes().len() - initial_committed;
                if grown < 2 {
                    anyhow::bail!("expected 2 payback notes after the settlement, got {grown}");
                }
                // Alice is paid the batch price for her whole 1,000 USDC: 998
                // ETH, not the 990 she asked. Bob gets his 1,000 USDC.
                let eth_paybacks = committed_amounts(&chain, eth_id);
                let usdc_paybacks = committed_amounts(&chain, usdc_id);
                if eth_paybacks.first() != Some(&998) {
                    anyhow::bail!("alice's ETH payback should be 998, saw {eth_paybacks:?}");
                }
                if usdc_paybacks.first() != Some(&1_000) {
                    anyhow::bail!("bob's USDC payback should be 1,000, saw {usdc_paybacks:?}");
                }
                // Both fills are exact: the solver keeps nothing.
                let kept = (
                    vault_balance(&chain, solver_id, usdc_id),
                    vault_balance(&chain, solver_id, eth_id),
                );
                if kept != (0, 0) {
                    anyhow::bail!("the solver should keep no surplus, holds {kept:?}");
                }
                Ok(())
            }
            .await;

            if let Err(e) = &phase_2 {
                let chain = rpc.mock_chain.read();
                println!(
                    "[test] FAILED: {e}\n[test] block_num={} alice_usdc={} bob_eth={} \
                     solver_usdc={} solver_eth={} committed={} (was {initial_committed})",
                    chain.latest_block_header().block_num().as_u64(),
                    vault_balance(&chain, alice_id, usdc_id),
                    vault_balance(&chain, bob_id, eth_id),
                    vault_balance(&chain, solver_id, usdc_id),
                    vault_balance(&chain, solver_id, eth_id),
                    chain.committed_notes().len(),
                );
            }
            cancel.cancel();
            if !solver_handle.is_finished() {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                        .await;
            }
            drop(user_temp);
            drop(solver_temp);
            phase_2
        })
        .await
}

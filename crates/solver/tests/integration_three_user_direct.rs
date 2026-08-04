//! Integration test: three users, direct matching, full pipeline.
//!
//! Architecture: two miden-client `Client`s sharing one `MockRpcApi`.
//!   * "User" Client holds Alice/Bob/Charlie wallets + USDC/ETH faucets +
//!     their Falcon keys. The test driver uses it to mint balances and submit
//!     PSWAP-creation txs via `TransactionRequestBuilder::build_pswap_create`.
//!   * "Solver" Client holds only the solver wallet. `solver::start` consumes
//!     this Client.
//!
//! Why two Clients: with one Client that owns the creators, the Miden note
//! screener marks PSWAP discoveries as "already tracked as output note" and
//! routes them to `summary.committed_notes` instead of `new_public_notes`.
//! The solver's ingest adapter consumes the latter, so it sees zero notes.
//! Splitting the Clients matches the production topology (solver doesn't run
//! the users' accounts) and the PSWAPs flow through `new_public_notes` as
//! tag-discovered public notes.

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
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{
    AssetPairConfig, EngineConfig, RpcConfig, SolverAccountConfig, SolverConfig,
};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, temp_paths, vault_balance, wait_for_realtime, MockClientFactory,
};

// L2: the solver's ingest/executor clients run on their own OS-thread runtimes,
// so the test's virtual clock can't drive them — this test runs on real time
// and polls observable chain state (`wait_for_realtime`) instead of stepping
// `tokio::time::advance`.
#[tokio::test]
async fn three_user_direct_matching() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            // 1. Empty MockChain wrapped in MockRpcApi, shared by both Clients.
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // 2. USER Client: owns alice/bob/charlie + USDC/ETH faucets.
            let (user_temp, user_keystore_path, user_store_path) = temp_paths()?;
            let mut user_client =
                build_test_client(rpc.clone(), user_keystore_path.clone(), user_store_path)
                    .await?;
            user_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow::anyhow!("user genesis: {e}"))?;
            let user_keystore =
                miden_client::keystore::FilesystemKeyStore::new(user_keystore_path.clone())
                    .map_err(|e| anyhow::anyhow!("user FilesystemKeyStore::new: {e}"))?;

            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let mode = AccountType::Public;

            let (usdc, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (eth, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (alice, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (bob, _) = insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (charlie, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;

            println!(
                "[test] usdc={}, eth={}, alice={}, bob={}, charlie={}",
                usdc.id().to_hex(),
                eth.id().to_hex(),
                alice.id().to_hex(),
                bob.id().to_hex(),
                charlie.id().to_hex(),
            );

            // 3. Fund users via mint+consume → prove → sync. Each round commits
            //    one block; sync_state pulls the new state into the user Client.
            mint_and_consume(&mut user_client, alice.id(), usdc.id(), NoteType::Public).await;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("user sync after alice mint: {e}"))?;

            mint_and_consume(&mut user_client, bob.id(), eth.id(), NoteType::Public).await;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("user sync after bob mint: {e}"))?;

            mint_and_consume(&mut user_client, charlie.id(), usdc.id(), NoteType::Public).await;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("user sync after charlie mint: {e}"))?;

            // 4. Each user submits a PSWAP-creation tx via build_pswap_create.
            //    Scenario: alice + bob form a profitable pair with 20 USDC surplus
            //    to the solver. Charlie is intentionally orphaned (no remaining
            //    ETH-offerer once bob is consumed), since the current matcher's
            //    integer-rounded fill math can't split bob's order across two
            //    USDC-side counterparties. A full 3-of-3 cycle is the triangular
            //    test's job, not this one.
            let alice_request = TransactionRequestBuilder::new()
                .build_pswap_create(
                    &PswapTransactionData::new(
                        alice.id(),
                        FungibleAsset::new(usdc.id(), 120)?,
                        FungibleAsset::new(eth.id(), 1)?,
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
                        FungibleAsset::new(eth.id(), 1)?,
                        FungibleAsset::new(usdc.id(), 100)?,
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

            let charlie_request = TransactionRequestBuilder::new()
                .build_pswap_create(
                    &PswapTransactionData::new(
                        charlie.id(),
                        FungibleAsset::new(usdc.id(), 100)?,
                        FungibleAsset::new(eth.id(), 1)?,
                    ),
                    NoteType::Public,
                    NoteType::Public,
                    None,
                    user_client.rng(),
                )
                .map_err(|e| anyhow::anyhow!("charlie build_pswap_create: {e}"))?;
            Box::pin(user_client.submit_new_transaction(charlie.id(), charlie_request))
                .await
                .map_err(|e| anyhow::anyhow!("charlie submit pswap: {e}"))?;

            rpc.prove_block();

            {
                let chain_ro = rpc.mock_chain.read();
                println!(
                    "[test] post-prove block_num={}, committed_notes={}",
                    chain_ro.latest_block_header().block_num().as_u64(),
                    chain_ro.committed_notes().len(),
                );
                for note in chain_ro.committed_notes().values() {
                    println!(
                        "[test]   note id={} tag={:?} block={}",
                        note.id(),
                        note.metadata().tag(),
                        note.inclusion_proof().location().block_num().as_u64(),
                    );
                }
            }

            // 5. SOLVER account provisioning. At L2 the executor client is
            //    built on its own thread by the factory, so we can't hand it a
            //    pre-built client. Instead a *throwaway* executor client (same
            //    store + keystore paths the factory will use) creates the
            //    solver wallet on disk, then is dropped — exactly the
            //    production model where the operator provisions the account
            //    and the solver process reloads it on start.
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id = {
                let mut solver_client = build_test_client(
                    rpc.clone(),
                    solver_keystore_path.clone(),
                    solver_store_path.clone(),
                )
                .await?;
                solver_client
                    .ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
                let solver_keystore =
                    miden_client::keystore::FilesystemKeyStore::new(solver_keystore_path.clone())
                        .map_err(|e| anyhow::anyhow!("solver FilesystemKeyStore::new: {e}"))?;
                let (solver_account, _) =
                    insert_new_wallet(&mut solver_client, mode, &solver_keystore, scheme).await?;
                solver_account.id()
                // solver_client dropped here: account + key persisted to disk.
            };
            println!("[test] solver={}", solver_id.to_hex());

            // L2 factory: builds the keyless ingest client + the keystore
            // executor client on their own threads, all against this same
            // shared MockRpcApi (one mock chain).
            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let executor_store_path = solver_store_path.to_string_lossy().into_owned();
            let ingest_store_path = solver_ingest_store.to_string_lossy().into_owned();
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store: solver_ingest_store,
                executor_store: solver_store_path,
                keystore: solver_keystore_path.clone(),
            });

            // 6. SolverConfig pointing at a per-test SQLite path.
            let solver_db = solver_temp.path().join("solver.sqlite3");
            let config = SolverConfig {
                rpc: RpcConfig {
                    endpoint: "http://unused".into(),
                    timeout_ms: 1_000,
                    prover_endpoint: None,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    app_db_path: solver_db.to_string_lossy().into_owned(),
                    executor_store_path,
                    ingest_store_path,
                    read_pool_size: 2,
                },
                pairs: vec![AssetPairConfig {
                    name: "USDC-ETH".into(),
                    asset_x_faucet_id: usdc.id().to_hex(),
                    asset_x_external_symbol: None,
                    asset_y_faucet_id: eth.id().to_hex(),
                    asset_y_external_symbol: None,
                }],
                engine: EngineConfig {
                    pulse_interval_ms: 200,
                    fetch_interval_ms: 100,
                    price_interval_ms: 60_000,
                    triangular_enabled: true,
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
                },
            };

            // 7. Spawn solver::start with the L2 factory. start() spawns the
            //    Send services on this LocalSet and the ingest/executor clients
            //    on their own OS threads.
            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            // Inject prices (CoinGecko is unreachable in tests). Direct
            // matching is rate-based and doesn't gate on USD price, but the
            // solver still needs a working price client.
            let price_map: std::collections::HashMap<_, u64> =
                [(usdc.id(), 100), (eth.id(), 100)].into_iter().collect();
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

            // 8. Wait for the alice↔bob fill to land. We can't observe paybacks
            //    via `vault_balance(alice, eth)` directly — payback notes are
            //    P2IDs that don't credit alice's vault until alice consumes
            //    them in a separate tx (which we don't run here). Instead we
            //    watch for two signals:
            //      a. The solver's USDC vault gets the 20 USDC surplus, which
            //         the `ConsumeAssetScript` deposits directly into the
            //         solver account in the same execution.
            //      b. The chain's committed-notes set grows by ≥2 paybacks.
            let alice_id = alice.id();
            let bob_id = bob.id();
            let charlie_id = charlie.id();
            let eth_id = eth.id();
            let usdc_id = usdc.id();
            let initial_committed_count = rpc.mock_chain.read().committed_notes().len();
            let wait_result = wait_for_realtime(
                &rpc,
                2000,
                std::time::Duration::from_millis(100),
                |chain| {
                    vault_balance(chain, solver_id, usdc_id) >= 20
                        && chain.committed_notes().len() >= initial_committed_count + 2
                },
            )
            .await;

            // Compute the verdict WITHOUT `?`/`assert!` so the cleanup below
            // always runs. An early return / panicking assert here would skip
            // `cancel.cancel()` and orphan the ingest + executor OS threads.
            let verdict: Result<()> = (|| {
                wait_result?;
                let chain_ro = rpc.mock_chain.read();
                let surplus = vault_balance(&chain_ro, solver_id, usdc_id);
                if surplus != 20 {
                    anyhow::bail!(
                        "solver should keep 20 USDC surplus (alice 120 − bob 100), got {surplus}"
                    );
                }
                // alice/bob keep post-mint balances until they consume their
                // paybacks; charlie's PSWAP is orphaned. Chain must grow by ≥2
                // (the two payback P2IDs).
                let grown = chain_ro.committed_notes().len() - initial_committed_count;
                if grown < 2 {
                    anyhow::bail!("expected ≥2 new committed notes (alice+bob paybacks), got {grown}");
                }
                Ok(())
            })();

            if let Err(e) = &verdict {
                let chain_ro = rpc.mock_chain.read();
                println!(
                    "[test] FAILED: {e}\n[test] block_num={} solver_usdc={} alice_usdc={} \
                     bob_eth={} charlie_usdc={} committed={} (was {})",
                    chain_ro.latest_block_header().block_num().as_u64(),
                    vault_balance(&chain_ro, solver_id, usdc_id),
                    vault_balance(&chain_ro, alice_id, usdc_id),
                    vault_balance(&chain_ro, bob_id, eth_id),
                    vault_balance(&chain_ro, charlie_id, usdc_id),
                    chain_ro.committed_notes().len(),
                    initial_committed_count,
                );
            }

            // Always clean up: cancel + bounded join so no OS thread leaks
            // across test cases, regardless of pass/fail.
            cancel.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                .await;
            drop(user_temp);
            drop(solver_temp);
            verdict
        })
        .await
}

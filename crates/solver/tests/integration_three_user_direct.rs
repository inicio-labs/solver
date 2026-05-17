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
use miden_client::note::{NoteAttachment, NoteType};
use miden_client::testing::common::{
    insert_new_fungible_faucet, insert_new_wallet, mint_and_consume,
};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::AccountStorageMode;
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{
    AssetPairConfig, EngineConfig, RpcConfig, SolverAccountConfig, SolverConfig,
};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, build_test_ingest_client, temp_paths, vault_balance, wait_for,
};

#[tokio::test(start_paused = true)]
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
            let mode = AccountStorageMode::Public;

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
                    NoteAttachment::default(),
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
                    NoteAttachment::default(),
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
                    NoteAttachment::default(),
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

            // 5. SOLVER Client: separate store + keystore. Only the solver
            //    account is tracked here — so user-created PSWAPs flow through
            //    `summary.new_public_notes` (tag-discovery path).
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let mut solver_client = build_test_client(
                rpc.clone(),
                solver_keystore_path.clone(),
                solver_store_path,
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
            let solver_id = solver_account.id();
            println!("[test] solver={}", solver_id.to_hex());

            // Keyless INGEST client: same mock chain (rpc), its own store, no
            // keystore/account. This is the chain-watching path; the executor
            // client above is the only one that signs.
            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let mut solver_ingest_client =
                build_test_ingest_client(rpc.clone(), solver_ingest_store).await?;
            solver_ingest_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow::anyhow!("solver ingest genesis: {e}"))?;

            // 6. SolverConfig pointing at a per-test SQLite path.
            let solver_db = solver_temp.path().join("solver.sqlite3");
            let config = SolverConfig {
                rpc: RpcConfig {
                    endpoint: "http://unused".into(),
                    timeout_ms: 1_000,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    store_path: solver_db.to_string_lossy().into_owned(),
                    ingest_store_path: None,
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
                },
            };

            // 7. Spawn solver::start consuming the solver Client.
            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(solver_ingest_client, solver_client, solver_id, config, solver_cancel)
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
            let wait_result = wait_for(&rpc, 120, |chain| {
                vault_balance(chain, solver_id, usdc_id) >= 20
                    && chain.committed_notes().len() >= initial_committed_count + 2
            })
            .await;

            if wait_result.is_err() {
                println!("[test] wait_for FAILED, dumping state...");
                if solver_handle.is_finished() {
                    let r = (&mut solver_handle).await;
                    println!("[test] solver task finished: {:?}", r);
                } else {
                    println!("[test] solver task still running");
                }
                let chain_ro = rpc.mock_chain.read();
                println!(
                    "[test] block_num: {}",
                    chain_ro.latest_block_header().block_num().as_u64()
                );
                println!("[test] solver usdc:   {}", vault_balance(&chain_ro, solver_id, usdc_id));
                println!("[test] alice usdc:    {}", vault_balance(&chain_ro, alice_id, usdc_id));
                println!("[test] bob eth:       {}", vault_balance(&chain_ro, bob_id, eth_id));
                println!("[test] charlie usdc:  {}", vault_balance(&chain_ro, charlie_id, usdc_id));
                println!(
                    "[test] committed_notes: {} (was {})",
                    chain_ro.committed_notes().len(),
                    initial_committed_count,
                );
            }
            wait_result?;

            // 9. Final assertions.
            let chain_ro = rpc.mock_chain.read();
            assert_eq!(
                vault_balance(&chain_ro, solver_id, usdc_id),
                20,
                "solver should keep 20 USDC surplus (alice's 120 offered − bob's 100 requested)",
            );
            // alice and bob's USDC/ETH balances stay at their post-mint values
            // until they consume their payback notes. charlie's PSWAP is
            // orphaned, so his 100 USDC stays locked in his PSWAP note.
            // Chain must have grown by ≥2 (the two payback P2IDs).
            assert!(
                chain_ro.committed_notes().len() >= initial_committed_count + 2,
                "expected ≥2 new committed notes (alice + bob paybacks), got {}",
                chain_ro.committed_notes().len() - initial_committed_count,
            );

            cancel.cancel();
            let _ = solver_handle;
            drop(user_temp);
            drop(solver_temp);
            Ok(())
        })
        .await
}

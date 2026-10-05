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

use anyhow::{Context, Result};
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{AssetPairConfig, EngineConfig, RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, temp_paths, vault_balance, wait_for_submitted_settlement, MockClientFactory,
    PgSchema,
};

#[derive(QueryableByName)]
struct ActiveWriterCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

async fn wait_for_restarted_writer(db_url: &str, application_name: &str) -> Result<()> {
    let started = std::time::Instant::now();
    loop {
        let url = db_url.to_owned();
        let name = application_name.to_owned();
        let count = tokio::task::spawn_blocking(move || -> Result<i64> {
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            let row: ActiveWriterCount = diesel::sql_query(
                "SELECT count(*)::bigint AS count FROM pg_stat_activity WHERE application_name = $1",
            )
            .bind::<diesel::sql_types::Text, _>(name)
            .get_result(&mut conn)?;
            Ok(row.count)
        })
        .await??;
        if count == 1 {
            return Ok(());
        }
        if started.elapsed() > std::time::Duration::from_secs(30) {
            anyhow::bail!("restarted solver did not acquire its PostgreSQL writer session");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn wait_for_confirmed_attempt(db_url: &str) -> Result<()> {
    let started = std::time::Instant::now();
    loop {
        let url = db_url.to_owned();
        let confirmed = tokio::task::spawn_blocking(move || -> Result<bool> {
            use solver::db::postgres_schema::{orders, settlement_attempts};

            // Confirmation retires the parents and deletes the attempt.
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            let unresolved: i64 = settlement_attempts::table.count().get_result(&mut conn)?;
            let executed: i64 = orders::table
                .filter(orders::status.eq("executed"))
                .count()
                .get_result(&mut conn)?;
            Ok(unresolved == 0 && executed > 0)
        })
        .await??;
        if confirmed {
            return Ok(());
        }
        if started.elapsed() > std::time::Duration::from_secs(30) {
            anyhow::bail!("restarted solver did not confirm the submitted settlement");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// L2: the solver's ingest/executor clients run on their own OS-thread runtimes,
// so the test uses real-time polling. Advance the mock chain only after the
// executor submits, or empty blocks can expire a transaction during proving.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn three_user_direct_matching() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let pg = PgSchema::new().await?;
            // 1. Empty MockChain wrapped in MockRpcApi, shared by both Clients.
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // 2. USER Client: owns alice/bob/charlie + USDC/ETH faucets.
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
            let (charlie, _) = user_client
                .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                .await?;

            println!(
                "[test] usdc={}, eth={}, alice={}, bob={}, charlie={}",
                usdc.id().to_hex(),
                eth.id().to_hex(),
                alice.id().to_hex(),
                bob.id().to_hex(),
                charlie.id().to_hex(),
            );

            rpc.prove_block();
            user_client.sync_state().await?;

            // 3. Fund users via mint+consume → prove → sync. Each round commits
            //    one block; sync_state pulls the new state into the user Client.
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

            user_client
                .mint_and_consume(charlie.id(), usdc.id(), NoteType::Public)
                .await?;
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

            // 6. SolverConfig; the application database is the isolated PostgreSQL schema.
            let config = SolverConfig {
                rpc: RpcConfig {
                    endpoint: "http://unused".into(),
                    timeout_ms: 1_000,
                    prover_endpoint: None,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
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
                    clearing_fee_ppm: 0,
                    clearing_max_source_age_secs: 60,
                    clearing_max_source_skew_secs: 30,
                    admin_port: 0,
                    debug_mode: false,
                    obs_port: 0,
                    readiness_freshness_secs: 60,
                    verify_interval_ms: 5_000,
                    price_api_base_url: None,
                    price_query_port: 8080,
                    price_query_bind: "127.0.0.1".to_string(),
                    price_query_max_inflight: 128,
                    price_query_max_batch: 50,
                    price_query_timeout_ms: 3000,
                    price_precision: "full".to_string(),
                    price_vs_currency: "usd".to_string(),
                    price_staleness_secs: 30,
                    swap_proving_estimate_ms: 2000,
                    swap_block_time_ms: 6000,
                    swap_offmarket_tolerance_bps: 50,
                    router_enabled: false,
                    router_bind: "127.0.0.1".to_string(),
                    router_port: 0,
                    router_max_connections: 64,
                    router_max_msg_bytes: 16384,
                    router_quote_ttl_ms: 20_000,
                    router_inflight_ttl_ms: 30_000,
                    maker_gateway_enabled: false,
                    maker_gateway_bind: "127.0.0.1".into(),
                    maker_gateway_port: 0,
                    maker_intake_round_submits: 500,
                    maker_intake_submit_queue: 4096,
                    maker_intake_cancel_queue: 1024,
                    maker_stream_buffer: 256,
                    maker_stream_heartbeat_ms: 10_000,
                    maker_watch_interval_ms: 1_000,
                    maker_settlement_buffer_ms: 30_000,
                },
            };

            // 7. Spawn solver::start with the L2 factory. start() spawns the
            //    Send services on this LocalSet and the ingest/executor clients
            //    on their own OS threads.
            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            // Clear at 100 USDC/ETH: Alice and Bob execute, leaving 20 USDC.
            // Both faucets have the same decimals, so the reference ratio is 100.
            let price_map: std::collections::HashMap<_, u64> =
                [(usdc.id(), 100), (eth.id(), 10_000)].into_iter().collect();
            let restart_factory = factory.clone();
            let restart_config = config.clone();
            let restart_price_map = price_map.clone();
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
            let wait_result: Result<()> = async {
                wait_for_submitted_settlement(&pg.url, std::time::Duration::from_secs(900), || {
                    solver_handle.is_finished()
                })
                .await?;

                // Restart after submission but before the mock chain confirms
                // the transaction. The new solver must reconcile the durable
                // submitted attempt using the same PostgreSQL and client stores.
                cancel.cancel();
                tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                    .await
                    .context("first solver did not stop after submission")???;
                let restart_cancel = CancellationToken::new();
                let restart_task_cancel = restart_cancel.clone();
                let mut restarted = tokio::task::spawn_local(async move {
                    solver::start(
                        restart_factory,
                        move |_sm, _key| {
                            Ok(
                                Box::new(solver::price::MockPriceClient::new(restart_price_map))
                                    as Box<dyn solver::price::PriceClient + Send + Sync>,
                            )
                        },
                        solver_id,
                        restart_config,
                        restart_task_cancel,
                    )
                    .await
                });
                let resumed: Result<()> = async {
                    wait_for_restarted_writer(&pg.url, &format!("solver/{}", solver_id.to_hex()))
                        .await?;
                    if restarted.is_finished() {
                        anyhow::bail!("restarted solver exited before chain confirmation");
                    }
                    rpc.prove_block();
                    let chain = rpc.mock_chain.read();
                    if vault_balance(&chain, solver_id, usdc_id) < 20
                        || chain.committed_notes().len() < initial_committed_count + 2
                    {
                        anyhow::bail!("submitted settlement did not credit surplus and paybacks");
                    }
                    drop(chain);
                    wait_for_confirmed_attempt(&pg.url).await?;
                    Ok(())
                }
                .await;
                restart_cancel.cancel();
                let stopped =
                    tokio::time::timeout(std::time::Duration::from_secs(30), &mut restarted).await;
                resumed?;
                stopped.context("restarted solver did not stop cleanly")???;
                Ok(())
            }
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
                    anyhow::bail!(
                        "expected ≥2 new committed notes (alice+bob paybacks), got {grown}"
                    );
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
            if !solver_handle.is_finished() {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                        .await;
            }
            drop(user_temp);
            drop(solver_temp);
            verdict
        })
        .await
}

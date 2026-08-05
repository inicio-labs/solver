//! Integration test: a PSWAP the solver's ingest client has already tracked
//! gets **consumed externally** (by a competing account, not the solver).
//!
//! Validates the consumed-note path end-to-end on the L2 threaded model:
//!   1. The solver discovers + tracks a lone PSWAP (writes it `active` in its
//!      DB, adds it to the matcher book). It is deliberately *unmatchable*
//!      (single note, no reciprocal counter-order, triangular disabled), so
//!      the solver never attempts to settle it — removing all races.
//!   2. `dave` (a non-solver account on the user client) fully fills/consumes
//!      that PSWAP via `build_pswap_consume`, putting its nullifier on-chain.
//!   3. The solver's keyless ingest client detects the external consumption on
//!      a later `sync_state` (`SyncSummary.consumed_notes` → `consumed_tx` →
//!      matcher `remove_order`; `ingest_once` marks the DB row terminal).
//!
//! Asserts: the order ends `onchain_nullified` in the solver DB, the solver
//! never settled it (its vault stays empty — no double-consume), the external
//! consumption really happened (dave received the offered USDC), and the
//! solver shuts down cleanly (no panic / loop / hang).

mod common;

use std::sync::Arc;

use anyhow::Result;
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{
    insert_new_fungible_faucet, insert_new_wallet, mint_and_consume,
};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_testing::MockChain;
use solver::config::{
    AssetPairConfig, EngineConfig, RpcConfig, SolverAccountConfig, SolverConfig,
};
use tokio_util::sync::CancellationToken;

use common::{build_test_client, temp_paths, vault_balance, MockClientFactory};

/// Read an order's status straight from the solver's SQLite DB (fresh
/// read-only connection; WAL allows concurrent reads while the solver writes).
/// `None` = no row yet (not ingested) or DB not ready.
fn order_status(db_path: &str, note_key: &[u8]) -> Option<String> {
    use solver::db::schema::orders;
    let mut conn = SqliteConnection::establish(db_path).ok()?;
    orders::table
        .filter(orders::note_id.eq(note_key))
        .select(orders::status)
        .first::<String>(&mut conn)
        .optional()
        .ok()
        .flatten()
}

#[tokio::test]
async fn already_consumed_pswap_is_retired_not_settled() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            // 1. Shared mock chain.
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // 2. USER client: USDC/ETH faucets, `alice` (PSWAP creator) and
            //    `dave` (external consumer — NOT the solver).
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
            let (dave, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let usdc_id = usdc.id();
            let eth_id = eth.id();
            let dave_id = dave.id();
            println!("[test] alice={} dave={}", alice.id().to_hex(), dave_id.to_hex());

            // 3. Fund alice with USDC (to offer) and dave with ETH (to fill).
            mint_and_consume(&mut user_client, alice.id(), usdc_id, NoteType::Public).await;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync after alice mint: {e}"))?;
            mint_and_consume(&mut user_client, dave_id, eth_id, NoteType::Public).await;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync after dave mint: {e}"))?;

            // 4. Alice creates ONE PSWAP: offer 100 USDC, want 1 ETH.
            let alice_request = TransactionRequestBuilder::new()
                .build_pswap_create(
                    &PswapTransactionData::new(
                        alice.id(),
                        FungibleAsset::new(usdc_id, 100)?,
                        FungibleAsset::new(eth_id, 1)?,
                    ),
                    NoteType::Public,
                    NoteType::Public,
                    None,
                    user_client.rng(),
                )
                .map_err(|e| anyhow::anyhow!("alice build_pswap_create: {e}"))?;
            let pswap_note = alice_request.expected_output_own_notes()[0].clone();
            let note_key = pswap_note.id().to_bytes().to_vec();
            Box::pin(user_client.submit_new_transaction(alice.id(), alice_request))
                .await
                .map_err(|e| anyhow::anyhow!("alice submit pswap: {e}"))?;
            rpc.prove_block();

            // 5. Provision the solver account (throwaway client persists it).
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id = {
                let mut sc = build_test_client(
                    rpc.clone(),
                    solver_keystore_path.clone(),
                    solver_store_path.clone(),
                )
                .await?;
                sc.ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
                let ks =
                    miden_client::keystore::FilesystemKeyStore::new(solver_keystore_path.clone())
                        .map_err(|e| anyhow::anyhow!("solver FilesystemKeyStore::new: {e}"))?;
                let (acct, _) = insert_new_wallet(&mut sc, mode, &ks, scheme).await?;
                acct.id()
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

            // 6. Config: single USDC-ETH pair (so the ingest client subscribes
            //    the tag and discovers alice's PSWAP), triangular DISABLED and
            //    no reciprocal order → the solver tracks it but can never
            //    match/settle it. No race.
            let solver_db = solver_temp.path().join("solver.sqlite3");
            let solver_db_path = solver_db.to_string_lossy().into_owned();
            let config = SolverConfig {
                rpc: RpcConfig { endpoint: "http://unused".into(), timeout_ms: 1_000, prover_endpoint: None },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    app_db_path: solver_db_path.clone(),
                    executor_store_path: executor_store_path.clone(),
                    ingest_store_path: ingest_store_path.clone(),
                    read_pool_size: 2,
                },
                pairs: vec![AssetPairConfig {
                    name: "USDC-ETH".into(),
                    asset_x_faucet_id: usdc_id.to_hex(),
                    asset_x_external_symbol: None,
                    asset_y_faucet_id: eth_id.to_hex(),
                    asset_y_external_symbol: None,
                }],
                engine: EngineConfig {
                    pulse_interval_ms: 200,
                    fetch_interval_ms: 100,
                    price_interval_ms: 60_000,
                    triangular_enabled: false,
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
                },
            };

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let price_map: std::collections::HashMap<_, u64> =
                [(usdc_id, 100), (eth_id, 100)].into_iter().collect();
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

            // 7. Phase 1: drive the chain until the solver has discovered +
            //    tracked the PSWAP (row `active` in its DB).
            let mut tracked = false;
            for _ in 0..400 {
                if order_status(&solver_db_path, &note_key).as_deref() == Some("active") {
                    tracked = true;
                    break;
                }
                rpc.prove_block();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            // 8. Phase 2: `dave` consumes alice's PSWAP externally (full fill:
            //    1 ETH in, 100 USDC out). Solver is NOT involved.
            let consumed_ok = if tracked {
                let consume_request = TransactionRequestBuilder::new()
                    .build_pswap_consume(&pswap_note, dave_id, 1, 0)
                    .map_err(|e| anyhow::anyhow!("dave build_pswap_consume: {e}"))?;
                Box::pin(user_client.submit_new_transaction(dave_id, consume_request))
                    .await
                    .map_err(|e| anyhow::anyhow!("dave submit consume: {e}"))?;
                rpc.prove_block();
                true
            } else {
                false
            };

            // 9. Phase 3: drive the chain until the solver's ingest syncs past
            //    the consumption and retires the order as terminal.
            let mut final_status = order_status(&solver_db_path, &note_key);
            if consumed_ok {
                for _ in 0..600 {
                    final_status = order_status(&solver_db_path, &note_key);
                    if final_status.as_deref() == Some("onchain_nullified") {
                        break;
                    }
                    rpc.prove_block();
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }

            // 10. Verdict (no panicking asserts → cleanup always runs).
            let verdict: Result<()> = (|| {
                if !tracked {
                    anyhow::bail!("solver never tracked the PSWAP (no `active` row appeared)");
                }
                if !consumed_ok {
                    anyhow::bail!("external consume was not submitted");
                }
                if final_status.as_deref() != Some("onchain_nullified") {
                    anyhow::bail!(
                        "order should be terminal `onchain_nullified` after external \
                         consumption, got {final_status:?}"
                    );
                }
                let chain = rpc.mock_chain.read();
                let solver_usdc = vault_balance(&chain, solver_id, usdc_id);
                if solver_usdc != 0 {
                    anyhow::bail!(
                        "solver must NOT have settled the externally-consumed PSWAP \
                         (solver USDC = {solver_usdc}, expected 0 — double-consume!)"
                    );
                }
                let dave_usdc = vault_balance(&chain, dave_id, usdc_id);
                if dave_usdc != 100 {
                    anyhow::bail!(
                        "external consumer dave should hold the 100 offered USDC \
                         (got {dave_usdc}) — scenario precondition failed"
                    );
                }
                Ok(())
            })();

            if let Err(e) = &verdict {
                let chain = rpc.mock_chain.read();
                println!(
                    "[test] FAILED: {e}\n[test] tracked={tracked} consumed_ok={consumed_ok} \
                     final_status={final_status:?} solver_usdc={} dave_usdc={} block={}",
                    vault_balance(&chain, solver_id, usdc_id),
                    vault_balance(&chain, dave_id, usdc_id),
                    chain.latest_block_header().block_num().as_u64(),
                );
            }

            // 11. Clean shutdown — bounded join, no orphan threads.
            cancel.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                .await;
            drop(user_temp);
            drop(solver_temp);
            verdict
        })
        .await
}

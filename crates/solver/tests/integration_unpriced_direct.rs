//! Regression-lock for audit finding **C2**: direct (pairwise) matching must
//! refuse to settle a trade involving a token with **no USD price**.
//!
//! Scenario: alice⇄bob form a raw-balanced reciprocal pair, but the offered
//! token `FOO` has no injected price. Correct behaviour (after the C2 fix —
//! call `feed.is_order_profitable(...)` on the direct path): the solver must
//! NOT settle it (an unpriced token is not matchable).
//!
//! `#[ignore]` ON PURPOSE: on current code the direct path never consults the
//! price feed (that is exactly bug C2), so today this trade *does* settle and
//! the assertion below fails. Un-ignore this test as the regression-lock the
//! moment the C2 fix lands.

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

use common::{build_test_client, temp_paths, vault_balance, MockClientFactory};

#[tokio::test]
async fn unpriced_token_not_settled_on_direct_path() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

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

            // `foo` is the UNPRICED token; `eth` is priced.
            let (foo, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (eth, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (alice, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (bob, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let foo_id = foo.id();
            let eth_id = eth.id();

            mint_and_consume(&mut user_client, alice.id(), foo_id, NoteType::Public).await;
            rpc.prove_block();
            user_client.sync_state().await.map_err(|e| anyhow::anyhow!("sync a: {e}"))?;
            mint_and_consume(&mut user_client, bob.id(), eth_id, NoteType::Public).await;
            rpc.prove_block();
            user_client.sync_state().await.map_err(|e| anyhow::anyhow!("sync b: {e}"))?;

            // Raw-balanced reciprocal pair (would settle under raw-ratio
            // matching): alice 120 FOO ⇄ 1 ETH ; bob 1 ETH ⇄ 100 FOO.
            for (creator, off, off_amt, req, req_amt) in [
                (alice.id(), foo_id, 120u64, eth_id, 1u64),
                (bob.id(), eth_id, 1u64, foo_id, 100u64),
            ] {
                let req_tx = TransactionRequestBuilder::new()
                    .build_pswap_create(
                        &PswapTransactionData::new(
                            creator,
                            FungibleAsset::new(off, off_amt)?,
                            FungibleAsset::new(req, req_amt)?,
                        ),
                        NoteType::Public,
                        NoteType::Public,
                        None,
                        user_client.rng(),
                    )
                    .map_err(|e| anyhow::anyhow!("build_pswap_create: {e}"))?;
                Box::pin(user_client.submit_new_transaction(creator, req_tx))
                    .await
                    .map_err(|e| anyhow::anyhow!("submit pswap: {e}"))?;
            }
            rpc.prove_block();

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
                        .map_err(|e| anyhow::anyhow!("FilesystemKeyStore::new: {e}"))?;
                let (acct, _) = insert_new_wallet(&mut sc, mode, &ks, scheme).await?;
                acct.id()
            };
            let ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let executor_store_path = solver_store_path.to_string_lossy().into_owned();
            let ingest_store_path = ingest_store.to_string_lossy().into_owned();
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store,
                executor_store: solver_store_path,
                keystore: solver_keystore_path.clone(),
            });
            let solver_db = solver_temp.path().join("solver.sqlite3");
            let config = SolverConfig {
                rpc: RpcConfig { endpoint: "http://unused".into(), timeout_ms: 1_000, prover_endpoint: None },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    app_db_path: solver_db.to_string_lossy().into_owned(),
                    executor_store_path,
                    ingest_store_path,
                    read_pool_size: 2,
                },
                pairs: vec![AssetPairConfig {
                    name: "FOO-ETH".into(),
                    asset_x_faucet_id: foo_id.to_hex(),
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
            };

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            // Only ETH is priced; FOO is deliberately absent → unpriced.
            let price_map: std::collections::HashMap<_, u64> =
                [(eth_id, 100)].into_iter().collect();
            let initial_committed = rpc.mock_chain.read().committed_notes().len();
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

            // Drive generously; with the C2 fix an unpriced token is never
            // matchable, so NOTHING should settle.
            for _ in 0..120 {
                rpc.prove_block();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            let verdict: Result<()> = {
                let chain = rpc.mock_chain.read();
                let solver_foo = vault_balance(&chain, solver_id, foo_id);
                let grown = chain.committed_notes().len() - initial_committed;
                drop(chain);
                if solver_foo != 0 {
                    Err(anyhow::anyhow!(
                        "solver settled an UNPRICED-token trade (FOO surplus = {solver_foo}); \
                         direct path must reject unpriced tokens (audit C2)"
                    ))
                } else if grown != 0 {
                    Err(anyhow::anyhow!(
                        "settlement paybacks appeared ({grown}) for an unpriced-token trade; \
                         direct path must reject unpriced tokens (audit C2)"
                    ))
                } else {
                    Ok(())
                }
            };

            if let Err(e) = &verdict {
                println!("[test] FAILED: {e}");
            }
            cancel.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle)
                .await;
            drop(user_temp);
            drop(solver_temp);
            verdict
        })
        .await
}

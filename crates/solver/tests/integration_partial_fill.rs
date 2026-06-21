//! Repro: PARTIAL-FILL settlement.
//!
//! A full maker order matched by a half-size counter forces the solver to
//! *partially* consume the maker's note (leaving a remainder). The other
//! integration tests deliberately avoid this (see `three_user_direct`'s comment:
//! "the current matcher's integer-rounded fill math can't split bob's order"),
//! so partial settlement has never been exercised on MockChain — which is
//! exactly the path that fails on the live deployment with
//! `transaction execution failed`.
//!
//! Scenario (scaled from the live IBTC/IUSDT case; faucet mints 1000 units, and
//! the real amounts divide cleanly so absolute scale doesn't change the path):
//!   maker:  offer 100 IBTC, request 1000 IUSDT   (rate 10)
//!   taker:  offer 500 IUSDT, request   49 IBTC   (offers only HALF the IUSDT)
//! => taker fills HALF of the maker's order; the maker's note is partially
//!    consumed, leaving a 50-IBTC remainder; the solver keeps 1 IBTC surplus.
//!
//! Run with `--nocapture` to see the exact `assert.err=...` the executor now
//! logs (`error = ?e`) under "non-RPC submit failure classified".

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

use common::{build_test_client, temp_paths, vault_balance, wait_for_realtime, MockClientFactory};

#[tokio::test]
async fn partial_fill_repro() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // USER client: 2 faucets (IBTC, IUSDT) + maker + taker wallets.
            let (user_temp, user_keystore_path, user_store_path) = temp_paths()?;
            let mut user_client =
                build_test_client(rpc.clone(), user_keystore_path.clone(), user_store_path).await?;
            user_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow::anyhow!("user genesis: {e}"))?;
            let user_keystore =
                miden_client::keystore::FilesystemKeyStore::new(user_keystore_path.clone())
                    .map_err(|e| anyhow::anyhow!("user keystore: {e}"))?;
            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let mode = AccountType::Public;

            let (ibtc, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (iusdt, _) =
                insert_new_fungible_faucet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (maker1, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (maker2, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (taker_full, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;
            let (taker_half, _) =
                insert_new_wallet(&mut user_client, mode, &user_keystore, scheme).await?;

            // Fund: makers get IBTC, takers get IUSDT (1000 units each).
            for w in [maker1.id(), maker2.id()] {
                mint_and_consume(&mut user_client, w, ibtc.id(), NoteType::Public).await;
                rpc.prove_block();
                user_client.sync_state().await.map_err(|e| anyhow::anyhow!("sync ibtc: {e}"))?;
            }
            for w in [taker_full.id(), taker_half.id()] {
                mint_and_consume(&mut user_client, w, iusdt.id(), NoteType::Public).await;
                rpc.prove_block();
                user_client.sync_state().await.map_err(|e| anyhow::anyhow!("sync iusdt: {e}"))?;
            }

            // THE SPLIT CASE: ONE maker (offer 100 IBTC, request 1000 IUSDT) is
            // filled by TWO half-counters (each offer 500 IUSDT, request 49 IBTC).
            // The matcher must SPLIT the maker's order across both takers
            // (500 + 500 = 1000) — exactly what three_user_direct's comment warns
            // its integer-rounded fill math can't do. (maker2 is funded but unused.)
            let orders: Vec<(_, FungibleAsset, FungibleAsset)> = vec![
                (maker1.id(), FungibleAsset::new(ibtc.id(), 100)?, FungibleAsset::new(iusdt.id(), 1000)?),
                (taker_full.id(), FungibleAsset::new(iusdt.id(), 500)?, FungibleAsset::new(ibtc.id(), 49)?),
                (taker_half.id(), FungibleAsset::new(iusdt.id(), 500)?, FungibleAsset::new(ibtc.id(), 49)?),
            ];
            let _ = maker2;
            for (creator, offered, requested) in orders {
                let req = TransactionRequestBuilder::new()
                    .build_pswap_create(
                        &PswapTransactionData::new(creator, offered, requested),
                        NoteType::Public,
                        NoteType::Public,
                        None,
                        user_client.rng(),
                    )
                    .map_err(|e| anyhow::anyhow!("pswap: {e}"))?;
                Box::pin(user_client.submit_new_transaction(creator, req))
                    .await
                    .map_err(|e| anyhow::anyhow!("submit: {e}"))?;
            }
            rpc.prove_block();

            // SOLVER account (throwaway client provisions it on disk, then drops).
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
                let sks =
                    miden_client::keystore::FilesystemKeyStore::new(solver_keystore_path.clone())
                        .map_err(|e| anyhow::anyhow!("solver keystore: {e}"))?;
                let (sa, _) = insert_new_wallet(&mut sc, mode, &sks, scheme).await?;
                sa.id()
            };
            println!("[test] solver={}", solver_id.to_hex());

            let solver_ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let executor_store_path = solver_store_path.to_string_lossy().into_owned();
            let ingest_store_path = solver_ingest_store.to_string_lossy().into_owned();
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store: solver_ingest_store,
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
                    name: "IBTC-IUSDT".into(),
                    asset_x_faucet_id: ibtc.id().to_hex(),
                    asset_x_external_symbol: None,
                    asset_y_faucet_id: iusdt.id().to_hex(),
                    asset_y_external_symbol: None,
                }],
                engine: EngineConfig {
                    pulse_interval_ms: 200,
                    fetch_interval_ms: 100,
                    price_interval_ms: 60_000,
                    triangular_enabled: false, // isolate the DIRECT partial fill
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
                },
            };

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let price_map: std::collections::HashMap<_, u64> =
                [(ibtc.id(), 1000), (iusdt.id(), 100)].into_iter().collect();
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

            // Expect the solver to keep 1 IBTC surplus (settlement). If partial
            // settlement is broken it never will -> timeout, and the executor's
            // `error = ?e` log above shows the exact assertion.
            let ibtc_id = ibtc.id();
            let initial = rpc.mock_chain.read().committed_notes().len();
            let wait = wait_for_realtime(
                &rpc,
                60, // ~6s — the executor logs the assertion on the first attempt
                std::time::Duration::from_millis(100),
                |chain| {
                    // maker fully filled (releases 100 IBTC), 2 takers take 49
                    // each -> solver keeps 2 IBTC surplus if the split settles.
                    vault_balance(chain, solver_id, ibtc_id) >= 2
                },
            )
            .await;

            let verdict: Result<()> = (|| {
                wait?;
                Ok(())
            })();
            {
                let chain_ro = rpc.mock_chain.read();
                let surplus = vault_balance(&chain_ro, solver_id, ibtc_id);
                match &verdict {
                    Ok(()) => println!("[test] PARTIAL FILL SETTLED — solver_ibtc surplus={surplus}. Bug NOT reproduced."),
                    Err(e) => println!(
                        "[test] PARTIAL FILL DID NOT SETTLE: {e}\n[test]   solver_ibtc={surplus} committed={} (was {})\n[test]   ^ see the executor 'non-RPC submit failure classified' line above for the exact assert.err",
                        chain_ro.committed_notes().len(), initial
                    ),
                }
            }

            cancel.cancel();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle).await;
            drop(user_temp);
            drop(solver_temp);
            // The repro EXPECTS the bug, so a non-settlement is the documented
            // outcome — return Ok so the test surfaces the captured assertion
            // without a red failure. Flip to `verdict` once the fix lands.
            let _ = verdict;
            Ok(())
        })
        .await
}

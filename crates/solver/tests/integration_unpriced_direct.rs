//! Regression-lock for audit finding **C2**: the solver must refuse to settle
//! a trade on a pair with **no usable price**.
//!
//! Scenario: alice⇄bob form a raw-balanced reciprocal FOO/ETH pair. FOO has
//! no Binance market (the mock Binance knows only `ETHUSDT`):
//! - configured with the unlisted symbol `FOOETH`, startup fails and names it;
//! - configured without a Binance market, the solver runs, but the pair has no
//!   price and the solver must NOT settle it.

mod common;

use std::sync::Arc;

use anyhow::Result;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, count_orders, temp_paths, vault_balance, MockClientFactory, PgSchema,
};

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn unpriced_token_not_settled_on_direct_path() -> Result<()> {
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

            // FOO/ETH has no listed Binance market; ETH alone is priced.
            let (foo, _) = user_client
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
            let foo_id = foo.id();
            let eth_id = eth.id();

            rpc.prove_block();
            user_client.sync_state().await?;

            user_client
                .mint_and_consume(alice.id(), foo_id, NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync a: {e}"))?;
            user_client
                .mint_and_consume(bob.id(), eth_id, NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync b: {e}"))?;

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
                let mut sc = TestClient::new(
                    build_test_client(
                        rpc.clone(),
                        solver_keystore_path.clone(),
                        solver_store_path.clone(),
                    )
                    .await?,
                );
                sc.ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
                let (acct, _) = sc
                    .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                    .await?;
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
            let binance = common::BinanceStub::start(&[("ETHUSDT", "ETH", "USDT", "1")]);
            let config_with = |pairs: Vec<solver::config::AssetPairConfig>| SolverConfig {
                rpc: RpcConfig {
                    endpoint: "http://unused".into(),
                    timeout_ms: 1_000,
                    prover_endpoint: None,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    executor_store_path: executor_store_path.clone(),
                    ingest_store_path: ingest_store_path.clone(),
                    read_pool_size: 2,
                },
                pairs,
                engine: common::engine_config(),
                binance: binance.config(),
            };

            // FOOETH is not listed: startup fails and names the market.
            let listed_nowhere = config_with(vec![common::pair_config(
                "FOO-ETH", foo_id, "FOO", eth_id, "ETH", "FOOETH",
            )]);
            let started = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                solver::start(
                    factory.clone(),
                    solver_id,
                    listed_nowhere,
                    CancellationToken::new(),
                ),
            )
            .await
            .expect("startup must fail promptly");
            match started {
                Err(error) if format!("{error:#}").contains("FOOETH") => {}
                other => panic!("startup must fail on the unlisted FOOETH: {other:?}"),
            }

            // Without a Binance market for FOO the solver runs, unpriced.
            let config = config_with(vec![common::unpriced_pair_config(
                "FOO-ETH", foo_id, eth_id, "ETH",
            )]);

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let initial_committed = rpc.mock_chain.read().committed_notes().len();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(factory, solver_id, config, solver_cancel).await
            });

            // Drive generously; a pair without a usable price never clears,
            // so NOTHING should settle.
            for _ in 0..120 {
                rpc.prove_block();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            // The solver ran the whole time and knew both orders: nothing
            // settled because the pair had no price, not because it stopped.
            if solver_handle.is_finished() {
                panic!("solver stopped early: {:?}", (&mut solver_handle).await);
            }
            assert_eq!(count_orders(&pg.url, "active").await, 2);
            let verdict: Result<()> = {
                let chain = rpc.mock_chain.read();
                let solver_foo = vault_balance(&chain, solver_id, foo_id);
                let grown = chain.committed_notes().len() - initial_committed;
                drop(chain);
                if solver_foo != 0 {
                    Err(anyhow::anyhow!(
                        "solver settled an UNPRICED-token trade (FOO surplus = {solver_foo}); \
                         a pair without a usable price must not clear (audit C2)"
                    ))
                } else if grown != 0 {
                    Err(anyhow::anyhow!(
                        "settlement paybacks appeared ({grown}) for an unpriced-token trade; \
                         a pair without a usable price must not clear (audit C2)"
                    ))
                } else {
                    Ok(())
                }
            };

            if let Err(e) = &verdict {
                println!("[test] FAILED: {e}");
            }
            cancel.cancel();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(30), &mut solver_handle).await;
            drop(user_temp);
            drop(solver_temp);
            verdict
        })
        .await
}

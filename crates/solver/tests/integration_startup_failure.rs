//! Integration test: a client-build failure at startup must surface as a
//! clean error, not a hang.
//!
//! Drives the L2 readiness gate (`a29efad`): `FailingExecutorFactory`
//! builds a real keyless ingest client but its `build_executor` always
//! errors. `start()` must observe the executor readiness `oneshot` carry
//! `Err`, cancel the global token, join *both* client OS threads, and
//! return `Err` — all within a bounded time (a hang ⇒ the join/readiness
//! plumbing is broken).

mod common;

use std::sync::Arc;

use anyhow::Result;
use miden_client::auth::AuthSchemeId;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_protocol::account::AccountType;
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{build_test_client, temp_paths, FailingExecutorFactory, PgSchema};

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn startup_failure_surfaces_clean_error_no_hang() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let _pg = PgSchema::new().await?;
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // Minimal solver account just to satisfy the config / start
            // signature (executor build fails before it's ever used).
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
                    .insert_account(
                        AccountSetup::wallet(AccountType::Public)
                            .auth_scheme(AuthSchemeId::Falcon512Poseidon2),
                    )
                    .await?;
                acct.id()
            };

            let factory: Arc<dyn solver::ClientFactory> = Arc::new(FailingExecutorFactory {
                rpc: rpc.clone(),
                ingest_store: solver_temp.path().join("ingest_store.sqlite3"),
            });

            // No markets: the feed starts with nothing to price.
            let binance = common::BinanceStub::start(&[]);
            let config = SolverConfig {
                rpc: RpcConfig {
                    endpoint: "http://unused".into(),
                    timeout_ms: 1_000,
                    prover_endpoint: None,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
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
                pairs: vec![], // no pairs needed; executor build fails first
                engine: common::engine_config(),
                binance: binance.config(),
            };

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let mut handle = tokio::task::spawn_local(async move {
                solver::start(factory, solver_id, config, solver_cancel).await
            });

            // start() must return (Err) within the bound. A timeout here means
            // the readiness/join plumbing hung — the failure this test guards.
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(30), &mut handle).await;

            let verdict: Result<()> = match outcome {
                Err(_) => Err(anyhow::anyhow!(
                    "start() hung after executor-build failure (no clean error within 30s) \
                     — readiness gate / thread join is broken"
                )),
                Ok(join_res) => match join_res {
                    Err(e) => Err(anyhow::anyhow!("solver task panicked: {e}")),
                    Ok(Ok(())) => Err(anyhow::anyhow!(
                        "start() returned Ok despite executor-build failure (should be Err)"
                    )),
                    // A clean error in bounded time, for the injected failure.
                    Ok(Err(error))
                        if format!("{error:#}").contains("injected executor build failure") =>
                    {
                        Ok(())
                    }
                    Ok(Err(error)) => Err(anyhow::anyhow!(
                        "start() failed for another reason: {error:#}"
                    )),
                },
            };

            if let Err(e) = &verdict {
                println!("[test] FAILED: {e}");
            }

            // `start()` already returned (and, on the readiness-Err path, has
            // itself cancelled + joined both client threads). Do NOT await
            // `handle` again — a completed tokio JoinHandle panics if polled
            // twice. `cancel` is idempotent / already fired internally.
            cancel.cancel();
            drop(solver_temp);
            verdict
        })
        .await
}

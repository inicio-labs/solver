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
                    prover_timeout_ms: 45_000,
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

/// Orders that went live at accept need the gateway's watcher. With the
/// gateway off and such an order unresolved, `start()` refuses at once and
/// names the escape hatch (ADR 0008).
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_solver_without_its_gateway_refuses_unconfirmed_orders() -> Result<()> {
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::block::BlockNumber;
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};
    use solver::db::maker_db;
    use solver::maker::{CommandHeader, MakerCommand};

    let pg = PgSchema::new().await?;
    let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
    let note: Note = PswapNote::builder()
        .sender(creator)
        .storage(
            PswapNoteStorage::builder()
                .min_requested_asset(FungibleAsset::new(
                    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?,
                    100,
                )?)
                .min_fill_step(AssetAmount::new(1)?)
                .creator_account_id(creator)
                .build(),
        )
        .serial_number(Word::from([1u32, 0, 0, 0]))
        .note_type(NoteType::Private)
        .offered_asset(FungibleAsset::new(
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?,
            50,
        )?)
        .build()?
        .into();
    let url = pg.url.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use diesel::prelude::*;
        use solver::db::postgres_schema::sync_state;
        let mut conn = solver::db::postgres_migrations::connect(&url)?;
        let maker = maker_db::create_maker_tx(&mut conn, "mm1")?.expect("new maker");
        let epoch: i64 = sync_state::table
            .find(1_i16)
            .select(sync_state::owner_epoch)
            .first(&mut conn)?;
        let mut gate = maker_db::AcceptGate::new(epoch, || Some(BlockNumber::from(3)));
        let header = CommandHeader::new(maker, "s1".into(), 1)?;
        let submit = MakerCommand::submit(note)?;
        let outcome = conn.transaction::<_, solver::db::DbError, _>(|conn| {
            maker_db::execute_command_with_tx(conn, &header, &submit, Some(&mut gate))
        })?;
        anyhow::ensure!(outcome.live.is_some(), "the order went live at accept");
        Ok(())
    })
    .await??;

    let rpc = Arc::new(MockRpcApi::new(MockChain::new()));
    let (temp, keystore, _) = temp_paths()?;
    let binance = common::BinanceStub::start(&[]);
    let mut engine = common::engine_config();
    engine.maker_gateway_enabled = false;
    let config = SolverConfig {
        rpc: RpcConfig {
            endpoint: "http://unused".into(),
            timeout_ms: 1_000,
            prover_endpoint: None,
            prover_timeout_ms: 45_000,
        },
        solver: SolverAccountConfig {
            account_id: creator.to_hex(),
            keystore_path: keystore.to_string_lossy().into_owned(),
            executor_store_path: temp
                .path()
                .join("executor.sqlite3")
                .to_string_lossy()
                .into_owned(),
            ingest_store_path: temp
                .path()
                .join("ingest.sqlite3")
                .to_string_lossy()
                .into_owned(),
            read_pool_size: 2,
        },
        pairs: vec![],
        engine,
        binance: binance.config(),
    };
    let factory: Arc<dyn solver::ClientFactory> = Arc::new(FailingExecutorFactory {
        rpc,
        ingest_store: temp.path().join("ingest.sqlite3"),
    });
    // In a LocalSet, as in production, so a solver that does not refuse
    // fails the assertion below rather than panicking on `spawn_local`.
    let cancel = CancellationToken::new();
    let local = tokio::task::LocalSet::new();
    let started = local
        .run_until(tokio::time::timeout(
            std::time::Duration::from_secs(30),
            solver::start(factory, creator, config, cancel.clone()),
        ))
        .await;
    cancel.cancel();
    let error = match started {
        Ok(Err(error)) => format!("{error:#}"),
        Ok(Ok(())) => anyhow::bail!("start returned Ok with unconfirmed orders and no gateway"),
        Err(_) => anyhow::bail!("start did not refuse within 30 s"),
    };
    anyhow::ensure!(
        error.contains("cancel-unconfirmed") && error.contains("maker_gateway_enabled"),
        "unexpected start error: {error}"
    );
    Ok(())
}

//! Maker gateway on the mock chain: today's path for a maker order end to
//! end, as the baseline for letting maker orders trade before their note is
//! confirmed.
//!
//! The solver runs as in the other integration tests, with the maker gateway
//! enabled and every client behind [`NodeLikeRpc`], a mock node that refuses
//! unknown or already spent notes the way a real Miden node does.

mod common;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{
    PaymentNoteDescription, PswapTransactionData, TransactionRequestBuilder,
};
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use solver::gateway::proto;
use tokio_util::sync::CancellationToken;

use common::maker::{
    expect_no_event, free_port, is_order_status, register_maker, wait_for_event, MakerClient,
};
use common::node_like::NodeLikeRpc;
use common::{
    build_test_client, temp_paths, wait_for_submitted_settlement, MockClientFactory, PgSchema,
};

const SCHEME: AuthSchemeId = AuthSchemeId::Falcon512Poseidon2;

/// A transaction spending a note nobody created is refused at submission,
/// with the node's error, instead of failing later in `prove_block`.
#[tokio::test]
async fn the_mock_node_refuses_unknown_notes_like_a_real_node() -> Result<()> {
    let rpc = NodeLikeRpc::new(Arc::new(MockRpcApi::new(MockChain::new())));
    let (_temp, keystore, store) = temp_paths()?;
    let mut client = TestClient::new(build_test_client(rpc.clone(), keystore, store).await?);
    client
        .ensure_genesis_in_place()
        .await
        .map_err(|e| anyhow!("genesis: {e}"))?;
    let mode = AccountType::Public;
    let (faucet, _) = client
        .insert_account(AccountSetup::faucet(mode).auth_scheme(SCHEME))
        .await?;
    let (alice, _) = client
        .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
        .await?;
    let (bob, _) = client
        .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
        .await?;
    rpc.prove_block();
    client.sync_state().await?;
    client
        .mint_and_consume(alice.id(), faucet.id(), NoteType::Public)
        .await?;
    rpc.prove_block();
    client.sync_state().await?;

    // Alice builds a payment to Bob but never sends it.
    let payment = TransactionRequestBuilder::new()
        .build_pay_to_id(
            PaymentNoteDescription::new(
                vec![FungibleAsset::new(faucet.id(), 5)?.into()],
                alice.id(),
                bob.id(),
            ),
            NoteType::Private,
            client.rng(),
        )
        .map_err(|e| anyhow!("build payment: {e}"))?;
    let unsent = payment
        .expected_output_own_notes()
        .into_iter()
        .next()
        .context("the payment creates no note")?;

    // Bob tries to consume it as an unauthenticated note.
    let consume = TransactionRequestBuilder::new()
        .build_consume_notes(vec![unsent.clone()])
        .map_err(|e| anyhow!("build consume: {e}"))?;
    let refused = Box::pin(client.submit_new_transaction(bob.id(), consume))
        .await
        .expect_err("the node must refuse a note nobody created");
    let text = format!("{refused:?}");
    assert!(
        text.contains("unauthenticated input notes are unknown")
            && text.contains(&format!("{:?}", unsent.id())),
        "unexpected refusal: {text}"
    );

    // Once Alice sends it, Bob's spend is accepted while it still waits
    // for a block, and both land together.
    Box::pin(client.submit_new_transaction(alice.id(), payment))
        .await
        .map_err(|e| anyhow!("send payment: {e}"))?;
    let consume = TransactionRequestBuilder::new()
        .build_consume_notes(vec![unsent])
        .map_err(|e| anyhow!("build consume: {e}"))?;
    Box::pin(client.submit_new_transaction(bob.id(), consume))
        .await
        .map_err(|e| anyhow!("consume a note waiting in the mempool: {e}"))?;
    rpc.prove_block();
    Ok(())
}

/// Today's path end to end: the maker sends its transaction, then
/// `SubmitOrder`; the order goes LIVE once its note is in a block, trades
/// against a user's order, and the maker sees the settlement events.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_maker_order_goes_live_after_its_note_lands_and_trades() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let pg = PgSchema::new().await?;
            let rpc = NodeLikeRpc::new(Arc::new(MockRpcApi::new(MockChain::new())));

            // Users, the maker and the faucets live in one test client.
            let (user_temp, user_keystore, user_store) = temp_paths()?;
            let mut users =
                TestClient::new(build_test_client(rpc.clone(), user_keystore, user_store).await?);
            users
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow!("user genesis: {e}"))?;
            let mode = AccountType::Public;
            let (usdc, _) = users
                .insert_account(AccountSetup::faucet(mode).auth_scheme(SCHEME))
                .await?;
            let (eth, _) = users
                .insert_account(AccountSetup::faucet(mode).auth_scheme(SCHEME))
                .await?;
            let (alice, _) = users
                .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
                .await?;
            let (maker, _) = users
                .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
                .await?;
            rpc.prove_block();
            users.sync_state().await?;
            for (account, faucet) in [(alice.id(), usdc.id()), (maker.id(), eth.id())] {
                users
                    .mint_and_consume(account, faucet, NoteType::Public)
                    .await?;
                rpc.prove_block();
                users.sync_state().await?;
            }

            // The solver account, provisioned on disk as in production.
            let (solver_temp, solver_keystore, solver_store) = temp_paths()?;
            let solver_id = {
                let mut provision = TestClient::new(
                    build_test_client(rpc.clone(), solver_keystore.clone(), solver_store.clone())
                        .await?,
                );
                provision
                    .ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow!("solver genesis: {e}"))?;
                let (account, _) = provision
                    .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
                    .await?;
                account.id()
            };
            let ingest_store = solver_temp.path().join("ingest_store.sqlite3");
            let binance = common::BinanceStub::start(&[
                ("ETHUSDC", "ETH", "USDC", "100"),
                ("ETHUSDT", "ETH", "USDT", "100"),
                ("USDCUSDT", "USDC", "USDT", "1"),
            ]);
            let mut engine = common::engine_config();
            engine.maker_gateway_enabled = true;
            engine.maker_gateway_port = free_port();
            engine.maker_watch_interval_ms = 200;
            engine.obs_port = free_port();
            let (port, obs_port) = (engine.maker_gateway_port, engine.obs_port);
            let config = SolverConfig {
                    rpc: RpcConfig {
                        endpoint: "http://unused".into(),
                        timeout_ms: 1_000,
                        prover_endpoint: None,
                        prover_timeout_ms: 45_000,
                    },
                    solver: SolverAccountConfig {
                        account_id: solver_id.to_hex(),
                        keystore_path: solver_keystore.to_string_lossy().into_owned(),
                        executor_store_path: solver_store.to_string_lossy().into_owned(),
                        ingest_store_path: ingest_store.to_string_lossy().into_owned(),
                        read_pool_size: 2,
                    },
                    pairs: vec![common::pair_config(
                        "USDC-ETH",
                        usdc.id(),
                        "USDC",
                        eth.id(),
                        "ETH",
                        "ETHUSDC",
                    )],
                    engine,
                    binance: binance.config(),
                };
            let api_key = register_maker(&pg.url, "mm1").await?;
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(),
                ingest_store,
                executor_store: solver_store,
                keystore: solver_keystore,
            });
            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(factory, solver_id, config, solver_cancel).await
            });

            let run: Result<()> = async {
                let mut gateway =
                    MakerClient::connect(port, &api_key, || solver_handle.is_finished()).await?;
                let mut events = gateway.events().await?;

                // 1. The maker sends its transaction: sell 1 ETH for 100 USDC,
                //    as a private note. The node accepts it; it waits for a block.
                let request = TransactionRequestBuilder::new()
                    .build_pswap_create(
                        &PswapTransactionData::new(
                            maker.id(),
                            FungibleAsset::new(eth.id(), 1)?,
                            FungibleAsset::new(usdc.id(), 100)?,
                        ),
                        NoteType::Private,
                        NoteType::Public,
                        None,
                        users.rng(),
                    )
                    .map_err(|e| anyhow!("maker order: {e}"))?;
                let order = request
                    .expected_output_own_notes()
                    .into_iter()
                    .next()
                    .context("the maker's request creates no note")?;
                Box::pin(users.submit_new_transaction(maker.id(), request))
                    .await
                    .map_err(|e| anyhow!("maker transaction: {e}"))?;

                // 2. Then SubmitOrder. Today the order waits for its note: no
                //    LIVE while the note is not in a block, and the watcher
                //    reports one pending lineage.
                let reply = gateway.submit(&order).await?;
                ensure!(
                    matches!(
                        reply.result,
                        Some(proto::command_reply::Result::Accepted(_))
                    ),
                    "submit not accepted: {reply:?}"
                );
                expect_no_event(&mut events, Duration::from_secs(2), |kind| {
                    is_order_status(kind, &order, proto::OrderState::Live)
                })
                .await
                .context("the order went LIVE before its note was in a block")?;
                wait_for_metric(obs_port, "solver_maker_pending_lineages", |v| v == 1.0).await?;

                // 3. The note lands; the watcher makes the order LIVE.
                rpc.prove_block();
                wait_for_event(&mut events, Duration::from_secs(120), |kind| {
                    is_order_status(kind, &order, proto::OrderState::Live)
                })
                .await
                .context("the maker order never went LIVE")?;
                wait_for_metric(obs_port, "solver_maker_pending_lineages", |v| v == 0.0).await?;
                wait_for_metric(obs_port, "solver_maker_watcher_synced_block", |v| v > 0.0).await?;

                // 4. A user buys 1 ETH for up to 120 USDC; the two trade at 100.
                let user_order = TransactionRequestBuilder::new()
                    .build_pswap_create(
                        &PswapTransactionData::new(
                            alice.id(),
                            FungibleAsset::new(usdc.id(), 120)?,
                            FungibleAsset::new(eth.id(), 1)?,
                        ),
                        NoteType::Public,
                        NoteType::Public,
                        None,
                        users.rng(),
                    )
                    .map_err(|e| anyhow!("user order: {e}"))?;
                Box::pin(users.submit_new_transaction(alice.id(), user_order))
                    .await
                    .map_err(|e| anyhow!("user transaction: {e}"))?;
                rpc.prove_block();
                wait_for_submitted_settlement(&pg.url, Duration::from_secs(900), || {
                    solver_handle.is_finished()
                })
                .await?;
                wait_for_event(&mut events, Duration::from_secs(60), |kind| {
                    matches!(kind, proto::event_body::Kind::SettlementPending(_))
                })
                .await
                .context("no SettlementPending for the maker")?;
                // SettlementPending commits before the executor submits, so keep
                // proving blocks until the settlement lands (well inside its
                // 20-block validity window).
                let mut resolved = false;
                for _ in 0..10 {
                    rpc.prove_block();
                    match wait_for_event(&mut events, Duration::from_secs(3), |kind| {
                        matches!(
                            kind,
                            proto::event_body::Kind::SettlementResolved(resolved) if resolved.committed
                        )
                    })
                    .await
                    {
                        Ok(_) => {
                            resolved = true;
                            break;
                        }
                        Err(error) if error.to_string().starts_with("event not seen within") => {}
                        Err(error) => return Err(error),
                    }
                }
                ensure!(resolved, "the maker's settlement never resolved as committed");

                // The order row is retired as executed.
                let url = pg.url.clone();
                let note_id = order.id().to_bytes();
                let status = tokio::task::spawn_blocking(move || -> Result<String> {
                    use solver::db::postgres_schema::orders;
                    let mut conn = solver::db::postgres_migrations::connect(&url)?;
                    Ok(orders::table
                        .find(note_id)
                        .select(orders::status)
                        .get_result(&mut conn)?)
                })
                .await??;
                ensure!(status == "executed", "maker order is {status}, not executed");

                // Nothing was refused or dropped on the way.
                let metrics = scrape(obs_port).await?;
                for line in metrics.lines() {
                    if line.starts_with("solver_executor_settlement_refusals_total")
                        || line.starts_with("solver_executor_settlements_dropped_total")
                    {
                        ensure!(line.ends_with(" 0"), "unexpected {line}");
                    }
                }
                Ok(())
            }
            .await;

            // Always stop the solver, so no client thread outlives the test.
            cancel.cancel();
            if !solver_handle.is_finished() {
                let _ = tokio::time::timeout(Duration::from_secs(30), &mut solver_handle).await;
            }
            drop(user_temp);
            drop(solver_temp);
            run
        })
        .await
}

/// The solver's `/metrics` text.
async fn scrape(obs_port: u16) -> Result<String> {
    Ok(reqwest::get(format!("http://127.0.0.1:{obs_port}/metrics"))
        .await?
        .error_for_status()?
        .text()
        .await?)
}

/// Wait until the metric `name` (no labels) satisfies `want`.
async fn wait_for_metric(obs_port: u16, name: &str, want: impl Fn(f64) -> bool) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        if let Ok(body) = scrape(obs_port).await {
            last = body.lines().find_map(|line| {
                line.strip_prefix(name)?
                    .strip_prefix(' ')?
                    .parse::<f64>()
                    .ok()
            });
            if last.is_some_and(&want) {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "{name} never reached the expected value; last {last:?}"
    ))
}

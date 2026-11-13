//! Maker gateway on the mock chain: a maker order end to end, on today's
//! path and live at accept (ADR 0008).
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
    PaymentNoteDescription, PswapTransactionData, TransactionRequest, TransactionRequestBuilder,
};
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
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

/// A solver on the mock chain with the maker gateway on, two faucets, a user
/// (Alice) holding USDC and a maker holding ETH, all in one test client.
struct Scenario {
    pg: PgSchema,
    rpc: Arc<NodeLikeRpc>,
    users: TestClient,
    usdc: AccountId,
    eth: AccountId,
    alice: AccountId,
    maker: AccountId,
    obs_port: u16,
    gateway: MakerClient,
    events: tonic::Streaming<proto::StreamMessage>,
    cancel: CancellationToken,
    solver: tokio::task::JoinHandle<Result<()>>,
    // Stops serving prices when dropped.
    _binance: common::BinanceStub,
    /// `/metrics` when the scenario started. Counters are process-wide and
    /// earlier scenarios in this process leave theirs behind.
    baseline: String,
    _temps: [tempfile::TempDir; 2],
}

impl Scenario {
    /// Start the solver with `engine` adjusting the engine settings. Runs
    /// inside a `LocalSet`.
    async fn start(engine_setup: impl FnOnce(&mut solver::config::EngineConfig)) -> Result<Self> {
        // `RUST_LOG=solver=info` shows the solver's log while debugging.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let pg = PgSchema::new().await?;
        let rpc = NodeLikeRpc::new(Arc::new(MockRpcApi::new(MockChain::new())));

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
        engine_setup(&mut engine);
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
        let solver = tokio::task::spawn_local(async move {
            solver::start(factory, solver_id, config, solver_cancel).await
        });
        let connected = MakerClient::connect(port, &api_key, || solver.is_finished()).await;
        let mut gateway = match connected {
            Ok(gateway) => gateway,
            Err(error) => {
                cancel.cancel();
                let _ = tokio::time::timeout(Duration::from_secs(30), solver).await;
                return Err(error);
            }
        };
        let events = gateway.events().await?;
        // The watcher has read the chain once: an order can go live at
        // accept from now on.
        wait_for_metric(obs_port, "solver_maker_watcher_synced_block", |v| v > 0.0).await?;
        let baseline = scrape(obs_port).await?;
        Ok(Self {
            pg,
            rpc,
            users,
            usdc: usdc.id(),
            eth: eth.id(),
            alice: alice.id(),
            maker: maker.id(),
            obs_port,
            gateway,
            events,
            cancel,
            solver,
            _binance: binance,
            baseline,
            _temps: [user_temp, solver_temp],
        })
    }

    /// Run `body`, then always stop the solver, so no client thread outlives
    /// the test.
    async fn run(
        engine_setup: impl FnOnce(&mut solver::config::EngineConfig),
        body: impl for<'a> FnOnce(
            &'a mut Scenario,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<()>> + 'a>,
        >,
    ) -> Result<()> {
        let mut scenario = Self::start(engine_setup).await?;
        let result = body(&mut scenario).await;
        scenario.cancel.cancel();
        if !scenario.solver.is_finished() {
            let _ = tokio::time::timeout(Duration::from_secs(30), &mut scenario.solver).await;
        }
        result
    }

    fn stopped(&self) -> bool {
        self.solver.is_finished()
    }

    /// The maker's order, not sent yet: sell 1 ETH for 100 USDC as a private
    /// note.
    fn maker_order(&mut self) -> Result<(TransactionRequest, Note)> {
        let request = TransactionRequestBuilder::new()
            .build_pswap_create(
                &PswapTransactionData::new(
                    self.maker,
                    FungibleAsset::new(self.eth, 1)?,
                    FungibleAsset::new(self.usdc, 100)?,
                ),
                NoteType::Private,
                NoteType::Public,
                None,
                self.users.rng(),
            )
            .map_err(|e| anyhow!("maker order: {e}"))?;
        let order = request
            .expected_output_own_notes()
            .into_iter()
            .next()
            .context("the maker's request creates no note")?;
        Ok((request, order))
    }

    /// Send `account`'s transaction; the node accepts it and it waits for a
    /// block.
    async fn send(&mut self, account: AccountId, request: TransactionRequest) -> Result<()> {
        Box::pin(self.users.submit_new_transaction(account, request))
            .await
            .map_err(|e| anyhow!("transaction: {e}"))?;
        Ok(())
    }

    /// Alice buys 1 ETH for up to 120 USDC, publicly, and her order lands.
    async fn user_buys(&mut self) -> Result<()> {
        let order = TransactionRequestBuilder::new()
            .build_pswap_create(
                &PswapTransactionData::new(
                    self.alice,
                    FungibleAsset::new(self.usdc, 120)?,
                    FungibleAsset::new(self.eth, 1)?,
                ),
                NoteType::Public,
                NoteType::Public,
                None,
                self.users.rng(),
            )
            .map_err(|e| anyhow!("user order: {e}"))?;
        self.send(self.alice, order).await?;
        self.rpc.prove_block();
        // Ingest stores it.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while common::count_orders(&self.pg.url, "active").await < 1 {
            ensure!(!self.stopped(), "the solver stopped");
            ensure!(
                std::time::Instant::now() < deadline,
                "the user's order was never ingested"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }

    async fn submit(&mut self, order: &Note) -> Result<()> {
        let reply = self.gateway.submit(order).await?;
        ensure!(
            matches!(
                reply.result,
                Some(proto::command_reply::Result::Accepted(_))
            ),
            "submit not accepted: {reply:?}"
        );
        Ok(())
    }

    async fn wait_for_live(&mut self, order: &Note, max_wait: Duration) -> Result<()> {
        wait_for_event(&mut self.events, max_wait, |kind| {
            is_order_status(kind, order, proto::OrderState::Live)
        })
        .await
        .context("the maker order never went LIVE")?;
        Ok(())
    }

    /// Prove blocks until the maker's settlement resolves as committed, well
    /// inside its 20-block validity window.
    async fn prove_until_committed(&mut self) -> Result<()> {
        for _ in 0..10 {
            self.rpc.prove_block();
            match wait_for_event(&mut self.events, Duration::from_secs(3), |kind| {
                matches!(
                    kind,
                    proto::event_body::Kind::SettlementResolved(resolved) if resolved.committed
                )
            })
            .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.to_string().starts_with("event not seen within") => {}
                Err(error) => return Err(error),
            }
        }
        Err(anyhow!(
            "the maker's settlement never resolved as committed"
        ))
    }

    /// The order row's status and its lineage's state.
    async fn state(&self, order: &Note) -> Result<(String, String)> {
        let url = self.pg.url.clone();
        let note_id = order.id().to_bytes();
        tokio::task::spawn_blocking(move || -> Result<(String, String)> {
            use solver::db::postgres_schema::{maker_lineages, orders};
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            let status = orders::table
                .find(&note_id)
                .select(orders::status)
                .get_result(&mut conn)?;
            let lineage = maker_lineages::table
                .filter(maker_lineages::note_id.eq(&note_id))
                .select(maker_lineages::state)
                .get_result(&mut conn)?;
            Ok((status, lineage))
        })
        .await?
    }

    /// Wait until the order row has `status`.
    async fn wait_for_status(&self, order: &Note, status: &str) -> Result<()> {
        // Long enough for a full proof on a slow runner.
        let deadline = std::time::Instant::now() + Duration::from_secs(900);
        loop {
            if let Ok((current, _)) = self.state(order).await {
                if current == status {
                    return Ok(());
                }
            }
            ensure!(!self.stopped(), "the solver stopped");
            ensure!(
                std::time::Instant::now() < deadline,
                "the order never became {status}; it is {:?}",
                self.state(order).await.ok()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until the change in the sum of `name`'s series whose labels
    /// contain `labels`, since the scenario started, satisfies `want`.
    /// Counters are recorded just after the commit and send they count.
    async fn grew(&self, name: &str, labels: &str, want: impl Fn(f64) -> bool) -> Result<()> {
        let start = total(&self.baseline, name, labels);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let delta = total(&scrape(self.obs_port).await?, name, labels) - start;
            if want(delta) {
                return Ok(());
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "{name}{{{labels}}} changed by {delta} since the scenario started"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn live_at_accept(engine: &mut solver::config::EngineConfig) {
    engine.maker_live_at_accept = true;
}

/// Sum of every series of `name`, whatever its labels, whose labels contain
/// `labels`.
fn total(metrics: &str, name: &str, labels: &str) -> f64 {
    metrics
        .lines()
        .filter(|line| line.starts_with(name) && line.contains(labels))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Today's path end to end: the maker sends its transaction, then
/// `SubmitOrder`; the order goes LIVE once its note is in a block, trades
/// against a user's order, and the maker sees the settlement events.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_maker_order_goes_live_after_its_note_lands_and_trades() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(Scenario::run(
            |_| {},
            |s| {
                Box::pin(async move {
                    // 1. The maker sends its transaction; it waits for a block.
                    let (request, order) = s.maker_order()?;
                    s.send(s.maker, request).await?;

                    // 2. Then SubmitOrder. With the switch off the order waits for
                    //    its note: no LIVE before the block, one pending lineage.
                    s.submit(&order).await?;
                    expect_no_event(&mut s.events, Duration::from_secs(2), |kind| {
                        is_order_status(kind, &order, proto::OrderState::Live)
                    })
                    .await
                    .context("the order went LIVE before its note was in a block")?;
                    wait_for_metric(s.obs_port, "solver_maker_pending_lineages", |v| v == 1.0)
                        .await?;

                    // 3. The note lands; the watcher makes the order LIVE.
                    s.rpc.prove_block();
                    s.wait_for_live(&order, Duration::from_secs(120)).await?;
                    wait_for_metric(s.obs_port, "solver_maker_pending_lineages", |v| v == 0.0)
                        .await?;
                    wait_for_metric(s.obs_port, "solver_maker_watcher_synced_block", |v| v > 0.0)
                        .await?;

                    // 4. A user buys 1 ETH for up to 120 USDC; the two trade at 100.
                    s.user_buys().await?;
                    wait_for_submitted_settlement(&s.pg.url, Duration::from_secs(900), || {
                        s.stopped()
                    })
                    .await?;
                    wait_for_event(&mut s.events, Duration::from_secs(60), |kind| {
                        matches!(kind, proto::event_body::Kind::SettlementPending(_))
                    })
                    .await
                    .context("no SettlementPending for the maker")?;
                    s.prove_until_committed().await?;
                    let (status, lineage) = s.state(&order).await?;
                    ensure!(
                        status == "executed",
                        "maker order is {status}, not executed"
                    );
                    ensure!(lineage == "activated", "lineage is {lineage}");

                    // Nothing was refused or dropped on the way.
                    s.grew("solver_executor_settlement_refusals_total", "", |d| {
                        d == 0.0
                    })
                    .await?;
                    s.grew("solver_executor_settlements_dropped_total", "", |d| {
                        d == 0.0
                    })
                    .await?;
                    Ok(())
                })
            },
        ))
        .await
}

/// Live at accept, the common case: the maker's order is LIVE before its note
/// is in any block, trades against a resting user order at once, and the
/// settlement lands in the same block as the maker's transaction, so the
/// watcher never sees the note; our confirmation activates the lineage.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_live_at_accept_order_trades_in_the_block_that_creates_it() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(Scenario::run(live_at_accept, |s| {
            Box::pin(async move {
                s.user_buys().await?;
                let (request, order) = s.maker_order()?;
                s.send(s.maker, request).await?;
                s.submit(&order).await?;
                // LIVE at once: no block is proved until the settlement is
                // submitted.
                s.wait_for_live(&order, Duration::from_secs(10)).await?;
                wait_for_submitted_settlement(&s.pg.url, Duration::from_secs(900), || s.stopped())
                    .await?;
                s.prove_until_committed().await?;

                let (status, lineage) = s.state(&order).await?;
                ensure!(
                    status == "executed",
                    "maker order is {status}, not executed"
                );
                ensure!(lineage == "activated", "lineage is {lineage}");
                s.grew("solver_maker_live_at_accept_total", "", |d| d == 1.0)
                    .await?;
                s.grew(
                    "solver_maker_unconfirmed_activations_total",
                    "via=\"settlement\"",
                    |d| d == 1.0,
                )
                .await
                .context("not activated by our settlement")?;
                s.grew("solver_executor_settlement_refusals_total", "", |d| {
                    d == 0.0
                })
                .await?;
                Ok(())
            })
        }))
        .await
}

/// SubmitOrder before the transaction: the node refuses our settlement for
/// the unknown note, the order waits out of the book (still LIVE to the
/// maker), the user's order goes back, and once the maker's transaction
/// lands the order returns and trades.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn an_order_whose_note_is_unknown_waits_then_trades() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(Scenario::run(live_at_accept, |s| {
            Box::pin(async move {
                s.user_buys().await?;
                let (request, order) = s.maker_order()?;
                s.submit(&order).await?;
                s.wait_for_live(&order, Duration::from_secs(10)).await?;
                s.wait_for_status(&order, "waiting").await?;
                s.grew(
                    "solver_executor_settlement_conflicts_total",
                    "kind=\"unknown_notes\"",
                    |d| d >= 1.0,
                )
                .await?;
                s.grew("solver_maker_sent_to_waiting_total", "", |d| d >= 1.0)
                    .await?;
                s.grew("solver_maker_unknown_notes_named_total", "", |d| d >= 1.0)
                    .await?;
                wait_for_metric(s.obs_port, "solver_maker_waiting_orders", |v| v == 1.0).await?;
                // The voided settlement still shows the order live.
                wait_for_event(&mut s.events, Duration::from_secs(10), |kind| {
                    matches!(
                        kind,
                        proto::event_body::Kind::SettlementResolved(resolved)
                            if !resolved.committed
                                && resolved.result.as_ref().is_some_and(|r| {
                                    r.status() == proto::ResultingStatus::Live
                                })
                    )
                })
                .await
                .context("no voided settlement reporting the order live")?;

                // The maker's transaction lands; the order returns and trades.
                s.send(s.maker, request).await?;
                s.rpc.prove_block();
                s.wait_for_status(&order, "active").await?;
                wait_for_submitted_settlement(&s.pg.url, Duration::from_secs(900), || s.stopped())
                    .await?;
                s.prove_until_committed().await?;
                let (status, _) = s.state(&order).await?;
                ensure!(
                    status == "executed",
                    "maker order is {status}, not executed"
                );
                s.grew("solver_maker_waiting_returned_total", "", |d| d == 1.0)
                    .await?;
                Ok(())
            })
        }))
        .await
}

/// A note that never arrives: once the cap's time and blocks have passed,
/// the order is cancelled with `note_not_received`.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn an_order_whose_note_never_arrives_is_capped() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(Scenario::run(
            |engine| {
                live_at_accept(engine);
                engine.maker_unconfirmed_cancel_after_secs = 10;
                engine.maker_unconfirmed_cancel_after_blocks = 2;
            },
            |s| {
                Box::pin(async move {
                    let (_, order) = s.maker_order()?;
                    s.submit(&order).await?;
                    s.wait_for_live(&order, Duration::from_secs(10)).await?;
                    for _ in 0..3 {
                        s.rpc.prove_block();
                    }
                    wait_for_event(&mut s.events, Duration::from_secs(60), |kind| {
                        matches!(
                            kind,
                            proto::event_body::Kind::OrderStatus(status)
                                if status.state() == proto::OrderState::Cancelled
                                    && status.reason == "note_not_received"
                        )
                    })
                    .await
                    .context("the order was never capped")?;
                    let (status, lineage) = s.state(&order).await?;
                    ensure!(
                        (status.as_str(), lineage.as_str()) == ("stopped", "rejected"),
                        "after the cap: {status}, {lineage}"
                    );
                    s.grew("solver_maker_cap_cancels_total", "", |d| d == 1.0)
                        .await?;
                    Ok(())
                })
            },
        ))
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

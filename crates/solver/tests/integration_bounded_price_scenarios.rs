//! Bounded clearing price (ADR 0007) scenarios: real PSWAP orders on the mock
//! chain, the real solver, and the real price API over HTTP. Each checks what
//! the chain pays every order, to the unit, and what the API quoted.
//!
//! The pair is USDC/ETH with USDC as base, priced by Binance's ETHUSDC at 2,
//! so the pair's mid is 0.5 ETH per USDC and a 30 bps band is
//! [0.4985, 0.5015]. "Sellers" sell USDC for ETH, "buyers" offer ETH for
//! USDC. Both test faucets have 10 decimals and a supply of 9,999,999 units.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::TransactionRequestBuilder;
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::rand::FeltRng;
use miden_protocol::note::Note;
use miden_standards::note::{P2idNote, P2idNoteStorage, PswapNote, PswapNoteStorage};
use miden_testing::MockChain;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use common::{build_test_client, temp_paths, vault_balance, MockClientFactory, PgSchema};

const SCHEME: AuthSchemeId = AuthSchemeId::Falcon512Poseidon2;
const MODE: AccountType = AccountType::Public;

/// Binance quotes: a 10 bps spread around each mid, so a solver pricing at
/// the bid or the ask instead of the midpoint would show.
fn markets() -> Vec<mock_binance::Market> {
    let market = |symbol: &str, base: &str, quote: &str, bid: &str, ask: &str| {
        mock_binance::Market::new(symbol, base, quote, bid, ask)
    };
    vec![
        market("ETHUSDC", "ETH", "USDC", "1.999", "2.001"),
        market("ETHUSDT", "ETH", "USDT", "1.999", "2.001"),
        market("USDCUSDT", "USDC", "USDT", "0.9995", "1.0005"),
        market("BTCUSDC", "BTC", "USDC", "1.999", "2.001"),
        market("BTCUSDT", "BTC", "USDT", "1.999", "2.001"),
    ]
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Offered and requested (faucet, amount).
#[derive(Clone, Copy, Debug)]
struct Order((AccountId, u64), (AccountId, u64));

/// One order as placed, to read its outcome back.
#[derive(Clone, Copy, Debug)]
struct Placed {
    owner: AccountId,
    offered: (AccountId, u64),
    requested: (AccountId, u64),
}

/// What happened to one order on chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Outcome {
    /// Offered units that left the order (filled).
    spent: u64,
    /// Requested units paid back to the owner.
    received: u64,
    /// Offered units still on a live PSWAP note.
    left: u64,
}

struct Qa {
    pg: PgSchema,
    rpc: Arc<MockRpcApi>,
    client: TestClient,
    usdc: AccountId,
    eth: AccountId,
    btc: AccountId,
    solver_id: AccountId,
    factory: Arc<dyn solver::ClientFactory>,
    keystore: String,
    executor_store: String,
    ingest_store: String,
    binance: mock_binance::MockThread,
    port: u16,
    solver: Option<(CancellationToken, tokio::task::JoinHandle<Result<()>>)>,
    _temps: (TempDir, TempDir),
}

impl Qa {
    async fn new() -> Result<Self> {
        let pg = PgSchema::new().await?;
        let rpc = Arc::new(MockRpcApi::new(MockChain::new()));
        let (user_temp, user_keystore, user_store) = temp_paths()?;
        let mut client =
            TestClient::new(build_test_client(rpc.clone(), user_keystore, user_store).await?);
        client
            .ensure_genesis_in_place()
            .await
            .map_err(|e| anyhow!("genesis: {e}"))?;
        let mut faucets = Vec::new();
        for _ in 0..3 {
            let (account, _) = client
                .insert_account(AccountSetup::faucet(MODE).auth_scheme(SCHEME))
                .await?;
            faucets.push(account.id());
        }
        rpc.prove_block();
        client.sync_state().await?;

        let (solver_temp, solver_keystore, solver_store) = temp_paths()?;
        let solver_id = {
            let mut solver_client = TestClient::new(
                build_test_client(rpc.clone(), solver_keystore.clone(), solver_store.clone())
                    .await?,
            );
            solver_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow!("solver genesis: {e}"))?;
            let (account, _) = solver_client
                .insert_account(AccountSetup::wallet(MODE).auth_scheme(SCHEME))
                .await?;
            account.id()
        };
        let ingest = solver_temp.path().join("ingest_store.sqlite3");
        let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
            rpc: rpc.clone(),
            ingest_store: ingest.clone(),
            executor_store: solver_store.clone(),
            keystore: solver_keystore.clone(),
        });
        let mut qa = Self {
            pg,
            rpc,
            client,
            usdc: faucets[0],
            eth: faucets[1],
            btc: faucets[2],
            solver_id,
            factory,
            keystore: solver_keystore.to_string_lossy().into_owned(),
            executor_store: solver_store.to_string_lossy().into_owned(),
            ingest_store: ingest.to_string_lossy().into_owned(),
            binance: mock_binance::MockThread::start(markets(), Default::default()),
            port: free_port(),
            solver: None,
            _temps: (user_temp, solver_temp),
        };
        // A faucet is on chain after its first transaction, and the solver
        // reads every token's decimals at startup: mint one unit of each.
        qa.wallet(&[(qa.usdc, 1), (qa.eth, 1), (qa.btc, 1)]).await?;
        Ok(qa)
    }

    /// A wallet holding `assets`.
    async fn wallet(&mut self, assets: &[(AccountId, u64)]) -> Result<AccountId> {
        let (wallet, _) = self
            .client
            .insert_account(AccountSetup::wallet(MODE).auth_scheme(SCHEME))
            .await?;
        self.rpc.prove_block();
        self.client.sync_state().await?;
        for &(faucet, amount) in assets {
            let request = TransactionRequestBuilder::new().build_mint_fungible_asset(
                FungibleAsset::new(faucet, amount)?,
                wallet.id(),
                NoteType::Public,
                self.client.rng(),
            )?;
            self.client
                .execute_tx_and_consume_output_notes(request, faucet, wallet.id())
                .await?;
            self.rpc.prove_block();
            self.client.sync_state().await?;
        }
        Ok(wallet.id())
    }

    /// A wallet funded for `order`, and the order placed from it.
    async fn trader(&mut self, order: Order, whole: bool) -> Result<Placed> {
        let owner = self.wallet(&[order.0]).await?;
        self.place(owner, order, whole).await
    }

    /// A PSWAP from `owner`, all-or-nothing when `whole`, committed in the
    /// next block.
    async fn place(&mut self, owner: AccountId, order: Order, whole: bool) -> Result<Placed> {
        let Order(offered, requested) = order;
        let (offered_asset, requested_asset) = (
            FungibleAsset::new(offered.0, offered.1)?,
            FungibleAsset::new(requested.0, requested.1)?,
        );
        let storage = PswapNoteStorage::builder()
            .min_requested_asset(requested_asset)
            .creator_account_id(owner)
            .payback_note_type(NoteType::Public);
        let storage = if whole {
            storage.min_fill_step(requested_asset.amount()).build()
        } else {
            storage.build()
        };
        let note: Note = PswapNote::builder()
            .sender(owner)
            .storage(storage)
            .serial_number(self.client.rng().draw_word())
            .note_type(NoteType::Public)
            .offered_asset(offered_asset)
            .build()?
            .into();
        let request = TransactionRequestBuilder::new()
            .own_output_notes(vec![note])
            .build()?;
        Box::pin(self.client.submit_new_transaction(owner, request))
            .await
            .map_err(|e| anyhow!("submit pswap: {e}"))?;
        self.rpc.prove_block();
        self.client.sync_state().await?;
        Ok(Placed {
            owner,
            offered,
            requested,
        })
    }

    /// Sell `usdc` USDC for at least `eth` ETH.
    fn sell(&self, usdc: u64, eth: u64) -> Order {
        Order((self.usdc, usdc), (self.eth, eth))
    }

    /// Offer `eth` ETH for at least `usdc` USDC.
    fn buy(&self, eth: u64, usdc: u64) -> Order {
        Order((self.eth, eth), (self.usdc, usdc))
    }

    fn config(&self, band_bps: u16, fee_ppm: u32, btc_pair: bool) -> SolverConfig {
        let mut pair =
            common::pair_config("USDC-ETH", self.usdc, "USDC", self.eth, "ETH", "ETHUSDC");
        pair.band_bps = band_bps;
        // 100,000 ETH units: the size of the orders here.
        pair.reference_amount = Some("0.00001".into());
        let mut pairs = vec![pair];
        if btc_pair {
            // The same market and book, no band: a control.
            pairs.push(common::pair_config(
                "USDC-BTC", self.usdc, "USDC", self.btc, "BTC", "BTCUSDC",
            ));
        }
        let mut engine = common::engine_config();
        engine.clearing_fee_ppm = fee_ppm;
        engine.price_query_port = self.port;
        SolverConfig {
            rpc: RpcConfig {
                endpoint: "http://unused".into(),
                timeout_ms: 1_000,
                prover_endpoint: None,
                prover_timeout_ms: 45_000,
            },
            solver: SolverAccountConfig {
                account_id: self.solver_id.to_hex(),
                keystore_path: self.keystore.clone(),
                executor_store_path: self.executor_store.clone(),
                ingest_store_path: self.ingest_store.clone(),
                read_pool_size: 2,
            },
            pairs,
            engine,
            binance: solver::config::BinanceConfig {
                stream_endpoints: [self.binance.ws_url(), self.binance.ws_url()],
                rest_endpoint: self.binance.rest_url(),
                quote_ttl: Duration::from_secs(30),
                max_spread_bps: 100,
                retry_min: Duration::from_millis(50),
                retry_max: Duration::from_millis(500),
                ..Default::default()
            },
        }
    }

    /// Start the solver; wait until its price API answers and `active`
    /// orders are live in the book.
    async fn start(&mut self, config: SolverConfig, active: i64) -> Result<()> {
        let cancel = CancellationToken::new();
        let (factory, id, token) = (self.factory.clone(), self.solver_id, cancel.clone());
        let handle =
            tokio::task::spawn_local(
                async move { solver::start(factory, id, config, token).await },
            );
        self.solver = Some((cancel, handle));
        let started = Instant::now();
        while self.pair_price(self.eth, self.usdc).await.is_err() {
            if self.solver_stopped() {
                bail!("solver stopped during startup");
            }
            if started.elapsed() > Duration::from_secs(120) {
                bail!("price API did not come up");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.wait_active(active).await
    }

    /// Wait until at least `count` orders are active, then a moment for the
    /// price API's mirror of the book.
    async fn wait_active(&self, count: i64) -> Result<()> {
        let started = Instant::now();
        while common::count_orders(&self.pg.url, "active").await < count {
            if self.solver_stopped() {
                bail!("solver stopped before {count} orders went live");
            }
            if started.elapsed() > Duration::from_secs(120) {
                bail!("{count} orders did not go live");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some((cancel, mut handle)) = self.solver.take() {
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(30), &mut handle)
                .await
                .context("solver did not stop")???;
        }
        Ok(())
    }

    fn solver_stopped(&self) -> bool {
        self.solver
            .as_ref()
            .is_none_or(|(_, handle)| handle.is_finished())
    }

    async fn get(&self, path: &str) -> Result<Value> {
        let response = reqwest::get(format!("http://127.0.0.1:{}{path}", self.port)).await?;
        let status = response.status();
        let body: Value = serde_json::from_str(&response.text().await?)?;
        if !status.is_success() {
            bail!("{path}: {status} {body}");
        }
        Ok(body)
    }

    /// `/v2/swap-eta` for an order not placed yet.
    async fn swap_eta(&self, order: Order, whole: bool) -> Result<Value> {
        let Order(offered, requested) = order;
        let step = if whole {
            format!("&min_fill_step={}", requested.1)
        } else {
            String::new()
        };
        self.get(&format!(
            "/v2/swap-eta?offered_faucet={}&requested_faucet={}&offered_amount={}&requested_amount={}{step}",
            offered.0.to_hex(),
            requested.0.to_hex(),
            offered.1,
            requested.1
        ))
        .await
    }

    async fn pair_price(&self, offered: AccountId, requested: AccountId) -> Result<Value> {
        self.get(&format!(
            "/v2/pair-price?offered_faucet={}&requested_faucet={}",
            offered.to_hex(),
            requested.to_hex()
        ))
        .await
    }

    async fn attempts(&self) -> Result<i64> {
        let url = self.pg.url.clone();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            use solver::db::postgres_schema::settlement_attempts;
            let mut conn = solver::db::postgres_migrations::connect(&url)?;
            Ok(settlement_attempts::table.count().get_result(&mut conn)?)
        })
        .await?
    }

    /// Whether a settlement is submitted within `wait`.
    async fn submitted_within(&self, wait: Duration) -> Result<bool> {
        let started = Instant::now();
        while started.elapsed() < wait {
            if self.attempts().await? > 0 {
                return Ok(true);
            }
            if self.solver_stopped() {
                bail!("solver stopped");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(false)
    }

    /// Commit the submitted settlement and wait until the solver confirms it.
    /// The attempt row is written just before submission, so keep proving
    /// blocks until the transaction is in one.
    async fn commit(&self) -> Result<()> {
        let started = Instant::now();
        while self.attempts().await? > 0 {
            if started.elapsed() > Duration::from_secs(120) {
                bail!("settlement was not confirmed");
            }
            if self.solver_stopped() {
                bail!("solver stopped before confirming");
            }
            self.rpc.prove_block();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    /// One settlement: submitted, committed, confirmed.
    async fn settle(&self) -> Result<()> {
        if !self.submitted_within(Duration::from_secs(600)).await? {
            bail!("no settlement was submitted");
        }
        self.commit().await
    }

    /// Nothing is submitted for `secs` seconds.
    async fn quiet_for(&self, secs: u64) -> Result<()> {
        if self.submitted_within(Duration::from_secs(secs)).await? {
            bail!("a settlement was submitted");
        }
        Ok(())
    }

    /// Payback amounts on chain per (owner, faucet): the P2ID notes the
    /// solver created.
    fn paybacks(&self) -> HashMap<(AccountId, AccountId), u64> {
        let chain = self.rpc.mock_chain.read();
        let mut paid = HashMap::new();
        for note in chain
            .committed_notes()
            .values()
            .filter_map(|note| note.note())
        {
            if note.metadata().sender() != self.solver_id
                || note.recipient().script().root() != P2idNote::script_root()
            {
                continue;
            }
            let Ok(storage) = P2idNoteStorage::try_from(note.recipient().storage().items()) else {
                continue;
            };
            for asset in note.assets().iter().filter_map(|asset| asset.as_fungible()) {
                *paid
                    .entry((storage.target(), asset.faucet_id()))
                    .or_insert(0) += asset.amount().as_u64();
            }
        }
        paid
    }

    /// The order's offered units still on an unspent PSWAP note.
    fn left(&self, order: &Placed) -> u64 {
        let chain = self.rpc.mock_chain.read();
        chain
            .committed_notes()
            .values()
            .filter_map(|note| note.note())
            .filter(|note| note.recipient().script().root() == PswapNote::script_root())
            .filter(|note| {
                chain
                    .nullifier_tree()
                    .get_block_num(&note.nullifier())
                    .is_none()
            })
            .filter_map(|note| PswapNote::try_from(note).ok())
            .filter(|pswap| pswap.storage().creator_account_id() == order.owner)
            .map(|pswap| pswap.offered_asset().amount().as_u64())
            .sum()
    }

    fn outcome(&self, order: &Placed) -> Outcome {
        let left = self.left(order);
        Outcome {
            spent: order.offered.1 - left,
            received: self
                .paybacks()
                .get(&(order.owner, order.requested.0))
                .copied()
                .unwrap_or(0),
            left,
        }
    }

    fn solver_holds(&self, faucet: AccountId) -> u64 {
        vault_balance(&self.rpc.mock_chain.read(), self.solver_id, faucet)
    }

    fn report(&self, orders: &[(&str, &Placed)]) {
        for (name, order) in orders {
            println!("[test]   {name}: {:?}", self.outcome(order));
        }
        println!(
            "[test]   solver keeps {} USDC, {} ETH",
            self.solver_holds(self.usdc),
            self.solver_holds(self.eth)
        );
    }
}

type Body<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

/// One scenario at a time: each sets the process's database URLs and runs a
/// solver.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Run `body` on a LocalSet; always stop the solver.
async fn scenario<F>(body: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a mut Qa) -> Body<'a>,
{
    let _serial = SERIAL.lock().await;
    tokio::task::LocalSet::new()
        .run_until(async move {
            let mut qa = Qa::new().await?;
            let outcome = body(&mut qa).await;
            if let Err(error) = &outcome {
                println!(
                    "[test] FAILED: {error:#}\n[test]   paybacks {:?}\n[test]   solver keeps {} USDC, {} ETH",
                    qa.paybacks(),
                    qa.solver_holds(qa.usdc),
                    qa.solver_holds(qa.eth)
                );
            }
            let stopped = qa.stop().await;
            outcome?;
            stopped
        })
        .await
}

fn amount(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_str()
        .ok_or_else(|| anyhow!("{key} missing in {value}"))?
        .parse()
        .with_context(|| key.to_string())
}

fn full(spent: u64, received: u64) -> Outcome {
    Outcome {
        spent,
        received,
        left: 0,
    }
}

/// The mirror: a seller hits a maker's bid 20 bps below the mid.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_seller_hits_a_bid_below_the_mid_as_quoted() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let bid = qa.trader(qa.buy(499_000, 1_000_000), false).await?;
            qa.start(qa.config(30, 0, false), 1).await?;
            qa.quiet_for(2).await?;
            let order = qa.sell(200_000, 96_000);
            let quote = qa.swap_eta(order, false).await?;
            println!("[test] swap-eta {quote}");
            let expected = amount(&quote, "expectedRequestedAmount")?;
            let seller = qa.trader(order, false).await?;
            qa.settle().await?;
            qa.report(&[("bid", &bid), ("seller", &seller)]);
            // 200,000 x 0.499.
            let s = qa.outcome(&seller);
            if s != full(200_000, 99_800) || expected != 99_800 {
                bail!("seller: quoted {expected}, got {s:?}");
            }
            let b = qa.outcome(&bid);
            if (b.spent, b.received, b.left) != (99_800, 200_000, 399_200) {
                bail!("bid: {b:?}");
            }
            Ok(())
        })
    })
    .await
}

/// Users meeting around the mid clear at the mid, band or not, to the
/// unit.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn users_around_the_mid_clear_at_the_mid_band_or_not() -> Result<()> {
    let mut seen = Vec::new();
    for band in [0u16, 30] {
        let (tx, rx) = std::sync::mpsc::channel();
        scenario(move |qa| {
            Box::pin(async move {
                let a = qa.trader(qa.sell(100_000, 49_900), false).await?;
                let b = qa.trader(qa.buy(50_000, 99_800), false).await?;
                qa.start(qa.config(band, 0, false), 2).await?;
                qa.settle().await?;
                println!("[test] band {band}:");
                qa.report(&[("seller", &a), ("buyer", &b)]);
                let kept = (qa.solver_holds(qa.usdc), qa.solver_holds(qa.eth));
                tx.send((qa.outcome(&a), qa.outcome(&b), kept))?;
                Ok(())
            })
        })
        .await?;
        seen.push(rx.recv()?);
    }
    let want = (full(100_000, 50_000), full(50_000, 100_000), (0, 0));
    if seen != [want, want] {
        bail!("band 0 and band 30 should both clear at the mid: {seen:?}");
    }
    Ok(())
}

/// An ask 40 bps above the mid is out of reach; when Binance moves
/// toward it the band covers it and it settles.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_price_move_brings_an_ask_into_the_band() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let ask = qa.trader(qa.sell(100_000, 50_200), false).await?; // 0.502
            let buyer = qa.trader(qa.buy(50_200, 90_000), false).await?;
            qa.start(qa.config(30, 0, false), 2).await?;
            qa.quiet_for(4).await?;
            // ETHUSDC 2 -> 1.996: the mid is 0.501002 ETH per USDC and the
            // band's top 0.502505.
            qa.binance.set_quote("ETHUSDC", "1.995", "1.997");
            qa.settle().await?;
            qa.report(&[("ask", &ask), ("buyer", &buyer)]);
            if qa.outcome(&buyer) != full(50_200, 100_000)
                || qa.outcome(&ask) != full(100_000, 50_200)
            {
                bail!("both should fill at the ask, 0.502");
            }
            Ok(())
        })
    })
    .await
}

/// An ask exactly at the band's top settles there.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn an_ask_at_the_band_top_settles() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let ask = qa.trader(qa.sell(300_000, 150_450), false).await?; // 0.5015
            let buyer = qa.trader(qa.buy(150_450, 290_000), false).await?;
            qa.start(qa.config(30, 0, false), 2).await?;
            qa.settle().await?;
            qa.report(&[("ask", &ask), ("buyer", &buyer)]);
            if qa.outcome(&buyer) != full(150_450, 300_000)
                || qa.outcome(&ask) != full(300_000, 150_450)
            {
                bail!("both should fill at 0.5015 exactly");
            }
            Ok(())
        })
    })
    .await
}

/// An ask a hair above the band's top never settles, and the API
/// does not promise a fill against it.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn an_ask_past_the_band_top_never_settles() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            qa.trader(qa.sell(300_000, 150_453), false).await?; // 0.50151
            qa.trader(qa.buy(160_000, 290_000), false).await?;
            qa.start(qa.config(30, 0, false), 2).await?;
            qa.quiet_for(5).await?;
            let quote = qa.swap_eta(qa.buy(10_000, 19_000), false).await?;
            println!("[test] a new buyer's swap-eta {quote}");
            if quote["fillStatus"] == "full" {
                bail!("the API should not promise a fill");
            }
            Ok(())
        })
    })
    .await
}

/// A bid exactly at the band's bottom settles there.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_bid_at_the_band_bottom_settles() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let bid = qa.trader(qa.buy(149_550, 300_000), false).await?; // 0.4985
            let seller = qa.trader(qa.sell(300_000, 140_000), false).await?;
            qa.start(qa.config(30, 0, false), 2).await?;
            qa.settle().await?;
            qa.report(&[("bid", &bid), ("seller", &seller)]);
            if qa.outcome(&seller) != full(300_000, 149_550)
                || qa.outcome(&bid) != full(149_550, 300_000)
            {
                bail!("both should fill at 0.4985 exactly");
            }
            Ok(())
        })
    })
    .await
}

/// A buyer fills part of an ask; the remainder keeps the ask's limit
/// and fills against the next buyer, quoted exactly.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_remainder_fills_in_the_next_batch_as_quoted() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let ask = qa.trader(qa.sell(100_000, 50_050), false).await?; // 0.5005
            qa.start(qa.config(30, 0, false), 1).await?;
            let first = qa.trader(qa.buy(25_025, 45_000), false).await?;
            qa.settle().await?;
            println!("[test] after the first buyer:");
            qa.report(&[("ask", &ask), ("first", &first)]);
            qa.wait_active(1).await?;
            let order = qa.buy(25_025, 45_000);
            let quote = qa.swap_eta(order, false).await?;
            println!("[test] second buyer's swap-eta {quote}");
            let second = qa.trader(order, false).await?;
            qa.settle().await?;
            qa.report(&[("ask", &ask), ("first", &first), ("second", &second)]);
            let fills = (qa.outcome(&first).received, qa.outcome(&second).received);
            if fills != (50_000, 50_000) || qa.outcome(&ask) != full(100_000, 50_050) {
                bail!("each buyer 50,000 USDC, the ask 50,050 ETH: {fills:?}");
            }
            if amount(&quote, "expectedRequestedAmount")? != 50_000 {
                bail!("the second buyer's quote should be exact");
            }
            Ok(())
        })
    })
    .await
}

/// All-or-nothing orders make the walk's price untradeable; the
/// matcher falls back to the mid, where two users trade, and the
/// all-or-nothing orders stay whole.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn nothing_at_the_band_price_falls_back_to_the_mid() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let a = qa.trader(qa.sell(100_000, 49_995), false).await?; // -1 bp
            let b = qa.trader(qa.buy(50_003, 100_000), false).await?; // +0.6 bp
            let whole_ask = qa.trader(qa.sell(1_000_000, 500_500), true).await?; // +10 bps
            let whole_bid = qa.trader(qa.buy(104_000, 200_000), true).await?; // +400 bps
            qa.start(qa.config(30, 0, false), 4).await?;
            qa.settle().await?;
            qa.quiet_for(3).await?;
            qa.report(&[
                ("a", &a),
                ("b", &b),
                ("whole ask", &whole_ask),
                ("whole bid", &whole_bid),
            ]);
            // a fills whole at the mid; b, larger than a, fills in part at
            // its own rate (50,003 ETH for 100,000 USDC).
            if qa.outcome(&a) != full(100_000, 50_000)
                || qa.outcome(&b)
                    != (Outcome {
                        spent: 50_000,
                        received: 99_996,
                        left: 3,
                    })
            {
                bail!("a and b should trade at the mid");
            }
            if qa.outcome(&whole_ask).spent != 0 || qa.outcome(&whole_bid).spent != 0 {
                bail!("the all-or-nothing orders should not trade");
            }
            Ok(())
        })
    })
    .await
}

/// With a 0.1% fee: the ask's fee-inclusive limit sits inside the
/// band, the buyer is paid what the quote said, and the solver keeps the
/// difference.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_fee_keeps_the_quote_exact() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            // 0.50075 raw, 0.501251 after the fee: inside the band.
            let ask = qa.trader(qa.sell(300_000, 150_225), false).await?;
            qa.start(qa.config(30, 1_000, false), 1).await?;
            qa.quiet_for(2).await?;
            let order = qa.buy(100_000, 190_000);
            let quote = qa.swap_eta(order, false).await?;
            let price = qa.pair_price(qa.eth, qa.usdc).await?;
            println!("[test] swap-eta {quote}\n[test] pair-price {price}");
            let buyer = qa.trader(order, false).await?;
            qa.settle().await?;
            qa.report(&[("ask", &ask), ("buyer", &buyer)]);
            let b = qa.outcome(&buyer);
            if b.received != amount(&quote, "expectedRequestedAmount")? || b.left != 0 {
                bail!("buyer {b:?}, quoted {}", quote["expectedRequestedAmount"]);
            }
            if qa.solver_holds(qa.usdc) + qa.solver_holds(qa.eth) == 0 {
                bail!("the solver should keep the fee");
            }
            Ok(())
        })
    })
    .await
}

/// Two pairs on the same market and the same book, one with a band:
/// only the banded pair settles.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn the_band_is_per_pair() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            qa.trader(qa.sell(100_000, 50_100), false).await?; // +20 bps
            let banded = qa.trader(qa.buy(50_100, 90_000), false).await?;
            let btc_buy = Order((qa.btc, 50_100), (qa.usdc, 90_000));
            qa.trader(Order((qa.usdc, 100_000), (qa.btc, 50_100)), false)
                .await?;
            let unbanded = qa.trader(btc_buy, false).await?;
            qa.start(qa.config(30, 0, true), 4).await?;
            qa.settle().await?;
            qa.quiet_for(4).await?;
            qa.report(&[("banded buyer", &banded), ("unbanded buyer", &unbanded)]);
            if qa.outcome(&banded).received != 100_000 || qa.outcome(&unbanded).received != 0 {
                bail!("only the banded pair should settle");
            }
            let control = qa.swap_eta(btc_buy, false).await?;
            println!("[test] unbanded pair swap-eta {control}");
            if control["expectedPrice"] != control["marketPrice"] {
                bail!("without a band the expected price is the mid");
            }
            Ok(())
        })
    })
    .await
}

/// A buyer much larger than the only ask is filled in part, and a
/// partial fill pays the order's own limit (PSWAP until protocol#4041), far
/// outside the band.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_partial_fill_pays_its_own_limit() -> Result<()> {
    scenario(|qa| {
        Box::pin(async move {
            let ask = qa.trader(qa.sell(100_000, 50_100), false).await?; // 0.501
            qa.start(qa.config(30, 0, false), 1).await?;
            let order = qa.buy(260_000, 500_000); // 0.52, +400 bps
            let quote = qa.swap_eta(order, false).await?;
            println!("[test] swap-eta {quote}");
            let buyer = qa.trader(order, false).await?;
            qa.settle().await?;
            qa.report(&[("ask", &ask), ("buyer", &buyer)]);
            let b = qa.outcome(&buyer);
            println!(
                "[test] the buyer paid {:.6} ETH per USDC; mid 0.5, band top 0.5015",
                b.spent as f64 / b.received as f64
            );
            // It spends the batch-price value of the ask (50,100 ETH) and is
            // paid at its own 0.52: 96,348 USDC, not 100,000.
            if b != (Outcome {
                spent: 50_100,
                received: 96_348,
                left: 209_900,
            }) {
                bail!("expected 96,348 USDC for 50,100 ETH: {b:?}");
            }
            if quote["fillStatus"] != "partial"
                || amount(&quote, "fillableRequestedAmount")? != 96_347
            {
                bail!("swap-eta should say partial, about 96,347");
            }
            Ok(())
        })
    })
    .await
}

/// A random book with a band. Checks every order and the totals:
/// limits honoured, full fills inside the band, nothing past the band
/// trades, all-or-nothing orders fill whole or not at all, and every unit is
/// accounted for. `QA_SEED` picks the book.
#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_random_book_keeps_every_invariant() -> Result<()> {
    let seed: u64 = std::env::var("QA_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or(7);
    scenario(move |qa| {
        Box::pin(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut orders = Vec::new();
            for _ in 0..12 {
                let usdc: u64 = rng.random_range(10_000..150_000);
                let bps: i64 = rng.random_range(-60..=60);
                let whole = rng.random_range(0..4) == 0;
                // usdc x 0.5 x (1 + bps), up for a sell's floor, down for a
                // buy's offer.
                let num = i128::from(usdc) * i128::from(10_000 + bps);
                let (side, order) = if rng.random_bool(0.5) {
                    ("sell", qa.sell(usdc, ((num + 19_999) / 20_000) as u64))
                } else {
                    ("buy", qa.buy((num / 20_000) as u64, usdc))
                };
                let kind = if whole { " whole" } else { "" };
                println!("[test] {side} {usdc} USDC at {bps:+} bps{kind}");
                orders.push((side, whole, qa.trader(order, whole).await?));
            }
            qa.start(qa.config(30, 0, false), orders.len() as i64)
                .await?;
            let mut rounds = 0;
            while qa.submitted_within(Duration::from_secs(10)).await? {
                qa.commit().await?;
                rounds += 1;
                if rounds > 8 {
                    bail!("more than 8 settlements");
                }
            }
            println!("[test] seed {seed}: {rounds} settlements");
            let (low, high) = (0.4985, 0.5015);
            let mut consumed: HashMap<AccountId, u64> = HashMap::new();
            let mut paid_out: HashMap<AccountId, u64> = HashMap::new();
            for (side, whole, order) in &orders {
                let o = qa.outcome(order);
                *consumed.entry(order.offered.0).or_default() += o.spent;
                *paid_out.entry(order.requested.0).or_default() += o.received;
                let (offered, requested) = (order.offered.1, order.requested.1);
                // ETH per USDC.
                let (limit, rate) = if *side == "sell" {
                    (
                        requested as f64 / offered as f64,
                        o.received as f64 / o.spent.max(1) as f64,
                    )
                } else {
                    (
                        offered as f64 / requested as f64,
                        o.spent as f64 / o.received.max(1) as f64,
                    )
                };
                let kind = match (o.spent, o.left) {
                    (0, _) => "none",
                    (_, 0) => "full",
                    _ => "partial",
                };
                println!("[test]   {side} limit {limit:.6}: {kind} {o:?} at {rate:.6}");
                // The order's own limit, to the unit: received >= spent x
                // requested / offered, less one unit of rounding.
                let floor = u128::from(o.spent) * u128::from(requested);
                if o.spent > 0 && u128::from(o.received + 1) * u128::from(offered) < floor {
                    bail!("{side} paid below its limit: {o:?}");
                }
                if *whole && kind == "partial" {
                    bail!("an all-or-nothing order filled in part: {o:?}");
                }
                let past_band = if *side == "sell" {
                    limit > high
                } else {
                    limit < low
                };
                if past_band && o.spent > 0 {
                    bail!("an order past the band traded: {o:?}");
                }
                let slack = 2.0 / o.spent.min(o.received).max(1) as f64;
                if kind == "full" && (rate < low * (1.0 - slack) || rate > high * (1.0 + slack)) {
                    bail!("a full fill outside the band: {rate}");
                }
            }
            for faucet in [qa.usdc, qa.eth] {
                let inflow = consumed.get(&faucet).copied().unwrap_or(0);
                let outflow = paid_out.get(&faucet).copied().unwrap_or(0);
                let kept = qa.solver_holds(faucet);
                if inflow != outflow + kept {
                    bail!("units unaccounted for: {inflow} in, {outflow} out, {kept} kept");
                }
            }
            qa.report(&[]);
            Ok(())
        })
    })
    .await
}

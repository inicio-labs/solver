//! Integer completion through the real solver, PostgreSQL, transaction request,
//! prover and MockChain VM. Outputs must also be spendable by their creators.

mod common;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::TransactionRequestBuilder;
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::crypto::rand::FeltRng;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use miden_standards::note::{P2idNote, P2idNoteStorage, PswapNote, PswapNoteStorage};
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{build_test_client, temp_paths, vault_balance, MockClientFactory, PgSchema};

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    price: &'static str,
    fee: u32,
    seller: (u64, u64),
    buyer: (u64, u64),
    paid: (u64, u64),
    // Current offered/requested terms of the remaining notes, if any.
    seller_tail: Option<(u64, u64)>,
    buyer_tail: Option<(u64, u64)>,
    solver_holds: (u64, u64),
    buyer_step: u64,
    extra_buyer: Option<(u64, u64, u64, u64, (u64, u64))>,
}

fn live_pswaps(rpc: &MockRpcApi, owner: AccountId) -> Vec<Note> {
    let chain = rpc.mock_chain.read();
    chain
        .committed_notes()
        .values()
        .filter_map(|entry| entry.note())
        .filter(|note| {
            chain
                .nullifier_tree()
                .get_block_num(&note.nullifier())
                .is_none()
        })
        .filter(|note| note.recipient().script().root() == PswapNote::script_root())
        .filter(|note| {
            PswapNote::try_from(*note)
                .is_ok_and(|pswap| pswap.storage().creator_account_id() == owner)
        })
        .cloned()
        .collect()
}

fn paybacks(rpc: &MockRpcApi, solver: AccountId, owner: AccountId) -> Vec<Note> {
    let chain = rpc.mock_chain.read();
    chain
        .committed_notes()
        .values()
        .filter_map(|entry| entry.note())
        .filter(|note| note.metadata().sender() == solver)
        .filter(|note| note.recipient().script().root() == P2idNote::script_root())
        .filter(|note| {
            P2idNoteStorage::try_from(note.recipient().storage().items())
                .is_ok_and(|storage| storage.target() == owner)
        })
        .cloned()
        .collect()
}

async fn check_lineage(
    url: &str,
    parent: Vec<u8>,
    expected_tail: Option<(u64, u64)>,
    step: u64,
) -> Result<bool> {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || -> Result<bool> {
        use solver::db::postgres_schema::orders;
        let mut conn = solver::db::postgres_migrations::connect(&url)?;
        let Some((status, priority)) = orders::table
            .find(&parent)
            .select((orders::status, orders::priority_seq))
            .first::<(String, i64)>(&mut conn)
            .optional()?
        else {
            return Ok(false);
        };
        if status != "executed" {
            return Ok(false);
        }
        ensure!(priority > 0, "parent lacks durable FIFO priority");
        let children = orders::table
            .filter(orders::priority_seq.eq(priority))
            .filter(orders::note_id.ne(&parent))
            .select((orders::status, orders::raw_data))
            .load::<(String, Vec<u8>)>(&mut conn)?;
        match expected_tail {
            None => ensure!(children.is_empty(), "completed order has a persisted tail"),
            Some((offered, requested)) => {
                if children.is_empty() {
                    return Ok(false);
                }
                ensure!(children.len() == 1, "parent has duplicate descendants");
                ensure!(children[0].0 == "active", "tail is not active");
                let note = Note::read_from_bytes(&children[0].1)?;
                let tail = PswapNote::try_from(&note)?;
                ensure!(
                    tail.offered_asset().amount().as_u64() == offered,
                    "persisted offered terms changed"
                );
                ensure!(
                    tail.storage().min_requested_asset().amount().as_u64() == requested,
                    "persisted requested terms changed"
                );
                ensure!(
                    tail.storage().min_fill_step().as_u64() == step,
                    "tail lost inherited minimum step"
                );
            }
        }
        Ok(true)
    })
    .await?
}

fn check_tail(
    rpc: &MockRpcApi,
    owner: AccountId,
    expected: Option<(u64, u64)>,
    step: u64,
) -> Result<()> {
    let notes = live_pswaps(rpc, owner);
    match expected {
        None => ensure!(notes.is_empty(), "completed creator still has a live PSWAP"),
        Some((offered, requested)) => {
            ensure!(notes.len() == 1, "expected exactly one live remainder");
            let tail = PswapNote::try_from(&notes[0])?;
            ensure!(
                tail.offered_asset().amount().as_u64() == offered,
                "wrong on-chain remainder offered amount"
            );
            ensure!(
                tail.storage().min_requested_asset().amount().as_u64() == requested,
                "wrong on-chain remainder requested minimum"
            );
            ensure!(
                tail.storage().min_fill_step().as_u64() == step,
                "wrong on-chain remainder minimum step"
            );
        }
    }
    Ok(())
}

async fn run(case: Case) -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            println!("{}: preparing isolated fixture", case.name);
            let pg = PgSchema::new().await?;
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));
            let (_user_temp, user_keys, user_store) = temp_paths()?;
            let mut client = TestClient::new(build_test_client(rpc.clone(), user_keys, user_store).await?);
            client.ensure_genesis_in_place().await.map_err(|e| anyhow!("genesis: {e}"))?;
            let mode = AccountType::Public;
            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let (base, _) = client.insert_account(AccountSetup::faucet(mode).auth_scheme(scheme)).await?;
            let (quote, _) = client.insert_account(AccountSetup::faucet(mode).auth_scheme(scheme)).await?;
            let (seller, _) = client.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
            let (buyer, _) = client.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
            rpc.prove_block();
            client.sync_state().await?;
            let mut traders = vec![
                (seller.id(), FungibleAsset::new(base.id(), case.seller.0)?, FungibleAsset::new(quote.id(), case.seller.1)?, 1, case.paid.0, case.seller_tail),
                (buyer.id(), FungibleAsset::new(quote.id(), case.buyer.0)?, FungibleAsset::new(base.id(), case.buyer.1)?, case.buyer_step, case.paid.1, case.buyer_tail),
            ];
            if let Some((offered, requested, step, paid, tail)) = case.extra_buyer {
                let (extra, _) = client.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
                traders.push((extra.id(), FungibleAsset::new(quote.id(), offered)?, FungibleAsset::new(base.id(), requested)?, step, paid, Some(tail)));
            }
            let mut parents = Vec::new();
            for &(owner, offered, requested, step, _, _) in &traders {
                let mint = TransactionRequestBuilder::new().build_mint_fungible_asset(offered, owner, NoteType::Public, client.rng())?;
                client.execute_tx_and_consume_output_notes(mint, offered.faucet_id(), owner).await?;
                rpc.prove_block();
                client.sync_state().await?;
                let storage = PswapNoteStorage::builder().min_requested_asset(requested)
                    .min_fill_step(AssetAmount::new(step)?).creator_account_id(owner)
                    .payback_note_type(NoteType::Public).build();
                let note: Note = PswapNote::builder().sender(owner).storage(storage)
                    .serial_number(client.rng().draw_word()).note_type(NoteType::Public)
                    .offered_asset(offered).build()?.into();
                parents.push(note.id().to_bytes());
                let request = TransactionRequestBuilder::new().own_output_notes(vec![note]).build()?;
                Box::pin(client.submit_new_transaction(owner, request)).await?;
                rpc.prove_block();
                client.sync_state().await?;
            }

            let (_solver_temp, solver_keys, solver_store) = temp_paths()?;
            let solver_id = {
                let mut provision = TestClient::new(build_test_client(rpc.clone(), solver_keys.clone(), solver_store.clone()).await?);
                provision.ensure_genesis_in_place().await.map_err(|e| anyhow!("solver genesis: {e}"))?;
                let (account, _) = provision.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
                account.id()
            };
            let ingest_store = _solver_temp.path().join("ingest.sqlite3");
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(MockClientFactory {
                rpc: rpc.clone(), ingest_store: ingest_store.clone(),
                executor_store: solver_store.clone(), keystore: solver_keys.clone(),
            });
            let binance = common::BinanceStub::start(&[("BTCUSDT", "BTC", "USDT", case.price)]);
            let mut engine = common::engine_config();
            engine.clearing_fee_ppm = case.fee;
            engine.clearing_integer_completion_repair = true;
            let config = SolverConfig {
                rpc: RpcConfig { endpoint: "http://unused".into(), timeout_ms: 1_000, prover_endpoint: None, prover_timeout_ms: 45_000 },
                solver: SolverAccountConfig { account_id: solver_id.to_hex(), keystore_path: solver_keys.to_string_lossy().into_owned(), executor_store_path: solver_store.to_string_lossy().into_owned(), ingest_store_path: ingest_store.to_string_lossy().into_owned(), read_pool_size: 2 },
                pairs: vec![common::pair_config("IBTC-IUSDT", base.id(), "BTC", quote.id(), "USDT", "BTCUSDT")],
                engine, binance: binance.config(),
            };
            let cancel = CancellationToken::new();
            let token = cancel.clone();
            let mut handle = tokio::task::spawn_local(async move { solver::start(factory, solver_id, config, token).await });
            let verdict: Result<()> = async {
                common::wait_for_submitted_settlement(&pg.url, Duration::from_secs(600), || handle.is_finished()).await?;
                println!("{}: settlement submitted", case.name);
                let mut confirmed = false;
                for _ in 0..180 {
                    rpc.prove_block();
                    let mut all_confirmed = true;
                    for (parent, &(_, _, _, step, _, tail)) in parents.iter().zip(&traders) {
                        all_confirmed &= check_lineage(&pg.url, parent.clone(), tail, step).await?;
                    }
                    if all_confirmed { confirmed = true; break; }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                ensure!(confirmed, "settlement parents/remainders never confirmed");
                for &(owner, _, requested, step, expected, tail) in &traders {
                    let notes = paybacks(&rpc, solver_id, owner);
                    ensure!(notes.len() == 1, "missing or duplicate committed creator paybacks");
                    let asset = notes[0].assets().iter().next().context("payback has no asset")?.as_fungible().context("payback is not fungible")?;
                    ensure!(asset.faucet_id() == requested.faucet_id() && asset.amount().as_u64() == expected, "{}: wrong creator payout: actual={}, expected={}", case.name, asset.amount().as_u64(), expected);
                    check_tail(&rpc, owner, tail, step)?;
                }
                let actual_residual = {
                    let chain = rpc.mock_chain.read();
                    (vault_balance(&chain, solver_id, base.id()), vault_balance(&chain, solver_id, quote.id()))
                };
                ensure!(actual_residual == case.solver_holds, "wrong exact solver residual/fee balance");
                println!("{}: committed seller/buyer payouts {:?}, tails {:?}/{:?}, solver residual {:?}", case.name, case.paid, case.seller_tail, case.buyer_tail, case.solver_holds);
                Ok(())
            }.await;
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(30), &mut handle).await.context("solver did not stop")???;
            verdict?;
            println!("{}: solver stopped; consuming creator paybacks", case.name);

            // Execute both creator paybacks, proving that committed P2IDs are usable.
            for &(owner, _, requested, _, expected, _) in &traders {
                let token = requested.faucet_id();
                client.sync_state().await?;
                let notes = paybacks(&rpc, solver_id, owner);
                let before = vault_balance(&rpc.mock_chain.read(), owner, token);
                let request = TransactionRequestBuilder::new().build_consume_notes(notes)?;
                Box::pin(client.submit_new_transaction(owner, request)).await?;
                rpc.prove_block();
                client.sync_state().await?;
                ensure!(vault_balance(&rpc.mock_chain.read(), owner, token) == before + expected, "creator could not spend its exact payback");
            }
            println!("{}: creator paybacks spendable", case.name);
            Ok(())
        })
        .await
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_base_deficit_completion_commits_exact_paybacks_and_maker_terms() -> Result<()> {
    for fee in [0, 50] {
        run(Case {
            name: "initial",
            price: "1.5",
            fee,
            seller: (10, 14),
            buyer: (5, 3),
            paid: (5, 3),
            seller_tail: Some((7, 9)),
            buyer_tail: None,
            solver_holds: (0, 0),
            buyer_step: 1,
            extra_buyer: None,
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_final_tail_completion_uses_current_note_terms() -> Result<()> {
    run(Case {
        name: "tail",
        price: "1.5",
        fee: 50,
        seller: (8, 11),
        buyer: (2, 1),
        paid: (2, 1),
        seller_tail: Some((7, 9)),
        buyer_tail: None,
        solver_holds: (0, 0),
        buyer_step: 1,
        extra_buyer: None,
    })
    .await
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_completion_pays_the_canonical_nonzero_50ppm_fee() -> Result<()> {
    // Exact decimal market price; full buyer gross is 32_306, less one fee unit.
    run(Case {
        name: "nonzero fee",
        price: "0.63",
        fee: 50,
        seller: (115_034, 72_464),
        buyer: (20_353, 32_302),
        paid: (20_351, 32_305),
        seller_tail: Some((82_728, 52_113)),
        buyer_tail: None,
        solver_holds: (1, 2),
        buyer_step: 1,
        extra_buyer: None,
    })
    .await
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn a_quote_deficit_completion_preserves_both_buyer_minimum_steps() -> Result<()> {
    run(Case {
        name: "quote deficit",
        price: "1.37",
        fee: 50,
        seller: (60, 80),
        buyer: (26, 15),
        paid: (82, 13),
        seller_tail: None,
        buyer_tail: Some((4, 2)),
        solver_holds: (5, 1),
        buyer_step: 8,
        extra_buyer: Some((69, 47, 42, 42, (8, 5))),
    })
    .await
}

// Real-node qualifications use the same expected integer outcomes as MockChain.
mod local_node_repair {
    use super::*;
    use anyhow::bail;
    use miden_client::rpc::{Endpoint, GrpcClient, NodeRpcClient};
    use miden_client::transaction::TransactionStatus;
    use miden_client::{builder::ClientBuilder, keystore::FilesystemKeyStore, Client};
    use miden_client_sqlite_store::ClientBuilderSqliteExt;
    use miden_protocol::transaction::TransactionId;
    use std::path::PathBuf;

    fn rpc_url() -> String {
        std::env::var("SOLVER_LOCAL_NODE_RPC").unwrap_or_else(|_| "http://127.0.0.1:57291".into())
    }
    fn funding_url() -> String {
        std::env::var("SOLVER_LOCAL_NODE_FUNDING")
            .unwrap_or_else(|_| "http://127.0.0.1:50401".into())
    }
    fn rpc() -> Result<Arc<dyn NodeRpcClient>> {
        let endpoint = Endpoint::try_from(rpc_url().as_str()).map_err(|e| anyhow!("{e}"))?;
        Ok(Arc::new(GrpcClient::new(&endpoint, 30_000)))
    }
    /// Pays new accounts from the local network's funding service.
    #[derive(Debug)]
    struct FundingService {
        url: String,
        amount: u64,
    }

    #[async_trait::async_trait(?Send)]
    impl miden_client::testing::fee::FeeFunder for FundingService {
        async fn fund(&self, account_ids: &[AccountId]) -> Result<Vec<(AccountId, Note)>> {
            let http = reqwest::Client::new();
            let mut funded = Vec::with_capacity(account_ids.len());
            for account_id in account_ids {
                #[derive(serde::Deserialize)]
                struct Funded {
                    note: String,
                }
                let body = serde_json::json!({
                    "account_id": account_id.to_hex(),
                    "amount": self.amount,
                });
                let text = http
                    .post(format!("{}/request-funds", self.url))
                    .header("content-type", "application/json")
                    .body(body.to_string())
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;
                let reply: Funded = serde_json::from_str(&text)?;
                let note = Note::read_from_bytes(&hex::decode(reply.note)?)?;
                funded.push((*account_id, note));
            }
            // The service answers before it sends the transaction that creates
            // the notes: wait until they are on chain, so a spend can't race it.
            let ids: Vec<_> = funded.iter().map(|(_, note)| note.id()).collect();
            for _ in 0..240 {
                if rpc()?.get_notes_by_id(&ids).await?.len() == ids.len() {
                    return Ok(funded);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            bail!("funding notes not on chain after two minutes")
        }
    }

    struct NodeClients {
        ingest_store: PathBuf,
        executor_store: PathBuf,
        keystore: PathBuf,
    }

    #[async_trait::async_trait(?Send)]
    impl solver::ClientFactory for NodeClients {
        async fn build_ingest(&self) -> Result<Client<FilesystemKeyStore>> {
            let mut client = ClientBuilder::new()
                .rpc(rpc()?)
                .sqlite_store(self.ingest_store.clone())
                .build()
                .await
                .map_err(|e| anyhow!("ingest client: {e:?}"))?;
            client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            Ok(client)
        }

        async fn build_executor(&self) -> Result<Client<FilesystemKeyStore>> {
            let keystore = Arc::new(
                FilesystemKeyStore::new(self.keystore.clone()).map_err(|e| anyhow!("{e:?}"))?,
            );
            let mut client = ClientBuilder::new()
                .rpc(rpc()?)
                .sqlite_store(self.executor_store.clone())
                .authenticator(keystore)
                .build()
                .await
                .map_err(|e| anyhow!("executor client: {e:?}"))?;
            client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            Ok(client)
        }

        fn rpc(&self) -> Result<Arc<dyn NodeRpcClient>> {
            rpc()
        }
    }

    async fn client(dir: &tempfile::TempDir, name: &str) -> Result<TestClient> {
        let keys = dir.path().join(format!("{name}-keys"));
        std::fs::create_dir_all(&keys)?;
        let inner = ClientBuilder::new()
            .rpc(rpc()?)
            .sqlite_store(dir.path().join(format!("{name}.sqlite3")))
            .authenticator(Arc::new(
                FilesystemKeyStore::new(keys).map_err(|e| anyhow!("{e:?}"))?,
            ))
            .build()
            .await
            .map_err(|e| anyhow!("{e:?}"))?;
        let mut client = TestClient::new(inner).with_fee_funder(Some(Arc::new(FundingService {
            url: funding_url(),
            amount: 50_000_000,
        })));
        client.sync_state().await?;
        Ok(client)
    }
    async fn committed(client: &mut TestClient, tx: TransactionId) -> Result<()> {
        for _ in 0..240 {
            client.sync_state().await?;
            let record = client
                .get_transactions(miden_client::store::TransactionFilter::Ids(vec![tx]))
                .await?
                .into_iter()
                .next()
                .context("transaction is not tracked")?;
            match record.status {
                TransactionStatus::Committed { .. } => return Ok(()),
                TransactionStatus::Discarded(cause) => bail!("transaction discarded: {cause:?}"),
                TransactionStatus::Pending => {}
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        bail!("transaction did not commit: {tx}")
    }
    async fn run_on_node(case: Case) -> Result<()> {
        tokio::task::LocalSet::new().run_until(async move {
            println!("local node {}: setup", case.name);
            let pg = PgSchema::new().await?;
            let dir = tempfile::tempdir()?;
            let mut users = client(&dir, "users").await?;
            let mode = AccountType::Public;
            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let (base, _) = users.insert_account(AccountSetup::faucet(mode).auth_scheme(scheme)).await?;
            let (quote, _) = users.insert_account(AccountSetup::faucet(mode).auth_scheme(scheme)).await?;
            let (seller, _) = users.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
            let (buyer, _) = users.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
            let mut traders = vec![
                (seller.id(), FungibleAsset::new(base.id(), case.seller.0)?, FungibleAsset::new(quote.id(), case.seller.1)?, 1, case.paid.0, case.seller_tail),
                (buyer.id(), FungibleAsset::new(quote.id(), case.buyer.0)?, FungibleAsset::new(base.id(), case.buyer.1)?, case.buyer_step, case.paid.1, case.buyer_tail),
            ];
            if let Some((offered, requested, step, paid, tail)) = case.extra_buyer {
                let (extra, _) = users.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
                traders.push((extra.id(), FungibleAsset::new(quote.id(), offered)?, FungibleAsset::new(base.id(), requested)?, step, paid, Some(tail)));
            }
            let mut parents = Vec::new();
            let mut original_notes = Vec::new();
            for &(owner, offered, requested, step, _, _) in &traders {
                let mint = TransactionRequestBuilder::new().build_mint_fungible_asset(offered, owner, NoteType::Public, users.rng())?;
                let tx = Box::pin(users.execute_tx_and_consume_output_notes(mint, offered.faucet_id(), owner)).await?;
                committed(&mut users, tx).await?;
                let storage = PswapNoteStorage::builder().min_requested_asset(requested).min_fill_step(AssetAmount::new(step)?)
                    .creator_account_id(owner).payback_note_type(NoteType::Public).build();
                let note: Note = PswapNote::builder().sender(owner).storage(storage).serial_number(users.rng().draw_word())
                    .note_type(NoteType::Public).offered_asset(offered).build()?.into();
                parents.push(note.id().to_bytes()); original_notes.push(note.clone());
                let request = TransactionRequestBuilder::new().own_output_notes(vec![note]).build()?;
                let tx = Box::pin(users.submit_new_transaction(owner, request)).await?;
                committed(&mut users, tx).await?;
            }
            let solver_keys = dir.path().join("solver-keys");

            let solver_id = {
                let mut provision = client(&dir, "solver").await?;
                let (account, _) = provision.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
                provision.deploy_account(account.id()).await?;
                account.id()
            };
            // The provisioning helper used solver.sqlite3; keep that exact store.
            let solver_store = dir.path().join("solver.sqlite3");
            let ingest_store = dir.path().join("ingest.sqlite3");
            let factory = Arc::new(NodeClients { ingest_store: ingest_store.clone(), executor_store: solver_store.clone(), keystore: solver_keys.clone() });
            let binance = common::BinanceStub::start(&[("BTCUSDT", "BTC", "USDT", case.price)]);
            let mut engine = common::engine_config();
            engine.clearing_fee_ppm = case.fee; engine.clearing_integer_completion_repair = true; engine.fetch_interval_ms = 500;
            let config = SolverConfig {
                rpc: RpcConfig { endpoint: rpc_url(), timeout_ms: 30_000, prover_endpoint: None, prover_timeout_ms: 120_000 },
                solver: SolverAccountConfig { account_id: solver_id.to_hex(), keystore_path: solver_keys.to_string_lossy().into_owned(), executor_store_path: solver_store.to_string_lossy().into_owned(), ingest_store_path: ingest_store.to_string_lossy().into_owned(), read_pool_size: 2 },
                pairs: vec![common::pair_config("IBTC-IUSDT", base.id(), "BTC", quote.id(), "USDT", "BTCUSDT")], engine, binance: binance.config(),
            };
            let cancel = CancellationToken::new(); let token = cancel.clone(); let solver_factory: Arc<dyn solver::ClientFactory> = factory.clone();
            let mut handle = tokio::task::spawn_local(async move { solver::start(solver_factory, solver_id, config, token).await });
            let mut creator_paybacks = Vec::new();
            let mut expected_residual = case.solver_holds;
            let verdict: Result<()> = async {
                common::wait_for_submitted_settlement(&pg.url, Duration::from_secs(600), || handle.is_finished()).await?;
                let mut confirmed = false;
                for _ in 0..240 {
                    let mut all = true;
                    for (parent, &(_, _, _, step, _, tail)) in parents.iter().zip(&traders) { all &= check_lineage(&pg.url, parent.clone(), tail, step).await?; }
                    if all { confirmed = true; break; }
                    ensure!(!handle.is_finished(), "solver stopped before confirmation");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                ensure!(confirmed, "local-node settlement did not confirm its exact lineage");
                for (note, &(owner, _, requested, _, expected, tail)) in original_notes.iter().zip(&traders) {
                    let (payback, remainder) = PswapNote::try_from(note)?.execute(solver_id, None, Some(FungibleAsset::new(requested.faucet_id(), expected)?))?;
                    ensure!(remainder.is_some() == tail.is_some(), "wrong completion branch");
                    let mut expected_ids = vec![payback.id()];
                    if let Some(remainder) = remainder { expected_ids.push(Note::from(remainder).id()); }
                    let actual = rpc()?.get_notes_by_id(&expected_ids).await?;
                    ensure!(actual.len() == expected_ids.len(), "exact creator payback/remainder IDs are not committed");
                    creator_paybacks.push((owner, requested.faucet_id(), expected, payback));
                }
                println!("local node {}: exact paybacks and remainder IDs committed; lineage persisted", case.name);
                if case.name == "base deficit" {
                    // The first repaired maker tail is reingested, then fills in
                    // a second independent solver batch at its current 7/9 terms.
                    let (_, tail) = PswapNote::try_from(&original_notes[0])?.execute(solver_id, None, Some(FungibleAsset::new(quote.id(), 5)?))?;
                    let tail: Note = tail.context("first repair has no maker tail")?.into();
                    let (next, _) = users.insert_account(AccountSetup::wallet(mode).auth_scheme(scheme)).await?;
                    let mint = TransactionRequestBuilder::new().build_mint_fungible_asset(FungibleAsset::new(quote.id(), 11)?, next.id(), NoteType::Public, users.rng())?;
                    let tx = Box::pin(users.execute_tx_and_consume_output_notes(mint, quote.id(), next.id())).await?;
                    committed(&mut users, tx).await?;
                    let storage = PswapNoteStorage::builder().min_requested_asset(FungibleAsset::new(base.id(), 7)?)
                        .min_fill_step(AssetAmount::new(1)?).creator_account_id(next.id()).payback_note_type(NoteType::Public).build();
                    let next_note: Note = PswapNote::builder().sender(next.id()).storage(storage).serial_number(users.rng().draw_word())
                        .note_type(NoteType::Public).offered_asset(FungibleAsset::new(quote.id(), 11)?).build()?.into();
                    let tx = Box::pin(users.submit_new_transaction(next.id(), TransactionRequestBuilder::new().own_output_notes(vec![next_note.clone()]).build()?)).await?;
                    committed(&mut users, tx).await?;
                    let (maker_paid, maker_left) = PswapNote::try_from(&tail)?.execute(solver_id, None, Some(FungibleAsset::new(quote.id(), 10)?))?;
                    let (next_paid, next_left) = PswapNote::try_from(&next_note)?.execute(solver_id, None, Some(FungibleAsset::new(base.id(), 7)?))?;
                    ensure!(maker_left.is_none() && next_left.is_none(), "second batch did not complete both current notes");
                    let ids = [maker_paid.id(), next_paid.id()];
                    let mut landed = false;
                    for _ in 0..1200 {
                        if rpc()?.get_notes_by_id(&ids).await?.len() == ids.len() { landed = true; break; }
                        ensure!(!handle.is_finished(), "solver stopped during second batch");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    ensure!(landed, "reingested maker remainder did not settle in a second batch");
                    let mut retired = false;
                    for _ in 0..240 {
                        let url = pg.url.clone(); let id = tail.id().to_bytes();
                        let (status, active_in_slot) = tokio::task::spawn_blocking(move || -> Result<(String,i64)> {
                            use solver::db::postgres_schema::orders;
                            let mut conn = solver::db::postgres_migrations::connect(&url)?;
                            let (status, priority) = orders::table.find(id).select((orders::status,orders::priority_seq)).first::<(String,i64)>(&mut conn)?;
                            let count = orders::table.filter(orders::priority_seq.eq(priority)).filter(orders::status.eq("active")).count().get_result(&mut conn)?;
                            Ok((status,count))
                        }).await??;
                        if status == "executed" && active_in_slot == 0 { retired = true; break; }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    ensure!(retired, "maker tail was not retired without an active duplicate");
                    creator_paybacks.push((seller.id(), quote.id(), 10, maker_paid));
                    creator_paybacks.push((next.id(), base.id(), 7, next_paid));
                    expected_residual.1 += 1;
                    println!("local node base deficit: repaired maker tail reingested and fully settled in second batch");
                }
                Ok(())
            }.await;
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(30), &mut handle).await.context("local solver did not stop")???;
            verdict?;
            let mut executor = TestClient::new(solver::ClientFactory::build_executor(factory.as_ref()).await?);
            executor.sync_state().await?;
            executor.assert_account_has_single_asset(solver_id, base.id(), expected_residual.0).await;
            executor.assert_account_has_single_asset(solver_id, quote.id(), expected_residual.1).await;
            for (owner, faucet, expected, note) in creator_paybacks {
                users.sync_state().await?;
                let before = users.account_reader(owner).get_balance(faucet).await?.as_u64();
                let request = TransactionRequestBuilder::new().build_consume_notes(vec![note])?;
                let tx = Box::pin(users.submit_new_transaction(owner, request)).await?;
                committed(&mut users, tx).await?;
                users.assert_account_has_single_asset(owner, faucet, before + expected).await;
            }
            println!("local node {}: creator paybacks spendable; exact fee/surplus balances verified", case.name);
            Ok(())
        }).await
    }
    #[tokio::test]
    #[ignore = "requires isolated local node/funding service and SOLVER_TEST_DATABASE_URL"]
    async fn local_node_base_deficit_completion() -> Result<()> {
        run_on_node(Case {
            name: "base deficit",
            price: "1.5",
            fee: 50,
            seller: (10, 14),
            buyer: (5, 3),
            paid: (5, 3),
            seller_tail: Some((7, 9)),
            buyer_tail: None,
            solver_holds: (0, 0),
            buyer_step: 1,
            extra_buyer: None,
        })
        .await
    }
    #[tokio::test]
    #[ignore = "requires isolated local node/funding service and SOLVER_TEST_DATABASE_URL"]
    async fn local_node_quote_deficit_completion() -> Result<()> {
        run_on_node(Case {
            name: "quote deficit",
            price: "1.37",
            fee: 50,
            seller: (60, 80),
            buyer: (26, 15),
            paid: (82, 13),
            seller_tail: None,
            buyer_tail: Some((4, 2)),
            solver_holds: (5, 1),
            buyer_step: 8,
            extra_buyer: Some((69, 47, 42, 42, (8, 5))),
        })
        .await
    }
    #[tokio::test]
    #[ignore = "requires isolated local node/funding service and SOLVER_TEST_DATABASE_URL"]
    async fn local_node_nonzero_50ppm_completion() -> Result<()> {
        run_on_node(Case {
            name: "nonzero fee",
            price: "0.63",
            fee: 50,
            seller: (115034, 72464),
            buyer: (20353, 32302),
            paid: (20351, 32305),
            seller_tail: Some((82728, 52113)),
            buyer_tail: None,
            solver_holds: (1, 2),
            buyer_step: 1,
            extra_buyer: None,
        })
        .await
    }
}

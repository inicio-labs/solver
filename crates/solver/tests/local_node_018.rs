//! Against a real local Miden node 0.18 (ADR 0008, plan item 8 and the
//! "local node" tests): the node behaviour live at accept relies on, checked
//! with the client the solver uses (miden-client 0.17.5).
//!
//! Start the node's local network first (`make local-network-up` in a
//! checkout of github.com/0xMiden/node at v0.18.0). The tests read:
//! - `SOLVER_LOCAL_NODE_RPC` (default `http://127.0.0.1:57291`);
//! - `SOLVER_LOCAL_NODE_FUNDING` (default `http://127.0.0.1:50401`), the
//!   network's funding service, which pays every new account its fees;
//! - `SOLVER_LOCAL_NODE_COMPOSE_DIR`, the node checkout, for the restart test.
//!
//! With `SOLVER_LOCAL_NODE_WRITE_FIXTURE=1`, the refusal texts the node sent
//! are written to `tests/fixtures/node-0.18-refusals.json`, which the
//! refusal parser's unit test reads.
//!
//! The full-solver test also needs PostgreSQL (`SOLVER_TEST_DATABASE_URL`).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use miden_client::auth::AuthSchemeId;
use miden_client::builder::ClientBuilder;
use miden_client::keystore::{FilesystemKeyStore, Keystore};
use miden_client::note::{Note, NoteType};
use miden_client::rpc::{
    AddTransactionError, Endpoint, EndpointError, GrpcClient, NodeRpcClient, RpcError,
};
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::transaction::{
    PaymentNoteDescription, TransactionRequestBuilder, TransactionStatus,
};
use miden_client::{Client, ClientError};
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::{AccountId, AccountType};
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::transaction::TransactionId;

const SCHEME: AuthSchemeId = AuthSchemeId::Falcon512Poseidon2;

fn rpc_url() -> String {
    std::env::var("SOLVER_LOCAL_NODE_RPC").unwrap_or_else(|_| "http://127.0.0.1:57291".into())
}

fn funding_url() -> String {
    std::env::var("SOLVER_LOCAL_NODE_FUNDING").unwrap_or_else(|_| "http://127.0.0.1:50401".into())
}

fn rpc() -> Result<Arc<dyn NodeRpcClient>> {
    let endpoint = Endpoint::try_from(rpc_url().as_str()).map_err(|e| anyhow!("endpoint: {e}"))?;
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

async fn client(dir: &tempfile::TempDir, name: &str) -> Result<TestClient> {
    let keystore_dir = dir.path().join(format!("{name}-keys"));
    std::fs::create_dir_all(&keystore_dir)?;
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_dir).map_err(|e| anyhow!("{e:?}"))?);
    let client: Client<FilesystemKeyStore> = ClientBuilder::new()
        .rpc(rpc()?)
        .sqlite_store(dir.path().join(format!("{name}.sqlite3")))
        .authenticator(keystore)
        .build()
        .await
        .map_err(|e| anyhow!("client: {e}"))?;
    let mut client = TestClient::new(client).with_fee_funder(Some(Arc::new(FundingService {
        url: funding_url(),
        amount: 10_000_000,
    })));
    client
        .sync_state()
        .await
        .map_err(|e| anyhow!("sync: {e}"))?;
    Ok(client)
}

/// The chain's native (fee) asset.
async fn fee_faucet(client: &TestClient) -> Result<AccountId> {
    let (header, _) = rpc()?.get_block_header_by_number(None, false).await?;
    let config = client
        .get_protocol_config(header.protocol_config_commitment())
        .await
        .map_err(|e| anyhow!("protocol config: {e}"))?;
    Ok(config.fee_asset_id().faucet_id())
}

/// A private payment of `amount` of the fee asset from `from` to `to`, built
/// but not sent.
fn payment(
    client: &mut TestClient,
    faucet: AccountId,
    from: AccountId,
    to: AccountId,
    amount: u64,
) -> Result<(miden_client::transaction::TransactionRequest, Note)> {
    let request = TransactionRequestBuilder::new()
        .build_pay_to_id(
            PaymentNoteDescription::new(vec![FungibleAsset::new(faucet, amount)?.into()], from, to),
            NoteType::Private,
            client.rng(),
        )
        .map_err(|e| anyhow!("payment: {e}"))?;
    let note = request
        .expected_output_own_notes()
        .into_iter()
        .next()
        .context("the payment creates no note")?;
    Ok((request, note))
}

fn consume(note: Note) -> Result<miden_client::transaction::TransactionRequest> {
    TransactionRequestBuilder::new()
        .build_consume_notes(vec![note])
        .map_err(|e| anyhow!("consume: {e}"))
}

/// Wait until `tx` is committed or discarded in `client`'s store.
async fn settled(client: &mut TestClient, tx: TransactionId) -> Result<TransactionStatus> {
    for _ in 0..120 {
        client
            .sync_state()
            .await
            .map_err(|e| anyhow!("sync: {e}"))?;
        let record = client
            .get_transactions(miden_client::store::TransactionFilter::Ids(vec![tx]))
            .await
            .map_err(|e| anyhow!("{e:?}"))?
            .into_iter()
            .next()
            .context("no record")?;
        if !matches!(record.status, TransactionStatus::Pending) {
            return Ok(record.status);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("transaction {tx} still pending after a minute")
}

fn state_conflict_message(error: &ClientError) -> Option<&str> {
    match error {
        ClientError::RpcError(RpcError::RequestError {
            endpoint_error:
                Some(EndpointError::AddTransaction(AddTransactionError::StateConflict { message })),
            ..
        }) => Some(message),
        _ => None,
    }
}

/// Two funded wallets: Alice pays, Bob consumes.
struct Wallets {
    _dir: tempfile::TempDir,
    alice_client: TestClient,
    bob_client: TestClient,
    alice: AccountId,
    bob: AccountId,
    faucet: AccountId,
}

async fn wallets() -> Result<Wallets> {
    let dir = tempfile::tempdir()?;
    let mut alice_client = client(&dir, "alice").await?;
    let mut bob_client = client(&dir, "bob").await?;
    let (alice, _) = alice_client
        .insert_account(AccountSetup::wallet(AccountType::Public).auth_scheme(SCHEME))
        .await?;
    let (bob, _) = bob_client
        .insert_account(AccountSetup::wallet(AccountType::Public).auth_scheme(SCHEME))
        .await?;
    let faucet = fee_faucet(&alice_client).await?;
    Ok(Wallets {
        _dir: dir,
        alice_client,
        bob_client,
        alice: alice.id(),
        bob: bob.id(),
        faucet,
    })
}

/// The settlement spends a note whose creating transaction still waits in
/// the mempool: the node accepts it, and both land together, the note erased.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 (see the module docs)"]
async fn a_note_waiting_in_the_mempool_can_be_spent() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut w = wallets().await?;
            let (request, note) = payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 1_000)?;
            let created = Box::pin(w.alice_client.submit_new_transaction(w.alice, request))
                .await
                .map_err(|e| anyhow!("alice pays: {e:?}"))?;
            // Spend it before any block: the node must accept this.
            let spent = Box::pin(
                w.bob_client
                    .submit_new_transaction(w.bob, consume(note.clone())?),
            )
            .await
            .map_err(|e| anyhow!("bob spends the waiting note: {e:?}"))?;
            let (a, b) = (
                settled(&mut w.alice_client, created).await?,
                settled(&mut w.bob_client, spent).await?,
            );
            ensure!(
                matches!(a, TransactionStatus::Committed { .. })
                    && matches!(b, TransactionStatus::Committed { .. }),
                "created {a:?}, spent {b:?}"
            );
            let (
                TransactionStatus::Committed {
                    block_number: ab, ..
                },
                TransactionStatus::Committed {
                    block_number: bb, ..
                },
            ) = (&a, &b)
            else {
                unreachable!()
            };
            let found = rpc()?.get_notes_by_id(&[note.id()]).await?;
            println!(
                "created in block {ab}, spent in block {bb}; note found by ID: {}",
                !found.is_empty()
            );
            Ok(())
        })
        .await
}

/// The node's refusal of a spend of a note nobody created, and of a second
/// spend of a note a waiting transaction already spends: the texts the
/// executor parses (ADR 0008, D3). Written to the fixture on request.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 (see the module docs)"]
async fn the_node_names_unknown_notes_in_its_refusal() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut w = wallets().await?;
            // Deploy Bob first, so the refusal concerns only the note.
            let (deploy, deploy_note) =
                payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 2_000)?;
            let deployed = Box::pin(w.alice_client.submit_new_transaction(w.alice, deploy))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut w.alice_client, deployed).await?;
            let consumed = Box::pin(
                w.bob_client
                    .submit_new_transaction(w.bob, consume(deploy_note)?),
            )
            .await
            .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut w.bob_client, consumed).await?;

            let (_, unsent) = payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 1_000)?;
            let refused = Box::pin(
                w.bob_client
                    .submit_new_transaction(w.bob, consume(unsent.clone())?),
            )
            .await
            .expect_err("the node must refuse a note nobody created");
            let message = state_conflict_message(&refused)
                .with_context(|| format!("not a state conflict: {refused:?}"))?
                .to_owned();
            println!("unknown notes refusal:\n{message}");
            ensure!(
                message.contains("unauthenticated input notes are unknown")
                    && message.contains(&format!("{:?}", unsent.id())),
                "the refusal does not name the note: {message}"
            );

            if std::env::var("SOLVER_LOCAL_NODE_WRITE_FIXTURE").as_deref() == Ok("1") {
                let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/node-0.18-refusals.json");
                std::fs::create_dir_all(path.parent().unwrap())?;
                let fixture = serde_json::json!({
                    "node": "0.18.0 (local network, commit 076f4db)",
                    "client": "miden-client 0.17.5",
                    "unknown_notes": {
                        "message": message,
                        "unknown_note": hex::encode(unsent.to_bytes()),
                    },
                });
                std::fs::write(&path, serde_json::to_string_pretty(&fixture)? + "\n")?;
                println!("wrote {}", path.display());
            }
            Ok(())
        })
        .await
}

/// A transaction moving the fee asset (USDCx) expires within the protocol's
/// cap, so a maker order offering USDCx can't wait forever.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 (see the module docs)"]
async fn a_fee_asset_transfer_expires_within_its_cap() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut w = wallets().await?;
            let (request, _) = payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 1_000)?;
            let result = Box::pin(w.alice_client.execute_transaction(w.alice, request))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            let executed = result.executed_transaction();
            let reference = executed.block_header().block_num();
            let expiration = executed.expiration_block_num();
            println!(
                "reference block {reference}, expires at {expiration} ({} blocks)",
                expiration.as_u32() - reference.as_u32()
            );
            ensure!(expiration.as_u32() > reference.as_u32());
            Ok(())
        })
        .await
}

/// A node restart between the maker's transaction and its block: the
/// waiting transaction is lost with the mempool, a spend of its note is then
/// refused as unknown, and the maker's client only learns it on expiry.
/// Needs `SOLVER_LOCAL_NODE_COMPOSE_DIR`.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 (see the module docs)"]
async fn a_node_restart_loses_a_waiting_transaction() -> Result<()> {
    let Ok(compose_dir) = std::env::var("SOLVER_LOCAL_NODE_COMPOSE_DIR") else {
        println!("SOLVER_LOCAL_NODE_COMPOSE_DIR not set; skipped");
        return Ok(());
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut w = wallets().await?;
            // Deploy Alice first, so the lost transaction is only the payment.
            let (deploy, _) = payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 1)?;
            let deployed = Box::pin(w.alice_client.submit_new_transaction(w.alice, deploy))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut w.alice_client, deployed).await?;

            let (request, note) = payment(&mut w.alice_client, w.faucet, w.alice, w.bob, 1_000)?;
            let created = Box::pin(w.alice_client.submit_new_transaction(w.alice, request))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            let restarted = std::process::Command::new("docker")
                .args(["compose", "restart", "sequencer"])
                .current_dir(&compose_dir)
                .status()?;
            ensure!(restarted.success(), "docker compose restart failed");
            // Wait for the RPC to answer again.
            for _ in 0..120 {
                if rpc()?.get_block_header_by_number(None, false).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let spend = Box::pin(w.bob_client.submit_new_transaction(w.bob, consume(note.clone())?)).await;
            match &spend {
                Ok(_) => println!("after the restart, the spend was ACCEPTED: the node kept the waiting transaction"),
                Err(error) => println!(
                    "after the restart, the spend was refused: {}",
                    state_conflict_message(error).map_or_else(|| format!("{error:?}"), str::to_owned)
                ),
            }
            let status = settled(&mut w.alice_client, created).await;
            println!("the maker's own transaction: {status:?}");
            Ok(())
        })
        .await
}

/// A second spend of a note a waiting transaction already spends (issue
/// #69): the node refuses it for its nullifier before it looks at the notes,
/// so a note in this state can't be told from an unknown one by name.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 (see the module docs)"]
async fn a_second_spend_of_a_waiting_spend_is_refused_by_nullifier() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let dir = tempfile::tempdir()?;
            let mut alice_client = client(&dir, "alice").await?;
            let mut bob_client = client(&dir, "bob").await?;
            let (alice, _) = alice_client
                .insert_account(AccountSetup::wallet(AccountType::Public).auth_scheme(SCHEME))
                .await?;
            let (bob, bob_key) = bob_client
                .insert_account(AccountSetup::wallet(AccountType::Public).auth_scheme(SCHEME))
                .await?;
            let (alice, bob) = (alice.id(), bob.id());
            let faucet = fee_faucet(&alice_client).await?;
            // Both on chain, and one note waiting for Bob.
            let (first, first_note) = payment(&mut alice_client, faucet, alice, bob, 2_000)?;
            let paid = Box::pin(alice_client.submit_new_transaction(alice, first))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut alice_client, paid).await?;
            let deployed = Box::pin(bob_client.submit_new_transaction(bob, consume(first_note)?))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut bob_client, deployed).await?;
            let (second, note) = payment(&mut alice_client, faucet, alice, bob, 1_000)?;
            let paid = Box::pin(alice_client.submit_new_transaction(alice, second))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            settled(&mut alice_client, paid).await?;

            // A second client of Bob's, at the same account state.
            let mut twin = client(&dir, "bob-twin").await?;
            twin.keystore()
                .add_key(&bob_key, bob)
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            twin.import_account_by_id(bob)
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            twin.sync_state().await.map_err(|e| anyhow!("{e:?}"))?;
            let twin_spend = Box::pin(twin.execute_transaction(bob, consume(note.clone())?))
                .await
                .map_err(|e| anyhow!("twin executes: {e:?}"))?;

            // Bob spends it; the spend waits for a block. Then the twin's.
            Box::pin(bob_client.submit_new_transaction(bob, consume(note.clone())?))
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            let proven = twin
                .prove_transaction(&twin_spend)
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            let refused = twin
                .submit_proven_transaction(proven, &twin_spend)
                .await
                .expect_err("a note already spent in the mempool");
            let message = state_conflict_message(&refused)
                .with_context(|| format!("not a state conflict: {refused:?}"))?;
            println!("second spend refusal:\n{message}");
            println!("nullifier: {:?}", note.nullifier());
            ensure!(message.contains("nullifiers already exist"), "{message}");

            if std::env::var("SOLVER_LOCAL_NODE_WRITE_FIXTURE").as_deref() == Ok("1") {
                let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/node-0.18-refusals.json");
                let mut fixture: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(&path)?)?;
                fixture["mempool_nullifiers"] = serde_json::json!({
                    "message": message,
                    "spent_note": hex::encode(note.to_bytes()),
                });
                std::fs::write(&path, serde_json::to_string_pretty(&fixture)? + "\n")?;
                println!("updated {}", path.display());
            }
            Ok(())
        })
        .await
}

/// Clients against the real node: as production builds them, with no seeded
/// encryption key (the node serves an attested one).
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
        let keystore =
            Arc::new(FilesystemKeyStore::new(self.keystore.clone()).map_err(|e| anyhow!("{e:?}"))?);
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

/// The whole solver, live at accept on, against the real node: the maker
/// sends its transaction, then SubmitOrder; the order is LIVE at once,
/// trades against a resting user order, and the settlement commits.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a local Miden node 0.18 and SOLVER_TEST_DATABASE_URL"]
async fn the_solver_settles_a_live_at_accept_order_on_node_0_18() -> Result<()> {
    use common::maker::{free_port, is_order_status, register_maker, wait_for_event, MakerClient};
    use miden_client::transaction::PswapTransactionData;
    use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
    use solver::gateway::proto;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let pg = common::PgSchema::new().await?;
            let dir = tempfile::tempdir()?;
            let mut users = client(&dir, "users").await?;
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
            for (account, faucet) in [(alice.id(), usdc.id()), (maker.id(), eth.id())] {
                Box::pin(users.mint_and_consume(account, faucet, NoteType::Public))
                    .await
                    .with_context(|| format!("mint to {account}"))?;
            }

            // The solver account, funded for its fees and on chain.
            let keystore = dir.path().join("solver-keys");
            std::fs::create_dir_all(&keystore)?;
            let executor_store = dir.path().join("executor.sqlite3");
            let solver_id = {
                let keys = Arc::new(
                    FilesystemKeyStore::new(keystore.clone()).map_err(|e| anyhow!("{e:?}"))?,
                );
                let provision: Client<FilesystemKeyStore> = ClientBuilder::new()
                    .rpc(rpc()?)
                    .sqlite_store(executor_store.clone())
                    .authenticator(keys)
                    .build()
                    .await
                    .map_err(|e| anyhow!("{e:?}"))?;
                let mut provision =
                    TestClient::new(provision).with_fee_funder(Some(Arc::new(FundingService {
                        url: funding_url(),
                        amount: 50_000_000,
                    })));
                provision.sync_state().await.map_err(|e| anyhow!("{e:?}"))?;
                let (account, _) = provision
                    .insert_account(AccountSetup::wallet(mode).auth_scheme(SCHEME))
                    .await?;
                provision.deploy_account(account.id()).await?;
                account.id()
            };

            let binance = common::BinanceStub::start(&[
                ("ETHUSDC", "ETH", "USDC", "100"),
                ("ETHUSDT", "ETH", "USDT", "100"),
                ("USDCUSDT", "USDC", "USDT", "1"),
            ]);
            let mut engine = common::engine_config();
            engine.maker_gateway_enabled = true;
            engine.maker_gateway_port = free_port();
            engine.maker_watch_interval_ms = 500;
            engine.fetch_interval_ms = 500;
            engine.obs_port = free_port();
            engine.maker_live_at_accept = true;
            let port = engine.maker_gateway_port;
            let ingest_store = dir.path().join("ingest.sqlite3");
            let config = SolverConfig {
                rpc: RpcConfig {
                    endpoint: rpc_url(),
                    timeout_ms: 30_000,
                    prover_endpoint: None,
                    prover_timeout_ms: 120_000,
                },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: keystore.to_string_lossy().into_owned(),
                    executor_store_path: executor_store.to_string_lossy().into_owned(),
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
            let factory: Arc<dyn solver::ClientFactory> = Arc::new(NodeClients {
                ingest_store,
                executor_store,
                keystore,
            });
            let cancel = tokio_util::sync::CancellationToken::new();
            let solver_cancel = cancel.clone();
            let mut solver = tokio::task::spawn_local(async move {
                solver::start(factory, solver_id, config, solver_cancel).await
            });

            let run: Result<()> = async {
                let mut gateway =
                    MakerClient::connect(port, &api_key, || solver.is_finished()).await?;
                let mut events = gateway.events().await?;
                // Alice's order rests in the book.
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
                    .map_err(|e| anyhow!("user order: {e:?}"))?;
                let deadline = std::time::Instant::now() + Duration::from_secs(120);
                while common::count_orders(&pg.url, "active").await < 1 {
                    ensure!(
                        std::time::Instant::now() < deadline,
                        "the user order was never ingested"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                // Give the watcher a fresh block before the maker submits.
                tokio::time::sleep(Duration::from_secs(2)).await;

                // The maker: transaction first, then SubmitOrder.
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
                    .context("no maker note")?;
                Box::pin(users.submit_new_transaction(maker.id(), request))
                    .await
                    .map_err(|e| anyhow!("maker order: {e:?}"))?;
                let submitted = std::time::Instant::now();
                let reply = gateway.submit(&order).await?;
                ensure!(
                    matches!(
                        reply.result,
                        Some(proto::command_reply::Result::Accepted(_))
                    ),
                    "submit not accepted: {reply:?}"
                );
                wait_for_event(&mut events, Duration::from_secs(10), |kind| {
                    is_order_status(kind, &order, proto::OrderState::Live)
                })
                .await
                .context("no LIVE at accept")?;
                println!("LIVE {:?} after SubmitOrder", submitted.elapsed());
                wait_for_event(&mut events, Duration::from_secs(600), |kind| {
                    matches!(kind, proto::event_body::Kind::SettlementResolved(r) if r.committed)
                })
                .await
                .context("the settlement never committed")?;
                println!("settled {:?} after SubmitOrder", submitted.elapsed());

                let url = pg.url.clone();
                let note_id = order.id().to_bytes();
                let (status, lineage) =
                    tokio::task::spawn_blocking(move || -> Result<(String, String)> {
                        use diesel::prelude::*;
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
                    .await??;
                println!("maker order {status}, lineage {lineage}");
                ensure!(status == "executed" && lineage == "activated");
                Ok(())
            }
            .await;
            cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(30), &mut solver).await;
            drop(binance);
            run
        })
        .await
}

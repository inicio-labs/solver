//! A real partial fill: one half-size taker consumes only part of the maker.
//! The solver must commit the paybacks and persist an active remainder with
//! the maker's FIFO priority. This is a release assertion, not a diagnostic
//! that returns success when settlement fails.

mod common;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use diesel::prelude::*;
use miden_client::auth::AuthSchemeId;
use miden_client::note::NoteType;
use miden_client::testing::common::{AccountSetup, TestClient};
use miden_client::testing::mock::MockRpcApi;
use miden_client::transaction::{PswapTransactionData, TransactionRequestBuilder};
use miden_protocol::account::AccountType;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_testing::MockChain;
use solver::config::{RpcConfig, SolverAccountConfig, SolverConfig};
use tokio_util::sync::CancellationToken;

use common::{
    build_test_client, temp_paths, vault_balance, wait_for_submitted_settlement, MockClientFactory,
    PgSchema,
};

async fn remainder_state(
    db_url: &str,
    parent_note_id: &[u8],
) -> Result<Option<(String, i64, String, i64, i64)>> {
    let url = db_url.to_owned();
    let parent_note_id = parent_note_id.to_vec();
    tokio::task::spawn_blocking(move || -> Result<_> {
        use solver::db::postgres_schema::orders;

        let mut conn = solver::db::postgres_migrations::connect(&url)?;
        let Some((parent_status, parent_priority)) = orders::table
            .find(&parent_note_id)
            .select((orders::status, orders::priority_seq))
            .first::<(String, i64)>(&mut conn)
            .optional()?
        else {
            return Ok(None);
        };
        // A confirmed settlement deletes its attempt and input rows, so the
        // remainder is found by the FIFO slot it inherits from its parent.
        let Some((child_status, child_priority, child_raw)) = orders::table
            .filter(orders::priority_seq.eq(parent_priority))
            .filter(orders::note_id.ne(&parent_note_id))
            .select((orders::status, orders::priority_seq, orders::raw_data))
            .first::<(String, i64, Vec<u8>)>(&mut conn)
            .optional()?
        else {
            return Ok(None);
        };
        // An order's terms live only in its serialized note.
        let child_note = <miden_protocol::note::Note as miden_protocol::crypto::utils::Deserializable>::read_from_bytes(&child_raw)?;
        let child_offered_amount =
            i64::try_from(solver::types::Order::from_note(&child_note)?.offered_amount)?;
        Ok(Some((
            parent_status,
            parent_priority,
            child_status,
            child_priority,
            child_offered_amount,
        )))
    })
    .await?
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
async fn partial_fill_repro() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let pg = PgSchema::new().await?;
            let rpc = Arc::new(MockRpcApi::new(MockChain::new()));

            // USER client: 2 faucets (IBTC, IUSDT), one maker and one taker.
            let (user_temp, user_keystore_path, user_store_path) = temp_paths()?;
            let mut user_client =
                TestClient::new(build_test_client(rpc.clone(), user_keystore_path.clone(), user_store_path).await?);
            user_client
                .ensure_genesis_in_place()
                .await
                .map_err(|e| anyhow::anyhow!("user genesis: {e}"))?;
            let scheme = AuthSchemeId::Falcon512Poseidon2;
            let mode = AccountType::Public;

            let (ibtc, _) = user_client
                .insert_account(AccountSetup::faucet(mode).auth_scheme(scheme))
                .await?;
            let (iusdt, _) = user_client
                .insert_account(AccountSetup::faucet(mode).auth_scheme(scheme))
                .await?;
            let (maker1, _) = user_client
                .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                .await?;
            let (taker_half, _) = user_client
                .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                .await?;

            // The client must learn the current protocol configuration from
            // a proved block before it can construct the first mint transaction.
            rpc.prove_block();
            user_client.sync_state().await?;

            // Fund the maker with IBTC and the taker with IUSDT.
            user_client
                .mint_and_consume(maker1.id(), ibtc.id(), NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync ibtc: {e}"))?;
            user_client
                .mint_and_consume(taker_half.id(), iusdt.id(), NoteType::Public)
                .await?;
            rpc.prove_block();
            user_client
                .sync_state()
                .await
                .map_err(|e| anyhow::anyhow!("sync iusdt: {e}"))?;

            // The taker offers only half the maker's requested IUSDT. A
            // successful settlement leaves a 50-IBTC maker remainder.
            let orders: Vec<(_, FungibleAsset, FungibleAsset)> = vec![
                (maker1.id(), FungibleAsset::new(ibtc.id(), 100)?, FungibleAsset::new(iusdt.id(), 1000)?),
                (taker_half.id(), FungibleAsset::new(iusdt.id(), 500)?, FungibleAsset::new(ibtc.id(), 49)?),
            ];
            let mut maker_note_id = None;
            for (index, (creator, offered, requested)) in orders.into_iter().enumerate() {
                let req = TransactionRequestBuilder::new()
                    .build_pswap_create(
                        &PswapTransactionData::new(creator, offered, requested),
                        NoteType::Public,
                        NoteType::Public,
                        None,
                        user_client.rng(),
                    )
                    .map_err(|e| anyhow::anyhow!("pswap: {e}"))?;
                if index == 0 {
                    maker_note_id = Some(req.expected_output_own_notes()[0].id().to_bytes());
                }
                Box::pin(user_client.submit_new_transaction(creator, req))
                    .await
                    .map_err(|e| anyhow::anyhow!("submit: {e}"))?;
            }
            let maker_note_id = maker_note_id.context("maker PSWAP has no note ID")?;
            rpc.prove_block();

            // SOLVER account (throwaway client provisions it on disk, then drops).
            let (solver_temp, solver_keystore_path, solver_store_path) = temp_paths()?;
            let solver_id = {
                let mut sc = TestClient::new(build_test_client(
                    rpc.clone(),
                    solver_keystore_path.clone(),
                    solver_store_path.clone(),
                )
                .await?);
                sc.ensure_genesis_in_place()
                    .await
                    .map_err(|e| anyhow::anyhow!("solver genesis: {e}"))?;
                let (sa, _) = sc
                    .insert_account(AccountSetup::wallet(mode).auth_scheme(scheme))
                    .await?;
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

            // IBTC/IUSDT clears at the BTCUSDT mid: 10 IUSDT per IBTC.
            let binance = common::BinanceStub::start(&[("BTCUSDT", "BTC", "USDT", "10")]);
            let config = SolverConfig {
                rpc: RpcConfig { endpoint: "http://unused".into(), timeout_ms: 1_000, prover_endpoint: None },
                solver: SolverAccountConfig {
                    account_id: solver_id.to_hex(),
                    keystore_path: solver_keystore_path.to_string_lossy().into_owned(),
                    executor_store_path,
                    ingest_store_path,
                    read_pool_size: 2,
                },
                pairs: vec![common::pair_config("IBTC-IUSDT", ibtc.id(), "BTC", iusdt.id(), "USDT", "BTCUSDT")],
                engine: common::engine_config(),
                binance: binance.config(),
            };

            let cancel = CancellationToken::new();
            let solver_cancel = cancel.clone();
            let mut solver_handle = tokio::task::spawn_local(async move {
                solver::start(
                    factory,
                    solver_id,
                    config,
                    solver_cancel,
                )
                .await
            });

            // The taker's minimum is 49 IBTC, but the clearing price can pay
            // it the fair 50. Only the paybacks and remainder are invariant.
            let ibtc_id = ibtc.id();
            let initial = rpc.mock_chain.read().committed_notes().len();
            let verdict: Result<()> = async {
                wait_for_submitted_settlement(
                    &pg.url,
                    std::time::Duration::from_secs(900),
                    || solver_handle.is_finished(),
                )
                .await?;
                rpc.prove_block();
                let committed = rpc.mock_chain.read().committed_notes().len();
                if committed < initial + 2 {
                    bail!("partial settlement produced fewer than two payback notes: committed={committed}, before={initial}");
                }
                // Ingest stores the remainder as soon as it appears on chain,
                // before the executor confirms our transaction ID; wait for the
                // parent to be retired, keeping the last state seen for the error.
                let mut persisted = None;
                for _ in 0..600 {
                    if let Some(state) = remainder_state(&pg.url, &maker_note_id).await? {
                        let confirmed = state.0 == "executed";
                        persisted = Some(state);
                        if confirmed {
                            break;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                let (parent_status, parent_priority, child_status, child_priority, child_amount) =
                    persisted.context("settlement has no persisted maker remainder")?;
                if parent_status != "executed"
                    || child_status != "active"
                    || parent_priority <= 0
                    || child_priority != parent_priority
                    || child_amount != 50
                {
                    bail!(
                        "invalid remainder: parent={parent_status}/{parent_priority}, child={child_status}/{child_priority}, amount={child_amount}"
                    );
                }
                Ok(())
            }
            .await;
            {
                let chain_ro = rpc.mock_chain.read();
                let surplus = vault_balance(&chain_ro, solver_id, ibtc_id);
                match &verdict {
                    Ok(()) => println!("[test] PARTIAL FILL SETTLED — paybacks committed, 50-IBTC remainder active (solver_ibtc={surplus})."),
                    Err(e) => println!(
                        "[test] PARTIAL FILL FAILED: {e}\n[test]   solver_ibtc={surplus} committed={} (was {})",
                        chain_ro.committed_notes().len(), initial
                    ),
                }
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

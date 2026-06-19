//! `e2e provision` — create + deploy the solver wallet and two fungible faucets
//! on devnet, fund the solver with an initial inventory buffer, and write the
//! artifacts + a ready `solver.toml`.
//!
//! Two separate client contexts (own store + keystore each):
//!  * **solver context** — owns the solver account; its store IS the solver
//!    runtime's executor store, so the running solver finds the account + key.
//!  * **operator context** — owns the faucets (and, later, the user wallets that
//!    create PSWAPs); mints the solver's buffer.

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;

use crate::accounts;
use crate::artifacts::{self, Artifacts, TokenInfo};
use crate::devnet;

const DECIMALS: u8 = 8;
const MAX_SUPPLY: u64 = 1_000_000_000_000;
/// Initial per-token inventory minted into the solver so it can bridge fills.
const SOLVER_BUFFER: u64 = 100_000_000;

pub async fn run() -> Result<()> {
    artifacts::ensure_dir()?;

    // 1. Solver context (its store == the solver runtime's executor store).
    let (mut solver_cli, solver_ks) =
        devnet::build_client(&artifacts::solver_executor_store(), &artifacts::solver_keystore())
            .await
            .context("build solver-provisioning client")?;

    // 2. Operator context (faucets).
    let (mut op_cli, op_ks) =
        devnet::build_client(&artifacts::operator_store(), &artifacts::operator_keystore())
            .await
            .context("build operator client")?;

    solver_cli.sync_state().await.context("initial solver sync")?;
    op_cli.sync_state().await.context("initial operator sync")?;

    // 3. Create accounts (deployed lazily on first tx).
    tracing::info!("creating solver wallet…");
    let solver_id = accounts::create_wallet(&mut solver_cli, &solver_ks).await?;
    tracing::info!(%solver_id, "solver wallet created");

    tracing::info!("creating faucet A (MTA)…");
    let faucet_a = accounts::create_faucet(&mut op_cli, &op_ks, "MTA", DECIMALS, MAX_SUPPLY).await?;
    tracing::info!("creating faucet B (MTB)…");
    let faucet_b = accounts::create_faucet(&mut op_cli, &op_ks, "MTB", DECIMALS, MAX_SUPPLY).await?;
    tracing::info!(%faucet_a, %faucet_b, "faucets created");

    // 4. Mint the solver's buffer (this also DEPLOYS each faucet on its first tx).
    tracing::info!("minting solver buffer from faucet A…");
    let (mint_a, _) = accounts::mint(&mut op_cli, faucet_a, solver_id, SOLVER_BUFFER).await?;
    accounts::wait_for_tx(&mut op_cli, mint_a).await.context("await mint A")?;
    tracing::info!("minting solver buffer from faucet B…");
    let (mint_b, _) = accounts::mint(&mut op_cli, faucet_b, solver_id, SOLVER_BUFFER).await?;
    accounts::wait_for_tx(&mut op_cli, mint_b).await.context("await mint B")?;

    // 5. Solver consumes the two mint notes (deploys the solver wallet + credits
    //    its vault). Poll sync until both committed notes are visible.
    let notes = wait_for_committed_notes(&mut solver_cli, 2).await?;
    tracing::info!(count = notes.len(), "solver consuming mint notes…");
    let consume_tx = accounts::consume(&mut solver_cli, solver_id, notes).await?;
    accounts::wait_for_tx(&mut solver_cli, consume_tx).await.context("await consume")?;

    // 6. Verify balances.
    let bal_a = accounts::balance(&solver_cli, solver_id, faucet_a).await.unwrap_or(0);
    let bal_b = accounts::balance(&solver_cli, solver_id, faucet_b).await.unwrap_or(0);

    // 7. Persist artifacts + solver config.
    let art = Artifacts {
        rpc_endpoint: devnet::DEVNET_RPC.to_string(),
        solver_account_id: solver_id.to_hex(),
        token_a: TokenInfo {
            faucet_id: faucet_a.to_hex(),
            symbol: "MTA".to_string(),
            decimals: DECIMALS,
            external_symbol: "tether".to_string(),
        },
        token_b: TokenInfo {
            faucet_id: faucet_b.to_hex(),
            symbol: "MTB".to_string(),
            decimals: DECIMALS,
            external_symbol: "ethereum".to_string(),
        },
        solver_keystore_path: artifacts::solver_keystore(),
        solver_executor_store_path: artifacts::solver_executor_store(),
        solver_ingest_store_path: artifacts::solver_ingest_store(),
        solver_app_db_path: artifacts::solver_app_db(),
        operator_store_path: artifacts::operator_store(),
        operator_keystore_path: artifacts::operator_keystore(),
    };
    art.save(&artifacts::artifacts_path())?;
    art.write_solver_config(&artifacts::solver_config_path())?;

    print_summary(&art, solver_id, faucet_a, faucet_b, bal_a, bal_b);
    Ok(())
}

/// Poll-sync the client until at least `n` committed input notes are visible.
async fn wait_for_committed_notes(
    client: &mut miden_client::Client<miden_client::keystore::FilesystemKeyStore>,
    n: usize,
) -> Result<Vec<miden_client::note::Note>> {
    for attempt in 0..40 {
        client.sync_state().await.context("sync while waiting for notes")?;
        let notes = accounts::committed_input_notes(client).await?;
        if notes.len() >= n {
            return Ok(notes);
        }
        tracing::debug!(attempt, have = notes.len(), want = n, "waiting for committed notes…");
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    }
    anyhow::bail!("timed out waiting for {n} committed notes for the solver account")
}

fn print_summary(
    art: &Artifacts,
    solver_id: AccountId,
    faucet_a: AccountId,
    faucet_b: AccountId,
    bal_a: u64,
    bal_b: u64,
) {
    println!("\n========================= E2E DEVNET PROVISIONED =========================");
    println!("RPC                : {}", art.rpc_endpoint);
    println!("SOLVER ACCOUNT ID  : {}", solver_id.to_hex());
    println!("  (send P2ID notes here to fund it; it consumes them via `e2e fund`)");
    println!("FAUCET A (MTA)     : {}", faucet_a.to_hex());
    println!("FAUCET B (MTB)     : {}", faucet_b.to_hex());
    println!("solver balance MTA : {bal_a}");
    println!("solver balance MTB : {bal_b}");
    println!("-------------------------------------------------------------------------");
    println!("artifacts          : {}", artifacts::artifacts_path());
    println!("solver config      : {}", artifacts::solver_config_path());
    println!("=========================================================================\n");
    println!("To create PSWAPs the solver can match, mint MTA/MTB to your wallets and");
    println!("create opposing orders (MTA->MTB and MTB->MTA). Or use `e2e load` to do");
    println!("it automatically, then `e2e run` to start the solver and settle them.");
}

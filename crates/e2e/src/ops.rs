//! `fund`, `load`, and `mint` — the self-driving + hand-off operations.

use anyhow::{anyhow, Context, Result};
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;

use crate::accounts;
use crate::artifacts::{self, Artifacts};
use crate::devnet;

/// Per-token amount each user offers in a PSWAP, and the (smaller) amount it
/// requests back — so the two opposing orders cross with positive surplus that
/// the solver captures.
const OFFER: u64 = 10_000_000;
const REQUEST: u64 = 9_000_000;

/// Resolve token A/B faucet ids from artifacts.
fn faucets(art: &Artifacts) -> Result<(AccountId, AccountId)> {
    let a = AccountId::from_hex(&art.token_a.faucet_id).map_err(|e| anyhow!("token_a id: {e}"))?;
    let b = AccountId::from_hex(&art.token_b.faucet_id).map_err(|e| anyhow!("token_b id: {e}"))?;
    Ok((a, b))
}

/// `e2e fund [--amount N]` — mint `amount` of each token to the solver and
/// consume every committed note targeting it (mints AND any P2ID notes a user
/// sent). Leaves the solver with a fresh inventory buffer.
pub async fn fund(amount: u64) -> Result<()> {
    let art = Artifacts::load(&artifacts::artifacts_path())?;
    let (faucet_a, faucet_b) = faucets(&art)?;
    let solver_id =
        AccountId::from_hex(&art.solver_account_id).map_err(|e| anyhow!("solver id: {e}"))?;

    let (mut solver_cli, _) =
        devnet::build_client(&art.solver_executor_store_path, &art.solver_keystore_path).await?;
    let (mut op_cli, _) =
        devnet::build_client(&art.operator_store_path, &art.operator_keystore_path).await?;

    op_cli.sync_state().await.context("operator sync")?;
    tracing::info!(amount, "minting top-up to solver…");
    let (m_a, _) = accounts::mint(&mut op_cli, faucet_a, solver_id, amount).await?;
    accounts::wait_for_tx(&mut op_cli, m_a).await?;
    let (m_b, _) = accounts::mint(&mut op_cli, faucet_b, solver_id, amount).await?;
    accounts::wait_for_tx(&mut op_cli, m_b).await?;

    // Consume everything currently committed for the solver (mints + user P2ID).
    let mut notes = Vec::new();
    for attempt in 0..40 {
        solver_cli.sync_state().await.context("solver sync")?;
        notes = accounts::committed_input_notes(&solver_cli).await?;
        if !notes.is_empty() {
            break;
        }
        tracing::debug!(attempt, "waiting for committed notes to consume…");
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    }
    if notes.is_empty() {
        tracing::warn!("no committed notes found for the solver to consume");
    } else {
        tracing::info!(count = notes.len(), "solver consuming notes…");
        let tx = accounts::consume(&mut solver_cli, solver_id, notes).await?;
        accounts::wait_for_tx(&mut solver_cli, tx).await?;
    }

    let bal_a = accounts::balance(&solver_cli, solver_id, faucet_a).await.unwrap_or(0);
    let bal_b = accounts::balance(&solver_cli, solver_id, faucet_b).await.unwrap_or(0);
    println!("solver balance: {bal_a} {} / {bal_b} {}", art.token_a.symbol, art.token_b.symbol);
    Ok(())
}

/// `e2e mint --to <hex> --token a|b --amount N` — mint to an arbitrary account
/// (e.g. fund a user's wallet so they can create PSWAPs in the hand-off flow).
pub async fn mint(to: &str, token: &str, amount: u64) -> Result<()> {
    let art = Artifacts::load(&artifacts::artifacts_path())?;
    let (faucet_a, faucet_b) = faucets(&art)?;
    let faucet = match token.to_ascii_lowercase().as_str() {
        "a" | "mta" => faucet_a,
        "b" | "mtb" => faucet_b,
        other => return Err(anyhow!("unknown token {other:?} (use a|b)")),
    };
    let target = AccountId::from_hex(to).map_err(|e| anyhow!("--to id: {e}"))?;

    let (mut op_cli, _) =
        devnet::build_client(&art.operator_store_path, &art.operator_keystore_path).await?;
    op_cli.sync_state().await.context("operator sync")?;
    let (tx, note) = accounts::mint(&mut op_cli, faucet, target, amount).await?;
    accounts::wait_for_tx(&mut op_cli, tx).await?;
    println!("minted {amount} of {token} to {to} (note {})", note.id().to_hex());
    Ok(())
}

/// `e2e load [--rounds N]` — provision two user wallets and create `N` opposing
/// PSWAP pairs (user1: A->B, user2: B->A) the solver can match. The offered
/// amount exceeds the counterparty's request, so the orders cross with surplus.
pub async fn load(rounds: u32) -> Result<()> {
    let art = Artifacts::load(&artifacts::artifacts_path())?;
    let (faucet_a, faucet_b) = faucets(&art)?;

    let (mut op_cli, op_ks) =
        devnet::build_client(&art.operator_store_path, &art.operator_keystore_path).await?;
    op_cli.sync_state().await.context("operator sync")?;

    tracing::info!("creating two user wallets…");
    let user1 = accounts::create_wallet(&mut op_cli, &op_ks).await?;
    let user2 = accounts::create_wallet(&mut op_cli, &op_ks).await?;
    tracing::info!(%user1, %user2, "user wallets created");

    let mut created: Vec<String> = Vec::new();
    for round in 1..=rounds {
        tracing::info!(round, "funding users + creating opposing PSWAP pair…");

        // user1 gets OFFER of A; user2 gets OFFER of B.
        let (m1, n1) = accounts::mint(&mut op_cli, faucet_a, user1, OFFER).await?;
        accounts::wait_for_tx(&mut op_cli, m1).await?;
        let c1 = accounts::consume(&mut op_cli, user1, vec![n1]).await?;
        accounts::wait_for_tx(&mut op_cli, c1).await?;

        let (m2, n2) = accounts::mint(&mut op_cli, faucet_b, user2, OFFER).await?;
        accounts::wait_for_tx(&mut op_cli, m2).await?;
        let c2 = accounts::consume(&mut op_cli, user2, vec![n2]).await?;
        accounts::wait_for_tx(&mut op_cli, c2).await?;

        // Opposing PSWAPs: user1 offers A wants B; user2 offers B wants A.
        let offered1 = FungibleAsset::new(faucet_a, OFFER).map_err(|e| anyhow!("asset: {e}"))?;
        let requested1 = FungibleAsset::new(faucet_b, REQUEST).map_err(|e| anyhow!("asset: {e}"))?;
        let (p1, pn1) = accounts::create_pswap(&mut op_cli, user1, offered1, requested1).await?;
        accounts::wait_for_tx(&mut op_cli, p1).await?;

        let offered2 = FungibleAsset::new(faucet_b, OFFER).map_err(|e| anyhow!("asset: {e}"))?;
        let requested2 = FungibleAsset::new(faucet_a, REQUEST).map_err(|e| anyhow!("asset: {e}"))?;
        let (p2, pn2) = accounts::create_pswap(&mut op_cli, user2, offered2, requested2).await?;
        accounts::wait_for_tx(&mut op_cli, p2).await?;

        tracing::info!(round, pswap1 = %pn1.id().to_hex(), pswap2 = %pn2.id().to_hex(), "pair created");
        created.push(pn1.id().to_hex());
        created.push(pn2.id().to_hex());
    }

    let path = format!("{}/pending_pswaps.json", artifacts::E2E_DIR);
    std::fs::write(&path, serde_json::to_string_pretty(&created)?)
        .with_context(|| format!("write {path}"))?;
    println!(
        "created {} PSWAP notes across {rounds} round(s); ids recorded in {path}",
        created.len()
    );
    println!("now run `e2e run` to start the solver and settle them.");
    Ok(())
}

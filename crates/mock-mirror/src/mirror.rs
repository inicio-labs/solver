//! The mirror: discover user PSWAPs, post favorable counter-orders, and refill
//! from faucets. One straight-line tick loop, one client, no channels.

use anyhow::{anyhow, Context, Result};
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::transaction::TransactionRequestBuilder;
use miden_client::Client;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::rand::FeltRng;
use miden_protocol::note::Note;
use miden_standards::note::{PswapNote, PswapNoteStorage};

use crate::config::MockConfig;

type MockClient = Client<FilesystemKeyStore>;

// ───────────────────────────── mirror math (pure) ──────────────────────────

/// Amounts for the mock's counter-order, derived purely from a user order.
///
/// The user offers `offered` of token A and requests `requested` of token B.
/// The mock will offer B and request A. To stay matchable *and* hand the solver
/// a spread, the mock offers exactly the B the user wants (scaled by the fill
/// fraction) and requests slightly LESS A than the user offered — `spread_bps`
/// less. The solver keeps the difference, and the strict cross-product gate
/// clears precisely because `counter_requested < user.offered`.
///
/// Returns `None` when the order is too small to mirror favorably (dust): if
/// rounding would leave no spread, there is no matchable counter to post.
///
/// Returns `(counter_offered /* B */, counter_requested /* A */)`.
pub fn favorable_counter(
    offered: u64,    // user's offered A
    requested: u64,  // user's requested B
    spread_bps: u64, // solver edge, validated to 1..10000
    fill_num: u64,   // fill-fraction numerator (full = 1)
    fill_den: u64,   // fill-fraction denominator (full = 1)
) -> Option<(u64, u64)> {
    let counter_offered = mul_div(requested, fill_num, fill_den); // B the mock offers
    let scaled_a = mul_div(offered, fill_num, fill_den); // A at the user's own rate
    if counter_offered == 0 || scaled_a == 0 {
        return None;
    }
    // Shave the spread off the A the mock asks for, so the solver keeps it.
    // The `.min(scaled_a - 1)` guarantees a strictly favorable counter even on
    // tiny amounts where the bps rounding would otherwise erase the spread.
    let counter_requested =
        mul_div(scaled_a, 10_000 - spread_bps, 10_000).min(scaled_a.saturating_sub(1));
    if counter_requested == 0 {
        return None; // too small to be favorable
    }
    Some((counter_offered, counter_requested))
}

/// `a * num / den` in u128 to avoid overflow, back to u64.
fn mul_div(a: u64, num: u64, den: u64) -> u64 {
    ((a as u128 * num as u128) / den as u128) as u64
}

// ───────────────────────────── discovery parse ─────────────────────────────

/// A user order parsed from a discovered PSWAP note. Mirrors the fields the
/// solver reads in `solver::types::Order::from_note`.
struct UserOrder {
    offered_faucet: AccountId,
    offered_amount: u64,
    requested_faucet: AccountId,
    requested_amount: u64,
    creator: AccountId,
}

/// Parse a discovered note as a PSWAP order, or `None` if it isn't one / is
/// degenerate.
fn parse_pswap(note: &Note) -> Option<UserOrder> {
    if note.recipient().script().root() != PswapNote::script_root() {
        return None;
    }
    let pswap = PswapNote::try_from(note).ok()?;
    let offered = pswap.offered_asset();
    let requested = pswap.storage().requested_asset();
    let offered_amount: u64 = offered.amount();
    let requested_amount: u64 = requested.amount();
    if offered_amount == 0 || requested_amount == 0 {
        return None;
    }
    Some(UserOrder {
        offered_faucet: offered.faucet_id(),
        offered_amount,
        requested_faucet: requested.faucet_id(),
        requested_amount,
        creator: pswap.storage().creator_account_id(),
    })
}

// ───────────────────────────── the tick loop ───────────────────────────────

/// Subscribe the configured pairs, then run the mirror/claim/replenish tick
/// loop until Ctrl-C.
pub async fn run(client: &mut MockClient, cfg: &MockConfig) -> Result<()> {
    let mock_id = parse_id(&cfg.mock.account_id, "mock.account_id")?;
    let solver_id = parse_id(&cfg.settings.solver_account_id, "solver_account_id")?;

    subscribe_pairs(client, cfg).await?;
    tracing::info!(pairs = cfg.pairs.len(), "mock mirror running");

    let mut rng_state = cfg.settings.seed;
    loop {
        if let Err(e) = tick(client, cfg, mock_id, solver_id, &mut rng_state).await {
            tracing::warn!(error = %e, "tick failed; will retry next interval");
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl-C received, shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(cfg.settings.sync_interval_ms)) => {}
        }
    }
}

async fn tick(
    client: &mut MockClient,
    cfg: &MockConfig,
    mock_id: AccountId,
    solver_id: AccountId,
    rng_state: &mut u64,
) -> Result<()> {
    let summary = client.sync_state().await.map_err(|e| anyhow!("sync_state: {e}"))?;

    // 1. MIRROR — collect new user orders, build all counters, submit one tx.
    let mut specs: Vec<(FungibleAsset, FungibleAsset)> = Vec::new();
    for note_id in summary.new_public_notes.iter().chain(summary.new_private_notes.iter()) {
        if specs.len() >= cfg.settings.max_mirrors_per_tick {
            tracing::warn!(cap = cfg.settings.max_mirrors_per_tick, "mirror cap hit this tick");
            break;
        }
        let Ok(Some(record)) = client.get_input_note(*note_id).await else { continue };
        let Ok(note): Result<Note, _> = (&record).try_into() else { continue };
        let Some(order) = parse_pswap(&note) else { continue };
        // Loop guard: never mirror our own notes (or the solver's).
        if order.creator == mock_id || order.creator == solver_id {
            continue;
        }
        let partial = next_unit(rng_state) < cfg.settings.partial_fill_probability;
        let (fnum, fden) = if partial { (1, 2) } else { (1, 1) };
        let Some((counter_offered, counter_requested)) = favorable_counter(
            order.offered_amount,
            order.requested_amount,
            cfg.settings.spread_bps,
            fnum,
            fden,
        ) else {
            continue; // dust
        };
        // Mock offers the user's requested token, requests the user's offered token.
        let offered = FungibleAsset::new(order.requested_faucet, counter_offered)
            .map_err(|e| anyhow!("counter offered asset: {e}"))?;
        let requested = FungibleAsset::new(order.offered_faucet, counter_requested)
            .map_err(|e| anyhow!("counter requested asset: {e}"))?;
        specs.push((offered, requested));
    }
    if !specs.is_empty() {
        let count = specs.len();
        let notes = build_counter_notes(client, mock_id, specs)?;
        let req = TransactionRequestBuilder::new()
            .own_output_notes(notes)
            .build()
            .map_err(|e| anyhow!("build mirror tx: {e}"))?;
        client
            .submit_new_transaction(mock_id, req)
            .await
            .map_err(|e| anyhow!("submit mirror tx: {e}"))?;
        tracing::info!(count, "posted counter-orders");
    }

    // 2. REPLENISH — mint a top-up for any token below its low-water mark.
    // (Trade proceeds arrive as P2ID notes payable to the mock and are tracked
    // by the client on sync; we deliberately don't reclaim them — the mock
    // sustains inventory by minting, which it can always do, so a manual claim
    // step would be redundant for a testnet harness.)
    for item in &cfg.inventory {
        let faucet = parse_id(&item.faucet_id, "inventory.faucet_id")?;
        let balance = client
            .account_reader(mock_id)
            .get_balance(faucet)
            .await
            .map_err(|e| anyhow!("get_balance: {e}"))?;
        if balance >= item.low_water {
            continue;
        }
        tracing::warn!(%faucet, balance, low_water = item.low_water, "inventory low; minting top-up");
        let asset = FungibleAsset::new(faucet, item.topup)
            .map_err(|e| anyhow!("topup asset: {e}"))?;
        let req = TransactionRequestBuilder::new()
            .build_mint_fungible_asset(asset, mock_id, NoteType::Public, client.rng())
            .map_err(|e| anyhow!("build mint tx: {e}"))?;
        // Submitted AS the faucet account (the mock must control it).
        client
            .submit_new_transaction(faucet, req)
            .await
            .map_err(|e| anyhow!("submit mint tx: {e}"))?;
    }

    Ok(())
}

/// Build one PSWAP note per (offered, requested) spec, all sent by the mock.
/// Replicates the builder snippet inside `build_pswap_create` so several notes
/// can share one transaction.
fn build_counter_notes(
    client: &mut MockClient,
    mock_id: AccountId,
    specs: Vec<(FungibleAsset, FungibleAsset)>,
) -> Result<Vec<Note>> {
    let rng = client.rng();
    let mut notes = Vec::with_capacity(specs.len());
    for (offered, requested) in specs {
        let storage = PswapNoteStorage::builder()
            .requested_asset(requested)
            .creator_account_id(mock_id)
            .payback_note_type(NoteType::Public)
            .build();
        let pswap = PswapNote::builder()
            .sender(mock_id)
            .storage(storage)
            .serial_number(rng.draw_word())
            .note_type(NoteType::Public)
            .offered_asset(offered)
            .maybe_attachment(None)
            .build()
            .map_err(|e| anyhow!("build counter pswap: {e}"))?;
        notes.push(pswap.into());
    }
    Ok(notes)
}

async fn subscribe_pairs(client: &mut MockClient, cfg: &MockConfig) -> Result<()> {
    for pair in &cfg.pairs {
        let a = parse_id(&pair.token_a_faucet_id, "pair.token_a_faucet_id")?;
        let b = parse_id(&pair.token_b_faucet_id, "pair.token_b_faucet_id")?;
        subscribe_dir(client, a, b).await?;
        subscribe_dir(client, b, a).await?;
    }
    Ok(())
}

/// Subscribe the tag for orders offering `offered` and requesting `requested`.
/// Tag depends only on faucet ids; the `1` amounts are placeholders.
async fn subscribe_dir(client: &mut MockClient, offered: AccountId, requested: AccountId) -> Result<()> {
    let offered_asset = FungibleAsset::new(offered, 1).map_err(|e| anyhow!("tag offered: {e}"))?;
    let requested_asset = FungibleAsset::new(requested, 1).map_err(|e| anyhow!("tag requested: {e}"))?;
    let tag = PswapNote::create_tag(NoteType::Public, &offered_asset, &requested_asset);
    client.add_note_tag(tag).await.map_err(|e| anyhow!("add_note_tag: {e}"))?;
    Ok(())
}

fn parse_id(hex: &str, what: &str) -> Result<AccountId> {
    AccountId::from_hex(hex).with_context(|| format!("invalid {what}: {hex:?}"))
}

/// Tiny seeded LCG → a value in [0, 1). Reproducible from `cfg.settings.seed`; used only
/// for the full-vs-partial coin flip, so it needn't be cryptographic.
fn next_unit(state: &mut u64) -> f64 {
    // Numerical Recipes LCG constants.
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    // Top 53 bits → [0, 1).
    ((*state >> 11) as f64) / ((1u64 << 53) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The solver's direct gate: user.offered * mock.offered > user.requested * mock.requested.
    fn gate_clears(uo: u64, ur: u64, mo: u64, mr: u64) -> bool {
        (uo as u128) * (mo as u128) > (ur as u128) * (mr as u128)
    }

    #[test]
    fn full_fill_is_favorable_and_matchable() {
        // user offers 1000 A, wants 318 B; spread 30bps.
        let (mo, mr) = favorable_counter(1000, 318, 30, 1, 1).unwrap();
        assert_eq!(mo, 318, "mock offers exactly the B the user wants");
        assert!(mr < 1000, "mock requests strictly less A than the user offered");
        assert!(gate_clears(1000, 318, mo, mr), "solver's cross-product gate must clear");
    }

    #[test]
    fn partial_fill_leaves_a_remainder_and_clears_gate() {
        // half fill: mock offers less B than the user wants -> remainder.
        let (mo, mr) = favorable_counter(1000, 318, 30, 1, 2).unwrap();
        assert!(mo < 318, "half fill offers less B than requested (leaves remainder)");
        assert!(gate_clears(1000, 318, mo, mr));
    }

    #[test]
    fn dust_is_skipped() {
        // 1:1 of size 1 cannot leave any spread -> no matchable counter.
        assert!(favorable_counter(1, 1, 30, 1, 1).is_none());
    }
}

//! The mirror: discover user PSWAPs, post favorable counter-orders, and refill
//! from faucets. One straight-line tick loop, one client, no channels.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::store::NoteFilter;
use miden_client::transaction::{TransactionRequest, TransactionRequestBuilder};
use miden_client::{Client, ClientError};
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::rand::FeltRng;
use miden_protocol::note::Note;
use miden_standards::note::{PswapNote, PswapNoteStorage};

use crate::config::MockConfig;

pub(crate) type MockClient = Client<FilesystemKeyStore>;

// ─────────────── resilient submit: backoff on transient failures ────────────

/// Max retry attempts for a transient submit failure. Attempt 0 is the first
/// try, so up to `MAX_SUBMIT_RETRIES + 1` submits before giving up.
const MAX_SUBMIT_RETRIES: u32 = 5;
/// Initial backoff; doubles each retry, capped at `MAX_SUBMIT_BACKOFF`.
const INITIAL_SUBMIT_BACKOFF: Duration = Duration::from_millis(500);
const MAX_SUBMIT_BACKOFF: Duration = Duration::from_secs(30);

/// A submit failure worth retrying: it came from the network (`RpcError`) or the
/// remote prover (`TransactionProvingError` — e.g. a "Timeout expired" /
/// `Cancelled` from a congested public prover). A `TransactionExecutorError` is
/// deterministic (a failed assertion — e.g. a foreign tag-collision P2ID note
/// whose target-account check fails): retrying cannot help, so it is NOT
/// transient and the caller skips it.
pub(crate) fn is_transient(err: &ClientError) -> bool {
    matches!(err, ClientError::RpcError(_) | ClientError::TransactionProvingError(_))
}

/// Submit a transaction with bounded exponential backoff on transient
/// (network / prover) failures.
///
/// IDEMPOTENCY (critical): a transient error — especially an RPC timeout — can
/// mean the tx actually *landed* but the response was lost. Blindly retrying
/// would double-post a counter-order (or double-consume a note). Two guards keep
/// retries safe:
///  1. `build_req` MUST be deterministic across attempts — the counter note's
///     serial (hence its note id), or the note being consumed, is fixed — so a
///     landed tx re-submitted is rejected by the network as a duplicate
///     (a deterministic error that stops the loop, never a second on-chain tx).
///  2. Before each retry we re-sync and compare the account nonce to its
///     pre-submit value: any landed tx (always FROM this account) advances it,
///     so if it advanced a prior attempt already succeeded — return Ok without
///     re-submitting.
///
/// `build_req` is a closure so the request (which `.build()` consumes) can be
/// rebuilt fresh each attempt from the same deterministic inputs.
pub(crate) async fn submit_with_backoff(
    client: &mut MockClient,
    account: AccountId,
    label: &str,
    mut build_req: impl FnMut() -> Result<TransactionRequest>,
) -> Result<()> {
    // Pre-submit nonce — the idempotency baseline (local store read, no
    // round-trip). Best-effort: if it can't be read we simply skip guard #2 and
    // rely on guard #1 (the network rejecting a duplicate).
    let nonce_before = client.account_reader(account).nonce().await.ok();

    let mut backoff = INITIAL_SUBMIT_BACKOFF;
    for attempt in 0..=MAX_SUBMIT_RETRIES {
        // On a retry, first check whether the previous attempt actually landed
        // (committed on-chain but its response was lost): a state sync + nonce
        // bump proves it, so we must NOT re-submit.
        if attempt > 0 {
            if let Some(before) = nonce_before {
                let _ = client.sync_state().await;
                if let Ok(now) = client.account_reader(account).nonce().await {
                    if now != before {
                        tracing::info!(label = %label, "prior attempt landed (nonce advanced); not re-submitting");
                        return Ok(());
                    }
                }
            }
        }

        let req = build_req().map_err(|e| anyhow!("{label}: build request: {e}"))?;
        match client.submit_new_transaction(account, req).await {
            Ok(_) => return Ok(()),
            Err(e) if is_transient(&e) && attempt < MAX_SUBMIT_RETRIES => {
                tracing::warn!(
                    label = %label, attempt, backoff_ms = backoff.as_millis() as u64, error = %e,
                    "transient submit error; backing off then retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_SUBMIT_BACKOFF);
            }
            // Exhausted retries, or a deterministic (non-transient) failure.
            Err(e) => return Err(anyhow!("{label}: {e}")),
        }
    }
    unreachable!("loop returns inside the matched arms")
}

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
    let offered_amount: u64 = offered.amount().into();
    let requested_amount: u64 = requested.amount().into();
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
    let mock_id = parse_id(
        cfg.mock
            .account_id
            .as_deref()
            .context("mock.account_id is required to run — `mock-mirror provision` first, then set it")?,
        "mock.account_id",
    )?;
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

    // MIRRORING TAKES PRIORITY OVER CLAIMING. `new_public_notes` is
    // EDGE-TRIGGERED — an order shows up only on the sync it first arrives, so we
    // MUST counter it this tick or it's lost. Claiming reads the *persistent*
    // `Committed` filter, so it can safely wait for an idle tick. Doing both in
    // one tick would also risk a same-account nonce clash (claim tx vs counter
    // tx). So: if this sync brought new notes, go straight to mirroring; only
    // claim incoming P2ID notes (funding + trade proceeds) on idle ticks.
    let has_new_notes =
        !summary.new_public_notes.is_empty() || !summary.new_private_notes.is_empty();
    if !has_new_notes {
        match claim_incoming(client, mock_id).await {
            Ok(n) if n > 0 => tracing::info!(claimed = n, "auto-claimed incoming notes"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "auto-claim failed; will retry"),
        }
        return Ok(());
    }

    // 1. MIRROR — post ONE counter tx PER user order, each retried independently.
    // Per-note (not one batched tx) so a single order's transient proving/RPC
    // failure can't stall the others: it's retried with backoff, and if it still
    // fails it's skipped without blocking the rest (mirrors `claim_incoming`).
    let mut considered = 0usize;
    let mut posted = 0usize;
    for note_id in summary.new_public_notes.iter().chain(summary.new_private_notes.iter()) {
        if considered >= cfg.settings.max_mirrors_per_tick {
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
        considered += 1;

        // Build the counter note ONCE so its serial (hence note id) is fixed
        // across retries — the idempotency guarantee `submit_with_backoff` relies
        // on (a landed-but-lost retry is rejected as a duplicate note).
        let counter = match build_counter_note(client, mock_id, offered, requested) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "skip order: build counter note failed");
                continue;
            }
        };
        match submit_with_backoff(client, mock_id, "counter", || {
            TransactionRequestBuilder::new()
                .own_output_notes(vec![counter.clone()])
                .build()
                .map_err(|e| anyhow!("build mirror tx: {e}"))
        })
        .await
        {
            Ok(()) => posted += 1,
            Err(e) => tracing::warn!(error = %e, order = ?note_id, "counter failed after retries; skipped"),
        }
    }
    if posted > 0 {
        tracing::info!(count = posted, "posted counter-orders");
    }

    // 2. MONITOR — warn (never mint) when inventory runs low. The mock does NOT
    // control the external faucets, so it can't self-replenish; it's funded
    // out-of-band by minting the public faucet to the mock's address. Trade
    // proceeds also arrive as P2ID notes payable to the mock and are tracked on
    // sync. If a token's balance is too low to post a counter, that tx simply
    // fails and is retried next tick — this warning is the operator's signal to
    // top it up.
    for item in &cfg.inventory {
        let faucet = parse_id(&item.faucet_id, "inventory.faucet_id")?;
        let balance = client
            .account_reader(mock_id)
            .get_balance(faucet)
            .await
            .map_err(|e| anyhow!("get_balance: {e}"))?;
        if balance < item.low_water {
            tracing::warn!(
                %faucet, balance, low_water = item.low_water,
                "inventory low — fund the mock by minting this token to its address"
            );
        }
    }

    Ok(())
}

/// Build one PSWAP counter note (mock offers `offered`, requests `requested`).
/// The serial is drawn once here; the caller reuses the returned note across
/// submit retries so its note id stays fixed (idempotency — see
/// `submit_with_backoff`).
fn build_counter_note(
    client: &mut MockClient,
    mock_id: AccountId,
    offered: FungibleAsset,
    requested: FungibleAsset,
) -> Result<Note> {
    let rng = client.rng();
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
    Ok(pswap.into())
}

/// Consume incoming **P2ID** notes (funding mints + trade-proceeds paybacks)
/// into the account's vault. Deliberately SKIPS PSWAP notes: those are the user
/// orders the mirror counters (and the solver consumes) — consuming them here
/// would steal/short-circuit them. Returns how many were consumed. Assumes the
/// client was just synced.
async fn claim_incoming(client: &mut MockClient, account: AccountId) -> Result<usize> {
    let mut notes: Vec<Note> = Vec::new();
    for record in client
        .get_input_notes(NoteFilter::Committed)
        .await
        .map_err(|e| anyhow!("get_input_notes: {e}"))?
    {
        let note: Note = record.try_into().map_err(|_| anyhow!("input-note record -> Note"))?;
        if note.recipient().script().root() == PswapNote::script_root() {
            continue; // a user PSWAP order — counter it, never consume it
        }
        notes.push(note);
    }
    if notes.is_empty() {
        return Ok(0);
    }
    // Consume per-note and tolerate un-consumable ones: on a busy network the
    // account's 32-bit note tag collides with foreign P2ID notes, which fail the
    // target-account assertion. A batch consume would fail the whole tx on one
    // such note; per-note skips them and claims the genuine ones.
    let mut ok = 0usize;
    for note in notes {
        // Retry transient prover/RPC failures with backoff; skip notes that fail
        // deterministically (foreign tag-collision P2ID notes whose target-account
        // assertion fails). Idempotent: a landed-but-lost consume is caught by the
        // nonce check, or rejected as an already-nullified note on retry.
        match submit_with_backoff(client, account, "claim", || {
            TransactionRequestBuilder::new()
                .build_consume_notes(vec![note.clone()])
                .map_err(|e| anyhow!("build_consume: {e}"))
        })
        .await
        {
            Ok(()) => ok += 1,
            Err(e) => tracing::debug!(error = %e, "skip un-consumable note (foreign tag-collision or exhausted)"),
        }
    }
    Ok(ok)
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

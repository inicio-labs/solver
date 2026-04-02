use std::collections::HashMap;

use anyhow::{Context, Result};
use miden_client::{keystore::FilesystemKeyStore, note::Note, store::NoteFilter, Client};
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::note::NoteType;
use miden_standards::note::PswapNote;
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};

use crate::events::{MatchDto, OrderDto, SolverEvent};
use crate::executor::Executor;
use crate::order::{AssetPair, Order};
use crate::simple_matcher::SimpleMatcher;

/// Register PSWAP note tags for an asset pair so that `sync_state` fetches
/// notes for both trade directions (offers X wants Y, and offers Y wants X).
pub async fn register_pair_tags(
    client: &mut Client<FilesystemKeyStore>,
    faucet_x: AccountId,
    faucet_y: AccountId,
) -> Result<()> {
    let asset_x = FungibleAsset::new(faucet_x, 1)
        .map_err(|e| anyhow::anyhow!("Failed to create asset X: {}", e))?;
    let asset_y = FungibleAsset::new(faucet_y, 1)
        .map_err(|e| anyhow::anyhow!("Failed to create asset Y: {}", e))?;

    let tag_xy = PswapNote::create_tag(NoteType::Public, &asset_x, &asset_y);
    let tag_yx = PswapNote::create_tag(NoteType::Public, &asset_y, &asset_x);

    client
        .add_note_tag(tag_xy)
        .await
        .context("Failed to register tag for X->Y")?;
    client
        .add_note_tag(tag_yx)
        .await
        .context("Failed to register tag for Y->X")?;

    Ok(())
}

/// Sync state and fetch committed PSWAP notes.
pub async fn fetch_pswap_notes(client: &mut Client<FilesystemKeyStore>) -> Result<Vec<Note>> {
    client.sync_state().await?;

    let note_records = client.get_input_notes(NoteFilter::Committed).await?;

    let notes: Vec<Note> = note_records
        .into_iter()
        .filter_map(|record| {
            let note: Result<Note, _> = record.try_into();
            note.ok()
        })
        .filter(|note| note.recipient().script().root() == PswapNote::script_root())
        .collect();

    Ok(notes)
}

fn emit(tx: &Option<broadcast::Sender<SolverEvent>>, event: SolverEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event);
    }
}

/// Start the solver event loop.
///
/// This is the main entry point for the solver library. It continuously:
/// 1. Fetches new PSWAP notes from the network
/// 2. Matches orders using SimpleMatcher
/// 3. Executes matched orders via Executor
pub async fn start(
    client: &mut Client<FilesystemKeyStore>,
    solver_id: AccountId,
    pairs: Vec<AssetPair>,
    fetch_interval_ms: u64,
    pulse_interval_ms: u64,
    event_tx: Option<broadcast::Sender<SolverEvent>>,
) -> Result<()> {
    // Register PSWAP note tags for each pair
    for pair in &pairs {
        register_pair_tags(client, pair.base, pair.quote)
            .await
            .with_context(|| {
                format!(
                    "Failed to register tags for pair {:?}/{:?}",
                    pair.base, pair.quote
                )
            })?;
    }

    // Order book state
    let mut state: HashMap<AssetPair, Vec<Order>> = HashMap::new();
    for pair in &pairs {
        state.insert(pair.clone(), Vec::new());
    }

    println!("Solver started. Solver ID: {:?}", solver_id);
    println!("Monitoring {} pair(s)", pairs.len());

    let mut fetch_tick = interval(Duration::from_millis(fetch_interval_ms));
    let mut pulse_tick = interval(Duration::from_millis(pulse_interval_ms));

    loop {
        tokio::select! {
            _ = fetch_tick.tick() => {
                // Fetch new notes
                match fetch_pswap_notes(client).await {
                    Ok(notes) => {
                        for note in &notes {
                            match Order::from_note(note) {
                                Ok(order) => {
                                    let pair = order.pair();
                                    let existing = state.entry(pair).or_default();
                                    let note_id = order.note.id();
                                    if !existing.iter().any(|o| o.note.id() == note_id) {
                                        existing.push(order);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse note {}: {}", note.id(), e);
                                }
                            }
                        }
                        println!("Fetched {} notes. Order book:", notes.len());
                        for (pair, orders) in &state {
                            println!("  pair base={} quote={}: {} orders",
                                pair.base.to_hex(), pair.quote.to_hex(), orders.len());
                        }

                        // Emit OrderBookSnapshot
                        for pair in &pairs {
                            if let Some(orders) = state.get(pair) {
                                let mut ask_dtos = Vec::new();
                                let mut bid_dtos = Vec::new();
                                for order in orders {
                                    if order.offered_faucet_id == pair.base {
                                        ask_dtos.push(OrderDto::from_order(order, "ask"));
                                    } else {
                                        bid_dtos.push(OrderDto::from_order(order, "bid"));
                                    }
                                }
                                emit(&event_tx, SolverEvent::OrderBookSnapshot {
                                    asks: ask_dtos,
                                    bids: bid_dtos,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Fetch error: {}", e);
                    }
                }
            }
            _ = pulse_tick.tick() => {
                // Run matching + execution for each pair
                for pair in &pairs {
                    let orders = match state.get(pair) {
                        Some(orders) if !orders.is_empty() => orders.clone(),
                        _ => continue,
                    };

                    println!("Pulse: {} orders for pair {:?}", orders.len(), pair);

                    let mut asks = Vec::new(); // offers X (base), wants Y (quote)
                    let mut bids = Vec::new(); // offers Y (quote), wants X (base)
                    for order in orders {
                        if order.offered_faucet_id == pair.base {
                            asks.push(order);
                        } else {
                            bids.push(order);
                        }
                    }

                    println!("[engine] Split: {} asks, {} bids", asks.len(), bids.len());
                    for (i, a) in asks.iter().enumerate() {
                        println!("[engine]   ask[{}]: offered={}({}) requested={}({}) price={:.4}",
                            i, a.offered_faucet_id.to_hex(), a.offered_amount,
                            a.requested_faucet_id.to_hex(), a.requested_amount, a.price_ratio());
                    }
                    for (i, b) in bids.iter().enumerate() {
                        println!("[engine]   bid[{}]: offered={}({}) requested={}({}) price={:.4}",
                            i, b.offered_faucet_id.to_hex(), b.offered_amount,
                            b.requested_faucet_id.to_hex(), b.requested_amount, b.price_ratio());
                    }

                    if asks.is_empty() || bids.is_empty() {
                        println!("[engine] Need both asks and bids to match, skipping");
                        continue;
                    }

                    let (bids, asks) = SimpleMatcher::run(bids, asks);

                    println!("[engine] After matcher:");
                    for (i, a) in asks.iter().enumerate() {
                        println!("[engine]   ask[{}]: fill_amount={}", i, a.fill_amount);
                    }
                    for (i, b) in bids.iter().enumerate() {
                        println!("[engine]   bid[{}]: fill_amount={}", i, b.fill_amount);
                    }

                    // Partition into filled and unfilled
                    let (filled_bids, unfilled_bids): (Vec<Order>, Vec<Order>) =
                        bids.into_iter().partition(|o| o.fill_amount != 0);
                    let (filled_asks, unfilled_asks): (Vec<Order>, Vec<Order>) =
                        asks.into_iter().partition(|o| o.fill_amount != 0);

                    if filled_bids.is_empty() || filled_asks.is_empty() {
                        println!("[engine] No matches: {} filled_bids, {} filled_asks", filled_bids.len(), filled_asks.len());
                        continue;
                    }

                    println!(
                        "Matched: {} bids + {} asks",
                        filled_bids.len(), filled_asks.len()
                    );

                    match Executor::execute_simple_match(
                        client,
                        solver_id,
                        &filled_asks,
                        &filled_bids,
                    )
                    .await
                    {
                        Ok(result) => {
                            emit(&event_tx, SolverEvent::MatchExecuted {
                                matched: MatchDto {
                                    ask_ids: filled_asks.iter().map(|o| o.note.id().to_string()).collect(),
                                    bid_ids: filled_bids.iter().map(|o| o.note.id().to_string()).collect(),
                                    surplus_x: result.surplus_x,
                                    surplus_y: result.surplus_y,
                                    total_x: result.total_x,
                                    total_y: result.total_y,
                                },
                            });

                            // Put back only the unfilled orders
                            let mut remaining = unfilled_bids;
                            remaining.extend(unfilled_asks);
                            state.insert(pair.clone(), remaining);
                        }
                        Err(e) => {
                            emit(&event_tx, SolverEvent::MatchFailed {
                                error: e.to_string(),
                            });
                            eprintln!("Failed to execute match: {}", e);
                        }
                    }
                }
            }
        }
    }
}

use std::collections::HashMap;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;
use tokio::time::{Duration, interval};

use solver::client::SolverClient;
use solver::config::SolverConfig;
use solver::executor::Executor;
use solver::matcher::Matcher;
use solver::order::{AssetPair, Order};

fn parse_account_id(hex_str: &str) -> Result<AccountId> {
    AccountId::from_hex(hex_str)
        .with_context(|| format!("Failed to parse account ID: {}", hex_str))
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SolverConfig::load("solver.toml")?;

    let solver_id = parse_account_id(&config.solver.account_id)
        .context("Failed to parse solver account ID")?;

    // Parse configured asset pairs
    let mut pair_configs: Vec<(AssetPair, AccountId, AccountId)> = Vec::new();
    for pair_cfg in &config.pairs {
        let faucet_x = parse_account_id(&pair_cfg.asset_x_faucet_id)
            .with_context(|| format!("Failed to parse faucet X for pair {}", pair_cfg.name))?;
        let faucet_y = parse_account_id(&pair_cfg.asset_y_faucet_id)
            .with_context(|| format!("Failed to parse faucet Y for pair {}", pair_cfg.name))?;
        let pair = AssetPair::new(faucet_x, faucet_y);
        pair_configs.push((pair, faucet_x, faucet_y));
    }

    // Single client (Client is not Send, so we use one client in a single-threaded loop)
    let mut solver_client = SolverClient::new(&config)
        .await
        .context("Failed to build solver client")?;

    // Register PSWAP note tags for each pair
    for (_, faucet_x, faucet_y) in &pair_configs {
        solver_client
            .register_pair_tags(*faucet_x, *faucet_y)
            .await
            .with_context(|| format!("Failed to register tags for pair {:?}/{:?}", faucet_x, faucet_y))?;
    }

    // Order book state
    let mut state: HashMap<AssetPair, Vec<Order>> = HashMap::new();
    for (pair, _, _) in &pair_configs {
        state.insert(pair.clone(), Vec::new());
    }

    println!("Solver started. Solver ID: {:?}", solver_id);
    println!("Monitoring {} pair(s)", pair_configs.len());

    let mut fetch_tick = interval(Duration::from_millis(config.engine.fetch_interval_ms));
    let mut pulse_tick = interval(Duration::from_millis(config.engine.pulse_interval_ms));

    loop {
        tokio::select! {
            _ = fetch_tick.tick() => {
                // Fetch new notes
                match solver_client.fetch_pswap_notes().await {
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
                        println!("Fetched {} notes", notes.len());
                    }
                    Err(e) => {
                        eprintln!("Fetch error: {}", e);
                    }
                }
            }
            _ = pulse_tick.tick() => {
                // Run matching + execution for each pair
                for (pair, faucet_x, faucet_y) in &pair_configs {
                    let orders = match state.get(pair) {
                        Some(orders) if !orders.is_empty() => orders.clone(),
                        _ => continue,
                    };

                    println!("Pulse: {} orders for pair {:?}", orders.len(), pair);

                    let group = match Matcher::run(orders, *faucet_x, *faucet_y) {
                        Some(g) => g,
                        None => continue,
                    };

                    println!(
                        "Found match group: {}A + {}B orders",
                        group.side_a.len(), group.side_b.len()
                    );

                    match Executor::execute_match_group(
                        &mut solver_client.client,
                        solver_id,
                        &group,
                    )
                    .await
                    {
                        Ok(()) => {
                            if let Some(orders) = state.get_mut(pair) {
                                let matched_ids: Vec<_> = group.side_a.iter()
                                    .chain(group.side_b.iter())
                                    .map(|f| f.order.note.id())
                                    .collect();
                                orders.retain(|o| !matched_ids.contains(&o.note.id()));
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to execute match group: {}", e);
                        }
                    }
                }
            }
        }
    }
}

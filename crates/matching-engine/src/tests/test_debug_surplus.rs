use crate::order_book::OrderBook;
use crate::price_feed::SimpleMapFeed;
use crate::direct_matching::run_direct_matching;
use super::{eth, usdc};

fn pseudo_rand(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// Core settlement invariant:
///   For token X: sum of X released by orders offering X >= sum of X received by orders requesting X
///
/// order A offers ETH, requests USDC. A releases ETH, receives USDC.
/// order B offers USDC, requests ETH. B releases USDC, receives ETH.
///
/// ETH flow: A releases offered_for(A.requested_filled) ETH. B receives B.requested_filled ETH.
///   → A.offered_for(A.requested_filled) >= B.requested_filled
///
/// USDC flow: B releases offered_for(B.requested_filled) USDC. A receives A.requested_filled USDC.
///   → B.offered_for(B.requested_filled) >= A.requested_filled
///
/// Both must hold for the settlement to be solvent.
#[test]
fn settlement_solvency_check() {
    let mut seed: u64 = 777888;
    let mut eth_deficit_count = 0u32;
    let mut usdc_deficit_count = 0u32;
    let mut max_eth_deficit = 0u32;
    let mut max_usdc_deficit = 0u32;
    let mut total_matched = 0u32;

    for _trial in 0..10_000 {
        let mut feed = SimpleMapFeed::new();
        feed.set_price_cents(eth(), 100 + (pseudo_rand(&mut seed) % 1000) as u64);
        feed.set_price_cents(usdc(), 100 + (pseudo_rand(&mut seed) % 1000) as u64);

        let mut book = OrderBook::new(feed);

        let off_a = 10 + (pseudo_rand(&mut seed) % 1000) as u32;
        let req_a = 10 + (pseudo_rand(&mut seed) % 1000) as u32;
        let off_b = 10 + (pseudo_rand(&mut seed) % 1000) as u32;
        let req_b = 10 + (pseudo_rand(&mut seed) % 1000) as u32;

        if book.add_user_order(eth(), usdc(), off_a, req_a).is_none() { continue; }
        if book.add_user_order(usdc(), eth(), off_b, req_b).is_none() { continue; }

        let (filled, _) = run_direct_matching(&mut book);
        if filled.is_empty() { continue; }
        total_matched += 1;

        let a = &book.orders[0]; // offers ETH, requests USDC
        let b = &book.orders[1]; // offers USDC, requests ETH

        if a.requested_filled() == 0 || b.requested_filled() == 0 { continue; }

        let a_released_eth = a.offered_for(a.requested_filled());
        let b_received_eth = b.requested_filled();
        let b_released_usdc = b.offered_for(b.requested_filled());
        let a_received_usdc = a.requested_filled();

        if a_released_eth < b_received_eth {
            eth_deficit_count += 1;
            max_eth_deficit = max_eth_deficit.max(b_received_eth - a_released_eth);
        }
        if b_released_usdc < a_received_usdc {
            usdc_deficit_count += 1;
            max_usdc_deficit = max_usdc_deficit.max(a_received_usdc - b_released_usdc);
        }
    }

    println!("Matched: {}", total_matched);
    println!("ETH deficits:  {}/{} (max {})", eth_deficit_count, total_matched, max_eth_deficit);
    println!("USDC deficits: {}/{} (max {})", usdc_deficit_count, total_matched, max_usdc_deficit);

    // STRICT: zero deficits in either direction
    assert_eq!(eth_deficit_count, 0, "ETH settlement insolvent in {} cases (max deficit {})", eth_deficit_count, max_eth_deficit);
    assert_eq!(usdc_deficit_count, 0, "USDC settlement insolvent in {} cases (max deficit {})", usdc_deficit_count, max_usdc_deficit);
}

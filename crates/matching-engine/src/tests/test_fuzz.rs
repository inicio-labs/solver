use crate::engine::MatchingEngine;
use crate::order_book::OrderBook;
use crate::price_feed::SimpleMapFeed;
use super::{eth, usdc, sol, btc, matic, NoteIdGen};

fn pseudo_rand(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

#[test]
fn fuzz_random_orders() {
    let token_a = eth();
    let token_b = usdc();
    let prices: [(u64, u64); 5] = [(100, 1), (1, 1), (1, 100), (2000, 1), (150, 1)];
    let offered_amounts = [1, 2, 5, 10, 50, 100, 500, 1000, 5000, 10000];
    let rate_pcts = [50, 70, 80, 90, 95, 100];

    let mut cases = 0u64;
    for &(pa, pb) in &prices {
        let mut feed = SimpleMapFeed::new();
        feed.set_price_cents(token_a, pa);
        feed.set_price_cents(token_b, pb);

        for &sa in &offered_amounts {
            for &ra in &rate_pcts {
                for &sb in &offered_amounts {
                    for &rb in &rate_pcts {
                        let ba = (sa as u128 * pa as u128 * ra as u128 / (pb as u128 * 100)) as u32;
                        let bb = (sb as u128 * pb as u128 * rb as u128 / (pa as u128 * 100)) as u32;
                        if ba == 0 || bb == 0 { continue; }

                        let mut book = OrderBook::new(feed.clone());
                        let mut gen = NoteIdGen::new();
                        book.add_user_order(gen.next(), token_a, token_b, sa, ba);
                        book.add_user_order(gen.next(), token_b, token_a, sb, bb);

                        let mut engine = MatchingEngine::new(book);
                        let batch = engine.run();

                        // Filled orders should have valid fill amounts
                        for oid in &batch.filled_orders {
                            let order = &engine.book.orders[oid];
                            assert!(order.requested_filled() > 0);
                            assert!(order.requested_filled() <= order.requested);
                        }
                        cases += 1;
                    }
                }
            }
        }
    }
    println!("fuzz_random_orders: {} cases passed", cases);
}

#[test]
fn fuzz_multi_token() {
    let tokens = [eth(), usdc(), sol(), btc(), matic()];
    let prices: [u64; 5] = [2000, 1, 150, 60000, 50];
    let mut seed: u64 = 12345;

    for trial in 0..50 {
        let mut feed = SimpleMapFeed::new();
        for i in 0..5 { feed.set_price_cents(tokens[i], prices[i]); }

        let mut book = OrderBook::new(feed.clone());
        let mut gen = NoteIdGen::new();

        let n = 20 + (pseudo_rand(&mut seed) % 20) as usize;
        for _ in 0..n {
            let si = (pseudo_rand(&mut seed) % 5) as usize;
            let mut bi = (pseudo_rand(&mut seed) % 5) as usize;
            if bi == si { bi = (si + 1) % 5; }

            let offered = 1 + (pseudo_rand(&mut seed) % 1000) as u32;
            let rate_pct = 50 + (pseudo_rand(&mut seed) % 50) as u64;
            let requested = (offered as u128 * prices[si] as u128 * rate_pct as u128
                / (prices[bi] as u128 * 100)) as u32;
            if requested == 0 { continue; }
            book.add_user_order(gen.next(), tokens[si], tokens[bi], offered, requested);
        }

        let mut engine = MatchingEngine::new(book);
        let batch = engine.run();

        for oid in &batch.filled_orders {
            let order = &engine.book.orders[oid];
            assert!(order.requested_filled() > 0, "trial {}: filled order has 0 fill", trial);
            assert!(order.requested_filled() <= order.requested, "trial {}: fill exceeds order", trial);
        }
        assert!(batch.remaining_orders <= engine.book.orders.len() as u32);
    }
    println!("fuzz_multi_token: 50 trials passed");
}

#[test]
fn fuzz_no_panic() {
    let token_a = eth();
    let token_b = usdc();
    let mut seed: u64 = 99999;

    for _ in 0..50 {
        let mut feed = SimpleMapFeed::new();
        feed.set_price_cents(token_a, 2000);
        feed.set_price_cents(token_b, 1);

        let mut book = OrderBook::new(feed);
        let mut gen = NoteIdGen::new();

        let ba = (pseudo_rand(&mut seed) % 100) as u32;
        let bb = (pseudo_rand(&mut seed) % 100000) as u32;
        if ba > 0 { book.add_protocol_balance(token_a, ba); }
        if bb > 0 { book.add_protocol_balance(token_b, bb); }

        for _ in 0..10 {
            let s = 1 + (pseudo_rand(&mut seed) % 50) as u32;
            let rp = 60 + (pseudo_rand(&mut seed) % 40) as u32;
            let b = s * 2000 * rp / 100;
            if b > 0 { book.add_user_order(gen.next(), token_a, token_b, s, b); }

            let sb = 2000 + (pseudo_rand(&mut seed) % 50000) as u32;
            let rpb = 60 + (pseudo_rand(&mut seed) % 40) as u32;
            let bb = sb * rpb / (2000 * 100);
            if bb > 0 { book.add_user_order(gen.next(), token_b, token_a, sb, bb); }
        }

        let mut engine = MatchingEngine::new(book);
        let _batch = engine.run(); // should not panic
    }
    println!("fuzz_no_panic: 50 trials passed");
}

#[test]
fn fuzz_realistic() {
    let tokens = [eth(), usdc(), sol(), btc(), matic()];
    let prices: [u64; 5] = [2000, 1, 150, 60000, 35];
    let mut seed: u64 = 20260327;

    for trial in 0..100 {
        let mut feed = SimpleMapFeed::new();
        for i in 0..5 { feed.set_price_cents(tokens[i], prices[i]); }

        let mut book = OrderBook::new(feed.clone());
        let mut gen = NoteIdGen::new();

        let n = 30 + (pseudo_rand(&mut seed) % 50) as usize;
        for _ in 0..n {
            let si = (pseudo_rand(&mut seed) % 5) as usize;
            let mut bi = (pseudo_rand(&mut seed) % 5) as usize;
            if bi == si { bi = (si + 1) % 5; }

            let size_class = pseudo_rand(&mut seed) % 100;
            let offered = if size_class < 60 {
                10 + (pseudo_rand(&mut seed) % 190) as u32
            } else if size_class < 90 {
                200 + (pseudo_rand(&mut seed) % 1800) as u32
            } else {
                2000 + (pseudo_rand(&mut seed) % 8000) as u32
            };

            let rate_pct = 70 + (pseudo_rand(&mut seed) % 30) as u64;
            let requested = (offered as u128 * prices[si] as u128 * rate_pct as u128
                / (prices[bi] as u128 * 100)) as u32;
            if requested == 0 { continue; }
            book.add_user_order(gen.next(), tokens[si], tokens[bi], offered, requested);
        }

        if pseudo_rand(&mut seed) % 3 == 0 {
            let bt = tokens[(pseudo_rand(&mut seed) % 5) as usize];
            let ba = 50 + (pseudo_rand(&mut seed) % 500) as u32;
            book.add_protocol_balance(bt, ba);
        }

        let mut engine = MatchingEngine::new(book);
        let batch = engine.run();

        for oid in &batch.filled_orders {
            let order = &engine.book.orders[oid];
            assert!(order.requested_filled() <= order.requested,
                "trial {}: fill exceeds order", trial);
        }
        assert!(batch.remaining_orders <= engine.book.orders.len() as u32);
    }
    println!("fuzz_realistic: 100 trials passed");
}

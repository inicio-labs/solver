use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::HashSet;

/// Collect all token pairs where orders exist in both directions.
fn collect_matchable_pairs<F: PriceFeed>(book: &OrderBook<F>) -> Vec<(TokenId, TokenId)> {
    let mut pairs = HashSet::new();
    let all_tokens: Vec<TokenId> = book.tokens.iter().copied().collect();

    for &token_a in &all_tokens {
        for &token_b in &all_tokens {
            if token_a >= token_b {
                continue;
            }
            let has_ab = book.has_orders(token_a, token_b);
            let has_ba = book.has_orders(token_b, token_a);

            if has_ab && has_ba {
                pairs.insert((token_a, token_b));
            }
        }
    }

    pairs.into_iter().collect()
}

/// Direct matching: greedy on BTreeMap.
/// Inserts filled order IDs into the provided set. Returns number of cycles executed.
pub fn run_direct_matching<F: PriceFeed>(book: &mut OrderBook<F>, filled_orders: &mut HashSet<OrderId>) -> u32 {
    let mut cycles_executed = 0u32;

    let pairs = collect_matchable_pairs(book);
    for (token_a, token_b) in pairs {
        cycles_executed += match_user_orders(book, token_a, token_b, filled_orders);
    }

    cycles_executed
}

/// User-to-user matching for a pair. Returns number of cycles executed.
fn match_user_orders<F: PriceFeed>(
    book: &mut OrderBook<F>,
    token_a: TokenId,
    token_b: TokenId,
    filled_orders: &mut HashSet<OrderId>,
) -> u32 {
    let mut cycles = 0;

    loop {
        // order_a: offers token_a, requests token_b
        let order_a_id = match book.best_order(token_a, token_b) {
            Some(order) => order.id,
            None => break,
        };

        // order_b: offers token_b, requests token_a
        let order_b_id = match book.best_order(token_b, token_a) {
            Some(order) => order.id,
            None => break,
        };

        // Match — clone to avoid double mutable borrow
        let mut order_a = book.orders[&order_a_id].clone();
        let mut order_b = book.orders[&order_b_id].clone();

        let match_result = match order_a.match_with(&mut order_b) {
            Some(r) => r,
            None => break,
        };

        // Write back
        book.orders.insert(order_a_id, order_a);
        book.orders.insert(order_b_id, order_b);

        // Track filled orders
        filled_orders.insert(order_a_id);
        filled_orders.insert(order_b_id);

        // Add surplus to protocol balance
        book.add_protocol_balance(token_a, match_result.surplus_offered);
        book.add_protocol_balance(token_b, match_result.surplus_requested);

        book.cleanup_if_filled(order_a_id);
        book.cleanup_if_filled(order_b_id);

        cycles += 1;
    }

    cycles
}

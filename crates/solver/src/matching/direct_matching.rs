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
pub fn run_direct_matching<F: PriceFeed>(book: &mut OrderBook<F>, filled_orders: &mut HashSet<OrderId>) -> u64 {
    let mut cycles_executed = 0u64;

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
) -> u64 {
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

        // Audit C2: the direct path must consult the price feed, exactly
        // like the triangular path does. Triangular's gate is "every leg's
        // token is priced, else skip the cycle" (price_feed.rs doctrine: a
        // missing price means "not matchable", never a bogus default); its
        // USD value-safety is the cycle surplus ≥ 0. The direct analogue:
        // require BOTH tokens of this pair to be priced here, and rely on
        // `apply_match`/`match_with`'s per-token surplus logic for the
        // value-safety of the executed amounts — that non-loss invariant is
        // exactly what the 10k-trial settlement-solvency test proves, and is
        // the direct counterpart of triangular's surplus ≥ 0. (Gating each
        // order on `is_order_profitable` instead is wrong: it rejects normal
        // spread orders where one side asks more USD than it offers.)
        if book.feed.price_cents(token_a).is_none()
            || book.feed.price_cents(token_b).is_none()
        {
            break;
        }

        // Encapsulated mutation: surplus and cleanup happen inside.
        if book.apply_match(order_a_id, order_b_id).is_none() {
            break;
        }

        filled_orders.insert(order_a_id);
        filled_orders.insert(order_b_id);

        cycles += 1;
    }

    cycles
}

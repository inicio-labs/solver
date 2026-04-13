use crate::matching::direct_matching;
use crate::matching::three_edge_cycle;
use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::HashSet;

pub struct MatchingEngine<F: PriceFeed> {
    pub book: OrderBook<F>,
}

impl<F: PriceFeed> MatchingEngine<F> {
    pub fn new(book: OrderBook<F>) -> Self {
        Self { book }
    }

    pub fn run(&mut self) -> SettlementBatch {
        let mut filled_orders = HashSet::new();
        let mut cycles_executed = 0u32;

        // Phase 1: Direct (pairwise) matching
        cycles_executed += direct_matching::run_direct_matching(&mut self.book, &mut filled_orders);

        // Phase 2: Three-edge cycle (triangular) matching on remaining orders
        cycles_executed += three_edge_cycle::run_three_edge_cycle(&mut self.book, &mut filled_orders);

        SettlementBatch {
            filled_orders,
            cycles_executed,
            remaining_orders: self.book.active_order_count(),
            protocol_balances: self
                .book
                .protocol_balances
                .iter()
                .filter(|(_, &amt)| amt > 0)
                .map(|(&t, &a)| (t, a))
                .collect(),
        }
    }
}

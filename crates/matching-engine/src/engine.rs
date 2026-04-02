use crate::direct_matching;
use crate::three_edge_cycle;
use crate::order_book::OrderBook;
use crate::price_feed::PriceFeed;
use crate::types::*;

pub struct MatchingEngine<F: PriceFeed> {
    pub book: OrderBook<F>,
}

impl<F: PriceFeed> MatchingEngine<F> {
    pub fn new(book: OrderBook<F>) -> Self {
        Self { book }
    }

    pub fn run(&mut self) -> SettlementBatch {
        // Phase 1: Direct (pairwise) matching
        let (mut filled_orders, mut cycles_executed) =
            direct_matching::run_direct_matching(&mut self.book);

        // Phase 2: Three-edge cycle (triangular) matching on remaining orders
        let (filled_phase2, cycles_phase2) =
            three_edge_cycle::run_three_edge_cycle(&mut self.book);

        filled_orders.extend(filled_phase2);
        cycles_executed += cycles_phase2;

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

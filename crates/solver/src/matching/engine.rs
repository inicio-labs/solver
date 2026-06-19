use crate::matching::direct_matching;
use crate::matching::three_edge_cycle;
use crate::matching::order_book::OrderBook;
use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::HashSet;

pub struct MatchingEngine<F: PriceFeed> {
    pub book: OrderBook<F>,
    /// Whether the triangular (3-cycle) matching phase runs on each tick.
    /// Direct (pairwise) matching always runs. Default `true`; toggle off
    /// via `with_triangular_enabled(false)` if you want to disable the
    /// O(T³) enumeration (e.g. with a large token set or for debugging).
    pub triangular_enabled: bool,
}

impl<F: PriceFeed> MatchingEngine<F> {
    pub fn new(book: OrderBook<F>) -> Self {
        Self { book, triangular_enabled: true }
    }

    /// Builder: set whether the 3-edge cycle phase runs.
    pub fn with_triangular_enabled(mut self, enabled: bool) -> Self {
        self.triangular_enabled = enabled;
        self
    }

    pub fn run(&mut self) -> SettlementBatch {
        let mut filled_orders = HashSet::new();
        let mut cycles_executed = 0u64;

        // Phase 1: Direct (pairwise) matching
        cycles_executed += direct_matching::run_direct_matching(&mut self.book, &mut filled_orders);

        // Phase 2: Three-edge cycle (triangular) matching on remaining orders.
        // Gated by `triangular_enabled` so operators can disable the heavier
        // O(T³) phase when only direct matching is desired.
        if self.triangular_enabled {
            cycles_executed += three_edge_cycle::run_three_edge_cycle(&mut self.book, &mut filled_orders);
        }

        SettlementBatch {
            filled_orders,
            cycles_executed,
            remaining_orders: self.book.active_order_count() as u64,
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

use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct OrderBook<F: PriceFeed> {
    pub orders: HashMap<OrderId, Order>,
    /// (offered_token, requested_token) → BTreeMap sorted by rate ascending (best first).
    pub pair_index: HashMap<(TokenId, TokenId), BTreeMap<RateKey, Vec<OrderId>>>,
    /// outgoing[A] = {B | order offering A requesting B exists}
    user_adjacency: HashMap<TokenId, HashSet<TokenId>>,
    /// incoming[B] = {A | order offering A requesting B exists}
    incoming_adjacency: HashMap<TokenId, HashSet<TokenId>>,
    /// Number of active orders per pair — avoids iteration in has_orders.
    active_pair_count: HashMap<(TokenId, TokenId), u32>,
    pub protocol_balances: HashMap<TokenId, Amount>,
    pub tokens: HashSet<TokenId>,
    pub feed: F,
}

impl<F: PriceFeed> OrderBook<F> {
    pub fn new(feed: F) -> Self {
        Self {
            orders: HashMap::new(),
            pair_index: HashMap::new(),
            user_adjacency: HashMap::new(),
            incoming_adjacency: HashMap::new(),
            active_pair_count: HashMap::new(),
            protocol_balances: HashMap::new(),
            tokens: HashSet::new(),
            feed,
        }
    }

    /// Add a user order to the book. All valid orders are accepted;
    /// profitability decisions are left to the matching engine.
    pub fn add_user_order(
        &mut self,
        note_id: OrderId,
        offered_token: TokenId,
        requested_token: TokenId,
        offered: Amount,
        requested: Amount,
    ) -> bool {
        if offered == 0 || requested == 0 {
            return false;
        }

        let order = Order {
            id: note_id,
            offered_token,
            requested_token,
            offered,
            requested,
            requested_remaining: requested,
        };
        let key = order.rate_key();
        self.orders.insert(note_id, order);

        self.pair_index
            .entry((offered_token, requested_token))
            .or_default()
            .entry(key)
            .or_default()
            .push(note_id);

        *self
            .active_pair_count
            .entry((offered_token, requested_token))
            .or_default() += 1;

        self.user_adjacency
            .entry(offered_token)
            .or_default()
            .insert(requested_token);

        self.incoming_adjacency
            .entry(requested_token)
            .or_default()
            .insert(offered_token);

        self.tokens.insert(offered_token);
        self.tokens.insert(requested_token);

        true
    }

    // === Protocol Balance ===

    pub fn add_protocol_balance(&mut self, token: TokenId, amount: Amount) {
        if amount == 0 {
            return;
        }
        *self.protocol_balances.entry(token).or_default() += amount;
        self.tokens.insert(token);
    }

    pub fn deduct_protocol_balance(&mut self, token: TokenId, amount: Amount) {
        let balance = self
            .protocol_balances
            .get_mut(&token)
            .expect("no protocol balance for token");
        assert!(*balance >= amount, "protocol balance underflow");
        *balance -= amount;
    }

    /// Get the best (cheapest) active order for a pair — front of BTreeMap.
    pub fn best_order(&mut self, offered: TokenId, requested: TokenId) -> Option<&Order> {
        let btree = self.pair_index.get_mut(&(offered, requested))?;
        let orders = &self.orders;

        while let Some((&key, ids)) = btree.iter_mut().next() {
            while ids.last().map_or(false, |id| {
                orders.get(id).map_or(true, |o| !o.is_active())
            }) {
                ids.pop();
            }
            if ids.is_empty() {
                btree.remove(&key);
                continue;
            }
            let best_id = ids.last().unwrap();
            return orders.get(best_id);
        }

        None
    }

    /// Check if any active orders exist for a pair — O(1) counter lookup.
    pub fn has_orders(&self, offered: TokenId, requested: TokenId) -> bool {
        self.active_pair_count
            .get(&(offered, requested))
            .map_or(false, |&c| c > 0)
    }

    /// Outgoing: tokens that `offered` has orders requesting.
    pub fn neighbors(&self, offered: TokenId) -> Vec<TokenId> {
        self.user_adjacency
            .get(&offered)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Incoming: tokens that have orders requesting `requested`.
    pub fn incoming_neighbors(&self, requested: TokenId) -> Vec<TokenId> {
        self.incoming_adjacency
            .get(&requested)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Clean up a fully consumed order.
    pub fn cleanup_order(&mut self, order_id: OrderId) {
        let order = match self.orders.get(&order_id) {
            Some(o) => o,
            None => return,
        };
        if order.is_active() {
            return;
        }

        let pair = (order.offered_token, order.requested_token);
        let key = order.rate_key();

        if let Some(count) = self.active_pair_count.get_mut(&pair) {
            *count = count.saturating_sub(1);
        }

        if let Some(btree) = self.pair_index.get_mut(&pair) {
            if let Some(ids) = btree.get_mut(&key) {
                if let Some(pos) = ids.iter().rposition(|&id| id == order_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    btree.remove(&key);
                }
            }
            if btree.is_empty() {
                self.pair_index.remove(&pair);
                if let Some(adj) = self.user_adjacency.get_mut(&pair.0) {
                    adj.remove(&pair.1);
                    if adj.is_empty() {
                        self.user_adjacency.remove(&pair.0);
                    }
                }
                if let Some(inc) = self.incoming_adjacency.get_mut(&pair.1) {
                    inc.remove(&pair.0);
                    if inc.is_empty() {
                        self.incoming_adjacency.remove(&pair.1);
                    }
                }
            }
        }
    }

    /// Cleanup an order if it is completely filled (removes from index).
    pub fn cleanup_if_filled(&mut self, order_id: OrderId) {
        if self.orders.get(&order_id).map_or(false, |o| o.is_completely_filled()) {
            self.cleanup_order(order_id);
        }
    }

    /// Remove an order from the book entirely — indices and HashMap.
    /// Works regardless of fill state.
    pub fn remove_order(&mut self, order_id: OrderId) {
        let order = match self.orders.get(&order_id) {
            Some(o) => o,
            None => return,
        };

        let pair = (order.offered_token, order.requested_token);
        let key = order.rate_key();

        if let Some(count) = self.active_pair_count.get_mut(&pair) {
            *count = count.saturating_sub(1);
        }

        if let Some(btree) = self.pair_index.get_mut(&pair) {
            if let Some(ids) = btree.get_mut(&key) {
                if let Some(pos) = ids.iter().rposition(|&id| id == order_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    btree.remove(&key);
                }
            }
            if btree.is_empty() {
                self.pair_index.remove(&pair);
                if let Some(adj) = self.user_adjacency.get_mut(&pair.0) {
                    adj.remove(&pair.1);
                    if adj.is_empty() {
                        self.user_adjacency.remove(&pair.0);
                    }
                }
                if let Some(inc) = self.incoming_adjacency.get_mut(&pair.1) {
                    inc.remove(&pair.0);
                    if inc.is_empty() {
                        self.incoming_adjacency.remove(&pair.1);
                    }
                }
            }
        }

        self.orders.remove(&order_id);
    }

    pub fn active_order_count(&self) -> u32 {
        self.orders.values().filter(|o| o.is_active()).count() as u32
    }
}

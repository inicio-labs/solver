use crate::matching::price_feed::PriceFeed;
use crate::matching::types::*;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

pub struct OrderBook<F: PriceFeed> {
    // Internal mutation must go through `apply_match`, `add_user_order`, `cleanup_if_filled`,
    // and `remove_order` — direct writes can violate the rate-key / active-count invariants.
    pub(crate) orders: HashMap<OrderId, Order>,
    /// (offered_token, requested_token) → BTreeMap sorted by rate ascending (best first).
    /// Per-rate orders are a FIFO deque: oldest order fills first (price-time priority).
    pub(crate) pair_index: HashMap<(TokenId, TokenId), BTreeMap<RateKey, VecDeque<OrderId>>>,
    /// outgoing[A] = {B | order offering A requesting B exists}
    user_adjacency: HashMap<TokenId, HashSet<TokenId>>,
    /// incoming[B] = {A | order offering A requesting B exists}
    incoming_adjacency: HashMap<TokenId, HashSet<TokenId>>,
    /// Number of active orders per pair — avoids iteration in has_orders.
    /// Parked (externally-routed) notes are NOT counted here: they are pulled
    /// out of the rate index on park and put back on unpark, so this counter
    /// always reflects exactly the notes the matching engine can see.
    active_pair_count: HashMap<(TokenId, TokenId), u32>,
    /// Notes handed to an external DEX: removed from the rate index (invisible
    /// to matching) but their `Order` struct is kept in `orders`. Maps the
    /// note id → (DEX it was offered to, unix-millis it was parked at).
    parked: HashMap<OrderId, (DexId, u64)>,
    /// Time-ordered queue of parks, for O(expiring) TTL reactivation. Parking
    /// happens in tick-time order so `parked_at` is monotonically non-decreasing
    /// and the queue stays sorted; a consumed note leaves only a tombstone here
    /// (its id is gone from `parked`), skipped when popped.
    park_queue: VecDeque<(u64, OrderId)>,
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
            parked: HashMap::new(),
            park_queue: VecDeque::new(),
            protocol_balances: HashMap::new(),
            tokens: HashSet::new(),
            feed,
        }
    }

    // === Orders ===

    /// Add a user order to the book.
    pub fn add_user_order(
        &mut self,
        note_id: OrderId,
        offered_token: TokenId,
        requested_token: TokenId,
        offered: Amount,
        requested: Amount,
    ) {
        // Idempotent on note_id: if the order is already live in the index, do
        // nothing — avoids a double-count / duplicate FIFO entry if the same id
        // is fed twice. A currently-parked id is re-added fresh (its stale park
        // bookkeeping is cleared first); its index slot + counter were already
        // removed at park time, so this re-increments cleanly.
        if self.orders.contains_key(&note_id) && !self.parked.contains_key(&note_id) {
            return;
        }
        self.parked.remove(&note_id);

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
        self.index_insert(note_id, offered_token, requested_token, key);

        self.tokens.insert(offered_token);
        self.tokens.insert(requested_token);
    }

    /// Insert an order id into the rate index + adjacency maps + active counter.
    /// Shared by `add_user_order` (new orders) and `reindex` (re-adding the
    /// existing struct on unpark) so both go through one accounting path.
    fn index_insert(
        &mut self,
        id: OrderId,
        offered_token: TokenId,
        requested_token: TokenId,
        key: RateKey,
    ) {
        self.pair_index
            .entry((offered_token, requested_token))
            .or_default()
            .entry(key)
            .or_default()
            .push_back(id);

        *self.active_pair_count.entry((offered_token, requested_token)).or_default() += 1;

        self.user_adjacency.entry(offered_token).or_default().insert(requested_token);
        self.incoming_adjacency.entry(requested_token).or_default().insert(offered_token);
    }

    /// Remove a fully filled order from the index. No-op if still active.
    /// The order remains in `orders` so callers can inspect its final fill state.
    pub fn cleanup_if_filled(&mut self, order_id: OrderId) {
        let (pair, key) = if let Some(order) = self.orders.get(&order_id) {
            if !order.is_completely_filled() { return; }
            ((order.offered_token, order.requested_token), order.rate_key())
        } else {
            return;
        };

        if let Some(count) = self.active_pair_count.get_mut(&pair) {
            *count = count.saturating_sub(1);
        }
        self.remove_from_index(pair, key, order_id);
    }

    /// Remove an order from the book entirely (e.g. for cancellation).
    /// For filled orders, prefer `cleanup_if_filled` to keep state accessible.
    pub fn remove_order(&mut self, order_id: OrderId) {
        let Some(order) = self.orders.remove(&order_id) else { return };

        // A parked note was already pulled out of the index and decremented from
        // the active counter at park time — just drop it from `orders`. Its
        // `park_queue` entry stays as a tombstone, skipped when later popped.
        if self.parked.remove(&order_id).is_some() {
            return;
        }

        let pair = (order.offered_token, order.requested_token);

        if order.is_active() {
            if let Some(count) = self.active_pair_count.get_mut(&pair) {
                *count = count.saturating_sub(1);
            }
        }

        self.remove_from_index(pair, order.rate_key(), order_id);
    }

    // === Park / unpark (external liquidity routing) ===

    /// Hand a note to an external DEX: remove it from the rate index (so the
    /// matching engine can no longer see or match it) while keeping its `Order`
    /// struct in `orders`. Mirrors the index-removal half of `remove_order`, so
    /// `active_pair_count` / `has_orders` / `best_order` stay correct with no
    /// change to them. No-op if the note is missing, already parked, or filled.
    pub fn park(&mut self, id: OrderId, dex: DexId, time: u64) {
        if self.parked.contains_key(&id) {
            return;
        }
        let Some(order) = self.orders.get(&id) else { return };
        if !order.is_active() {
            return;
        }
        let pair = (order.offered_token, order.requested_token);
        let key = order.rate_key();

        if let Some(count) = self.active_pair_count.get_mut(&pair) {
            *count = count.saturating_sub(1);
        }
        self.remove_from_index(pair, key, id);

        self.parked.insert(id, (dex, time));
        self.park_queue.push_back((time, id));
    }

    /// Immediately unpark a specific note — the **rollback** of a `park` whose
    /// handover was never delivered (e.g. a `try_send` into a full channel).
    /// Re-adds it to the rate index and drops it from `parked`; the now-stale
    /// `park_queue` entry becomes a harmless tombstone, skipped by the next
    /// `reactivate_parked_older_than`. Returns the DEX it had been parked for, or
    /// `None` if it wasn't parked.
    pub fn unpark(&mut self, id: OrderId) -> Option<DexId> {
        let (dex, _) = self.parked.remove(&id)?;
        self.reindex(id);
        Some(dex)
    }

    /// Unpark every note parked longer than `ttl` ms as of `time` (a no-show by
    /// the DEX it was handed to). Returns `(id, dex)` for each note returned to
    /// matching (the `dex` it no-showed at, for the caller's log). O(number
    /// expiring) — pops only the expired prefix of the queue.
    pub fn reactivate_parked_older_than(&mut self, ttl: u64, time: u64) -> Vec<(OrderId, DexId)> {
        let mut reactivated = Vec::new();
        while let Some(&(parked_at, id)) = self.park_queue.front() {
            // Monotonic queue: once the front isn't expired, none behind it are.
            if parked_at.saturating_add(ttl) > time {
                break;
            }
            self.park_queue.pop_front();
            // Skip tombstones (id already consumed → gone from `parked`); `unpark`
            // does the remove-from-`parked` + reindex.
            if let Some(dex) = self.unpark(id) {
                reactivated.push((id, dex));
            }
        }
        reactivated
    }

    /// Re-add a parked note's **existing** struct to the rate index — does NOT
    /// rebuild the `Order`, so any fill state is preserved (unlike
    /// `add_user_order`, which resets `requested_remaining`). The note rejoins
    /// at the back of its rate-key FIFO.
    fn reindex(&mut self, id: OrderId) {
        let Some(order) = self.orders.get(&id) else { return };
        if !order.is_active() {
            return;
        }
        let (ot, rt, key) = (order.offered_token, order.requested_token, order.rate_key());
        self.index_insert(id, ot, rt, key);
    }

    /// Number of notes currently parked (handed to a DEX, awaiting consume/TTL).
    pub fn parked_count(&self) -> usize {
        self.parked.len()
    }

    /// Is this note currently parked?
    pub fn is_parked(&self, id: OrderId) -> bool {
        self.parked.contains_key(&id)
    }

    /// Get the best (cheapest) active order for a pair.
    /// At a given rate, returns the *oldest* order (FIFO / price-time priority).
    pub fn best_order(&mut self, offered: TokenId, requested: TokenId) -> Option<&Order> {
        let btree = self.pair_index.get_mut(&(offered, requested))?;
        let orders = &self.orders;

        while let Some((&key, ids)) = btree.iter_mut().next() {
            // Lazily drop inactive entries from the front of the FIFO queue.
            while ids.front().map_or(false, |id| {
                orders.get(id).map_or(true, |o| !o.is_active())
            }) {
                ids.pop_front();
            }
            if ids.is_empty() {
                btree.remove(&key);
                continue;
            }
            let best_id = ids.front().unwrap();
            return orders.get(best_id);
        }

        None
    }

    /// Active orders for a pair in **rate order** (best/cheapest first), as they
    /// sit in `pair_index` (ascending `RateKey`, FIFO within a rate). Parked notes
    /// aren't in the index, so they're excluded; the index may still hold lazily
    /// un-pruned inactive ids, so we filter `is_active`. Feeds `select_notes`
    /// pre-sorted for the external pass.
    pub fn notes_for_pair(&self, offered: TokenId, requested: TokenId) -> Vec<Order> {
        let Some(btree) = self.pair_index.get(&(offered, requested)) else {
            return Vec::new();
        };
        btree
            .values()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.orders.get(id))
            .filter(|o| o.is_active())
            .cloned()
            .collect()
    }

    /// Check if any active orders exist for a pair — O(1).
    pub fn has_orders(&self, offered: TokenId, requested: TokenId) -> bool {
        self.active_pair_count
            .get(&(offered, requested))
            .map_or(false, |&c| c > 0)
    }

    /// Tokens that have orders offering `offered` (outgoing neighbors).
    pub fn neighbors(&self, offered: TokenId) -> Vec<TokenId> {
        self.user_adjacency
            .get(&offered)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Tokens that have orders requesting `requested` (incoming neighbors).
    pub fn incoming_neighbors(&self, requested: TokenId) -> Vec<TokenId> {
        self.incoming_adjacency
            .get(&requested)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn active_order_count(&self) -> u32 {
        self.active_pair_count.values().sum()
    }

    // === Matching ===

    /// Apply a match between two orders by id. This is the single production
    /// path that mutates orders after insertion — `direct_matching` calls it
    /// instead of cloning/inserting manually. Surplus from the trade is
    /// credited to protocol balances; fully filled orders are cleaned out of
    /// the rate index automatically. Returns `None` if either order is
    /// missing, equal to the other, or the trade isn't profitable.
    pub fn apply_match(&mut self, a_id: OrderId, b_id: OrderId) -> Option<MatchResult> {
        if a_id == b_id {
            return None;
        }
        let mut order_a = self.orders.get(&a_id)?.clone();
        let mut order_b = self.orders.get(&b_id)?.clone();
        let result = order_a.match_with(&mut order_b)?;

        let (token_a, token_b) = (order_a.offered_token, order_b.offered_token);
        let (surplus_a, surplus_b) = (result.surplus_offered, result.surplus_requested);

        self.orders.insert(a_id, order_a);
        self.orders.insert(b_id, order_b);

        self.add_protocol_balance(token_a, surplus_a);
        self.add_protocol_balance(token_b, surplus_b);

        self.cleanup_if_filled(a_id);
        self.cleanup_if_filled(b_id);

        Some(result)
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
        if let Some(balance) = self.protocol_balances.get_mut(&token) {
            *balance = balance.saturating_sub(amount);
        }
    }

    // === Private Helpers ===

    /// Remove an order from pair_index and adjacency maps.
    /// Cleans up empty entries to keep maps lean.
    /// Uses order-preserving `remove(pos)` so FIFO ordering is intact after cancellations.
    fn remove_from_index(&mut self, pair: (TokenId, TokenId), key: RateKey, order_id: OrderId) {
        let Some(btree) = self.pair_index.get_mut(&pair) else { return };

        if let Some(ids) = btree.get_mut(&key) {
            if let Some(pos) = ids.iter().position(|&id| id == order_id) {
                ids.remove(pos);
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

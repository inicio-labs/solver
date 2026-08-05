use crate::matching::order_book::OrderBook;
use crate::price::WatchPriceFeed;
use super::{eth, usdc, NoteIdGen};

fn make_feed() -> WatchPriceFeed {
    let mut feed = WatchPriceFeed::new();
    feed.set_price_cents(eth(), 2000);
    feed.set_price_cents(usdc(), 1);
    feed
}

#[test]
fn add_order_at_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn add_order_below_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 3000 USDC, request 1 ETH -- offering more than oracle -> accepted
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1);
}

#[test]
fn accepts_order_above_oracle() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    // Offer 1900 USDC, request 1 ETH -- order book accepts all valid orders;
    // profitability filtering is the matching engine's responsibility.
    book.add_user_order(gen.next(), usdc(), eth(), 1900, 1);
}

#[test]
fn best_order_returns_cheapest() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1); // rate 1/3000 -- cheaper
    book.add_user_order(gen.next(), usdc(), eth(), 2500, 1); // rate 1/2500

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(best.offered, 3000, "most generous order should be 3000:1");
}

#[test]
fn notes_for_pair_rate_ordered_and_excludes_parked() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let a = gen.next();
    let b = gen.next();
    let c = gen.next();
    book.add_user_order(a, usdc(), eth(), 2000, 1); // rate 1/2000 (worst)
    book.add_user_order(b, usdc(), eth(), 3000, 1); // rate 1/3000 (best)
    book.add_user_order(c, usdc(), eth(), 2500, 1); // rate 1/2500 (mid)

    // Ascending requested/offered — best (most generous) first, as select_notes wants.
    let ids: Vec<_> = book.notes_for_pair(usdc(), eth()).iter().map(|o| o.id).collect();
    assert_eq!(ids, vec![b, c, a]);

    // A parked note is out of the index, so it drops from the list.
    book.park(b, 7, 100);
    let ids: Vec<_> = book.notes_for_pair(usdc(), eth()).iter().map(|o| o.id).collect();
    assert_eq!(ids, vec![c, a], "parked note excluded");
}

#[test]
fn best_order_returns_best_rate() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1600); // rate 1600
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1800); // rate 1800
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1500); // rate 1500 -- best (lowest rate = most generous)

    let best = book.best_order(eth(), usdc()).unwrap();
    assert_eq!(best.requested, 1500, "best order should have lowest rate (1500)");
}

#[test]
fn best_order_skips_inactive() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.orders.get_mut(&id).unwrap().full_fill();
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1);

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(best.offered, 3000);
}

#[test]
fn best_level_none_when_empty() {
    let book = OrderBook::new(make_feed());
    assert!(book.best_level(usdc(), eth()).is_none());
}

#[test]
fn best_level_single_order_volume() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 2000);
    assert_eq!(vol, 2000); // offered_remaining of the single order
}

#[test]
fn best_level_sums_volume_at_same_rate() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 4000, 2); // same rate (2/4000)
    let (_rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(vol, 6000, "summed offered_remaining of both orders at the rate");
}

#[test]
fn best_level_returns_best_rate_only() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(gen.next(), usdc(), eth(), 3000, 1); // rate 1/3000 — best
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 3000, "most generous level");
    assert_eq!(vol, 3000, "only the best level's volume, not the 2000 level");
}

#[test]
fn best_level_skips_inactive_front() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 3000, 1); // best rate, but filled below
    book.orders.get_mut(&id).unwrap().full_fill();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1); // next active level
    let (rate, vol) = book.best_level(usdc(), eth()).unwrap();
    assert_eq!(rate.offered, 2000, "skips the fully-filled best level");
    assert_eq!(vol, 2000);
}

#[test]
fn snapshot_best_levels_covers_each_directed_pair() {
    let mut book = OrderBook::new(make_feed());
    let mut gen = NoteIdGen::new();
    book.add_user_order(gen.next(), usdc(), eth(), 2000, 1);
    book.add_user_order(gen.next(), eth(), usdc(), 1, 1800);
    let snap = book.snapshot_best_levels();
    assert_eq!(snap.len(), 2);
    assert!(snap.contains_key(&(usdc(), eth())));
    assert!(snap.contains_key(&(eth(), usdc())));
}

#[test]
fn cleanup_removes_inactive() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.orders.get_mut(&id).unwrap().full_fill();
    book.cleanup_if_filled(id);
    assert_eq!(book.active_order_count(), 0);
}

#[test]
fn protocol_balance_add_deduct() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_protocol_balance(eth(), 5);
    assert_eq!(book.protocol_balances[&eth()], 5);
    book.deduct_protocol_balance(eth(), 2);
    assert_eq!(book.protocol_balances[&eth()], 3);
}

#[test]
fn protocol_balance_saturates_at_zero() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    book.add_protocol_balance(eth(), 1);
    book.deduct_protocol_balance(eth(), 2);
    assert_eq!(book.protocol_balances[&eth()], 0);
}

#[test]
fn fifo_at_same_rate_returns_older_order_first() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let older = gen.next();
    let newer = gen.next();
    // Two same-rate orders: same offered/requested, inserted in order.
    book.add_user_order(older, usdc(), eth(), 2000, 1);
    book.add_user_order(newer, usdc(), eth(), 2000, 1);

    let best = book.best_order(usdc(), eth()).unwrap();
    assert_eq!(
        best.id, older,
        "FIFO: oldest order at the same rate should fill first (price-time priority)"
    );
}

// === Park / unpark (external liquidity routing) ===

const DEX_A: crate::matching::types::DexId = 1;
const DEX_B: crate::matching::types::DexId = 2;

#[test]
fn park_removes_from_index_but_keeps_struct() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    assert_eq!(book.active_order_count(), 1);

    book.park(id, DEX_A, 1_000);
    // Invisible to matching...
    assert_eq!(book.active_order_count(), 0, "parked note is not counted");
    assert!(book.best_order(usdc(), eth()).is_none(), "parked note not in the index");
    // ...but its struct is retained and it is marked parked.
    assert!(book.is_parked(id));
    assert_eq!(book.parked_count(), 1);
    assert!(book.orders.contains_key(&id), "Order struct kept in `orders`");
}

#[test]
fn unpark_on_ttl_restores_to_index() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.park(id, DEX_A, 1_000);

    // Not yet expired (ttl 100, now 1050).
    assert!(book.reactivate_parked_older_than(100, 1_050).is_empty());
    assert_eq!(book.active_order_count(), 0);

    // Expired (now 1101 >= 1000 + 100).
    let woke = book.reactivate_parked_older_than(100, 1_101);
    assert_eq!(woke, vec![(id, DEX_A)], "returns (id, dex) of the no-show");
    assert_eq!(book.active_order_count(), 1, "back in the index");
    assert!(!book.is_parked(id));
    assert!(book.best_order(usdc(), eth()).is_some());
}

#[test]
fn unpark_immediate_rollback_restores_and_leaves_tombstone() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.park(id, DEX_A, 1_000);
    assert_eq!(book.active_order_count(), 0);

    // Immediate rollback (a handover that was never delivered): returns the DEX
    // and restores the note to the index now — no TTL wait, no penalty.
    assert_eq!(book.unpark(id), Some(DEX_A));
    assert!(!book.is_parked(id));
    assert_eq!(book.active_order_count(), 1, "back in the index immediately");
    assert!(book.best_order(usdc(), eth()).is_some());

    // The stale park_queue entry is a tombstone: a later TTL sweep skips it, so
    // the unparked note is not re-woken.
    assert!(book.reactivate_parked_older_than(1, 9_999).is_empty(), "tombstone skipped");
    assert_eq!(book.active_order_count(), 1, "still exactly one active note");

    // Unparking a note that isn't parked is a no-op.
    assert_eq!(book.unpark(id), None);
}

#[test]
fn consume_of_parked_note_no_double_decrement() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.park(id, DEX_A, 1_000); // active_pair_count 1 -> 0
    // DEX consumed it on-chain -> consumed_rx -> remove_order.
    book.remove_order(id);
    assert_eq!(book.active_order_count(), 0, "no underflow / double-decrement");
    assert_eq!(book.parked_count(), 0);
    assert!(!book.orders.contains_key(&id));
    // The stale park_queue entry is a tombstone: reactivation must skip it cleanly.
    assert!(book.reactivate_parked_older_than(100, 5_000).is_empty(), "consumed note not resurrected");
}

#[test]
fn active_count_invariant_across_park_unpark_consume() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let (a, b, c) = (gen.next(), gen.next(), gen.next());
    book.add_user_order(a, usdc(), eth(), 2000, 1);
    book.add_user_order(b, usdc(), eth(), 3000, 1);
    book.add_user_order(c, usdc(), eth(), 2500, 1);
    assert_eq!(book.active_order_count(), 3);

    book.park(a, DEX_A, 10);
    book.park(b, DEX_B, 20);
    assert_eq!(book.active_order_count(), 1, "only c left in the index");

    // b is consumed on-chain; a will no-show.
    book.remove_order(b);
    assert_eq!(book.active_order_count(), 1);
    assert_eq!(book.parked_count(), 1, "only a still parked");

    let woke = book.reactivate_parked_older_than(5, 100);
    assert_eq!(woke, vec![(a, DEX_A)], "a reactivates; b's queue entry is a skipped tombstone");
    assert_eq!(book.active_order_count(), 2, "a + c");
    assert_eq!(book.parked_count(), 0);
    assert!(!book.orders.contains_key(&b));
}

#[test]
fn unpark_preserves_partial_fill_state() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, eth(), usdc(), 1, 100); // offer 1 eth, request 100 usdc
    book.orders.get_mut(&id).unwrap().fill(40); // partial fill -> 60 remaining
    assert_eq!(book.orders.get(&id).unwrap().requested_remaining, 60);

    book.park(id, DEX_A, 1_000);
    book.reactivate_parked_older_than(1, 2_000);
    // unpark re-indexes the EXISTING struct; it must NOT rebuild (which resets to 100).
    assert_eq!(
        book.orders.get(&id).unwrap().requested_remaining, 60,
        "fill state preserved across park/unpark"
    );
}

#[test]
fn park_is_idempotent_and_keeps_first_dex() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.park(id, DEX_A, 1_000);
    book.park(id, DEX_B, 2_000); // second park ignored
    assert_eq!(book.parked_count(), 1);
    assert_eq!(book.active_order_count(), 0, "only one decrement");
    let woke = book.reactivate_parked_older_than(1, 5_000);
    assert_eq!(woke, vec![(id, DEX_A)], "keeps the first DEX it was offered to");
}

#[test]
fn add_user_order_idempotent_on_duplicate_id() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.add_user_order(id, usdc(), eth(), 2000, 1); // duplicate id
    assert_eq!(book.active_order_count(), 1, "no double count on duplicate add");
}

#[test]
fn apply_match_same_id_is_none() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    assert!(book.apply_match(id, id).is_none(), "cannot match an order with itself");
}

#[test]
fn removing_last_order_clears_adjacency() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    assert_eq!(book.neighbors(usdc()), vec![eth()]);
    assert_eq!(book.incoming_neighbors(eth()), vec![usdc()]);
    book.remove_order(id);
    assert!(book.neighbors(usdc()).is_empty(), "pair pruned from adjacency on last remove");
    assert!(book.incoming_neighbors(eth()).is_empty());
}

#[test]
fn cleanup_if_filled_is_noop_on_active_or_missing() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let id = gen.next();
    book.add_user_order(id, usdc(), eth(), 2000, 1);
    book.cleanup_if_filled(id); // still active → no-op
    assert_eq!(book.active_order_count(), 1);
    book.cleanup_if_filled(gen.next()); // unknown id → no-op
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn remove_one_of_two_rates_keeps_the_pair() {
    let feed = make_feed();
    let mut book = OrderBook::new(feed);
    let mut gen = NoteIdGen::new();
    let a = gen.next();
    let b = gen.next();
    book.add_user_order(a, usdc(), eth(), 2000, 1); // rate 1/2000
    book.add_user_order(b, usdc(), eth(), 3000, 1); // rate 1/3000 (different key)
    book.remove_order(a); // a's rate-key empties, but the pair keeps b
    assert_eq!(book.active_order_count(), 1);
    assert!(book.best_order(usdc(), eth()).is_some());
    assert_eq!(book.neighbors(usdc()), vec![eth()], "pair retained while b remains");
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]
    /// Random sequences of add / park / reactivate / remove must never panic and
    /// must keep `active_order_count()` exactly equal to the set of orders that
    /// are active AND not parked — the one invariant matching relies on.
    #[test]
    fn prop_park_unpark_active_count_invariant(
        ops in prop::collection::vec((0u8..5u8, 0u8..20u8), 0..60),
    ) {
        let mut book = OrderBook::new(make_feed());
        let mut gen = NoteIdGen::new();
        let mut ids = Vec::new();
        let mut clock = 0u64;
        for (op, idx) in ops {
            clock += 1;
            match op {
                0 => {
                    let id = gen.next();
                    book.add_user_order(id, usdc(), eth(), 2000, 1);
                    ids.push(id);
                }
                1 => { if !ids.is_empty() { book.park(ids[idx as usize % ids.len()], 1, clock); } }
                2 => { let _ = book.reactivate_parked_older_than(0, clock + 1); }
                3 => { if !ids.is_empty() { book.remove_order(ids[idx as usize % ids.len()]); } }
                _ => { if !ids.is_empty() { book.park(ids[idx as usize % ids.len()], 2, clock); } }
            }
            let active: Vec<_> = book
                .orders
                .iter()
                .filter(|(_, o)| o.is_active())
                .map(|(id, _)| *id)
                .collect();
            let expected = active.iter().filter(|id| !book.is_parked(**id)).count() as u32;
            prop_assert_eq!(book.active_order_count(), expected);
        }
    }
}

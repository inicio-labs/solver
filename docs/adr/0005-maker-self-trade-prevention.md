# 5. Self-trade prevention for maker orders

- **Status:** Accepted, implemented. Decided with Vaibhav Jindal on 2026-10-23 to 2026-10-25. D1 to D3 are built with their proposed defaults; D6 is Vaibhav's to send.
- **Date:** 2026-10-25
- **Deciders:** Vaibhav Jindal. Maker requirement from Jordy (Pragma), 2026-10-23.
- **Scope:** PSWAP batch clearing in the 0.17 solver, maker orders only (orders with a maker ID from the gateway).
- **Related:** [ADR 0003](0003-market-maker-v1-gateway.md) (maker gateway, lineages, cutoffs, `live_orders`, reservation), [ADR 0004](0004-binance-bookticker-prices.md) (the batch price), [market maker guide](../market-maker-guide.md).

## Context

Each round clears every configured pair as one batch at a single price: the Binance midpoint `P` for that pair. An order is eligible when its limit beats `P` by at least the protocol fee:

```text
sell eligible:  limit ≤ P × (1 − fee)
buy  eligible:  limit ≥ P × (1 + fee)
```

All eligible sells and buys of the pair are pooled and filled at `P`. Nothing compares the owners of the orders. When one maker has an eligible buy **and** an eligible sell in the same batch, its sell can fill its own buy: it pays the fee twice, its inventory does not move, and the round reports volume that no one traded.

Both orders can only be eligible together when the maker's bid is above its ask by more than the fee band. A normal two-sided quote, bid below ask, can never trigger this. Makers cross themselves by accident: a new quote reaches the solver before the cancel of the old one, or two strategies of one firm trade against each other.

The maker's requirement (Pragma, 2026-10-23):

1. Same direction: no action. Both orders keep their normal price and time priority.
2. Opposite direction, and both would match each other in the batch: cancel the older order and send an `OrderStatus` event for it. The newer order stays and trades with other participants as usual.
3. Their normal, non-crossing bid and ask are never touched.

Exchanges with continuous books decide this when an incoming order meets a resting one (Binance `EXPIRE_MAKER`, Eurex "cancel passive"). Batch auctions usually apply nothing and leave abuse to surveillance; see *Alternatives considered*. Our batch has no incoming order, and a maker on both sides would hit this on every round it stays crossed, so we need our own rule.

## Decision

### Who and when

- **Maker orders only.** The rule groups orders by maker ID. Orders without a maker ID, public or otherwise, are never skipped, cancelled or even inspected.
- **Per pair, per round, over all the maker's eligible orders.** The side is decided from every order of the maker on the pair that is eligible at this round's price, not only the ones that reach the window (the top `max_orders_per_side` per side). A newer order just outside the window still states the maker's latest intent; deciding inside the window alone would trade a stale order against it (see *Verification*).
- **Age** is the maker's submission sequence of the order's root (`MakerTag.root_seq`). The maker allocates it from one counter (guide, section 6), it is unique per maker, and every remainder of an order carries the same value. A partly filled order keeps its age.

### The rule

For each maker with eligible orders on both sides of a pair:

1. **The newest order's side wins.** Let `newest_buy` and `newest_sell` be the highest `root_seq` among the maker's eligible buys and sells. If `newest_buy > newest_sell` the maker is a buyer this round, otherwise a seller.
2. **Cutoff** = the newest `root_seq` on the losing side.
3. **Kept** = the maker's eligible orders on the winning side with `root_seq > cutoff`.
4. **Sit-outs** = all its other eligible orders: the whole losing side, and winning-side orders older than the cutoff.
5. Sit-outs are skipped when the window is filled, so they take no slot; the next orders in line take their place.
6. Clearing runs unchanged.
7. **If any kept order receives a fill in the reserved settlement, every sit-out of that maker on the pair is cancelled**: stopped for good, like `CancelOrder`, including its future remainders, with an `OrderStatus CANCELLED` event (reason `self_trade_prevention`) per order.
8. **If no kept order receives a fill, nothing is cancelled.** The sit-outs are evaluated again next round.

Steps 1 to 4 give exactly the result of replaying the maker's orders in submission order with "each opposite-side order cancels the older live ones": the replay leaves the orders after the last change of side, and the last change of side is the newest order on the losing side. Two maximums replace the replay, with no sorting.

### Examples

`B` is a maker buy, `S` a sell, numbered by `root_seq`; `→` means submitted later. All are eligible this round unless noted.

| Maker orders | Into matching | Kept order filled | Kept order not filled |
|---|---|---|---|
| B1, B2 | B1, B2 | no action | no action |
| B1 → S1 | S1 | cancel B1 | nothing; B1 sat out |
| S1 → B1 | B1 | cancel S1 | nothing |
| B1 → S1 → B2 | B2 | cancel B1 and S1 | nothing |
| B1, B2 → S1 | S1 | cancel B1 and B2 | nothing |
| B1 → S1, S2 | S1, S2 | cancel B1 | nothing |
| B1 → S1 → B2 → S2 | S2 | cancel B1, S1, B2 | nothing |
| B1 eligible, S1 not eligible | B1 | no conflict | no conflict |
| Normal spread (bid below ask) | as today | never touched | never touched |
| B1 → S1 → B2, B2 ranks just outside the buy window | B2 if it reaches the window; never S1 or B1 | cancel B1 and S1 | nothing |

Two makers are independent: maker M's buy may trade with maker N's sell. One maker using several accounts is one owner, because grouping is by maker ID.

### Algorithm

Per pair, inside the existing `ClearingBook::build_pair_batch`, in the existing admission walk (best price first, stop at `max_orders_per_side` or at the first order that is not eligible):

```text
user order                          → admit
maker order, maker not decided yet  → decide that maker once for this round:
                                        read its orders on this pair from the maker index,
                                        keep the eligible ones, take newest_buy and newest_sell;
                                        both present → (winning side, cutoff); else no conflict
maker order that is not kept        → skip: a sit-out takes no slot
maker order that is kept            → admit
```

Because the decision does not depend on what reaches the window, one walk is enough: there is no refill step and nothing to check twice. A maker that appears in neither window trades nothing this round and is not decided at all.

The walk returns the batch plus, per decided maker with sit-outs, its sit-out note IDs (all its eligible orders on the pair that are not kept, inside or outside the window).

After clearing, the matcher checks each decided maker for a fill in the pair's accepted plan. For a maker with a fill, it attaches the sit-outs as pending cancels to that maker's first filled order. Once the combined batch is sent (the same `try_send` success after which it deactivates the batch's orders today), it deactivates those sit-outs too, so no later round can select them while the cancel is being written. If the send fails, nothing is deactivated, as today.

Pending cancels are not inputs: they are not consumed, proven or broadcast. They travel as book orders sharing the note the book already holds (a reference-counted pointer, no copy of note bytes), because every give-back path returns book orders. The executor never splits a pair's group across transactions, so they always travel with the settlement that fills the kept order.

**Cost.** With no maker order in the walk (the usual case for user-only pairs) nothing changes. For each maker the walk meets, the decision reads that maker's orders on this pair once per pair per round (`MakerIndex::orders_on_pair` checks each entry's tokens, so orders on other pairs cost a comparison, not a book lookup), with one eligibility check each and two maximums. A sit-out skipped by the walk costs one lookup in a small local map. After clearing, one pass over the plan's fills. There is no new index and no state kept between rounds. The cost is linear in the maker orders on the pair, crossed or not, because every order is checked for eligibility; measured in the dev build (`clearing_book/stp_scale.rs`, one tick over all pairs; rule off ≈ 0.3–0.5 ms at every size): one-sided makers on five pairs, 10 × 20 orders per pair 2.4 ms, 50 × 100 59 ms, 50 × 400 235 ms; fully crossed makers on two pairs, 50 × 100 27 ms, 50 × 1000 293 ms. Realistic books are in the first row; large ladders need the deferred index or the gateway limit below.

### Restoration

The matcher deactivates sit-outs with a pending cancel exactly as it deactivates the orders it sends, and `ExecutionBatch::book_orders()` returns them with the group's inputs. Every existing restoration path therefore covers them, with no new code path:

| Failure | Executor outcome today | Sit-outs |
|---|---|---|
| RPC failure during execution, an input consumed while proving, a database hiccup | `GiveBack` → `release_held` (drops notes spent on chain, returns the rest through `write_book` and `live_orders`) | returned with the group; spent or maker-cancelled ones are filtered the same way |
| Deterministic build, proving or solvency failure | `Failed` → pair groups retried alone; a group that still fails is held with a doubling delay, then given back | held and returned with their group |
| No fee headroom, RPC or database unavailable | `Paused` → verification mode retries the same transactions in order | stay attached; cancelled only if the retried reservation commits, skipping any no longer live |
| Malformed batch | held, then given back | held and returned with it |
| Shutdown or crash before the reservation commits | nothing written; restart hydration loads live orders | loaded as live, never stopped |
| Reservation committed, then rejected or expired | `finish_discarded_settlement_tx` releases the inputs | stay cancelled |
| Reservation committed, outcome unknown | reconciled by transaction ID until the expiry block | already cancelled |
| Crash after the reservation commits | pending settlement reloaded and reconciled | stops already durable |

### Data structures

No new index, no stored state between rounds, no database migration.

```rust
// types.rs: the kept order carries what to cancel if its settlement is reserved
pub struct FilledNote {
    // ...existing fields...
    pub cancel_on_reserve: Vec<BookOrder>,   // empty for almost every note; shares the book's Arc<Note>
}

// clearing: one per crossed maker, returned with the pair batch
pub(crate) struct MakerDecision {
    pub maker_id: MakerId,
    pub sit_out: Vec<NoteId>,
}
```

- The cancels are attached to the maker's **first** filled order in the pair, so they are written once and follow that order through every split, retry, hold and give-back. `ExecutionBatch`, `split_batch` and `pair_groups` are unchanged; `book_orders()` also returns the pending cancels' orders.
- A crossed maker's only orders in the pair's batch are its kept ones, so "a kept order filled" is checked as "the maker has a fill in this pair's plan". `MakerDecision` needs no kept list.
- During the walk, `build_pair_batch` keeps the decisions in a local `Vec<(MakerId, Option<(winning side, cutoff, sit-outs)>)>` with a linear scan (a pair has a handful of makers); an empty `Vec` does not allocate. `None` records a maker whose eligible orders are all on one side.
- No lineage, maker or depth is carried: the reservation reads them from the sit-out's `live_orders` row, the same lookup that checks it is still live, in the query that already checks the inputs.
- The executor's in-memory `PendingSettlement` gets `cancelled: Vec<NoteId>`: every pending cancel of the settlement, appended to the `removed` list of its confirmation or discard message. It does not need to know which ones the reservation actually stopped: a skipped one was already removed from the matcher by the maker's cancel or by ingest.
- Accessors only: `Order::maker_tag()`, `Order::to_book_order()`, `Order::is_eligible()`, and `MakerIndex::orders_on_pair()`, a view of the existing `entries_of_maker` filtered by the entry's tokens. `MakerIndex` itself is unchanged; it already exists for maker cancels, attribution, expiry and RFQ exclusion.
- Reused as-is: the book index, the inactive state for held orders, `MakerTag.root_seq`, `BookUpdate`, `MakerUpdate::LineageCancelled`, the `maker_stops` and `maker_events` tables, the `live_orders` view.

### Configuration

`engine.maker_self_trade_prevention` (bool, default `true`). When `false`, `build_pair_batch` skips the rule entirely and the matcher behaves as before this change. It exists so the rule can be switched off without a release if it misbehaves in production.

### Durability

A settlement passes three moments: the matcher **selects** it, the executor proves it and **reserves** its inputs in PostgreSQL before any broadcast, and the chain **confirms** it. "A kept order is matched" means the second moment: the settlement that fills a kept order is reserved.

Cancels are written by the executor in the settlement's reservation transaction: `maker_db::cancel_self_trades_tx` runs right after `prepare_settlement_tx`, in the same database transaction. `prepare_settlement_tx` already locks the control rows of every maker that owns an input; a maker with a filled kept order is one of them. In the same commit:

- a stop row is stored for each pending cancel's lineage (the `CancelOrder` mechanism), skipping any order that is no longer in `live_orders` (cancelled by the maker or spent elsewhere meanwhile);
- one `OrderStatus CANCELLED` event is appended to the maker's feed per stop row actually inserted, after the reservation's `SettlementPending` events. A stop row that already exists (the maker's own cancel got there first) inserts nothing and sends no event.

The reservation publishes nothing to the matcher, as today; it is a midpoint. The matcher learns the final outcome of every order it sent from the one message the executor already sends at the end: the give-back (`release_held`), the confirmation (`confirm_settlement_tx`) or the discard (`finish_discarded_settlement_tx`). Held sit-outs ride on the same messages:

- Failure before the reservation: they are in the give-back's `active` list (through `book_orders()`), so they come back.
- Confirmation or discard after the reservation: the executor appends their IDs to that message's `removed` list. It keeps them in its in-memory `PendingSettlement` (`cancelled: Vec<NoteId>`). After a restart that list is gone, which is harmless: startup hydration reads `live_orders`, where they are stopped, so the matcher never holds them again. Removing an ID the matcher does not hold is a no-op.

No extra publication, no change to how the reservation is written. If the reservation does not commit, nothing of this is written; a reserved settlement that later fails on chain does not undo the cancels (see *Restoration*).

An order already reserved in a settlement in flight is not in the book, so it is never a sit-out and never cancelled. A sit-out that the maker cancels itself in the meantime is stopped once: both writers insert stop rows with `ON CONFLICT DO NOTHING`, and they serialize on the maker's control-row lock. The "still live" check cannot race with ingest or the spend watcher, because those write through the same single core writer as the reservation.

### Maker-facing changes

The `OrderStatus` event, its `reason` field and the event feed already exist. What is missing is a state for "a live order of yours was stopped by the solver": `LIVE` means eligible, `REJECTED` is only sent before an order goes live, `UNAVAILABLE` means the note was spent outside the solver, and a maker's own cancels send no event at all (guide, section 8). `SettlementResolved CANCELLED` covers a remainder after a fill, not a live order. Reusing `REJECTED` or `UNAVAILABLE` would break the order state machine makers implement from the guide.

- **Proto:** add one value, `ORDER_STATE_CANCELLED = 4` in `OrderState`, "Stopped by the solver; `reason` says why". This rule sets `reason` to `self_trade_prevention`. The change is additive; a maker client must accept the new value.
- **Events:** one `OrderStatus` per cancelled order, with its current `note_id` and `depth`. A maker's own cancels still produce no event.
- **`ListActiveMakerOrders`** stops listing a cancelled order (it reads `live_orders`).
- **Funds:** a cancelled order's tokens stay in its note until the maker reclaims it (`maker reclaim`). A public note can still be filled on chain by others; a private note cannot.
- **Guide:** a new section "Self-trade prevention" with the rule, the example table, the event and the state diagram's new arrow `Live → Cancelled: OrderStatus CANCELLED`. It also tells makers to send `SubmitOrder` before committing a public note (see *Consequences and limits*).

## Consequences and limits

- A crossed maker trades on one side per round, at its normal price and time priority, so its edge is unchanged on that side.
- Accepted side effect: while the kept order does not fill, the older crossing order keeps sitting out every round, even if it could have traded with other participants, and the maker gets no event for that. It ends when a kept order fills (the sit-out is then cancelled), when the maker cancels one side, or when the price moves so that the orders no longer cross.
- A crossed maker's stale side never trades, even in a round where its newest side does not reach the window. That follows the maker's latest intent, as the requirement asks.
- A small newer order can cancel larger older ones on the other side (B1 5 BTC, B2 5 BTC → S1 0.1 BTC cancels both buys). Open decision D1.
- Orders without a maker ID are not covered: a public account can still trade with itself. Open decision D2.
- Ingest picks up every public PSWAP note directly from the chain. If a maker's public note reaches the chain before the solver has stored its `SubmitOrder`, the note enters the book with no maker ID, and the rule cannot group it with the maker's other orders until the submit links it (guide, section 7). Private notes never have this gap: the solver only learns them through the gateway. A public note submitted before it is committed is linked from the start, because the lineage claim already exists when ingest activates it through `live_orders`. The guide therefore tells makers to use private notes, or to send `SubmitOrder` before committing a public note.
- Accepted for public maker notes (decided 2026-10-25: makers are expected to send private orders): a public note that ingest put in the book before the watcher verified its claim can sit out and be held, and then be re-sent as active when the watcher verifies it (`maker_db::activate_tx` returns every Active row of the verified submissions). It can then trade in a later round, and if that round is reserved first, the cancel finds it no longer live and skips it: the maker trades both sides across two settlements, with no `CANCELLED`. Private notes cannot hit this, because the watcher stores a private order itself when it verifies it, so the matcher never holds one with a pending claim. Fix if public maker notes matter: have `activate_tx` return only the rows it stores itself.
- The depth and swap-ETA mirror still counts sit-outs, so quoted depth can be slightly high on a crossed maker's losing side.
- Crowding the window with crossed orders is no longer free: sit-outs take no slot, users fill the freed slots, and the kept side either trades and pays fees or nothing could trade anyway. Crowding with ordinary orders is the separate per-side cap question (issue #50).

## Alternatives considered

| Alternative | Why not |
|---|---|
| Allow it, watch afterwards (what NYSE-group, BOX and Eurex auctions do; Gnosis v1 and CoW do the same) | Those auctions run a few times a day. Here a two-sided maker would self-match every round and pay double fees. |
| Keep the larger side | Throws away the maker's other side even when users want it, and ignores the maker's latest intent. |
| One side per round, side chosen by volume, or "makers fill only the users' gap" | Without cancellation the maker's stale order never goes away; the gap version also puts a two-sided maker behind users on its own side, which punishes quoting both sides. |
| Net after matching | The maker's own buy usually ranks first and takes its own sell; after netting, the user next in line has already lost the fill. |
| Decide when an order arrives (exchange style) and keep a "blocked" state | Needs stored state and unblock rules. Our batch already gives a natural moment to decide, the round. |
| A per-maker sorted index kept between rounds | Extra state on every book change; reading a maker's few orders on the pair once per round is cheap enough. |
| Decide inside the window only, with a refill (and a repeat when the refill crosses another maker) | Trades stale orders against the maker's newest intent when a newer opposite order sits just outside the window, and needs the refill and repeat machinery. |
| Binary search for the eligibility boundary | `admit` already starts at the best price and stops at the first order that does not qualify; a range query would do the same work plus an exact price-to-key conversion. |

## Verification

### Formal checks of the design (done 2026-10-25)

Scripts: [`0005/check_rule.py`](0005/check_rule.py) (decision rule), [`0005/check_all_eligible.py`](0005/check_all_eligible.py) (windows), [`0005/check_single_refill.py`](0005/check_single_refill.py) (the rejected window-only variants) and [`0005/check_lifecycle.py`](0005/check_lifecycle.py) (life cycle), plain Python 3 with no dependencies; each runs in seconds. Run them from that directory.

- **Decision rule, exhaustive:** for all 131,070 side sequences of length 1 to 16, the two-maximum decision equals the literal submission-order replay; the kept set is never empty, is one-sided, contains the newest order and is exactly the final same-side run.
- **Windows, exhaustive and random:** 364,032 books (3 orders per side with two makers, 4 per side with one maker, window 1 to 3) and 200,000 random books (up to 9 per side, three makers, window 1 to 5). In every case no maker ends up on both sides, no user order is skipped, every maker's newest eligible order is kept, each window is exactly the first `W` orders not skipped, and no side is left empty while users wait. Crowding the windows with 100 crossed sells and 100 crossed buys still lets 50 users trade on each side.
- **Why not decide inside the window:** a rule that only looks at window orders, with or without a refill, would trade a maker order against the maker's newest intent in 142,008 of the exhaustive books and 52,469 of the random ones (a newer opposite order just outside the window). Without any refill it also leaves a side empty while users wait (2,028 exhaustive, 5,720 random) and lets one maker block a pair by crowding both windows.
- **Life cycle, model-checked:** every interleaving of two rounds, proof failure, a failing group held, a pause and retry, reservation commit or rollback, chain confirm or discard, a crash anywhere, maker cancels of either order, and the user's note spent elsewhere: 1,584 reachable states (and 1,578 for the variant that remembers only the cancels the reservation actually stopped, which is also correct but not needed). No state breaks: stop only after a committed reservation; one `CANCELLED` exactly when stopped; `SettlementPending` before `CANCELLED`; no `CANCELLED` after the maker's own cancel; the matcher never has a non-live order active; no settlement fills a maker on both sides; once the executor holds nothing, the matcher holds nothing and every live order is active again.
- **The checks catch real mistakes:** four broken variants each fail with a shortest trace: the final message omits the sit-outs (`round → reserve → confirm` leaves B1 held), cancel at selection (`round`), give-back returns only inputs (`round → proof fails` leaves B1 held), no live check at reservation (`round → maker cancels B1 → reserve` sends a wrong `CANCELLED`).
- These check the design, not the Rust code. The tests below check the code against the same properties.

### Tests in the implementation

- Book (`matcher/clearing_book.rs`): every row of the example table; the decision equals a literal submission-order replay for all 510 side sequences of 1 to 8 orders; two makers, orders without a maker ID (the fixtures share one creator account) and a maker whose sell is not eligible are left alone; the configuration switch; an order in flight does not cross its maker; sit-outs take no window slot, and a newest order outside the window still decides the side (without the rule, the window takes the stale orders).
- Matcher (`internal_clear`): a kept order without a counterparty sends nothing and cancels nothing; a filled kept order carries the sit-out, which is not an input and leaves the index; a give-back returns it active; the final message removes it.
- PostgreSQL (`maker_db`): a rolled-back reservation writes nothing; the reservation stops only live maker orders, sends one `CANCELLED` (reason `self_trade_prevention`) right after the kept order's `SettlementPending`, and skips an order the maker cancelled first and an order without a maker; a later maker cancel of it stops nothing twice. `reserve_settlement_tx` locks a sit-out's maker before any write: with a trigger pausing it after the stop row, the maker's own `CancelOrder` waits instead of deadlocking (without that lock the same test fails with `deadlock detected`).
- Maker index: `orders_on_pair` returns the maker's orders on the pair in both directions and nothing else.
- Cost (`clearing_book/stp_scale.rs`, ignored timing tests): see *Cost*.
- Executor: pending cancels follow their note through `split_batch` and `pair_groups` and are returned with a held group; they are never built as inputs; a discarded settlement's message removes them.
- The existing suites: 390 pure, 128 PostgreSQL library (`--skip live_binance`, as CI) and all 6 integration tests; clippy adds no warning in the changed files; the CI formatting check passes; the maker kit builds with the new proto value.

### QA outside this change (2026-10-25)

Run in scratch copies and not committed yet: a QA suite built from this ADR's text (54 tests, about 8,400 generated books); three end-to-end runs of the real solver against the mock chain, with private maker notes over gRPC (fill and `CANCELLED`, no counterparty, failure before the reservation, cancels racing it, node rejection after it, a restart between reservation and confirmation, and a gRPC stream receiving state 4), plus 13 executor-path tests; PostgreSQL races on real sessions (15 tests, at least 50 rounds each); mutation testing (51 mutants, 80% killed by the suite above, 92% with four added tests); an extended life-cycle model (12 scenarios); and a brute-force check that the decision equals the replay for all 1,398,100 side and eligibility sequences of up to 10 orders. Findings: the reservation's lock gap (fixed here by `reserve_settlement_tx`), the public-note limit and the in-flight limit (see *Consequences and limits*), and a pre-existing lock-order gap in the spend report (ADR 0006, a separate change). Not done: a verification-mode (fee pause) end-to-end run, because the mock chain charges no fee.

## Open decisions

| # | Decision | Proposed default |
|---|---|---|
| D1 | A small newer order cancels larger older opposite orders | Yes: the rule follows the maker's latest intent, whatever the size |
| D2 | Orders without a maker ID stay uncovered | Yes, for now |
| D3 | A public maker note that reaches the chain before its `SubmitOrder` is stored is unlinked for that time | Accept; the guide tells makers to use private notes or submit before committing |
| D6 | Tell Pragma before release: the decision covers all eligible orders (not only the window), cancel only when a kept order's settlement is reserved, the B1 → S1 → B2 result, the `CANCELLED` event, submit before committing public notes | Yes |

Decided: D4, "matched" = the settlement filling a kept order is reserved, with the cancels in the same commit (see *Durability*); D5, the configuration switch (see *Configuration*).

## Deferred work

- If makers multiply: group the existing `MakerIndex` by pair and side, so `decide` reads only the pair's orders and skips a maker with orders on one side in O(1); and a gateway limit on live orders per maker per pair. Today the decision walks the maker's entries on all pairs, skips other pairs by the entry's tokens (`MakerIndex::orders_on_pair`) and checks every order on the pair for eligibility; see *Cost* for measurements.

- `ReplaceOrder`: an atomic cancel-and-submit command, which would remove most crossings at the source.
- An informational event when an order sits out a round.
- Audit-trail events for sit-outs and cancels.
- Excluding sit-outs from the depth and swap-ETA mirror.

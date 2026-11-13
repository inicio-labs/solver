# 8. Maker orders live at accept

- **Status:** Accepted, implemented behind `engine.maker_live_at_accept` (off by default).
- **Date:** 2026-11-14
- **Deciders:** Vaibhav Jindal.
- **Scope:** private-note maker orders submitted through the maker gateway (ADR 0003).
- **Related:** [ADR 0003](0003-market-maker-v1-gateway.md) (the gateway and its watcher), [ADR 0005](0005-maker-self-trade-prevention.md) (self-trade prevention), [market maker guide](../market-maker-guide.md#13-orders-live-at-accept), [runbook](../postgres-runbook.md#maker-orders-live-at-accept), issues [#67](https://github.com/inicio-labs/solver/issues/67) (strikes, last look, node trouble, expiry-based cap, switching off without a restart), [#68](https://github.com/inicio-labs/solver/issues/68) (caps on unconfirmed orders), [#69](https://github.com/inicio-labs/solver/issues/69) (a note spent by a waiting transaction).

## Context

A maker order trades only after its note is in a block, the maker-note watcher has seen it, and the next matcher round has run: about 3 to 4 seconds after `SubmitOrder`. A maker re-quoting in a moving market loses most of that time.

The Miden node accepts a transaction that spends a note created by another transaction still waiting in its mempool; both then land in one block. This is byte-identical from node 0.17.0 to 0.18.2. The maker kit and Pragma send `SubmitOrder` only after the node accepted their transaction, so in the normal case the note our settlement spends is already known to the node.

## Decision

A private maker order goes live when the gateway accepts `SubmitOrder`, before its note is in a block. If the note never arrives, the node's refusal and the watcher's cap take the order back out.

### Words

- **Root row:** the order row of the note the maker submitted (`orders.note_id = maker_lineages.note_id`). Every rule below applies to root rows only: remainders exist only after our settlement committed, which also activates the lineage.
- **Flagged:** `maker_lineages.live_at_accept = true`: the order went live at accept.
- **Unconfirmed:** flagged, and the lineage is still `pending`: its note is not seen yet.
- **Eligible:** passes the maker's stops and cutoffs. The `eligible_orders` view is `live_orders` without its status filter.
- **`waiting`:** a new order status. An unconfirmed order out of the book until its note lands. The maker still sees it as live.

### D1–D11

| # | Decision |
|---|---|
| D1 | Every maker is trusted. One switch, `engine.maker_live_at_accept`, off by default; changing it needs a restart. |
| D2 | Only private notes go live at accept. Public notes keep the confirmed path (ingest and the watcher). |
| D3 | When a settlement with unconfirmed parents never commits, what the node said decides where they go (table below). |
| D4 | A `waiting` order comes back only when its note is in a block. |
| D5 | The maker sees a `waiting` order as LIVE: in `ListActiveMakerOrders`, and as `LIVE` in a voided `SettlementResolved`. No proto change. A cancelled one becomes `stopped` and is reported CANCELLED. |
| D6 | **The cap.** The watcher cancels an unconfirmed order only when all hold: (a) `maker_unconfirmed_cancel_after_secs` (default 60) since accept, by the database clock; (b) the watcher has synced to `accept_block + maker_unconfirmed_cancel_after_blocks` (default 20); (c) this round did not find the note; (d) one exact lookup of the note and its nullifier confirms it is not on chain. The lookup's answer counts as the round's observation: if it finds the note, the order is activated. At most 16 lookups per round; the rest wait for the next. A failed lookup cancels nothing. The watcher imports an unconfirmed note's history from 10 blocks before `accept_block`, so a note a lagging node missed on its first lookup is still found. |
| D7 | **Freshness.** `accept_block` is the watcher's latest synced block, written once at accept. An order goes live at accept only if that block was read from the chain less than 10 s ago. Intake reads it inside its transaction, after it share-locks the owner epoch. |
| D8 | Settlement isolation (keeping a doubtful maker input out of other settlements) comes later, if the counters show it is needed. |
| D9 | Accepted risks, below. |
| D10 | Startup recovery keeps today's behaviour: a refused retry waits for the transaction's expiry. |
| D11 | Two PRs: metrics and a mock-chain harness first, then this feature. **Outstanding:** the plan's manual tests against a local node 0.18, and a refusal text captured from it as a fixture. The parser's markers are copied from the node's source (0.17.6 and 0.18.0); they are not yet pinned by a captured sample. |

### Accept

With the switch on, a newly claimed submit of a private note, in intake's transaction:
1. `SELECT owner_epoch … FOR SHARE`, once per round: a new owner's claim waits for this commit, and a round after a takeover fails (and stops the solver) instead of putting a ghost order in a book nobody reads.
2. Read the watcher's block (D7). Not fresh: the confirmed path.
3. Insert the root row `ON CONFLICT (note_id) DO NOTHING RETURNING`. An existing row: the confirmed path.
4. Run `live_book_update_tx` on it, the one place maker tags are added. Not kept (the maker cancelled it first, or a cutoff covers it): delete the row and take the confirmed path, so nothing differs from today.
5. Only if kept: set `live_at_accept` and `accept_block`, append LIVE.
6. Publish `BookUpdate { active, maker_updates }` in the round's one update, under the publication guard.
7. After commit, wake the event streams.

The confirmed path is today's: no row until the watcher sees the note. Accept fallbacks are counted per maker (`watcher_stale`, `conflict`, `public`).

### Order life

| Event | What happens |
|---|---|
| The watcher sees the note (in a round, or through the cap's lookup) | Lineage `activated` (`activated_at`). A `waiting` root returns to the book through `live_book_update_tx`, with no event. An `active` root needs nothing; existing rows are never sent to the matcher again (this applies with the switch off too). No second LIVE. |
| Our settlement lands | `confirm_settlement_tx` activates the parents' pending flagged lineages. When the settlement lands in the block that creates the note, the note is erased and the watcher never sees it; this is the only way the lineage learns it was real. |
| Release of a failed settlement | D3. |
| Cap | Lineage `rejected` whatever the root's status. An `active` or `waiting` root gets a stop row, is `stopped`, leaves the book, and the maker hears `CANCELLED` `note_not_received` if it saw the order live. A `stopped` or `onchain_nullified` root: closed silently. A `settling` root: skipped until its settlement resolves. |
| Spent elsewhere | By the root's current status: `settling` waits; `executed` (our settlement) activates the lineage; any other status becomes `onchain_nullified`, the lineage `rejected`, with one `UNAVAILABLE` "note spent outside this solver" if the maker saw it live. |
| A different note | `settling` waits for its settlement. Otherwise the lineage is `rejected` with one `REJECTED`, and the root is `stopped`. |
| One bad note in a round | A note the tracker can't look up or import gets no observation this round; the others carry on. A failed batch lookup or import is retried note by note. A note whose details a stored record of another note already holds is decided by an exact lookup instead of an import. Once the cap is due, an unread unconfirmed order is decided by the cap's exact lookup. A failure of the chain sync itself still fails the round. `solver_maker_watcher_unobserved_notes` counts the notes a round left unread. |
| A note found under our ID | It is ours: a note ID commits to the whole note, metadata included, so a node answer whose metadata looks different (a withheld attachment) is the same note. A note that really differs has another ID and is never found; the cap ends its order with `note_not_received`. The tracker therefore never reports a different note, and REJECTED is not sent. |
| Lineage rejected | No longer watched. |
| Maker cancels | Existing `CancelOrder` / `CancelAll`. A `waiting` root whose note lands later goes through `live_book_update_tx`, so it is stored `stopped`. |
| Switch off | No new order goes live at accept; existing flagged lineages keep their rules. |
| Gateway off at startup while unconfirmed orders exist | The solver refuses to start and names the escape hatch. |

### D3: where unconfirmed parents go

The executor keeps only the node's answer, in memory, until the release commits. The release decides inside its transaction, against the orders as they are then, so a note the watcher found meanwhile is already back to normal.

| What the node said | Unconfirmed parents |
|---|---|
| Code 2 "unknown notes", naming at least one of the settlement's parents | Only the named unconfirmed parents wait; the rest go back to the book. The node names every unknown note in one refusal, so an unnamed one was known. |
| Code 2 "unknown notes", naming nothing we recognise (the text changed?) | All wait. |
| Code 2 "account commitment does not match" | Back to the book: the node checks the notes first. |
| Code 2 mempool nullifiers or duplicate outputs; code 8; code 1; code 3; any other code; no details; unreadable text | All wait: the node stopped before the notes check, or we can't tell. |
| Accepted, but it never landed; outcome unknown, then expired; a refused attempt reloaded after a restart | All wait. |
| Our own failure before the node; reservation refused; a build or proving failure | Today's behaviour: back to the book. |

Always: a parent the maker cancelled is `stopped` and reported `CANCELLED`; a flagged root whose lineage is `rejected` is `stopped`; a parent spent on chain is `onchain_nullified` and reported `SPENT_ELSEWHERE`; users always go back to the book; waiting parents go into `removed`.

The node prints a state conflict as text. The executor matches `unauthenticated input notes are unknown`, `does not match the current commitment`, `nullifiers already exist` and `output notes already exist` (node 0.17.0–0.18.2, `block-producer/src/errors.rs`), and finds each parent's `NoteId` by its `Debug` form. Re-check these on every node upgrade: a change makes every unconfirmed order wait (safe) and shows as `unreadable` or `unknown_notes_unmatched`.

## Accepted risks

1. **Chaining.** Our settlements build on each other through the solver account; a dropped maker transaction drops our later settlements for about 60 s. This already happens on node restarts.
2. **Empty blocks during a prover backlog** can satisfy the cap's block part early and cancel a healthy order. Mitigation: generous defaults (60 s and 20 blocks), tuned from PR A's counters.
3. **One careless or hostile maker can slow settlement for everyone** (a model put user fills at 30.6% on one pair). Mitigation: the switch, which needs a restart. Future: a per-maker fuse (#67).
4. **Last look through the depth API** (a model put it at +1.12 bps per fill for users). Future: the fuse.
5. Last look through `SettlementPending` (#67 item 2); depth not backed by a note, and caps (#68); a spend waiting in the mempool (#69).
6. Spoofing when `band_bps > 0`: an unconfirmed order can move the bounded price.
7. Same-side re-quote overlap: a new quote sent before the old one's cancel lands. The guide explains it.
8. A `waiting` order keeps its queue number.
9. The block part of the cap can start up to about 4 blocks early (D7 allows a 10 s old block); the seconds part still holds.
10. A slow maker transaction can land just after its cap; the maker reclaims the note.
11. A note erased by the maker's own reclaim in the same block ends as `note_not_received`.
12. A crash before a refusal is recorded makes recovery retry until expiry (about 60 s, one or two proofs).
13. An order released into `waiting` after its cap time has passed can be cancelled in the next round, even if a re-sent maker transaction is about to land.

## Operations

- **Metrics** (all in memory, no database read on scrape): `solver_executor_settlement_conflicts_total{kind}`, `solver_executor_settlements_dropped_total{unconfirmed_maker_input}`, per maker `solver_maker_live_at_accept_total`, `solver_maker_accept_fallbacks_total{reason}`, `solver_maker_unknown_notes_named_total`, `solver_maker_sent_to_waiting_total`, `solver_maker_waiting_returned_total`, `solver_maker_cap_cancels_total`, `solver_maker_unconfirmed_activations_total{via}`, and the gauges `solver_maker_unconfirmed_orders`, `solver_maker_waiting_orders`, `solver_maker_oldest_unconfirmed_order_age_seconds`, `solver_maker_watcher_unobserved_notes`. `solver_maker_watcher_block_read_age_seconds` approximates the D7 age by the wall clock; D7 itself measures it with a monotonic clock, so a clock step can't make an old block look fresh.
- **Escape hatch:** `solver-bin cancel-unconfirmed`, with the solver stopped, under the ownership lock: first claims a new owner epoch (so an intake session left over from the stopped solver can't put another order live), then closes every unconfirmed lineage and stops its root and every `waiting` row, sending `CANCELLED` `note_not_received` to makers that saw them live. A root a settlement still holds stays with it: if that settlement fails, the order stays out of the book; if it commits, its remainder is reported `CANCELLED`.
- **Rollback:** the migration's down refuses while `waiting` rows or unconfirmed lineages exist; run the hatch first.

## Consequences

- Makers' orders trade about 3 to 4 seconds sooner.
- A maker can see `REJECTED`, `UNAVAILABLE` or `CANCELLED` `note_not_received` after `LIVE`.
- Activation no longer re-sends rows that already exist, with the switch on or off.
- The watcher no longer fails a whole round for one unreadable or mismatched note.

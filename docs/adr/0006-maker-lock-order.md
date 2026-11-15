# 6. Lock order for maker rows

- **Status:** Accepted, implemented.
- **Date:** 2026-11-14
- **Deciders:** Vaibhav Jindal. The deadlock was found by QA on 2026-10-25; the fix was agreed on 2026-10-26.
- **Scope:** PostgreSQL transactions of the 0.17 solver that lock maker rows (`makers`) together with order rows.
- **Related:** [ADR 0003](0003-market-maker-v1-gateway.md) (intake session, reservation, the first statement of this lock order), [ADR 0005](0005-maker-self-trade-prevention.md) (a reservation that cancels sit-outs).

## Context

Two database sessions write at the same time: the core writer (executor, ingest, maker-note watcher, startup) and the intake session (maker commands, one transaction per group-commit round, ADR 0003). When two transactions lock the same two rows in opposite order, PostgreSQL waits `deadlock_timeout` (1 s by default) and then aborts one of them with `deadlock detected` (SQLSTATE 40P01).

A maker's row in `makers` is locked for two reasons:

- **Event numbering.** Appending an event (`maker_db::append_event_tx`) increments `makers.next_event_seq`. The row lock makes appenders of one maker wait for each other, so the maker's committed event sequences have no gaps.
- **Turn-taking between a cancel and a reservation.** A cancel raises its cutoff under the maker's row lock, and a reservation checks liveness under the same lock. Either the cancel sees the reservation as exposure, or the reservation sees the cutoff.

So a transaction that appends events for several makers, or reserves orders of several makers, locks several maker rows.

## Decision

### The rule

A transaction that locks rows of several makers takes its locks in one global order, up front:

1. The order rows it will change, by note ID, in one statement (`ORDER BY note_id FOR UPDATE`).
2. Then the maker rows, by maker ID, in one call to `maker_db::lock_makers_ordered_tx`, or a prelock that ends in it.
3. Both before its first event or other maker-row write. After that it never waits for an order row while it holds a maker row.

### Who follows it

| Transaction | Session | Order rows | Maker rows |
|---|---|---|---|
| Reservation (`prepare_settlement_tx`, `reserve_settlement_tx`) | core writer | inputs, by note ID | owners of the inputs and of the sit-outs, by maker ID |
| Intake round (`gateway::intake`) | intake | `prelock_submission_orders_tx` | `prelock_command_makers_tx` |
| Watcher round | core writer | `prelock_watcher_tx` | `prelock_watcher_tx` |
| Confirm and discard (`confirm_settlement_tx`, `finish_discarded_settlement_tx`) | core writer | inputs, by parent note ID | `report_settlement_tx`, before its liveness reads and events |
| Retiring spent orders (`mark_orders_onchain_nullified_tx`) | core writer | `prelock_watcher_tx` | `prelock_watcher_tx` (this ADR) |

### The violation

`postgres_db::mark_orders_onchain_nullified_tx` retires orders whose notes were spent elsewhere. It called `maker_db::report_spent_tx`, which appends one `OrderStatus UNAVAILABLE` per live maker order, in row order. Each append updates that maker's row, so the makers were locked one at a time in note order, not maker-ID order. The order rows were locked last, by the `UPDATE` that retires them.

Example: makers M1 < M2, and M2's note sorts first. The retirement locks M2 (its first event), then waits for M1. An intake round with commands for both makers locks M1, then waits for M2. Deadlock.

The watcher prelocks before it calls this function, so it was safe. Three callers had no prelock:

- the executor's give-back of held orders (`release_held` in `executor/executor.rs`);
- ingest sync (`ingest/ingest.rs`);
- startup reconciliation (`pipeline.rs`).

**Why production doesn't hit it today.** The give-back and ingest run inside `PgPool::write_book`, which holds the publish lock. The intake round holds that same lock for its whole transaction (`gateway::intake`, via `publication_guard`), so those two can't interleave. Startup reconciliation runs before the gateway starts.

So the deadlock is latent. It happens only if a caller runs outside `write_book`, or intake stops holding the publish lock across its transaction. The regression test below reproduces the interleaving with two plain sessions. The fix makes the function follow the lock rule on its own, instead of depending on the publish lock for that. Planned work (the self-trade rule in ADR 0009) adds more maker locking inside book writes.

### The fix

`mark_orders_onchain_nullified_tx` now calls `prelock_watcher_tx(conn, &[], &[], &sorted)` before `report_spent_tx`. That locks the spent order rows by note ID, reads which of them are live maker orders, and locks those makers through `lock_makers_ordered_tx`. It is one line in the one function every caller goes through. In the watcher round the second call asks again for locks it already holds: two extra reads, no waits.

A side effect: `report_spent_tx` now reads liveness under the maker locks. A cancel either commits before that read, and the order is not reported, or waits until the retirement commits. A maker whose cancel was confirmed no longer gets `UNAVAILABLE` for that order afterwards.

### For new code

Any code that appends events, or locks rows, for more than one maker takes its locks up front: order rows by note ID, then `lock_makers_ordered_tx` (or a prelock that ends in it), before the first write. Never let a loop over orders lock makers one by one by appending their events. A function that appends events without locking first says in its doc comment that its caller must hold the locks, as `report_spent_tx` and `cancel_self_trades_tx` do.

## Consequences

- **If the two transactions had interleaved,** a deadlock would have stalled the single core writer for about 1 s, with executor, ingest and watcher writes queued behind it. PostgreSQL then aborts one side. An aborted intake round answers every command in it with `OutcomeUnknown`, and makers must retry by request ID. An aborted core-writer transaction is retried by its caller. `solver_db_deadlocks_total` counts both.
- **Today the publish lock keeps them apart,** so this is a guard against future changes, not a fix for an observed failure. The function is now safe whichever caller or session runs it.
- Retiring spent orders costs two more reads per call.

## Verification

- `maker_db::tests::retiring_spent_maker_orders_does_not_deadlock_an_intake_round` (PostgreSQL). Two makers whose notes sort opposite to their maker IDs each have a live order. A trigger pauses the retirement after its first event while an intake round, written exactly as `gateway::intake` writes it, cancels both makers. With the fix both transactions commit and each maker gets one `UNAVAILABLE`. Without the prelock line the intake round fails with `deadlock detected`.

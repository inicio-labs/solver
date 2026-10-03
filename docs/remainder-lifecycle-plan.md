# PSWAP remainder lifecycle (V1)

## Scope and ownership

This change covers the direct-clearing executor, ingestion, the SQLite order store, and the clearing matcher. It does not add a market-maker API or change the clearing-price algorithm. A single submitted transaction can consume several parent notes and produce zero or one PSWAP remainder for each. The executor owns the transaction outcome; ingestion is a second confirmation observer; SQLite owns the authoritative order state; the matcher keeps a rebuildable in-memory view.

## Invariants

- A parent note and its remainder are never simultaneously matchable.
- A remainder inherits the parent's original price-time sequence. Its new note ID identifies the new on-chain note; it does not reset time priority.
- Submission is not confirmation. Only a confirmed transaction or an included expected output activates the child and retires the parent.
- An uncertain outcome remains `settling`; it is never silently returned to the live book.
- Executor and ingestion may observe the same result in either order. Database transitions and matcher notifications must be idempotent.

## Durable data and transitions

Local execution yields a transaction ID and output-note IDs. After proving, but before submission, persist the transaction ID, serialized executed result, each input note ID, its expected payback note ID, and each expected remainder's full note bytes in one SQLite transaction that marks the inputs `settling`. Verify that both predicted payback and remainder IDs occur in the actual executed outputs.

On confirmed settlement, one SQLite transaction marks the inputs `executed`, inserts each child as `active` with its parent's `priority_seq`, and records the attempt as confirmed. The executor publishes one `BookUpdate` containing parent removals and child activations. If ingestion discovered the output first, it uses the same transition. A duplicate observer does nothing.

On a definitive failure, record `rejected` before the fallible input-nullifier check. Then one SQLite transaction marks consumed inputs terminal, reactivates valid parents, and deletes the attempt and its mappings. If classification fails, recovery resumes it without resubmitting the rejected transaction. A pending or unknown result stays `settling`. Client-side `Stale` and `DiscardedInitialState` can represent a local timeout and its dependent transactions, not an on-chain failure, so both stay reserved. Never reset all `settling` rows unconditionally.

If the client lost its local submission record, query the known payback note ID for on-chain inclusion. This works for full and partial fills. If absent, the executor can re-prove and resubmit the serialized *same* execution result and transaction ID. A later rejection does not prove the first copy failed; leave such an attempt `uncertain` and alert. The safe behavior is to keep its parent unavailable, not guess that it failed.

The intended sequence is:

```text
Active in-memory parent → send batch → mark Inactive (off matchable index)
              → prove exact transaction → persist attempt + Settling parent
              → submit same transaction ID → observe commitment
              → Executed parent + Active remainder in one SQLite transaction
              → notify matcher (parent removal, remainder activation)
```

An attempted submission that returns an RPC timeout remains `Settling`: the timeout does not prove non-inclusion. A discarded transaction is handled separately by checking its input nullifiers. Transactions are not re-executed during retry or recovery, because that could create a different ID while the first transaction is still live.

The original `priority_seq` and arrival timestamp are copied to the child. The parent and child therefore share a lineage priority, though only one is active at a time. The priority index is non-unique; the DB trigger assigns a new sequence only to newly ingested unrelated orders (`priority_seq = 0`).

## Matcher and race policy

The matcher owns the live in-memory book. In direct clearing, sending a batch to the executor marks its parents `Inactive`: their records stay in memory, but they leave the matchable price/FIFO index. They cannot be selected again while the outcome is pending. A confirmed outcome removes the parent (idempotently) and adds any remainder. A definite failure reactivates a still-valid parent via the existing order channel. SQLite persists the same state for restart recovery, but is not queried by each matching tick. The price/FIFO index uses the inherited sequence only for the active tip of a lineage.

All producers use one ordered channel. `DbPool::update_book` reserves queue capacity before acquiring the single writer connection, commits the database transaction, then publishes without yielding while still holding that connection. This preserves database order in the messages and avoids waiting for channel space with a database transaction open. The matcher applies each complete `BookUpdate` synchronously. Only batch matching waits for the timer. If ingestion discovers and consumes a note in the same sync, consumption wins and no activation is published. Delayed re-feeds recheck durable status before publishing, so they cannot resurrect consumed or reserved orders.

Database commit and process memory cannot be crash-atomic. Critical worker failure stops the coordinated pipeline; an external supervisor must restart the process. Unexpected shutdown returns an error, distinct from an operator-requested cancellation. We do not restart the matcher independently while the executor may own orders that still have an `active` database row during proving.

Before direct-clearing hydration, ingestion syncs and replays included PSWAP notes retained in its client store. It recognizes expected remainders before ordinary insertion, deduplicates against persisted orders, checks consumption, and commits the recovered changes. Existing FIFO sequences remain unchanged; missed discoveries use the client's creation timestamp and note ID as a deterministic recovery tie-break. The executor starts only after this reconciliation/bootstrap has completed, and then reconciles reserved attempts. The matcher never polls the database during normal matching.

## Code boundaries

- `db`: atomic attempt reservation, confirmed parent-to-child handoff, discarded-attempt classification, and active-only startup hydration.
- `executor`: predict and verify actual output IDs, submit one fixed transaction, reconcile its outcome, and notify the matcher.
- `ingest`: recognize a linked remainder before ordinary note insertion, and observe on-chain parent nullifiers.
- `matcher`: mark dispatched parents Inactive; apply complete outcome events immediately, matching only on the timer.

This was the original SQLite design note. The application database now uses the PostgreSQL baseline in `crates/solver/migrations_postgres`; the obsolete SQLite `schema.sql` and migrations were removed. This document is historical, not an operator migration procedure.

## Verification

Cover full and partial fills, executor-first and ingest-first confirmation, duplicate observations, submission without confirmation, definite failure, crash/restart with an outstanding attempt, stale order-add events, and inherited priority relative to newer orders. Inject failures into cleanup and sync persistence to prove rollback. Test closed/full matcher queues, missed durable-client discoveries, and a remainder discovered and consumed in one sync. The final transaction-output check must use the actual executed output IDs, not only a predicted note constructor.

Before production rollout, execute the linked unit and integration tests and run one real Miden settlement with a partial remainder, one full fill, and a forced restart between submission and confirmation. Compilation alone is not proof of the network recovery path.

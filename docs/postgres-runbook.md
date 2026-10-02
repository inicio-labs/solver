# PostgreSQL application database runbook

This applies only to the solver's orders, unresolved settlement attempts, sync
cursor, and token registry. The keyless-ingest and signing-executor Miden client
stores remain separate SQLite files. Deploy one solver process per application
schema. There is no SQLite-to-PostgreSQL data copy, dual write, outbox, or
active-active mode in V1.

## Fresh deployment

1. Provision PostgreSQL 14 or newer near the solver with TLS, monitored free space, daily
   base backups, WAL archiving/PITR, and a tested database restart procedure.
   Budget at least ten connections for the solver: one persistent writer, four
   readers by default, one migration/operations connection, and headroom.
   Connect the solver **directly** to PostgreSQL, or through a pooler in
   **session** mode only. A transaction-mode pooler (PgBouncer
   `pool_mode = transaction`, Supabase's pooled port 6543, and similar) moves
   the writer between server connections between transactions. That breaks
   the session-held ownership lock and the per-session settings, and conflicts
   with Diesel's prepared-statement cache; the solver would detect a
   different backend and stop. The solver already pools its own connections.
2. Create a dedicated schema and three roles: migration owner, solver writer,
   and solver reader. The writer needs schema `USAGE`, DML on application
   tables, `USAGE, SELECT` on the `orders_priority_seq_seq` identity sequence,
   and `SELECT` on `__diesel_schema_migrations`. The reader needs schema
   `USAGE` and `SELECT` on application tables and migration history. Grant no
   schema creation privilege to either runtime role. Set the same
   `search_path` for all three roles (or in all three URLs) so the migration,
   writer, and reader resolve the same tables. Configure default grants for
   future migrations as well.
3. Set `SOLVER_MIGRATION_DATABASE_URL` to the migration-role URL and run
   `solver-bin migrate-db` once. Set `SOLVER_DATABASE_URL` to the writer URL
   and run `solver-bin check-db` (it checks the writer connection only). Set
   `SOLVER_READ_DATABASE_URL` to the reader URL for the runtime; startup
   verifies that both URLs resolve the same database and schema. Use `sslmode=verify-full`, a trusted root CA,
   `connect_timeout=5`, and supported libpq TCP keepalive settings in remote
   URLs. Store credentials in a secret manager or protected environment file;
   never put them in `solver.toml` or a command log.
4. Keep `executor_store_path` and `ingest_store_path` as distinct SQLite files.
   Start one solver. A second process pointed at the same application schema
   must exit with “another solver already owns” before any pipeline work.
5. Check `/readyz`, sync freshness, order/settlement counts, and PostgreSQL
   session `application_name`. Verify one full fill and one partial fill before
   enabling normal traffic. A partial remainder must inherit its parent's FIFO
   priority.

The former SQLite setting `app_db_path` is no longer read; remove it from
`solver.toml`. A config that still sets it is accepted without a warning.

## Cutover from a SQLite deployment

There is no data copy. Before switching, stop accepting new orders and let the
old solver run until it has no unresolved settlement (every attempt confirmed
or released, every order `executed`, `onchain_nullified`, or still `active`).
Then stop it, prepare the PostgreSQL database as above, and start the new
solver. Startup replays the ingest client's stored notes into PostgreSQL, so
still-active orders return; a settlement that was in flight at the switch is
not known to the new database and its parents would be re-matched.

## Schema migrations

Normal startup never runs DDL. Each solver binary embeds the migrations it was
built with, and startup requires the database's applied set to equal that set
exactly: a missing migration stops with "run migrate-db", an extra one with
"use a compatible solver binary". Because only one solver may run per schema,
every schema change is a stop-then-start deploy:

1. Stop the solver.
2. With the **new** binary: `solver-bin migrate-db` (migration role).
3. Start the new binary.

To roll back after step 2, revert the migration with the new binary before
starting the old one, for example `diesel migration revert` against the
migration-role URL from the solver crate directory; the old binary refuses to
start while the newer migration is applied. Every migration after the baseline
must ship a `down.sql` that undoes it without data loss. The baseline's own
`down.sql` drops every application table and is only appropriate before the
first production order.

## Failure and restart

The solver holds one PostgreSQL advisory lock on its writer session. If that
session is lost (database restart or failover, network cut, terminated
backend), the solver reconnects in place for up to 30 seconds while holding
its internal writer lock, re-takes the advisory lock, and checks the
`sync_state.owner_epoch` it claimed at startup. A write lost before `COMMIT`
returns an ordinary error; a write lost during `COMMIT` is resolved with
`pg_xact_status` and returns its real outcome. If the previous backend still
holds the lock after a network cut, the solver terminates that backend.

The solver stops only when it cannot safely continue: another solver holds
the lock or advanced the owner epoch while it was disconnected, the writer
does not come back within the reconnect window, the live session no longer
holds its lock, a database worker panics, or a write is still running after
the 30-second client deadline. Every other database error is returned to the
caller and handled in place:

- A failed read (including the public price API and `/readyz`) returns an
  error to that caller; the read pool reconnects on the next checkout.
- A lock or statement timeout rolls back that one transaction on a healthy
  session; the caller re-feeds or retries.
- Ingest keeps a sync result whose write failed and retries it on the next
  tick, up to five times, before stopping the pipeline.
- A settlement whose lifecycle write failed is retried on the next
  reconciliation tick.

When the executor cannot settle — no fee headroom, the node RPC or the
database writer unavailable — it enters **verification mode**: it stops
reading the matcher queue, holds the batch it was working on, and every
`engine.verify_interval_ms` (default 5 s) checks that the node answers, the fee
balance covers one settlement, the writer commits, and any orders it owes the
matcher are returned. It retries without limit. Once all checks pass it runs
the held batch and resumes.
The matcher keeps one candidate batch queued and leaves every other order live
in its book, so nothing is re-matched at a stale price. Watch for
`executor entering verification mode` in the logs; a long stay means the
node, the fee balance, or PostgreSQL needs attention, not the solver.

The solver sets `idle_session_timeout = 0` on its own sessions, so a server or
role default that closes idle clients does not end the writer session.

When the solver does stop, restart the whole solver; restarting only a Rust
task does not rebuild the matcher. The restart reconciles durable notes and
unresolved settlements and rehydrates the matcher from what committed. Alert
on `solver_db_writer_reconnects_total` increasing: each step is a recovered
session loss worth explaining.

Use a process supervisor with `Restart=on-failure`, bounded restart backoff,
and a shutdown deadline (for example, systemd `TimeoutStopSec=30s` followed
by its final kill signal). The database's `statement_timeout=10s` and
`lock_timeout=2s` bound server work. The solver also stops waiting after 30
seconds for one client-side write, stops the whole pipeline, and bounds worker joins to 15 seconds and main-runtime teardown to
5 seconds. An already-running blocking query is not cancelled by that deadline;
its commit outcome may be uncertain and restart hydration checks durable state.
Configure libpq/TCP failure detection and alert if the supervisor has to
force-stop the process. Diagnose a database-server failure before restarting
PostgreSQL; a single solver network failure does not justify restarting it.

Check `GET /health` for process liveness and `GET /readyz` for read-pool
reachability, original writer ownership, and recent ingest sync (the schema is
verified once at startup; a probe never waits behind application writes). Alert on any
database deadlock, repeated lock timeout, writer ownership loss, migration
mismatch, repeated critical restart, stale sync, and an unresolved settlement
older than its chain expiry plus reconciliation allowance. Track pool wait,
transaction latency, order status counts, unresolved attempts by status and
age, and matcher/executor channel capacity. `GET /metrics` on the same
loopback-only observability port exposes solver-side DB call/error counters,
wait and duration histograms, timeout/deadlock counters, connection, writer
ownership and fatal-shutdown gauges, channel remaining capacity, and skipped
matching ticks.
Scrape PostgreSQL statistics separately for order/attempt counts and server
deadlocks; the solver endpoint does not scan the full orders table on every
scrape. Do not log credentials, serialized notes, or transaction payloads.

### Query statistics and table maintenance

Enable `pg_stat_statements` (`shared_preload_libraries`, then
`CREATE EXTENSION pg_stat_statements` in the application database) and review
the top statements by total and mean time after load tests and periodically
in production. Every solver query is a short, parameterised Diesel statement,
so a statement that climbs that list points at a missing index or a plan
change.

`orders` only grows: executed and consumed orders stay as history, and every
order changes status two or three times, leaving dead row versions behind.
Watch `n_dead_tup`, `last_autovacuum` and `last_autoanalyze` for `orders` in
`pg_stat_user_tables`. Once the table holds a few hundred thousand rows, lower
its autovacuum thresholds so cleanup keeps pace:

```sql
ALTER TABLE orders SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
```

Active-book hydration reads only the partial index on active orders, so query
speed does not degrade with history; storage and backup size do (roughly
800 bytes per order). A retention policy for retired orders needs its own
design, because startup recovery uses stored note IDs to recognise notes it
has already ingested.

## Backup and restore

Back up PostgreSQL and both Miden SQLite client stores together with a recorded
chain height and software/schema version. A historical PostgreSQL-only PITR
restore can put the database *behind* an executor store that has already
submitted transactions; do not simply resume matching from such a mismatch.
Keep the solver stopped, identify every in-flight/confirmed settlement against
the chain and both client stores, then restore a jointly consistent checkpoint
or repair the application state before startup. Rehearse this with a disposable
environment and record the result before production approval. A fresh empty
database is appropriate only before the first production order/settlement.

## Release gates

Run the unit suite and PostgreSQL integration suite against a disposable
database (`SOLVER_TEST_DATABASE_URL`, including ignored PostgreSQL tests),
then a release build with the deployment's libpq/TLS packaging. Benchmark
100-note ingestion, 511-input prepare/confirm, 50-token bulk reads, 100,000
active-order hydration, and concurrent public reads on a staging topology
with representative network latency. Record observed P95/P99 and connection
counts rather than treating the provisional plan thresholds as measured data.
Finally run live Miden full-fill, partial-fill, restart-before-confirmation,
commit-before-matcher-send recovery, and backup/restore rehearsals. Do not
mark a release complete without those environment-dependent checks.

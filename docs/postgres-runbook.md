# PostgreSQL application database runbook

This applies only to the solver's orders, notes, settlement attempts, sync
cursor, and token registry. The keyless-ingest and signing-executor Miden client
stores remain separate SQLite files. Deploy one solver process per application
schema. There is no SQLite-to-PostgreSQL data copy, dual write, outbox, or
active-active mode in V1.

## Fresh deployment

1. Provision PostgreSQL near the solver with TLS, monitored free space, daily
   base backups, WAL archiving/PITR, and a tested database restart procedure.
   Budget at least ten connections for the solver: one persistent writer, four
   readers by default, one migration/operations connection, and headroom.
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
   `solver-bin migrate-db` once. Set `SOLVER_DATABASE_URL` and
   `SOLVER_READ_DATABASE_URL` to the writer and reader URLs, respectively, and
   run `solver-bin check-db`. Use `sslmode=verify-full`, a trusted root CA,
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

Normal startup never runs DDL. A missing or unexpected migration fails before
workers start. Apply future migrations with the operator command while the
solver is stopped, then deploy a compatible binary.

## Failure and restart

The solver holds one PostgreSQL advisory lock on its original writer session.
If that connection or lock is lost, restart the whole solver; restarting only a
Rust task does not rebuild the matcher. A lost PostgreSQL response does not
prove that a write failed. The restart reconciles durable notes and unresolved
settlements and rehydrates the matcher from what committed.

Use a process supervisor with `Restart=on-failure`, bounded restart backoff,
and a shutdown deadline (for example, systemd `TimeoutStopSec=30s` followed
by its final kill signal). The database's `statement_timeout=10s` and
`lock_timeout=2s` bound server work. The solver also stops waiting after 30
seconds for one client-side database operation, marks the whole pipeline
unhealthy, and bounds worker joins to 15 seconds and main-runtime teardown to
5 seconds. An already-running blocking query is not cancelled by that deadline;
its commit outcome may be uncertain and restart hydration checks durable state.
Configure libpq/TCP failure detection and alert if the supervisor has to
force-stop the process. Diagnose a database-server failure before restarting
PostgreSQL; a single solver network failure does not justify restarting it.

Check `GET /health` for process liveness and `GET /readyz` for PostgreSQL
schema, original writer ownership, and recent ingest sync. Alert on any
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

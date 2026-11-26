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
   and run `solver-bin check-db` (it checks the writer connection only: the
   migration history, and `SELECT` on the views the solver reads directly). Set
   `SOLVER_READ_DATABASE_URL` to the reader URL for the runtime; startup
   verifies that both URLs resolve the same database and schema. Use `sslmode=verify-full`, a trusted root CA,
   `connect_timeout=5`, and libpq TCP keepalives in remote URLs, for example
   `keepalives_idle=5&keepalives_interval=2&keepalives_count=3&tcp_user_timeout=15000`,
   so a dead connection fails within about 15 seconds and takes the in-place
   reconnect path below rather than the 30-second write deadline. Store credentials in a secret manager or protected environment file;
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
3. If the migration adds a table or view and your default grants don't cover
   new relations, grant it to the writer and reader roles now. The
   live-at-accept migration adds the view `eligible_orders`, and the
   self-trade storage migration the view `eligible_lineages`; both roles need
   `SELECT` on each. Reverting the self-trade storage migration drops
   `eligible_lineages` with its grants, so grant it again after a re-apply.
4. `solver-bin check-db` with the writer URL. It checks the migration history
   and that the role can read the views the solver reads directly.
5. Start the new binary. Startup runs the same checks for the writer and the
   reader, and stops with the name of a view a role cannot read.

To roll back after step 2, revert the migration with the new binary before
starting the old one: `solver-bin revert-db` (migration role, same
`SOLVER_MIGRATION_DATABASE_URL`); the old binary refuses to start while the
newer migration is applied. `migrate-db` and `revert-db` take the solver's
ownership lock first, so they refuse to run while a solver is up, and no solver
starts until they finish. `revert-db` refuses to revert the baseline. Every migration after the baseline
must ship a `down.sql` that undoes it without data loss. The baseline's own
`down.sql` drops every application table and is only appropriate before the
first production order.

## Failure and restart

The solver holds one PostgreSQL advisory lock on its writer session. If that
session is lost (database restart or failover, network cut, terminated
backend), the solver reconnects in place, retrying with backoff (up to 5
seconds apart) until the database answers. It holds its internal writer lock
meanwhile, so other writes wait instead of failing; the matcher keeps its book
and reads keep working. It then re-takes the advisory lock and checks the
`sync_state.owner_epoch` it claimed at startup. A write lost before `COMMIT`
returns an ordinary error; a write lost during `COMMIT` is resolved with
`pg_xact_status` and returns its real outcome. If the previous backend still
holds the lock after a network cut, the solver terminates that backend.

The solver stops only when it cannot safely continue: another solver holds
the lock or advanced the owner epoch while it was disconnected, the outcome
of a commit lost in flight cannot be determined, the live session no longer
holds its lock, a writer worker panics, or a write is still running after the
30-second client deadline. Every other database error is returned to the
caller and handled in place:

- A failed read (including the maker gateway and `/readyz`) returns an
  error to that caller; the read pool reconnects on the next checkout. The
  maker gateway may use at most `read_pool_size - 1` read connections, so
  public traffic cannot starve ingest, the executor, or `/readyz`. The public
  price API reads the database only once, at startup. If the
  executor cannot load its pending settlements at startup (pool busy,
  database briefly unreachable), it retries every second instead of stopping.
- A lock or statement timeout rolls back that one transaction on a healthy
  session; the caller re-feeds or retries.
- Ingest keeps a sync result whose write failed and retries it on every
  tick for as long as the failure is transient (no extra RPC); `/readyz`
  reports the stalled sync meanwhile.
- A settlement whose lifecycle write failed is retried on the next
  reconciliation tick.

A transaction that fails the same way every time it is built (a bad note, an
insolvent batch, an execution or proving error) is retried one pair group at
a time, so the healthy pairs settle at once. A group that still fails on its
own is held back from the matcher for 30 seconds, doubling on each repeat
failure up to 5 minutes, and released once the hold is over. Watch for
`pair group keeps failing; holding its orders`.

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
role default that closes idle clients does not end the writer session. It also
sets server-side TCP keepalives on them (`tcp_keepalives_idle = 10`,
`tcp_keepalives_interval = 5`, `tcp_keepalives_count = 3`, and on Linux
servers `tcp_user_timeout = 25s`). If the solver's host or network vanishes,
the server ends the orphaned writer backend within about 25 seconds and
releases the ownership lock, so a restarted solver can start instead of
failing with "another solver already owns" until the operating system's
default keepalive (about two hours) notices. A pooler or proxy between the
solver and PostgreSQL must forward the disconnect for this to work; otherwise
end the stale backend by hand with `pg_terminate_backend` on the pid holding
the advisory lock in `pg_locks`. It also
sets `lc_messages = 'C'` so metrics can tell lock, statement and deadlock
timeouts apart. That needs a superuser or, on PostgreSQL 15+, `GRANT SET ON
PARAMETER lc_messages TO <writer>, <reader>`; without it the solver runs
normally, logs one warning, and counts those timeouts as `database` errors.

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
ownership and fatal-shutdown gauges, channel remaining capacity, skipped
matching ticks, settlement refusals by node code, dropped settlements, and
released inputs; with the maker gateway enabled, also the maker-note
watcher's synced block and read age and the pending submissions it waits on
(see the README's Observability section). These are all held in memory: a
scrape reads no database.
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
With Postgres's default trigger (20% of the table dead) a large `orders` table
would wait about a day between vacuums, and dead entries would pile up in its
partial indexes meanwhile. The `2026-11-19-000001_watcher_live_index`
migration sets these per-table settings on `orders`:

- `autovacuum_vacuum_scale_factor = 0.01`, `autovacuum_vacuum_threshold = 1000`:
  vacuum at 1% of the rows + 1000 dead rows. The trigger grows with the
  table, so the time between runs does too (about 3,400 dead rows at 240k
  rows, 11,000 at 1M).
- `vacuum_index_cleanup = on`: since PostgreSQL 14, vacuum skips the index
  pass when under 2% of the table's pages hold dead rows (the log line says
  `index scan bypassed`), and a 1% trigger usually is under that. Without
  this setting the dead index entries would stay until a larger run. Each
  run now reads every `orders` index in full.
- `log_autovacuum_min_duration = 0`: the server log shows every autovacuum
  and auto-analyze run on `orders`, with its duration and pages.

Before deploying the migration, check what is already set; it overrides
these four and keeps any other:

```sql
SELECT reloptions FROM pg_class WHERE oid = 'orders'::regclass;
```

Check that cleanup keeps pace:

```sql
SELECT n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count, last_autoanalyze
FROM pg_stat_user_tables WHERE relname = 'orders';

SELECT indexrelname,
       pg_relation_size(indexrelid) / current_setting('block_size')::int AS pages,
       idx_scan
FROM pg_stat_user_indexes WHERE relname = 'orders';
```

`n_dead_tup` should drop back after each run. An index file never shrinks
(vacuum frees pages inside it), so `orders_live_status_idx` settles at tens
of pages and should not grow day over day; `orders_pkey` and
`orders_lineage_idx` grow with the table. To change a setting, use
`ALTER TABLE orders SET (...)`. Reverting the migration resets only these
four settings to the server defaults; any other per-table setting stays.

Active-book hydration reads only the partial index on active orders, and the
maker watcher reads only `orders_live_status_idx` (Active and Stopped maker
orders), so query speed does not degrade with history; storage and backup size
do (roughly 800 bytes per order). A retention policy for retired orders needs its own
design, because startup recovery uses stored note IDs to recognise notes it
has already ingested.

## Maker orders live at accept

`engine.maker_live_at_accept` (off by default, [ADR 0008](adr/0008-maker-orders-live-at-accept.md)) lets a private maker order trade before its note is in a block. It needs the maker gateway and a watch interval of at most 2 s.

**Switching.** Changing it needs a restart. Turning it off stops new orders going live at accept; orders that already did keep their rules (the watcher still activates or caps them).

**Rollback.** The migration's `down.sql` refuses while any maker order went live at accept and still lacks its note, or any order is `waiting`. Stop the solver, run the escape hatch, then `revert-db`.

**Escape hatch.** With the solver stopped (it takes the ownership lock and refuses otherwise):

```bash
SOLVER_DATABASE_URL=… solver-bin cancel-unconfirmed
```

It closes every such order, sends each maker `OrderStatus CANCELLED` (`note_not_received`) for the ones it saw live, and prints how many lineages it closed and how many orders it cancelled per maker. A lineage whose order a settlement still holds, or its maker had cancelled, is closed with no order cancelled. Use it before a rollback, when the gateway must stay off, and after a testnet chain reset (the notes will never arrive). The solver refuses to start with the gateway off while such orders exist (orders their maker cancelled don't count).

**Alerts:**
- `solver_maker_unknown_notes_named_total` rising for one maker: its transactions reach the node after its `SubmitOrder`, or not at all.
- `solver_maker_cap_cancels_total` rising: notes that never arrive.
- `solver_executor_settlements_dropped_total{unconfirmed_maker_input="true"}` above 0: a maker transaction dropped from the mempool took our settlement with it.
- `solver_executor_settlement_conflicts_total{kind="unreadable"}` or `{kind="unknown_notes_unmatched"}` above 0: the node words its refusals differently; every unconfirmed order now waits. On every node upgrade, re-capture the text: start the node's local network and run `SOLVER_LOCAL_NODE_WRITE_FIXTURE=1 cargo test -p solver --test local_node_018 -- --ignored --test-threads=1`, which rewrites `tests/fixtures/node-0.18-refusals.json` for the parser's unit test.
- `solver_maker_watcher_block_read_age_seconds` above 10 for long: every order takes the confirmed path (`solver_maker_accept_fallbacks_total{reason="watcher_stale"}`).
- Several makers' unknown-note refusals at once: the node lost its mempool (a restart). Their orders wait, and return when their notes land or are capped.
- `solver_maker_watcher_unobserved_notes` above 0 for long: notes the watcher can't read. Unconfirmed ones are still decided by the cap's lookup; activated ones go unwatched for spends until they read again.

**Queries** join on root rows (`orders.note_id = maker_lineages.note_id`): unconfirmed orders are `maker_lineages` rows with `accept_block IS NOT NULL AND state = 'pending'`; `waiting` orders have `orders.status = 'waiting'`.

**After an asynchronous failover**, restart the solver as after any failover: the watcher re-reads every pending lineage, and the cap counts from the database clock.

## Self-trade prevention at submit

`engine.maker_cancel_crossing_on_submit` (off by default, [ADR 0009](adr/0009-self-trade-prevention-at-submit.md)) cancels the lower-seq order of a maker's crossing bid and ask on one pair when a `SubmitOrder` is accepted. It needs the maker gateway. The old rule (`engine.maker_self_trade_prevention`, ADR 0005) stays on beside it until the soak below passes.

**Before switch-on**, on each network:

1. Deploy this build with the switch **off** first (`migrate-db`, then `check-db`, which checks `SELECT` on the new view `eligible_lineages`; grant it if your default grants don't cover new views, see [Fresh deployment](#fresh-deployment) step 2) and run it for at least a day. This applies the storage migration while nothing depends on it, and gives the baseline in step 4.
2. Every maker has run the self-trade checks of the guide's section 15 against a test network with the switch **on**: the changes in the guide's "What changes for your client" list break strict clients.
3. Nothing the rule cannot see is open. The startup log line `maker orders self-trade prevention at submit cannot see` and the gauges `solver_maker_open_lineages_without_amounts` and `solver_maker_open_orders_without_maker_id` give the counts at startup (the gauges are absent until a count succeeds: no gauge is not 0). The query below lists the first kind at any time, per order. Orders claimed before the storage migration have no amounts and are never compared with any order, not even a new one. Expiry does not clear them (nothing retires an expired order), and neither does a note that never lands. What clears one: its maker's `CancelOrder`, a `CancelAll` with a higher seq covering it, a full fill, or a spend of its note that the watcher sees. Ask the maker to cancel what is left. An open order row without a maker should not exist after the migration (its backfill and the insert trigger cover every open row): a non-zero second count is a solver bug, so report it.

   ```sql
   SELECT l.maker_id, l.lineage_id, l.root_seq, l.state, l.expires_at_unix_ms
   FROM eligible_lineages l
   WHERE l.offered_amount IS NULL
     AND (l.state = 'pending' OR EXISTS (
          SELECT 1 FROM orders o
          WHERE o.lineage_id = l.lineage_id
            AND o.status IN ('active', 'settling', 'waiting')))
   ORDER BY l.maker_id, l.root_seq;
   ```
4. Take a baseline of `solver_executor_reservation_stopped_input_total` from step 1's run, as a daily increase: makers' own cancels racing the matcher count there too. Counters restart at zero with the process, so compare daily increases, never totals.
5. Tell the makers the switch-on date. Turn the switch on and restart. Once the restarted solver is ready (`/readyz`), take the database's clock as the switch-on time: `SELECT now();`. The queries below take it as `:switch_on` (`psql -v switch_on="'2026-12-01 10:00:07+00'"`). A time taken before the restart is too early: submits the old process accepted with the switch off would show as real misses in check 1. After a rollback, take the time the same way when the switch goes back on.

**Soak checks** (run daily; each joins orders to `maker_lineages` through `lineage_id`; plain `SELECT`s, so they run on a read-only session or a standby):

```sql
-- 1. Real misses: crossed pairs at the submitted prices where at least one
--    order is from after switch-on. MUST be 0 at all times. If not: keep the
--    rows, switch off and restart, and ask the maker to cancel the older order
--    of each pair; then report it (the rule has a bug).
WITH open_lineages AS (
    -- Not stopped or cut off, and pending, with an open row, or a parent
    -- retired while its settlement is unresolved: what the rule counts open.
    SELECT l.*, m.created_at
    FROM eligible_lineages l
    JOIN maker_lineages m USING (lineage_id)
    WHERE l.offered_amount IS NOT NULL
      AND ((l.state = 'pending' AND l.accept_block IS NULL)
           OR EXISTS (SELECT 1 FROM orders o
                      WHERE o.lineage_id = l.lineage_id
                        AND o.status IN ('active', 'settling', 'waiting'))
           OR EXISTS (SELECT 1 FROM settlement_inputs si
                      JOIN orders o ON o.note_id = si.parent_note_id
                      WHERE o.lineage_id = l.lineage_id)))
SELECT a.maker_id, a.lineage_id, a.root_seq, b.lineage_id, b.root_seq
FROM open_lineages a
JOIN open_lineages b
  ON b.maker_id = a.maker_id AND b.market = a.market AND b.direction > a.direction
WHERE a.offered_amount::numeric * b.offered_amount
      >= a.requested_amount::numeric * b.requested_amount
  AND greatest(a.created_at, b.created_at) >= :switch_on;

-- 2. Old pairs: the same, both orders from before switch-on. They drain as
--    the orders fill, expire or are cancelled; ask the maker to cancel any
--    that lingers. Same query, ending with:
--      AND greatest(a.created_at, b.created_at) < :switch_on;

-- 3. The old rule's stops on orders from after switch-on that a newer
--    opposite order, claimed while they were still open, crosses at the
--    submitted prices: should be 0 (the new rule cancels those first). The
--    old rule's other stops, for rounding, are not listed.
SELECT s.maker_id, s.lineage_id, s.created_at
FROM maker_stops s
JOIN maker_lineages x ON x.maker_id = s.maker_id AND x.lineage_id = s.lineage_id
WHERE s.source = 'stp_reservation' AND x.created_at >= :switch_on
  AND EXISTS (
      SELECT 1 FROM maker_lineages y
      WHERE y.maker_id = x.maker_id AND y.market = x.market
        AND y.direction <> x.direction AND y.root_seq > x.root_seq
        AND y.created_at < s.created_at
        AND x.offered_amount::numeric * y.offered_amount
            >= x.requested_amount::numeric * y.requested_amount);

-- 4. Stops by who wrote them, per maker per day.
SELECT maker_id, coalesce(source, '(before 2026-11-25)') AS source,
       date_trunc('day', created_at) AS day, count(*)
FROM maker_stops
GROUP BY 1, 2, 3
ORDER BY 3 DESC, 1, 2;
```

**Metrics to watch** (per maker, each appearing at its first increment, unless said otherwise):

- `solver_maker_stp_on_submit_cancels_total{loser="entering"}`: the maker's own new orders that lost to a newer crossing order, because they arrived after it or in the same round as it. Three causes: late retries, a seq counter that goes backwards, or two crossing orders sent back to back, which land in one round now and then and which the guide calls normal. So a single increase is only worth a look; contact the maker if it keeps rising, or rises with `failed_precondition` refusals. To be told of the first one: `increase(solver_maker_stp_on_submit_cancels_total{loser="entering"}[10m]) > 0 or (solver_maker_stp_on_submit_cancels_total{loser="entering"} unless solver_maker_stp_on_submit_cancels_total{loser="entering"} offset 10m)` (the second part catches a series that has just appeared, which `increase` reads as 0).
- `solver_maker_stp_on_submit_cancels_total{loser="older"}`: normal re-quoting. It includes orders a settlement held, which that settlement may still fill.
- `solver_maker_submits_dead_on_arrival_total{reason}`: submits the maker had already cancelled or cut off.
- `solver_gateway_requests_total{method="submit_order",code="failed_precondition"}`: submits refused because more than 1,000 of the maker's orders on the pair have a higher seq: a seq counter gone backwards, often a clock that is behind after a restart. Tell the maker.
- `solver_maker_submits_near_expiry_total`: counted whatever the switch. A high share means the maker sends expiries of 30 s or less: tell them. Such a submit never trades, but with the switch on it still cancels the maker's older crossing orders.
- `solver_maker_stp_newer_rows_total`: orders of the maker read because they have a higher seq than a submit's. A steady trickle is normal with several senders or retries.
- `solver_executor_reservation_stopped_input_total` (global, always shown): compare its daily increase with the baseline from step 4; a few more a day is expected.

**Rollback:** set the switch to `false` and restart. Never delete `maker_stops` rows: they are cancels the makers were told about. Reverting the storage migration as well (`revert-db`) means running a build from before it, and drops the stored amounts and the stops' `source` and `caused_by_lineage` (checks 3 and 4 lose their history): orders claimed before it is applied again count as pre-migration, and step 3 above must pass again.

**The storage migration** takes `ACCESS EXCLUSIVE` on `orders`, `maker_lineages` and `maker_stops` until it commits: new columns with a CHECK scan their table, three indexes are built without `CONCURRENTLY`, and the open order rows get their maker in one `UPDATE`. It turns its own statement timeout off. With the solver stopped, as for every migration here, it takes seconds on a large `orders`; to size it first, time `migrate-db` on a restored copy. Its insert trigger, which copies the maker onto new order rows, is pinned to the schema the migration ran in: after restoring into a schema with another name, or renaming it, run `ALTER FUNCTION <schema>.orders_fill_maker_id() SET search_path = <schema>`, or every order insert fails.

**Removing the old rule** (one change, after the soak), on every network that will run that build, since switch-on: checks 1, 2 and 3 return no rows, both startup counts are 0, and rounding on nearly filled remainders stays a known limitation ([#93](https://github.com/inicio-labs/solver/issues/93)). The change removes ADR 0005's matcher, executor and reservation code and its switch (`engine.maker_self_trade_prevention`), makes the rule at submit always on (no `engine.maker_cancel_crossing_on_submit`), and turns check 1 into a periodic metric.

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

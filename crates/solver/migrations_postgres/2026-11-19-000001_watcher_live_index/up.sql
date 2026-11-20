-- Fail fast instead of waiting behind a long lock holder (for example an
-- anti-wraparound vacuum, which does not cancel itself for DDL). Diesel runs
-- each migration in one transaction, so this ends with it.
SET LOCAL lock_timeout = '30s';

-- The maker watcher reads every Active or Stopped maker order each round
-- (`watched_maker_orders_tx`, once a second). Without this index that read
-- scans all of `orders`, which keeps every order ever seen. The index holds
-- only the orders the watcher reads, so its entries follow how many orders
-- are live now, not how many have ever existed.
CREATE INDEX orders_live_status_idx ON orders (lineage_id)
    WHERE status IN ('active', 'stopped');

-- An order leaving Active or Stopped leaves a dead entry in this index (and
-- every status change a dead row version) until vacuum removes it. With the
-- default trigger (20% of the table dead) a table of filled history waits
-- about a day between vacuums, so vacuum at 1% + 1000 dead rows instead.
-- Since PostgreSQL 14 vacuum skips the index pass when under 2% of the
-- table's pages hold dead rows, which a 1% trigger usually is: force the
-- index pass, or the dead entries stay until a later, larger run. Log every
-- run, so the server log shows each one's duration and pages.
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    vacuum_index_cleanup = on,
    log_autovacuum_min_duration = 0
);

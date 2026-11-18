-- The maker watcher reads every Active or Stopped maker order each round
-- (`watched_maker_orders_tx`, once a second). Without this index that read
-- scans all of `orders`, which keeps every order ever seen. The index holds
-- only the orders the watcher reads, so its size follows how many orders
-- are live now, not how many have ever existed.
CREATE INDEX orders_live_status_idx ON orders (lineage_id)
    WHERE status IN ('active', 'stopped');

-- Every status change leaves a dead row version and a dead entry in the
-- partial indexes until vacuum. With the default trigger (20% of the table
-- dead) a table of filled history waits about a day between vacuums. Vacuum
-- at 1% + 1000 dead rows instead, and log each run.
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    log_autovacuum_min_duration = 0
);

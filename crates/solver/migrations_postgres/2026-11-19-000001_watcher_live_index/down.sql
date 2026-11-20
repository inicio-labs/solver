SET LOCAL lock_timeout = '30s';

-- Resets only the settings the up migration set; any other per-table
-- setting stays as it is.
ALTER TABLE orders RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    vacuum_index_cleanup,
    log_autovacuum_min_duration
);
DROP INDEX orders_live_status_idx;

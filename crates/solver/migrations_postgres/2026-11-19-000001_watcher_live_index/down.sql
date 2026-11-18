ALTER TABLE orders RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    log_autovacuum_min_duration
);
DROP INDEX orders_live_status_idx;

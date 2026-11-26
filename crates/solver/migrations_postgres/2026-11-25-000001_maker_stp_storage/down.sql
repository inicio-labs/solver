SET LOCAL lock_timeout = '30s';

-- The previous binary reads none of these. Re-applying the migration
-- needs the grant on `eligible_lineages` again.
DROP VIEW eligible_lineages;
ALTER TABLE maker_stops
    DROP CONSTRAINT maker_stops_cause_is_self_trade,
    DROP COLUMN caused_by_lineage,
    DROP COLUMN source;
DROP INDEX orders_maker_open_idx;
DROP TRIGGER orders_fill_maker_id ON orders;
DROP FUNCTION orders_fill_maker_id();
ALTER TABLE orders DROP COLUMN maker_id;
ALTER TABLE maker_lineages RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    vacuum_index_cleanup,
    log_autovacuum_min_duration
);
DROP INDEX maker_lineages_pending_group_idx;
DROP INDEX maker_lineages_seq_idx;
ALTER TABLE maker_lineages
    DROP CONSTRAINT maker_lineages_amounts_together,
    DROP COLUMN requested_amount,
    DROP COLUMN offered_amount;

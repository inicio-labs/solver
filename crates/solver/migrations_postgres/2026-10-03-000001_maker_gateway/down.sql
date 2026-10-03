-- The previous binary knows nothing of makers or cutoffs: reverting after any
-- maker activity would let it trade orders that makers were told are cancelled.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM maker_lineages) OR EXISTS (SELECT 1 FROM orders WHERE status = 'stopped') THEN
        RAISE EXCEPTION 'maker orders exist; reverting this migration would revive cancelled orders';
    END IF;
END $$;

DROP VIEW live_orders;
DROP TABLE maker_events;
DROP TABLE maker_stops;
DROP TABLE maker_cutoffs;
DROP TABLE maker_lineages;
DROP TABLE maker_commands;
DROP TABLE api_keys;
DROP TABLE makers;
DROP INDEX orders_lineage_idx;
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('active', 'settling', 'executed', 'onchain_nullified'));
ALTER TABLE orders
    DROP COLUMN direction,
    DROP COLUMN market,
    DROP COLUMN depth,
    DROP COLUMN lineage_id;

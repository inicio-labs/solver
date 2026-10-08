-- The previous binary knows nothing of makers or cutoffs: reverting after any
-- maker command would drop acknowledged claims, cutoffs and stops, and let it
-- trade orders that makers were told are cancelled. A cancel can come before
-- any submit, so checking claims alone is not enough. Every claim, cutoff and
-- stop is written by a stored command, so this one check covers them all.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM maker_commands) THEN
        RAISE EXCEPTION 'maker commands exist; reverting this migration would lose acknowledged maker commands, including cancels';
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

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM orders WHERE status = 'waiting')
    OR EXISTS (SELECT 1 FROM maker_lineages WHERE live_at_accept AND state = 'pending') THEN
 RAISE EXCEPTION 'maker orders that went live at accept are unresolved; stop the solver and run the escape hatch first';
 END IF;
END $$;
DROP VIEW live_orders;
DROP VIEW eligible_orders;
CREATE VIEW live_orders AS
SELECT o.note_id, o.raw_data, o.arrival_unix, o.status, o.priority_seq,
       o.lineage_id, o.depth, o.market, o.direction,
       m.maker_id, m.root_seq, m.expires_at_unix_ms
FROM orders o
LEFT JOIN maker_lineages m ON m.lineage_id = o.lineage_id
WHERE o.status = 'active'
  AND NOT EXISTS (
      SELECT 1 FROM maker_cutoffs c
      WHERE c.maker_id = m.maker_id
        AND m.root_seq < c.cutoff
        AND (c.market = '' OR c.market = o.market)
        AND (c.direction = '' OR c.direction = o.direction))
  AND NOT EXISTS (
      SELECT 1 FROM maker_stops s
      WHERE s.maker_id = m.maker_id AND s.lineage_id = o.lineage_id);

ALTER TABLE maker_lineages
    DROP CONSTRAINT maker_lineages_live_at_accept_check,
    DROP COLUMN live_at_accept,
    DROP COLUMN accept_block,
    DROP COLUMN activated_at;
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('active', 'settling', 'executed', 'onchain_nullified', 'stopped'));

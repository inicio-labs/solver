DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM maker_lineages WHERE expires_at_unix_ms IS NOT NULL) THEN
 RAISE EXCEPTION 'maker expiry exists; rollback would lose acknowledged selection deadlines';
 END IF;
END $$;
DROP VIEW live_orders;
CREATE VIEW live_orders AS
SELECT o.note_id, o.raw_data, o.arrival_unix, o.status, o.priority_seq,
       o.lineage_id, o.depth, o.market, o.direction,
       m.maker_id, m.root_seq
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

ALTER TABLE maker_lineages DROP COLUMN expires_at_unix_ms;

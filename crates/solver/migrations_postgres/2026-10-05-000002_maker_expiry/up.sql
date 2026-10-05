-- Expiry is inherited metadata, checked before matcher selection only.
-- Do not filter this view by time: already selected work may reserve later.
ALTER TABLE maker_lineages ADD COLUMN expires_at_unix_ms BIGINT CHECK (expires_at_unix_ms > 0);
CREATE OR REPLACE VIEW live_orders AS
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

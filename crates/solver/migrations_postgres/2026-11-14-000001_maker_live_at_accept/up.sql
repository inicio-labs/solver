-- Maker orders that go live when the gateway accepts them, before their note
-- is in a block (ADR 0008). A `waiting` order is out of the book until its
-- note lands; the lineage records that it went live at accept and when.
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('active', 'settling', 'executed', 'onchain_nullified', 'stopped', 'waiting'));

ALTER TABLE maker_lineages
    ADD COLUMN live_at_accept BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN accept_block BIGINT CHECK (accept_block >= 0),
    ADD COLUMN activated_at TIMESTAMPTZ,
    ADD CONSTRAINT maker_lineages_live_at_accept_check
        CHECK (NOT live_at_accept OR accept_block IS NOT NULL);

-- Every order row the maker's cutoffs and stops admit, whatever its status.
-- `live_orders` is the active part; a `waiting` maker order is eligible but
-- not live.
CREATE VIEW eligible_orders AS
SELECT o.note_id, o.raw_data, o.arrival_unix, o.status, o.priority_seq,
       o.lineage_id, o.depth, o.market, o.direction,
       m.maker_id, m.root_seq, m.expires_at_unix_ms
FROM orders o
LEFT JOIN maker_lineages m ON m.lineage_id = o.lineage_id
WHERE NOT EXISTS (
      SELECT 1 FROM maker_cutoffs c
      WHERE c.maker_id = m.maker_id
        AND m.root_seq < c.cutoff
        AND (c.market = '' OR c.market = o.market)
        AND (c.direction = '' OR c.direction = o.direction))
  AND NOT EXISTS (
      SELECT 1 FROM maker_stops s
      WHERE s.maker_id = m.maker_id AND s.lineage_id = o.lineage_id);

CREATE OR REPLACE VIEW live_orders AS
SELECT note_id, raw_data, arrival_unix, status, priority_seq,
       lineage_id, depth, market, direction,
       maker_id, root_seq, expires_at_unix_ms
FROM eligible_orders
WHERE status = 'active';

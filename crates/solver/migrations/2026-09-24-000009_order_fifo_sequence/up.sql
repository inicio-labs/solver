-- Persist ingestion order independently of wall-clock timestamps and SELECT
-- row order. Existing rows inherit their insertion order; subsequent inserts
-- allocate above the highest sequence ever issued, even if orders are later
-- deleted or SQLite reuses a rowid.
ALTER TABLE orders ADD COLUMN priority_seq BIGINT NOT NULL DEFAULT 0;
UPDATE orders SET priority_seq = rowid;
CREATE UNIQUE INDEX orders_priority_seq_unique ON orders(priority_seq);
CREATE TABLE order_priority_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq BIGINT NOT NULL CHECK (last_seq >= 0)
);
INSERT INTO order_priority_counter (id, last_seq)
SELECT 1, COALESCE(MAX(priority_seq), 0) FROM orders;
CREATE TRIGGER orders_assign_priority_seq
AFTER INSERT ON orders
BEGIN
    UPDATE order_priority_counter SET last_seq = last_seq + 1 WHERE id = 1;
    UPDATE orders
       SET priority_seq = (SELECT last_seq FROM order_priority_counter WHERE id = 1)
     WHERE rowid = NEW.rowid;
END;

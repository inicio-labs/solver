-- Extend the orders.status CHECK constraint to include 'onchain_nullified'
-- so the solver can mark orders whose note has been consumed on-chain
-- (by another party or by a previous attempt that we lost track of).
--
-- SQLite doesn't support ALTER TABLE to modify a CHECK constraint, so we
-- rebuild the table: create new with the new constraint, copy data, drop
-- old, rename new. PRAGMAs disable foreign-key cascades during the rebuild
-- (we don't use foreign keys here, but it's the standard safe pattern).

PRAGMA foreign_keys=OFF;

CREATE TABLE orders_new (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    requested_asset BLOB NOT NULL,
    requested_amount BIGINT NOT NULL,
    offered_asset BLOB NOT NULL,
    offered_amount BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'settling', 'executed', 'onchain_nullified'))
);

INSERT INTO orders_new (
    note_id, account_id, requested_asset, requested_amount,
    offered_asset, offered_amount, timestamp, status
)
SELECT
    note_id, account_id, requested_asset, requested_amount,
    offered_asset, offered_amount, timestamp, status
FROM orders;

DROP TABLE orders;
ALTER TABLE orders_new RENAME TO orders;

PRAGMA foreign_keys=ON;

-- Revert CHECK constraint to the original 3-value form. Rows currently
-- in 'onchain_nullified' must be migrated to a value the old constraint
-- allows; we choose 'executed' (terminal, irreversible — matches the
-- "this order is done from the matcher's perspective" semantics).
--
-- WARNING: this loses the distinction between "we executed it" and
-- "someone else consumed it." Acceptable for a down-migration.

PRAGMA foreign_keys=OFF;

UPDATE orders SET status = 'executed' WHERE status = 'onchain_nullified';

CREATE TABLE orders_new (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    requested_asset BLOB NOT NULL,
    requested_amount BIGINT NOT NULL,
    offered_asset BLOB NOT NULL,
    offered_amount BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'settling', 'executed'))
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

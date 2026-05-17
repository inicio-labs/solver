CREATE TABLE orders (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    requested_asset BLOB NOT NULL,
    requested_amount BIGINT NOT NULL,
    offered_asset BLOB NOT NULL,
    offered_amount BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'settling', 'executed'))
);

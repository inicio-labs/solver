-- Fresh solver database schema. Safe to run again when the service restarts.
BEGIN;

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_fetched_block BIGINT NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO sync_state (id, last_fetched_block) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS notes (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    raw_data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    requested_asset BLOB NOT NULL,
    requested_amount BIGINT NOT NULL,
    offered_asset BLOB NOT NULL,
    offered_amount BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'settling', 'executed', 'onchain_nullified')),
    priority_seq BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS orders_priority_seq_index ON orders (priority_seq);

CREATE TABLE IF NOT EXISTS order_priority_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq BIGINT NOT NULL CHECK (last_seq >= 0)
);
INSERT OR IGNORE INTO order_priority_counter (id, last_seq) VALUES (1, 0);

-- The ingestion sequence is durable and does not depend on wall-clock time
-- or SQLite rowid reuse after an order is deleted.
CREATE TRIGGER IF NOT EXISTS orders_assign_priority_seq
AFTER INSERT ON orders
WHEN NEW.priority_seq = 0
BEGIN
    UPDATE order_priority_counter SET last_seq = last_seq + 1 WHERE id = 1;
    UPDATE orders
       SET priority_seq = (SELECT last_seq FROM order_priority_counter WHERE id = 1)
     WHERE rowid = NEW.rowid;
END;

-- The executor records the exact locally executed transaction after proving,
-- but before submitting it. A child note can then be recognized even if ingestion sees it
-- before the executor's confirmation poll.
CREATE TABLE IF NOT EXISTS settlement_attempts (
    tx_id BLOB PRIMARY KEY,
    tx_result BLOB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'submitted', 'uncertain', 'rejected', 'confirmed'))
);

CREATE TABLE IF NOT EXISTS settlement_inputs (
    tx_id BLOB NOT NULL REFERENCES settlement_attempts(tx_id),
    parent_note_id BLOB NOT NULL REFERENCES orders(note_id),
    payback_note_id BLOB NOT NULL,
    child_note_id BLOB,
    child_note_data BLOB,
    PRIMARY KEY (tx_id, parent_note_id),
    CHECK ((child_note_id IS NULL) = (child_note_data IS NULL))
);
CREATE INDEX IF NOT EXISTS settlement_inputs_child_idx ON settlement_inputs(child_note_id);
CREATE INDEX IF NOT EXISTS settlement_inputs_parent_idx ON settlement_inputs(parent_note_id);

CREATE TABLE IF NOT EXISTS generated_notes (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    source_note_a BLOB NOT NULL,
    source_note_b BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS registered_tokens (
    token_id BLOB PRIMARY KEY,
    created_at BIGINT NOT NULL,
    external_symbol TEXT,
    decimals INTEGER,
    ticker TEXT
);

COMMIT;

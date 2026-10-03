-- Market-maker gateway store (ADR 0003). Orders gain the keys that identify
-- them across fills and markets; makers get their keys, commands, lineage
-- claims, cancellation barriers and event feed.

-- Columns computed from the note by whichever path inserts the row. Rows
-- written before this migration keep NULL and are never maker orders.
ALTER TABLE orders
    ADD COLUMN lineage_id BYTEA,
    ADD COLUMN depth BIGINT CHECK (depth >= 0),
    ADD COLUMN market BYTEA,
    ADD COLUMN direction BYTEA;
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('active', 'settling', 'executed', 'onchain_nullified', 'stopped'));
CREATE INDEX orders_lineage_idx ON orders (lineage_id) WHERE lineage_id IS NOT NULL;

-- The maker control row. A cancel and a reservation of the maker's orders both
-- lock it (FOR NO KEY UPDATE), so the two serialize per maker; foreign-key
-- checks take only KEY SHARE and never wait on it. `next_event_seq` hands out
-- the maker's contiguous event sequence.
CREATE TABLE makers (
    maker_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (name <> ''),
    next_event_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_event_seq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only a hash of each key is stored; the key itself is shown once.
CREATE TABLE api_keys (
    key_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    maker_id BIGINT NOT NULL REFERENCES makers (maker_id),
    key_hash BYTEA NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

-- Every maker command with its stored reply, for idempotent retries.
CREATE TABLE maker_commands (
    maker_id BIGINT NOT NULL REFERENCES makers (maker_id),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    seq BIGINT NOT NULL CHECK (seq > 0),
    kind TEXT NOT NULL CHECK (kind IN ('submit', 'cancel_all', 'cancel_order')),
    payload BYTEA NOT NULL,
    result TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (maker_id, request_id),
    UNIQUE (maker_id, seq)
);

-- One claim per order lineage (creator plus root serial). Orders join it by
-- `lineage_id`, so a remainder inherits the maker without copying, and either
-- the claim or the order row may be written first. `note_id` is the submitted
-- note; `state` tracks its chain verification by the maker-note watcher.
CREATE TABLE maker_lineages (
    lineage_id BYTEA PRIMARY KEY,
    maker_id BIGINT NOT NULL,
    request_id TEXT NOT NULL,
    root_seq BIGINT NOT NULL CHECK (root_seq > 0),
    note_id BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'activated', 'rejected')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (maker_id, request_id) REFERENCES maker_commands (maker_id, request_id)
);
CREATE INDEX maker_lineages_pending_idx ON maker_lineages (created_at) WHERE state = 'pending';

-- Cancel-all barriers: orders whose root sequence is below `cutoff` are
-- stopped. An empty market or direction means every market or both directions.
CREATE TABLE maker_cutoffs (
    maker_id BIGINT NOT NULL REFERENCES makers (maker_id),
    market BYTEA NOT NULL DEFAULT '',
    direction BYTEA NOT NULL DEFAULT '',
    cutoff BIGINT NOT NULL CHECK (cutoff > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (maker_id, market, direction)
);

-- Targeted cancels: one stopped lineage each. It may precede the submit.
CREATE TABLE maker_stops (
    maker_id BIGINT NOT NULL REFERENCES makers (maker_id),
    lineage_id BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (maker_id, lineage_id)
);

-- The maker event feed. V1 deletes no events; `created_at` allows expiry later.
CREATE TABLE maker_events (
    maker_id BIGINT NOT NULL REFERENCES makers (maker_id),
    event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    event_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL
        CHECK (kind IN ('order_status', 'settlement_pending', 'settlement_resolved')),
    lineage_id BYTEA,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (maker_id, event_seq)
);

-- The one liveness rule: active, and not stopped by an applicable cutoff or a
-- targeted stop. Orders without a maker claim (public orders) pass through.
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

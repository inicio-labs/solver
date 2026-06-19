CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    last_fetched_block BIGINT NOT NULL DEFAULT 0
);

INSERT INTO sync_state (id, last_fetched_block) VALUES (1, 0);

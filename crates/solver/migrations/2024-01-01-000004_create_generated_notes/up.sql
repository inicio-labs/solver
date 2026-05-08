CREATE TABLE generated_notes (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    source_note_a BLOB NOT NULL,
    source_note_b BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at BIGINT NOT NULL
);

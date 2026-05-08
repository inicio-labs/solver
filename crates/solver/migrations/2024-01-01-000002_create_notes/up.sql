CREATE TABLE notes (
    note_id BLOB PRIMARY KEY,
    account_id BLOB NOT NULL,
    raw_data BLOB NOT NULL
);

diesel::table! {
    sync_state (id) {
        id -> Integer,
        last_fetched_block -> BigInt,
    }
}

diesel::table! {
    notes (note_id) {
        note_id -> Binary,
        account_id -> Binary,
        raw_data -> Binary,
    }
}

diesel::table! {
    orders (note_id) {
        note_id -> Binary,
        account_id -> Binary,
        requested_asset -> Binary,
        requested_amount -> BigInt,
        offered_asset -> Binary,
        offered_amount -> BigInt,
        timestamp -> BigInt,
        status -> Text,
    }
}

diesel::table! {
    generated_notes (note_id) {
        note_id -> Binary,
        account_id -> Binary,
        source_note_a -> Binary,
        source_note_b -> Binary,
        data -> Binary,
        created_at -> BigInt,
    }
}

diesel::table! {
    registered_tokens (id) {
        id -> Integer,
        token_id -> Binary,
        created_at -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(sync_state, notes, orders, generated_notes, registered_tokens);

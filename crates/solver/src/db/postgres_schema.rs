//! Diesel mapping for the PostgreSQL application schema. Keep this in sync
//! with `migrations_postgres`.

diesel::table! {
    sync_state (id) {
        id -> SmallInt,
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
        arrival_unix -> BigInt,
        status -> Text,
        priority_seq -> BigInt,
    }
}

diesel::table! {
    settlement_attempts (tx_id) {
        tx_id -> Binary,
        tx_result -> Binary,
        status -> Text,
        created_at_unix -> BigInt,
    }
}

diesel::table! {
    settlement_inputs (tx_id, parent_note_id) {
        tx_id -> Binary,
        parent_note_id -> Binary,
        payback_note_id -> Binary,
        child_note_id -> Nullable<Binary>,
        child_note_data -> Nullable<Binary>,
    }
}

diesel::table! {
    registered_tokens (token_id) {
        token_id -> Binary,
        created_at_unix -> BigInt,
        external_symbol -> Nullable<Text>,
        decimals -> Nullable<Integer>,
        ticker -> Nullable<Text>,
    }
}

diesel::joinable!(orders -> notes (note_id));
diesel::joinable!(settlement_inputs -> orders (parent_note_id));
diesel::joinable!(settlement_inputs -> settlement_attempts (tx_id));

diesel::allow_tables_to_appear_in_same_query!(
    sync_state,
    notes,
    orders,
    settlement_attempts,
    settlement_inputs,
    registered_tokens,
);

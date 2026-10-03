//! Diesel mapping for the PostgreSQL application schema. Keep this in sync
//! with `migrations_postgres`.

diesel::table! {
    sync_state (id) {
        id -> SmallInt,
        last_fetched_block -> BigInt,
        owner_epoch -> BigInt,
    }
}

diesel::table! {
    orders (note_id) {
        note_id -> Binary,
        raw_data -> Binary,
        arrival_unix -> BigInt,
        status -> Text,
        priority_seq -> BigInt,
        lineage_id -> Nullable<Binary>,
        depth -> Nullable<BigInt>,
        market -> Nullable<Binary>,
        direction -> Nullable<Binary>,
    }
}

diesel::table! {
    settlement_attempts (tx_id) {
        tx_id -> Binary,
        tx_result -> Binary,
        status -> Text,
    }
}

diesel::table! {
    settlement_inputs (tx_id, parent_note_id) {
        tx_id -> Binary,
        parent_note_id -> Binary,
        child_note_id -> Nullable<Binary>,
        child_note_data -> Nullable<Binary>,
    }
}

diesel::table! {
    registered_tokens (token_id) {
        token_id -> Binary,
        external_symbol -> Nullable<Text>,
        decimals -> Nullable<Integer>,
        ticker -> Nullable<Text>,
    }
}

diesel::table! {
    makers (maker_id) {
        maker_id -> BigInt,
        name -> Text,
        next_event_seq -> BigInt,
    }
}

diesel::table! {
    api_keys (key_id) {
        key_id -> BigInt,
        maker_id -> BigInt,
        key_hash -> Binary,
        revoked_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    maker_commands (maker_id, request_id) {
        maker_id -> BigInt,
        request_id -> Text,
        seq -> BigInt,
        kind -> Text,
        payload -> Binary,
        result -> Text,
    }
}

diesel::table! {
    maker_lineages (lineage_id) {
        lineage_id -> Binary,
        maker_id -> BigInt,
        request_id -> Text,
        root_seq -> BigInt,
        note_id -> Binary,
        state -> Text,
    }
}

diesel::table! {
    maker_cutoffs (maker_id, market, direction) {
        maker_id -> BigInt,
        market -> Binary,
        direction -> Binary,
        cutoff -> BigInt,
    }
}

diesel::table! {
    maker_stops (maker_id, lineage_id) {
        maker_id -> BigInt,
        lineage_id -> Binary,
    }
}

// `event_id` (a UUID with a database default) is left unmapped: diesel is
// built without its uuid feature, and the gateway reads it as text.
diesel::table! {
    maker_events (maker_id, event_seq) {
        maker_id -> BigInt,
        event_seq -> BigInt,
        kind -> Text,
        lineage_id -> Nullable<Binary>,
        payload -> Text,
    }
}

// A view: the one liveness rule (ADR 0003). Read-only.
diesel::table! {
    live_orders (note_id) {
        note_id -> Binary,
        raw_data -> Binary,
        arrival_unix -> BigInt,
        status -> Text,
        priority_seq -> BigInt,
        lineage_id -> Nullable<Binary>,
        depth -> Nullable<BigInt>,
        market -> Nullable<Binary>,
        direction -> Nullable<Binary>,
        maker_id -> Nullable<BigInt>,
        root_seq -> Nullable<BigInt>,
    }
}

diesel::joinable!(settlement_inputs -> orders (parent_note_id));
diesel::joinable!(api_keys -> makers (maker_id));
diesel::joinable!(settlement_inputs -> settlement_attempts (tx_id));

diesel::allow_tables_to_appear_in_same_query!(
    sync_state,
    orders,
    settlement_attempts,
    settlement_inputs,
    registered_tokens,
    makers,
    api_keys,
    maker_commands,
    maker_lineages,
    maker_cutoffs,
    maker_stops,
    maker_events,
    live_orders,
);

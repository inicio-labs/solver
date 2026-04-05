pub mod models;
pub mod schema;

use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;

use crate::types::OrderStatus;
use self::models::*;
use self::schema::*;

pub type DbPool = r2d2::Pool<ConnectionManager<SqliteConnection>>;

/// Initialize the database: create pool, run migrations, seed sync_state.
pub fn init_db(database_url: &str) -> Result<DbPool> {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = r2d2::Pool::builder().max_size(1).build(manager)?;

    let mut conn = pool.get()?;
    run_migrations(&mut conn)?;

    Ok(pool)
}

/// Run SQL migrations to create tables.
fn run_migrations(conn: &mut SqliteConnection) -> Result<()> {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            last_fetched_block BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(conn)?;

    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS notes (
            note_id BLOB PRIMARY KEY,
            account_id BLOB NOT NULL,
            raw_data BLOB NOT NULL
        )",
    )
    .execute(conn)?;

    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS orders (
            note_id BLOB PRIMARY KEY,
            account_id BLOB NOT NULL,
            requested_asset BLOB NOT NULL,
            requested_amount BIGINT NOT NULL,
            offered_asset BLOB NOT NULL,
            offered_amount BIGINT NOT NULL,
            timestamp BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'in_flight', 'executed'))
        )",
    )
    .execute(conn)?;

    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS generated_notes (
            note_id BLOB PRIMARY KEY,
            account_id BLOB NOT NULL,
            source_note_a BLOB NOT NULL,
            source_note_b BLOB NOT NULL,
            data BLOB NOT NULL,
            created_at BIGINT NOT NULL
        )",
    )
    .execute(conn)?;

    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS registered_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id BLOB NOT NULL UNIQUE,
            created_at BIGINT NOT NULL
        )",
    )
    .execute(conn)?;

    diesel::sql_query(
        "INSERT OR IGNORE INTO sync_state (id, last_fetched_block) VALUES (1, 0)",
    )
    .execute(conn)?;

    Ok(())
}

// ── Query Functions ─────────────────────────────────────────────────────────

/// Get the last fetched block number.
pub fn get_last_fetched_block(conn: &mut SqliteConnection) -> Result<u64> {
    let state = sync_state::table
        .find(1)
        .select(SyncState::as_select())
        .first(conn)?;
    Ok(state.last_fetched_block as u64)
}

/// Atomic batch insert: notes + orders + update block number.
/// Uses INSERT OR IGNORE for notes to handle idempotent re-fetches.
pub fn insert_notes_batch(
    conn: &mut SqliteConnection,
    new_notes: &[NoteRow],
    new_orders: &[OrderRow],
    block_number: u64,
) -> Result<()> {
    conn.transaction(|conn| {
        for note in new_notes {
            diesel::insert_or_ignore_into(notes::table)
                .values(note)
                .execute(conn)?;
        }

        for order in new_orders {
            diesel::insert_or_ignore_into(orders::table)
                .values(order)
                .execute(conn)?;
        }

        diesel::update(sync_state::table.find(1))
            .set(sync_state::last_fetched_block.eq(block_number as i64))
            .execute(conn)?;

        Ok(())
    })
}

/// Get all active orders.
pub fn get_active_orders(conn: &mut SqliteConnection) -> Result<Vec<OrderRow>> {
    let results = orders::table
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(OrderRow::as_select())
        .load(conn)?;
    Ok(results)
}

/// Update an order's status by note_id.
pub fn update_order_status(
    conn: &mut SqliteConnection,
    note_id: &[u8],
    status: OrderStatus,
) -> Result<()> {
    diesel::update(orders::table.find(note_id))
        .set(orders::status.eq(status.as_str()))
        .execute(conn)?;
    Ok(())
}

/// Atomic trade execution: insert generated note + mark both orders as executed.
pub fn execute_trade_atomic(
    conn: &mut SqliteConnection,
    generated_note: &GeneratedNoteRow,
    source_note_a: &[u8],
    source_note_b: &[u8],
) -> Result<()> {
    conn.transaction(|conn| {
        diesel::insert_into(generated_notes::table)
            .values(generated_note)
            .execute(conn)?;

        diesel::update(orders::table.find(source_note_a))
            .set(orders::status.eq(OrderStatus::Executed.as_str()))
            .execute(conn)?;

        diesel::update(orders::table.find(source_note_b))
            .set(orders::status.eq(OrderStatus::Executed.as_str()))
            .execute(conn)?;

        Ok(())
    })
}

/// Reset orders back to active (for execution failures).
pub fn reset_orders_to_active(conn: &mut SqliteConnection, note_ids: &[Vec<u8>]) -> Result<()> {
    for note_id in note_ids {
        diesel::update(orders::table.find(note_id))
            .set(orders::status.eq(OrderStatus::Active.as_str()))
            .execute(conn)?;
    }
    Ok(())
}

// ── Registered Tokens ───────────────────────────────────────────────────────

/// Get all registered tokens.
pub fn get_registered_tokens(conn: &mut SqliteConnection) -> Result<Vec<RegisteredTokenRow>> {
    let results = registered_tokens::table
        .select(RegisteredTokenRow::as_select())
        .load(conn)?;
    Ok(results)
}

/// Register a new token. Returns true if inserted, false if already exists.
pub fn register_token(conn: &mut SqliteConnection, token_id: &[u8]) -> Result<bool> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let inserted = diesel::insert_or_ignore_into(registered_tokens::table)
        .values(&NewRegisteredToken {
            token_id: token_id.to_vec(),
            created_at: now,
        })
        .execute(conn)?;

    Ok(inserted > 0)
}

/// Remove a registered token. Returns true if deleted.
pub fn unregister_token(conn: &mut SqliteConnection, token_id: &[u8]) -> Result<bool> {
    let deleted = diesel::delete(
        registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)),
    )
    .execute(conn)?;

    Ok(deleted > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> DbPool {
        init_db(":memory:").expect("failed to create test DB")
    }

    #[test]
    fn test_init_and_sync_state() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();
        let block = get_last_fetched_block(&mut conn).unwrap();
        assert_eq!(block, 0);
    }

    #[test]
    fn test_insert_notes_batch() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();

        let note = NoteRow {
            note_id: vec![1, 2, 3],
            account_id: vec![4, 5, 6],
            raw_data: vec![7, 8, 9],
        };

        let order = OrderRow {
            note_id: vec![1, 2, 3],
            account_id: vec![4, 5, 6],
            requested_asset: vec![10, 11],
            requested_amount: 500,
            offered_asset: vec![20, 21],
            offered_amount: 1000,
            timestamp: 1000,
            status: OrderStatus::Active.as_str().to_string(),
        };

        insert_notes_batch(&mut conn, &[note], &[order], 42).unwrap();

        let block = get_last_fetched_block(&mut conn).unwrap();
        assert_eq!(block, 42);

        let active = get_active_orders(&mut conn).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].offered_amount, 1000);
    }

    #[test]
    fn test_update_order_status() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();

        let note = NoteRow {
            note_id: vec![1],
            account_id: vec![2],
            raw_data: vec![3],
        };
        let order = OrderRow {
            note_id: vec![1],
            account_id: vec![2],
            requested_asset: vec![10],
            requested_amount: 100,
            offered_asset: vec![20],
            offered_amount: 200,
            timestamp: 100,
            status: OrderStatus::Active.as_str().to_string(),
        };

        insert_notes_batch(&mut conn, &[note], &[order], 1).unwrap();

        let active = get_active_orders(&mut conn).unwrap();
        assert_eq!(active.len(), 1);

        update_order_status(&mut conn, &active[0].note_id, OrderStatus::InFlight).unwrap();

        let active_after = get_active_orders(&mut conn).unwrap();
        assert_eq!(active_after.len(), 0);
    }

    #[test]
    fn test_idempotent_note_insert() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();

        let note = NoteRow {
            note_id: vec![1, 2, 3],
            account_id: vec![4],
            raw_data: vec![5],
        };

        insert_notes_batch(&mut conn, &[note.clone()], &[], 1).unwrap();

        let note2 = NoteRow {
            note_id: vec![1, 2, 3],
            account_id: vec![4],
            raw_data: vec![99],
        };
        insert_notes_batch(&mut conn, &[note2], &[], 2).unwrap();

        let block = get_last_fetched_block(&mut conn).unwrap();
        assert_eq!(block, 2);
    }

    #[test]
    fn test_register_and_list_tokens() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();

        let token_a = vec![1, 2, 3, 4];
        let token_b = vec![5, 6, 7, 8];

        assert!(register_token(&mut conn, &token_a).unwrap());
        assert!(register_token(&mut conn, &token_b).unwrap());

        let tokens = get_registered_tokens(&mut conn).unwrap();
        assert_eq!(tokens.len(), 2);

        assert!(!register_token(&mut conn, &token_a).unwrap());
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 2);
    }

    #[test]
    fn test_unregister_token() {
        let pool = test_pool();
        let mut conn = pool.get().unwrap();

        let token = vec![10, 20];

        register_token(&mut conn, &token).unwrap();
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 1);

        assert!(unregister_token(&mut conn, &token).unwrap());
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 0);

        assert!(!unregister_token(&mut conn, &token).unwrap());
    }
}

pub mod models;
pub mod schema;

use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};

use crate::types::{OrderStatus, TokenId};
use self::models::*;
use self::schema::*;

pub type DbConn = r2d2::PooledConnection<ConnectionManager<SqliteConnection>>;

/// Connection pool with separate write (max 1) and read (configurable) pools.
/// Both pools run WAL mode PRAGMAs on every new connection.
#[derive(Clone)]
pub struct DbPool {
    write: r2d2::Pool<ConnectionManager<SqliteConnection>>,
    read: r2d2::Pool<ConnectionManager<SqliteConnection>>,
}

impl DbPool {
    pub fn write_conn(&self) -> Result<DbConn, r2d2::PoolError> {
        self.write.get()
    }

    pub fn read_conn(&self) -> Result<DbConn, r2d2::PoolError> {
        self.read.get()
    }
}

#[derive(Debug)]
struct WalCustomizer;

impl r2d2::CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for WalCustomizer {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> std::result::Result<(), diesel::r2d2::Error> {
        diesel::sql_query("PRAGMA journal_mode=WAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA busy_timeout=5000")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA synchronous=NORMAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        Ok(())
    }
}

/// Initialize the database with separate read/write pools and WAL mode.
/// `read_pool_size` controls how many concurrent read connections are allowed.
pub fn init_db(database_url: &str, read_pool_size: u32) -> Result<DbPool> {
    let write_pool = r2d2::Pool::builder()
        .max_size(1)
        .connection_customizer(Box::new(WalCustomizer))
        .build(ConnectionManager::<SqliteConnection>::new(database_url))?;

    let read_pool = r2d2::Pool::builder()
        .max_size(read_pool_size)
        .connection_customizer(Box::new(WalCustomizer))
        .build(ConnectionManager::<SqliteConnection>::new(database_url))?;

    let mut conn = write_pool.get()?;
    conn.run_pending_migrations(MIGRATIONS)
        .map_err(|e| anyhow::anyhow!("migration failed: {e}"))?;

    Ok(DbPool { write: write_pool, read: read_pool })
}

// ── Sync State ───────────────────────────────────────────────────────────────

pub fn get_last_fetched_block(conn: &mut SqliteConnection) -> Result<u64> {
    let state = sync_state::table
        .find(1)
        .select(SyncState::as_select())
        .first(conn)?;
    Ok(state.last_fetched_block as u64)
}

/// Atomic batch insert: notes + orders + advance block cursor.
/// Uses INSERT OR IGNORE so re-fetched notes are safely skipped.
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

// ── Orders ────────────────────────────────────────────────────────────────────

pub fn get_active_orders(conn: &mut SqliteConnection) -> Result<Vec<OrderRow>> {
    let results = orders::table
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(OrderRow::as_select())
        .load(conn)?;
    Ok(results)
}

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

pub fn reset_orders_to_active(conn: &mut SqliteConnection, note_ids: &[Vec<u8>]) -> Result<()> {
    for note_id in note_ids {
        diesel::update(orders::table.find(note_id))
            .set(orders::status.eq(OrderStatus::Active.as_str()))
            .execute(conn)?;
    }
    Ok(())
}

/// Atomic trade execution: insert generated note + mark both source orders as executed.
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

// ── Registered Tokens ─────────────────────────────────────────────────────────

pub fn get_registered_tokens(conn: &mut SqliteConnection) -> Result<Vec<RegisteredTokenRow>> {
    let results = registered_tokens::table
        .select(RegisteredTokenRow::as_select())
        .load(conn)?;
    Ok(results)
}

/// Returns true if inserted, false if already exists.
pub fn register_token(conn: &mut SqliteConnection, token_id: &[u8]) -> Result<bool> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let inserted = diesel::insert_or_ignore_into(registered_tokens::table)
        .values(&RegisteredTokenRow {
            token_id: token_id.to_vec(),
            created_at: now,
        })
        .execute(conn)?;

    Ok(inserted > 0)
}

/// Returns true if deleted, false if not found.
pub fn unregister_token(conn: &mut SqliteConnection, token_id: &[u8]) -> Result<bool> {
    let deleted = diesel::delete(
        registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)),
    )
    .execute(conn)?;

    Ok(deleted > 0)
}

/// Load all registered tokens as `TokenId` values.
pub fn load_registered_tokens(pool: &DbPool) -> anyhow::Result<Vec<TokenId>> {
    let mut conn = pool.read_conn()?;
    let rows = get_registered_tokens(&mut conn)?;
    rows.iter()
        .map(|row| {
            TokenId::read_from(&mut SliceReader::new(&row.token_id))
                .map_err(|e| anyhow::anyhow!("invalid token in DB: {e}"))
        })
        .collect()
}

/// Seed tokens from config into the DB (idempotent).
pub fn seed_tokens_from_config(pool: &DbPool, tokens: &[TokenId]) -> anyhow::Result<()> {
    let mut conn = pool.write_conn()?;
    for token in tokens {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        register_token(&mut conn, &bytes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> DbPool {
        init_db(":memory:", 1).expect("failed to create test DB")
    }

    #[test]
    fn test_init_and_sync_state() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let block = get_last_fetched_block(&mut conn).unwrap();
        assert_eq!(block, 0);
    }

    #[test]
    fn test_insert_notes_batch() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

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
        let mut conn = pool.write_conn().unwrap();

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
        let mut conn = pool.write_conn().unwrap();

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
        let mut conn = pool.write_conn().unwrap();

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
        let mut conn = pool.write_conn().unwrap();

        let token = vec![10, 20];

        register_token(&mut conn, &token).unwrap();
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 1);

        assert!(unregister_token(&mut conn, &token).unwrap());
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 0);

        assert!(!unregister_token(&mut conn, &token).unwrap());
    }
}

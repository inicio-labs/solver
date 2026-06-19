use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};

use crate::types::{IngestOrder, OrderId, OrderStatus, TokenId};
use crate::db::models::*;
use crate::db::schema::*;

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
///
/// Uses INSERT OR IGNORE so re-fetched notes are safely skipped. Returns the
/// set of `orders.note_id` byte-vecs that were *actually* inserted by this
/// call (rows-affected == 1). Duplicates (already-known orders) are excluded
/// from the returned set — callers use this to forward each order to the
/// matcher exactly once, durably, without an in-memory dedup set.
pub fn insert_notes_batch(
    conn: &mut SqliteConnection,
    new_notes: &[NoteRow],
    new_orders: &[OrderRow],
    block_number: u64,
) -> Result<HashSet<Vec<u8>>> {
    conn.transaction(|conn| {
        for note in new_notes {
            diesel::insert_or_ignore_into(notes::table)
                .values(note)
                .execute(conn)?;
        }
        let mut inserted = HashSet::new();
        for order in new_orders {
            let affected = diesel::insert_or_ignore_into(orders::table)
                .values(order)
                .execute(conn)?;
            if affected == 1 {
                inserted.insert(order.note_id.clone());
            }
        }
        diesel::update(sync_state::table.find(1))
            .set(sync_state::last_fetched_block.eq(block_number as i64))
            .execute(conn)?;
        Ok(inserted)
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

/// Atomically update the status of a batch of orders identified by note_id.
/// Returns the number of rows updated.
pub fn update_orders_status(
    conn: &mut SqliteConnection,
    note_ids: &[Vec<u8>],
    new_status: OrderStatus,
) -> Result<usize> {
    let count = diesel::update(orders::table.filter(orders::note_id.eq_any(note_ids)))
        .set(orders::status.eq(new_status.as_str()))
        .execute(conn)?;
    Ok(count)
}

/// Mark orders as `OnchainNullified` (terminal) — used when ingest detects
/// a nullifier on-chain via `sync_state.consumed_notes`, or when the
/// executor's per-note nullifier check after a tx error identifies which
/// inputs were zombies. Idempotent and safe to call concurrently with the
/// executor: the `status IN ('active', 'settling')` guard prevents
/// downgrading already-`Executed` rows. Returns the number of rows touched.
pub fn mark_orders_onchain_nullified(
    conn: &mut SqliteConnection,
    note_ids: &[Vec<u8>],
) -> Result<usize> {
    let count = diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(note_ids))
            .filter(orders::status.eq_any([
                OrderStatus::Active.as_str(),
                OrderStatus::Settling.as_str(),
            ])),
    )
    .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
    .execute(conn)?;
    Ok(count)
}

/// Boot-time recovery: any orders left in `Settling` from a previous run are
/// reset to `Active` so the matcher can re-consider them. A perfect recovery
/// would reconcile against the chain; this is the conservative fallback
/// (re-submission of an already-consumed note is rejected by the on-chain
/// script, so this is safe).
pub fn reset_all_settling_to_active(conn: &mut SqliteConnection) -> Result<usize> {
    let count = diesel::update(orders::table.filter(orders::status.eq(OrderStatus::Settling.as_str())))
        .set(orders::status.eq(OrderStatus::Active.as_str()))
        .execute(conn)?;
    Ok(count)
}

/// Load all currently-Active orders joined with their raw note bytes, ready to
/// hydrate an in-memory `OrderBook`. The matcher calls this on startup so that
/// orders ingest persisted but never delivered (channel send failed, process
/// crashed) are still picked up — DB is the source of truth.
pub fn load_active_orders_with_notes(conn: &mut SqliteConnection) -> Result<Vec<IngestOrder>> {
    let rows: Vec<(OrderRow, Vec<u8>)> = orders::table
        .inner_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select((OrderRow::as_select(), notes::raw_data))
        .load(conn)?;

    let mut out = Vec::with_capacity(rows.len());
    for (order_row, raw_data) in rows {
        let note_id = OrderId::read_from(&mut SliceReader::new(&order_row.note_id))
            .map_err(|e| anyhow::anyhow!("invalid note_id in DB: {e}"))?;
        let offered_token = TokenId::read_from(&mut SliceReader::new(&order_row.offered_asset))
            .map_err(|e| anyhow::anyhow!("invalid offered_asset in DB: {e}"))?;
        let requested_token = TokenId::read_from(&mut SliceReader::new(&order_row.requested_asset))
            .map_err(|e| anyhow::anyhow!("invalid requested_asset in DB: {e}"))?;
        out.push(IngestOrder {
            note_id,
            offered_token,
            requested_token,
            offered_amount: order_row.offered_amount as u64,
            requested_amount: order_row.requested_amount as u64,
            raw_note_data: raw_data,
        });
    }
    Ok(out)
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
/// Optionally takes an `external_symbol` (e.g. CoinGecko ID like `"usd-coin"`)
/// for price-feed lookups. Pass `None` if no price source mapping is known yet.
pub fn register_token(
    conn: &mut SqliteConnection,
    token_id: &[u8],
    external_symbol: Option<&str>,
) -> Result<bool> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let inserted = diesel::insert_or_ignore_into(registered_tokens::table)
        .values(&RegisteredTokenRow {
            token_id: token_id.to_vec(),
            created_at: now,
            external_symbol: external_symbol.map(|s| s.to_string()),
        })
        .execute(conn)?;

    Ok(inserted > 0)
}

/// Update a token's external_symbol (e.g. via admin API). Returns true if a
/// row was updated, false if the token isn't registered.
pub fn update_token_symbol(
    conn: &mut SqliteConnection,
    token_id: &[u8],
    symbol: Option<&str>,
) -> Result<bool> {
    let new_value = symbol.map(|s| s.to_string());
    let updated = diesel::update(
        registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)),
    )
    .set(registered_tokens::external_symbol.eq(new_value))
    .execute(conn)?;
    Ok(updated > 0)
}

/// Load (token_id → external_symbol) for all tokens that have a non-null
/// symbol. Used by `HttpPriceClient` to hydrate its in-memory cache at boot.
pub fn load_token_symbols(pool: &DbPool) -> Result<HashMap<TokenId, String>> {
    let mut conn = pool.read_conn()?;
    let rows = get_registered_tokens(&mut conn)?;
    let mut out = HashMap::new();
    for row in rows {
        let Some(symbol) = row.external_symbol else { continue };
        let token = TokenId::read_from(&mut SliceReader::new(&row.token_id))
            .map_err(|e| anyhow::anyhow!("invalid token in DB: {e}"))?;
        out.insert(token, symbol);
    }
    Ok(out)
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

/// Seed tokens (with optional symbol mappings) from config into the DB
/// (idempotent). Used at boot to hydrate the registered_tokens table from
/// `solver.toml` `[[pairs]]` entries.
pub fn seed_tokens_from_config(
    pool: &DbPool,
    tokens: &[(TokenId, Option<String>)],
) -> anyhow::Result<()> {
    let mut conn = pool.write_conn()?;
    for (token, symbol) in tokens {
        let mut bytes = Vec::new();
        token.write_into(&mut bytes);
        register_token(&mut conn, &bytes, symbol.as_deref())?;
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

        update_order_status(&mut conn, &active[0].note_id, OrderStatus::Settling).unwrap();

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
    fn insert_notes_batch_reports_only_first_insert() {
        // Load-bearing invariant for the DB-first-insert dedup that replaced
        // the in-memory `seen_notes` HashSet: the same order's note_id is
        // returned on the FIRST insert and excluded on every repeat, so
        // ingest forwards it to the matcher exactly once.
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        let note = NoteRow {
            note_id: vec![9, 9, 9],
            account_id: vec![1],
            raw_data: vec![1],
        };
        let order = OrderRow {
            note_id: vec![9, 9, 9],
            account_id: vec![1],
            requested_asset: vec![1],
            requested_amount: 1,
            offered_asset: vec![1],
            offered_amount: 1,
            timestamp: 1,
            status: OrderStatus::Active.as_str().to_string(),
        };

        let first =
            insert_notes_batch(&mut conn, &[note.clone()], &[order.clone()], 1).unwrap();
        assert_eq!(first.len(), 1);
        assert!(first.contains(&vec![9, 9, 9]));

        let second = insert_notes_batch(&mut conn, &[note], &[order], 2).unwrap();
        assert!(
            second.is_empty(),
            "re-inserting a known order must report zero newly-inserted note_ids"
        );
    }

    #[test]
    fn test_register_and_list_tokens() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        let token_a = vec![1, 2, 3, 4];
        let token_b = vec![5, 6, 7, 8];

        assert!(register_token(&mut conn, &token_a, None).unwrap());
        assert!(register_token(&mut conn, &token_b, Some("ethereum")).unwrap());

        let tokens = get_registered_tokens(&mut conn).unwrap();
        assert_eq!(tokens.len(), 2);

        assert!(!register_token(&mut conn, &token_a, None).unwrap());
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 2);
    }

    #[test]
    fn test_update_token_symbol() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        let token = vec![42, 43, 44];

        // No-op on unknown token.
        assert!(!update_token_symbol(&mut conn, &token, Some("usd-coin")).unwrap());

        register_token(&mut conn, &token, None).unwrap();
        assert!(update_token_symbol(&mut conn, &token, Some("usd-coin")).unwrap());

        let rows = get_registered_tokens(&mut conn).unwrap();
        assert_eq!(rows[0].external_symbol.as_deref(), Some("usd-coin"));

        // Clearing the symbol works too.
        assert!(update_token_symbol(&mut conn, &token, None).unwrap());
        let rows = get_registered_tokens(&mut conn).unwrap();
        assert!(rows[0].external_symbol.is_none());
    }

    #[test]
    fn test_unregister_token() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        let token = vec![10, 20];

        register_token(&mut conn, &token, None).unwrap();
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 1);

        assert!(unregister_token(&mut conn, &token).unwrap());
        assert_eq!(get_registered_tokens(&mut conn).unwrap().len(), 0);

        assert!(!unregister_token(&mut conn, &token).unwrap());
    }

    /// Helper: insert one minimal order row at the given status.
    fn seed_order(conn: &mut SqliteConnection, note_id: &[u8], status: OrderStatus) {
        let note = NoteRow {
            note_id: note_id.to_vec(),
            account_id: vec![1],
            raw_data: vec![1],
        };
        let order = OrderRow {
            note_id: note_id.to_vec(),
            account_id: vec![1],
            requested_asset: vec![1],
            requested_amount: 1,
            offered_asset: vec![1],
            offered_amount: 1,
            timestamp: 1,
            status: status.as_str().to_string(),
        };
        insert_notes_batch(conn, &[note], &[order], 1).unwrap();
    }

    fn order_status(conn: &mut SqliteConnection, note_id: &[u8]) -> String {
        orders::table
            .filter(orders::note_id.eq(note_id))
            .select(orders::status)
            .first::<String>(conn)
            .unwrap()
    }

    #[test]
    fn mark_orders_onchain_nullified_promotes_active_and_settling() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        seed_order(&mut conn, &[1, 1, 1], OrderStatus::Active);
        seed_order(&mut conn, &[2, 2, 2], OrderStatus::Settling);

        let updated = mark_orders_onchain_nullified(
            &mut conn,
            &[vec![1, 1, 1], vec![2, 2, 2]],
        )
        .unwrap();
        assert_eq!(updated, 2);

        assert_eq!(order_status(&mut conn, &[1, 1, 1]), "onchain_nullified");
        assert_eq!(order_status(&mut conn, &[2, 2, 2]), "onchain_nullified");
    }

    #[test]
    fn mark_orders_onchain_nullified_preserves_executed() {
        // Defends the idempotency invariant: ingest may see a consumed note
        // for an order our own executor already marked Executed. The status
        // guard MUST prevent the row from being downgraded.
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        seed_order(&mut conn, &[3, 3, 3], OrderStatus::Executed);

        let updated =
            mark_orders_onchain_nullified(&mut conn, &[vec![3, 3, 3]]).unwrap();
        assert_eq!(updated, 0, "Executed row must not be downgraded");
        assert_eq!(order_status(&mut conn, &[3, 3, 3]), "executed");
    }

    #[test]
    fn mark_orders_onchain_nullified_idempotent_on_already_terminal() {
        // Calling twice on the same OnchainNullified row updates zero rows.
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();

        seed_order(&mut conn, &[4, 4, 4], OrderStatus::Active);

        let first = mark_orders_onchain_nullified(&mut conn, &[vec![4, 4, 4]]).unwrap();
        assert_eq!(first, 1);

        let second = mark_orders_onchain_nullified(&mut conn, &[vec![4, 4, 4]]).unwrap();
        assert_eq!(second, 0, "second call must be a no-op");
        assert_eq!(order_status(&mut conn, &[4, 4, 4]), "onchain_nullified");
    }
}

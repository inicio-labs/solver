use anyhow::{anyhow, Result};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = include_str!("../../schema.sql");

use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;

use crate::db::models::*;
use crate::db::schema::*;
use crate::types::{BookOrder, BookUpdate, OrderId, OrderStatus, SettlementError, TokenId};

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

    /// Reserve channel capacity before writing. Commit and publish while holding
    /// the single writer connection, with no await between them. Concurrent
    /// observers therefore publish in database order. A process crash in this
    /// tiny gap is repaired by startup hydration, not by an in-memory retry.
    pub async fn update_book(
        &self,
        sender: &tokio::sync::mpsc::Sender<BookUpdate>,
        update: impl FnOnce(&mut SqliteConnection) -> Result<BookUpdate>,
    ) -> Result<()> {
        let permit = sender
            .reserve()
            .await
            .map_err(|_| anyhow::anyhow!("matcher channel closed"))?;
        let mut conn = self.write_conn()?;
        let outcome = conn.transaction(|conn| update(conn))?;
        if !outcome.is_empty() {
            permit.send(outcome);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct WalCustomizer;

impl r2d2::CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for WalCustomizer {
    fn on_acquire(
        &self,
        conn: &mut SqliteConnection,
    ) -> std::result::Result<(), diesel::r2d2::Error> {
        diesel::sql_query("PRAGMA journal_mode=WAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA busy_timeout=5000")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA synchronous=NORMAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA foreign_keys=ON")
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
    conn.batch_execute(SCHEMA)?;

    Ok(DbPool {
        write: write_pool,
        read: read_pool,
    })
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
/// map of `orders.note_id` to persisted FIFO sequence for rows actually
/// inserted by this call (rows-affected == 1). Duplicates are excluded
/// from the returned set — callers use this to forward each order to the
/// matcher exactly once, durably, without an in-memory dedup set.
pub fn insert_notes_batch(
    conn: &mut SqliteConnection,
    new_notes: &[NoteRow],
    new_orders: &[OrderRow],
    block_number: u64,
) -> Result<HashMap<Vec<u8>, u64>> {
    conn.transaction(|conn| {
        for note in new_notes {
            diesel::insert_or_ignore_into(notes::table)
                .values(note)
                .execute(conn)?;
        }
        let mut inserted = HashMap::new();
        for order in new_orders {
            let affected = diesel::insert_or_ignore_into(orders::table)
                .values(order)
                .execute(conn)?;
            if affected == 1 {
                let sequence: i64 = orders::table
                    .find(&order.note_id)
                    .select(orders::priority_seq)
                    .first(conn)?;
                let sequence = u64::try_from(sequence)
                    .map_err(|_| anyhow::anyhow!("invalid persisted FIFO sequence"))?;
                if sequence == 0 {
                    anyhow::bail!("missing persisted FIFO sequence after insert");
                }
                inserted.insert(order.note_id.clone(), sequence);
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

/// All inputs of one locally executed transaction are reserved together.
/// This write happens after proof but before network submission.
pub fn prepare_settlement(
    conn: &mut SqliteConnection,
    attempt: &SettlementAttemptRow,
    inputs: &[SettlementInputRow],
) -> Result<()> {
    conn.transaction(|conn| {
        diesel::insert_into(settlement_attempts::table)
            .values(attempt)
            .execute(conn)?;
        for input in inputs {
            if input.tx_id != attempt.tx_id {
                return Err(SettlementError::InputTransactionMismatch.into());
            }
            let changed = diesel::update(
                orders::table
                    .find(&input.parent_note_id)
                    .filter(orders::status.eq(OrderStatus::Active.as_str())),
            )
            .set(orders::status.eq(OrderStatus::Settling.as_str()))
            .execute(conn)?;
            if changed != 1 {
                return Err(SettlementError::InputOrderNotActive.into());
            }
        }
        diesel::insert_into(settlement_inputs::table)
            .values(inputs)
            .execute(conn)?;
        Ok(())
    })
}

pub fn mark_settlement_submitted(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<()> {
    diesel::update(
        settlement_attempts::table
            .find(tx_id)
            .filter(settlement_attempts::status.eq("prepared")),
    )
    .set(settlement_attempts::status.eq("submitted"))
    .execute(conn)?;
    Ok(())
}

pub fn mark_settlement_uncertain(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<()> {
    diesel::update(
        settlement_attempts::table
            .find(tx_id)
            .filter(settlement_attempts::status.ne("confirmed")),
    )
    .set(settlement_attempts::status.eq("uncertain"))
    .execute(conn)?;
    Ok(())
}

/// Remember a definite rejection before the fallible nullifier check. Recovery
/// completes classification instead of resubmitting a known rejected attempt.
pub fn mark_settlement_rejected(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<()> {
    diesel::update(
        settlement_attempts::table
            .find(tx_id)
            .filter(settlement_attempts::status.ne("confirmed")),
    )
    .set(settlement_attempts::status.eq("rejected"))
    .execute(conn)?;
    Ok(())
}

pub fn unresolved_settlements(conn: &mut SqliteConnection) -> Result<Vec<SettlementAttemptRow>> {
    Ok(settlement_attempts::table
        .filter(settlement_attempts::status.ne("confirmed"))
        .select(SettlementAttemptRow::as_select())
        .load(conn)?)
}

/// One committed output is enough to prove inclusion of the whole atomic
/// transaction, including full fills that have no remainder note.
pub fn settlement_payback_id(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<OrderId> {
    let bytes: Vec<u8> = settlement_inputs::table
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .select(settlement_inputs::payback_note_id)
        .first(conn)?;
    Ok(OrderId::read_from(&mut SliceReader::new(&bytes))?)
}

pub fn settlement_parents(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<Vec<BookOrder>> {
    // A missing persisted parent must fail recovery, not silently disappear.
    let parents: Vec<(Option<OrderRow>, Option<Vec<u8>>)> = settlement_inputs::table
        .left_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .left_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .select((Option::<OrderRow>::as_select(), notes::raw_data.nullable()))
        .load(conn)?;
    parents
        .into_iter()
        .map(|(row, raw)| {
            let row = row.ok_or_else(|| anyhow!("settlement parent order missing"))?;
            let raw = raw.ok_or_else(|| anyhow!("settlement parent note missing"))?;
            row.into_book_order(raw)
        })
        .collect()
}

pub fn finish_discarded_settlement(
    conn: &mut SqliteConnection,
    tx_id: &[u8],
    consumed: &HashSet<OrderId>,
) -> Result<BookUpdate> {
    conn.transaction(|conn| {
        let status: String = settlement_attempts::table
            .find(tx_id)
            .select(settlement_attempts::status)
            .first(conn)?;
        if status == "confirmed" {
            return Ok(BookUpdate::default());
        }
        let parents = settlement_parents(conn, tx_id)?;
        let mut update = BookUpdate::default();
        for parent in parents {
            let parent_id = parent.id();
            let current: String = orders::table
                .find(parent_id.to_bytes().as_slice())
                .select(orders::status)
                .first(conn)?;
            // Never undo a terminal state observed after the RPC snapshot.
            if current != OrderStatus::Settling.as_str() {
                continue;
            }
            let status = if consumed.contains(&parent_id) {
                update.removed.push(parent_id);
                OrderStatus::OnchainNullified
            } else {
                update.active.push(parent);
                OrderStatus::Active
            };
            diesel::update(orders::table.find(parent_id.to_bytes().as_slice()))
                .set(orders::status.eq(status.as_str()))
                .execute(conn)?;
        }
        diesel::delete(settlement_inputs::table.filter(settlement_inputs::tx_id.eq(tx_id)))
            .execute(conn)?;
        diesel::delete(settlement_attempts::table.find(tx_id)).execute(conn)?;
        Ok(update)
    })
}

/// Delayed/pre-submission re-feeds must not resurrect an order that ingestion
/// consumed, or a different attempt reserved, while the re-feed was waiting.
pub fn active_book_update(
    conn: &mut SqliteConnection,
    candidates: Vec<BookOrder>,
) -> Result<BookUpdate> {
    let ids: Vec<_> = candidates
        .iter()
        .map(|order| order.id().to_bytes().to_vec())
        .collect();
    let active: HashSet<Vec<u8>> = orders::table
        .filter(orders::note_id.eq_any(ids))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    Ok(BookUpdate {
        removed: Vec::new(),
        active: candidates
            .into_iter()
            .filter(|order| active.contains(order.id().to_bytes().as_slice()))
            .collect(),
    })
}

impl SettlementInputRow {
    fn child_order(
        &self,
        priority_seq: u64,
        arrival_unix: u64,
    ) -> Result<Option<(BookOrder, crate::types::Order)>> {
        let (Some(child_id), Some(raw_note_data)) = (&self.child_note_id, &self.child_note_data)
        else {
            return Ok(None);
        };
        let note = Note::read_from(&mut SliceReader::new(raw_note_data))?;
        if note.id().to_bytes().as_slice() != child_id {
            return Err(SettlementError::ChildIdMismatch.into());
        }
        let parsed = crate::types::Order::from_note(&note)?;
        Ok(Some((
            BookOrder {
                priority_seq,
                arrival_unix,
                note: std::sync::Arc::new(note),
            },
            parsed,
        )))
    }
}

/// Commit the parent→child handoff in one SQLite transaction. A second
/// observer of the same confirmation gets an empty result and sends nothing.
pub fn confirm_settlement(conn: &mut SqliteConnection, tx_id: &[u8]) -> Result<BookUpdate> {
    conn.transaction(|conn| {
        let status: String = settlement_attempts::table
            .find(tx_id)
            .select(settlement_attempts::status)
            .first(conn)?;
        if status == "confirmed" {
            return Ok(BookUpdate::default());
        }
        if !matches!(
            status.as_str(),
            "prepared" | "submitted" | "uncertain" | "rejected"
        ) {
            return Err(SettlementError::InvalidConfirmationStatus(status).into());
        }

        let inputs: Vec<SettlementInputRow> = settlement_inputs::table
            .filter(settlement_inputs::tx_id.eq(tx_id))
            .select(SettlementInputRow::as_select())
            .load(conn)?;
        let mut activation = BookUpdate::default();
        for input in inputs {
            let parent: OrderRow = orders::table
                .find(&input.parent_note_id)
                .select(OrderRow::as_select())
                .first(conn)?;
            let parent_id = OrderId::read_from(&mut SliceReader::new(&input.parent_note_id))?;
            activation.removed.push(parent_id);
            if let Some((child, terms)) = input.child_order(
                u64::try_from(parent.priority_seq)?,
                u64::try_from(parent.timestamp)?,
            )? {
                diesel::insert_or_ignore_into(notes::table)
                    .values(NoteRow {
                        note_id: child.id().to_bytes().to_vec(),
                        account_id: terms.creator_id.to_bytes().to_vec(),
                        raw_data: child.note.to_bytes(),
                    })
                    .execute(conn)?;
                diesel::insert_or_ignore_into(orders::table)
                    .values(OrderRow {
                        note_id: child.id().to_bytes().to_vec(),
                        account_id: terms.creator_id.to_bytes().to_vec(),
                        requested_asset: terms.requested_faucet_id.to_bytes().to_vec(),
                        requested_amount: i64::try_from(terms.requested_amount)?,
                        offered_asset: terms.offered_faucet_id.to_bytes().to_vec(),
                        offered_amount: i64::try_from(terms.offered_amount)?,
                        timestamp: parent.timestamp,
                        status: OrderStatus::Active.as_str().to_string(),
                        priority_seq: parent.priority_seq,
                    })
                    .execute(conn)?;
                let status: String = orders::table
                    .find(child.id().to_bytes().as_slice())
                    .select(orders::status)
                    .first(conn)?;
                if status == OrderStatus::Active.as_str() {
                    activation.active.push(child);
                }
            }
            diesel::update(orders::table.find(&input.parent_note_id))
                .set(orders::status.eq(OrderStatus::Executed.as_str()))
                .execute(conn)?;
        }
        diesel::update(settlement_attempts::table.find(tx_id))
            .set(settlement_attempts::status.eq("confirmed"))
            .execute(conn)?;
        Ok(activation)
    })
}

/// A synced child note proves that its expected output was included. Return
/// `None` for ordinary notes; return an empty activation for a duplicate.
pub fn confirm_expected_remainder(
    conn: &mut SqliteConnection,
    child_id: &[u8],
) -> Result<Option<BookUpdate>> {
    let tx_id: Option<Vec<u8>> = settlement_inputs::table
        .filter(settlement_inputs::child_note_id.eq(child_id))
        .select(settlement_inputs::tx_id)
        .first(conn)
        .optional()?;
    tx_id.map(|id| confirm_settlement(conn, &id)).transpose()
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
    if note_ids.is_empty() {
        return Ok(0);
    }
    // An input from our own unresolved transaction is reconciled by its tx ID.
    // A nullifier alone cannot tell whether our submission committed or some
    // competing consumer won, so do not downgrade it here.
    let reserved: HashSet<Vec<u8>> = settlement_inputs::table
        .inner_join(
            settlement_attempts::table.on(settlement_inputs::tx_id.eq(settlement_attempts::tx_id)),
        )
        .filter(settlement_inputs::parent_note_id.eq_any(note_ids))
        .filter(settlement_attempts::status.ne("confirmed"))
        .select(settlement_inputs::parent_note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    let external: Vec<&Vec<u8>> = note_ids
        .iter()
        .filter(|id| !reserved.contains(*id))
        .collect();
    if external.is_empty() {
        return Ok(0);
    }
    let count = diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(external))
            .filter(
                orders::status
                    .eq_any([OrderStatus::Active.as_str(), OrderStatus::Settling.as_str()]),
            ),
    )
    .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
    .execute(conn)?;
    Ok(count)
}

/// Load all currently-Active orders joined with their raw note bytes, ready to
/// hydrate an in-memory `OrderBook`. The matcher calls this on startup so that
/// orders ingest persisted but never delivered (channel send failed, process
/// crashed) are still picked up — DB is the source of truth.
pub fn load_active_orders_with_notes(conn: &mut SqliteConnection) -> Result<Vec<BookOrder>> {
    let rows: Vec<(OrderRow, Vec<u8>)> = orders::table
        .inner_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .order(orders::priority_seq.asc())
        .select((OrderRow::as_select(), notes::raw_data))
        .load(conn)?;

    let mut out = Vec::with_capacity(rows.len());
    for (order_row, raw_data) in rows {
        match order_row.into_book_order(raw_data) {
            Ok(order) if order.priority_seq > 0 => out.push(order),
            Ok(_) => anyhow::bail!("active order lacks persisted FIFO sequence"),
            Err(e) => {
                tracing::warn!(error = %e, "skipping active order whose stored note does not parse");
            }
        }
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
            // Fetched on-chain after registration (see ingest); NULL until known.
            decimals: None,
            ticker: None,
        })
        .execute(conn)?;

    Ok(inserted > 0)
}

/// Persist a token's on-chain metadata (decimals + ticker), fetched once from
/// the faucet account. Returns true if a registered row was updated.
pub fn set_token_metadata(
    conn: &mut SqliteConnection,
    token_id: &[u8],
    decimals: Option<i32>,
    ticker: Option<&str>,
) -> Result<bool> {
    let updated =
        diesel::update(registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)))
            .set((
                registered_tokens::decimals.eq(decimals),
                registered_tokens::ticker.eq(ticker.map(|s| s.to_string())),
            ))
            .execute(conn)?;
    Ok(updated > 0)
}

/// Fetch one registered token row by id (for the price API: registered? +
/// decimals/ticker). Returns `None` if the faucet is not registered.
pub fn get_registered_token(
    conn: &mut SqliteConnection,
    token_id: &[u8],
) -> Result<Option<RegisteredTokenRow>> {
    let row = registered_tokens::table
        .filter(registered_tokens::token_id.eq(token_id))
        .select(RegisteredTokenRow::as_select())
        .first(conn)
        .optional()?;
    Ok(row)
}

/// Pool-level convenience for the price API: fetch one token row via a read conn.
pub fn fetch_token_row(pool: &DbPool, token_id: &[u8]) -> Result<Option<RegisteredTokenRow>> {
    let mut conn = pool.read_conn()?;
    get_registered_token(&mut conn, token_id)
}

/// Update a token's external_symbol (e.g. via admin API). Returns true if a
/// row was updated, false if the token isn't registered.
pub fn update_token_symbol(
    conn: &mut SqliteConnection,
    token_id: &[u8],
    symbol: Option<&str>,
) -> Result<bool> {
    let new_value = symbol.map(|s| s.to_string());
    let updated =
        diesel::update(registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)))
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
        let Some(symbol) = row.external_symbol else {
            continue;
        };
        let token = TokenId::read_from(&mut SliceReader::new(&row.token_id))
            .map_err(|e| anyhow::anyhow!("invalid token in DB: {e}"))?;
        out.insert(token, symbol);
    }
    Ok(out)
}

/// Returns true if deleted, false if not found.
pub fn unregister_token(conn: &mut SqliteConnection, token_id: &[u8]) -> Result<bool> {
    let deleted =
        diesel::delete(registered_tokens::table.filter(registered_tokens::token_id.eq(token_id)))
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
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    fn partial_note_pair() -> (Note, Note) {
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let solver = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2
            .try_into()
            .unwrap();
        let mut rng = RandomCoin::new(Word::default());
        let parent: Note = PswapNote::builder()
            .sender(creator)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 10).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .serial_number(rng.draw_word())
            .note_type(NoteType::Public)
            .offered_asset(FungibleAsset::new(offered, 10).unwrap())
            .build()
            .unwrap()
            .into();
        let (_, remainder) = PswapNote::try_from(&parent)
            .unwrap()
            .execute(
                solver,
                None,
                Some(FungibleAsset::new(requested, 5).unwrap()),
            )
            .unwrap();
        (parent, Note::from(remainder.unwrap()))
    }

    fn prepared_fixture(conn: &mut SqliteConnection) -> (Note, Note, Vec<u8>, i64) {
        let (parent, child) = partial_note_pair();
        let parsed = crate::types::Order::from_note(&parent).unwrap();
        let parent_id = parent.id().to_bytes().to_vec();
        insert_notes_batch(
            conn,
            &[NoteRow {
                note_id: parent_id.clone(),
                account_id: parsed.creator_id.to_bytes().to_vec(),
                raw_data: parent.to_bytes(),
            }],
            &[OrderRow {
                note_id: parent_id.clone(),
                account_id: parsed.creator_id.to_bytes().to_vec(),
                requested_asset: parsed.requested_faucet_id.to_bytes().to_vec(),
                requested_amount: 10,
                offered_asset: parsed.offered_faucet_id.to_bytes().to_vec(),
                offered_amount: 10,
                timestamp: 1,
                status: "active".into(),
                priority_seq: 0,
            }],
            1,
        )
        .unwrap();
        let priority = get_active_orders(conn).unwrap()[0].priority_seq;
        let tx_id = vec![7u8; 32];
        prepare_settlement(
            conn,
            &SettlementAttemptRow {
                tx_id: tx_id.clone(),
                tx_result: vec![1],
                status: "prepared".into(),
            },
            &[SettlementInputRow {
                tx_id: tx_id.clone(),
                parent_note_id: parent_id.clone(),
                payback_note_id: vec![8u8; 32],
                child_note_id: Some(child.id().to_bytes().to_vec()),
                child_note_data: Some(child.to_bytes()),
            }],
        )
        .unwrap();
        (parent, child, tx_id, priority)
    }

    #[test]
    fn confirmed_remainder_inherits_priority_once() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let (parent, child, tx_id, priority) = prepared_fixture(&mut conn);
        assert_eq!(
            settlement_payback_id(&mut conn, &tx_id).unwrap().to_bytes(),
            [8u8; 32]
        );
        assert_eq!(
            mark_orders_onchain_nullified(&mut conn, &[parent.id().to_bytes().to_vec()]).unwrap(),
            0
        );

        let first = confirm_expected_remainder(&mut conn, child.id().to_bytes().as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(first.removed, vec![parent.id()]);
        assert_eq!(first.active.len(), 1);
        assert_eq!(first.active[0].priority_seq, priority as u64);
        let duplicate = confirm_settlement(&mut conn, &tx_id).unwrap();
        assert!(duplicate.removed.is_empty() && duplicate.active.is_empty());
        mark_settlement_submitted(&mut conn, &tx_id).unwrap();
        mark_settlement_uncertain(&mut conn, &tx_id).unwrap();
        let status: String = settlement_attempts::table
            .find(&tx_id)
            .select(settlement_attempts::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "confirmed");
        let live = get_active_orders(&mut conn).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].note_id, child.id().to_bytes());
        assert_eq!(live[0].priority_seq, priority);
    }

    #[test]
    fn stored_order_metadata_is_checked_at_hydration_boundary() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let (parent, _, _, priority) = prepared_fixture(&mut conn);
        let row: OrderRow = orders::table
            .find(parent.id().to_bytes().as_slice())
            .select(OrderRow::as_select())
            .first(&mut conn)
            .unwrap();
        let ingested = row.clone().into_book_order(parent.to_bytes()).unwrap();
        assert_eq!(ingested.id(), parent.id());
        assert_eq!(ingested.priority_seq, priority as u64);
        assert_eq!(ingested.note.as_ref(), &parent);
        let mut wrong_amount = row.clone();
        wrong_amount.requested_amount += 1;
        assert!(wrong_amount.into_book_order(parent.to_bytes()).is_err());
        let mut wrong_asset = row;
        wrong_asset.requested_asset = wrong_asset.offered_asset.clone();
        assert!(wrong_asset.into_book_order(parent.to_bytes()).is_err());
    }

    #[test]
    fn settlement_parent_load_preserves_data_and_rejects_missing_note() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let (parent, _, tx_id, priority) = prepared_fixture(&mut conn);
        let parents = settlement_parents(&mut conn, &tx_id).unwrap();
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].id(), parent.id());
        assert_eq!(parents[0].priority_seq, priority as u64);
        assert_eq!(parents[0].note.as_ref(), &parent);

        diesel::delete(notes::table.find(parent.id().to_bytes().as_slice()))
            .execute(&mut conn)
            .unwrap();
        assert!(settlement_parents(&mut conn, &tx_id).is_err());
    }

    #[test]
    fn rejected_cleanup_rolls_back_parent_and_attempt_together() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let (parent, _, tx_id, _) = prepared_fixture(&mut conn);
        mark_settlement_rejected(&mut conn, &tx_id).unwrap();
        conn.batch_execute(
            "CREATE TRIGGER reject_cleanup BEFORE DELETE ON settlement_attempts
            BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END;",
        )
        .unwrap();
        assert!(finish_discarded_settlement(&mut conn, &tx_id, &HashSet::new()).is_err());
        assert!(get_active_orders(&mut conn).unwrap().is_empty());
        assert_eq!(
            unresolved_settlements(&mut conn).unwrap()[0].status,
            "rejected"
        );
        assert_eq!(
            settlement_parents(&mut conn, &tx_id).unwrap()[0].id(),
            parent.id()
        );

        conn.batch_execute("DROP TRIGGER reject_cleanup;").unwrap();
        let update = finish_discarded_settlement(&mut conn, &tx_id, &HashSet::new()).unwrap();
        assert_eq!(update.active[0].id(), parent.id());
        assert!(unresolved_settlements(&mut conn).unwrap().is_empty());
        assert!(settlement_parents(&mut conn, &tx_id).unwrap().is_empty());
        assert_eq!(get_active_orders(&mut conn).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn closed_matcher_does_not_commit_confirmation() {
        let pool = test_pool();
        let (_, _, tx_id, _) = prepared_fixture(&mut pool.write_conn().unwrap());
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        assert!(pool
            .update_book(&sender, |conn| confirm_settlement(conn, &tx_id))
            .await
            .is_err());
        assert_eq!(
            unresolved_settlements(&mut pool.write_conn().unwrap())
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn full_matcher_queue_waits_before_database_transition() {
        let pool = test_pool();
        let (parent, child, tx_id, priority) = prepared_fixture(&mut pool.write_conn().unwrap());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender.send(BookUpdate::default()).await.unwrap();
        let publish = pool.update_book(&sender, |conn| confirm_settlement(conn, &tx_id));
        tokio::pin!(publish);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut publish)
                .await
                .is_err()
        );
        assert_eq!(
            unresolved_settlements(&mut pool.write_conn().unwrap())
                .unwrap()
                .len(),
            1
        );
        receiver.recv().await.unwrap();
        publish.await.unwrap();
        let outcome = receiver.recv().await.unwrap();
        assert_eq!(outcome.removed, vec![parent.id()]);
        assert_eq!(outcome.active[0].id(), child.id());
        assert_eq!(outcome.active[0].priority_seq, priority as u64);
    }

    #[test]
    fn delayed_refeed_cannot_resurrect_consumed_order() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        let (parent, _, tx_id, _) = prepared_fixture(&mut conn);
        let update = finish_discarded_settlement(&mut conn, &tx_id, &HashSet::new()).unwrap();
        mark_orders_onchain_nullified(&mut conn, &[parent.id().to_bytes().to_vec()]).unwrap();
        assert!(active_book_update(&mut conn, update.active)
            .unwrap()
            .is_empty());
    }

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
    fn fresh_schema_can_run_again_without_resetting_state() {
        let pool = test_pool();
        let mut conn = pool.write_conn().unwrap();
        diesel::sql_query("UPDATE sync_state SET last_fetched_block = 42 WHERE id = 1")
            .execute(&mut conn)
            .unwrap();

        conn.batch_execute(SCHEMA).unwrap();
        assert_eq!(get_last_fetched_block(&mut conn).unwrap(), 42);
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
            priority_seq: 0,
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
            priority_seq: 0,
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
            priority_seq: 0,
        };

        let first = insert_notes_batch(&mut conn, &[note.clone()], &[order.clone()], 1).unwrap();
        assert_eq!(first.len(), 1);
        assert!(first.contains_key(&vec![9, 9, 9]));
        assert_eq!(first[&vec![9, 9, 9]], 1);

        let second = insert_notes_batch(&mut conn, &[note], &[order], 2).unwrap();
        assert!(
            second.is_empty(),
            "re-inserting a known order must report zero newly-inserted note_ids"
        );
        let persisted: i64 = orders::table
            .find(vec![9, 9, 9])
            .select(orders::priority_seq)
            .first(&mut conn)
            .unwrap();
        assert_eq!(persisted, 1);

        // A removed highest-priority row must not let a later note inherit
        // its sequence (SQLite may reuse the deleted rowid).
        diesel::delete(orders::table.find(vec![9, 9, 9]))
            .execute(&mut conn)
            .unwrap();
        let later_note = NoteRow {
            note_id: vec![8, 8, 8],
            account_id: vec![1],
            raw_data: vec![2],
        };
        let later_order = OrderRow {
            note_id: later_note.note_id.clone(),
            account_id: vec![1],
            requested_asset: vec![1],
            requested_amount: 1,
            offered_asset: vec![1],
            offered_amount: 1,
            timestamp: 2,
            status: OrderStatus::Active.as_str().to_string(),
            priority_seq: 0,
        };
        let later = insert_notes_batch(&mut conn, &[later_note], &[later_order], 3).unwrap();
        assert_eq!(later[&vec![8, 8, 8]], 2);
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
            priority_seq: 0,
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

        let updated =
            mark_orders_onchain_nullified(&mut conn, &[vec![1, 1, 1], vec![2, 2, 2]]).unwrap();
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

        let updated = mark_orders_onchain_nullified(&mut conn, &[vec![3, 3, 3]]).unwrap();
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

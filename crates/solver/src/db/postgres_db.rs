//! PostgreSQL transaction-scoped application queries.
//!
//! These helpers never start transactions or check out connections. Call them
//! only from `PgPool::{read,write,write_book}` so synchronous libpq work stays
//! on blocking workers and each lifecycle transition owns one transaction.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::error::{DbError, DbResult};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;

use super::maker_db::{self, SettlementPhase};
use super::postgres_models::{
    maker_tag, LiveOrderRow, NewOrderRow, NewRemainderOrderRow, OrderKeyColumns, OrderRow,
    RegisteredTokenRow, SettlementAttemptRow, SettlementInputRow, SettlementStatus,
};
use super::postgres_schema::{
    live_orders, maker_lineages, makers, orders, registered_tokens, settlement_attempts,
    settlement_inputs, sync_state,
};
use crate::maker::MakerTag;
use crate::types::{BookOrder, BookUpdate, Order, OrderId, OrderStatus, SettlementError, TokenId};
use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;

/// Rows per multi-row INSERT. The widest row (a remainder order) binds eight
/// parameters, far below PostgreSQL's 65,535-parameter statement limit.
const INSERT_CHUNK_ROWS: usize = 1_000;

pub fn get_last_fetched_block_tx(conn: &mut PgConnection) -> DbResult<u64> {
    let block: i64 = sync_state::table
        .find(1_i16)
        .select(sync_state::last_fetched_block)
        .first(conn)?;
    Ok(u64::try_from(block)?)
}

/// Insert an ordinary ingest batch in a caller-owned transaction. Only rows
/// returned by PostgreSQL were newly inserted and may activate in the matcher.
/// Rows come from `NewOrderRow::ingested`, which already parsed each note.
pub fn insert_orders_batch_tx(
    conn: &mut PgConnection,
    new_orders: &[NewOrderRow],
    block_number: u64,
) -> DbResult<HashMap<Vec<u8>, u64>> {
    let block_number =
        i64::try_from(block_number).map_err(|_| DbError::BlockOutOfRange(block_number))?;
    let mut inserted: Vec<(Vec<u8>, i64)> = Vec::with_capacity(new_orders.len());
    for chunk in new_orders.chunks(INSERT_CHUNK_ROWS) {
        inserted.extend(
            diesel::insert_into(orders::table)
                .values(chunk)
                .on_conflict(orders::note_id)
                .do_nothing()
                .returning((orders::note_id, orders::priority_seq))
                .get_results::<(Vec<u8>, i64)>(conn)?,
        );
    }
    let advanced = diesel::update(sync_state::table.find(1_i16))
        .set(sync_state::last_fetched_block.eq(block_number))
        .execute(conn)?;
    if advanced != 1 {
        return Err(DbError::MissingSyncCursor);
    }
    tracing::debug!(
        inserted_orders = inserted.len(),
        block_number,
        "persisted PostgreSQL ingest batch"
    );
    inserted
        .into_iter()
        .map(|(id, sequence)| Ok((id, u64::try_from(sequence)?)))
        .collect()
}

/// Which of these notes are already stored as orders. Startup recovery uses
/// this to replay only the client discoveries the database has not seen.
pub fn existing_note_ids_tx(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
) -> DbResult<HashSet<Vec<u8>>> {
    if note_ids.is_empty() {
        return Ok(HashSet::new());
    }
    Ok(orders::table
        .filter(orders::note_id.eq_any(note_ids))
        .select(orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect())
}

/// Every order that can trade now, through the one liveness rule, with maker
/// tags: startup hydration of the matcher.
pub fn load_live_orders_tx(conn: &mut PgConnection) -> DbResult<Vec<BookOrder>> {
    let rows: Vec<LiveOrderRow> = live_orders::table
        .order(live_orders::priority_seq.asc())
        .select(LiveOrderRow::as_select())
        .load(conn)?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        match row.into_book_order() {
            Ok(order) => result.push(order),
            Err(error) => {
                tracing::warn!(%error, "skipping active order whose stored note does not parse");
            }
        }
    }
    Ok(result)
}

/// Pass every order a book-changing transaction activates through the one
/// liveness rule, inside that transaction (`PgPool::write_book` calls this
/// for every update). Orders that are not live are dropped from the update:
/// rows no longer Active, and maker orders below a cutoff or stopped, which
/// are stored Stopped here since this write happens anyway. Live orders gain
/// their maker tag.
pub fn live_book_update_tx(
    conn: &mut PgConnection,
    mut update: BookUpdate,
) -> DbResult<BookUpdate> {
    if update.active.is_empty() {
        return Ok(update);
    }
    let ids: Vec<Vec<u8>> = update
        .active
        .iter()
        .map(|order| order.id().to_bytes().to_vec())
        .collect();
    let live: HashMap<Vec<u8>, Option<MakerTag>> = live_orders::table
        .filter(live_orders::note_id.eq_any(&ids))
        .select((
            live_orders::note_id,
            live_orders::maker_id,
            live_orders::root_seq,
        ))
        .load::<(Vec<u8>, Option<i64>, Option<i64>)>(conn)?
        .into_iter()
        .map(|(id, maker_id, root_seq)| Ok((id, maker_tag(maker_id, root_seq)?)))
        .collect::<DbResult<_>>()?;
    let excluded: Vec<Vec<u8>> = ids
        .into_iter()
        .filter(|id| !live.contains_key(id))
        .collect();
    if !excluded.is_empty() {
        // Only a maker order can be Active yet excluded by the rule.
        diesel::update(
            orders::table
                .filter(orders::note_id.eq_any(&excluded))
                .filter(orders::status.eq(OrderStatus::Active.as_str())),
        )
        .set(orders::status.eq(OrderStatus::Stopped.as_str()))
        .execute(conn)?;
    }
    update
        .active
        .retain_mut(|order| match live.get(order.id().to_bytes().as_slice()) {
            Some(tag) => {
                order.maker = *tag;
                true
            }
            None => false,
        });
    Ok(update)
}

/// Fill the order keys of unfinished rows stored before the maker migration,
/// so a lineage a maker claims later applies to them too. Run at startup,
/// before hydration; rows that cannot be parsed keep NULL and are never maker
/// orders.
pub fn backfill_order_keys_tx(conn: &mut PgConnection) -> DbResult<usize> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = orders::table
        .filter(orders::lineage_id.is_null())
        .filter(
            orders::status.eq_any([OrderStatus::Active.as_str(), OrderStatus::Settling.as_str()]),
        )
        .select((orders::note_id, orders::raw_data))
        .load(conn)?;
    let mut filled = 0;
    for (note_id, raw_data) in rows {
        let keys = Note::read_from(&mut SliceReader::new(&raw_data))
            .map_err(DbError::from)
            .and_then(|note| OrderKeyColumns::of(&note));
        match keys {
            Ok(keys) => {
                filled += diesel::update(orders::table.find(&note_id))
                    .set(&keys)
                    .execute(conn)?;
            }
            Err(error) => {
                tracing::warn!(note_id = %hex::encode(&note_id), %error, "stored order has no order keys");
            }
        }
    }
    Ok(filled)
}

/// Retire orders whose notes are consumed on chain. A consumed note is spent
/// whoever consumed it, so a parent reserved by an unresolved settlement is
/// retired too: confirmation still marks it Executed if our transaction was
/// the consumer, and a discarded settlement only releases parents that are
/// still Settling, so a consumed parent can never return to the book.
pub fn mark_orders_onchain_nullified_tx(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
) -> DbResult<usize> {
    if note_ids.is_empty() {
        return Ok(0);
    }
    let mut sorted = note_ids.to_vec();
    sorted.sort();
    sorted.dedup();
    super::maker_db::report_spent_tx(conn, &sorted)?;
    Ok(
        diesel::update(orders::table.filter(orders::note_id.eq_any(sorted)).filter(
            orders::status.eq_any([
                OrderStatus::Active.as_str(),
                OrderStatus::Settling.as_str(),
                OrderStatus::Stopped.as_str(),
            ]),
        ))
        .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
        .execute(conn)?,
    )
}

/// Reserve all parents after proof, before submission. A missing or consumed
/// parent rolls back the attempt row and every other parent transition.
pub fn prepare_settlement_tx(
    conn: &mut PgConnection,
    attempt: &SettlementAttemptRow,
    inputs: &[SettlementInputRow],
    cached_fills: Option<&maker_db::SettlementFills>,
) -> DbResult<()> {
    if inputs.is_empty() {
        return Err(SettlementError::NoInputs.into());
    }
    // The executor builds `attempt` and `inputs` from one executed transaction
    // (`SettlementAttemptRow::prepared`, `BatchComponents::settlement_inputs`),
    // so their IDs and child fields agree by construction.
    let mut parent_ids: Vec<_> = inputs
        .iter()
        .map(|input| input.parent_note_id.clone())
        .collect();
    parent_ids.sort();
    parent_ids.dedup();
    if parent_ids.len() != inputs.len() {
        return Err(SettlementError::DuplicateParent.into());
    }
    diesel::insert_into(settlement_attempts::table)
        .values(attempt)
        .execute(conn)?;
    let locked: Vec<OrderRow> = orders::table
        .filter(orders::note_id.eq_any(&parent_ids))
        .order(orders::note_id.asc())
        .for_update()
        .select(OrderRow::as_select())
        .load(conn)?;
    if locked.len() != parent_ids.len() {
        return Err(SettlementError::MissingInputOrder.into());
    }
    // Serialize with cancels: lock the control rows of the makers that own
    // these inputs, found from the locked rows (an attribution can arrive
    // after the matcher picked a candidate), in maker-ID order. A cancel
    // raising a cutoff takes the same lock, so either it sees this
    // reservation as exposure or this check sees its cutoff.
    let owners: Vec<i64> = orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(orders::note_id.eq_any(&parent_ids))
        .select(maker_lineages::maker_id)
        .distinct()
        .load(conn)?;
    if !owners.is_empty() {
        makers::table
            .filter(makers::maker_id.eq_any(&owners))
            .order(makers::maker_id.asc())
            .for_no_key_update()
            .select(makers::maker_id)
            .load::<i64>(conn)?;
    }
    let live: i64 = live_orders::table
        .filter(live_orders::note_id.eq_any(&parent_ids))
        .count()
        .get_result(conn)?;
    if usize::try_from(live)? != parent_ids.len() {
        return Err(SettlementError::InputOrderNotActive.into());
    }
    let parents: HashMap<&[u8], &OrderRow> = locked
        .iter()
        .map(|parent| (parent.note_id.as_slice(), parent))
        .collect();
    // The only validation of a child: confirmation and ingest rely on it.
    for input in inputs {
        if let Some(raw_child) = &input.child_note_data {
            let child = Note::read_from(&mut SliceReader::new(raw_child))?;
            let parent = parents
                .get(input.parent_note_id.as_slice())
                .ok_or(SettlementError::MissingInputOrder)?;
            validate_remainder(parent, &child)?;
        }
    }
    let changed = diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(&parent_ids))
            .filter(orders::status.eq(OrderStatus::Active.as_str())),
    )
    .set(orders::status.eq(OrderStatus::Settling.as_str()))
    .execute(conn)?;
    diesel::insert_into(settlement_inputs::table)
        .values(inputs)
        .execute(conn)?;
    let report_inputs = inputs
        .iter()
        .map(|input| {
            let parent = parents
                .get(input.parent_note_id.as_slice())
                .ok_or(SettlementError::MissingInputOrder)?;
            Ok((input, *parent))
        })
        .collect::<DbResult<Vec<_>>>()?;
    maker_db::report_settlement_tx(
        conn,
        &attempt.tx_id,
        &report_inputs,
        SettlementPhase::Pending,
        cached_fills,
    )?;
    tracing::info!(
        tx_id = %hex::encode(&attempt.tx_id),
        parents = changed,
        status = "prepared",
        "persisted settlement reservation"
    );
    Ok(())
}

fn transition_attempt_status_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    from: &[SettlementStatus],
    to: SettlementStatus,
) -> DbResult<()> {
    let from: Vec<_> = from.iter().map(|status| status.as_str()).collect();
    let to = to.as_str();
    let changed = diesel::update(
        settlement_attempts::table
            .find(tx_id)
            .filter(settlement_attempts::status.eq_any(from)),
    )
    .set(settlement_attempts::status.eq(to))
    .execute(conn)?;
    if changed == 1 {
        return Ok(());
    }
    let current: Option<String> = settlement_attempts::table
        .find(tx_id)
        .select(settlement_attempts::status)
        .first(conn)
        .optional()?;
    match current.as_deref() {
        // Repeated observations are harmless.
        Some(status) if status == to => Ok(()),
        Some(status) => Err(DbError::InvalidTransition {
            from: status.to_owned(),
            to,
        }),
        None => Err(DbError::MissingAttempt(to)),
    }
}

pub fn mark_settlement_uncertain_tx(conn: &mut PgConnection, tx_id: &[u8]) -> DbResult<()> {
    transition_attempt_status_tx(
        conn,
        tx_id,
        &[SettlementStatus::Prepared],
        SettlementStatus::Uncertain,
    )
}

pub fn mark_settlement_rejected_tx(conn: &mut PgConnection, tx_id: &[u8]) -> DbResult<()> {
    transition_attempt_status_tx(
        conn,
        tx_id,
        &[SettlementStatus::Prepared, SettlementStatus::Uncertain],
        SettlementStatus::Rejected,
    )
}

/// Hydrate unresolved attempts and every parent from one PostgreSQL statement.
/// One statement gives a consistent snapshot without a multi-query recovery
/// transaction, and an attempt missing any required row is a startup error.
#[derive(Debug)]
pub struct UnresolvedAttempt {
    pub attempt: SettlementAttemptRow,
    pub parents: Vec<BookOrder>,
    /// Remainders the transaction creates when it commits.
    pub children: Vec<Note>,
    pub fills: maker_db::SettlementFills,
}

type UnresolvedAttemptJoinRow = (
    SettlementAttemptRow,
    Option<SettlementInputRow>,
    Option<OrderRow>,
);

pub fn load_unresolved_attempts_tx(conn: &mut PgConnection) -> DbResult<Vec<UnresolvedAttempt>> {
    let rows: Vec<UnresolvedAttemptJoinRow> = settlement_attempts::table
        .left_join(
            settlement_inputs::table.on(settlement_attempts::tx_id.eq(settlement_inputs::tx_id)),
        )
        .left_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .order((
            settlement_attempts::tx_id.asc(),
            settlement_inputs::parent_note_id.asc(),
        ))
        .select((
            SettlementAttemptRow::as_select(),
            Option::<SettlementInputRow>::as_select(),
            Option::<OrderRow>::as_select(),
        ))
        .load(conn)?;
    let mut attempts = BTreeMap::<Vec<u8>, UnresolvedAttempt>::new();
    for (attempt, input, parent_row) in rows {
        let input = input.ok_or(DbError::Corrupt("unresolved settlement has no input"))?;
        let parent_row = parent_row.ok_or(DbError::Corrupt(
            "unresolved settlement parent order is missing",
        ))?;
        let parent = parent_row.into_book_order()?;
        let child = input
            .child_note_data
            .as_deref()
            .map(|raw| Note::read_from(&mut SliceReader::new(raw)))
            .transpose()?;
        let entry = attempts
            .entry(attempt.tx_id.clone())
            .or_insert_with(|| UnresolvedAttempt {
                attempt,
                parents: Vec::new(),
                children: Vec::new(),
                fills: HashMap::new(),
            });
        if let Some(amount) = input.fill_amount {
            entry.fills.insert(
                input.parent_note_id.clone(),
                maker_db::input_fill(&parent.note, u64::try_from(amount)?, child.as_ref())?,
            );
        }
        entry.parents.push(parent);
        entry.children.extend(child);
    }
    Ok(attempts.into_values().collect())
}

/// Lock an unresolved attempt row; `false` when it was already resolved.
fn lock_attempt(conn: &mut PgConnection, tx_id: &[u8]) -> DbResult<bool> {
    Ok(settlement_attempts::table
        .find(tx_id)
        .for_update()
        .select(settlement_attempts::tx_id)
        .first::<Vec<u8>>(conn)
        .optional()?
        .is_some())
}

/// A remainder must be a valid PSWAP order of its parent's creator, and the
/// parent must hold a FIFO slot for it to inherit.
fn validate_remainder(parent: &OrderRow, child: &Note) -> DbResult<()> {
    let child_terms = Order::from_note(child)?;
    let parent_terms = Order::from_note(&parent.note()?)?;
    if child_terms.creator_id != parent_terms.creator_id {
        return Err(SettlementError::InvalidRemainder.into());
    }
    if parent.priority_seq <= 0 {
        return Err(DbError::Corrupt("parent order lacks a FIFO priority"));
    }
    Ok(())
}

/// A remainder inherits its parent's FIFO slot and arrival time. Its ID
/// matched a child validated at prepare, and a note ID commits to the note's
/// contents, so the row is built directly.
fn remainder_of(
    parent: &OrderRow,
    note: Note,
    raw_data: Vec<u8>,
) -> DbResult<(NewRemainderOrderRow, BookOrder)> {
    let row = NewRemainderOrderRow {
        note_id: note.id().to_bytes().to_vec(),
        raw_data,
        arrival_unix: parent.arrival_unix,
        priority_seq: parent.priority_seq,
        keys: OrderKeyColumns::of(&note)?,
    };
    let order = BookOrder {
        priority_seq: u64::try_from(parent.priority_seq)?,
        arrival_unix: u64::try_from(parent.arrival_unix)?,
        note: std::sync::Arc::new(note),
        maker: None,
    };
    Ok((row, order))
}

/// Insert remainders, skipping any already stored (ingest and confirmation
/// may both see one). Returns the IDs inserted by this call.
fn insert_remainders(
    conn: &mut PgConnection,
    rows: &[NewRemainderOrderRow],
) -> DbResult<HashSet<Vec<u8>>> {
    if rows.is_empty() {
        return Ok(HashSet::new());
    }
    Ok(diesel::insert_into(orders::table)
        .values(rows)
        .on_conflict(orders::note_id)
        .do_nothing()
        .returning(orders::note_id)
        .get_results::<Vec<u8>>(conn)?
        .into_iter()
        .collect())
}

/// Release a settlement whose transaction never commits, once the parents'
/// on-chain nullifiers are known. Consumed parents retire, the rest return to
/// the book, and the attempt is deleted. A missing attempt was already
/// resolved, so this is a no-op.
pub fn finish_discarded_settlement_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    consumed: &HashSet<OrderId>,
) -> DbResult<BookUpdate> {
    if !lock_attempt(conn, tx_id)? {
        return Ok(BookUpdate::default());
    }
    let inputs: Vec<(SettlementInputRow, OrderRow)> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .order(settlement_inputs::parent_note_id.asc())
        .for_update()
        .select((SettlementInputRow::as_select(), OrderRow::as_select()))
        .load(conn)?;

    let mut active_ids = Vec::new();
    let mut consumed_ids = Vec::new();
    let mut update = BookUpdate::default();
    for (_, row) in &inputs {
        // Only a reserved parent is ours to release.
        if row.status != OrderStatus::Settling.as_str() {
            tracing::warn!(note_id = %hex::encode(&row.note_id), status = %row.status, "discarded settlement parent is not settling; leaving it");
            continue;
        }
        let parent = row.clone().into_book_order()?;
        let id = parent.id();
        if consumed.contains(&id) {
            consumed_ids.push(id.to_bytes().to_vec());
            update.removed.push(id);
        } else {
            active_ids.push(id.to_bytes().to_vec());
            update.active.push(parent);
        }
    }
    for (ids, status) in [
        (&active_ids, OrderStatus::Active),
        (&consumed_ids, OrderStatus::OnchainNullified),
    ] {
        if !ids.is_empty() {
            diesel::update(orders::table.filter(orders::note_id.eq_any(ids)))
                .set(orders::status.eq(status.as_str()))
                .execute(conn)?;
        }
    }
    let report_inputs: Vec<_> = inputs
        .iter()
        .map(|(input, parent)| (input, parent))
        .collect();
    maker_db::report_settlement_tx(conn, tx_id, &report_inputs, SettlementPhase::Voided, None)?;
    diesel::delete(settlement_attempts::table.find(tx_id)).execute(conn)?;
    tracing::info!(
        tx_id = %hex::encode(tx_id),
        reactivated = active_ids.len(),
        consumed = consumed_ids.len(),
        status = "discarded",
        "persisted discarded settlement"
    );
    Ok(update)
}

/// Called only by the executor, after the node confirmed our transaction ID:
/// retire the parents, add any remainder not yet ingested with the parent's
/// FIFO slot, and delete the attempt. A missing attempt was already resolved,
/// so this is a no-op.
///
/// `consumed_children` are remainders whose nullifiers the executor already
/// found on chain: they keep the parent's FIFO slot but are stored
/// OnchainNullified and never activated.
pub fn confirm_settlement_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    consumed_children: &HashSet<OrderId>,
    commit_block: BlockNumber,
    consumer: AccountId,
    cached_fills: Option<&maker_db::SettlementFills>,
) -> DbResult<BookUpdate> {
    if !lock_attempt(conn, tx_id)? {
        return Ok(BookUpdate::default());
    }
    let rows: Vec<(SettlementInputRow, OrderRow)> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .order(settlement_inputs::parent_note_id.asc())
        .for_update()
        .select((SettlementInputRow::as_select(), OrderRow::as_select()))
        .load(conn)?;

    let mut removed = Vec::with_capacity(rows.len());
    let mut children = Vec::new();
    let mut child_rows = Vec::new();
    let mut parent_ids = Vec::with_capacity(rows.len());
    for (input, parent) in &rows {
        // Our transaction consumed it, whatever the database last recorded.
        if parent.status != OrderStatus::Settling.as_str() {
            tracing::warn!(note_id = %hex::encode(&parent.note_id), status = %parent.status, "confirmed parent was not settling; retiring it");
        }
        removed.push(OrderId::read_from(&mut SliceReader::new(
            &input.parent_note_id,
        ))?);
        parent_ids.push(input.parent_note_id.clone());
        // Validated against this parent at prepare time.
        if let Some(raw) = &input.child_note_data {
            let child_note = Note::read_from(&mut SliceReader::new(raw))?;
            let (row, child) = remainder_of(parent, child_note, raw.clone())?;
            child_rows.push(row);
            children.push(child);
        }
    }
    // Ingest may already have inserted and announced a remainder; only the
    // ones inserted here are new to the matcher.
    let newly_inserted = insert_remainders(conn, &child_rows)?;
    let consumed_child_ids: Vec<_> = children
        .iter()
        .filter(|child| consumed_children.contains(&child.id()))
        .map(|child| child.id().to_bytes().to_vec())
        .collect();
    if !consumed_child_ids.is_empty() {
        diesel::update(
            orders::table
                .filter(orders::note_id.eq_any(&consumed_child_ids))
                .filter(orders::status.eq(OrderStatus::Active.as_str())),
        )
        .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
        .execute(conn)?;
        removed.extend(
            children
                .iter()
                .map(BookOrder::id)
                .filter(|id| consumed_children.contains(id)),
        );
    }
    diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(&parent_ids))
            .filter(orders::status.ne(OrderStatus::Executed.as_str())),
    )
    .set(orders::status.eq(OrderStatus::Executed.as_str()))
    .execute(conn)?;
    let committed = SettlementPhase::Committed {
        block: commit_block,
        consumer,
    };
    let report_inputs: Vec<_> = rows.iter().map(|(input, parent)| (input, parent)).collect();
    maker_db::report_settlement_tx(conn, tx_id, &report_inputs, committed, cached_fills)?;
    diesel::delete(settlement_attempts::table.find(tx_id)).execute(conn)?;
    let active: Vec<BookOrder> = children
        .into_iter()
        .filter(|child| {
            let id = child.id();
            newly_inserted.contains(id.to_bytes().as_slice()) && !consumed_children.contains(&id)
        })
        .collect();
    tracing::info!(
        tx_id = %hex::encode(tx_id),
        parents = parent_ids.len(),
        remainders = active.len(),
        status = "confirmed",
        "persisted settlement confirmation"
    );
    Ok(BookUpdate {
        removed,
        active,
        maker_updates: Vec::new(),
    })
}

/// Ingest observed notes that our own settlements expect as remainders. Each
/// is a live order on chain whoever created it — another filler of the same
/// parent and amount produces an identical note — so it is inserted Active
/// with its parent's FIFO slot. The settlement itself is not touched: only
/// the executor confirms it, from our transaction ID.
///
/// Returns the IDs recognised as expected remainders (so ingest does not
/// also treat them as fresh orders) and the newly activated ones.
pub fn ingest_expected_remainders_tx(
    conn: &mut PgConnection,
    observed: &[Note],
) -> DbResult<(HashSet<Vec<u8>>, Vec<BookOrder>)> {
    if observed.is_empty() {
        return Ok((HashSet::new(), Vec::new()));
    }
    let observed_ids: Vec<_> = observed
        .iter()
        .map(|note| note.id().to_bytes().to_vec())
        .collect();
    let parents: HashMap<Vec<u8>, OrderRow> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .filter(settlement_inputs::child_note_id.eq_any(&observed_ids))
        .select((
            settlement_inputs::child_note_id.assume_not_null(),
            OrderRow::as_select(),
        ))
        .load::<(Vec<u8>, OrderRow)>(conn)?
        .into_iter()
        .collect();
    let expected: HashSet<Vec<u8>> = parents.keys().cloned().collect();
    let mut rows = Vec::new();
    let mut candidates = Vec::new();
    for note in observed {
        let Some(parent) = parents.get(note.id().to_bytes().as_slice()) else {
            continue;
        };
        let (row, candidate) = remainder_of(parent, note.clone(), note.to_bytes())?;
        rows.push(row);
        candidates.push(candidate);
    }
    let inserted = insert_remainders(conn, &rows)?;
    let active = candidates
        .into_iter()
        .filter(|order| inserted.contains(order.id().to_bytes().as_slice()))
        .collect();
    Ok((expected, active))
}

pub fn get_registered_tokens_tx(conn: &mut PgConnection) -> DbResult<Vec<RegisteredTokenRow>> {
    Ok(registered_tokens::table
        .select(RegisteredTokenRow::as_select())
        .load(conn)?)
}

pub fn get_registered_token_tx(
    conn: &mut PgConnection,
    token: TokenId,
) -> DbResult<Option<RegisteredTokenRow>> {
    Ok(registered_tokens::table
        .find(token.to_bytes())
        .select(RegisteredTokenRow::as_select())
        .first(conn)
        .optional()?)
}

/// Serve a whole public price batch in one PostgreSQL query.
pub fn fetch_token_rows_tx(
    conn: &mut PgConnection,
    token_ids: &[Vec<u8>],
) -> DbResult<HashMap<Vec<u8>, RegisteredTokenRow>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(registered_tokens::table
        .filter(registered_tokens::token_id.eq_any(token_ids))
        .select(RegisteredTokenRow::as_select())
        .load::<RegisteredTokenRow>(conn)?
        .into_iter()
        .map(|row| (row.token_id.clone(), row))
        .collect())
}

pub fn register_token_tx(
    conn: &mut PgConnection,
    token: TokenId,
    external_symbol: Option<&str>,
) -> DbResult<bool> {
    let row = RegisteredTokenRow {
        token_id: token.to_bytes(),
        external_symbol: external_symbol.map(str::to_owned),
        decimals: None,
        ticker: None,
    };
    Ok(diesel::insert_into(registered_tokens::table)
        .values(&row)
        .on_conflict(registered_tokens::token_id)
        .do_nothing()
        .execute(conn)?
        == 1)
}

pub fn set_token_metadata_tx(
    conn: &mut PgConnection,
    token: TokenId,
    decimals: Option<u8>,
    ticker: Option<&str>,
) -> DbResult<bool> {
    Ok(
        diesel::update(registered_tokens::table.find(token.to_bytes()))
            .set((
                registered_tokens::decimals.eq(decimals.map(i32::from)),
                registered_tokens::ticker.eq(ticker.map(str::to_owned)),
            ))
            .execute(conn)?
            == 1,
    )
}

pub fn update_token_symbol_tx(
    conn: &mut PgConnection,
    token: TokenId,
    symbol: Option<&str>,
) -> DbResult<bool> {
    Ok(
        diesel::update(registered_tokens::table.find(token.to_bytes()))
            .set(registered_tokens::external_symbol.eq(symbol.map(str::to_owned)))
            .execute(conn)?
            == 1,
    )
}

pub fn unregister_token_tx(conn: &mut PgConnection, token: TokenId) -> DbResult<bool> {
    Ok(diesel::delete(registered_tokens::table.find(token.to_bytes())).execute(conn)? == 1)
}

pub fn load_token_symbols_tx(conn: &mut PgConnection) -> DbResult<HashMap<TokenId, String>> {
    let mut result = HashMap::new();
    for row in get_registered_tokens_tx(conn)? {
        if let Some(symbol) = &row.external_symbol {
            result.insert(row.token()?, symbol.clone());
        }
    }
    Ok(result)
}

/// On-chain decimals of every registered token that has them.
pub fn load_token_decimals_tx(conn: &mut PgConnection) -> DbResult<HashMap<TokenId, u8>> {
    let mut result = HashMap::new();
    for row in get_registered_tokens_tx(conn)? {
        if let Some(decimals) = row.token_decimals() {
            result.insert(row.token()?, decimals);
        }
    }
    Ok(result)
}

pub fn load_registered_tokens_tx(conn: &mut PgConnection) -> DbResult<Vec<TokenId>> {
    get_registered_tokens_tx(conn)?
        .into_iter()
        .map(|row| row.token())
        .collect()
}

pub fn seed_tokens_from_config_tx(
    conn: &mut PgConnection,
    tokens: &[(TokenId, Option<String>)],
) -> DbResult<()> {
    for (token, symbol) in tokens {
        register_token_tx(conn, *token, symbol.as_deref())?;
    }
    Ok(())
}

/// The account a test settlement is consumed by.
#[cfg(test)]
pub(crate) fn test_consumer() -> AccountId {
    miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
        .try_into()
        .expect("valid test account ID")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_migrations;
    use crate::db::postgres_test::TestSchema;
    use anyhow::Result;
    use diesel::connection::SimpleConnection;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    use std::sync::{Arc, Barrier};

    fn order_note(serial_number: Word) -> Result<Note> {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        Ok(PswapNote::builder()
            .sender(creator)
            .serial_number(serial_number)
            .note_type(NoteType::Public)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 10)?)
                    .min_fill_step(AssetAmount::new(1)?)
                    .creator_account_id(creator)
                    .build(),
            )
            .offered_asset(FungibleAsset::new(offered, 10)?)
            .build()?
            .into())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn bulk_511_parent_transition_is_atomic_and_idempotent() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let mut order_rows = Vec::with_capacity(511);
        let mut inputs = Vec::with_capacity(511);
        let mut rng = RandomCoin::new(Word::default());
        for _ in 0..511 {
            let note = order_note(rng.draw_word())?;
            let child = order_note(rng.draw_word())?;
            let id = note.id().to_bytes();
            let order_row = NewOrderRow::ingested(&note, 10)?;
            order_rows.push(order_row);
            inputs.push(SettlementInputRow {
                tx_id: vec![7],
                parent_note_id: id,
                child_note_id: Some(child.id().to_bytes()),
                child_note_data: Some(child.to_bytes()),
                fill_amount: None,
            });
        }
        let inserted = conn
            .transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &order_rows, 42))?;
        assert_eq!(inserted.len(), 511);
        assert!(inserted.values().all(|priority| *priority > 0));
        assert_eq!(get_last_fetched_block_tx(conn)?, 42);
        let duplicate = conn
            .transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &order_rows, 43))?;
        assert!(duplicate.is_empty());
        assert_eq!(get_last_fetched_block_tx(conn)?, 43);

        let attempt = SettlementAttemptRow {
            tx_id: vec![7],
            tx_result: vec![9],
            status: "prepared".into(),
        };
        conn.transaction::<_, DbError, _>(|conn| {
            prepare_settlement_tx(conn, &attempt, &inputs, None)
        })?;
        let settling: i64 = orders::table
            .filter(orders::status.eq(OrderStatus::Settling.as_str()))
            .count()
            .get_result(conn)?;
        assert_eq!(settling, 511);

        let competing = SettlementAttemptRow {
            tx_id: vec![10],
            tx_result: vec![11],
            status: "prepared".into(),
        };
        let competing_inputs: Vec<_> = inputs
            .iter()
            .cloned()
            .map(|mut input| {
                input.tx_id = vec![10];
                input
            })
            .collect();
        assert!(conn
            .transaction::<_, DbError, _>(|conn| {
                prepare_settlement_tx(conn, &competing, &competing_inputs, None)
            })
            .is_err());
        let attempts: i64 = settlement_attempts::table.count().get_result(conn)?;
        let mappings: i64 = settlement_inputs::table.count().get_result(conn)?;
        assert_eq!((attempts, mappings), (1, 511));

        let update = conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_tx(
                conn,
                &attempt.tx_id,
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                None,
            )
        })?;
        assert_eq!(update.removed.len(), 511);
        assert_eq!(update.active.len(), 511);
        let children: HashMap<_, _> = update
            .active
            .iter()
            .map(|child| (child.id().to_bytes(), child.priority_seq))
            .collect();
        for input in &inputs {
            assert_eq!(
                children[&input.child_note_id.clone().expect("child ID")],
                inserted[&input.parent_note_id],
                "a remainder must retain its parent's FIFO priority"
            );
        }
        let duplicate = conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_tx(
                conn,
                &attempt.tx_id,
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                None,
            )
        })?;
        assert!(duplicate.is_empty());
        let executed: i64 = orders::table
            .filter(orders::status.eq(OrderStatus::Executed.as_str()))
            .count()
            .get_result(conn)?;
        assert_eq!(executed, 511);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn two_connections_confirm_once() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let note = order_note(Word::default())?;
        let id = note.id().to_bytes();
        let order_row = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, DbError, _>(|conn| {
            insert_orders_batch_tx(conn, &[order_row], 1)?;
            prepare_settlement_tx(
                conn,
                &SettlementAttemptRow {
                    tx_id: vec![7],
                    tx_result: vec![9],
                    status: "prepared".into(),
                },
                &[SettlementInputRow {
                    tx_id: vec![7],
                    parent_note_id: id.clone(),
                    child_note_id: None,
                    child_note_data: None,
                    fill_amount: None,
                }],
                None,
            )
        })?;

        let barrier = Arc::new(Barrier::new(2));
        let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let mut workers = Vec::new();
        for _ in 0..2 {
            let url = url.clone();
            let schema = fixture.name.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || -> Result<usize> {
                let mut conn = postgres_migrations::connect(&url)?;
                conn.batch_execute(&format!("SET search_path TO {schema}"))?;
                barrier.wait();
                let update = conn.transaction::<_, DbError, _>(|conn| {
                    confirm_settlement_tx(
                        conn,
                        &[7],
                        &HashSet::new(),
                        BlockNumber::GENESIS,
                        test_consumer(),
                        None,
                    )
                })?;
                Ok(update.removed.len())
            }));
        }
        let mut result: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("confirmation worker panicked"))
            .collect::<Result<_>>()?;
        result.sort();
        assert_eq!(result, vec![0, 1]);
        // Confirmation deletes the attempt; the parent is retired.
        let remaining: i64 = settlement_attempts::table
            .count()
            .get_result(&mut fixture.conn)?;
        assert_eq!(remaining, 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn two_connections_cannot_reserve_the_same_parent() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let note = order_note(Word::default())?;
        let order_row = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, DbError, _>(|conn| {
            insert_orders_batch_tx(conn, &[order_row], 1)?;
            Ok(())
        })?;

        let barrier = Arc::new(Barrier::new(2));
        let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let mut workers = Vec::new();
        for attempt_id in [31_u8, 32_u8] {
            let schema = fixture.name.clone();
            let url = url.clone();
            let barrier = barrier.clone();
            let parent_id = note.id().to_bytes();
            workers.push(std::thread::spawn(move || -> Result<bool> {
                let mut conn = postgres_migrations::connect(&url)?;
                conn.batch_execute(&format!("SET search_path TO {schema}"))?;
                let attempt = SettlementAttemptRow {
                    tx_id: vec![attempt_id],
                    tx_result: vec![attempt_id],
                    status: "prepared".into(),
                };
                let input = SettlementInputRow {
                    tx_id: vec![attempt_id],
                    parent_note_id: parent_id,
                    child_note_id: None,
                    child_note_data: None,
                    fill_amount: None,
                };
                barrier.wait();
                match conn.transaction::<_, DbError, _>(|conn| {
                    prepare_settlement_tx(conn, &attempt, &[input], None)
                }) {
                    Ok(()) => Ok(true),
                    Err(DbError::Settlement(SettlementError::InputOrderNotActive)) => Ok(false),
                    Err(error) => Err(error.into()),
                }
            }));
        }
        let mut reserved: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("prepare worker panicked"))
            .collect::<Result<_>>()?;
        reserved.sort();
        assert_eq!(reserved, vec![false, true]);
        let attempts: i64 = settlement_attempts::table
            .count()
            .get_result(&mut fixture.conn)?;
        let inputs: i64 = settlement_inputs::table
            .count()
            .get_result(&mut fixture.conn)?;
        assert_eq!((attempts, inputs), (1, 1));
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn confirmation_and_discard_never_split_a_transition() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let note = order_note(Word::default())?;
        let order_row = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, DbError, _>(|conn| {
            insert_orders_batch_tx(conn, &[order_row], 1)?;
            prepare_settlement_tx(
                conn,
                &SettlementAttemptRow {
                    tx_id: vec![41],
                    tx_result: vec![42],
                    status: "prepared".into(),
                },
                &[SettlementInputRow {
                    tx_id: vec![41],
                    parent_note_id: note.id().to_bytes(),
                    child_note_id: None,
                    child_note_data: None,
                    fill_amount: None,
                }],
                None,
            )
        })?;
        let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let schema = fixture.name.clone();
        let barrier = Arc::new(Barrier::new(2));
        let confirm_barrier = barrier.clone();
        let confirm_url = url.clone();
        let confirm_schema = schema.clone();
        let confirmer = std::thread::spawn(move || -> Result<BookUpdate> {
            let mut conn = postgres_migrations::connect(&confirm_url)?;
            conn.batch_execute(&format!("SET search_path TO {confirm_schema}"))?;
            confirm_barrier.wait();
            conn.transaction::<_, DbError, _>(|conn| {
                confirm_settlement_tx(
                    conn,
                    &[41],
                    &HashSet::new(),
                    BlockNumber::GENESIS,
                    test_consumer(),
                    None,
                )
            })
            .map_err(Into::into)
        });
        let discarder = std::thread::spawn(move || -> Result<BookUpdate> {
            let mut conn = postgres_migrations::connect(&url)?;
            conn.batch_execute(&format!("SET search_path TO {schema}"))?;
            barrier.wait();
            conn.transaction::<_, DbError, _>(|conn| {
                finish_discarded_settlement_tx(conn, &[41], &HashSet::new())
            })
            .map_err(Into::into)
        });
        let confirmed = confirmer.join().expect("confirmation worker panicked")?;
        let discarded = discarder.join().expect("discard worker panicked")?;
        // Whichever commits first resolves the attempt; the other is a no-op.
        let remaining: i64 = settlement_attempts::table
            .count()
            .get_result(&mut fixture.conn)?;
        assert_eq!(remaining, 0);
        let parent_status: String = orders::table
            .find(note.id().to_bytes())
            .select(orders::status)
            .first(&mut fixture.conn)?;
        if parent_status == OrderStatus::Executed.as_str() {
            assert_eq!(confirmed.removed.len(), 1);
            assert!(discarded.is_empty());
        } else {
            assert_eq!(parent_status, OrderStatus::Active.as_str());
            assert!(confirmed.is_empty());
            assert_eq!(discarded.active.len(), 1);
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn nullifier_observation_and_prepare_serialize() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let note = order_note(Word::default())?;
        let parent_id = note.id().to_bytes();
        let order_row = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, DbError, _>(|conn| {
            insert_orders_batch_tx(conn, &[order_row], 1)?;
            Ok(())
        })?;
        let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let schema = fixture.name.clone();
        let barrier = Arc::new(Barrier::new(2));
        let prepare_barrier = barrier.clone();
        let prepare_url = url.clone();
        let prepare_schema = schema.clone();
        let prepare_parent = parent_id.clone();
        let preparer = std::thread::spawn(move || -> Result<bool> {
            let mut conn = postgres_migrations::connect(&prepare_url)?;
            conn.batch_execute(&format!("SET search_path TO {prepare_schema}"))?;
            prepare_barrier.wait();
            match conn.transaction::<_, DbError, _>(|conn| {
                prepare_settlement_tx(
                    conn,
                    &SettlementAttemptRow {
                        tx_id: vec![51],
                        tx_result: vec![52],
                        status: "prepared".into(),
                    },
                    &[SettlementInputRow {
                        tx_id: vec![51],
                        parent_note_id: prepare_parent,
                        child_note_id: None,
                        child_note_data: None,
                        fill_amount: None,
                    }],
                    None,
                )
            }) {
                Ok(()) => Ok(true),
                Err(DbError::Settlement(SettlementError::InputOrderNotActive)) => Ok(false),
                Err(error) => Err(error.into()),
            }
        });
        let observer = std::thread::spawn(move || -> Result<usize> {
            let mut conn = postgres_migrations::connect(&url)?;
            conn.batch_execute(&format!("SET search_path TO {schema}"))?;
            barrier.wait();
            conn.transaction::<_, DbError, _>(|conn| {
                mark_orders_onchain_nullified_tx(conn, &[parent_id])
            })
            .map_err(Into::into)
        });
        let prepared = preparer.join().expect("prepare worker panicked")?;
        let changed = observer.join().expect("nullifier worker panicked")?;
        let attempts: i64 = settlement_attempts::table
            .count()
            .get_result(&mut fixture.conn)?;
        let status: String = orders::table
            .find(note.id().to_bytes())
            .select(orders::status)
            .first(&mut fixture.conn)?;
        // Whichever commits first, the consumed note ends retired: reserved
        // or not, a spent parent never stays live.
        assert_eq!(attempts, i64::from(prepared));
        assert_eq!(changed, 1);
        assert_eq!(status, OrderStatus::OnchainNullified.as_str());
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn recovery_rejects_attempt_without_inputs() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        diesel::insert_into(settlement_attempts::table)
            .values(SettlementAttemptRow {
                tx_id: vec![21],
                tx_result: vec![22],
                status: "prepared".into(),
            })
            .execute(&mut fixture.conn)?;
        let error = load_unresolved_attempts_tx(&mut fixture.conn)
            .expect_err("incomplete attempt must not disappear from recovery");
        assert!(
            matches!(
                error,
                DbError::Corrupt("unresolved settlement has no input")
            ),
            "{error:?}"
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn invalid_ingest_and_child_rows_roll_back() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let note = order_note(Word::default())?;
        // Only a valid PSWAP note becomes an order row.
        let not_an_order = miden_standards::note::P2idNote::builder()
            .sender(note.metadata().sender())
            .target(note.metadata().sender())
            .asset(
                *note
                    .assets()
                    .iter()
                    .next()
                    .expect("order note has an asset"),
            )
            .serial_number(Word::default())
            .build()?;
        assert!(NewOrderRow::ingested(&Note::from(not_an_order), 10).is_err());

        let order_row = NewOrderRow::ingested(&note, 10)?;
        conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &[order_row], 1))?;
        let attempt = SettlementAttemptRow {
            tx_id: vec![7],
            tx_result: vec![8],
            status: "prepared".into(),
        };
        let input = SettlementInputRow {
            tx_id: attempt.tx_id.clone(),
            parent_note_id: note.id().to_bytes(),
            child_note_id: Some(vec![10]),
            child_note_data: Some(vec![11]),
            fill_amount: None,
        };
        assert!(conn
            .transaction::<_, DbError, _>(|conn| {
                prepare_settlement_tx(conn, &attempt, &[input], None)
            })
            .is_err());
        assert_eq!(
            settlement_attempts::table.count().get_result::<i64>(conn)?,
            0
        );
        let status: String = orders::table
            .find(note.id().to_bytes())
            .select(orders::status)
            .first(conn)?;
        assert_eq!(status, OrderStatus::Active.as_str());
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn missing_sync_cursor_rolls_back_the_entire_ingest_batch() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        diesel::delete(sync_state::table.find(1_i16)).execute(conn)?;
        let note = order_note(Word::default())?;
        let order_row = NewOrderRow::ingested(&note, 10)?;
        let error = conn
            .transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &[order_row], 9))
            .expect_err("ingest must not commit notes without its sync cursor");
        assert!(matches!(error, DbError::MissingSyncCursor), "{error:?}");
        assert_eq!(orders::table.count().get_result::<i64>(conn)?, 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn token_registry_bulk_lookup_and_validation() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let first: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let second: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        let first_id = first.to_bytes();
        let second_id = second.to_bytes();
        assert!(register_token_tx(conn, first, Some("usd-coin"))?);
        assert!(!register_token_tx(conn, first, Some("ignored"))?);
        assert!(register_token_tx(conn, second, None)?);
        assert!(set_token_metadata_tx(conn, first, Some(6), Some("USDC"))?);
        assert!(set_token_metadata_tx(conn, second, Some(18), Some("WETH"))?);

        let rows = fetch_token_rows_tx(conn, &[first_id.clone(), second_id.clone()])?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[&first_id].decimals, Some(6));
        assert_eq!(rows[&first_id].ticker.as_deref(), Some("USDC"));
        assert_eq!(rows[&first_id].external_symbol.as_deref(), Some("usd-coin"));
        assert!(update_token_symbol_tx(conn, first, Some("usd-coin-new"))?);
        assert_eq!(
            load_token_symbols_tx(conn)?.get(&first).map(String::as_str),
            Some("usd-coin-new")
        );
        assert!(unregister_token_tx(conn, second)?);
        assert!(!unregister_token_tx(conn, second)?);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn ingest_batch_beyond_the_bind_parameter_limit_is_chunked() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        // Seven binds per row: 10,000 rows need 70,000 parameters, more than
        // the 65,535 PostgreSQL accepts in one statement.
        let rows: Vec<NewOrderRow> = (0..10_000_u32)
            .map(|index| NewOrderRow {
                note_id: index.to_be_bytes().to_vec(),
                raw_data: vec![1],
                arrival_unix: 1,
                keys: OrderKeyColumns {
                    lineage_id: index.to_be_bytes().to_vec(),
                    depth: 0,
                    market: vec![1],
                    direction: vec![1],
                },
            })
            .collect();
        let inserted =
            conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &rows, 5))?;
        assert_eq!(inserted.len(), 10_000);
        let priorities: HashSet<u64> = inserted.values().copied().collect();
        assert_eq!(
            priorities.len(),
            10_000,
            "every order gets its own FIFO slot"
        );
        assert_eq!(get_last_fetched_block_tx(conn)?, 5);

        let again =
            conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &rows, 6))?;
        assert!(again.is_empty(), "a replayed batch inserts nothing");
        Ok(())
    }
}

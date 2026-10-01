//! PostgreSQL transaction-scoped application queries.
//!
//! These helpers never start transactions or check out connections. Call them
//! only from `PgPool::{read,write,write_book}` so synchronous libpq work stays
//! on blocking workers and each lifecycle transition owns one transaction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;

use super::postgres_models::{
    NewOrderRow, NewRemainderOrderRow, NoteRow, OrderRow, RegisteredTokenRow, SettlementAttemptRow,
    SettlementInputRow, SettlementStatus,
};
use super::postgres_schema::{
    notes, orders, registered_tokens, settlement_attempts, settlement_inputs, sync_state,
};
use crate::types::{BookOrder, BookUpdate, OrderId, OrderStatus, SettlementError, TokenId};

pub fn get_last_fetched_block_tx(conn: &mut PgConnection) -> Result<u64> {
    let block: i64 = sync_state::table
        .find(1_i16)
        .select(sync_state::last_fetched_block)
        .first(conn)?;
    Ok(u64::try_from(block)?)
}

/// Insert an ordinary ingest batch in a caller-owned transaction. Only rows
/// returned by PostgreSQL were newly inserted and may activate in the matcher.
pub fn insert_notes_batch_tx(
    conn: &mut PgConnection,
    new_notes: &[NoteRow],
    new_orders: &[NewOrderRow],
    block_number: u64,
) -> Result<HashMap<Vec<u8>, u64>> {
    let block_number =
        i64::try_from(block_number).context("sync height exceeds PostgreSQL BIGINT")?;
    let mut validated = HashMap::with_capacity(new_notes.len());
    for row in new_notes {
        let note = Note::read_from(&mut SliceReader::new(&row.raw_data))
            .context("ingest note bytes do not decode")?;
        let terms =
            crate::types::Order::from_note(&note).context("ingest note is not a valid order")?;
        ensure!(
            note.id().to_bytes() == row.note_id && terms.creator_id.to_bytes() == row.account_id,
            "ingest note ID or creator does not match its serialized note"
        );
        validated.insert(row.note_id.as_slice(), terms);
    }
    for row in new_orders {
        let terms = validated
            .get(row.note_id.as_slice())
            .context("ingest order has no corresponding validated note")?;
        ensure!(
            row.account_id == terms.creator_id.to_bytes()
                && row.requested_asset == terms.requested_faucet_id.to_bytes()
                && row.requested_amount == i64::try_from(terms.requested_amount)?
                && row.offered_asset == terms.offered_faucet_id.to_bytes()
                && row.offered_amount == i64::try_from(terms.offered_amount)?
                && row.arrival_unix >= 0,
            "ingest order columns disagree with the serialized note"
        );
    }
    let inserted_notes = if !new_notes.is_empty() {
        diesel::insert_into(notes::table)
            .values(new_notes)
            .on_conflict(notes::note_id)
            .do_nothing()
            .execute(conn)?
    } else {
        0
    };
    let inserted: Vec<(Vec<u8>, i64)> = if new_orders.is_empty() {
        Vec::new()
    } else {
        diesel::insert_into(orders::table)
            .values(new_orders)
            .on_conflict(orders::note_id)
            .do_nothing()
            .returning((orders::note_id, orders::priority_seq))
            .get_results(conn)?
    };
    let advanced = diesel::update(sync_state::table.find(1_i16))
        .set(sync_state::last_fetched_block.eq(block_number))
        .execute(conn)?;
    ensure!(advanced == 1, "PostgreSQL sync cursor row is missing");
    tracing::debug!(
        inserted_notes,
        inserted_orders = inserted.len(),
        block_number,
        "persisted PostgreSQL ingest batch"
    );
    inserted
        .into_iter()
        .map(|(id, sequence)| {
            let sequence = u64::try_from(sequence)?;
            ensure!(
                sequence > 0,
                "PostgreSQL assigned a nonpositive FIFO priority"
            );
            Ok((id, sequence))
        })
        .collect()
}

pub fn get_active_orders_tx(conn: &mut PgConnection) -> Result<Vec<OrderRow>> {
    Ok(orders::table
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(OrderRow::as_select())
        .load(conn)?)
}

pub fn load_active_orders_with_notes_tx(conn: &mut PgConnection) -> Result<Vec<BookOrder>> {
    let rows: Vec<(OrderRow, Vec<u8>)> = orders::table
        .inner_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .order(orders::priority_seq.asc())
        .select((OrderRow::as_select(), notes::raw_data))
        .load(conn)?;
    let mut result = Vec::with_capacity(rows.len());
    for (row, raw) in rows {
        match row.into_book_order(raw) {
            Ok(order) => result.push(order),
            Err(error) => {
                // Additional historical corruption fail-fast is deferred for
                // V1, but all new writes use validated constructors.
                tracing::warn!(%error, "skipping active order whose stored note does not parse");
            }
        }
    }
    Ok(result)
}

pub fn active_book_update_tx(
    conn: &mut PgConnection,
    candidates: Vec<BookOrder>,
) -> Result<BookUpdate> {
    let ids: Vec<_> = candidates
        .iter()
        .map(|order| order.id().to_bytes().to_vec())
        .collect();
    if ids.is_empty() {
        return Ok(BookUpdate::default());
    }
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

/// Nullifier observation and reservation lookup share the same row locks.
/// This prevents a prepare transaction from slipping between the two reads.
pub fn mark_orders_onchain_nullified_tx(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
) -> Result<usize> {
    if note_ids.is_empty() {
        return Ok(0);
    }
    let mut sorted = note_ids.to_vec();
    sorted.sort();
    sorted.dedup();
    let locked: Vec<Vec<u8>> = orders::table
        .filter(orders::note_id.eq_any(&sorted))
        .order(orders::note_id.asc())
        .for_update()
        .select(orders::note_id)
        .load(conn)?;
    if locked.is_empty() {
        return Ok(0);
    }
    let reserved: HashSet<Vec<u8>> = settlement_inputs::table
        .inner_join(
            settlement_attempts::table.on(settlement_inputs::tx_id.eq(settlement_attempts::tx_id)),
        )
        .filter(settlement_inputs::parent_note_id.eq_any(&locked))
        .filter(settlement_attempts::status.ne(SettlementStatus::Confirmed.as_str()))
        .select(settlement_inputs::parent_note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    let external: Vec<_> = locked
        .into_iter()
        .filter(|id| !reserved.contains(id))
        .collect();
    if external.is_empty() {
        return Ok(0);
    }
    Ok(diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(external))
            .filter(
                orders::status
                    .eq_any([OrderStatus::Active.as_str(), OrderStatus::Settling.as_str()]),
            ),
    )
    .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
    .execute(conn)?)
}

/// Reserve all parents after proof, before submission. A missing or consumed
/// parent rolls back the attempt row and every other parent transition.
pub fn prepare_settlement_tx(
    conn: &mut PgConnection,
    attempt: &SettlementAttemptRow,
    inputs: &[SettlementInputRow],
) -> Result<()> {
    ensure!(!inputs.is_empty(), "settlement has no inputs");
    ensure!(
        attempt.settlement_status()? == SettlementStatus::Prepared,
        "new settlement is not prepared"
    );
    let mut parent_ids: Vec<_> = inputs
        .iter()
        .map(|input| input.parent_note_id.clone())
        .collect();
    for input in inputs {
        if input.tx_id != attempt.tx_id {
            return Err(SettlementError::InputTransactionMismatch.into());
        }
        ensure!(
            input.child_note_id.is_some() == input.child_note_data.is_some(),
            "settlement child ID/data pair is incomplete"
        );
    }
    parent_ids.sort();
    parent_ids.dedup();
    ensure!(
        parent_ids.len() == inputs.len(),
        "duplicate settlement parent"
    );
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
        bail!("settlement input order missing");
    }
    if locked
        .iter()
        .any(|parent| parent.status != OrderStatus::Active.as_str())
    {
        return Err(SettlementError::InputOrderNotActive.into());
    }
    let parents: HashMap<&[u8], &OrderRow> = locked
        .iter()
        .map(|parent| (parent.note_id.as_slice(), parent))
        .collect();
    for input in inputs {
        if let (Some(child_id), Some(raw_child)) = (&input.child_note_id, &input.child_note_data) {
            let child = Note::read_from(&mut SliceReader::new(raw_child))
                .context("expected settlement child bytes do not decode")?;
            if child.id().to_bytes().as_slice() != child_id {
                return Err(SettlementError::ChildIdMismatch.into());
            }
            let parent = parents
                .get(input.parent_note_id.as_slice())
                .context("settlement child parent was not locked")?;
            NewRemainderOrderRow::from_parent(parent, &child)
                .context("expected settlement child is not a valid remainder order")?;
        }
    }
    let changed = diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(&parent_ids))
            .filter(orders::status.eq(OrderStatus::Active.as_str())),
    )
    .set(orders::status.eq(OrderStatus::Settling.as_str()))
    .execute(conn)?;
    ensure!(
        changed == parent_ids.len(),
        "settlement reservation count changed"
    );
    diesel::insert_into(settlement_inputs::table)
        .values(inputs)
        .execute(conn)?;
    tracing::info!(
        tx_id = %hex::encode(&attempt.tx_id),
        parents = changed,
        status = "prepared",
        "persisted settlement reservation"
    );
    Ok(())
}

pub fn unresolved_settlements_tx(conn: &mut PgConnection) -> Result<Vec<SettlementAttemptRow>> {
    Ok(settlement_attempts::table
        .filter(settlement_attempts::status.ne(SettlementStatus::Confirmed.as_str()))
        .select(SettlementAttemptRow::as_select())
        .load(conn)?)
}

fn transition_attempt_status_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    from: &[SettlementStatus],
    to: SettlementStatus,
) -> Result<()> {
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
    let current = current
        .as_deref()
        .map(SettlementStatus::try_from)
        .transpose()?;
    match current {
        // A concurrent confirmation is a documented idempotent race.
        Some(SettlementStatus::Confirmed) => Ok(()),
        // Repeated network-status observations are harmless.
        Some(status) if status.as_str() == to => Ok(()),
        Some(status) => bail!(
            "invalid settlement status transition {} -> {to}",
            status.as_str()
        ),
        None => bail!("settlement attempt missing during {to} transition"),
    }
}

pub fn mark_settlement_submitted_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<()> {
    transition_attempt_status_tx(
        conn,
        tx_id,
        &[SettlementStatus::Prepared],
        SettlementStatus::Submitted,
    )
}

pub fn mark_settlement_uncertain_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<()> {
    transition_attempt_status_tx(
        conn,
        tx_id,
        &[SettlementStatus::Prepared, SettlementStatus::Submitted],
        SettlementStatus::Uncertain,
    )
}

pub fn mark_settlement_rejected_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<()> {
    transition_attempt_status_tx(
        conn,
        tx_id,
        &[
            SettlementStatus::Prepared,
            SettlementStatus::Submitted,
            SettlementStatus::Uncertain,
        ],
        SettlementStatus::Rejected,
    )
}

pub fn settlement_payback_id_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<OrderId> {
    let bytes: Vec<u8> = settlement_inputs::table
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .select(settlement_inputs::payback_note_id)
        .first(conn)?;
    Ok(OrderId::read_from(&mut SliceReader::new(&bytes))?)
}

pub fn settlement_parents_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<Vec<BookOrder>> {
    let rows: Vec<(OrderRow, Vec<u8>)> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .inner_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .order(settlement_inputs::parent_note_id.asc())
        .select((OrderRow::as_select(), notes::raw_data))
        .load(conn)?;
    rows.into_iter()
        .map(|(row, raw)| row.into_book_order(raw))
        .collect()
}

/// Hydrate unresolved attempts and every parent from one PostgreSQL statement.
/// One statement gives a consistent snapshot without a multi-query recovery
/// transaction, and an attempt missing any required row is a startup error.
#[derive(Debug)]
pub struct UnresolvedAttempt {
    pub attempt: SettlementAttemptRow,
    pub payback_id: OrderId,
    pub parents: Vec<BookOrder>,
}

type UnresolvedAttemptJoinRow = (
    SettlementAttemptRow,
    Option<SettlementInputRow>,
    Option<OrderRow>,
    Option<Vec<u8>>,
);

pub fn load_unresolved_attempts_tx(conn: &mut PgConnection) -> Result<Vec<UnresolvedAttempt>> {
    let rows: Vec<UnresolvedAttemptJoinRow> = settlement_attempts::table
        .left_join(
            settlement_inputs::table.on(settlement_attempts::tx_id.eq(settlement_inputs::tx_id)),
        )
        .left_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .left_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(settlement_attempts::status.ne(SettlementStatus::Confirmed.as_str()))
        .order((
            settlement_attempts::tx_id.asc(),
            settlement_inputs::parent_note_id.asc(),
        ))
        .select((
            SettlementAttemptRow::as_select(),
            Option::<SettlementInputRow>::as_select(),
            Option::<OrderRow>::as_select(),
            notes::raw_data.nullable(),
        ))
        .load(conn)?;
    let mut attempts = BTreeMap::<Vec<u8>, UnresolvedAttempt>::new();
    for (attempt, input, parent_row, raw_note) in rows {
        let input = input.context("unresolved settlement has no input")?;
        let parent_row = parent_row.context("unresolved settlement parent order is missing")?;
        let raw_note = raw_note.context("unresolved settlement parent note is missing")?;
        ensure!(
            attempt.tx_id == input.tx_id && input.parent_note_id == parent_row.note_id,
            "unresolved settlement mapping does not match its parent"
        );
        let payback_id = OrderId::read_from(&mut SliceReader::new(&input.payback_note_id))?;
        let parent = parent_row.into_book_order(raw_note)?;
        match attempts.entry(attempt.tx_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(UnresolvedAttempt {
                    attempt,
                    payback_id,
                    parents: vec![parent],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                ensure!(
                    entry.get().attempt.status == attempt.status
                        && entry.get().attempt.tx_result == attempt.tx_result,
                    "unresolved settlement changed within one snapshot"
                );
                entry.get_mut().parents.push(parent);
            }
        }
    }
    Ok(attempts.into_values().collect())
}

/// Discard a definitely rejected settlement after its chain-derived consumed
/// set is known. The attempt and all parents are locked before classification.
pub fn finish_discarded_settlement_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    consumed: &HashSet<OrderId>,
) -> Result<BookUpdate> {
    let status: String = settlement_attempts::table
        .find(tx_id)
        .for_update()
        .select(settlement_attempts::status)
        .first(conn)?;
    let typed_status = SettlementStatus::try_from(status.as_str())?;
    if typed_status == SettlementStatus::Confirmed {
        return Ok(BookUpdate::default());
    }
    // The executor may learn a definite local discard while the durable
    // attempt still says prepared/submitted. The chain/nullifier check was
    // completed by the caller before entering this transaction; the row lock
    // and Confirmed guard above serialize a concurrent ingest confirmation.
    ensure!(
        typed_status != SettlementStatus::Confirmed,
        "cannot discard confirmed settlement"
    );
    let rows: Vec<(OrderRow, Vec<u8>)> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .inner_join(notes::table.on(orders::note_id.eq(notes::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .order(settlement_inputs::parent_note_id.asc())
        .for_update()
        .select((OrderRow::as_select(), notes::raw_data))
        .load(conn)?;
    ensure!(!rows.is_empty(), "discarded settlement has no parents");

    let mut active_ids = Vec::new();
    let mut consumed_ids = Vec::new();
    let mut update = BookUpdate::default();
    for (row, raw) in rows {
        ensure!(
            row.status == OrderStatus::Settling.as_str(),
            "discarded settlement parent is not settling"
        );
        let parent = row.into_book_order(raw)?;
        let id = parent.id();
        if consumed.contains(&id) {
            consumed_ids.push(id.to_bytes().to_vec());
            update.removed.push(id);
        } else {
            active_ids.push(id.to_bytes().to_vec());
            update.active.push(parent);
        }
    }
    if !active_ids.is_empty() {
        let changed = diesel::update(
            orders::table
                .filter(orders::note_id.eq_any(&active_ids))
                .filter(orders::status.eq(OrderStatus::Settling.as_str())),
        )
        .set(orders::status.eq(OrderStatus::Active.as_str()))
        .execute(conn)?;
        ensure!(
            changed == active_ids.len(),
            "discarded active-parent count changed"
        );
    }
    if !consumed_ids.is_empty() {
        let changed = diesel::update(
            orders::table
                .filter(orders::note_id.eq_any(&consumed_ids))
                .filter(orders::status.eq(OrderStatus::Settling.as_str())),
        )
        .set(orders::status.eq(OrderStatus::OnchainNullified.as_str()))
        .execute(conn)?;
        ensure!(
            changed == consumed_ids.len(),
            "discarded consumed-parent count changed"
        );
    }
    let deleted = diesel::delete(settlement_attempts::table.find(tx_id)).execute(conn)?;
    ensure!(deleted == 1, "discarded settlement attempt disappeared");
    tracing::info!(
        tx_id = %hex::encode(tx_id),
        reactivated = active_ids.len(),
        consumed = consumed_ids.len(),
        status = "discarded",
        "persisted discarded settlement"
    );
    Ok(update)
}

/// Executor-first and ingest-first confirmation both call this transition.
/// The attempt lock makes only one observer return a nonempty book update.
pub fn confirm_settlement_tx(conn: &mut PgConnection, tx_id: &[u8]) -> Result<BookUpdate> {
    let status: String = settlement_attempts::table
        .find(tx_id)
        .for_update()
        .select(settlement_attempts::status)
        .first(conn)?;
    let typed_status = SettlementStatus::try_from(status.as_str())?;
    if typed_status == SettlementStatus::Confirmed {
        return Ok(BookUpdate::default());
    }
    if !matches!(
        typed_status,
        SettlementStatus::Prepared
            | SettlementStatus::Submitted
            | SettlementStatus::Uncertain
            | SettlementStatus::Rejected
    ) {
        return Err(SettlementError::InvalidConfirmationStatus(status).into());
    }
    let rows: Vec<(SettlementInputRow, OrderRow)> = settlement_inputs::table
        .inner_join(orders::table.on(settlement_inputs::parent_note_id.eq(orders::note_id)))
        .filter(settlement_inputs::tx_id.eq(tx_id))
        .order(settlement_inputs::parent_note_id.asc())
        .for_update()
        .select((SettlementInputRow::as_select(), OrderRow::as_select()))
        .load(conn)?;
    ensure!(
        !rows.is_empty(),
        "confirmed attempt has no settlement inputs"
    );

    let mut removed = Vec::with_capacity(rows.len());
    let mut children = Vec::new();
    let mut child_notes = Vec::new();
    let mut child_orders = Vec::new();
    let mut parent_ids = Vec::with_capacity(rows.len());
    for (input, parent) in rows {
        ensure!(
            parent.status == OrderStatus::Settling.as_str(),
            "confirmed parent is not settling"
        );
        let parent_id = OrderId::read_from(&mut SliceReader::new(&input.parent_note_id))?;
        removed.push(parent_id);
        parent_ids.push(input.parent_note_id);
        match (input.child_note_id, input.child_note_data) {
            (None, None) => {}
            (Some(expected_id), Some(raw)) => {
                let child_note = Note::read_from(&mut SliceReader::new(&raw))?;
                if child_note.id().to_bytes().as_slice() != expected_id {
                    return Err(SettlementError::ChildIdMismatch.into());
                }
                let (note_row, order_row) =
                    NewRemainderOrderRow::from_parent(&parent, &child_note)?;
                children.push(BookOrder {
                    priority_seq: u64::try_from(parent.priority_seq)?,
                    arrival_unix: u64::try_from(parent.arrival_unix)?,
                    note: std::sync::Arc::new(child_note),
                });
                child_notes.push(note_row);
                child_orders.push(order_row);
            }
            _ => bail!("settlement child ID/data pair is incomplete"),
        }
    }
    if !child_notes.is_empty() {
        diesel::insert_into(notes::table)
            .values(&child_notes)
            .on_conflict(notes::note_id)
            .do_nothing()
            .execute(conn)?;
        diesel::insert_into(orders::table)
            .values(&child_orders)
            .on_conflict(orders::note_id)
            .do_nothing()
            .execute(conn)?;
    }
    let child_ids: Vec<_> = children
        .iter()
        .map(|child| child.id().to_bytes().to_vec())
        .collect();
    let active_children: HashSet<Vec<u8>> = if child_ids.is_empty() {
        HashSet::new()
    } else {
        orders::table
            .filter(orders::note_id.eq_any(child_ids))
            .filter(orders::status.eq(OrderStatus::Active.as_str()))
            .select(orders::note_id)
            .load::<Vec<u8>>(conn)?
            .into_iter()
            .collect()
    };
    let changed = diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(&parent_ids))
            .filter(orders::status.eq(OrderStatus::Settling.as_str())),
    )
    .set(orders::status.eq(OrderStatus::Executed.as_str()))
    .execute(conn)?;
    ensure!(
        changed == parent_ids.len(),
        "confirmed parent count changed"
    );
    let changed = diesel::update(settlement_attempts::table.find(tx_id))
        .set(settlement_attempts::status.eq(SettlementStatus::Confirmed.as_str()))
        .execute(conn)?;
    ensure!(changed == 1, "confirmed attempt disappeared");
    tracing::info!(
        tx_id = %hex::encode(tx_id),
        parents = parent_ids.len(),
        remainders = active_children.len(),
        status = "confirmed",
        "persisted settlement confirmation"
    );
    Ok(BookUpdate {
        removed,
        active: children
            .into_iter()
            .filter(|child| active_children.contains(child.id().to_bytes().as_slice()))
            .collect(),
    })
}

/// One lookup for every child in an ingest batch; each affected attempt is
/// confirmed once under the attempt-row lock.
pub fn confirm_expected_remainders_tx(
    conn: &mut PgConnection,
    child_ids: &[Vec<u8>],
) -> Result<(HashSet<Vec<u8>>, BookUpdate)> {
    if child_ids.is_empty() {
        return Ok((HashSet::new(), BookUpdate::default()));
    }
    let mappings: Vec<(Vec<u8>, Vec<u8>)> = settlement_inputs::table
        .filter(settlement_inputs::child_note_id.eq_any(child_ids))
        .select((
            settlement_inputs::child_note_id.assume_not_null(),
            settlement_inputs::tx_id,
        ))
        .load(conn)?;
    let mut expected = HashSet::new();
    let mut attempts = HashSet::new();
    for (child_id, attempt_id) in mappings {
        expected.insert(child_id);
        attempts.insert(attempt_id);
    }
    let mut attempts: Vec<_> = attempts.into_iter().collect();
    attempts.sort();
    let mut combined = BookUpdate::default();
    for tx_id in attempts {
        let update = confirm_settlement_tx(conn, &tx_id)?;
        combined.removed.extend(update.removed);
        combined.active.extend(update.active);
    }
    Ok((expected, combined))
}

pub fn get_registered_tokens_tx(conn: &mut PgConnection) -> Result<Vec<RegisteredTokenRow>> {
    Ok(registered_tokens::table
        .select(RegisteredTokenRow::as_select())
        .load(conn)?)
}

pub fn get_registered_token_tx(
    conn: &mut PgConnection,
    token_id: &[u8],
) -> Result<Option<RegisteredTokenRow>> {
    Ok(registered_tokens::table
        .find(token_id)
        .select(RegisteredTokenRow::as_select())
        .first(conn)
        .optional()?)
}

/// Serve a whole public price batch in one PostgreSQL query.
pub fn fetch_token_rows_tx(
    conn: &mut PgConnection,
    token_ids: &[Vec<u8>],
) -> Result<HashMap<Vec<u8>, RegisteredTokenRow>> {
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
    token_id: &[u8],
    external_symbol: Option<&str>,
) -> Result<bool> {
    validate_token_id(token_id)?;
    let created_at_unix = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
    let row = RegisteredTokenRow {
        token_id: token_id.to_vec(),
        created_at_unix,
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
    token_id: &[u8],
    decimals: Option<i32>,
    ticker: Option<&str>,
) -> Result<bool> {
    validate_token_id(token_id)?;
    if let Some(decimals) = decimals {
        u8::try_from(decimals).context("token decimals must fit a u8")?;
    }
    Ok(diesel::update(registered_tokens::table.find(token_id))
        .set((
            registered_tokens::decimals.eq(decimals),
            registered_tokens::ticker.eq(ticker.map(str::to_owned)),
        ))
        .execute(conn)?
        == 1)
}

pub fn update_token_symbol_tx(
    conn: &mut PgConnection,
    token_id: &[u8],
    symbol: Option<&str>,
) -> Result<bool> {
    validate_token_id(token_id)?;
    Ok(diesel::update(registered_tokens::table.find(token_id))
        .set(registered_tokens::external_symbol.eq(symbol.map(str::to_owned)))
        .execute(conn)?
        == 1)
}

pub fn unregister_token_tx(conn: &mut PgConnection, token_id: &[u8]) -> Result<bool> {
    validate_token_id(token_id)?;
    Ok(diesel::delete(registered_tokens::table.find(token_id)).execute(conn)? == 1)
}

fn validate_token_id(bytes: &[u8]) -> Result<TokenId> {
    let token = TokenId::read_from(&mut SliceReader::new(bytes))
        .context("token ID is not a valid serialized account ID")?;
    ensure!(
        token.to_bytes() == bytes,
        "token ID bytes are not canonical"
    );
    Ok(token)
}

pub fn load_token_symbols_tx(conn: &mut PgConnection) -> Result<HashMap<TokenId, String>> {
    let mut result = HashMap::new();
    for row in get_registered_tokens_tx(conn)? {
        if let Some(symbol) = row.external_symbol {
            let token = TokenId::read_from(&mut SliceReader::new(&row.token_id))?;
            result.insert(token, symbol);
        }
    }
    Ok(result)
}

pub fn load_registered_tokens_tx(conn: &mut PgConnection) -> Result<Vec<TokenId>> {
    get_registered_tokens_tx(conn)?
        .into_iter()
        .map(|row| Ok(TokenId::read_from(&mut SliceReader::new(&row.token_id))?))
        .collect()
}

pub fn seed_tokens_from_config_tx(
    conn: &mut PgConnection,
    tokens: &[(TokenId, Option<String>)],
) -> Result<()> {
    for (token, symbol) in tokens {
        register_token_tx(conn, &token.to_bytes(), symbol.as_deref())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_migrations;
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

    struct SchemaFixture {
        conn: PgConnection,
        name: String,
    }

    impl SchemaFixture {
        fn new() -> Result<Self> {
            let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
            let mut conn = postgres_migrations::connect(&url)?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!("solver_data_{}_{}_{}", std::process::id(), nonce, sequence);
            conn.batch_execute(&format!("CREATE SCHEMA {name}; SET search_path TO {name}"))?;
            postgres_migrations::migrate(&mut conn)?;
            Ok(Self { conn, name })
        }
    }

    impl Drop for SchemaFixture {
        fn drop(&mut self) {
            let _ = self.conn.batch_execute("ROLLBACK");
            let _ = self.conn.batch_execute(&format!(
                "SET search_path TO public; DROP SCHEMA {} CASCADE",
                self.name
            ));
        }
    }

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
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;
        let mut note_rows = Vec::with_capacity(511);
        let mut order_rows = Vec::with_capacity(511);
        let mut inputs = Vec::with_capacity(511);
        let mut rng = RandomCoin::new(Word::default());
        for _ in 0..511 {
            let note = order_note(rng.draw_word())?;
            let child = order_note(rng.draw_word())?;
            let id = note.id().to_bytes();
            let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
            note_rows.push(note_row);
            order_rows.push(order_row);
            inputs.push(SettlementInputRow {
                tx_id: vec![7],
                parent_note_id: id,
                payback_note_id: vec![8],
                child_note_id: Some(child.id().to_bytes()),
                child_note_data: Some(child.to_bytes()),
            });
        }
        let inserted = conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &note_rows, &order_rows, 42)
        })?;
        assert_eq!(inserted.len(), 511);
        assert!(inserted.values().all(|priority| *priority > 0));
        assert_eq!(get_last_fetched_block_tx(conn)?, 42);
        let duplicate = conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &note_rows, &order_rows, 43)
        })?;
        assert!(duplicate.is_empty());
        assert_eq!(get_last_fetched_block_tx(conn)?, 43);

        let attempt = SettlementAttemptRow {
            tx_id: vec![7],
            tx_result: vec![9],
            status: "prepared".into(),
            created_at_unix: 11,
        };
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            prepare_settlement_tx(conn, &attempt, &inputs)
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
            created_at_unix: 12,
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
            .transaction::<_, anyhow::Error, _>(|conn| {
                prepare_settlement_tx(conn, &competing, &competing_inputs)
            })
            .is_err());
        let attempts: i64 = settlement_attempts::table.count().get_result(conn)?;
        let mappings: i64 = settlement_inputs::table.count().get_result(conn)?;
        assert_eq!((attempts, mappings), (1, 511));

        let update = conn.transaction::<_, anyhow::Error, _>(|conn| {
            confirm_settlement_tx(conn, &attempt.tx_id)
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
        let duplicate = conn.transaction::<_, anyhow::Error, _>(|conn| {
            confirm_settlement_tx(conn, &attempt.tx_id)
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
        let mut fixture = SchemaFixture::new()?;
        let note = order_note(Word::default())?;
        let id = note.id().to_bytes();
        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &[note_row], &[order_row], 1)?;
            prepare_settlement_tx(
                conn,
                &SettlementAttemptRow {
                    tx_id: vec![7],
                    tx_result: vec![9],
                    status: "prepared".into(),
                    created_at_unix: 11,
                },
                &[SettlementInputRow {
                    tx_id: vec![7],
                    parent_note_id: id.clone(),
                    payback_note_id: vec![8],
                    child_note_id: None,
                    child_note_data: None,
                }],
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
                let update = conn
                    .transaction::<_, anyhow::Error, _>(|conn| confirm_settlement_tx(conn, &[7]))?;
                Ok(update.removed.len())
            }));
        }
        let mut result: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("confirmation worker panicked"))
            .collect::<Result<_>>()?;
        result.sort();
        assert_eq!(result, vec![0, 1]);
        let status: String = settlement_attempts::table
            .find(vec![7])
            .select(settlement_attempts::status)
            .first(&mut fixture.conn)?;
        assert_eq!(status, "confirmed");
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn two_connections_cannot_reserve_the_same_parent() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let note = order_note(Word::default())?;
        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &[note_row], &[order_row], 1)?;
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
                    created_at_unix: 1,
                };
                let input = SettlementInputRow {
                    tx_id: vec![attempt_id],
                    parent_note_id: parent_id,
                    payback_note_id: vec![99],
                    child_note_id: None,
                    child_note_data: None,
                };
                barrier.wait();
                match conn.transaction::<_, anyhow::Error, _>(|conn| {
                    prepare_settlement_tx(conn, &attempt, &[input])
                }) {
                    Ok(()) => Ok(true),
                    Err(error) if error.to_string().contains("not active") => Ok(false),
                    Err(error) => Err(error),
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
        let mut fixture = SchemaFixture::new()?;
        let note = order_note(Word::default())?;
        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &[note_row], &[order_row], 1)?;
            prepare_settlement_tx(
                conn,
                &SettlementAttemptRow {
                    tx_id: vec![41],
                    tx_result: vec![42],
                    status: "prepared".into(),
                    created_at_unix: 1,
                },
                &[SettlementInputRow {
                    tx_id: vec![41],
                    parent_note_id: note.id().to_bytes(),
                    payback_note_id: vec![99],
                    child_note_id: None,
                    child_note_data: None,
                }],
            )
        })?;
        let url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let schema = fixture.name.clone();
        let barrier = Arc::new(Barrier::new(2));
        let confirm_barrier = barrier.clone();
        let confirm_url = url.clone();
        let confirm_schema = schema.clone();
        let confirmer = std::thread::spawn(move || -> Result<Option<usize>> {
            let mut conn = postgres_migrations::connect(&confirm_url)?;
            conn.batch_execute(&format!("SET search_path TO {confirm_schema}"))?;
            confirm_barrier.wait();
            match conn.transaction::<_, anyhow::Error, _>(|conn| confirm_settlement_tx(conn, &[41]))
            {
                Ok(update) => Ok(Some(update.removed.len())),
                Err(error)
                    if matches!(
                        error.downcast_ref::<diesel::result::Error>(),
                        Some(diesel::result::Error::NotFound)
                    ) =>
                {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        });
        let discarder = std::thread::spawn(move || -> Result<BookUpdate> {
            let mut conn = postgres_migrations::connect(&url)?;
            conn.batch_execute(&format!("SET search_path TO {schema}"))?;
            barrier.wait();
            conn.transaction::<_, anyhow::Error, _>(|conn| {
                finish_discarded_settlement_tx(conn, &[41], &HashSet::new())
            })
        });
        let confirmed = confirmer.join().expect("confirmation worker panicked")?;
        let discarded = discarder.join().expect("discard worker panicked")?;
        let attempt: Option<String> = settlement_attempts::table
            .find(vec![41])
            .select(settlement_attempts::status)
            .first(&mut fixture.conn)
            .optional()?;
        let parent_status: String = orders::table
            .find(note.id().to_bytes())
            .select(orders::status)
            .first(&mut fixture.conn)?;
        match attempt.as_deref() {
            Some("confirmed") => {
                assert_eq!(confirmed, Some(1));
                assert!(discarded.is_empty());
                assert_eq!(parent_status, OrderStatus::Executed.as_str());
            }
            None => {
                assert_eq!(confirmed, None);
                assert_eq!(discarded.active.len(), 1);
                assert_eq!(parent_status, OrderStatus::Active.as_str());
            }
            other => bail!("invalid state after confirmation/discard race: {other:?}"),
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn nullifier_observation_and_prepare_serialize() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let note = order_note(Word::default())?;
        let parent_id = note.id().to_bytes();
        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        fixture.conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &[note_row], &[order_row], 1)?;
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
            match conn.transaction::<_, anyhow::Error, _>(|conn| {
                prepare_settlement_tx(
                    conn,
                    &SettlementAttemptRow {
                        tx_id: vec![51],
                        tx_result: vec![52],
                        status: "prepared".into(),
                        created_at_unix: 1,
                    },
                    &[SettlementInputRow {
                        tx_id: vec![51],
                        parent_note_id: prepare_parent,
                        payback_note_id: vec![99],
                        child_note_id: None,
                        child_note_data: None,
                    }],
                )
            }) {
                Ok(()) => Ok(true),
                Err(error) if error.to_string().contains("not active") => Ok(false),
                Err(error) => Err(error),
            }
        });
        let observer = std::thread::spawn(move || -> Result<usize> {
            let mut conn = postgres_migrations::connect(&url)?;
            conn.batch_execute(&format!("SET search_path TO {schema}"))?;
            barrier.wait();
            conn.transaction::<_, anyhow::Error, _>(|conn| {
                mark_orders_onchain_nullified_tx(conn, &[parent_id])
            })
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
        if prepared {
            assert_eq!((attempts, changed), (1, 0));
            assert_eq!(status, OrderStatus::Settling.as_str());
        } else {
            assert_eq!((attempts, changed), (0, 1));
            assert_eq!(status, OrderStatus::OnchainNullified.as_str());
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn recovery_rejects_attempt_without_inputs() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        diesel::insert_into(settlement_attempts::table)
            .values(SettlementAttemptRow {
                tx_id: vec![21],
                tx_result: vec![22],
                status: "prepared".into(),
                created_at_unix: 1,
            })
            .execute(&mut fixture.conn)?;
        let error = load_unresolved_attempts_tx(&mut fixture.conn)
            .expect_err("incomplete attempt must not disappear from recovery");
        assert!(error.to_string().contains("no input"));
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn invalid_ingest_and_child_rows_roll_back() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;
        let note = order_note(Word::default())?;
        let (mut note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        note_row.account_id = vec![0];
        assert!(conn
            .transaction::<_, anyhow::Error, _>(|conn| {
                insert_notes_batch_tx(conn, &[note_row], &[order_row.clone()], 9)
            })
            .is_err());
        assert_eq!(notes::table.count().get_result::<i64>(conn)?, 0);
        assert_eq!(get_last_fetched_block_tx(conn)?, 0);

        let (note_row, mut wrong_order) = NewOrderRow::ingested(&note, 10)?;
        wrong_order.requested_amount += 1;
        assert!(conn
            .transaction::<_, anyhow::Error, _>(|conn| {
                insert_notes_batch_tx(conn, &[note_row], &[wrong_order], 9)
            })
            .is_err());
        assert_eq!(notes::table.count().get_result::<i64>(conn)?, 0);
        assert_eq!(get_last_fetched_block_tx(conn)?, 0);

        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            insert_notes_batch_tx(conn, &[note_row], &[order_row], 1)
        })?;
        let attempt = SettlementAttemptRow {
            tx_id: vec![7],
            tx_result: vec![8],
            status: "prepared".into(),
            created_at_unix: 1,
        };
        let input = SettlementInputRow {
            tx_id: attempt.tx_id.clone(),
            parent_note_id: note.id().to_bytes(),
            payback_note_id: vec![9],
            child_note_id: Some(vec![10]),
            child_note_data: Some(vec![11]),
        };
        assert!(conn
            .transaction::<_, anyhow::Error, _>(|conn| {
                prepare_settlement_tx(conn, &attempt, &[input])
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
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;
        diesel::delete(sync_state::table.find(1_i16)).execute(conn)?;
        let note = order_note(Word::default())?;
        let (note_row, order_row) = NewOrderRow::ingested(&note, 10)?;
        let error = conn
            .transaction::<_, anyhow::Error, _>(|conn| {
                insert_notes_batch_tx(conn, &[note_row], &[order_row], 9)
            })
            .expect_err("ingest must not commit notes without its sync cursor");
        assert!(error.to_string().contains("sync cursor row is missing"));
        assert_eq!(notes::table.count().get_result::<i64>(conn)?, 0);
        assert_eq!(orders::table.count().get_result::<i64>(conn)?, 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn token_registry_bulk_lookup_and_validation() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;
        let first: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let second: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        let first_id = first.to_bytes();
        let second_id = second.to_bytes();
        assert!(register_token_tx(conn, &first_id, Some("usd-coin"))?);
        assert!(!register_token_tx(conn, &first_id, Some("ignored"))?);
        assert!(register_token_tx(conn, &second_id, None)?);
        assert!(set_token_metadata_tx(
            conn,
            &first_id,
            Some(6),
            Some("USDC")
        )?);
        assert!(set_token_metadata_tx(
            conn,
            &second_id,
            Some(18),
            Some("WETH")
        )?);
        assert!(set_token_metadata_tx(conn, &first_id, Some(-1), None).is_err());
        assert!(register_token_tx(conn, &[0], None).is_err());

        let rows = fetch_token_rows_tx(conn, &[first_id.clone(), second_id.clone()])?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[&first_id].decimals, Some(6));
        assert_eq!(rows[&first_id].ticker.as_deref(), Some("USDC"));
        assert_eq!(rows[&first_id].external_symbol.as_deref(), Some("usd-coin"));
        assert!(update_token_symbol_tx(
            conn,
            &first_id,
            Some("usd-coin-new")
        )?);
        assert_eq!(
            load_token_symbols_tx(conn)?.get(&first).map(String::as_str),
            Some("usd-coin-new")
        );
        assert!(unregister_token_tx(conn, &second_id)?);
        assert!(!unregister_token_tx(conn, &second_id)?);
        Ok(())
    }
}

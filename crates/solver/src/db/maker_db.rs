//! Transaction-scoped maker store queries (ADR 0003).
//!
//! Like `postgres_db`, these never start transactions. Maker commands are
//! written by the intake session, many to one transaction (group commit), so
//! nothing here may fail for one command's content: inserts use
//! `ON CONFLICT DO NOTHING` and classify what they returned, and headers were
//! validated by [`CommandHeader::new`]. Only a database failure is an error,
//! and it fails the whole batch, none of which was acknowledged.

use diesel::dsl::{count_star, now, sql};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use std::collections::{BTreeMap, HashMap, HashSet};

use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;

use super::error::{DbError, DbResult};
use super::postgres_models::{NewOrderRow, OrderRow, SettlementInputRow};
use super::postgres_schema::{
    api_keys, live_orders, maker_commands, maker_cutoffs, maker_events, maker_lineages,
    maker_stops, makers, orders,
};
use crate::gateway::{self, proto};
use crate::maker::{
    CommandHeader, CommandReply, CommandResult, CutoffScope, EventWake, LineageId, MakerCommand,
    MakerFact, MakerId, MakerTag,
};
use crate::types::{BookUpdate, OrderKeys, OrderStatus};

/// Create a maker; `None` when the name is taken.
pub fn create_maker_tx(conn: &mut PgConnection, name: &str) -> DbResult<Option<MakerId>> {
    Ok(diesel::insert_into(makers::table)
        .values(makers::name.eq(name))
        .on_conflict_do_nothing()
        .returning(makers::maker_id)
        .get_result(conn)
        .optional()?)
}

/// Store a key hash for `maker_id`; `None` when the maker does not exist.
pub fn issue_api_key_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    key_hash: &[u8],
) -> DbResult<Option<i64>> {
    let exists = diesel::select(diesel::dsl::exists(makers::table.find(maker_id)))
        .get_result::<bool>(conn)?;
    if !exists {
        return Ok(None);
    }
    Ok(Some(
        diesel::insert_into(api_keys::table)
            .values((
                api_keys::maker_id.eq(maker_id),
                api_keys::key_hash.eq(key_hash),
            ))
            .returning(api_keys::key_id)
            .get_result(conn)?,
    ))
}

/// Revoke a key; `false` when it does not exist or was already revoked.
pub fn revoke_api_key_tx(conn: &mut PgConnection, key_id: i64) -> DbResult<bool> {
    let revoked = diesel::update(
        api_keys::table
            .find(key_id)
            .filter(api_keys::revoked_at.is_null()),
    )
    .set(api_keys::revoked_at.eq(now))
    .execute(conn)?;
    Ok(revoked == 1)
}

/// The maker a presented key belongs to, unless it is unknown or revoked.
pub fn authenticate_tx(conn: &mut PgConnection, key_hash: &[u8]) -> DbResult<Option<MakerId>> {
    Ok(api_keys::table
        .filter(api_keys::key_hash.eq(key_hash))
        .filter(api_keys::revoked_at.is_null())
        .select(api_keys::maker_id)
        .first(conn)
        .optional()?)
}

/// Lock the maker control row. A cancel and a reservation of the maker's
/// orders both take it, so they serialize per maker. `None` when the maker
/// does not exist.
fn lock_maker(conn: &mut PgConnection, maker_id: MakerId) -> DbResult<Option<MakerId>> {
    Ok(makers::table
        .find(maker_id)
        .for_no_key_update()
        .select(makers::maker_id)
        .first(conn)
        .optional()?)
}

/// Write one maker command, returning its reply and the fact the matcher must
/// learn once the transaction commits. An exact retry or a conflict changes
/// nothing and has no fact: a fact lost to a crash is restored by the
/// matcher's startup hydration.
pub fn execute_command_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
) -> DbResult<(CommandReply, Option<MakerFact>)> {
    if matches!(
        command,
        MakerCommand::CancelAll { .. } | MakerCommand::CancelOrder { .. }
    ) && lock_maker(conn, header.maker_id)?.is_none()
    {
        return Err(DbError::Corrupt("authenticated maker is missing"));
    }
    if let Some(reply) = claim_command(conn, header, command)? {
        return Ok((reply, None));
    }
    let maker_id = header.maker_id;
    let (result, fact) = match command {
        MakerCommand::Submit { note, keys } => {
            let claimed = diesel::insert_into(maker_lineages::table)
                .values((
                    maker_lineages::lineage_id.eq(&keys.lineage_id),
                    maker_lineages::maker_id.eq(maker_id),
                    maker_lineages::request_id.eq(&header.request_id),
                    maker_lineages::root_seq.eq(seq_column(header.seq)?),
                    maker_lineages::note_id.eq(note.id().to_bytes().to_vec()),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?
                == 1;
            if claimed {
                let tag = MakerTag {
                    maker_id,
                    root_seq: header.seq,
                };
                let fact = MakerFact::LineageAttributed {
                    lineage_id: keys.lineage_id.clone(),
                    tag,
                };
                (CommandResult::Accepted, Some(fact))
            } else {
                (CommandResult::AlreadyRegistered, None)
            }
        }
        MakerCommand::CancelAll { scope } => {
            let cutoff: i64 = diesel::insert_into(maker_cutoffs::table)
                .values((
                    maker_cutoffs::maker_id.eq(maker_id),
                    maker_cutoffs::market.eq(&scope.market),
                    maker_cutoffs::direction.eq(&scope.direction),
                    maker_cutoffs::cutoff.eq(seq_column(header.seq)?),
                ))
                .on_conflict((
                    maker_cutoffs::maker_id,
                    maker_cutoffs::market,
                    maker_cutoffs::direction,
                ))
                .do_update()
                .set(maker_cutoffs::cutoff.eq(sql::<BigInt>(
                    "GREATEST(maker_cutoffs.cutoff, excluded.cutoff)",
                )))
                .returning(maker_cutoffs::cutoff)
                .get_result(conn)?;
            let settling = settling_below_cutoff(conn, maker_id, scope, cutoff)?;
            let cutoff = u64::try_from(cutoff)?;
            let fact = MakerFact::CutoffRaised {
                maker_id,
                scope: scope.clone(),
                cutoff,
            };
            (CommandResult::Applied { cutoff, settling }, Some(fact))
        }
        MakerCommand::CancelOrder { lineage_id } => {
            diesel::insert_into(maker_stops::table)
                .values((
                    maker_stops::maker_id.eq(maker_id),
                    maker_stops::lineage_id.eq(lineage_id),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?;
            let settling = settling_in_lineage(conn, maker_id, lineage_id)?;
            let fact = MakerFact::LineageStopped {
                maker_id,
                lineage_id: lineage_id.clone(),
            };
            (CommandResult::Stopped { settling }, Some(fact))
        }
    };
    let stored = serde_json::to_string(&result)
        .map_err(|_| DbError::Corrupt("command result does not serialize"))?;
    diesel::update(maker_commands::table.find((maker_id, &header.request_id)))
        .set(maker_commands::result.eq(stored))
        .execute(conn)?;
    Ok((CommandReply::Committed(result), fact))
}

/// Record the command, or return the reply it already has: the stored result
/// for an exact retry, a conflict when its request ID or sequence was used
/// for something else. `None` means this call claimed it.
fn claim_command(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
) -> DbResult<Option<CommandReply>> {
    let seq = seq_column(header.seq)?;
    let payload = command.payload();
    let claimed = diesel::insert_into(maker_commands::table)
        .values((
            maker_commands::maker_id.eq(header.maker_id),
            maker_commands::request_id.eq(&header.request_id),
            maker_commands::seq.eq(seq),
            maker_commands::kind.eq(command.kind()),
            maker_commands::payload.eq(&payload),
            maker_commands::result.eq(""),
        ))
        .on_conflict_do_nothing()
        .execute(conn)?;
    if claimed == 1 {
        return Ok(None);
    }
    let existing: Option<(i64, String, Vec<u8>, String)> = maker_commands::table
        .find((header.maker_id, &header.request_id))
        .select((
            maker_commands::seq,
            maker_commands::kind,
            maker_commands::payload,
            maker_commands::result,
        ))
        .first(conn)
        .optional()?;
    Ok(Some(match existing {
        Some((stored_seq, kind, stored_payload, result))
            if stored_seq == seq && kind == command.kind() && stored_payload == payload =>
        {
            CommandReply::Replayed(
                serde_json::from_str(&result)
                    .map_err(|_| DbError::Corrupt("stored command result is unreadable"))?,
            )
        }
        // The request ID holds another command, or the sequence belongs to
        // another request ID.
        _ => CommandReply::Conflict,
    }))
}

/// The stored reply of `request_id`, for a maker whose reply was lost.
pub fn stored_result_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    request_id: &str,
) -> DbResult<Option<CommandResult>> {
    let stored: Option<String> = maker_commands::table
        .find((maker_id, request_id))
        .select(maker_commands::result)
        .first(conn)
        .optional()?;
    stored
        .map(|result| {
            serde_json::from_str(&result)
                .map_err(|_| DbError::Corrupt("stored command result is unreadable"))
        })
        .transpose()
}

/// `maker_id`'s newest event sequence (0 before its first event).
pub fn latest_event_seq_tx(conn: &mut PgConnection, maker_id: MakerId) -> DbResult<u64> {
    let next: Option<i64> = makers::table
        .find(maker_id)
        .select(makers::next_event_seq)
        .first(conn)
        .optional()?;
    Ok(u64::try_from(next.unwrap_or(1) - 1)?)
}

/// A stored maker event, as the stream sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub seq: u64,
    pub event_id: String,
    pub created_at_unix_ms: i64,
    pub lineage_id: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

/// Up to `limit` of `maker_id`'s events after `after_seq`, in order.
pub fn read_events_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    after_seq: u64,
    limit: i64,
) -> DbResult<Vec<StoredEvent>> {
    let after_seq = i64::try_from(after_seq).unwrap_or(i64::MAX);
    maker_events::table
        .filter(maker_events::maker_id.eq(maker_id))
        .filter(maker_events::event_seq.gt(after_seq))
        .order(maker_events::event_seq.asc())
        .limit(limit)
        .select((
            maker_events::event_seq,
            sql::<Text>("event_id::text"),
            sql::<BigInt>("(extract(epoch FROM created_at) * 1000)::bigint"),
            maker_events::lineage_id,
            maker_events::payload,
        ))
        .load::<(i64, String, i64, Option<Vec<u8>>, Vec<u8>)>(conn)?
        .into_iter()
        .map(|(seq, event_id, created_at_unix_ms, lineage_id, payload)| {
            Ok(StoredEvent {
                seq: u64::try_from(seq)?,
                event_id,
                created_at_unix_ms,
                lineage_id,
                payload,
            })
        })
        .collect()
}

fn seq_column(seq: u64) -> DbResult<i64> {
    Ok(i64::try_from(seq)?)
}

/// Orders of `maker_id` in `scope` below `cutoff` that a settlement already
/// reserved: the cancel cannot stop them, so its reply reports them.
fn settling_below_cutoff(
    conn: &mut PgConnection,
    maker_id: MakerId,
    scope: &CutoffScope,
    cutoff: i64,
) -> DbResult<u64> {
    let mut query = orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(maker_lineages::maker_id.eq(maker_id))
        .filter(maker_lineages::root_seq.lt(cutoff))
        .filter(orders::status.eq(OrderStatus::Settling.as_str()))
        .select(count_star())
        .into_boxed();
    if !scope.market.is_empty() {
        query = query.filter(orders::market.eq(&scope.market));
    }
    if !scope.direction.is_empty() {
        query = query.filter(orders::direction.eq(&scope.direction));
    }
    Ok(u64::try_from(query.get_result::<i64>(conn)?)?)
}

fn settling_in_lineage(
    conn: &mut PgConnection,
    maker_id: MakerId,
    lineage_id: &LineageId,
) -> DbResult<u64> {
    let settling: i64 = orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(maker_lineages::maker_id.eq(maker_id))
        .filter(maker_lineages::lineage_id.eq(lineage_id))
        .filter(orders::status.eq(OrderStatus::Settling.as_str()))
        .select(count_star())
        .get_result(conn)?;
    Ok(u64::try_from(settling)?)
}

/// Every cancel-all barrier: startup hydration of the matcher's cutoffs.
pub fn load_cutoffs_tx(conn: &mut PgConnection) -> DbResult<Vec<(MakerId, CutoffScope, u64)>> {
    maker_cutoffs::table
        .select((
            maker_cutoffs::maker_id,
            maker_cutoffs::market,
            maker_cutoffs::direction,
            maker_cutoffs::cutoff,
        ))
        .load::<(i64, Vec<u8>, Vec<u8>, i64)>(conn)?
        .into_iter()
        .map(|(maker_id, market, direction, cutoff)| {
            Ok((
                maker_id,
                CutoffScope { market, direction },
                u64::try_from(cutoff)?,
            ))
        })
        .collect()
}

/// An accepted submit whose note the maker-note watcher has not verified.
#[derive(Debug, Clone)]
pub struct PendingSubmission {
    pub lineage_id: LineageId,
    pub maker_id: MakerId,
    pub note: Note,
}

/// Every pending submission, with the note its submit stored.
pub fn pending_submissions_tx(conn: &mut PgConnection) -> DbResult<Vec<PendingSubmission>> {
    maker_lineages::table
        .inner_join(
            maker_commands::table.on(maker_commands::maker_id
                .eq(maker_lineages::maker_id)
                .and(maker_commands::request_id.eq(maker_lineages::request_id))),
        )
        .filter(maker_lineages::state.eq(LineageState::Pending.as_str()))
        .select((
            maker_lineages::lineage_id,
            maker_lineages::maker_id,
            maker_commands::payload,
        ))
        .load::<(Vec<u8>, i64, Vec<u8>)>(conn)?
        .into_iter()
        .map(|(lineage_id, maker_id, payload)| {
            Ok(PendingSubmission {
                lineage_id,
                maker_id,
                note: Note::read_from_bytes(&payload)?,
            })
        })
        .collect()
}

/// A lineage's chain verification by the maker-note watcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum LineageState {
    Pending,
    Activated,
    Rejected,
}

impl LineageState {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Mark pending lineages `state`; returns the ones this call changed, so a
/// repeated round reports each lineage once.
fn settle_lineages(
    conn: &mut PgConnection,
    lineages: &[&LineageId],
    state: LineageState,
) -> DbResult<HashSet<LineageId>> {
    Ok(diesel::update(
        maker_lineages::table
            .filter(maker_lineages::lineage_id.eq_any(lineages))
            .filter(maker_lineages::state.eq(LineageState::Pending.as_str())),
    )
    .set(maker_lineages::state.eq(state.as_str()))
    .returning(maker_lineages::lineage_id)
    .get_results::<Vec<u8>>(conn)?
    .into_iter()
    .collect())
}

fn order_status(note: &Note, state: proto::OrderState, reason: &str) -> proto::EventBody {
    let depth = OrderKeys::from_note(note).map_or(0, |keys| keys.depth);
    proto::EventBody {
        kind: Some(proto::event_body::Kind::OrderStatus(proto::OrderStatus {
            note_id: note.id().to_bytes().to_vec(),
            depth,
            state: state.into(),
            reason: reason.to_owned(),
        })),
    }
}

/// Activate submitted notes the watcher verified as committed and unspent,
/// in one core-writer transaction: store each order (ingest may already have
/// stored a public one), mark its lineage activated, and report Live for the
/// ones the liveness rule admits. Returns the book update; `write_book`
/// passes it through `live_orders`, storing cancelled ones Stopped.
pub fn activate_tx(
    conn: &mut PgConnection,
    submissions: &[PendingSubmission],
    arrival_unix: u64,
) -> DbResult<BookUpdate> {
    if submissions.is_empty() {
        return Ok(BookUpdate::default());
    }
    let rows = submissions
        .iter()
        .map(|submission| NewOrderRow::parsed(&submission.note, arrival_unix))
        .collect::<DbResult<Vec<_>>>()?;
    diesel::insert_into(orders::table)
        .values(&rows)
        .on_conflict(orders::note_id)
        .do_nothing()
        .execute(conn)?;
    let lineages: Vec<_> = submissions.iter().map(|s| &s.lineage_id).collect();
    let activated = settle_lineages(conn, &lineages, LineageState::Activated)?;
    let ids: Vec<Vec<u8>> = rows.iter().map(|row| row.note_id.clone()).collect();
    let live: HashSet<Vec<u8>> = live_orders::table
        .filter(live_orders::note_id.eq_any(&ids))
        .select(live_orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    for submission in submissions {
        let id = submission.note.id().to_bytes().to_vec();
        if activated.contains(&submission.lineage_id) && live.contains(&id) {
            let body = order_status(&submission.note, proto::OrderState::Live, "");
            gateway::append_event_tx(
                conn,
                submission.maker_id,
                Some(&submission.lineage_id),
                &body,
            )?;
        }
    }
    let active = orders::table
        .filter(orders::note_id.eq_any(&ids))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(OrderRow::as_select())
        .load::<OrderRow>(conn)?
        .into_iter()
        .map(OrderRow::into_book_order)
        .collect::<DbResult<_>>()?;
    Ok(BookUpdate {
        removed: Vec::new(),
        active,
    })
}

/// Reject submissions the watcher found unusable, reporting each once.
pub fn reject_tx(
    conn: &mut PgConnection,
    rejections: &[(PendingSubmission, proto::OrderState, &'static str)],
) -> DbResult<()> {
    let lineages: Vec<_> = rejections.iter().map(|(s, ..)| &s.lineage_id).collect();
    let rejected = settle_lineages(conn, &lineages, LineageState::Rejected)?;
    for (submission, state, reason) in rejections {
        if rejected.contains(&submission.lineage_id) {
            let body = order_status(&submission.note, *state, reason);
            gateway::append_event_tx(
                conn,
                submission.maker_id,
                Some(&submission.lineage_id),
                &body,
            )?;
        }
    }
    Ok(())
}

/// Report Unavailable for the live maker orders among `note_ids`, which are
/// about to be retired because their notes were spent elsewhere. Called by
/// every path that retires consumed orders, before it updates them. Orders
/// already reserved by our own settlement are not live, so they are not
/// reported.
pub fn report_spent_tx(conn: &mut PgConnection, note_ids: &[Vec<u8>]) -> DbResult<()> {
    /// Note ID, maker, lineage and depth of a live maker order.
    type Spent = (Vec<u8>, i64, Option<Vec<u8>>, Option<i64>);
    let spent: Vec<Spent> = live_orders::table
        .filter(live_orders::note_id.eq_any(note_ids))
        .filter(live_orders::maker_id.is_not_null())
        .select((
            live_orders::note_id,
            live_orders::maker_id.assume_not_null(),
            live_orders::lineage_id,
            live_orders::depth,
        ))
        .load(conn)?;
    for (note_id, maker_id, lineage_id, depth) in spent {
        let body = proto::EventBody {
            kind: Some(proto::event_body::Kind::OrderStatus(proto::OrderStatus {
                note_id,
                depth: u32::try_from(depth.unwrap_or(0))?,
                state: proto::OrderState::Unavailable.into(),
                reason: "note spent outside this solver".into(),
            })),
        };
        gateway::append_event_tx(conn, maker_id, lineage_id.as_deref(), &body)?;
    }
    Ok(())
}

/// Maker orders that may still be spent elsewhere: Active or Stopped rows
/// of a claimed lineage. Settling ones are resolved by the executor.
pub fn watched_maker_orders_tx(conn: &mut PgConnection) -> DbResult<Vec<Vec<u8>>> {
    Ok(orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(
            orders::status.eq_any([OrderStatus::Active.as_str(), OrderStatus::Stopped.as_str()]),
        )
        .select(orders::note_id)
        .load(conn)?)
}

/// The stored notes of `note_ids`.
pub fn order_notes_tx(conn: &mut PgConnection, note_ids: &[Vec<u8>]) -> DbResult<Vec<Note>> {
    orders::table
        .filter(orders::note_id.eq_any(note_ids))
        .select(OrderRow::as_select())
        .load::<OrderRow>(conn)?
        .iter()
        .map(OrderRow::note)
        .collect()
}

/// Where a settlement stands when it is reported to makers.
#[derive(Debug, Clone, Copy)]
pub enum SettlementPhase {
    /// Reserved and recorded; it may be broadcast.
    Pending,
    /// Our transaction is verified committed in `block`, consumed by
    /// `consumer` (the solver account).
    Committed {
        block: BlockNumber,
        consumer: AccountId,
    },
    /// It can never commit; nothing was filled.
    Voided,
}

/// What the maker needs to rebuild its payback and remainder notes for one
/// consumed input. The executor always fills with
/// `PswapNote::execute(solver, None, Some(fill))`, so the parent note, the
/// fill amount and the stored remainder determine every value exactly.
fn input_fill(
    parent: &Note,
    fill_amount: u64,
    remainder: Option<&Note>,
    lineage_id: &[u8],
) -> DbResult<proto::InputFill> {
    let pswap = PswapNote::try_from(parent).map_err(crate::types::OrderError::from)?;
    let offered = pswap.offered_asset().amount().as_u64();
    let (remaining_offered, remaining_requested) = match remainder {
        Some(note) => {
            let rest = PswapNote::try_from(note).map_err(crate::types::OrderError::from)?;
            (
                rest.offered_asset().amount().as_u64(),
                rest.storage().min_requested_amount(),
            )
        }
        None => (0, 0),
    };
    Ok(proto::InputFill {
        note_id: parent.id().to_bytes().to_vec(),
        lineage_id: lineage_id.to_vec(),
        depth: pswap.parent_depth() + 1,
        payback_amount: fill_amount,
        offered_paid: offered
            .checked_sub(remaining_offered)
            .ok_or(DbError::Corrupt("remainder offers more than its parent"))?,
        remaining_offered,
        remaining_requested,
        remainder_note_id: remainder.map_or_else(Vec::new, |note| note.id().to_bytes().to_vec()),
    })
}

/// Where an order stands after a resolved settlement: `subject` is the
/// remainder when there is one, else the input itself.
fn resulting_status(
    subject: &[u8],
    live: &HashSet<Vec<u8>>,
    statuses: &HashMap<Vec<u8>, String>,
) -> proto::ResultingStatus {
    if live.contains(subject) {
        proto::ResultingStatus::Live
    } else if statuses.get(subject).map(String::as_str)
        == Some(OrderStatus::OnchainNullified.as_str())
    {
        proto::ResultingStatus::Spent
    } else {
        proto::ResultingStatus::Stopped
    }
}

/// Report a settlement to each maker whose orders are among its inputs, one
/// event per maker, in the transaction that made the change: SettlementPending
/// when it is reserved, SettlementResolved when it commits or is voided.
/// `inputs` pairs each journal row with its parent order. Inputs of public
/// orders, and of attempts prepared before fills were recorded, are not
/// reported.
pub fn report_settlement_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    inputs: &[(SettlementInputRow, OrderRow)],
    phase: SettlementPhase,
) -> DbResult<()> {
    let parent_ids: Vec<&Vec<u8>> = inputs
        .iter()
        .map(|(input, _)| &input.parent_note_id)
        .collect();
    let owners: HashMap<Vec<u8>, (MakerId, Vec<u8>)> = orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(orders::note_id.eq_any(&parent_ids))
        .select((
            orders::note_id,
            maker_lineages::maker_id,
            maker_lineages::lineage_id,
        ))
        .load::<(Vec<u8>, i64, Vec<u8>)>(conn)?
        .into_iter()
        .map(|(note_id, maker_id, lineage_id)| (note_id, (maker_id, lineage_id)))
        .collect();
    if owners.is_empty() {
        return Ok(());
    }
    // A resolved input's order now lives on in its remainder, if any.
    let subjects: Vec<Vec<u8>> = inputs
        .iter()
        .map(|(input, _)| {
            input
                .child_note_id
                .clone()
                .unwrap_or_else(|| input.parent_note_id.clone())
        })
        .collect();
    let live: HashSet<Vec<u8>> = live_orders::table
        .filter(live_orders::note_id.eq_any(&subjects))
        .select(live_orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    let statuses: HashMap<Vec<u8>, String> = orders::table
        .filter(orders::note_id.eq_any(&subjects))
        .select((orders::note_id, orders::status))
        .load::<(Vec<u8>, String)>(conn)?
        .into_iter()
        .collect();

    let mut reports: BTreeMap<MakerId, (Vec<proto::InputFill>, Vec<proto::InputResult>)> =
        BTreeMap::new();
    for ((input, parent), subject) in inputs.iter().zip(&subjects) {
        let Some((maker_id, lineage_id)) = owners.get(&input.parent_note_id) else {
            continue;
        };
        let remainder = input
            .child_note_data
            .as_deref()
            .map(Note::read_from_bytes)
            .transpose()?;
        let fill = input
            .fill_amount
            .map(|amount| {
                input_fill(
                    &parent.note()?,
                    u64::try_from(amount)?,
                    remainder.as_ref(),
                    lineage_id,
                )
            })
            .transpose()?;
        let (fills, results) = reports.entry(*maker_id).or_default();
        let status = match phase {
            SettlementPhase::Pending => {
                fills.extend(fill);
                continue;
            }
            SettlementPhase::Committed { .. } => {
                fills.extend(fill);
                if input.child_note_id.is_some() {
                    resulting_status(subject, &live, &statuses)
                } else {
                    proto::ResultingStatus::Filled
                }
            }
            SettlementPhase::Voided => resulting_status(subject, &live, &statuses),
        };
        results.push(proto::InputResult {
            note_id: input.parent_note_id.clone(),
            lineage_id: lineage_id.clone(),
            status: status.into(),
        });
    }

    for (maker_id, (fills, results)) in reports {
        let kind = match phase {
            SettlementPhase::Pending if fills.is_empty() => continue,
            SettlementPhase::Pending => {
                proto::event_body::Kind::SettlementPending(proto::SettlementPending {
                    tx_id: tx_id.to_vec(),
                    fills,
                })
            }
            SettlementPhase::Committed { block, consumer } => {
                proto::event_body::Kind::SettlementResolved(proto::SettlementResolved {
                    tx_id: tx_id.to_vec(),
                    committed: true,
                    commit_block: block.as_u32(),
                    consumer_account_id: consumer.to_bytes(),
                    fills,
                    results,
                })
            }
            SettlementPhase::Voided => {
                proto::event_body::Kind::SettlementResolved(proto::SettlementResolved {
                    tx_id: tx_id.to_vec(),
                    committed: false,
                    results,
                    ..Default::default()
                })
            }
        };
        let body = proto::EventBody { kind: Some(kind) };
        gateway::append_event_tx(conn, maker_id, None, &body)?;
    }
    Ok(())
}

/// The three V1 events (frozen; ADR 0003).
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum EventKind {
    OrderStatus,
    SettlementPending,
    SettlementResolved,
}

/// Append an event to `maker_id`'s feed in the caller's business transaction
/// and return its sequence. The counter row update serializes appenders, so a
/// maker's committed sequences are contiguous; a rolled-back transaction
/// returns its numbers with it.
pub fn append_event_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    kind: EventKind,
    lineage_id: Option<&[u8]>,
    payload: &[u8],
) -> DbResult<u64> {
    let next: i64 = diesel::update(makers::table.find(maker_id))
        .set(makers::next_event_seq.eq(makers::next_event_seq + 1))
        .returning(makers::next_event_seq)
        .get_result(conn)?;
    let event_seq = next - 1;
    EventWake::mark_appended();
    let kind: &'static str = kind.into();
    diesel::insert_into(maker_events::table)
        .values((
            maker_events::maker_id.eq(maker_id),
            maker_events::event_seq.eq(event_seq),
            maker_events::kind.eq(kind),
            maker_events::lineage_id.eq(lineage_id),
            maker_events::payload.eq(payload),
        ))
        .execute(conn)?;
    Ok(u64::try_from(event_seq)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_db::{
        backfill_order_keys_tx, confirm_settlement_tx, finish_discarded_settlement_tx,
        insert_orders_batch_tx, live_book_update_tx, load_live_orders_tx, prepare_settlement_tx,
        test_consumer,
    };
    use crate::db::postgres_migrations;
    use crate::db::postgres_models::{NewOrderRow, SettlementAttemptRow, SettlementInputRow};
    use crate::db::postgres_schema::live_orders;
    use crate::db::postgres_test::TestSchema;
    use crate::types::OrderKeys;
    use crate::types::{BookOrder, BookUpdate, SettlementError};
    use anyhow::Result;
    use diesel::connection::SimpleConnection;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteAttachment, PswapNoteStorage};
    use prost::Message as _;
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    fn tokens() -> (AccountId, AccountId) {
        (
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(),
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap(),
        )
    }

    fn pswap(serial: u32, offered: AccountId, requested: AccountId) -> PswapNote {
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        PswapNote::builder()
            .sender(creator)
            .storage(
                PswapNoteStorage::builder()
                    .min_requested_asset(FungibleAsset::new(requested, 100).unwrap())
                    .min_fill_step(AssetAmount::new(1).unwrap())
                    .creator_account_id(creator)
                    .build(),
            )
            .serial_number(Word::from([serial, 0, 0, 0]))
            .note_type(NoteType::Private)
            .offered_asset(FungibleAsset::new(offered, 50).unwrap())
            .build()
            .unwrap()
    }

    fn note(serial: u32) -> Note {
        let (x, y) = tokens();
        pswap(serial, x, y).into()
    }

    fn remainder(parent: &Note) -> Note {
        let parent = PswapNote::try_from(parent).unwrap();
        parent
            .remainder_note(
                ACCOUNT_ID_REGULAR_PRIVATE_ACCOUNT_UPDATABLE_CODE
                    .try_into()
                    .unwrap(),
                &PswapNoteAttachment::new(AssetAmount::new(40).unwrap(), parent.order_id(), 1),
                AssetAmount::new(30).unwrap(),
                AssetAmount::new(60).unwrap(),
            )
            .unwrap()
    }

    fn header(maker_id: MakerId, request_id: &str, seq: u64) -> CommandHeader {
        CommandHeader::new(maker_id, request_id.into(), seq).unwrap()
    }

    fn run(
        conn: &mut PgConnection,
        maker_id: MakerId,
        request_id: &str,
        seq: u64,
        command: MakerCommand,
    ) -> Result<(CommandReply, Option<MakerFact>)> {
        Ok(conn.transaction::<_, DbError, _>(|conn| {
            execute_command_tx(conn, &header(maker_id, request_id, seq), &command)
        })?)
    }

    fn submit(note: &Note) -> MakerCommand {
        MakerCommand::submit(note.clone()).unwrap()
    }

    fn cancel_all(scope: CutoffScope) -> MakerCommand {
        MakerCommand::CancelAll { scope }
    }

    fn maker(conn: &mut PgConnection, name: &str) -> Result<MakerId> {
        Ok(create_maker_tx(conn, name)?.unwrap())
    }

    /// Store `notes` as ordinary ingested orders.
    fn ingest(conn: &mut PgConnection, notes: &[&Note]) -> Result<()> {
        let rows: Vec<_> = notes
            .iter()
            .map(|note| NewOrderRow::ingested(note, 1))
            .collect::<DbResult<_>>()?;
        conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &rows, 1))?;
        Ok(())
    }

    fn live(conn: &mut PgConnection, notes: &[&Note]) -> Result<Vec<bool>> {
        let ids: Vec<Vec<u8>> = live_orders::table.select(live_orders::note_id).load(conn)?;
        Ok(notes
            .iter()
            .map(|note| ids.contains(&note.id().to_bytes().to_vec()))
            .collect())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn retries_replay_and_reused_ids_conflict() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let maker_id = maker(conn, "alpha")?;
        let order = note(1);

        let (reply, fact) = run(conn, maker_id, "r1", 1, submit(&order))?;
        assert_eq!(reply, CommandReply::Committed(CommandResult::Accepted));
        let lineage_id = OrderKeys::from_note(&order)?.lineage_id;
        assert_eq!(
            fact,
            Some(MakerFact::LineageAttributed {
                lineage_id,
                tag: MakerTag {
                    maker_id,
                    root_seq: 1
                }
            })
        );

        // The exact retry gets the stored reply and repeats no fact.
        let (reply, fact) = run(conn, maker_id, "r1", 1, submit(&order))?;
        assert_eq!(reply, CommandReply::Replayed(CommandResult::Accepted));
        assert_eq!(fact, None);
        // Same request ID with other contents, or the same sequence under
        // another request ID, is a conflict.
        assert_eq!(
            run(conn, maker_id, "r1", 1, submit(&note(2)))?.0,
            CommandReply::Conflict
        );
        assert_eq!(
            run(conn, maker_id, "r1", 2, submit(&order))?.0,
            CommandReply::Conflict
        );
        assert_eq!(
            run(conn, maker_id, "r2", 1, cancel_all(CutoffScope::all()))?.0,
            CommandReply::Conflict
        );
        // Another maker's IDs and sequences are its own.
        let other = maker(conn, "beta")?;
        assert_eq!(
            run(conn, other, "r1", 1, cancel_all(CutoffScope::all()))?.0,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 1,
                settling: 0
            })
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_lineage_is_claimed_once() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let order = note(1);
        run(conn, alpha, "a1", 1, submit(&order))?;

        // A later note of the same lineage, by either maker, is refused,
        // and a cancel cannot be undone by resubmitting under a new ID.
        for (maker_id, request, seq, note) in [
            (alpha, "a2", 2, order.clone()),
            (alpha, "a3", 3, remainder(&order)),
            (beta, "b1", 1, order.clone()),
        ] {
            let (reply, fact) = run(conn, maker_id, request, seq, submit(&note))?;
            assert_eq!(
                reply,
                CommandReply::Committed(CommandResult::AlreadyRegistered)
            );
            assert_eq!(fact, None);
        }
        let owner: MakerId = maker_lineages::table
            .select(maker_lineages::maker_id)
            .first(conn)?;
        assert_eq!(owner, alpha);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn cutoffs_only_rise_and_stop_older_submits_in_scope() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let (x, y) = tokens();
        let (old, new, sell, public, beta_order) = (
            note(1),
            note(2),
            Note::from(pswap(3, y, x)),
            note(4),
            note(5),
        );
        run(conn, alpha, "s90", 90, submit(&old))?;
        run(conn, alpha, "s101", 101, submit(&new))?;
        run(conn, alpha, "s95", 95, submit(&sell))?;
        run(conn, beta, "s1", 1, submit(&beta_order))?;
        ingest(conn, &[&old, &new, &sell, &public, &beta_order])?;
        let all = [&old, &new, &sell, &public, &beta_order];
        assert_eq!(live(conn, &all)?, [true; 5]);

        // Cancel the x→y direction at 100: only alpha's older x→y order stops.
        let (reply, fact) = run(
            conn,
            alpha,
            "c100",
            100,
            cancel_all(CutoffScope::direction(x, y)),
        )?;
        assert_eq!(
            reply,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 100,
                settling: 0
            })
        );
        assert!(matches!(
            fact,
            Some(MakerFact::CutoffRaised { cutoff: 100, .. })
        ));
        assert_eq!(live(conn, &all)?, [false, true, true, true, true]);

        // A delayed lower cancel cannot lower the barrier.
        assert_eq!(
            run(
                conn,
                alpha,
                "c80",
                80,
                cancel_all(CutoffScope::direction(x, y))
            )?
            .0,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 100,
                settling: 0
            })
        );
        // The whole market at 96 also stops the y→x order; 101 stays.
        run(
            conn,
            alpha,
            "c96",
            96,
            cancel_all(CutoffScope::market(x, y)),
        )?;
        assert_eq!(live(conn, &all)?, [false, true, false, true, true]);

        // A delayed submit below the barrier is accepted but never live, and
        // its remainder inherits the root sequence.
        let delayed = note(6);
        assert_eq!(
            run(conn, alpha, "s91", 91, submit(&delayed))?.0,
            CommandReply::Committed(CommandResult::Accepted)
        );
        let child = remainder(&delayed);
        ingest(conn, &[&delayed, &child])?;
        assert_eq!(live(conn, &[&delayed, &child])?, [false, false]);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn an_empty_scope_still_bars_later_arrivals() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        run(conn, alpha, "c10", 10, cancel_all(CutoffScope::all()))?;
        let late = note(1);
        run(conn, alpha, "s5", 5, submit(&late))?;
        ingest(conn, &[&late])?;
        assert_eq!(live(conn, &[&late])?, [false]);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_targeted_stop_applies_before_or_after_submit_for_its_maker_only() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let (before, after, kept) = (note(1), note(2), note(3));
        let stop = |note: &Note| MakerCommand::CancelOrder {
            lineage_id: OrderKeys::from_note(note).unwrap().lineage_id,
        };

        // Stopped before its submit arrives.
        assert_eq!(
            run(conn, alpha, "x1", 1, stop(&before))?.0,
            CommandReply::Committed(CommandResult::Stopped { settling: 0 })
        );
        run(conn, alpha, "s2", 2, submit(&before))?;
        run(conn, alpha, "s3", 3, submit(&after))?;
        run(conn, alpha, "s4", 4, submit(&kept))?;
        // Beta cannot stop alpha's order.
        run(conn, beta, "x1", 1, stop(&kept))?;
        ingest(conn, &[&before, &after, &kept])?;
        assert_eq!(live(conn, &[&before, &after, &kept])?, [false, true, true]);

        run(conn, alpha, "x5", 5, stop(&after))?;
        let child = remainder(&after);
        ingest(conn, &[&child])?;
        assert_eq!(
            live(conn, &[&before, &after, &kept, &child])?,
            [false, false, true, false]
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn cancels_report_reserved_orders_and_never_rewrite_rows() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let (reserved, idle) = (note(1), note(2));
        run(conn, alpha, "s1", 1, submit(&reserved))?;
        run(conn, alpha, "s2", 2, submit(&idle))?;
        ingest(conn, &[&reserved, &idle])?;
        diesel::update(orders::table.filter(orders::note_id.eq(reserved.id().to_bytes().to_vec())))
            .set(orders::status.eq(OrderStatus::Settling.as_str()))
            .execute(conn)?;

        let statuses = |conn: &mut PgConnection| -> Result<Vec<String>> {
            Ok(orders::table
                .order(orders::priority_seq)
                .select(orders::status)
                .load(conn)?)
        };
        let before = statuses(conn)?;
        assert_eq!(
            run(conn, alpha, "c3", 3, cancel_all(CutoffScope::all()))?.0,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 3,
                settling: 1
            })
        );
        let stop = MakerCommand::CancelOrder {
            lineage_id: OrderKeys::from_note(&reserved)?.lineage_id,
        };
        assert_eq!(
            run(conn, alpha, "x4", 4, stop)?.0,
            CommandReply::Committed(CommandResult::Stopped { settling: 1 })
        );
        assert_eq!(statuses(conn)?, before, "a cancel writes no order row");
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn one_conflicting_command_does_not_abort_its_batch() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        run(conn, alpha, "s1", 1, submit(&note(1)))?;
        let replies = conn.transaction::<_, DbError, _>(|conn| {
            [
                (header(alpha, "c9", 9), cancel_all(CutoffScope::all())),
                (header(alpha, "s1", 1), submit(&note(2))),
                (header(alpha, "s1", 1), submit(&note(1))),
                (header(alpha, "s2", 2), submit(&note(1))),
                (header(alpha, "s3", 3), submit(&note(3))),
            ]
            .iter()
            .map(|(header, command)| Ok(execute_command_tx(conn, header, command)?.0))
            .collect::<DbResult<Vec<_>>>()
        })?;
        assert_eq!(
            replies,
            [
                CommandReply::Committed(CommandResult::Applied {
                    cutoff: 9,
                    settling: 0
                }),
                CommandReply::Conflict,
                CommandReply::Replayed(CommandResult::Accepted),
                CommandReply::Committed(CommandResult::AlreadyRegistered),
                CommandReply::Committed(CommandResult::Accepted),
            ]
        );
        let stored: i64 = maker_commands::table.count().get_result(conn)?;
        assert_eq!(stored, 4);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn event_sequences_are_contiguous_per_maker() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let append = |conn: &mut PgConnection, maker_id| {
            conn.transaction::<_, DbError, _>(|conn| {
                append_event_tx(conn, maker_id, EventKind::OrderStatus, None, b"")
            })
        };
        assert_eq!(append(conn, alpha)?, 1);
        assert_eq!(append(conn, alpha)?, 2);
        assert_eq!(append(conn, beta)?, 1);
        // A rolled-back append gives its number back.
        let rolled_back = conn.transaction::<(), DbError, _>(|conn| {
            append_event_tx(conn, alpha, EventKind::SettlementPending, None, b"")?;
            Err(DbError::Corrupt("roll back"))
        });
        assert!(rolled_back.is_err());
        assert_eq!(append(conn, alpha)?, 3);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn api_keys_authenticate_until_revoked() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        assert_eq!(create_maker_tx(conn, "alpha")?, None, "names are unique");
        let key = crate::maker::new_api_key();
        let hash = crate::maker::api_key_hash(&key);
        assert_eq!(issue_api_key_tx(conn, alpha + 1_000, &hash)?, None);
        let key_id = issue_api_key_tx(conn, alpha, &hash)?.unwrap();
        assert_eq!(authenticate_tx(conn, &hash)?, Some(alpha));
        assert_eq!(
            authenticate_tx(conn, &crate::maker::api_key_hash("mmk_wrong"))?,
            None
        );
        assert!(revoke_api_key_tx(conn, key_id)?);
        assert!(!revoke_api_key_tx(conn, key_id)?);
        assert_eq!(authenticate_tx(conn, &hash)?, None);
        Ok(())
    }

    fn reserve(conn: &mut PgConnection, tx: u8, parent: &Note) -> DbResult<()> {
        prepare_settlement_tx(
            conn,
            &SettlementAttemptRow {
                tx_id: vec![tx],
                tx_result: vec![tx],
                status: "prepared".into(),
            },
            &[SettlementInputRow {
                tx_id: vec![tx],
                parent_note_id: parent.id().to_bytes().to_vec(),
                child_note_id: None,
                child_note_data: None,
                fill_amount: None,
            }],
        )
    }

    fn connect(fixture: &TestSchema) -> Result<PgConnection> {
        let mut conn = postgres_migrations::connect(&std::env::var("SOLVER_TEST_DATABASE_URL")?)?;
        conn.batch_execute(&format!("SET search_path TO {}", fixture.name))?;
        Ok(conn)
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_cancel_waits_for_a_reservation_in_flight_and_reports_it() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let alpha = maker(&mut fixture.conn, "alpha")?;
        let quote = note(1);
        run(&mut fixture.conn, alpha, "s1", 1, submit(&quote))?;
        ingest(&mut fixture.conn, &[&quote])?;

        // The reservation holds the maker control row until it commits.
        let held = Arc::new(Barrier::new(2));
        let mut reserver = connect(&fixture)?;
        let reserving = {
            let (held, quote) = (held.clone(), quote.clone());
            std::thread::spawn(move || {
                reserver.transaction::<_, DbError, _>(|conn| {
                    reserve(conn, 1, &quote)?;
                    held.wait();
                    std::thread::sleep(Duration::from_millis(300));
                    Ok(())
                })
            })
        };
        held.wait();
        let started = Instant::now();
        let mut canceller = connect(&fixture)?;
        let (reply, _) = run(
            &mut canceller,
            alpha,
            "c2",
            2,
            cancel_all(CutoffScope::all()),
        )?;
        reserving.join().expect("reserver panicked")?;
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "the cancel waited"
        );
        assert_eq!(
            reply,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 2,
                settling: 1
            }),
            "the reservation that won is reported as exposure"
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_committed_cancel_fails_every_later_reservation() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let (quote, stopped, public) = (note(1), note(2), note(3));
        run(conn, alpha, "s1", 1, submit(&quote))?;
        run(conn, alpha, "s5", 5, submit(&stopped))?;
        ingest(conn, &[&quote, &stopped, &public])?;
        run(conn, alpha, "c2", 2, cancel_all(CutoffScope::all()))?;
        let lineage_id = OrderKeys::from_note(&stopped)?.lineage_id;
        run(
            conn,
            alpha,
            "x6",
            6,
            MakerCommand::CancelOrder { lineage_id },
        )?;

        for (tx, parent) in [(1, &quote), (2, &stopped)] {
            let result = conn.transaction::<_, DbError, _>(|conn| reserve(conn, tx, parent));
            assert!(matches!(
                result,
                Err(DbError::Settlement(SettlementError::InputOrderNotActive))
            ));
        }
        conn.transaction::<_, DbError, _>(|conn| reserve(conn, 3, &public))?;
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn book_updates_carry_only_live_orders_and_their_tags() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let (cut, kept, public) = (note(1), note(2), note(3));
        run(conn, alpha, "s1", 1, submit(&cut))?;
        run(conn, alpha, "s5", 5, submit(&kept))?;
        run(conn, alpha, "c3", 3, cancel_all(CutoffScope::all()))?;
        ingest(conn, &[&cut, &kept, &public])?;

        let candidates = [&cut, &kept, &public].map(|note| BookOrder {
            priority_seq: 1,
            arrival_unix: 1,
            note: Arc::new((*note).clone()),
            maker: None,
        });
        let update = conn.transaction::<_, DbError, _>(|conn| {
            live_book_update_tx(
                conn,
                BookUpdate {
                    removed: Vec::new(),
                    active: candidates.to_vec(),
                },
            )
        })?;
        let tags: Vec<_> = update
            .active
            .iter()
            .map(|order| (order.id(), order.maker))
            .collect();
        let tag = MakerTag {
            maker_id: alpha,
            root_seq: 5,
        };
        assert_eq!(tags, [(kept.id(), Some(tag)), (public.id(), None)]);
        let status: String = orders::table
            .find(cut.id().to_bytes().to_vec())
            .select(orders::status)
            .first(conn)?;
        assert_eq!(
            status,
            OrderStatus::Stopped.as_str(),
            "stored Stopped by a write that happened anyway"
        );

        let hydrated: Vec<_> = load_live_orders_tx(conn)?
            .into_iter()
            .map(|order| (order.id(), order.maker))
            .collect();
        assert_eq!(hydrated, tags);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn order_keys_are_backfilled_for_rows_stored_before_the_migration() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (older, finished) = (note(1), note(2));
        ingest(conn, &[&older, &finished])?;
        diesel::update(orders::table)
            .set((
                orders::lineage_id.eq(None::<Vec<u8>>),
                orders::depth.eq(None::<i64>),
                orders::market.eq(None::<Vec<u8>>),
                orders::direction.eq(None::<Vec<u8>>),
            ))
            .execute(conn)?;
        diesel::update(orders::table.find(finished.id().to_bytes().to_vec()))
            .set(orders::status.eq(OrderStatus::Executed.as_str()))
            .execute(conn)?;
        assert_eq!(backfill_order_keys_tx(conn)?, 1, "only unfinished rows");
        let lineage: Option<Vec<u8>> = orders::table
            .find(older.id().to_bytes().to_vec())
            .select(orders::lineage_id)
            .first(conn)?;
        assert_eq!(lineage, Some(OrderKeys::from_note(&older)?.lineage_id));
        assert_eq!(backfill_order_keys_tx(conn)?, 0);
        Ok(())
    }

    /// A journal row filling `parent` with `amount` exactly as the executor
    /// does, and the payback and remainder notes the transaction creates.
    fn fill(tx: u8, parent: &Note, amount: u64) -> (SettlementInputRow, Note, Option<Note>) {
        let pswap = PswapNote::try_from(parent).unwrap();
        let requested = pswap.storage().requested_faucet_id();
        let (payback, remainder) = pswap
            .execute(
                test_consumer(),
                None,
                Some(FungibleAsset::new(requested, amount).unwrap()),
            )
            .unwrap();
        let remainder = remainder.map(Note::from);
        let row = SettlementInputRow {
            tx_id: vec![tx],
            parent_note_id: parent.id().to_bytes().to_vec(),
            child_note_id: remainder.as_ref().map(|note| note.id().to_bytes().to_vec()),
            child_note_data: remainder.as_ref().map(Serializable::to_bytes),
            fill_amount: Some(i64::try_from(amount).unwrap()),
        };
        (row, payback, remainder)
    }

    fn prepare(conn: &mut PgConnection, tx: u8, inputs: &[SettlementInputRow]) -> Result<()> {
        let attempt = SettlementAttemptRow {
            tx_id: vec![tx],
            tx_result: vec![tx],
            status: "prepared".into(),
        };
        conn.transaction::<_, DbError, _>(|conn| prepare_settlement_tx(conn, &attempt, inputs))?;
        Ok(())
    }

    fn settlement_events(
        conn: &mut PgConnection,
        maker_id: MakerId,
    ) -> Result<Vec<proto::event_body::Kind>> {
        Ok(read_events_tx(conn, maker_id, 0, 100)?
            .into_iter()
            .map(|event| {
                proto::EventBody::decode(event.payload.as_slice())
                    .unwrap()
                    .kind
                    .unwrap()
            })
            .collect())
    }

    fn resolved(kind: &proto::event_body::Kind) -> &proto::SettlementResolved {
        match kind {
            proto::event_body::Kind::SettlementResolved(resolved) => resolved,
            other => panic!("expected SettlementResolved, got {other:?}"),
        }
    }

    /// The maker's side: rebuild both output notes from its ORIGINAL note and
    /// the reported fill, with the pinned SDK.
    fn rebuild(original: &Note, fill: &proto::InputFill, consumer: &[u8]) -> (Note, Option<Note>) {
        let original = PswapNote::try_from(original).unwrap();
        let consumer = AccountId::read_from_bytes(consumer).unwrap();
        let attachment = |amount| {
            PswapNoteAttachment::new(
                AssetAmount::new(amount).unwrap(),
                original.order_id(),
                fill.depth,
            )
        };
        let payback = original
            .payback_note(consumer, &attachment(fill.payback_amount))
            .unwrap();
        let remainder = (!fill.remainder_note_id.is_empty()).then(|| {
            original
                .remainder_note(
                    consumer,
                    &attachment(fill.offered_paid),
                    AssetAmount::new(fill.remaining_offered).unwrap(),
                    AssetAmount::new(fill.remaining_requested).unwrap(),
                )
                .unwrap()
        });
        (payback, remainder)
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_maker_rebuilds_its_notes_from_reported_fills_across_rounds() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let (quote, other, public) = (note(1), note(2), note(3));
        run(conn, alpha, "s1", 1, submit(&quote))?;
        run(conn, beta, "s1", 1, submit(&other))?;
        ingest(conn, &[&quote, &other, &public])?;

        // Round 1: a partial fill of alpha's quote, beside beta's and a public order.
        let (row, payback, remainder) = fill(1, &quote, 40);
        let (beta_row, ..) = fill(1, &other, 100);
        let (public_row, ..) = fill(1, &public, 100);
        prepare(conn, 1, &[row, beta_row, public_row])?;
        let pending = settlement_events(conn, alpha)?;
        let proto::event_body::Kind::SettlementPending(pending) = &pending[0] else {
            panic!("expected SettlementPending");
        };
        assert_eq!(pending.fills.len(), 1, "only alpha's own input");
        assert_eq!(pending.fills[0].payback_amount, 40);

        let block = BlockNumber::from(42_u32);
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_tx(conn, &[1], &HashSet::new(), block, test_consumer())
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(&events[1]);
        assert!(report.committed);
        assert_eq!(report.commit_block, 42);
        assert_eq!(report.fills, pending.fills, "the fill is what was intended");
        assert_eq!(
            report.results[0].status(),
            proto::ResultingStatus::Live,
            "the remainder is live"
        );
        let (rebuilt_payback, rebuilt_remainder) =
            rebuild(&quote, &report.fills[0], &report.consumer_account_id);
        assert_eq!(rebuilt_payback.id(), payback.id());
        let remainder = remainder.unwrap();
        assert_eq!(rebuilt_remainder.unwrap().id(), remainder.id());
        // Beta hears about its own full fill only.
        let beta_events = settlement_events(conn, beta)?;
        assert_eq!(resolved(&beta_events[1]).fills.len(), 1);
        assert_eq!(
            resolved(&beta_events[1]).results[0].status(),
            proto::ResultingStatus::Filled
        );

        // Round 2 fills the remainder; the maker still rebuilds from its original note.
        let (row, payback, second) = fill(2, &remainder, 20);
        prepare(conn, 2, &[row])?;
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_tx(conn, &[2], &HashSet::new(), block, test_consumer())
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(events.last().unwrap());
        assert_eq!(report.fills[0].depth, 2);
        let (rebuilt_payback, rebuilt_remainder) =
            rebuild(&quote, &report.fills[0], &report.consumer_account_id);
        assert_eq!(rebuilt_payback.id(), payback.id());
        assert_eq!(rebuilt_remainder.unwrap().id(), second.unwrap().id());
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_late_fill_after_a_cancel_reports_its_remainder_stopped() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let quote = note(1);
        run(conn, alpha, "s1", 1, submit(&quote))?;
        ingest(conn, &[&quote])?;
        let (row, ..) = fill(1, &quote, 40);
        prepare(conn, 1, &[row])?;
        // The cancel lands while the settlement is in flight.
        assert_eq!(
            run(conn, alpha, "c2", 2, cancel_all(CutoffScope::all()))?.0,
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 2,
                settling: 1
            })
        );
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_tx(
                conn,
                &[1],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
            )
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(events.last().unwrap());
        assert!(report.committed);
        assert_eq!(report.fills.len(), 1, "the fill is reported");
        assert_eq!(report.results[0].status(), proto::ResultingStatus::Stopped);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_voided_settlement_reports_where_each_input_stands() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let (returned, spent, stopped) = (note(1), note(2), note(3));
        for (seq, order) in [(1, &returned), (2, &spent), (3, &stopped)] {
            run(conn, alpha, &format!("s{seq}"), seq, submit(order))?;
        }
        ingest(conn, &[&returned, &spent, &stopped])?;
        let rows: Vec<_> = [&returned, &spent, &stopped]
            .into_iter()
            .map(|order| fill(1, order, 100).0)
            .collect();
        prepare(conn, 1, &rows)?;
        let lineage_id = OrderKeys::from_note(&stopped)?.lineage_id;
        run(
            conn,
            alpha,
            "x4",
            4,
            MakerCommand::CancelOrder { lineage_id },
        )?;

        let consumed = HashSet::from([spent.id()]);
        conn.transaction::<_, DbError, _>(|conn| {
            finish_discarded_settlement_tx(conn, &[1], &consumed)
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(events.last().unwrap());
        assert!(!report.committed);
        assert!(report.fills.is_empty(), "nothing was filled");
        let statuses: HashMap<Vec<u8>, proto::ResultingStatus> = report
            .results
            .iter()
            .map(|result| (result.note_id.clone(), result.status()))
            .collect();
        assert_eq!(
            statuses[&returned.id().to_bytes().to_vec()],
            proto::ResultingStatus::Live
        );
        assert_eq!(
            statuses[&spent.id().to_bytes().to_vec()],
            proto::ResultingStatus::Spent
        );
        assert_eq!(
            statuses[&stopped.id().to_bytes().to_vec()],
            proto::ResultingStatus::Stopped
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn reverting_after_maker_activity_is_refused() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        run(conn, alpha, "s1", 1, submit(&note(1)))?;
        // The settlement-fills column goes first; the maker tables refuse.
        postgres_migrations::revert_last(conn)?;
        assert!(postgres_migrations::revert_last(conn).is_err());

        let mut empty = TestSchema::migrated()?;
        postgres_migrations::revert_last(&mut empty.conn)?;
        postgres_migrations::revert_last(&mut empty.conn)?;
        postgres_migrations::migrate(&mut empty.conn)?;
        Ok(())
    }
}

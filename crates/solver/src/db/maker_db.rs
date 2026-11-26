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
use diesel::sql_types::{BigInt, Binary, Nullable, Text};
use std::collections::{HashMap, HashSet};

use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;

use super::error::{DbError, DbResult};
use super::postgres_db::INSERT_CHUNK_ROWS;
use super::postgres_models::{NewOrderRow, OrderRow, SettlementInputRow};
use super::postgres_schema::{
    api_keys, eligible_orders, live_orders, maker_commands, maker_cutoffs, maker_events,
    maker_lineages, maker_stops, makers, orders, sync_state,
};
use crate::gateway::{self, proto};
use crate::maker::{
    CommandHeader, CommandReply, CommandResult, CutoffScope, EventWake, LineageId, MakerCommand,
    MakerId, MakerUpdate,
};
use crate::types::{now_unix, BookOrder, BookUpdate, OrderId, OrderKeys, OrderStatus};

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

/// Reserve every maker control row for a group-commit round in the same
/// order as settlement. A round may contain commands for several makers in
/// arbitrary arrival order; locking one per command can deadlock a
/// reservation covering those makers in the opposite order.
pub fn prelock_command_makers_tx(
    conn: &mut PgConnection,
    commands: &[(CommandHeader, MakerCommand)],
) -> DbResult<()> {
    let maker_ids: Vec<_> = commands.iter().map(|(header, _)| header.maker_id).collect();
    lock_makers_ordered_tx(conn, &maker_ids)
}

/// Lock all event-counter rows a multi-maker business transaction may update.
pub fn lock_makers_ordered_tx(conn: &mut PgConnection, maker_ids: &[MakerId]) -> DbResult<()> {
    let mut maker_ids = maker_ids.to_vec();
    maker_ids.sort_unstable();
    maker_ids.dedup();
    if maker_ids.is_empty() {
        return Ok(());
    }
    makers::table
        .filter(makers::maker_id.eq_any(maker_ids))
        .order(makers::maker_id.asc())
        .for_no_key_update()
        .select(makers::maker_id)
        .load::<MakerId>(conn)?;
    Ok(())
}

/// The watcher may write status events for several makers in one round.
/// Acquire existing order rows first, then all maker counters in ID order,
/// matching a multi-maker settlement reservation.
pub fn prelock_watcher_tx(
    conn: &mut PgConnection,
    activations: &[PendingSubmission],
    rejections: &[PendingSubmission],
    capped: &[PendingSubmission],
    spent_ids: &[Vec<u8>],
) -> DbResult<()> {
    let submissions = || activations.iter().chain(rejections).chain(capped);
    let mut note_ids = spent_ids.to_vec();
    note_ids.extend(submissions().map(|s| s.note_id.clone()));
    note_ids.sort();
    note_ids.dedup();
    if !note_ids.is_empty() {
        orders::table
            .filter(orders::note_id.eq_any(&note_ids))
            .order(orders::note_id.asc())
            .for_update()
            .select(orders::note_id)
            .load::<Vec<u8>>(conn)?;
    }
    let mut makers: Vec<_> = submissions().map(|s| s.maker_id).collect();
    if !spent_ids.is_empty() {
        makers.extend(
            eligible_orders::table
                .filter(eligible_orders::note_id.eq_any(spent_ids))
                .filter(eligible_orders::status.eq_any(shown_live()))
                .filter(eligible_orders::maker_id.is_not_null())
                .select(eligible_orders::maker_id.assume_not_null())
                .load::<MakerId>(conn)?,
        );
    }
    lock_makers_ordered_tx(conn, &makers)
}

/// [`execute_command_tx`] on the confirmed path, for tests that need
/// only the reply and the matcher's update.
#[cfg(test)]
pub fn execute_command_confirmed_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
) -> DbResult<(CommandReply, Option<MakerUpdate>)> {
    let outcome = execute_command_tx(conn, header, command, AcceptMode::Off)?;
    Ok((outcome.reply, outcome.update))
}

/// Why a submit with live at accept switched on still took the confirmed path
/// (ADR 0008), for `/metrics`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum AcceptFallback {
    /// The watcher's latest block was not read recently enough.
    WatcherStale,
}

/// Whether a submit goes live at accept in one intake round (ADR
/// 0008), decided once for the round by [`AcceptGate::resolve`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptMode {
    /// Live at accept is off: every order takes the confirmed path.
    Off,
    /// On, but the round has no fresh watcher block: the confirmed path,
    /// counted as a fallback.
    Stale,
    /// On: an order accepted in this round is measured from this block.
    Fresh(BlockNumber),
}

/// Intake's owner-epoch guard, and its live-at-accept gate (ADR 0008).
pub struct AcceptGate {
    owner_epoch: i64,
    /// `None` when live at accept is off: the gate then only guards the
    /// owner epoch, for self-trade prevention at submit (ADR 0009).
    fresh_block: Option<Box<dyn Fn() -> Option<BlockNumber> + Send + Sync>>,
}

impl AcceptGate {
    /// `fresh_block` is the watcher's latest synced block if it was read from
    /// the chain recently enough, else `None`.
    pub fn new(
        owner_epoch: i64,
        fresh_block: impl Fn() -> Option<BlockNumber> + Send + Sync + 'static,
    ) -> Self {
        Self {
            owner_epoch,
            fresh_block: Some(Box::new(fresh_block)),
        }
    }

    /// Only the owner-epoch guard: every submit takes the confirmed path.
    pub fn epoch_only(owner_epoch: i64) -> Self {
        Self {
            owner_epoch,
            fresh_block: None,
        }
    }

    /// Decide the mode for a round that holds a submit. It guards a
    /// writer takeover: it share-locks the owner epoch, so a new owner's
    /// claim waits for this commit, and it fails if a new owner claimed it
    /// already. The freshness is read after that lock.
    ///
    /// This must be the round's first statement. In one solver, intake never
    /// overlaps ingest: both hold the publication guard through their
    /// transaction. But during a takeover, the old process's intake runs
    /// beside the new owner's ingest, which updates `sync_state` before it
    /// locks maker rows; taking this share lock after a maker lock would
    /// deadlock with it. Commands take the resolved mode, never the gate, so
    /// no later statement can take the lock.
    pub fn resolve(&self, conn: &mut PgConnection) -> DbResult<AcceptMode> {
        let current: i64 = sync_state::table
            .find(1_i16)
            .for_share()
            .select(sync_state::owner_epoch)
            .first(conn)?;
        if current != self.owner_epoch {
            return Err(DbError::IntakeEpochMoved {
                ours: self.owner_epoch,
                current,
            });
        }
        Ok(match &self.fresh_block {
            None => AcceptMode::Off,
            Some(fresh_block) => fresh_block().map_or(AcceptMode::Stale, AcceptMode::Fresh),
        })
    }
}

/// What one maker command did.
#[derive(Debug)]
pub struct CommandOutcome {
    pub reply: CommandReply,
    /// The update the matcher must learn once the transaction commits.
    pub update: Option<MakerUpdate>,
    /// The order that went live at accept, with its maker tag (ADR 0008).
    pub live: Option<BookOrder>,
    /// Why a submit that could have gone live at accept did not.
    pub fallback: Option<AcceptFallback>,
}

/// Write one maker command, returning its reply and the update the matcher
/// must learn once the transaction commits. An exact retry or a conflict
/// changes nothing and has no update: a change lost to a crash is restored
/// by the matcher's startup hydration.
///
/// Unless `accept` is off, a newly claimed note goes live in this
/// transaction (ADR 0008) if the round's watcher block is fresh and the
/// order is not cancelled already; otherwise it takes the confirmed path.
pub fn execute_command_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
    accept: AcceptMode,
) -> DbResult<CommandOutcome> {
    write_command_tx(conn, header, command, accept, false)
}

/// [`execute_command_tx`] in an intake round with self-trade prevention at
/// submit on (ADR 0009): a submit is only claimed (the round decides which
/// claims go live), and one with more than [`MAX_NEWER_LINEAGES`] of its
/// maker's orders on the pair above its seq is refused before the claim.
/// An exact retry still gets its stored reply.
pub fn claim_command_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
) -> DbResult<CommandOutcome> {
    write_command_tx(conn, header, command, AcceptMode::Off, true)
}

fn write_command_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    command: &MakerCommand,
    accept: AcceptMode,
    refuse_below_history: bool,
) -> DbResult<CommandOutcome> {
    let replayed = |reply| CommandOutcome {
        reply,
        update: None,
        live: None,
        fallback: None,
    };
    if lock_maker(conn, header.maker_id)?.is_none() {
        return Err(DbError::Corrupt("authenticated maker is missing"));
    }
    if let Some(reply) = claim_command(conn, header, command)? {
        return Ok(replayed(reply));
    }
    let maker_id = header.maker_id;
    let mut live = None;
    let mut fallback = None;
    let (result, update) = match command {
        MakerCommand::Submit {
            note,
            keys,
            amounts,
            expires_at_unix_ms,
        } => {
            if refuse_below_history
                && self_trade::far_below_history_tx(conn, maker_id, &keys.market, header.seq)?
            {
                (CommandResult::SeqBelowHistory, None)
            } else if claim_lineage_tx(conn, header, note, keys, *amounts, *expires_at_unix_ms)? {
                match accept {
                    AcceptMode::Off => {}
                    AcceptMode::Stale => fallback = Some(AcceptFallback::WatcherStale),
                    AcceptMode::Fresh(block) => {
                        live = go_live_at_accept_tx(conn, block, maker_id, &keys.lineage_id, note)?;
                    }
                }
                (CommandResult::Accepted, None)
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
            let update = MakerUpdate::CutoffRaised {
                maker_id,
                scope: scope.clone(),
                cutoff,
            };
            (CommandResult::Applied { cutoff, settling }, Some(update))
        }
        MakerCommand::CancelOrder { lineage_id } => {
            stop_lineages_tx(
                conn,
                &[Stop {
                    maker_id,
                    lineage_id,
                    source: StopSource::Maker,
                    caused_by: None,
                }],
            )?;
            let settling = settling_in_lineage(conn, maker_id, lineage_id)?;
            let update = MakerUpdate::LineageCancelled {
                maker_id,
                lineage_id: lineage_id.clone(),
            };
            (CommandResult::Stopped { settling }, Some(update))
        }
    };
    let stored = serde_json::to_string(&result)
        .map_err(|_| DbError::Corrupt("command result does not serialize"))?;
    diesel::update(maker_commands::table.find((maker_id, &header.request_id)))
        .set(maker_commands::result.eq(stored))
        .execute(conn)?;
    Ok(CommandOutcome {
        reply: CommandReply::Committed(result),
        update,
        live,
        fallback,
    })
}

/// Claim a submit's lineage for its maker, with the amounts it was submitted
/// at. `false` when the lineage is claimed already, or when its lineage or
/// its note already has an order row: a maker order is a private note, and
/// its row is written only after the claim. The note check matters only for
/// a row with a NULL `lineage_id`, which is a row stored before the
/// 2026-10-03 migration (every writer since sets it from the note). Without
/// it, go-live at accept's plain insert could meet such a row and fail the
/// whole round.
fn claim_lineage_tx(
    conn: &mut PgConnection,
    header: &CommandHeader,
    note: &Note,
    keys: &OrderKeys,
    (offered, requested): (u64, u64),
    expires_at_unix_ms: Option<u64>,
) -> DbResult<bool> {
    let claimed = diesel::sql_query(
        "INSERT INTO maker_lineages (lineage_id, maker_id, request_id, root_seq, note_id,
                                     market, direction, expires_at_unix_ms,
                                     offered_amount, requested_amount)
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
         WHERE NOT EXISTS (SELECT 1 FROM orders WHERE lineage_id = $1 OR note_id = $5)
         ON CONFLICT DO NOTHING",
    )
    .bind::<Binary, _>(&keys.lineage_id)
    .bind::<BigInt, _>(header.maker_id)
    .bind::<Text, _>(&header.request_id)
    .bind::<BigInt, _>(seq_column(header.seq)?)
    .bind::<Binary, _>(note.id().to_bytes().to_vec())
    .bind::<Binary, _>(&keys.market)
    .bind::<Binary, _>(&keys.direction)
    .bind::<Nullable<BigInt>, _>(expires_at_unix_ms.map(i64::try_from).transpose()?)
    .bind::<BigInt, _>(i64::try_from(offered)?)
    .bind::<BigInt, _>(i64::try_from(requested)?)
    .execute(conn)?;
    Ok(claimed == 1)
}

/// Put a newly claimed submit's order live now, before its note is in a
/// block (ADR 0008): store the root row, pass it through the liveness rule,
/// and only if it is kept, flag the lineage with `accept_block` and report
/// LIVE. An order cancelled before its submit leaves no row and takes the
/// confirmed path. The claim guaranteed that the note has no row yet.
pub(crate) fn go_live_at_accept_tx(
    conn: &mut PgConnection,
    accept_block: BlockNumber,
    maker_id: MakerId,
    lineage_id: &LineageId,
    note: &Note,
) -> DbResult<Option<BookOrder>> {
    let row = NewOrderRow::parsed(note, now_unix())?;
    let inserted: OrderRow = diesel::insert_into(orders::table)
        .values(&row)
        .returning(OrderRow::as_returning())
        .get_result(conn)?;
    let mut kept = super::postgres_db::live_book_update_tx(
        conn,
        BookUpdate {
            active: vec![inserted.into_book_order()?],
            ..BookUpdate::default()
        },
    )?;
    let Some(order) = kept.active.pop() else {
        // Cancelled before it was submitted: the confirmed path, as if this
        // row had never been written.
        diesel::delete(orders::table.find(&row.note_id)).execute(conn)?;
        return Ok(None);
    };
    diesel::update(maker_lineages::table.find(lineage_id))
        .set(maker_lineages::accept_block.eq(Some(i64::from(accept_block.as_u32()))))
        .execute(conn)?;
    let body = order_status(note, proto::OrderState::Live, "");
    gateway::append_event_tx(conn, maker_id, Some(lineage_id), &body)?;
    Ok(Some(order))
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

/// Statuses a maker sees as live: tradable, or waiting for its note to land
/// (ADR 0008).
fn shown_live() -> [&'static str; 2] {
    [OrderStatus::Active.as_str(), OrderStatus::Waiting.as_str()]
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
    /// The note's ID as stored, the key of its order row.
    pub note_id: Vec<u8>,
    /// For an order that went live at accept (ADR 0008): the watcher's
    /// latest block then.
    pub accept_block: Option<BlockNumber>,
    /// Seconds since the submit was accepted, by the database clock.
    pub age_secs: f64,
    /// A settlement holds the root row: that settlement decides it, not the
    /// cap.
    pub root_settling: bool,
}

impl PendingSubmission {
    /// The order went live at accept and its note is not seen yet.
    pub fn unconfirmed(&self) -> bool {
        self.accept_block.is_some()
    }
}

/// Every pending submission, with the note its submit stored, oldest first.
pub fn pending_submissions_tx(conn: &mut PgConnection) -> DbResult<Vec<PendingSubmission>> {
    maker_lineages::table
        .inner_join(
            maker_commands::table.on(maker_commands::maker_id
                .eq(maker_lineages::maker_id)
                .and(maker_commands::request_id.eq(maker_lineages::request_id))),
        )
        .left_join(orders::table.on(orders::note_id.eq(maker_lineages::note_id)))
        .filter(maker_lineages::state.eq(LineageState::Pending.as_str()))
        .order(sql::<diesel::sql_types::Timestamptz>(
            "maker_lineages.created_at ASC",
        ))
        .select((
            maker_lineages::lineage_id,
            maker_lineages::maker_id,
            maker_commands::payload,
            maker_lineages::accept_block,
            sql::<diesel::sql_types::Double>(
                "EXTRACT(EPOCH FROM now() - maker_lineages.created_at)::float8",
            ),
            orders::status.nullable(),
        ))
        .load::<(Vec<u8>, i64, Vec<u8>, Option<i64>, f64, Option<String>)>(conn)?
        .into_iter()
        .map(
            |(lineage_id, maker_id, payload, accept_block, age_secs, status)| {
                let accept_block = accept_block
                    .map(|block| u32::try_from(block).map(BlockNumber::from))
                    .transpose()?;
                let note = Note::read_from_bytes(&payload)?;
                Ok(PendingSubmission {
                    lineage_id,
                    maker_id,
                    note_id: note.id().to_bytes().to_vec(),
                    note,
                    accept_block,
                    age_secs,
                    root_settling: status.as_deref() == Some(OrderStatus::Settling.as_str()),
                })
            },
        )
        .collect()
}

/// A lineage's chain verification by the maker-note watcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
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
    if lineages.is_empty() {
        return Ok(HashSet::new());
    }
    let changed = diesel::update(
        maker_lineages::table
            .filter(maker_lineages::lineage_id.eq_any(lineages))
            .filter(maker_lineages::state.eq(LineageState::Pending.as_str())),
    )
    .set(maker_lineages::state.eq(state.as_str()))
    .returning(maker_lineages::lineage_id)
    .get_results::<Vec<u8>>(conn)?;
    Ok(changed.into_iter().collect())
}

/// The current status of each existing row among `note_ids`.
fn order_statuses(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
) -> DbResult<HashMap<Vec<u8>, String>> {
    if note_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(orders::table
        .filter(orders::note_id.eq_any(note_ids))
        .select((orders::note_id, orders::status))
        .load(conn)?
        .into_iter()
        .collect())
}

/// The rows among `note_ids` the maker sees as live: active or waiting, and
/// not covered by one of its cancels.
fn shown_live_ids(conn: &mut PgConnection, note_ids: &[Vec<u8>]) -> DbResult<HashSet<Vec<u8>>> {
    if note_ids.is_empty() {
        return Ok(HashSet::new());
    }
    Ok(eligible_orders::table
        .filter(eligible_orders::note_id.eq_any(note_ids))
        .filter(eligible_orders::status.eq_any(shown_live()))
        .select(eligible_orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect())
}

/// Set `status` on the rows among `note_ids` currently in one of `from`;
/// returns the ones changed.
fn move_orders(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
    from: &[OrderStatus],
    status: OrderStatus,
) -> DbResult<Vec<Vec<u8>>> {
    if note_ids.is_empty() {
        return Ok(Vec::new());
    }
    let from: Vec<_> = from.iter().map(|status| status.as_str()).collect();
    Ok(diesel::update(
        orders::table
            .filter(orders::note_id.eq_any(note_ids))
            .filter(orders::status.eq_any(from)),
    )
    .set(orders::status.eq(status.as_str()))
    .returning(orders::note_id)
    .get_results(conn)?)
}

fn order_ids(note_ids: &[Vec<u8>]) -> DbResult<Vec<OrderId>> {
    note_ids
        .iter()
        .map(|bytes| Ok(OrderId::read_from_bytes(bytes)?))
        .collect()
}

fn order_status(note: &Note, state: proto::OrderState, reason: &str) -> proto::EventBody {
    let depth = OrderKeys::from_note(note).map_or(0, |keys| keys.depth);
    status_body(note.id().to_bytes().to_vec(), depth, state, reason)
}

/// An `OrderStatus` event for the order version `note_id` at `depth`.
fn status_body(
    note_id: Vec<u8>,
    depth: u32,
    state: proto::OrderState,
    reason: &str,
) -> proto::EventBody {
    proto::EventBody {
        kind: Some(proto::event_body::Kind::OrderStatus(proto::OrderStatus {
            note_id,
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
///
/// Only rows this call stores, and roots that waited for their note (ADR
/// 0008), go to the matcher: any other existing row is already there. An
/// order that went live at accept was reported LIVE then.
pub fn activate_tx(
    conn: &mut PgConnection,
    submissions: &[PendingSubmission],
    arrival_unix: u64,
) -> DbResult<Activated> {
    if submissions.is_empty() {
        return Ok(Activated::default());
    }
    let rows = submissions
        .iter()
        .map(|submission| NewOrderRow::parsed(&submission.note, arrival_unix))
        .collect::<DbResult<Vec<_>>>()?;
    let mut sent: Vec<Vec<u8>> = diesel::insert_into(orders::table)
        .values(&rows)
        .on_conflict(orders::note_id)
        .do_nothing()
        .returning(orders::note_id)
        .get_results(conn)?;
    let lineages: Vec<_> = submissions.iter().map(|s| &s.lineage_id).collect();
    let activated = settle_lineages(conn, &lineages, LineageState::Activated)?;
    let ids: Vec<Vec<u8>> = rows.iter().map(|row| row.note_id.clone()).collect();
    let returned = move_orders(conn, &ids, &[OrderStatus::Waiting], OrderStatus::Active)?;
    let live: HashSet<Vec<u8>> = live_orders::table
        .filter(live_orders::note_id.eq_any(&ids))
        .select(live_orders::note_id)
        .load::<Vec<u8>>(conn)?
        .into_iter()
        .collect();
    // Back in the book: a root the maker cancelled while it waited is stored
    // stopped by `write_book` and never reaches it.
    let returned_makers = submissions
        .iter()
        .map(|s| (s, s.note_id.clone()))
        .filter(|(_, id)| returned.contains(id) && live.contains(id))
        .map(|(s, _)| s.maker_id)
        .collect();
    sent.extend(returned);
    for submission in submissions {
        let id = submission.note_id.clone();
        if activated.contains(&submission.lineage_id)
            && !submission.unconfirmed()
            && live.contains(&id)
        {
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
        .filter(orders::note_id.eq_any(&sent))
        .filter(orders::status.eq(OrderStatus::Active.as_str()))
        .select(OrderRow::as_select())
        .load::<OrderRow>(conn)?
        .into_iter()
        .map(OrderRow::into_book_order)
        .collect::<DbResult<_>>()?;
    Ok(Activated {
        update: BookUpdate {
            removed: Vec::new(),
            active,
            maker_updates: Vec::new(),
        },
        returned_makers,
        unconfirmed: submissions
            .iter()
            .filter(|s| s.unconfirmed() && activated.contains(&s.lineage_id))
            .map(|s| s.lineage_id.clone())
            .collect(),
    })
}

/// What [`activate_tx`] did.
#[derive(Debug, Default)]
pub struct Activated {
    pub update: BookUpdate,
    /// The maker of each root that came back from `waiting`.
    pub returned_makers: Vec<MakerId>,
    /// Lineages that went live at accept that this call activated.
    pub unconfirmed: HashSet<LineageId>,
}

/// What a watcher write did beyond its book update, for `/metrics`.
#[derive(Debug, Default)]
pub struct WatcherWrites {
    /// The activation, its update taken out for the book.
    pub activated: Activated,
    /// The maker of each order the cap cancelled.
    pub cap_cancels: Vec<MakerId>,
}

/// Close submissions whose note the watcher found spent before they were
/// confirmed, reporting each once, and return the orders to take out of the
/// book.
///
/// A public note claimed before maker intake became private-only can trade
/// before its claim is activated. When the note the watcher found spent was
/// consumed by our own settlement, the settlement events already told the
/// maker, so the claim is activated without an event; while that settlement
/// is still in flight, the claim waits for the next round. The caller holds the order rows (`prelock_watcher_tx`).
/// Orders that went live at accept are closed by [`close_unconfirmed_tx`].
pub fn reject_tx(conn: &mut PgConnection, spent: &[PendingSubmission]) -> DbResult<Vec<OrderId>> {
    let (unconfirmed, spent): (Vec<_>, Vec<_>) = spent
        .iter()
        .partition(|submission| submission.unconfirmed());
    let (removed, _) = close_unconfirmed_tx(conn, unconfirmed, Close::Spent)?;
    let ids: Vec<Vec<u8>> = spent.iter().map(|s| s.note_id.clone()).collect();
    let statuses = order_statuses(conn, &ids)?;
    let mut traded = Vec::new();
    let mut unusable = Vec::new();
    for submission in spent {
        let note_id = submission.note_id.clone();
        match statuses.get(&note_id).map(String::as_str) {
            // Our settlement is still in flight: decide once it resolves.
            Some(status) if status == OrderStatus::Settling.as_str() => {}
            Some(status) if status == OrderStatus::Executed.as_str() => {
                traded.push(&submission.lineage_id);
            }
            _ => unusable.push(submission),
        }
    }
    settle_lineages(conn, &traded, LineageState::Activated)?;

    let lineages: Vec<_> = unusable.iter().map(|s| &s.lineage_id).collect();
    let rejected = settle_lineages(conn, &lineages, LineageState::Rejected)?;
    // An order its maker cancelled before it went live is closed quietly: a
    // cancel produces no event, and a spend of a cancelled order's note
    // (usually the maker's own reclaim) produces none either.
    let cancelled = cancelled_lineages(conn, &lineages)?;
    for submission in unusable {
        if rejected.contains(&submission.lineage_id)
            && !cancelled.contains_key(&submission.lineage_id)
        {
            let body = order_status(
                &submission.note,
                proto::OrderState::Unavailable,
                "note spent before it became live",
            );
            gateway::append_event_tx(
                conn,
                submission.maker_id,
                Some(&submission.lineage_id),
                &body,
            )?;
        }
    }
    Ok(removed)
}

/// How [`close_unconfirmed_tx`] closes an order that went live at accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    /// Its note was spent elsewhere: the row is retired `onchain_nullified`
    /// and the maker hears UNAVAILABLE.
    Spent,
    /// The cap (ADR 0008 D6): its note never arrived. The row gets a stop row
    /// and is stopped, and the maker hears CANCELLED `note_not_received`.
    NoteNotReceived,
}

/// Close orders that went live at accept (ADR 0008), decided by the root's
/// current status. While a settlement holds the root, nothing changes: that
/// settlement decides it. Otherwise the lineage is closed whatever the root's
/// status, and an active or waiting root (also a stopped one, for
/// [`Close::Spent`]) is moved as `close` says. The maker hears of it only if
/// it saw the order live, so an order it cancelled itself is only tidied up.
///
/// A root our own settlement spent is not here: its confirmation activates
/// the lineage in the transaction that writes `executed`.
///
/// Returns the orders to take out of the book, and the maker of each order
/// it told.
pub fn close_unconfirmed_tx<'a>(
    conn: &mut PgConnection,
    roots: impl IntoIterator<Item = &'a PendingSubmission>,
    close: Close,
) -> DbResult<(Vec<OrderId>, Vec<MakerId>)> {
    let roots: Vec<&PendingSubmission> = roots.into_iter().collect();
    if roots.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let ids: Vec<Vec<u8>> = roots.iter().map(|s| s.note_id.clone()).collect();
    let statuses = order_statuses(conn, &ids)?;
    let shown = shown_live_ids(conn, &ids)?;
    let settling = |s: &PendingSubmission| {
        statuses.get(&s.note_id).map(String::as_str) == Some(OrderStatus::Settling.as_str())
    };
    let closing: Vec<&PendingSubmission> = roots.into_iter().filter(|s| !settling(s)).collect();
    let lineages: Vec<_> = closing.iter().map(|s| &s.lineage_id).collect();
    let rejected = settle_lineages(conn, &lineages, LineageState::Rejected)?;
    let closed: Vec<&PendingSubmission> = closing
        .into_iter()
        .filter(|s| rejected.contains(&s.lineage_id))
        .collect();
    let closed_ids: Vec<Vec<u8>> = closed.iter().map(|s| s.note_id.clone()).collect();
    let (from, to, state, reason): (&[OrderStatus], _, _, _) = match close {
        Close::Spent => (
            &[
                OrderStatus::Active,
                OrderStatus::Waiting,
                OrderStatus::Stopped,
            ],
            OrderStatus::OnchainNullified,
            proto::OrderState::Unavailable,
            "note spent outside this solver",
        ),
        Close::NoteNotReceived => (
            &[OrderStatus::Active, OrderStatus::Waiting],
            OrderStatus::Stopped,
            proto::OrderState::Cancelled,
            StopSource::Cap.reason(),
        ),
    };
    let moved = move_orders(conn, &closed_ids, from, to)?;
    let moved_now: HashSet<&Vec<u8>> = moved.iter().collect();
    // Only the orders it moved: a spent or stopped root's lineage is closed
    // silently.
    let closed: Vec<&PendingSubmission> = closed
        .into_iter()
        .filter(|s| moved_now.contains(&s.note_id))
        .collect();
    if close == Close::NoteNotReceived {
        let stops: Vec<Stop> = closed
            .iter()
            .map(|s| Stop {
                maker_id: s.maker_id,
                lineage_id: &s.lineage_id,
                source: StopSource::Cap,
                caused_by: None,
            })
            .collect();
        stop_lineages_tx(conn, &stops)?;
    }
    let mut told = Vec::new();
    for submission in closed {
        if shown.contains(&submission.note_id) {
            tell_tx(
                conn,
                submission.maker_id,
                &submission.lineage_id,
                &submission.note,
                state,
                reason,
            )?;
            told.push(submission.maker_id);
        }
    }
    Ok((order_ids(&moved)?, told))
}

/// Who stopped a lineage, stored in `maker_stops.source`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum StopSource {
    /// The maker's `CancelOrder`.
    Maker,
    /// The live-at-accept cap: the note never arrived (ADR 0008).
    Cap,
    /// The operator's `cancel-unconfirmed` (ADR 0008).
    EscapeHatch,
    /// A reservation that filled the maker's kept order (ADR 0005).
    StpReservation,
    /// A crossing submit of the same maker (ADR 0009).
    StpOnSubmit,
}

impl StopSource {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// The reason a maker hears for an order stopped this way: every event
    /// for a stop takes it from here.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Maker => MAKER_CANCEL,
            Self::Cap | Self::EscapeHatch => NOTE_NOT_RECEIVED,
            Self::StpReservation | Self::StpOnSubmit => SELF_TRADE_PREVENTION,
        }
    }
}

/// One lineage to stop.
#[derive(Clone, Copy, Debug)]
pub struct Stop<'a> {
    pub maker_id: MakerId,
    pub lineage_id: &'a LineageId,
    pub source: StopSource,
    /// The newer order of the maker that crossed it (ADR 0009).
    pub caused_by: Option<&'a LineageId>,
}

/// Write `stops`, one statement per [`INSERT_CHUNK_ROWS`], and return the
/// lineages this call stopped. A lineage stopped already keeps its first stop
/// row, and is not returned: whoever stopped it first already did whatever
/// follows. The lineage ID alone names the stop because every caller that
/// passes more than one takes each (maker, lineage) pair from a claim, and a
/// lineage is claimed once.
///
/// Every stop row is written here. The caller holds the makers' locks. Only
/// a self-trade stop at submit names the order that caused it: the table's
/// check refuses a cause with any other source, even on a row it skips.
pub fn stop_lineages_tx(conn: &mut PgConnection, stops: &[Stop]) -> DbResult<HashSet<LineageId>> {
    debug_assert!(
        stops
            .iter()
            .all(|stop| stop.caused_by.is_none() || stop.source == StopSource::StpOnSubmit),
        "only a self-trade stop at submit names a cause"
    );
    let mut stopped = HashSet::new();
    for chunk in stops.chunks(INSERT_CHUNK_ROWS) {
        let rows: Vec<_> = chunk
            .iter()
            .map(|stop| {
                (
                    maker_stops::maker_id.eq(stop.maker_id),
                    maker_stops::lineage_id.eq(stop.lineage_id),
                    maker_stops::source.eq(Some(stop.source.as_str())),
                    maker_stops::caused_by_lineage.eq(stop.caused_by),
                )
            })
            .collect();
        stopped.extend(
            diesel::insert_into(maker_stops::table)
                .values(rows)
                .on_conflict_do_nothing()
                .returning(maker_stops::lineage_id)
                .get_results::<Vec<u8>>(conn)?,
        );
    }
    Ok(stopped)
}

/// Tell a maker its order `note` is now `state`, for `reason`.
fn tell_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    lineage_id: &LineageId,
    note: &Note,
    state: proto::OrderState,
    reason: &str,
) -> DbResult<()> {
    let body = order_status(note, state, reason);
    gateway::append_event_tx(conn, maker_id, Some(lineage_id), &body)?;
    Ok(())
}

/// Activate the lineages of orders that went live at accept whose root our
/// own confirmed settlement spent, among `note_ids` (ADR 0008). No event: the
/// settlement report told the maker. Returns the maker of each it activated.
pub fn activate_spent_roots_tx(
    conn: &mut PgConnection,
    note_ids: &[Vec<u8>],
) -> DbResult<Vec<MakerId>> {
    if note_ids.is_empty() {
        return Ok(Vec::new());
    }
    let roots: Vec<(LineageId, MakerId)> = maker_lineages::table
        .filter(maker_lineages::note_id.eq_any(note_ids))
        .filter(maker_lineages::accept_block.is_not_null())
        .filter(maker_lineages::state.eq(LineageState::Pending.as_str()))
        .select((maker_lineages::lineage_id, maker_lineages::maker_id))
        .load(conn)?;
    let lineages: Vec<_> = roots.iter().map(|(lineage_id, _)| lineage_id).collect();
    let activated = settle_lineages(conn, &lineages, LineageState::Activated)?;
    Ok(roots
        .iter()
        .filter(|(lineage_id, _)| activated.contains(lineage_id))
        .map(|(_, maker_id)| *maker_id)
        .collect())
}

/// Orders that went live at accept whose note is still not seen and that can
/// still trade, wait or settle (ADR 0008): the `solver_maker_unconfirmed_orders`
/// gauge. The startup guard refuses to run without the watcher while any
/// exist. An order its maker cancelled is not counted: without the watcher it
/// can neither trade nor come back. The escape hatch still closes its lineage.
pub fn unconfirmed_count_tx(conn: &mut PgConnection) -> DbResult<i64> {
    Ok(pending_lineage_stats_tx(conn)?.unconfirmed)
}

/// What the escape hatch did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HatchReport {
    /// Unconfirmed lineages it closed.
    pub lineages_closed: usize,
    /// Orders it cancelled, per maker. A closed lineage whose root a
    /// settlement holds, or its maker had cancelled, adds none.
    pub cancelled: Vec<(MakerId, usize)>,
}

/// The escape hatch (ADR 0008), run with the solver stopped: close every
/// lineage that went live at accept and is still pending, and stop its root
/// and every `waiting` row, as the cap does. A maker that saw the order live
/// hears CANCELLED `note_not_received`. A root a settlement still holds stays
/// with that settlement: if it fails, the closed lineage keeps it out of the
/// book; if it commits, its remainder is reported CANCELLED, since the
/// lineage is stopped.
///
/// Its first statement claims a new owner epoch, so an intake session left
/// over from a stopped solver can no longer put an order live afterwards.
pub fn cancel_unconfirmed_tx(conn: &mut PgConnection) -> DbResult<HatchReport> {
    diesel::sql_query("UPDATE sync_state SET owner_epoch = owner_epoch + 1 WHERE id = 1")
        .execute(conn)?;
    let pending: Vec<(Vec<u8>, i64, Vec<u8>)> = maker_lineages::table
        .filter(maker_lineages::state.eq(LineageState::Pending.as_str()))
        .filter(maker_lineages::accept_block.is_not_null())
        .select((
            maker_lineages::lineage_id,
            maker_lineages::maker_id,
            maker_lineages::note_id,
        ))
        .load(conn)?;
    // A waiting row always has such a lineage; any other is closed too.
    /// A waiting row's note ID, and its lineage's maker and ID, if any.
    type Stray = (Vec<u8>, Option<i64>, Option<Vec<u8>>);
    let stray: Vec<Stray> = orders::table
        .left_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(orders::status.eq(OrderStatus::Waiting.as_str()))
        .select((
            orders::note_id,
            maker_lineages::maker_id.nullable(),
            maker_lineages::lineage_id.nullable(),
        ))
        .load(conn)?;
    let mut rows: Vec<Vec<u8>> = pending.iter().map(|(_, _, note)| note.clone()).collect();
    rows.extend(stray.iter().map(|(note, ..)| note.clone()));
    rows.sort();
    rows.dedup();
    orders::table
        .filter(orders::note_id.eq_any(&rows))
        .order(orders::note_id.asc())
        .for_update()
        .select(orders::note_id)
        .load::<Vec<u8>>(conn)?;
    let mut makers: Vec<MakerId> = pending.iter().map(|(_, maker, _)| *maker).collect();
    makers.extend(stray.iter().filter_map(|(_, maker, _)| *maker));
    lock_makers_ordered_tx(conn, &makers)?;

    let shown = shown_live_ids(conn, &rows)?;
    let lineages: Vec<&LineageId> = pending.iter().map(|(lineage, ..)| lineage).collect();
    let lineages_closed = settle_lineages(conn, &lineages, LineageState::Rejected)?.len();
    // By root note: a waiting row is usually also a pending lineage's root.
    let closing: std::collections::BTreeMap<Vec<u8>, (MakerId, Vec<u8>)> = pending
        .into_iter()
        .map(|(lineage, maker, note)| (note, (maker, lineage)))
        .chain(
            stray
                .into_iter()
                .filter_map(|(note, maker, lineage)| Some((note, (maker?, lineage?)))),
        )
        .collect();
    let stops: Vec<Stop> = closing
        .values()
        .map(|(maker_id, lineage_id)| Stop {
            maker_id: *maker_id,
            lineage_id,
            source: StopSource::EscapeHatch,
            caused_by: None,
        })
        .collect();
    stop_lineages_tx(conn, &stops)?;
    let stopped = move_orders(
        conn,
        &rows,
        &[OrderStatus::Active, OrderStatus::Waiting],
        OrderStatus::Stopped,
    )?;
    let mut per_maker: HashMap<MakerId, usize> = HashMap::new();
    // An order its maker had cancelled is moved too (its row may still say
    // `active` or `waiting`), but it was not the hatch's to cancel: only the
    // orders the maker saw live are counted and told.
    for (note_id, (maker_id, lineage_id)) in &closing {
        if !stopped.contains(note_id) || !shown.contains(note_id) {
            continue;
        }
        *per_maker.entry(*maker_id).or_default() += 1;
        let note = orders::table
            .find(note_id)
            .select(OrderRow::as_select())
            .first::<OrderRow>(conn)?
            .note()?;
        tell_tx(
            conn,
            *maker_id,
            lineage_id,
            &note,
            proto::OrderState::Cancelled,
            StopSource::EscapeHatch.reason(),
        )?;
    }
    let mut cancelled: Vec<_> = per_maker.into_iter().collect();
    cancelled.sort_unstable();
    Ok(HatchReport {
        lineages_closed,
        cancelled,
    })
}

/// The maker's reason when the cap cancels an order whose note never arrived.
pub const NOTE_NOT_RECEIVED: &str = "note_not_received";
/// The maker's reason when its own crossing order cancelled an order.
pub const SELF_TRADE_PREVENTION: &str = "self_trade_prevention";
/// The maker's reason for an order its own cancel stopped.
pub const MAKER_CANCEL: &str = "maker_cancel";

/// Report Unavailable for the maker orders among `note_ids` that the maker
/// sees as live (active, or waiting for their note: ADR 0008), which are
/// about to be retired because their notes were spent elsewhere. Called by
/// every path that retires consumed orders, before it updates them. Orders
/// already reserved by our own settlement are not live, so they are not
/// reported.
pub fn report_spent_tx(conn: &mut PgConnection, note_ids: &[Vec<u8>]) -> DbResult<()> {
    /// Note ID, maker, lineage and depth of a live maker order.
    type Spent = (Vec<u8>, i64, Option<Vec<u8>>, Option<i64>);
    let spent: Vec<Spent> = eligible_orders::table
        .filter(eligible_orders::note_id.eq_any(note_ids))
        .filter(eligible_orders::status.eq_any(shown_live()))
        .filter(eligible_orders::maker_id.is_not_null())
        .select((
            eligible_orders::note_id,
            eligible_orders::maker_id.assume_not_null(),
            eligible_orders::lineage_id,
            eligible_orders::depth,
        ))
        .load(conn)?;
    for (note_id, maker_id, lineage_id, depth) in spent {
        let body = status_body(
            note_id,
            u32::try_from(depth.unwrap_or(0))?,
            proto::OrderState::Unavailable,
            "note spent outside this solver",
        );
        gateway::append_event_tx(conn, maker_id, lineage_id.as_deref(), &body)?;
    }
    Ok(())
}

/// Stop the sit-outs of a reservation that fills a crossed maker's kept
/// order (ADR 0005), in the reservation's transaction: a stop row per lineage,
/// with its own source, and one `OrderStatus CANCELLED` per order it
/// stopped. Orders no longer live (cancelled by the maker or spent meanwhile)
/// are skipped. Only `postgres_db::reserve_settlement_tx` may call it: that
/// function locks every sit-out's maker first, and taking the lock here,
/// after other makers', could deadlock with the maker's own `CancelOrder`.
pub(super) fn cancel_self_trades_tx(conn: &mut PgConnection, note_ids: &[Vec<u8>]) -> DbResult<()> {
    if note_ids.is_empty() {
        return Ok(());
    }
    /// Note ID, maker, lineage and depth of a live maker order. The view
    /// takes the maker from the lineage's claim, so a maker means a lineage.
    type Live = (Vec<u8>, i64, Vec<u8>, Option<i64>);
    let live: Vec<Live> = live_orders::table
        .filter(live_orders::note_id.eq_any(note_ids))
        .filter(live_orders::maker_id.is_not_null())
        .select((
            live_orders::note_id,
            live_orders::maker_id.assume_not_null(),
            live_orders::lineage_id.assume_not_null(),
            live_orders::depth,
        ))
        .load(conn)?;
    let stops: Vec<Stop> = live
        .iter()
        .map(|(_, maker_id, lineage_id, _)| Stop {
            maker_id: *maker_id,
            lineage_id,
            source: StopSource::StpReservation,
            caused_by: None,
        })
        .collect();
    // The makers are locked and the view drops stopped lineages, so every
    // stop here is new; `remove` only keeps a lineage named twice to one
    // event.
    let mut stopped = stop_lineages_tx(conn, &stops)?;
    for (note_id, maker_id, lineage_id, depth) in live {
        if !stopped.remove(&lineage_id) {
            continue;
        }
        let body = status_body(
            note_id,
            u32::try_from(depth.unwrap_or(0))?,
            proto::OrderState::Cancelled,
            StopSource::StpReservation.reason(),
        );
        gateway::append_event_tx(conn, maker_id, Some(&lineage_id), &body)?;
    }
    Ok(())
}

/// Maker orders that may still be spent elsewhere: Active or Stopped rows
/// of a claimed lineage. Settling ones are resolved by the executor. A lineage
/// that went live at accept (ADR 0008) is left out while pending, since the
/// pending round decides its root by the row's status. Once it is rejected,
/// its root is left out (its note may never have landed), but a remainder is
/// watched: our committed settlement created that note, for example after
/// the escape hatch closed a lineage whose root a settlement held.
pub fn watched_maker_orders_tx(conn: &mut PgConnection) -> DbResult<Vec<Vec<u8>>> {
    Ok(orders::table
        .inner_join(
            maker_lineages::table.on(maker_lineages::lineage_id.nullable().eq(orders::lineage_id)),
        )
        .filter(
            orders::status.eq_any([OrderStatus::Active.as_str(), OrderStatus::Stopped.as_str()]),
        )
        .filter(
            maker_lineages::accept_block
                .is_null()
                .or(maker_lineages::state.eq(LineageState::Activated.as_str()))
                .or(orders::depth.gt(0_i64)),
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
pub(crate) fn input_fill(
    parent: &Note,
    fill_amount: u64,
    remainder: Option<&Note>,
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
        proto::ResultingStatus::SpentElsewhere
    } else {
        proto::ResultingStatus::Cancelled
    }
}

/// The lineages among `lineages` that their maker cancelled (not admitted by
/// `eligible_lineages`: a stop row or a cutoff covers them), each with the
/// reason a maker hears: the source of its stop row, else `maker_cancel` for
/// a cutoff. A stop wins over a cutoff. A stop written before stops recorded
/// their source has an empty reason; a lineage not cancelled is absent.
pub(super) fn cancelled_lineages(
    conn: &mut PgConnection,
    lineages: &[&LineageId],
) -> DbResult<HashMap<LineageId, &'static str>> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Binary)]
        lineage_id: Vec<u8>,
        #[diesel(sql_type = Nullable<Text>)]
        source: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        stopped: bool,
    }
    if lineages.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT l.lineage_id, s.source, s.lineage_id IS NOT NULL AS stopped
         FROM maker_lineages l
         LEFT JOIN maker_stops s ON s.maker_id = l.maker_id AND s.lineage_id = l.lineage_id
         WHERE l.lineage_id = ANY($1)
           AND NOT EXISTS (SELECT 1 FROM eligible_lineages e WHERE e.lineage_id = l.lineage_id)",
    )
    .bind::<diesel::sql_types::Array<Binary>, _>(lineages)
    .load(conn)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let reason = if row.stopped {
                row.source
                    .as_deref()
                    .and_then(|source| source.parse::<StopSource>().ok())
                    .map_or("", StopSource::reason)
            } else {
                MAKER_CANCEL
            };
            (row.lineage_id, reason)
        })
        .collect())
}

/// Immutable fill details computed once from the executor's original notes and
/// predicted remainders, or rebuilt from durable inputs during startup.
pub type SettlementFills = HashMap<Vec<u8>, proto::InputFill>;

/// Append one event per maker-owned input in the settlement's transaction.
/// Public inputs and legacy inputs without a recorded fill are excluded in
/// every phase. Current ownership and final eligibility always come from SQL.
pub fn report_settlement_tx(
    conn: &mut PgConnection,
    tx_id: &[u8],
    inputs: &[(&SettlementInputRow, &OrderRow)],
    phase: SettlementPhase,
    cached_fills: Option<&SettlementFills>,
) -> DbResult<()> {
    let parent_ids: Vec<_> = inputs
        .iter()
        .filter(|(input, _)| input.fill_amount.is_some())
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
        .map(|(note, maker, lineage)| (note, (maker, lineage)))
        .collect();
    if owners.is_empty() {
        return Ok(());
    }

    // Cancellation and event append share these locks. Acquire them in maker-ID
    // order before final liveness reads, using the same helper as intake.
    let maker_ids: Vec<_> = owners.values().map(|(maker, _)| *maker).collect();
    lock_makers_ordered_tx(conn, &maker_ids)?;

    let subject = |input: &SettlementInputRow| match phase {
        SettlementPhase::Committed { .. } => input
            .child_note_id
            .clone()
            .unwrap_or_else(|| input.parent_note_id.clone()),
        _ => input.parent_note_id.clone(),
    };
    let (live, statuses) = if matches!(phase, SettlementPhase::Pending) {
        (HashSet::new(), HashMap::new())
    } else {
        let subjects: Vec<_> = inputs
            .iter()
            .filter(|(input, _)| owners.contains_key(&input.parent_note_id))
            .map(|(input, _)| subject(input))
            .collect();
        // A waiting order (ADR 0008) is live to its maker.
        let live = eligible_orders::table
            .filter(eligible_orders::note_id.eq_any(&subjects))
            .filter(eligible_orders::status.eq_any(shown_live()))
            .select(eligible_orders::note_id)
            .load::<Vec<u8>>(conn)?
            .into_iter()
            .collect();
        let statuses = orders::table
            .filter(orders::note_id.eq_any(&subjects))
            .select((orders::note_id, orders::status))
            .load::<(Vec<u8>, String)>(conn)?
            .into_iter()
            .collect();
        (live, statuses)
    };
    let reasons = if matches!(phase, SettlementPhase::Pending) {
        HashMap::new()
    } else {
        let lineages: Vec<&LineageId> = owners.values().map(|(_, lineage)| lineage).collect();
        cancelled_lineages(conn, &lineages)?
    };
    // Stable per-maker ordering makes replay independent of SQL plan order.
    let mut ordered: Vec<_> = inputs
        .iter()
        .filter(|(input, _)| owners.contains_key(&input.parent_note_id))
        .collect();
    ordered.sort_by_key(|(input, _)| (owners[&input.parent_note_id].0, &input.parent_note_id));
    for (input, parent) in ordered {
        let (maker_id, lineage_id) = &owners[&input.parent_note_id];
        let Some(amount) = input.fill_amount else {
            continue;
        };
        // A void only returns the parent; it needs neither fill decoding nor a
        // predicted child that will never exist on chain.
        let fill = if matches!(phase, SettlementPhase::Voided) {
            None
        } else {
            Some(match cached_fills {
                Some(fills) => fills
                    .get(&input.parent_note_id)
                    .cloned()
                    .ok_or(DbError::Corrupt("missing cached settlement fill"))?,
                None => {
                    let remainder = input
                        .child_note_data
                        .as_deref()
                        .map(Note::read_from_bytes)
                        .transpose()?;
                    input_fill(&parent.note()?, u64::try_from(amount)?, remainder.as_ref())?
                }
            })
        };
        let kind = match phase {
            SettlementPhase::Pending => {
                proto::event_body::Kind::SettlementPending(proto::SettlementPending {
                    tx_id: tx_id.to_vec(),
                    fill,
                })
            }
            resolved => {
                let (committed, commit_block, consumer_account_id) = match resolved {
                    SettlementPhase::Committed { block, consumer } => {
                        (true, block.as_u32(), consumer.to_bytes())
                    }
                    _ => (false, 0, Vec::new()),
                };
                let status = if committed && input.child_note_id.is_none() {
                    proto::ResultingStatus::FullyFilled
                } else {
                    resulting_status(&subject(input), &live, &statuses)
                };
                // A CANCELLED result for a lineage neither stopped nor cut
                // off gets an empty reason. No known flow reaches it; this is
                // the only place an unknown reason comes from.
                let reason = if status == proto::ResultingStatus::Cancelled {
                    reasons.get(lineage_id).copied().unwrap_or_default()
                } else {
                    ""
                };
                proto::event_body::Kind::SettlementResolved(proto::SettlementResolved {
                    tx_id: tx_id.to_vec(),
                    committed,
                    commit_block,
                    consumer_account_id,
                    fill,
                    result: Some(proto::InputResult {
                        note_id: input.parent_note_id.clone(),
                        status: status.into(),
                        reason: reason.to_owned(),
                    }),
                })
            }
        };
        gateway::append_event_tx(
            conn,
            *maker_id,
            Some(lineage_id),
            &proto::EventBody { kind: Some(kind) },
        )?;
    }
    Ok(())
}

/// Submitted orders whose note the watcher has not resolved yet.
#[derive(QueryableByName, Debug, Clone, Copy, PartialEq)]
pub struct PendingLineageStats {
    #[diesel(sql_type = BigInt)]
    pub count: i64,
    /// Seconds since the oldest was submitted, by the database clock.
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    pub oldest_age_secs: Option<f64>,
    /// Of those, orders that went live at accept (ADR 0008) and can still
    /// trade, wait or settle: their maker has not cancelled them.
    #[diesel(sql_type = BigInt)]
    pub unconfirmed: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    pub oldest_unconfirmed_age_secs: Option<f64>,
    /// Of those, orders out of the book until their note lands.
    #[diesel(sql_type = BigInt)]
    pub waiting: i64,
}

/// Count pending lineages through the partial index on pending rows. The age
/// is computed by PostgreSQL, so it does not depend on the host clock. A
/// waiting order is always the root of a pending lineage that went live at
/// accept, so it is counted through the same rows. Roots are read through
/// `eligible_orders`, so an order its maker cancelled is not unconfirmed,
/// whatever its row's status still says.
pub fn pending_lineage_stats_tx(conn: &mut PgConnection) -> DbResult<PendingLineageStats> {
    Ok(diesel::sql_query(
        "SELECT count(*) AS count,
                EXTRACT(EPOCH FROM now() - min(m.created_at))::float8 AS oldest_age_secs,
                count(*) FILTER (WHERE o.status IN ('active', 'waiting', 'settling'))
                    AS unconfirmed,
                EXTRACT(EPOCH FROM now() - min(m.created_at)
                    FILTER (WHERE o.status IN ('active', 'waiting', 'settling')))::float8
                    AS oldest_unconfirmed_age_secs,
                count(o.note_id) FILTER (WHERE o.status = 'waiting') AS waiting
         FROM maker_lineages m
         LEFT JOIN eligible_orders o ON o.note_id = m.note_id AND m.accept_block IS NOT NULL
         WHERE m.state = 'pending'",
    )
    .get_result(conn)?)
}

/// What self-trade prevention at submit (ADR 0009) cannot see, counted at
/// startup: both must be 0 before the check is switched on.
#[derive(QueryableByName, Debug, Clone, Copy, PartialEq, Eq)]
pub struct StpReadiness {
    /// Open lineages claimed before their amounts were stored. They never
    /// cross, so the check cannot cancel them or be cancelled by them.
    #[diesel(sql_type = BigInt)]
    pub lineages_without_amounts: i64,
    /// Open maker order rows without their maker, which the check's read
    /// cannot find.
    #[diesel(sql_type = BigInt)]
    pub orders_without_maker: i64,
}

/// Count [`StpReadiness`]. A lineage is open while it is pending or one of
/// its rows is active, settling or waiting, and its maker has not stopped it.
pub fn stp_readiness_tx(conn: &mut PgConnection) -> DbResult<StpReadiness> {
    Ok(diesel::sql_query(
        "SELECT
            (SELECT count(*) FROM eligible_lineages l
             WHERE l.offered_amount IS NULL
               AND (l.state = 'pending' OR EXISTS (
                    SELECT 1 FROM orders o
                    WHERE o.lineage_id = l.lineage_id
                      AND o.status IN ('active', 'settling', 'waiting'))))
                AS lineages_without_amounts,
            (SELECT count(*) FROM orders o
             JOIN maker_lineages l ON l.lineage_id = o.lineage_id
             WHERE o.status IN ('active', 'settling', 'waiting') AND o.maker_id IS NULL)
                AS orders_without_maker",
    )
    .get_result(conn)?)
}

/// One maker-owned order admitted by the database's liveness rule.
#[derive(QueryableByName, Debug, Clone, PartialEq, Eq)]
pub struct ActiveMakerOrderRow {
    #[diesel(sql_type = Binary)]
    pub note_id: Vec<u8>,
    #[diesel(sql_type = Binary)]
    pub lineage_id: Vec<u8>,
    #[diesel(sql_type = BigInt)]
    pub root_seq: i64,
    #[diesel(sql_type = Text)]
    pub request_id: String,
    #[diesel(sql_type = BigInt)]
    pub depth: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<BigInt>)]
    pub expires_at_unix_ms: Option<i64>,
}

/// A page of the maker's active orders, ordered by exact note ID: those the
/// matcher may fill, and those waiting for their note (ADR 0008), which the
/// maker sees as live. `eligible_orders` applies the same cancellation rule as
/// matching and reservation. This read does not promise that the in-memory
/// matcher has processed the order yet.
/// An order whose expiry is at or before `cutoff_unix_ms` is left out. The
/// gateway passes now plus the settlement buffer, which is when the matcher
/// stops filling the order (`MakerIndex::expired`). `live_orders` itself keeps
/// it, because reservation must still accept work the matcher selected before
/// then.
pub fn active_maker_orders_page_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    after: &[u8],
    limit: i64,
    cutoff_unix_ms: i64,
) -> DbResult<Vec<ActiveMakerOrderRow>> {
    Ok(diesel::sql_query(
        // Two parts, so each status keeps its partial index.
        "SELECT * FROM (
             SELECT l.note_id, l.lineage_id, l.root_seq, m.request_id, l.depth, m.expires_at_unix_ms
             FROM live_orders l
             JOIN maker_lineages m ON m.lineage_id = l.lineage_id
             WHERE l.maker_id = $1 AND l.note_id > $2 AND l.depth IS NOT NULL
               AND (m.expires_at_unix_ms IS NULL OR m.expires_at_unix_ms > $4)
             UNION ALL
             SELECT l.note_id, l.lineage_id, l.root_seq, m.request_id, l.depth, m.expires_at_unix_ms
             FROM eligible_orders l
             JOIN maker_lineages m ON m.lineage_id = l.lineage_id
             WHERE l.status = 'waiting'
               AND l.maker_id = $1 AND l.note_id > $2 AND l.depth IS NOT NULL
               AND (m.expires_at_unix_ms IS NULL OR m.expires_at_unix_ms > $4)
         ) listed
         ORDER BY note_id
         LIMIT $3",
    )
    .bind::<BigInt, _>(maker_id)
    .bind::<Binary, _>(after)
    .bind::<BigInt, _>(limit)
    .bind::<BigInt, _>(cutoff_unix_ms)
    .load(conn)?)
}

/// The three V1 events (frozen; ADR 0003).
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum EventKind {
    OrderStatus,
    SettlementPending,
    SettlementResolved,
}

/// One event to append: its lineage, kind and encoded body.
type NewEvent<'a> = (Option<&'a [u8]>, EventKind, Vec<u8>);

/// Append `events` to `maker_id`'s feed in order, in the caller's business
/// transaction, and return the first one's sequence (`None` for no events).
/// One counter update for all of them, then one insert per
/// [`INSERT_CHUNK_ROWS`]. The counter row update serializes appenders, so a
/// maker's committed sequences are contiguous; a rolled-back transaction
/// returns its numbers with it.
fn append_events_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    events: &[NewEvent],
) -> DbResult<Option<u64>> {
    let count = i64::try_from(events.len())?;
    if count == 0 {
        return Ok(None);
    }
    let next: i64 = diesel::update(makers::table.find(maker_id))
        .set(makers::next_event_seq.eq(makers::next_event_seq + count))
        .returning(makers::next_event_seq)
        .get_result(conn)?;
    EventWake::mark_appended();
    let first = next - count;
    let numbered: Vec<_> = events.iter().zip(first..).collect();
    for chunk in numbered.chunks(INSERT_CHUNK_ROWS) {
        let rows: Vec<_> = chunk
            .iter()
            .map(|((lineage_id, kind, payload), event_seq)| {
                let kind: &'static str = (*kind).into();
                (
                    maker_events::maker_id.eq(maker_id),
                    maker_events::event_seq.eq(*event_seq),
                    maker_events::kind.eq(kind),
                    maker_events::lineage_id.eq(*lineage_id),
                    maker_events::payload.eq(payload),
                )
            })
            .collect();
        diesel::insert_into(maker_events::table)
            .values(rows)
            .execute(conn)?;
    }
    Ok(Some(u64::try_from(first)?))
}

/// Append one event to `maker_id`'s feed (see [`append_events_tx`]) and
/// return its sequence.
pub fn append_event_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    kind: EventKind,
    lineage_id: Option<&[u8]>,
    payload: &[u8],
) -> DbResult<u64> {
    append_events_tx(conn, maker_id, &[(lineage_id, kind, payload.to_vec())])?
        .ok_or(DbError::Corrupt("one event appended none"))
}

mod self_trade;
pub use self_trade::{
    self_trade_round_tx, DeadCause, SelfTradeRound, StpCancel, Submitted, MAX_NEWER_LINEAGES,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_db::{
        confirm_settlement_update_tx, finish_discarded_settlement_tx, insert_orders_batch_tx,
        live_book_update_tx, load_live_orders_tx, prepare_settlement_tx, reserve_settlement_tx,
        test_consumer, NodeAnswer,
    };
    use crate::db::postgres_migrations;
    use crate::db::postgres_models::{NewOrderRow, SettlementAttemptRow, SettlementInputRow};
    use crate::db::postgres_schema::live_orders;
    use crate::db::postgres_test::{fill, maker, prepare, revert_after, TestSchema};
    use crate::maker::MakerTag;

    /// The migration that adds the maker tables.
    const MAKER_GATEWAY: &str = "20261003000001";
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
    ) -> Result<(CommandReply, Option<MakerUpdate>)> {
        Ok(conn.transaction::<_, DbError, _>(|conn| {
            execute_command_confirmed_tx(conn, &header(maker_id, request_id, seq), &command)
        })?)
    }

    fn submit(note: &Note) -> MakerCommand {
        MakerCommand::submit(note.clone()).unwrap()
    }

    fn cancel_all(scope: CutoffScope) -> MakerCommand {
        MakerCommand::CancelAll { scope }
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

    /// Only pending lineages count, and the age is the oldest pending one's,
    /// by the database clock.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn pending_lineage_stats_count_pending_rows_and_age_the_oldest() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let empty = pending_lineage_stats_tx(conn)?;
        assert_eq!((empty.count, empty.oldest_age_secs), (0, None));
        assert_eq!((empty.unconfirmed, empty.waiting), (0, 0));

        let maker_id = maker(conn, "alpha")?;
        for (seq, serial) in [(1, 1), (2, 2), (3, 3)] {
            run(
                conn,
                maker_id,
                &format!("r{seq}"),
                seq,
                submit(&note(serial)),
            )?;
        }
        // An activated lineage is older still, but no longer pending.
        conn.batch_execute(&format!(
            "UPDATE maker_lineages SET state = 'activated', created_at = now() - interval '2 days'
               WHERE note_id = '\\x{}'::bytea;
             UPDATE maker_lineages SET created_at = now() - interval '1 hour'
               WHERE note_id = '\\x{}'::bytea;",
            hex::encode(note(1).id().to_bytes()),
            hex::encode(note(2).id().to_bytes()),
        ))?;
        let stats = pending_lineage_stats_tx(conn)?;
        assert_eq!(stats.count, 2);
        let age = stats.oldest_age_secs.unwrap();
        assert!((3600.0..3660.0).contains(&age), "oldest pending age {age}");
        assert_eq!(
            (stats.unconfirmed, stats.oldest_unconfirmed_age_secs),
            (0, None)
        );

        // The third went live at accept and now waits for its note.
        let third = hex::encode(note(3).id().to_bytes());
        conn.batch_execute(&format!(
            "UPDATE maker_lineages SET accept_block = 7
               WHERE note_id = '\\x{third}'::bytea;"
        ))?;
        let row = NewOrderRow::parsed(&note(3), 1)?;
        diesel::insert_into(orders::table)
            .values(&row)
            .execute(conn)?;
        conn.batch_execute(&format!(
            "UPDATE orders SET status = 'waiting' WHERE note_id = '\\x{third}'::bytea"
        ))?;
        let stats = pending_lineage_stats_tx(conn)?;
        assert_eq!((stats.count, stats.unconfirmed, stats.waiting), (2, 1, 1));
        assert!(stats.oldest_unconfirmed_age_secs.unwrap() < 60.0);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn retries_replay_and_reused_ids_conflict() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let maker_id = maker(conn, "alpha")?;
        let order = note(1);

        // A submit has nothing for the matcher: its order row comes later.
        let (reply, fact) = run(conn, maker_id, "r1", 1, submit(&order))?;
        assert_eq!(reply, CommandReply::Committed(CommandResult::Accepted));
        assert_eq!(fact, None);

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
            Some(MakerUpdate::CutoffRaised { cutoff: 100, .. })
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

    /// The reason a maker hears for each way a lineage can be stopped.
    #[test]
    fn every_stop_source_has_its_reason() {
        let reasons: Vec<_> = [
            StopSource::Maker,
            StopSource::Cap,
            StopSource::EscapeHatch,
            StopSource::StpReservation,
            StopSource::StpOnSubmit,
        ]
        .into_iter()
        .map(|source| (source.as_str(), source.reason()))
        .collect();
        assert_eq!(
            reasons,
            [
                ("maker", "maker_cancel"),
                ("cap", "note_not_received"),
                ("escape_hatch", "note_not_received"),
                ("stp_reservation", "self_trade_prevention"),
                ("stp_on_submit", "self_trade_prevention"),
            ]
        );
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
            .map(|(header, command)| Ok(execute_command_confirmed_tx(conn, header, command)?.0))
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

    /// Several sessions append one maker's events at once, some rolling back,
    /// while a reader follows the feed by cursor the way a stream does. The
    /// reader must never see a later event before an earlier one commits,
    /// or it would move its cursor past an event and lose it.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_cursor_reader_never_skips_events_appended_concurrently() -> Result<()> {
        const WRITERS: usize = 3;
        const APPENDS: usize = 150;
        let mut fixture = TestSchema::migrated()?;
        let alpha = maker(&mut fixture.conn, "alpha")?;
        let mut writers = (0..WRITERS)
            .map(|_| connect(&fixture))
            .collect::<Result<Vec<_>>>()?;
        let mut reader = connect(&fixture)?;
        let done = std::sync::atomic::AtomicUsize::new(0);

        let (committed, followed) = std::thread::scope(|scope| {
            let appending: Vec<_> = writers
                .iter_mut()
                .map(|conn| {
                    let done = &done;
                    scope.spawn(move || -> Result<usize> {
                        let mut committed = 0;
                        for i in 0..APPENDS {
                            let rollback = i % 7 == 3;
                            let outcome = conn.transaction::<_, DbError, _>(|conn| {
                                append_event_tx(conn, alpha, EventKind::OrderStatus, None, b"")?;
                                // Hold the event uncommitted for a moment.
                                std::thread::sleep(Duration::from_micros(rand::random_range(
                                    0..300,
                                )));
                                if rollback {
                                    return Err(DbError::Corrupt("roll back"));
                                }
                                Ok(())
                            });
                            committed += usize::from(outcome.is_ok());
                        }
                        done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(committed)
                    })
                })
                .collect();
            // Follow the feed: each read starts right after the cursor.
            let mut cursor = 0u64;
            let following = (|| -> Result<u64> {
                loop {
                    let finished = done.load(std::sync::atomic::Ordering::SeqCst) == WRITERS;
                    for event in read_events_tx(&mut reader, alpha, cursor, 1_000)? {
                        assert_eq!(event.seq, cursor + 1, "the feed skipped an event");
                        cursor = event.seq;
                    }
                    if finished {
                        return Ok(cursor);
                    }
                }
            })();
            let committed: usize = appending
                .into_iter()
                .map(|writer| writer.join().expect("writer panicked"))
                .sum::<Result<usize>>()?;
            Ok::<_, anyhow::Error>((committed, following?))
        })?;
        assert_eq!(
            followed, committed as u64,
            "every committed event was followed"
        );
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
            None,
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

    /// Races a cancel against a reservation of the same order on two sessions,
    /// many times. Whichever commits first, the two agree: the cancel reports
    /// the order as exposure exactly when the reservation won, and a
    /// reservation never commits after the cancel.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_cancel_and_a_reservation_racing_always_agree() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let mut reserver = connect(&fixture)?;
        let mut canceller = connect(&fixture)?;
        let mut won = [0u32; 2];
        for round in 0..60u8 {
            let maker_id = maker(&mut fixture.conn, &format!("m{round}"))?;
            let quote = note(u32::from(round) + 1);
            run(&mut fixture.conn, maker_id, "s1", 1, submit(&quote))?;
            ingest(&mut fixture.conn, &[&quote])?;

            let start = Barrier::new(2);
            let jitter = || {
                std::thread::sleep(Duration::from_micros(rand::random_range(0..1_500)));
            };
            let (reserved, reply) = std::thread::scope(|scope| {
                let reserving = scope.spawn(|| {
                    start.wait();
                    jitter();
                    reserver.transaction::<_, DbError, _>(|conn| reserve(conn, round, &quote))
                });
                start.wait();
                jitter();
                let reply = run(
                    &mut canceller,
                    maker_id,
                    "c2",
                    2,
                    cancel_all(CutoffScope::all()),
                );
                (reserving.join().expect("reserver panicked"), reply)
            });
            let (reply, _) = reply?;
            let CommandReply::Committed(CommandResult::Applied {
                cutoff: 2,
                settling,
            }) = reply
            else {
                panic!("round {round}: unexpected cancel reply {reply:?}");
            };
            assert_eq!(
                settling,
                u64::from(reserved.is_ok()),
                "round {round}: the cancel reports the reservation iff it won ({reserved:?})"
            );
            assert_eq!(
                live(&mut fixture.conn, &[&quote])?,
                [false],
                "round {round}"
            );
            won[usize::from(reserved.is_err())] += 1;
        }
        println!("reservation won {} rounds, cancel won {}", won[0], won[1]);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn reverse_arrival_cancel_round_does_not_deadlock_a_two_maker_reservation() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let alpha = maker(&mut fixture.conn, "alpha")?;
        let beta = maker(&mut fixture.conn, "beta")?;
        let first = note(51);
        let second = note(52);
        run(&mut fixture.conn, alpha, "s1", 1, submit(&first))?;
        run(&mut fixture.conn, beta, "s1", 1, submit(&second))?;
        ingest(&mut fixture.conn, &[&first, &second])?;

        let held = Arc::new(Barrier::new(2));
        let mut reserver = connect(&fixture)?;
        let reserving = {
            let held = held.clone();
            std::thread::spawn(move || -> DbResult<()> {
                reserver.transaction::<_, DbError, _>(|conn| {
                    // Hold A while the B-then-A cancel round begins.
                    lock_maker(conn, alpha)?;
                    held.wait();
                    std::thread::sleep(Duration::from_millis(100));
                    prepare_settlement_tx(
                        conn,
                        &SettlementAttemptRow {
                            tx_id: vec![51],
                            tx_result: vec![51],
                            status: "prepared".into(),
                        },
                        &[first, second]
                            .into_iter()
                            .map(|note| SettlementInputRow {
                                tx_id: vec![51],
                                parent_note_id: note.id().to_bytes().to_vec(),
                                child_note_id: None,
                                child_note_data: None,
                                fill_amount: None,
                            })
                            .collect::<Vec<_>>(),
                        None,
                    )
                })
            })
        };
        let mut canceller = connect(&fixture)?;
        held.wait();
        let commands = vec![
            (header(beta, "c2", 2), cancel_all(CutoffScope::all())),
            (header(alpha, "c2", 2), cancel_all(CutoffScope::all())),
        ];
        canceller.transaction::<_, DbError, _>(|conn| {
            prelock_command_makers_tx(conn, &commands)?;
            for (header, command) in &commands {
                execute_command_confirmed_tx(conn, header, command)?;
            }
            Ok(())
        })?;
        reserving.join().expect("reservation panicked")?;
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_claim_takes_no_order_row_lock_and_refuses_a_reserved_note_at_once() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let alpha = maker(&mut fixture.conn, "alpha")?;
        let quote = note(41);
        ingest(&mut fixture.conn, &[&quote])?;

        let held = Arc::new(Barrier::new(2));
        // Set just before the reservation commits.
        let committing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut reserver = connect(&fixture)?;
        let reserving = {
            let (held, quote, committing) = (held.clone(), quote.clone(), committing.clone());
            std::thread::spawn(move || {
                reserver.transaction::<_, DbError, _>(|conn| {
                    reserve(conn, 41, &quote)?;
                    held.wait();
                    std::thread::sleep(Duration::from_millis(300));
                    committing.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        // Connected before the reservation's 300 ms window starts.
        let mut claimant = connect(&fixture)?;
        held.wait();
        let (reply, _) = run(&mut claimant, alpha, "s1", 1, submit(&quote))?;
        assert!(
            !committing.load(std::sync::atomic::Ordering::SeqCst),
            "the claim must not wait for the reserved row"
        );
        reserving.join().expect("reserver panicked")?;
        assert_eq!(
            reply,
            CommandReply::Committed(CommandResult::AlreadyRegistered)
        );
        Ok(())
    }

    /// A submit is refused when any row of its lineage exists (here only a
    /// remainder), or a row with its note under no lineage.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_claim_is_refused_when_its_lineage_or_its_note_has_a_row() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let (root, remainder, unattributed) = (note(61), note(62), note(63));
        ingest(conn, &[&remainder, &unattributed])?;
        let lineage_id = OrderKeys::from_note(&root)?.lineage_id;
        diesel::update(
            orders::table.filter(orders::note_id.eq(remainder.id().to_bytes().to_vec())),
        )
        .set(orders::lineage_id.eq(Some(&lineage_id)))
        .execute(conn)?;
        diesel::update(
            orders::table.filter(orders::note_id.eq(unattributed.id().to_bytes().to_vec())),
        )
        .set(orders::lineage_id.eq(None::<Vec<u8>>))
        .execute(conn)?;
        for (seq, order) in [(1, &root), (2, &unattributed)] {
            let (reply, _) = run(conn, alpha, &format!("s{seq}"), seq, submit(order))?;
            assert_eq!(
                reply,
                CommandReply::Committed(CommandResult::AlreadyRegistered),
                "seq {seq}"
            );
        }
        let claims: i64 = maker_lineages::table.count().get_result(conn)?;
        assert_eq!(claims, 0);
        assert_eq!(
            live(conn, &[&remainder, &unattributed])?,
            [true, true],
            "both stay ordinary orders"
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
                    maker_updates: Vec::new(),
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
            expires_at_unix_ms: None,
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
    fn a_reservation_cancels_its_live_sit_outs_once_in_the_same_commit() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        // `kept` is filled; the maker already cancelled `stopped`; `public`
        // has no maker.
        let (kept, sit_out, stopped, public) = (note(1), note(2), note(3), note(4));
        run(conn, alpha, "s1", 1, submit(&kept))?;
        run(conn, alpha, "s2", 2, submit(&sit_out))?;
        run(conn, alpha, "s3", 3, submit(&stopped))?;
        ingest(conn, &[&kept, &sit_out, &stopped, &public])?;
        let lineage = |note: &Note| OrderKeys::from_note(note).unwrap().lineage_id;
        run(
            conn,
            alpha,
            "x4",
            4,
            MakerCommand::CancelOrder {
                lineage_id: lineage(&stopped),
            },
        )?;
        let ids = |notes: &[&Note]| -> Vec<Vec<u8>> {
            notes
                .iter()
                .map(|note| note.id().to_bytes().to_vec())
                .collect()
        };
        let cancels = ids(&[&sit_out, &stopped, &public]);
        let stops = |conn: &mut PgConnection| -> Result<i64> {
            Ok(maker_stops::table.count().get_result(conn)?)
        };
        let (input, _, _) = fill(5, &kept, 40);
        let attempt = |tx: u8| SettlementAttemptRow {
            tx_id: vec![tx; 32],
            tx_result: vec![tx],
            status: "prepared".into(),
        };

        // A reservation that rolls back cancels nothing.
        let rolled_back = conn.transaction::<(), DbError, _>(|conn| {
            reserve_settlement_tx(
                conn,
                &attempt(5),
                std::slice::from_ref(&input),
                None,
                &cancels,
            )?;
            Err(DbError::Corrupt("roll back"))
        });
        assert!(rolled_back.is_err());
        assert_eq!(live(conn, &[&kept, &sit_out])?, [true, true]);
        assert_eq!(stops(conn)?, 1);

        conn.transaction::<_, DbError, _>(|conn| {
            reserve_settlement_tx(
                conn,
                &attempt(5),
                std::slice::from_ref(&input),
                None,
                &cancels,
            )
        })?;
        assert_eq!(
            live(conn, &[&sit_out, &stopped, &public])?,
            [false, false, true],
            "only live maker orders are stopped"
        );
        assert_eq!(stops(conn)?, 2);
        let events = settlement_events(conn, alpha)?;
        let cancelled: Vec<_> = events
            .iter()
            .enumerate()
            .filter_map(|(at, kind)| match kind {
                proto::event_body::Kind::OrderStatus(status)
                    if status.state == i32::from(proto::OrderState::Cancelled) =>
                {
                    Some((at, status))
                }
                _ => None,
            })
            .collect();
        assert_eq!(cancelled.len(), 1, "one event, for the order it stopped");
        let (at, status) = cancelled[0];
        assert_eq!(status.note_id, sit_out.id().to_bytes().to_vec());
        assert_eq!(status.reason, "self_trade_prevention");
        assert!(
            matches!(
                events[at - 1],
                proto::event_body::Kind::SettlementPending(_)
            ),
            "the kept order's SettlementPending comes first"
        );

        // The maker's own cancel of it afterwards stops nothing twice.
        run(
            conn,
            alpha,
            "x6",
            6,
            MakerCommand::CancelOrder {
                lineage_id: lineage(&sit_out),
            },
        )?;
        assert_eq!(stops(conn)?, 2);
        assert_eq!(settlement_events(conn, alpha)?.len(), events.len());
        Ok(())
    }

    /// The reservation locks a sit-out's maker before it writes anything, even
    /// when that maker owns none of its inputs (the matcher never builds this
    /// today; the lock must not depend on it). A trigger holds the
    /// reservation between the sit-out's stop row and its event, where the
    /// maker's own `CancelOrder` used to take the maker lock and deadlock.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_reservation_locks_a_sit_outs_maker_before_cancelling_it() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let alpha = maker(&mut fixture.conn, "alpha")?;
        let (sit_out, public) = (note(1), note(2));
        run(&mut fixture.conn, alpha, "s1", 1, submit(&sit_out))?;
        ingest(&mut fixture.conn, &[&sit_out, &public])?;
        fixture.conn.batch_execute(
            "CREATE FUNCTION pause_after_stop() RETURNS trigger LANGUAGE plpgsql
                 AS $$ BEGIN PERFORM pg_sleep(0.5); RETURN NULL; END $$;
             CREATE TRIGGER pause_after_stop AFTER INSERT ON maker_stops
                 FOR EACH ROW EXECUTE FUNCTION pause_after_stop()",
        )?;
        let (input, _, _) = fill(5, &public, 40);
        let attempt = SettlementAttemptRow {
            tx_id: vec![5; 32],
            tx_result: vec![5],
            status: "prepared".into(),
        };
        let sit_outs = vec![sit_out.id().to_bytes().to_vec()];
        let mut reserver = connect(&fixture)?;
        let reserving = std::thread::spawn(move || {
            reserver.transaction::<_, DbError, _>(|conn| {
                reserve_settlement_tx(conn, &attempt, &[input], None, &sit_outs)
            })
        });
        // Wait until the reservation sleeps after its stop row.
        let started = Instant::now();
        let paused = diesel::dsl::sql::<diesel::sql_types::Bool>(
            "EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep')",
        );
        while !diesel::select(paused.clone()).get_result::<bool>(&mut fixture.conn)? {
            assert!(started.elapsed() < Duration::from_secs(5), "never paused");
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut canceller = connect(&fixture)?;
        let cancel = MakerCommand::CancelOrder {
            lineage_id: OrderKeys::from_note(&sit_out)?.lineage_id,
        };
        let cancelled = run(&mut canceller, alpha, "x2", 2, cancel);
        reserving.join().expect("reserver panicked")?;
        assert_eq!(
            cancelled?.0,
            CommandReply::Committed(CommandResult::Stopped { settling: 0 }),
            "the cancel waited for the reservation instead of deadlocking"
        );
        assert_eq!(
            maker_stops::table
                .count()
                .get_result::<i64>(&mut fixture.conn)?,
            1
        );
        let cancelled_events = settlement_events(&mut fixture.conn, alpha)?
            .into_iter()
            .filter(|kind| {
                matches!(kind, proto::event_body::Kind::OrderStatus(status)
                    if status.state == i32::from(proto::OrderState::Cancelled))
            })
            .count();
        assert_eq!(cancelled_events, 1, "the reservation stopped it first");
        Ok(())
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
        assert!(pending.fill.is_some(), "alpha has one input per event");
        assert_eq!(pending.fill.as_ref().unwrap().payback_amount, 40);

        let block = BlockNumber::from(42_u32);
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                block,
                test_consumer(),
                None,
            )
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(&events[1]);
        assert!(report.committed);
        assert_eq!(report.commit_block, 42);
        assert_eq!(report.fill, pending.fill, "the fill is what was intended");
        assert_eq!(
            report.result.as_ref().unwrap().status(),
            proto::ResultingStatus::Live,
            "the remainder is live"
        );
        let (rebuilt_payback, rebuilt_remainder) = rebuild(
            &quote,
            report.fill.as_ref().unwrap(),
            &report.consumer_account_id,
        );
        assert_eq!(rebuilt_payback.id(), payback.id());
        let remainder = remainder.unwrap();
        assert_eq!(rebuilt_remainder.unwrap().id(), remainder.id());
        // Beta hears about its own full fill only.
        let beta_events = settlement_events(conn, beta)?;
        assert!(resolved(&beta_events[1]).fill.is_some());
        assert_eq!(
            resolved(&beta_events[1]).result.as_ref().unwrap().status(),
            proto::ResultingStatus::FullyFilled
        );

        // Round 2 fills the remainder; the maker still rebuilds from its original note.
        let (row, payback, second) = fill(2, &remainder, 20);
        prepare(conn, 2, &[row])?;
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[2; 32],
                &HashSet::new(),
                block,
                test_consumer(),
                None,
            )
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(events.last().unwrap());
        assert_eq!(report.fill.as_ref().unwrap().depth, 2);
        let (rebuilt_payback, rebuilt_remainder) = rebuild(
            &quote,
            report.fill.as_ref().unwrap(),
            &report.consumer_account_id,
        );
        assert_eq!(rebuilt_payback.id(), payback.id());
        assert_eq!(rebuilt_remainder.unwrap().id(), second.unwrap().id());
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_late_fill_after_a_cancel_reports_its_remainder_cancelled() -> Result<()> {
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
            confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                None,
            )
        })?;
        let events = settlement_events(conn, alpha)?;
        let report = resolved(events.last().unwrap());
        assert!(report.committed);
        assert!(report.fill.is_some(), "the fill is reported");
        let result = report.result.as_ref().unwrap();
        assert_eq!(
            (result.status(), result.reason.as_str()),
            (proto::ResultingStatus::Cancelled, MAKER_CANCEL)
        );
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
            .map(|order| fill(1, order, 40).0)
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
            finish_discarded_settlement_tx(conn, &[1; 32], &consumed, &NodeAnswer::Unclear)
        })?;
        let events = settlement_events(conn, alpha)?;
        assert_eq!(events.len(), 6, "one pending and one resolution per order");
        let reports: Vec<_> = events[3..].iter().map(resolved).collect();
        assert!(reports
            .iter()
            .all(|report| !report.committed && report.fill.is_none()));
        let statuses: HashMap<Vec<u8>, (proto::ResultingStatus, &str)> = reports
            .iter()
            .map(|report| report.result.as_ref().unwrap())
            .map(|result| {
                let outcome = (result.status(), result.reason.as_str());
                (result.note_id.clone(), outcome)
            })
            .collect();
        assert_eq!(
            statuses[&returned.id().to_bytes().to_vec()],
            (proto::ResultingStatus::Live, "")
        );
        assert_eq!(
            statuses[&spent.id().to_bytes().to_vec()],
            (proto::ResultingStatus::SpentElsewhere, "")
        );
        assert_eq!(
            statuses[&stopped.id().to_bytes().to_vec()],
            (proto::ResultingStatus::Cancelled, MAKER_CANCEL)
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn per_order_events_are_atomic_replayable_and_recovered() -> Result<()> {
        use crate::db::postgres_db::load_unresolved_attempts_tx;
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        let quotes = [note(1), note(2)];
        for (index, quote) in quotes.iter().enumerate() {
            run(
                conn,
                alpha,
                &format!("s{index}"),
                index as u64 + 1,
                submit(quote),
            )?;
        }
        ingest(conn, &[&quotes[0], &quotes[1]])?;
        let rows: Vec<_> = quotes.iter().map(|quote| fill(1, quote, 40).0).collect();
        let fills: SettlementFills = quotes
            .iter()
            .map(|quote| {
                let (_, _, child) = fill(1, quote, 40);
                Ok((
                    quote.id().to_bytes().to_vec(),
                    input_fill(quote, 40, child.as_ref())?,
                ))
            })
            .collect::<DbResult<_>>()?;
        let attempt = SettlementAttemptRow {
            tx_id: vec![1; 32],
            tx_result: vec![1],
            status: "prepared".into(),
        };
        let rolled_back = conn.transaction::<(), DbError, _>(|conn| {
            prepare_settlement_tx(conn, &attempt, &rows, Some(&fills))?;
            Err(diesel::result::Error::RollbackTransaction.into())
        });
        assert!(rolled_back.is_err());
        assert!(read_events_tx(conn, alpha, 0, 100)?.is_empty());
        conn.transaction::<_, DbError, _>(|conn| {
            prepare_settlement_tx(conn, &attempt, &rows, Some(&fills))
        })?;
        let recovered = load_unresolved_attempts_tx(conn)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].fills, fills);
        // Failing after the first per-order event must roll back both the
        // earlier event and every settlement state change.
        let first = fills.keys().min().unwrap();
        let incomplete = HashMap::from([(first.clone(), fills[first].clone())]);
        let failed = conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                Some(&incomplete),
            )
        });
        assert!(matches!(
            failed,
            Err(DbError::Corrupt("missing cached settlement fill"))
        ));
        assert_eq!(read_events_tx(conn, alpha, 0, 100)?.len(), 2);
        assert_eq!(load_unresolved_attempts_tx(conn)?.len(), 1);
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                Some(&recovered[0].fills),
            )
        })?;
        let events = read_events_tx(conn, alpha, 0, 100)?;
        assert_eq!(events.len(), 4);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.seq, index as u64 + 1);
            assert!(event.lineage_id.is_some());
        }
        for (pending, resolved) in events[..2].iter().zip(&events[2..]) {
            assert_eq!(pending.lineage_id, resolved.lineage_id);
        }
        let replay = read_events_tx(conn, alpha, 2, 100)?;
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].payload, events[2].payload);
        // Repeated observations cannot append another resolution.
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                Some(&fills),
            )
        })?;
        assert_eq!(read_events_tx(conn, alpha, 0, 100)?.len(), 4);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn legacy_inputs_never_emit_settlement_events_even_if_claimed_later() -> Result<()> {
        for committed in [false, true] {
            let mut fixture = TestSchema::migrated()?;
            let conn = &mut fixture.conn;
            let alpha = maker(conn, "alpha")?;
            let quote = note(1);
            ingest(conn, &[&quote])?;
            let (mut row, ..) = fill(1, &quote, 40);
            row.fill_amount = None;
            prepare(conn, 1, &[row])?;
            run(conn, alpha, "s1", 1, submit(&quote))?;
            if committed {
                conn.transaction::<_, DbError, _>(|conn| {
                    confirm_settlement_update_tx(
                        conn,
                        &[1; 32],
                        &HashSet::new(),
                        BlockNumber::GENESIS,
                        test_consumer(),
                        None,
                    )
                })?;
            } else {
                conn.transaction::<_, DbError, _>(|conn| {
                    finish_discarded_settlement_tx(
                        conn,
                        &[1; 32],
                        &HashSet::new(),
                        &NodeAnswer::Unclear,
                    )
                })?;
            }
            assert!(settlement_events(conn, alpha)?.is_empty());
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn resolution_reads_cancellation_only_after_acquiring_the_maker_lock() -> Result<()> {
        #[derive(diesel::QueryableByName)]
        struct Pid {
            #[diesel(sql_type = diesel::sql_types::Integer)]
            pid: i32,
        }
        #[derive(diesel::QueryableByName)]
        struct Blocked {
            #[diesel(sql_type = diesel::sql_types::Bool)]
            blocked: bool,
        }
        for committed in [false, true] {
            let mut fixture = TestSchema::migrated()?;
            let alpha = maker(&mut fixture.conn, "alpha")?;
            let quote = note(1);
            run(&mut fixture.conn, alpha, "s1", 1, submit(&quote))?;
            ingest(&mut fixture.conn, &[&quote])?;
            prepare(&mut fixture.conn, 1, &[fill(1, &quote, 40).0])?;
            let mut resolver = connect(&fixture)?;
            resolver.batch_execute("SET statement_timeout = '10s'")?;
            let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                .get_result::<Pid>(&mut resolver)?
                .pid;
            fixture.conn.batch_execute("BEGIN")?;
            makers::table
                .find(alpha)
                .for_no_key_update()
                .select(makers::maker_id)
                .first::<i64>(&mut fixture.conn)?;
            let resolving = std::thread::spawn(move || -> DbResult<()> {
                resolver.transaction(|conn| {
                    if committed {
                        confirm_settlement_update_tx(
                            conn,
                            &[1; 32],
                            &HashSet::new(),
                            BlockNumber::GENESIS,
                            test_consumer(),
                            None,
                        )?;
                    } else {
                        finish_discarded_settlement_tx(
                            conn,
                            &[1; 32],
                            &HashSet::new(),
                            &NodeAnswer::Unclear,
                        )?;
                    }
                    Ok(())
                })
            });
            // Wait for a real database lock wait, not an assumed scheduling delay.
            let started = Instant::now();
            loop {
                let blocked =
                    diesel::sql_query("SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked")
                        .bind::<diesel::sql_types::Integer, _>(pid)
                        .get_result::<Blocked>(&mut fixture.conn)?
                        .blocked;
                if blocked {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "resolver never waited"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            execute_command_confirmed_tx(
                &mut fixture.conn,
                &header(alpha, "c2", 2),
                &cancel_all(CutoffScope::all()),
            )?;
            fixture.conn.batch_execute("COMMIT")?;
            resolving.join().expect("resolver panicked")?;
            let events = settlement_events(&mut fixture.conn, alpha)?;
            let report = resolved(events.last().unwrap());
            assert_eq!(
                report.result.as_ref().unwrap().status(),
                proto::ResultingStatus::Cancelled
            );
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn active_maker_orders_only_include_live_rows_and_page_by_note_id() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
        let (cut, active, stopped, settling, partial, other_maker, public) = (
            note(1),
            note(2),
            note(3),
            note(4),
            note(5),
            note(6),
            note(7),
        );
        for (seq, order) in [
            (1, &cut),
            (10, &active),
            (3, &stopped),
            (4, &settling),
            (6, &partial),
        ] {
            run(conn, alpha, &format!("s{seq}"), seq, submit(order))?;
        }
        run(conn, beta, "s1", 1, submit(&other_maker))?;
        ingest(
            conn,
            &[
                &cut,
                &active,
                &stopped,
                &settling,
                &partial,
                &other_maker,
                &public,
            ],
        )?;
        run(conn, alpha, "c2", 2, cancel_all(CutoffScope::all()))?;
        run(
            conn,
            alpha,
            "x11",
            11,
            MakerCommand::CancelOrder {
                lineage_id: OrderKeys::from_note(&stopped)?.lineage_id,
            },
        )?;
        prepare(conn, 1, &[fill(1, &settling, 40).0])?;
        let (row, _, remainder) = fill(2, &partial, 40);
        prepare(conn, 2, &[row])?;
        conn.transaction::<_, DbError, _>(|conn| {
            confirm_settlement_update_tx(
                conn,
                &[2; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                None,
            )
        })?;
        let remainder = remainder.unwrap();

        let mut orders = Vec::new();
        let mut after = Vec::new();
        loop {
            let page = active_maker_orders_page_tx(conn, alpha, &after, 1, 0)?;
            if page.is_empty() {
                break;
            }
            after = page[0].note_id.clone();
            orders.extend(page);
        }
        let ids: Vec<_> = orders.iter().map(|order| order.note_id.clone()).collect();
        let mut expected = vec![
            active.id().to_bytes().to_vec(),
            remainder.id().to_bytes().to_vec(),
        ];
        expected.sort();
        assert_eq!(ids, expected);
        assert_eq!(
            orders.iter().map(|order| order.depth).collect::<Vec<_>>(),
            ids.iter()
                .map(|id| if *id == remainder.id().to_bytes().to_vec() {
                    1
                } else {
                    0
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            active_maker_orders_page_tx(conn, beta, &[], 10, 0)?.len(),
            1
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
        // Every migration after the maker tables goes first; they refuse.
        revert_after(conn, MAKER_GATEWAY)?;
        assert!(postgres_migrations::revert_last(conn).is_err());

        let mut empty = TestSchema::migrated()?;
        revert_after(&mut empty.conn, "20260930000001")?;
        postgres_migrations::migrate(&mut empty.conn)?;
        Ok(())
    }
    /// A cancel can be the only command a maker ever sent. Reverting would
    /// drop that acknowledged cancel, so the rollback is refused for it too.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn reverting_after_a_cancel_alone_is_refused() -> Result<()> {
        let (x, y) = tokens();
        let cancels = [
            cancel_all(CutoffScope::direction(x, y)),
            MakerCommand::CancelOrder {
                lineage_id: OrderKeys::from_note(&note(1))?.lineage_id,
            },
        ];
        for cancel in cancels {
            let mut fixture = TestSchema::migrated()?;
            let conn = &mut fixture.conn;
            let alpha = maker(conn, "alpha")?;
            run(conn, alpha, "c1", 1, cancel)?;
            // Every migration after the maker tables goes first; they refuse.
            revert_after(conn, MAKER_GATEWAY)?;
            assert!(postgres_migrations::revert_last(conn).is_err());
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    fn expiry_survives_hydration_retries_and_remainders_without_reservation_check() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "expiry")?;
        let parent = note(1);
        let mut command = submit(&parent);
        if let MakerCommand::Submit {
            expires_at_unix_ms, ..
        } = &mut command
        {
            *expires_at_unix_ms = Some(1);
        }
        assert!(matches!(
            run(conn, alpha, "s1", 1, command.clone())?.0,
            CommandReply::Committed(CommandResult::Accepted)
        ));
        assert!(matches!(
            run(conn, alpha, "s1", 1, command.clone())?.0,
            CommandReply::Replayed(CommandResult::Accepted)
        ));
        if let MakerCommand::Submit {
            expires_at_unix_ms, ..
        } = &mut command
        {
            *expires_at_unix_ms = Some(2);
        }
        assert_eq!(
            run(conn, alpha, "s1", 1, command)?.0,
            CommandReply::Conflict
        );
        ingest(conn, &[&parent])?;
        let page = active_maker_orders_page_tx(conn, alpha, &[], 10, 0)?;
        assert_eq!(page[0].expires_at_unix_ms, Some(1));
        // From the cutoff on, the maker's listing leaves the order out.
        assert!(active_maker_orders_page_tx(conn, alpha, &[], 10, 1)?.is_empty());
        let hydrated = load_live_orders_tx(conn)?;
        assert_eq!(hydrated[0].maker.unwrap().expires_at_unix_ms, Some(1));
        let (input, _, child) = fill(1, &parent, 40);
        // Deliberately long-expired: reservation must still accept work the
        // matcher selected earlier. Expiry is not a live_orders SQL predicate.
        prepare(conn, 1, &[input])?;
        let update = conn.transaction::<_, DbError, _>(|conn| {
            let update = confirm_settlement_update_tx(
                conn,
                &[1; 32],
                &HashSet::new(),
                BlockNumber::GENESIS,
                test_consumer(),
                None,
            )?;
            live_book_update_tx(conn, update)
        })?;
        assert_eq!(update.active[0].id(), child.unwrap().id());
        assert_eq!(update.active[0].maker.unwrap().expires_at_unix_ms, Some(1));
        // Every migration after the expiry goes first; it refuses.
        revert_after(conn, "20261005000002")?;
        assert!(
            postgres_migrations::revert_last(conn).is_err(),
            "rollback cannot discard a recorded expiry"
        );
        Ok(())
    }
}

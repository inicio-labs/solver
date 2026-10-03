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
use diesel::sql_types::BigInt;
use miden_protocol::crypto::utils::Serializable;

use super::error::{DbError, DbResult};
use super::postgres_schema::{
    api_keys, maker_commands, maker_cutoffs, maker_events, maker_lineages, maker_stops, makers,
    orders,
};
use crate::maker::{
    CommandHeader, CommandReply, CommandResult, CutoffScope, LineageId, MakerCommand, MakerFact,
    MakerId, MakerTag,
};
use crate::types::OrderStatus;

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
    if lock_maker(conn, maker_id)?.is_none() {
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
    payload: &str,
) -> DbResult<u64> {
    let next: i64 = diesel::update(makers::table.find(maker_id))
        .set(makers::next_event_seq.eq(makers::next_event_seq + 1))
        .returning(makers::next_event_seq)
        .get_result(conn)?;
    let event_seq = next - 1;
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
    use crate::db::postgres_db::insert_orders_batch_tx;
    use crate::db::postgres_migrations;
    use crate::db::postgres_models::NewOrderRow;
    use crate::db::postgres_schema::live_orders;
    use crate::db::postgres_test::TestSchema;
    use crate::types::OrderKeys;
    use anyhow::Result;
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
                append_event_tx(conn, maker_id, EventKind::OrderStatus, None, "{}")
            })
        };
        assert_eq!(append(conn, alpha)?, 1);
        assert_eq!(append(conn, alpha)?, 2);
        assert_eq!(append(conn, beta)?, 1);
        // A rolled-back append gives its number back.
        let rolled_back = conn.transaction::<(), DbError, _>(|conn| {
            append_event_tx(conn, alpha, EventKind::SettlementPending, None, "{}")?;
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

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn reverting_after_maker_activity_is_refused() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let conn = &mut fixture.conn;
        let alpha = maker(conn, "alpha")?;
        run(conn, alpha, "s1", 1, submit(&note(1)))?;
        assert!(postgres_migrations::revert_last(conn).is_err());

        let mut empty = TestSchema::migrated()?;
        postgres_migrations::revert_last(&mut empty.conn)?;
        postgres_migrations::migrate(&mut empty.conn)?;
        Ok(())
    }
}

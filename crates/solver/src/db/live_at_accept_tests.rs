//! PostgreSQL tests for maker orders that go live at accept (ADR 0008):
//! accept, activation, confirmation, release by the node's answer (D3), the
//! spent and mismatched rules, the cap, and the escape hatch.

use std::collections::HashSet;

use anyhow::Result;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteType};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
};
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};
use prost::Message as _;

use super::maker_db::{self, AcceptFallback, AcceptGate, CommandOutcome, PendingSubmission};
use super::postgres_db::{
    self, confirm_settlement_with_tx, finish_discarded_settlement_tx, live_book_update_tx,
    prepare_settlement_tx, test_consumer, NodeAnswer,
};
use super::postgres_models::{NewOrderRow, SettlementAttemptRow, SettlementInputRow};
use super::postgres_schema::{maker_lineages, maker_stops, orders, sync_state};
use super::postgres_test::TestSchema;
use super::{postgres_migrations, DbError};
use crate::gateway::proto;
use crate::maker::{
    CommandHeader, CommandReply, CommandResult, CutoffScope, MakerCommand, MakerId,
};
use crate::types::{OrderId, OrderKeys};

fn note_of(serial: u32, note_type: NoteType) -> Note {
    let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
        .try_into()
        .unwrap();
    let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
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
        .note_type(note_type)
        .offered_asset(FungibleAsset::new(offered, 50).unwrap())
        .build()
        .unwrap()
        .into()
}

fn note(serial: u32) -> Note {
    note_of(serial, NoteType::Private)
}

fn id(note: &Note) -> Vec<u8> {
    note.id().to_bytes().to_vec()
}

fn maker(conn: &mut PgConnection, name: &str) -> Result<MakerId> {
    Ok(maker_db::create_maker_tx(conn, name)?.unwrap())
}

fn epoch(conn: &mut PgConnection) -> Result<i64> {
    Ok(sync_state::table
        .find(1_i16)
        .select(sync_state::owner_epoch)
        .first(conn)?)
}

/// Run `command` through intake's write with live at accept on, the watcher
/// fresh at `block` (or stale for `None`).
fn accept_with(
    conn: &mut PgConnection,
    maker_id: MakerId,
    seq: u64,
    command: &MakerCommand,
    block: Option<u32>,
) -> Result<CommandOutcome> {
    let mut gate = AcceptGate::new(epoch(conn)?, move || block.map(BlockNumber::from));
    let header = CommandHeader::new(maker_id, format!("r{seq}"), seq).unwrap();
    Ok(conn.transaction::<_, DbError, _>(|conn| {
        maker_db::execute_command_with_tx(conn, &header, command, Some(&mut gate))
    })?)
}

fn accept(
    conn: &mut PgConnection,
    maker_id: MakerId,
    seq: u64,
    note: &Note,
) -> Result<CommandOutcome> {
    accept_with(
        conn,
        maker_id,
        seq,
        &MakerCommand::submit(note.clone())?,
        Some(7),
    )
}

fn command(
    conn: &mut PgConnection,
    maker_id: MakerId,
    seq: u64,
    command: MakerCommand,
) -> Result<()> {
    let header = CommandHeader::new(maker_id, format!("r{seq}"), seq).unwrap();
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::execute_command_tx(conn, &header, &command)
    })?;
    Ok(())
}

fn cancel(conn: &mut PgConnection, maker_id: MakerId, seq: u64, note: &Note) -> Result<()> {
    let lineage_id = OrderKeys::from_note(note)?.lineage_id;
    command(
        conn,
        maker_id,
        seq,
        MakerCommand::CancelOrder { lineage_id },
    )
}

fn status(conn: &mut PgConnection, note: &Note) -> Result<Option<String>> {
    Ok(orders::table
        .find(id(note))
        .select(orders::status)
        .first(conn)
        .optional()?)
}

fn set_status(conn: &mut PgConnection, note: &Note, status: &str) -> Result<()> {
    diesel::update(orders::table.find(id(note)))
        .set(orders::status.eq(status))
        .execute(conn)?;
    Ok(())
}

/// The lineage's state, flag, accept block and whether it has `activated_at`.
fn lineage(conn: &mut PgConnection, note: &Note) -> Result<(String, bool, Option<i64>, bool)> {
    Ok(maker_lineages::table
        .filter(maker_lineages::note_id.eq(id(note)))
        .select((
            maker_lineages::state,
            maker_lineages::live_at_accept,
            maker_lineages::accept_block,
            maker_lineages::activated_at.is_not_null(),
        ))
        .first(conn)?)
}

fn pending(conn: &mut PgConnection, note: &Note) -> Result<PendingSubmission> {
    Ok(maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .find(|s| s.note.id() == note.id())
        .expect("a pending submission"))
}

/// Every event of `maker_id`, decoded.
fn events(conn: &mut PgConnection, maker_id: MakerId) -> Result<Vec<proto::event_body::Kind>> {
    Ok(maker_db::read_events_tx(conn, maker_id, 0, 1000)?
        .into_iter()
        .map(|event| {
            proto::EventBody::decode(event.payload.as_slice())
                .unwrap()
                .kind
                .unwrap()
        })
        .collect())
}

/// The OrderStatus events of `maker_id`: (note, state, reason).
fn statuses(
    conn: &mut PgConnection,
    maker_id: MakerId,
) -> Result<Vec<(Vec<u8>, proto::OrderState, String)>> {
    Ok(events(conn, maker_id)?
        .into_iter()
        .filter_map(|kind| match kind {
            proto::event_body::Kind::OrderStatus(status) => {
                let state = status.state();
                Some((status.note_id, state, status.reason))
            }
            _ => None,
        })
        .collect())
}

/// The status each SettlementResolved reported, per input note.
fn resolutions(
    conn: &mut PgConnection,
    maker_id: MakerId,
) -> Result<Vec<(Vec<u8>, proto::ResultingStatus)>> {
    Ok(events(conn, maker_id)?
        .into_iter()
        .filter_map(|kind| match kind {
            proto::event_body::Kind::SettlementResolved(resolved) => {
                let result = resolved.result.unwrap();
                let status = result.status();
                Some((result.note_id, status))
            }
            _ => None,
        })
        .collect())
}

fn live(state: proto::OrderState) -> bool {
    state == proto::OrderState::Live
}

/// Reserve `parents` in settlement `tx`, each filled by 40.
fn reserve(conn: &mut PgConnection, tx: u8, parents: &[&Note]) -> Result<()> {
    let inputs: Vec<SettlementInputRow> = parents
        .iter()
        .map(|parent| {
            let pswap = PswapNote::try_from(*parent).unwrap();
            let requested = pswap.storage().requested_faucet_id();
            let (_, remainder) = pswap
                .execute(
                    test_consumer(),
                    None,
                    Some(FungibleAsset::new(requested, 40).unwrap()),
                )
                .unwrap();
            let remainder = remainder.map(Note::from);
            SettlementInputRow {
                tx_id: vec![tx; 32],
                parent_note_id: id(parent),
                child_note_id: remainder.as_ref().map(id),
                child_note_data: remainder.as_ref().map(Serializable::to_bytes),
                fill_amount: Some(40),
            }
        })
        .collect();
    let attempt = SettlementAttemptRow {
        tx_id: vec![tx; 32],
        tx_result: vec![tx],
        status: "prepared".into(),
    };
    conn.transaction::<_, DbError, _>(|conn| prepare_settlement_tx(conn, &attempt, &inputs, None))?;
    Ok(())
}

fn release(
    conn: &mut PgConnection,
    tx: u8,
    answer: NodeAnswer,
) -> Result<postgres_db::DiscardedSettlement> {
    Ok(conn.transaction::<_, DbError, _>(|conn| {
        let mut released =
            finish_discarded_settlement_tx(conn, &[tx; 32], &HashSet::new(), &answer)?;
        released.update = live_book_update_tx(conn, released.update)?;
        Ok(released)
    })?)
}

fn order_id(note: &Note) -> OrderId {
    note.id()
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn an_accepted_private_order_goes_live_once_with_its_tag() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let order = note(1);

    let outcome = accept(conn, alpha, 3, &order)?;
    assert_eq!(
        outcome.reply,
        CommandReply::Committed(CommandResult::Accepted)
    );
    assert_eq!(outcome.fallback, None);
    let live_order = outcome.live.expect("live at accept");
    assert_eq!(live_order.id(), order.id());
    let tag = live_order.maker.expect("tagged");
    assert_eq!((tag.maker_id, tag.root_seq), (alpha, 3));
    assert_eq!(status(conn, &order)?.as_deref(), Some("active"));
    assert_eq!(
        lineage(conn, &order)?,
        ("pending".into(), true, Some(7), false)
    );
    assert_eq!(
        statuses(conn, alpha)?,
        [(id(&order), proto::OrderState::Live, String::new())]
    );
    // It trades now, is listed, and the pending round owns it, not the
    // generic spent watch.
    assert_eq!(postgres_db::load_live_orders_tx(conn)?.len(), 1);
    assert_eq!(
        maker_db::active_maker_orders_page_tx(conn, alpha, &[], 10, 0)?.len(),
        1
    );
    assert!(maker_db::watched_maker_orders_tx(conn)?.is_empty());
    let submission = pending(conn, &order)?;
    assert_eq!(submission.accept_block, Some(BlockNumber::from(7)));
    assert!(submission.age_secs < 60.0);

    // A lost reply's exact retry changes nothing.
    let retried = accept(conn, alpha, 3, &order)?;
    assert_eq!(
        retried.reply,
        CommandReply::Replayed(CommandResult::Accepted)
    );
    assert!(retried.live.is_none());
    assert_eq!(statuses(conn, alpha)?.len(), 1);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn other_submits_take_the_confirmed_path_without_a_trace() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let unflagged = |conn: &mut PgConnection, note: &Note| -> Result<()> {
        let (state, flagged, block, _) = lineage(conn, note)?;
        assert_eq!((state.as_str(), flagged, block), ("pending", false, None));
        Ok(())
    };

    let public = note_of(1, NoteType::Public);
    let outcome = accept(conn, alpha, 1, &public)?;
    assert_eq!(
        (outcome.live.is_none(), outcome.fallback),
        (true, Some(AcceptFallback::Public))
    );
    assert_eq!(status(conn, &public)?, None);
    unflagged(conn, &public)?;

    let stale = note(2);
    let outcome = accept_with(conn, alpha, 2, &MakerCommand::submit(stale.clone())?, None)?;
    assert_eq!(outcome.fallback, Some(AcceptFallback::WatcherStale));
    assert_eq!(status(conn, &stale)?, None);
    unflagged(conn, &stale)?;

    let stored = note(3);
    let row = NewOrderRow::parsed(&stored, 1)?;
    diesel::insert_into(orders::table)
        .values(&row)
        .execute(conn)?;
    let outcome = accept(conn, alpha, 3, &stored)?;
    assert_eq!(outcome.fallback, Some(AcceptFallback::Conflict));
    unflagged(conn, &stored)?;

    // Cancelled before its submit, by lineage or by a later cutoff: no row,
    // no flag, no LIVE.
    let stopped = note(4);
    cancel(conn, alpha, 4, &stopped)?;
    let outcome = accept(conn, alpha, 5, &stopped)?;
    assert_eq!((outcome.live.is_none(), outcome.fallback), (true, None));
    assert_eq!(status(conn, &stopped)?, None);
    unflagged(conn, &stopped)?;
    let below = note(6);
    command(
        conn,
        alpha,
        20,
        MakerCommand::CancelAll {
            scope: CutoffScope::all(),
        },
    )?;
    let outcome = accept(conn, alpha, 10, &below)?;
    assert!(outcome.live.is_none());
    assert_eq!(status(conn, &below)?, None);
    unflagged(conn, &below)?;
    assert!(statuses(conn, alpha)?.is_empty());

    // The confirmed path itself, with the switch off.
    let off = note(7);
    command(conn, alpha, 21, MakerCommand::submit(off.clone())?)?;
    assert_eq!(status(conn, &off)?, None);
    unflagged(conn, &off)?;
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_new_owner_stops_an_old_intake_from_going_live() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let url = fixture.url.clone();
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let ours = epoch(conn)?;

    // While an intake round holds the epoch, a new owner's claim waits.
    let mut intake = postgres_migrations::connect(&url)?;
    intake.batch_execute("BEGIN")?;
    let mut gate = AcceptGate::new(ours, || Some(BlockNumber::from(7)));
    let header = CommandHeader::new(alpha, "r1".into(), 1).unwrap();
    let first = note(1);
    let outcome = maker_db::execute_command_with_tx(
        &mut intake,
        &header,
        &MakerCommand::submit(first.clone())?,
        Some(&mut gate),
    )?;
    assert!(outcome.live.is_some());
    conn.batch_execute("SET lock_timeout = '200ms'")?;
    let claim = conn.batch_execute("UPDATE sync_state SET owner_epoch = owner_epoch + 1");
    assert!(claim.is_err(), "the claim must wait for the intake round");
    intake.batch_execute("COMMIT")?;
    conn.batch_execute("RESET lock_timeout")?;

    // Once a new owner claimed it, the old intake refuses and writes nothing.
    conn.batch_execute("UPDATE sync_state SET owner_epoch = owner_epoch + 1")?;
    let second = note(2);
    let mut gate = AcceptGate::new(ours, || Some(BlockNumber::from(7)));
    let header = CommandHeader::new(alpha, "r2".into(), 2).unwrap();
    let submit = MakerCommand::submit(second.clone())?;
    let refused = conn.transaction::<_, DbError, _>(|conn| {
        maker_db::execute_command_with_tx(conn, &header, &submit, Some(&mut gate))
    });
    assert!(
        matches!(refused, Err(DbError::IntakeEpochMoved { .. })),
        "{refused:?}"
    );
    assert_eq!(status(conn, &second)?, None);
    assert!(
        maker_lineages::table
            .filter(maker_lineages::note_id.eq(id(&second)))
            .count()
            .get_result::<i64>(conn)?
            == 0
    );
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn activation_returns_waiting_roots_and_sends_nothing_else() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (waiting, live_one, cancelled) = (note(1), note(2), note(3));
    for (seq, order) in [(1, &waiting), (2, &live_one), (3, &cancelled)] {
        accept(conn, alpha, seq, order)?;
    }
    set_status(conn, &waiting, "waiting")?;
    set_status(conn, &cancelled, "waiting")?;
    // The maker still sees a waiting order (D5).
    let listed = maker_db::active_maker_orders_page_tx(conn, alpha, &[], 10, 0)?;
    assert_eq!(listed.len(), 3);
    assert!(listed.iter().any(|row| row.note_id == id(&waiting)));
    cancel(conn, alpha, 4, &cancelled)?;
    let submissions = [
        pending(conn, &waiting)?,
        pending(conn, &live_one)?,
        pending(conn, &cancelled)?,
    ];
    let (update, returned) = conn.transaction::<_, DbError, _>(|conn| {
        let activated = maker_db::activate_tx(conn, &submissions, 1)?;
        Ok((
            live_book_update_tx(conn, activated.update)?,
            activated.returned_makers,
        ))
    })?;
    // Only the waiting root that is still live goes to the book; the one
    // already there is not sent again; the cancelled one is stored stopped.
    let sent: Vec<_> = update.active.iter().map(|order| order.id()).collect();
    assert_eq!(sent, [waiting.id()]);
    assert_eq!(returned, [alpha, alpha]);
    assert_eq!(status(conn, &waiting)?.as_deref(), Some("active"));
    assert_eq!(status(conn, &live_one)?.as_deref(), Some("active"));
    assert_eq!(status(conn, &cancelled)?.as_deref(), Some("stopped"));
    for order in [&waiting, &live_one, &cancelled] {
        let (state, _, _, activated_at) = lineage(conn, order)?;
        assert_eq!((state.as_str(), activated_at), ("activated", true));
    }
    // LIVE came at accept: none again.
    assert_eq!(statuses(conn, alpha)?.len(), 3);
    assert!(statuses(conn, alpha)?
        .iter()
        .all(|(_, state, _)| live(*state)));
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn our_confirmed_settlement_activates_a_root_it_erased() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let order = note(1);
    accept(conn, alpha, 1, &order)?;
    reserve(conn, 9, &[&order])?;
    let confirmed = conn.transaction::<_, DbError, _>(|conn| {
        confirm_settlement_with_tx(
            conn,
            &[9; 32],
            &HashSet::new(),
            BlockNumber::from(8),
            test_consumer(),
            None,
        )
    })?;
    assert_eq!(confirmed.activated_makers, [alpha]);
    assert_eq!(status(conn, &order)?.as_deref(), Some("executed"));
    let (state, _, _, activated_at) = lineage(conn, &order)?;
    assert_eq!((state.as_str(), activated_at), ("activated", true));
    Ok(())
}

/// The statuses of (a, b, user) after a release, the release, and what the
/// maker was told per input.
type Released = (
    [String; 3],
    postgres_db::DiscardedSettlement,
    Vec<(Vec<u8>, proto::ResultingStatus)>,
);

/// Two unconfirmed maker parents and one ordinary order in one settlement,
/// released with `answer`. Returns the statuses of (a, b, user) and the
/// release.
fn released_with(answer: impl FnOnce(&Note, &Note, &Note) -> NodeAnswer) -> Result<Released> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (a, b, user) = (note(1), note(2), note(3));
    accept(conn, alpha, 1, &a)?;
    accept(conn, alpha, 2, &b)?;
    let row = NewOrderRow::parsed(&user, 1)?;
    diesel::insert_into(orders::table)
        .values(&row)
        .execute(conn)?;
    reserve(conn, 9, &[&a, &b, &user])?;
    let released = release(conn, 9, answer(&a, &b, &user))?;
    let statuses = [&a, &b, &user].map(|order| status(conn, order).unwrap().unwrap());
    Ok((statuses, released, resolutions(conn, alpha)?))
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_release_follows_what_the_node_said() -> Result<()> {
    let named = |ids: &[&Note]| NodeAnswer::UnknownNotes(ids.iter().map(|n| order_id(n)).collect());

    // Naming one unconfirmed parent: only it waits.
    let (statuses, released, reported) = released_with(|a, _, _| named(&[a]))?;
    assert_eq!(statuses, ["waiting", "active", "active"]);
    assert_eq!((released.unconfirmed, released.waiting), (2, 1));
    assert_eq!(
        (released.named_makers.len(), released.waiting_makers.len()),
        (1, 1)
    );
    assert_eq!(released.update.active.len(), 2);
    assert_eq!(released.update.removed.len(), 1);
    // A waiting order is still live to its maker.
    assert!(reported
        .iter()
        .all(|(_, status)| *status == proto::ResultingStatus::Live));

    // Naming only another parent: the node knew both maker notes.
    let (statuses, released, _) = released_with(|_, _, user| named(&[user]))?;
    assert_eq!(statuses, ["active", "active", "active"]);
    assert!(released.named_makers.is_empty());
    // Naming nothing we recognise: every unconfirmed parent waits.
    let (statuses, released, _) = released_with(|_, _, _| named(&[]))?;
    assert_eq!(statuses, ["waiting", "waiting", "active"]);
    assert_eq!(released.waiting, 2);

    // Every note was known.
    let (statuses, _, _) = released_with(|_, _, _| NodeAnswer::AccountMismatch)?;
    assert_eq!(statuses, ["active", "active", "active"]);
    // Never sent.
    let (statuses, _, _) = released_with(|_, _, _| NodeAnswer::NotSent)?;
    assert_eq!(statuses, ["active", "active", "active"]);
    // Anything else, a drop, an expiry, a restart.
    let (statuses, released, _) = released_with(|_, _, _| NodeAnswer::Unclear)?;
    assert_eq!(statuses, ["waiting", "waiting", "active"]);
    assert_eq!(released.update.active.len(), 1);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_release_decides_by_the_rows_as_they_are_now() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (found, cancelled) = (note(1), note(2));
    accept(conn, alpha, 1, &found)?;
    accept(conn, alpha, 2, &cancelled)?;
    reserve(conn, 9, &[&found, &cancelled])?;
    // While settling: the watcher finds the first note, and the maker
    // cancels the second order.
    let submission = pending(conn, &found)?;
    conn.transaction::<_, DbError, _>(|conn| maker_db::activate_tx(conn, &[submission], 1))?;
    cancel(conn, alpha, 3, &cancelled)?;

    let answer = NodeAnswer::UnknownNotes([found.id(), cancelled.id()].into_iter().collect());
    let released = release(conn, 9, answer)?;
    // No longer unconfirmed: back to the book. Cancelled: stopped, reported
    // CANCELLED.
    assert_eq!(status(conn, &found)?.as_deref(), Some("active"));
    assert_eq!(status(conn, &cancelled)?.as_deref(), Some("stopped"));
    assert_eq!((released.unconfirmed, released.waiting), (1, 0));
    let reported: HashSet<_> = resolutions(conn, alpha)?.into_iter().collect();
    assert_eq!(
        reported,
        HashSet::from([
            (id(&found), proto::ResultingStatus::Live),
            (id(&cancelled), proto::ResultingStatus::Cancelled),
        ])
    );

    // The cap closes the cancelled order's lineage later, silently.
    let before = statuses(conn, alpha)?.len();
    let stale = pending(conn, &cancelled)?;
    let (removed, closed) =
        conn.transaction::<_, DbError, _>(|conn| maker_db::cap_unconfirmed_tx(conn, &[stale]))?;
    assert!(removed.is_empty());
    assert!(closed.is_empty(), "closed silently, nothing stopped");
    assert_eq!(lineage(conn, &cancelled)?.0, "rejected");
    assert_eq!(statuses(conn, alpha)?.len(), before);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_spent_unconfirmed_order_is_decided_by_its_status() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (waiting, ours, settling) = (note(1), note(2), note(3));
    for (seq, order) in [(1, &waiting), (2, &ours), (3, &settling)] {
        accept(conn, alpha, seq, order)?;
    }
    set_status(conn, &waiting, "waiting")?;
    set_status(conn, &ours, "executed")?;
    reserve(conn, 9, &[&settling])?;
    let spent = |s: PendingSubmission| {
        (
            s,
            proto::OrderState::Unavailable,
            "note spent before it became live",
        )
    };
    let rejections = [
        spent(pending(conn, &waiting)?),
        spent(pending(conn, &ours)?),
        spent(pending(conn, &settling)?),
    ];
    let removed =
        conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &rejections))?;
    assert_eq!(removed, [waiting.id()]);
    assert_eq!(
        status(conn, &waiting)?.as_deref(),
        Some("onchain_nullified")
    );
    assert_eq!(lineage(conn, &waiting)?.0, "rejected");
    // Our own settlement spent it: activated, never rejected.
    assert_eq!(lineage(conn, &ours)?.0, "activated");
    // Still in flight: decided once it resolves.
    assert_eq!(status(conn, &settling)?.as_deref(), Some("settling"));
    assert_eq!(lineage(conn, &settling)?.0, "pending");
    let reports: Vec<_> = statuses(conn, alpha)?
        .into_iter()
        .filter(|(_, state, _)| !live(*state))
        .collect();
    assert_eq!(
        reports,
        [(
            id(&waiting),
            proto::OrderState::Unavailable,
            "note spent outside this solver".to_owned()
        )]
    );
    // Once only.
    conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &rejections[..1]))?;
    assert_eq!(statuses(conn, alpha)?.len(), reports.len() + 3);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_spend_seen_elsewhere_first_is_reported_once() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let order = note(1);
    accept(conn, alpha, 1, &order)?;
    set_status(conn, &order, "waiting")?;
    // Another path (a held release) retires it first.
    conn.transaction::<_, DbError, _>(|conn| {
        postgres_db::mark_orders_onchain_nullified_tx(conn, &[id(&order)])
    })?;
    assert_eq!(status(conn, &order)?.as_deref(), Some("onchain_nullified"));
    let rejection = (
        pending(conn, &order)?,
        proto::OrderState::Unavailable,
        "spent",
    );
    conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &[rejection]))?;
    assert_eq!(lineage(conn, &order)?.0, "rejected");
    let unavailable = statuses(conn, alpha)?
        .into_iter()
        .filter(|(_, state, _)| *state == proto::OrderState::Unavailable)
        .count();
    assert_eq!(unavailable, 1);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_different_note_waits_for_its_settlement_then_is_rejected_once() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let order = note(1);
    accept(conn, alpha, 1, &order)?;
    reserve(conn, 9, &[&order])?;
    let differs = |s| {
        (
            s,
            proto::OrderState::Rejected,
            "committed note differs from the submitted note",
        )
    };
    let first = differs(pending(conn, &order)?);
    conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &[first]))?;
    assert_eq!(lineage(conn, &order)?.0, "pending");
    release(conn, 9, NodeAnswer::Unclear)?;
    assert_eq!(status(conn, &order)?.as_deref(), Some("waiting"));
    let second = differs(pending(conn, &order)?);
    let removed = conn.transaction::<_, DbError, _>(|conn| {
        maker_db::reject_tx(conn, std::slice::from_ref(&second))
    })?;
    assert_eq!(removed, [order.id()]);
    assert_eq!(status(conn, &order)?.as_deref(), Some("stopped"));
    assert_eq!(lineage(conn, &order)?.0, "rejected");
    let rejected = |conn: &mut PgConnection| -> Result<usize> {
        Ok(statuses(conn, alpha)?
            .into_iter()
            .filter(|(_, state, _)| *state == proto::OrderState::Rejected)
            .count())
    };
    assert_eq!(rejected(conn)?, 1);
    conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &[second]))?;
    assert_eq!(rejected(conn)?, 1);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_cap_closes_each_lineage_by_its_roots_status() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (active, waiting, settling, cancelled, spent) =
        (note(1), note(2), note(3), note(4), note(5));
    for (seq, order) in [
        (1, &active),
        (2, &waiting),
        (3, &settling),
        (4, &cancelled),
        (5, &spent),
    ] {
        accept(conn, alpha, seq, order)?;
    }
    set_status(conn, &waiting, "waiting")?;
    reserve(conn, 9, &[&settling])?;
    // The maker cancelled it; the book write stored it stopped.
    cancel(conn, alpha, 6, &cancelled)?;
    set_status(conn, &cancelled, "stopped")?;
    set_status(conn, &spent, "onchain_nullified")?;
    let capped: Vec<_> = [&active, &waiting, &settling, &cancelled, &spent]
        .into_iter()
        .map(|order| pending(conn, order))
        .collect::<Result<_>>()?;
    let before = statuses(conn, alpha)?.len();

    let (removed, closed) =
        conn.transaction::<_, DbError, _>(|conn| maker_db::cap_unconfirmed_tx(conn, &capped))?;
    let removed: HashSet<_> = removed.into_iter().collect();
    assert_eq!(removed, HashSet::from([active.id(), waiting.id()]));
    assert_eq!(closed, [alpha, alpha], "the two orders it stopped");
    for order in [&active, &waiting] {
        assert_eq!(status(conn, order)?.as_deref(), Some("stopped"));
    }
    for order in [&active, &waiting, &cancelled, &spent] {
        assert_eq!(lineage(conn, order)?.0, "rejected");
    }
    assert_eq!(lineage(conn, &settling)?.0, "pending");
    assert_eq!(status(conn, &spent)?.as_deref(), Some("onchain_nullified"));
    let stops: i64 = maker_stops::table.count().get_result(conn)?;
    assert_eq!(
        stops, 3,
        "the maker's own cancel plus one per order stopped"
    );
    // The maker hears CANCELLED only for the orders it saw live.
    let cancels: Vec<_> = statuses(conn, alpha)?[before..].to_vec();
    assert_eq!(
        cancels,
        [&active, &waiting].map(|order| (
            id(order),
            proto::OrderState::Cancelled,
            maker_db::NOTE_NOT_RECEIVED.to_owned()
        ))
    );
    // Closed lineages are no longer watched, and a second cap does nothing.
    assert!(maker_db::watched_maker_orders_tx(conn)?.is_empty());
    let again =
        conn.transaction::<_, DbError, _>(|conn| maker_db::cap_unconfirmed_tx(conn, &capped))?;
    assert!(again.0.is_empty() && again.1.is_empty());
    assert_eq!(statuses(conn, alpha)?.len(), before + 2);
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_hatch_cancels_unconfirmed_orders_and_unblocks_rollback() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (active, waiting, confirmed) = (note(1), note(2), note(3));
    for (seq, order) in [(1, &active), (2, &waiting), (3, &confirmed)] {
        accept(conn, alpha, seq, order)?;
    }
    set_status(conn, &waiting, "waiting")?;
    let submission = pending(conn, &confirmed)?;
    conn.transaction::<_, DbError, _>(|conn| maker_db::activate_tx(conn, &[submission], 1))?;
    assert_eq!(maker_db::unconfirmed_count_tx(conn)?, 2);
    let stats = maker_db::pending_lineage_stats_tx(conn)?;
    assert_eq!((stats.unconfirmed, stats.waiting), (2, 1));

    // The rollback refuses while they exist.
    let refused = postgres_migrations::revert_last(conn);
    assert!(refused.is_err(), "{refused:?}");

    let cancelled = postgres_migrations::cancel_unconfirmed(conn)?;
    assert_eq!(cancelled, [(alpha, 2)]);
    assert_eq!(maker_db::unconfirmed_count_tx(conn)?, 0);
    for order in [&active, &waiting] {
        assert_eq!(status(conn, order)?.as_deref(), Some("stopped"));
    }
    assert_eq!(status(conn, &confirmed)?.as_deref(), Some("active"));
    let cancels = statuses(conn, alpha)?
        .into_iter()
        .filter(|(_, state, reason)| {
            *state == proto::OrderState::Cancelled && reason == maker_db::NOTE_NOT_RECEIVED
        })
        .count();
    assert_eq!(cancels, 2);
    assert!(postgres_migrations::cancel_unconfirmed(conn)?.is_empty());
    postgres_migrations::revert_last(conn)?;
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn an_unconfirmed_order_still_in_the_book_is_retired_when_spent_or_different() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (reclaimed, differs, cancelled) = (note(1), note(2), note(3));
    for (seq, order) in [(1, &reclaimed), (2, &differs), (3, &cancelled)] {
        accept(conn, alpha, seq, order)?;
    }
    // The maker cancels the third, then its note turns out different.
    cancel(conn, alpha, 4, &cancelled)?;
    let rejections = [
        (
            pending(conn, &reclaimed)?,
            proto::OrderState::Unavailable,
            "note spent before it became live",
        ),
        (
            pending(conn, &differs)?,
            proto::OrderState::Rejected,
            "committed note differs from the submitted note",
        ),
        (
            pending(conn, &cancelled)?,
            proto::OrderState::Rejected,
            "committed note differs from the submitted note",
        ),
    ];
    let removed: HashSet<_> = conn
        .transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &rejections))?
        .into_iter()
        .collect();
    assert_eq!(
        removed,
        HashSet::from([reclaimed.id(), differs.id(), cancelled.id()])
    );
    assert_eq!(
        status(conn, &reclaimed)?.as_deref(),
        Some("onchain_nullified")
    );
    assert_eq!(status(conn, &differs)?.as_deref(), Some("stopped"));
    for order in [&reclaimed, &differs, &cancelled] {
        assert_eq!(lineage(conn, order)?.0, "rejected");
    }
    // One event each for the two the maker saw live; none for the one it
    // cancelled.
    let terminal: Vec<_> = statuses(conn, alpha)?
        .into_iter()
        .filter(|(_, state, _)| !live(*state))
        .map(|(note, state, _)| (note, state))
        .collect();
    assert_eq!(
        terminal,
        [
            (id(&reclaimed), proto::OrderState::Unavailable),
            (id(&differs), proto::OrderState::Rejected),
        ]
    );
    Ok(())
}

#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_hatch_tells_nothing_about_an_order_the_maker_cancelled() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let order = note(1);
    accept(conn, alpha, 1, &order)?;
    cancel(conn, alpha, 2, &order)?;
    set_status(conn, &order, "waiting")?;
    let before = epoch(conn)?;
    assert_eq!(postgres_migrations::cancel_unconfirmed(conn)?, [(alpha, 1)]);
    assert_eq!(status(conn, &order)?.as_deref(), Some("stopped"));
    assert_eq!(statuses(conn, alpha)?.len(), 1, "LIVE only");
    // An old intake can't put an order live after the hatch.
    assert_eq!(epoch(conn)?, before + 1);
    Ok(())
}

/// A round with only a public submit still reports why it took the
/// confirmed path, without taking the owner-epoch lock.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_public_only_round_reports_its_fallback() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    // An epoch the gate would refuse: the round must never check it.
    let gate = AcceptGate::new(epoch(conn)? + 100, || Some(BlockNumber::from(7)));
    let commands = [(
        CommandHeader::new(alpha, "r1".into(), 1).unwrap(),
        MakerCommand::submit(note_of(1, NoteType::Public))?,
    )];
    let outcomes = conn.transaction::<_, DbError, _>(|conn| {
        crate::gateway::intake::intake_round_tx(conn, &commands, Some(gate))
    })?;
    assert_eq!(outcomes[0].1.fallback, Some(AcceptFallback::Public));
    assert!(outcomes[0].1.live.is_none());
    Ok(())
}

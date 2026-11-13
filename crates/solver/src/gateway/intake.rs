//! The maker intake writer (ADR 0003). One task owns the intake session and
//! writes waiting commands in group-commit rounds: every waiting cancel
//! first, then submits up to the round limit. Commands that arrive while a
//! round commits form the next round, so batching costs no added wait.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::error::IntakeError;
use diesel::pg::PgConnection;
use miden_protocol::note::NoteType;

use super::metrics as gateway_metrics;
use crate::db::maker_db::{execute_command_with_tx, AcceptGate, CommandOutcome};
use crate::db::{DbResult, IntakeSession};
use crate::maker::{CommandHeader, CommandReply, EventWake, MakerCommand, MakerId};

type Reply = oneshot::Sender<Result<CommandReply, IntakeError>>;

struct Request {
    header: CommandHeader,
    command: MakerCommand,
    reply: Reply,
}

/// Handlers' side of the intake: cancels have their own queue, so a flood of
/// submits can neither fill it nor delay them.
#[derive(Clone)]
pub struct IntakeSender {
    cancels_tx: mpsc::Sender<Request>,
    submits_tx: mpsc::Sender<Request>,
}

pub struct IntakeReceiver {
    cancels_rx: mpsc::Receiver<Request>,
    submits_rx: mpsc::Receiver<Request>,
}

impl IntakeSender {
    /// Create the service-wide command queues and their single writer side.
    pub fn new(cancel_capacity: usize, submit_capacity: usize) -> (Self, IntakeReceiver) {
        let (cancels_tx, cancels_rx) = mpsc::channel(cancel_capacity);
        let (submits_tx, submits_rx) = mpsc::channel(submit_capacity);
        (
            Self {
                cancels_tx,
                submits_tx,
            },
            IntakeReceiver {
                cancels_rx,
                submits_rx,
            },
        )
    }

    /// Queue a validated command and wait for its durable reply. A full queue
    /// fails at once instead of hanging the caller.
    pub async fn execute(
        &self,
        header: CommandHeader,
        command: MakerCommand,
    ) -> Result<CommandReply, IntakeError> {
        let queue = match command {
            MakerCommand::Submit { .. } => &self.submits_tx,
            MakerCommand::CancelAll { .. } | MakerCommand::CancelOrder { .. } => &self.cancels_tx,
        };
        let (reply, replied) = oneshot::channel();
        queue
            .try_send(Request {
                header,
                command,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => IntakeError::Busy,
                mpsc::error::TrySendError::Closed(_) => IntakeError::Stopped,
            })?;
        replied.await.map_err(|_| IntakeError::Stopped)?
    }
}

/// Write commands until `cancel` fires or every handler is gone. After each
/// commit the committed maker updates go to the matcher first, so a maker that sees
/// Applied knows its cutoff is already queued there, then every handler gets
/// its reply. A failed or uncertain round replies `OutcomeUnknown` to all of
/// its commands; a retry with the same request ID resolves the durable result.
///
/// With live at accept on (ADR 0008), orders that went live in the round go
/// to the matcher in the same update, and the event streams are woken for
/// their LIVE.
pub async fn run_intake(
    mut session: IntakeSession,
    mut intake: IntakeReceiver,
    book_tx: mpsc::Sender<crate::types::BookUpdate>,
    max_submits: usize,
    cancel: CancellationToken,
) {
    loop {
        let first = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            Some(request) = intake.cancels_rx.recv() => request,
            Some(request) = intake.submits_rx.recv() => request,
            else => return,
        };
        let mut round = Vec::new();
        let mut first_submit = None;
        match first.command {
            MakerCommand::Submit { .. } => first_submit = Some(first),
            _ => round.push(first),
        }
        while let Ok(request) = intake.cancels_rx.try_recv() {
            round.push(request);
        }
        let mut submit_count = 0;
        if let Some(request) = first_submit {
            round.push(request);
            submit_count = 1;
        }
        while submit_count < max_submits {
            match intake.submits_rx.try_recv() {
                Ok(request) => {
                    round.push(request);
                    submit_count += 1;
                }
                Err(_) => break,
            }
        }

        let (commands, replies): (Vec<_>, Vec<_>) = round
            .into_iter()
            .map(|request| ((request.header, request.command), request.reply))
            .unzip();
        let size = commands.len();
        let gate = session.accept_gate();
        let mut publication = session.publication_guard().await;
        let outcome = session
            .transaction(move |conn| intake_round_tx(conn, &commands, gate))
            .await;
        match outcome {
            Ok(mut outcomes) => {
                let maker_updates: Vec<_> = outcomes
                    .iter_mut()
                    .filter_map(|(_, outcome)| outcome.update.take())
                    .collect();
                let mut active = Vec::new();
                for (maker_id, outcome) in &mut outcomes {
                    if let Some(order) = outcome.live.take() {
                        gateway_metrics::count(gateway_metrics::LIVE_AT_ACCEPT, *maker_id);
                        active.push(order);
                    }
                    if let Some(reason) = outcome.fallback {
                        gateway_metrics::count_fallback(*maker_id, reason);
                    }
                }
                let went_live = !active.is_empty();
                if !maker_updates.is_empty() || went_live {
                    // The publication guard makes this the next committed
                    // matcher update after every earlier book transaction.
                    if book_tx
                        .send(crate::types::BookUpdate {
                            removed: Vec::new(),
                            active,
                            maker_updates,
                        })
                        .await
                        .is_err()
                    {
                        let error = Arc::new(crate::db::DbError::MatcherStopped);
                        for reply in replies {
                            let _ = reply.send(Err(IntakeError::OutcomeUnknown(error.clone())));
                        }
                        return;
                    }
                }
                if went_live {
                    // The core writer's append flag is not ours to take.
                    EventWake::global().notify();
                }
                for (reply, (_, outcome)) in replies.into_iter().zip(outcomes) {
                    let _ = reply.send(Ok(outcome.reply));
                }
            }
            Err(error) => {
                tracing::warn!(%error, commands = size, "maker intake round outcome unknown");
                let error = Arc::new(error);
                for reply in replies {
                    let _ = reply.send(Err(IntakeError::OutcomeUnknown(error.clone())));
                }
                return;
            }
        }
        publication.complete();
    }
}

/// One intake round's writes, in its transaction: with live at accept on and
/// a private submit in the round, the owner-epoch guard first (see
/// [`AcceptGate::accept_block`]), then the order rows and maker rows the
/// round may touch, in the reservation's lock order, then every command.
pub(crate) fn intake_round_tx(
    conn: &mut PgConnection,
    commands: &[(CommandHeader, MakerCommand)],
    mut gate: Option<AcceptGate>,
) -> DbResult<Vec<(MakerId, CommandOutcome)>> {
    let private_submit = commands.iter().any(|(_, command)| {
        matches!(command, MakerCommand::Submit { note, .. }
            if note.metadata().note_type() == NoteType::Private)
    });
    match gate.as_mut() {
        Some(gate) if private_submit => {
            gate.accept_block(conn)?;
        }
        _ => gate = None,
    }
    crate::db::maker_db::prelock_submission_orders_tx(conn, commands)?;
    crate::db::maker_db::prelock_command_makers_tx(conn, commands)?;
    commands
        .iter()
        .map(|(header, command)| {
            let outcome = execute_command_with_tx(conn, header, command, gate.as_mut())?;
            Ok((header.maker_id, outcome))
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::postgres_test::TestDb;
    use crate::db::{maker_db, DbError};
    use crate::maker::MakerUpdate;
    use crate::maker::{CommandResult, CutoffScope, MakerId};
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::note::{Note, NoteType};
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    pub(crate) fn pswap_note(serial: u32) -> Note {
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
            .note_type(NoteType::Private)
            .offered_asset(FungibleAsset::new(offered, 50).unwrap())
            .build()
            .unwrap()
            .into()
    }

    async fn maker(db: &TestDb) -> MakerId {
        db.pool
            .write(|conn| maker_db::create_maker_tx(conn, "alpha"))
            .await
            .unwrap()
            .unwrap()
    }

    fn header(maker_id: MakerId, request_id: &str, seq: u64) -> CommandHeader {
        CommandHeader::new(maker_id, request_id.into(), seq).unwrap()
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_round_writes_cancels_first_and_queues_updates_before_replying() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let (intake, mut queues) = IntakeSender::new(8, 8);
        let (updates_tx, mut updates) = mpsc::channel(8);
        let execute = |request_id: &'static str, seq, command| {
            let intake = intake.clone();
            tokio::spawn(async move {
                intake
                    .execute(header(alpha, request_id, seq), command)
                    .await
            })
        };
        // All three wait in the queues before the writer starts: one round.
        let first = execute("s1", 1, MakerCommand::submit(pswap_note(1)).unwrap());
        let cancel = execute(
            "c2",
            2,
            MakerCommand::CancelAll {
                scope: CutoffScope::all(),
            },
        );
        let last = execute("s3", 3, MakerCommand::submit(pswap_note(3)).unwrap());
        while queues.cancels_rx.len() + queues.submits_rx.len() < 3 {
            tokio::task::yield_now().await;
        }
        let stop = CancellationToken::new();
        let writer = tokio::spawn({
            let session = db.pool.intake_session();
            let queues = std::mem::replace(&mut queues, IntakeSender::new(1, 1).1);
            run_intake(session, queues, updates_tx, 500, stop.clone())
        });

        assert_eq!(
            cancel.await.unwrap().unwrap(),
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 2,
                settling: 0
            })
        );
        // The cancel was written first, and its update is queued before any
        // handler hears back.
        let committed = updates.try_recv().unwrap();
        assert!(matches!(
            committed.maker_updates.first(),
            Some(MakerUpdate::CutoffRaised { cutoff: 2, .. })
        ));
        for submit in [first, last] {
            assert_eq!(
                submit.await.unwrap().unwrap(),
                CommandReply::Committed(CommandResult::Accepted)
            );
        }
        assert!(matches!(
            committed.maker_updates.get(1),
            Some(MakerUpdate::OrdersAttributed { .. })
        ));
        stop.cancel();
        writer.await.unwrap();
    }

    /// Retries can land in the same round as the command they repeat. Each
    /// still gets the stored outcome, conflicts are refused, nothing is
    /// stored twice, and the cancel's barrier holds within the round.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn duplicates_and_conflicts_in_one_round_get_consistent_replies() {
        use crate::db::postgres_models::NewOrderRow;
        use crate::db::postgres_schema::{live_orders, maker_commands};
        use diesel::prelude::*;
        use miden_protocol::crypto::utils::Serializable;

        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let (intake, mut queues) = IntakeSender::new(16, 16);
        let (updates_tx, _updates) = mpsc::channel(16);
        let submit = |serial| MakerCommand::submit(pswap_note(serial)).unwrap();
        let all = || MakerCommand::CancelAll {
            scope: CutoffScope::all(),
        };
        let commands = [
            ("s1", 1, submit(1)),
            ("s1", 1, submit(1)),    // the same command again
            ("s1", 1, submit(2)),    // its ID with another note
            ("other", 1, submit(3)), // its sequence with another ID
            ("s4", 4, submit(4)),    // below the cancel's barrier
            ("c5", 5, all()),
            ("s6", 6, submit(6)), // above it
        ];
        let mut replies = Vec::new();
        for (queued, (request_id, seq, command)) in commands.into_iter().enumerate() {
            let intake = intake.clone();
            replies.push(tokio::spawn(async move {
                intake
                    .execute(header(alpha, request_id, seq), command)
                    .await
            }));
            // Queue them in this order, all before the writer starts.
            while queues.cancels_rx.len() + queues.submits_rx.len() <= queued {
                tokio::task::yield_now().await;
            }
        }
        let stop = CancellationToken::new();
        let writer = tokio::spawn({
            let session = db.pool.intake_session();
            let queues = std::mem::replace(&mut queues, IntakeSender::new(1, 1).1);
            run_intake(session, queues, updates_tx, 500, stop.clone())
        });
        let mut results = Vec::new();
        for reply in replies {
            results.push(reply.await.unwrap().unwrap());
        }
        let accepted = CommandResult::Accepted;
        assert_eq!(
            results,
            [
                CommandReply::Committed(accepted.clone()),
                CommandReply::Replayed(accepted.clone()),
                CommandReply::Conflict,
                CommandReply::Conflict,
                CommandReply::Committed(accepted.clone()),
                CommandReply::Committed(CommandResult::Applied {
                    cutoff: 5,
                    settling: 0
                }),
                CommandReply::Committed(accepted),
            ]
        );

        let notes = [pswap_note(1), pswap_note(4), pswap_note(6)];
        let (stored, live) = db
            .pool
            .write(move |conn| {
                let stored: i64 = maker_commands::table
                    .filter(maker_commands::maker_id.eq(alpha))
                    .count()
                    .get_result(conn)?;
                let rows = notes
                    .iter()
                    .map(|note| NewOrderRow::ingested(note, 1))
                    .collect::<crate::db::DbResult<Vec<_>>>()?;
                crate::db::postgres_db::insert_orders_batch_tx(conn, &rows, 1)?;
                let live = live_orders::table
                    .select(live_orders::note_id)
                    .load::<Vec<u8>>(conn)?;
                Ok((stored, live))
            })
            .await
            .unwrap();
        assert_eq!(stored, 4, "s1, s4, c5 and s6, once each");
        assert_eq!(
            live,
            [pswap_note(6).id().to_bytes().to_vec()],
            "only the submit above the barrier can trade"
        );
        stop.cancel();
        writer.await.unwrap();
    }

    /// Ingest advances `sync_state` and then appends maker events, which
    /// lock maker rows. A live-at-accept round share-locks `sync_state` before
    /// any maker row, so the two queue one behind the other. Taking it after
    /// the maker lock deadlocks, and an intake round that fails stops the
    /// solver.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    fn a_live_at_accept_round_and_ingest_never_deadlock() -> anyhow::Result<()> {
        use crate::db::postgres_migrations::connect;
        use crate::db::postgres_test::TestSchema;
        use diesel::connection::SimpleConnection;
        use diesel::prelude::*;
        use std::sync::{Arc, Barrier};

        let mut fixture = TestSchema::migrated()?;
        let alpha = maker_db::create_maker_tx(&mut fixture.conn, "alpha")?.unwrap();
        let epoch: i64 = crate::db::postgres_schema::sync_state::table
            .find(1_i16)
            .select(crate::db::postgres_schema::sync_state::owner_epoch)
            .first(&mut fixture.conn)?;
        let barrier = Arc::new(Barrier::new(2));
        let ingest = std::thread::spawn({
            let (url, barrier) = (fixture.url.clone(), barrier.clone());
            move || -> anyhow::Result<()> {
                let mut conn = connect(&url)?;
                conn.batch_execute("BEGIN; UPDATE sync_state SET last_fetched_block = 5")?;
                barrier.wait();
                // Let the intake round take whatever it takes first.
                std::thread::sleep(std::time::Duration::from_millis(500));
                conn.batch_execute(&format!(
                    "UPDATE makers SET next_event_seq = next_event_seq WHERE maker_id = {alpha}; COMMIT"
                ))?;
                Ok(())
            }
        });
        let intake = std::thread::spawn({
            let (url, barrier) = (fixture.url.clone(), barrier.clone());
            move || -> anyhow::Result<bool> {
                let mut conn = connect(&url)?;
                let commands = [(
                    header(alpha, "s1", 1),
                    MakerCommand::submit(pswap_note(1)).unwrap(),
                )];
                let gate =
                    AcceptGate::new(epoch, || Some(miden_protocol::block::BlockNumber::from(7)));
                barrier.wait();
                let outcomes = conn.transaction::<_, crate::db::DbError, _>(|conn| {
                    intake_round_tx(conn, &commands, Some(gate))
                })?;
                Ok(outcomes[0].1.live.is_some())
            }
        });
        ingest.join().expect("ingest thread")?;
        assert!(
            intake.join().expect("intake thread")?,
            "went live at accept"
        );
        Ok(())
    }

    /// With live at accept on, a fresh private submit is in the round's one
    /// matcher update, tagged, next to its attribution, and the event
    /// streams wake for its LIVE. A stale watcher sends it the confirmed way.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_live_at_accept_round_publishes_the_order_and_wakes_streams() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let fresh = Arc::new(AtomicBool::new(true));
        let session = db.pool.intake_session().with_live_at_accept({
            let fresh = fresh.clone();
            move || {
                fresh
                    .load(Ordering::Relaxed)
                    .then(|| miden_protocol::block::BlockNumber::from(7))
            }
        });
        let (intake, queues) = IntakeSender::new(4, 4);
        let (book_tx, mut book_rx) = mpsc::channel(4);
        let stop = CancellationToken::new();
        let writer = tokio::spawn(run_intake(session, queues, book_tx, 10, stop.clone()));
        let mut woken = EventWake::global().subscribe();
        woken.mark_unchanged();

        let order = pswap_note(1);
        assert_eq!(
            intake
                .execute(
                    header(alpha, "s1", 1),
                    MakerCommand::submit(order.clone()).unwrap()
                )
                .await
                .unwrap(),
            CommandReply::Committed(CommandResult::Accepted)
        );
        let update = book_rx.recv().await.unwrap();
        assert_eq!(update.active.len(), 1);
        assert_eq!(update.active[0].id(), order.id());
        assert_eq!(update.active[0].maker.map(|tag| tag.maker_id), Some(alpha));
        assert!(matches!(
            update.maker_updates.as_slice(),
            [MakerUpdate::OrdersAttributed { .. }]
        ));
        assert!(woken.has_changed().unwrap(), "streams woken for LIVE");

        fresh.store(false, Ordering::Relaxed);
        intake
            .execute(
                header(alpha, "s2", 2),
                MakerCommand::submit(pswap_note(2)).unwrap(),
            )
            .await
            .unwrap();
        let update = book_rx.recv().await.unwrap();
        assert!(
            update.active.is_empty(),
            "a stale watcher: the confirmed path"
        );
        stop.cancel();
        writer.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_maker_cancel_follows_earlier_book_updates_on_one_stream() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let order_id = pswap_note(9).id();
        let (book_tx, mut book_rx) = mpsc::channel(2);
        db.pool
            .write_book(&book_tx, move |_| {
                Ok(crate::types::BookUpdate {
                    removed: vec![order_id],
                    active: Vec::new(),
                    maker_updates: Vec::new(),
                })
            })
            .await
            .unwrap();

        let (intake, queues) = IntakeSender::new(2, 2);
        let stop = CancellationToken::new();
        let writer = tokio::spawn(run_intake(
            db.pool.intake_session(),
            queues,
            book_tx,
            2,
            stop.clone(),
        ));
        assert_eq!(
            intake
                .execute(
                    header(alpha, "c1", 1),
                    MakerCommand::CancelAll {
                        scope: CutoffScope::all()
                    },
                )
                .await
                .unwrap(),
            CommandReply::Committed(CommandResult::Applied {
                cutoff: 1,
                settling: 0
            }),
        );
        assert_eq!(book_rx.recv().await.unwrap().removed, vec![order_id]);
        assert!(matches!(
            book_rx.recv().await.unwrap().maker_updates.first(),
            Some(MakerUpdate::CutoffRaised { cutoff: 1, .. })
        ));
        stop.cancel();
        writer.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_committed_cancel_with_no_matcher_stops_publication() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let (book_tx, book_rx) = mpsc::channel(1);
        drop(book_rx);
        let (intake, queues) = IntakeSender::new(2, 2);
        let stop = CancellationToken::new();
        let writer = tokio::spawn(run_intake(
            db.pool.intake_session(),
            queues,
            book_tx,
            2,
            stop,
        ));
        let reply = intake
            .execute(
                header(alpha, "c1", 1),
                MakerCommand::CancelAll {
                    scope: CutoffScope::all(),
                },
            )
            .await;
        assert!(matches!(
            reply,
            Err(IntakeError::OutcomeUnknown(ref error)) if matches!(**error, DbError::MatcherStopped)
        ));
        writer.await.unwrap();
        assert!(db.pool.fatal_token().is_cancelled());
    }

    #[tokio::test]
    async fn a_full_queue_fails_fast_and_a_gone_writer_answers_stopped() {
        let (intake, queues) = IntakeSender::new(1, 1);
        let submit = || MakerCommand::submit(pswap_note(1)).unwrap();
        let waiting = tokio::spawn({
            let intake = intake.clone();
            async move { intake.execute(header(1, "a", 1), submit()).await }
        });
        while queues.submits_rx.is_empty() {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            intake.execute(header(1, "b", 2), submit()).await,
            Err(IntakeError::Busy)
        ));
        // A cancel has its own queue: a full submit queue does not block it.
        let cancel = tokio::spawn({
            let intake = intake.clone();
            async move {
                intake
                    .execute(
                        header(1, "c", 3),
                        MakerCommand::CancelAll {
                            scope: CutoffScope::all(),
                        },
                    )
                    .await
            }
        });
        while queues.cancels_rx.is_empty() {
            tokio::task::yield_now().await;
        }
        drop(queues);
        assert!(matches!(waiting.await.unwrap(), Err(IntakeError::Stopped)));
        assert!(matches!(cancel.await.unwrap(), Err(IntakeError::Stopped)));
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_round_that_cannot_commit_acknowledges_nothing() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let (intake, queues) = IntakeSender::new(8, 8);
        let (updates_tx, mut updates) = mpsc::channel(8);
        // The pool turned fatal: the intake session refuses to write.
        db.pool.fatal_token().cancel();
        let stop = CancellationToken::new();
        let writer = tokio::spawn(run_intake(
            db.pool.intake_session(),
            queues,
            updates_tx,
            500,
            stop.clone(),
        ));
        let reply = intake
            .execute(
                header(alpha, "c1", 1),
                MakerCommand::CancelAll {
                    scope: CutoffScope::all(),
                },
            )
            .await;
        assert!(matches!(
            reply,
            Err(IntakeError::OutcomeUnknown(ref error)) if matches!(**error, DbError::WriterUnsafe)
        ));
        assert!(
            updates.try_recv().is_err(),
            "nothing committed, nothing sent"
        );
        stop.cancel();
        writer.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_lost_commit_reply_stops_intake_until_hydration_restores_attribution() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db).await;
        let order = pswap_note(66);
        let row = crate::db::postgres_models::NewOrderRow::ingested(&order, 1).unwrap();
        db.pool
            .write(move |conn| crate::db::postgres_db::insert_orders_batch_tx(conn, &[row], 1))
            .await
            .unwrap();
        let mut session = db.pool.intake_session();
        session.simulate_lost_commit_reply();
        let (intake, queues) = IntakeSender::new(2, 2);
        let (book_tx, mut book_rx) = mpsc::channel(2);
        let writer = tokio::spawn(run_intake(
            session,
            queues,
            book_tx,
            2,
            CancellationToken::new(),
        ));
        let reply = intake
            .execute(
                header(alpha, "s1", 1),
                MakerCommand::submit(order.clone()).unwrap(),
            )
            .await;
        assert!(matches!(
            reply,
            Err(IntakeError::OutcomeUnknown(ref error))
                if matches!(**error, DbError::Corrupt("simulated lost COMMIT response"))
        ));
        writer.await.unwrap();
        assert!(db.pool.fatal_token().is_cancelled());
        assert!(
            book_rx.try_recv().is_err(),
            "no matcher update was published"
        );

        // The stored command is replayable, but a replay has no update of
        // its own. Recovery must hydrate the matcher before accepting it.
        let request_id = String::from("s1");
        let retry_header = header(alpha, "s1", 1);
        let retry_command = MakerCommand::submit(order.clone()).unwrap();
        let url = db.url().to_owned();
        let (stored, live, replay) = tokio::task::spawn_blocking(move || -> DbResult<_> {
            let mut conn = crate::db::postgres_migrations::connect(&url)?;
            let stored = maker_db::stored_result_tx(&mut conn, alpha, &request_id)?;
            let live = crate::db::postgres_db::load_live_orders_tx(&mut conn)?;
            let replay = maker_db::execute_command_tx(&mut conn, &retry_header, &retry_command)?;
            Ok((stored, live, replay))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stored, Some(CommandResult::Accepted));
        let hydrated = live.iter().find(|entry| entry.id() == order.id()).unwrap();
        assert_eq!(hydrated.maker.unwrap().maker_id, alpha);
        assert_eq!(hydrated.maker.unwrap().root_seq, 1);
        assert_eq!(
            replay,
            (CommandReply::Replayed(CommandResult::Accepted), None)
        );
    }
}

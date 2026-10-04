//! The maker intake writer (ADR 0003). One task owns the intake session and
//! writes waiting commands in group-commit rounds: every waiting cancel
//! first, then submits up to the round limit. Commands that arrive while a
//! round commits form the next round, so batching costs no added wait.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::error::IntakeError;
use crate::db::maker_db::execute_command_tx;
use crate::db::{DbResult, IntakeSession};
use crate::maker::{CommandHeader, CommandReply, MakerCommand};

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
        let mut submits = Vec::new();
        match first.command {
            MakerCommand::Submit { .. } => submits.push(first),
            _ => round.push(first),
        }
        while let Ok(request) = intake.cancels_rx.try_recv() {
            round.push(request);
        }
        while submits.len() < max_submits {
            match intake.submits_rx.try_recv() {
                Ok(request) => submits.push(request),
                Err(_) => break,
            }
        }
        round.extend(submits);

        let (commands, replies): (Vec<_>, Vec<_>) = round
            .into_iter()
            .map(|request| ((request.header, request.command), request.reply))
            .unzip();
        let size = commands.len();
        let mut publication = session.publication_guard().await;
        let outcome = session
            .transaction(move |conn| {
                crate::db::maker_db::prelock_submission_orders_tx(conn, &commands)?;
                crate::db::maker_db::prelock_command_makers_tx(conn, &commands)?;
                commands
                    .iter()
                    .map(|(header, command)| execute_command_tx(conn, header, command))
                    .collect::<DbResult<Vec<_>>>()
            })
            .await;
        match outcome {
            Ok(outcomes) => {
                let (results, committed): (Vec<_>, Vec<_>) = outcomes.into_iter().unzip();
                let maker_updates: Vec<_> = committed.into_iter().flatten().collect();
                if !maker_updates.is_empty() {
                    // The publication guard makes this the next committed
                    // matcher update after every earlier book transaction.
                    if book_tx
                        .send(crate::types::BookUpdate {
                            removed: Vec::new(),
                            active: Vec::new(),
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
                for (reply, result) in replies.into_iter().zip(results) {
                    let _ = reply.send(Ok(result));
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

#[cfg(test)]
pub(super) mod tests {
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
}

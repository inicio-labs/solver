//! The maker event feed (ADR 0003). Events are rows written in the business
//! transaction that produced them; a stream only reads committed rows, so a
//! crash after commit loses nothing and a reconnect resumes from a cursor.

use std::time::Duration;

use diesel::pg::PgConnection;
use prost::Message;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::proto::{self, event_body, stream_message};
use crate::db::maker_db::{self, EventKind, StoredEvent};
use crate::db::{DbError, DbPool, DbResult};
use crate::maker::{EventWake, MakerId};
use crate::types::now_millis;

/// Events read per query while a stream catches up.
const PAGE: i64 = 256;

/// Append `body` to `maker_id`'s feed in the caller's business transaction;
/// returns its sequence. The core writer wakes the streams after commit.
pub fn append_event_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    lineage_id: Option<&[u8]>,
    body: &proto::EventBody,
) -> DbResult<u64> {
    let kind = match body.kind {
        Some(event_body::Kind::OrderStatus(_)) => EventKind::OrderStatus,
        Some(event_body::Kind::SettlementPending(_)) => EventKind::SettlementPending,
        Some(event_body::Kind::SettlementResolved(_)) => EventKind::SettlementResolved,
        None => return Err(DbError::Corrupt("maker event has no body")),
    };
    maker_db::append_event_tx(conn, maker_id, kind, lineage_id, &body.encode_to_vec())
}

#[derive(Clone, Copy)]
pub struct StreamConfig {
    /// Messages a subscriber may leave unread before it is disconnected.
    pub buffer: usize,
    /// Keep-alive interval, which also re-checks the API key.
    pub heartbeat: Duration,
}

pub(super) type StreamSender = mpsc::Sender<Result<proto::StreamMessage, Status>>;

/// One subscriber's stream.
pub(super) struct Subscriber {
    pub maker_id: MakerId,
    pub key_hash: Vec<u8>,
    pub cursor: u64,
}

/// Feed one subscriber: every event after its cursor, a ReplayComplete once
/// caught up, then each new event once it commits, and a keep-alive carrying
/// the cursor. Reads go through the public read budget, so streams cannot
/// take every read connection, and no read is held while waiting on the
/// subscriber. A subscriber that takes nothing for a whole keep-alive
/// interval is disconnected (RESOURCE_EXHAUSTED) and resumes from its cursor;
/// a revoked key ends the stream at the next keep-alive.
pub(super) async fn feed(
    pool: DbPool,
    wake: EventWake,
    config: StreamConfig,
    subscriber: Subscriber,
    tx: StreamSender,
    cancel: CancellationToken,
) {
    let Subscriber {
        maker_id,
        key_hash,
        mut cursor,
    } = subscriber;
    let mut woken = wake.subscribe();
    let mut keep_alive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.heartbeat,
        config.heartbeat,
    );
    let mut replayed = false;
    loop {
        // Mark the wake seen before reading, so a commit during the read
        // wakes the next round.
        woken.borrow_and_update();
        loop {
            let after = cursor;
            let page = match pool
                .read_public(move |conn| maker_db::read_events_tx(conn, maker_id, after, PAGE))
                .await
            {
                Ok(page) => page,
                Err(_) => {
                    let _ = tx.try_send(Err(Status::unavailable(
                        "event feed unavailable; reconnect from your last cursor",
                    )));
                    return;
                }
            };
            let caught_up = page.len() < PAGE as usize;
            for event in page {
                let seq = event.seq;
                let message = match event_message(event) {
                    Ok(message) => message,
                    Err(_) => {
                        let _ = tx.try_send(Err(Status::internal("a stored event is unreadable")));
                        return;
                    }
                };
                if !send(&tx, message, config.heartbeat).await {
                    return;
                }
                cursor = seq;
            }
            if caught_up {
                break;
            }
        }
        if !replayed {
            replayed = true;
            let complete = proto::ReplayComplete {
                through_seq: cursor,
            };
            if !send(
                &tx,
                stream_message::Message::ReplayComplete(complete),
                config.heartbeat,
            )
            .await
            {
                return;
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tx.closed() => return,
            _ = woken.changed() => {}
            _ = keep_alive.tick() => {
                let key_hash = key_hash.clone();
                let owner = pool
                    .read_public(move |conn| maker_db::authenticate_tx(conn, &key_hash))
                    .await;
                // A failed check is retried at the next keep-alive.
                if matches!(owner, Ok(owner) if owner != Some(maker_id)) {
                    let _ = tx.try_send(Err(Status::unauthenticated("API key revoked")));
                    return;
                }
                let beat = proto::KeepAlive {
                    cursor,
                    server_time_unix_ms: unix_ms(),
                };
                if !send(&tx, stream_message::Message::KeepAlive(beat), config.heartbeat).await {
                    return;
                }
            }
        }
    }
}

/// Queue a message, waiting up to `patience` for the subscriber to make
/// room, so a long replay paces itself to the subscriber. One slot is always
/// left free for the disconnect notice sent when the wait runs out.
async fn send(tx: &StreamSender, message: stream_message::Message, patience: Duration) -> bool {
    match tokio::time::timeout(patience, tx.reserve_many(2)).await {
        Ok(Ok(mut permits)) => {
            if let Some(permit) = permits.next() {
                permit.send(Ok(proto::StreamMessage {
                    message: Some(message),
                }));
            }
            true
        }
        Ok(Err(_closed)) => false,
        Err(_elapsed) => {
            let _ = tx.try_send(Err(Status::resource_exhausted(
                "event consumer too slow; reconnect from your last cursor",
            )));
            false
        }
    }
}

pub(super) fn unix_ms() -> i64 {
    i64::try_from(now_millis()).unwrap_or(i64::MAX)
}

fn event_message(event: StoredEvent) -> Result<stream_message::Message, prost::DecodeError> {
    Ok(stream_message::Message::Event(proto::MakerEvent {
        seq: event.seq,
        event_id: event.event_id,
        created_at_unix_ms: event.created_at_unix_ms,
        lineage_id: event.lineage_id.unwrap_or_default(),
        body: Some(proto::EventBody::decode(event.payload.as_slice())?),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_test::TestDb;

    fn body(note: u8) -> proto::EventBody {
        proto::EventBody {
            kind: Some(event_body::Kind::SettlementPending(
                proto::SettlementPending {
                    tx_id: vec![note],
                    fills: Vec::new(),
                },
            )),
        }
    }

    struct Maker {
        maker_id: MakerId,
        key_id: i64,
        key_hash: Vec<u8>,
    }

    async fn maker(db: &TestDb, name: &'static str, events: u8) -> Maker {
        let key_hash = crate::maker::api_key_hash(&crate::maker::new_api_key());
        let hash = key_hash.clone();
        let (maker_id, key_id) = db
            .pool
            .write(move |conn| {
                let maker_id = maker_db::create_maker_tx(conn, name)?.unwrap();
                let key_id = maker_db::issue_api_key_tx(conn, maker_id, &hash)?.unwrap();
                for note in 0..events {
                    append_event_tx(conn, maker_id, None, &body(note))?;
                }
                Ok((maker_id, key_id))
            })
            .await
            .unwrap();
        Maker {
            maker_id,
            key_id,
            key_hash,
        }
    }

    fn start(
        db: &TestDb,
        maker: &Maker,
        config: StreamConfig,
    ) -> (
        mpsc::Receiver<Result<proto::StreamMessage, Status>>,
        CancellationToken,
    ) {
        let (tx, rx) = mpsc::channel(config.buffer);
        let cancel = CancellationToken::new();
        tokio::spawn(feed(
            db.pool.clone(),
            EventWake::default(),
            config,
            Subscriber {
                maker_id: maker.maker_id,
                key_hash: maker.key_hash.clone(),
                cursor: 0,
            },
            tx,
            cancel.clone(),
        ));
        (rx, cancel)
    }

    fn seq(message: Result<proto::StreamMessage, Status>) -> u64 {
        match message.unwrap().message.unwrap() {
            stream_message::Message::Event(event) => event.seq,
            other => panic!("expected an event, got {other:?}"),
        }
    }

    fn replay_complete(message: Result<proto::StreamMessage, Status>) -> u64 {
        match message.unwrap().message.unwrap() {
            stream_message::Message::ReplayComplete(complete) => complete.through_seq,
            other => panic!("expected ReplayComplete, got {other:?}"),
        }
    }

    const SLOW_HEARTBEAT: Duration = Duration::from_secs(3_600);

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_stream_sees_only_its_makers_events() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db, "alpha", 2).await;
        let _beta = maker(&db, "beta", 3).await;
        let maker_id = alpha.maker_id;
        let bodiless = db
            .pool
            .write(move |conn| {
                append_event_tx(conn, maker_id, None, &proto::EventBody { kind: None })
            })
            .await;
        assert!(bodiless.is_err(), "an event needs a body");
        let (mut rx, cancel) = start(
            &db,
            &alpha,
            StreamConfig {
                buffer: 8,
                heartbeat: SLOW_HEARTBEAT,
            },
        );
        assert_eq!(seq(rx.recv().await.unwrap()), 1);
        assert_eq!(seq(rx.recv().await.unwrap()), 2);
        assert_eq!(replay_complete(rx.recv().await.unwrap()), 2);
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err(), "beta's events are not alpha's");
        cancel.cancel();
        assert!(rx.recv().await.is_none(), "shutdown ends the stream");
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_long_replay_paces_itself_to_the_consumer() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db, "alpha", 40).await;
        let (mut rx, _cancel) = start(
            &db,
            &alpha,
            StreamConfig {
                buffer: 3,
                heartbeat: Duration::from_secs(5),
            },
        );
        for expected in 1..=40 {
            assert_eq!(seq(rx.recv().await.unwrap()), expected);
        }
        assert_eq!(replay_complete(rx.recv().await.unwrap()), 40);
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn a_consumer_that_takes_nothing_is_disconnected_to_resume_later() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db, "alpha", 5).await;
        let (mut rx, _cancel) = start(
            &db,
            &alpha,
            StreamConfig {
                buffer: 3,
                heartbeat: Duration::from_millis(50),
            },
        );
        // Nobody reads while the feed fills the buffer.
        while rx.len() < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(seq(rx.recv().await.unwrap()), 1);
        assert_eq!(seq(rx.recv().await.unwrap()), 2);
        let notice = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(notice.code(), tonic::Code::ResourceExhausted);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn revoking_the_key_ends_an_open_stream() {
        let db = TestDb::new().await.unwrap();
        let alpha = maker(&db, "alpha", 0).await;
        let (mut rx, _cancel) = start(
            &db,
            &alpha,
            StreamConfig {
                buffer: 8,
                heartbeat: Duration::from_millis(50),
            },
        );
        assert_eq!(replay_complete(rx.recv().await.unwrap()), 0);
        match rx.recv().await.unwrap().unwrap().message.unwrap() {
            stream_message::Message::KeepAlive(beat) => assert_eq!(beat.cursor, 0),
            other => panic!("expected a keep-alive, got {other:?}"),
        }
        let key_id = alpha.key_id;
        db.pool
            .write(move |conn| maker_db::revoke_api_key_tx(conn, key_id))
            .await
            .unwrap();
        let ended = loop {
            match rx.recv().await.unwrap() {
                Ok(_) => continue,
                Err(status) => break status,
            }
        };
        assert_eq!(ended.code(), tonic::Code::Unauthenticated);
        assert!(rx.recv().await.is_none());
    }
}

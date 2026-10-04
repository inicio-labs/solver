//! The maker gateway's gRPC service and its thread.
//!
//! Handlers only authenticate, decode and validate; the intake writer makes
//! every command durable. Note payloads and API keys are never logged.

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Result};
use miden_client::rpc::NodeRpcClient;
use miden_protocol::account::AccountId;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use super::config::{GatewayConfig, StreamConfig};
use super::error::IntakeError;
use super::events::{feed, unix_ms, Subscriber};
use super::intake::{run_intake, IntakeSender};
use super::proto::maker_gateway_server::{MakerGateway, MakerGatewayServer};
use super::proto::{self, command_reply};
use super::watcher::{run_watcher, RpcChain, Watcher};
use crate::db::{maker_db, postgres_db, DbPool};
use crate::maker::EventWake;
use crate::maker::{
    api_key_hash, market_key, CommandHeader, CommandReply, CommandResult, CutoffScope,
    MakerCommand, MakerId,
};
use crate::types::{BookUpdate, TokenId};

/// Largest accepted request. A PSWAP note is a few kilobytes.
const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Bounds persistent feed tasks, channel memory, and periodic public reads.
const MAX_EVENT_STREAMS: usize = 128;

/// Creator account ID plus four serial-number elements of eight bytes.
const LINEAGE_ID_LEN: usize = AccountId::SERIALIZED_SIZE + 32;

/// Orders per GetMakerState page, by default and at most.
const DEFAULT_STATE_PAGE: u32 = 500;
const MAX_STATE_PAGE: u32 = 1_000;

#[derive(Clone)]
pub struct MakerGatewayService {
    pool: DbPool,
    intake: IntakeSender,
    markets: Arc<HashSet<Vec<u8>>>,
    events: EventWake,
    stream: StreamConfig,
    event_slots: Arc<Semaphore>,
    /// Ends every event stream at shutdown, so the server can stop.
    cancel: CancellationToken,
}

impl MakerGatewayService {
    pub fn new(
        pool: DbPool,
        intake: IntakeSender,
        markets: HashSet<Vec<u8>>,
        events: EventWake,
        stream: StreamConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pool,
            intake,
            markets: Arc::new(markets),
            events,
            stream,
            event_slots: Arc::new(Semaphore::new(MAX_EVENT_STREAMS)),
            cancel,
        }
    }

    pub fn into_server(self) -> MakerGatewayServer<Self> {
        MakerGatewayServer::new(self).max_decoding_message_size(MAX_REQUEST_BYTES)
    }

    /// The maker whose unrevoked API key the request carries, and the key's
    /// hash. Checked on every call, so a revoked key stops working at once.
    /// Gateway reads use the public read budget, which always leaves a read
    /// connection to the solver pipeline.
    async fn authenticate<T>(&self, request: &Request<T>) -> Result<(MakerId, Vec<u8>), Status> {
        let key = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing API key"))?;
        let key_hash = api_key_hash(key);
        let lookup = key_hash.clone();
        let maker_id = self
            .pool
            .read_public(move |conn| maker_db::authenticate_tx(conn, &lookup))
            .await
            .map_err(|_| Status::unavailable("cannot check the API key now; retry"))?
            .ok_or_else(|| Status::unauthenticated("unknown or revoked API key"))?;
        Ok((maker_id, key_hash))
    }

    async fn execute(
        &self,
        header: CommandHeader,
        command: MakerCommand,
    ) -> Result<Response<proto::CommandReply>, Status> {
        match self.intake.execute(header, command).await {
            Ok(CommandReply::Committed(result)) => Ok(Response::new(reply(result, false))),
            Ok(CommandReply::Replayed(result)) => Ok(Response::new(reply(result, true))),
            Ok(CommandReply::Conflict) => Err(Status::already_exists(
                "request ID or sequence already used for a different command",
            )),
            Err(error) => Err(intake_status(&error)),
        }
    }
}

#[tonic::async_trait]
impl MakerGateway for MakerGatewayService {
    type StreamEventsStream = ReceiverStream<Result<proto::StreamMessage, Status>>;

    async fn stream_events(
        &self,
        request: Request<proto::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let (maker_id, key_hash) = self.authenticate(&request).await?;
        let subscriber = Subscriber {
            maker_id,
            key_hash,
            cursor: request.into_inner().after_seq,
        };
        let slot = self
            .event_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many event subscriptions"))?;
        let (tx, rx) = mpsc::channel(self.stream.buffer.max(2));
        let pool = self.pool.clone();
        let events = self.events.clone();
        let stream = self.stream;
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let _slot = slot;
            feed(pool, events, stream, subscriber, tx, cancel).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn submit_order(
        &self,
        request: Request<proto::SubmitOrderRequest>,
    ) -> Result<Response<proto::CommandReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let request = request.into_inner();
        let header = header(maker_id, request.header)?;
        let note = Note::read_from_bytes(&request.note)
            .ok()
            .filter(|note| note.to_bytes() == request.note)
            .ok_or_else(|| Status::invalid_argument("note is not a canonical miden note"))?;
        let command = MakerCommand::submit(note).map_err(|error| {
            Status::invalid_argument(format!("note is not a valid PSWAP order: {error}"))
        })?;
        if let MakerCommand::Submit { keys, .. } = &command {
            if !self.markets.contains(&keys.market) {
                return Err(Status::invalid_argument(
                    "this solver does not trade that pair",
                ));
            }
        }
        self.execute(header, command).await
    }

    async fn cancel_all(
        &self,
        request: Request<proto::CancelAllRequest>,
    ) -> Result<Response<proto::CommandReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let (header, scope) = request.into_inner().verify(maker_id)?;
        self.execute(header, MakerCommand::CancelAll { scope })
            .await
    }

    async fn cancel_order(
        &self,
        request: Request<proto::CancelOrderRequest>,
    ) -> Result<Response<proto::CommandReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let (header, lineage_id) = request.into_inner().verify(maker_id)?;
        self.execute(header, MakerCommand::CancelOrder { lineage_id })
            .await
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let latest_seq = self
            .pool
            .read_public(move |conn| maker_db::latest_event_seq_tx(conn, maker_id))
            .await
            .map_err(|_| Status::unavailable("cannot read the event feed now; retry"))?;
        let received = request.into_inner().received_through_seq;
        tracing::debug!(
            maker_id,
            received,
            lag = latest_seq.saturating_sub(received),
            "maker heartbeat"
        );
        Ok(Response::new(proto::HeartbeatReply {
            server_time_unix_ms: unix_ms(),
            latest_seq,
        }))
    }

    async fn get_maker_state(
        &self,
        request: Request<proto::GetMakerStateRequest>,
    ) -> Result<Response<proto::GetMakerStateReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let request = request.into_inner();
        let after = request.page_token;
        if !after.is_empty() && after.len() != LINEAGE_ID_LEN {
            return Err(Status::invalid_argument("invalid page token"));
        }
        let size = match request.page_size {
            0 => DEFAULT_STATE_PAGE,
            size => size.min(MAX_STATE_PAGE),
        };
        let first = after.is_empty();
        let (page, summary) = self
            .pool
            .read_public(move |conn| {
                let page = maker_db::maker_orders_page_tx(conn, maker_id, &after, i64::from(size))?;
                let summary = if first {
                    Some((
                        maker_db::maker_cutoffs_tx(conn, maker_id)?,
                        postgres_db::get_last_fetched_block_tx(conn)?,
                        maker_db::latest_event_seq_tx(conn, maker_id)?,
                    ))
                } else {
                    None
                };
                Ok((page, summary))
            })
            .await
            .map_err(|_| Status::unavailable("cannot read maker state now; retry"))?;
        let next_page_token = match page.last() {
            Some(last) if page.len() == size as usize => last.lineage_id.clone(),
            _ => Vec::new(),
        };
        let mut reply = proto::GetMakerStateReply {
            orders: page
                .into_iter()
                .map(maker_order)
                .collect::<Result<_, _>>()?,
            next_page_token,
            server_time_unix_ms: unix_ms(),
            ..Default::default()
        };
        if let Some((cutoffs, height, latest)) = summary {
            reply.cutoffs = cutoffs
                .into_iter()
                .map(|(scope, cutoff)| cutoff_message(&scope, cutoff))
                .collect();
            reply.sync_height = u32::try_from(height).unwrap_or(u32::MAX);
            reply.latest_event_seq = latest;
        }
        Ok(Response::new(reply))
    }

    async fn get_command(
        &self,
        request: Request<proto::GetCommandRequest>,
    ) -> Result<Response<proto::CommandReply>, Status> {
        let (maker_id, _) = self.authenticate(&request).await?;
        let request_id = request.into_inner().request_id;
        let stored = self
            .pool
            .read_public(move |conn| maker_db::stored_result_tx(conn, maker_id, &request_id))
            .await
            .map_err(|_| Status::unavailable("cannot read command status now; retry"))?;
        match stored {
            Some(result) => Ok(Response::new(reply(result, true))),
            None => Err(Status::not_found("no command with this request ID")),
        }
    }
}

fn header(
    maker_id: MakerId,
    header: Option<proto::CommandHeader>,
) -> Result<CommandHeader, Status> {
    let header = header.ok_or_else(|| Status::invalid_argument("missing command header"))?;
    CommandHeader::new(maker_id, header.request_id, header.seq)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

impl proto::CancelAllRequest {
    /// Validate the whole request before it reaches the durable intake.
    fn verify(self, maker_id: MakerId) -> Result<(CommandHeader, CutoffScope), Status> {
        let scope = scope(&self)?;
        let header = header(maker_id, self.header)?;
        Ok((header, scope))
    }
}

impl proto::CancelOrderRequest {
    /// Validate the lineage key and command header before queueing the cancel.
    fn verify(self, maker_id: MakerId) -> Result<(CommandHeader, Vec<u8>), Status> {
        if self.lineage_id.len() != LINEAGE_ID_LEN {
            return Err(Status::invalid_argument(format!(
                "lineage ID must be {LINEAGE_ID_LEN} bytes"
            )));
        }
        Ok((header(maker_id, self.header)?, self.lineage_id))
    }
}

/// A faucet ID in its canonical 15-byte serialization.
fn faucet(bytes: &[u8]) -> Result<TokenId, Status> {
    AccountId::read_from_bytes(bytes)
        .ok()
        .filter(|id| id.to_bytes() == bytes)
        .ok_or_else(|| Status::invalid_argument("faucet ID is not a canonical account ID"))
}

/// Filters intersect; an ambiguous or contradictory filter is rejected, never
/// broadened to more orders.
fn scope(request: &proto::CancelAllRequest) -> Result<CutoffScope, Status> {
    match proto::OrderType::try_from(request.order_type) {
        Ok(proto::OrderType::Unspecified | proto::OrderType::Pswap) => {}
        Err(_) => return Err(Status::invalid_argument("unknown order type")),
    }
    let market = request
        .market
        .as_ref()
        .map(|market| Ok::<_, Status>((faucet(&market.faucet_a)?, faucet(&market.faucet_b)?)))
        .transpose()?;
    let direction = request
        .direction
        .as_ref()
        .map(|direction| {
            Ok::<_, Status>((
                faucet(&direction.offered_faucet)?,
                faucet(&direction.requested_faucet)?,
            ))
        })
        .transpose()?;
    for (a, b) in market.iter().chain(direction.iter()) {
        if a == b {
            return Err(Status::invalid_argument(
                "a pair needs two different faucets",
            ));
        }
    }
    Ok(match (market, direction) {
        (None, None) => CutoffScope::all(),
        (Some((a, b)), None) => CutoffScope::market(a, b),
        (Some((a, b)), Some((offered, requested))) => {
            if market_key(a, b) != market_key(offered, requested) {
                return Err(Status::invalid_argument(
                    "market and direction name different pairs",
                ));
            }
            CutoffScope::direction(offered, requested)
        }
        (None, Some((offered, requested))) => CutoffScope::direction(offered, requested),
    })
}

fn maker_order(view: maker_db::MakerOrderView) -> Result<proto::MakerOrder, Status> {
    let state = match (view.status.as_deref(), view.live) {
        (None, _) => proto::MakerOrderState::Pending,
        (Some("settling"), _) => proto::MakerOrderState::Settling,
        (Some("active"), true) => proto::MakerOrderState::Live,
        // Active below a cutoff, or stored Stopped.
        (Some(_), _) => proto::MakerOrderState::Stopped,
    };
    let corrupt = |_| Status::internal("stored maker order is unreadable");
    Ok(proto::MakerOrder {
        lineage_id: view.lineage_id,
        root_seq: u64::try_from(view.root_seq).map_err(corrupt)?,
        request_id: view.request_id,
        state: state.into(),
        note_id: view.note_id,
        depth: u32::try_from(view.depth.unwrap_or(0)).map_err(corrupt)?,
        cancelled: view.cancelled,
    })
}

/// A stored scope back on the wire: each key holds two faucet IDs.
fn cutoff_message(scope: &CutoffScope, cutoff: u64) -> proto::Cutoff {
    let halves = |key: &[u8]| {
        let (a, b) = key.split_at(key.len() / 2);
        (a.to_vec(), b.to_vec())
    };
    let market = (!scope.market.is_empty()).then(|| {
        let (faucet_a, faucet_b) = halves(&scope.market);
        proto::Market { faucet_a, faucet_b }
    });
    let direction = (!scope.direction.is_empty()).then(|| {
        let (offered_faucet, requested_faucet) = halves(&scope.direction);
        proto::Direction {
            offered_faucet,
            requested_faucet,
        }
    });
    proto::Cutoff {
        market,
        direction,
        cutoff,
    }
}

fn reply(result: CommandResult, replayed: bool) -> proto::CommandReply {
    let result = match result {
        CommandResult::Accepted => command_reply::Result::Accepted(proto::Accepted {}),
        CommandResult::AlreadyRegistered => {
            command_reply::Result::AlreadyRegistered(proto::AlreadyRegistered {})
        }
        CommandResult::Applied { cutoff, settling } => {
            command_reply::Result::Applied(proto::Applied { cutoff, settling })
        }
        CommandResult::Stopped { settling } => {
            command_reply::Result::Stopped(proto::Stopped { settling })
        }
    };
    proto::CommandReply {
        replayed,
        result: Some(result),
    }
}

fn intake_status(error: &IntakeError) -> Status {
    match error {
        IntakeError::Busy => Status::unavailable("durable acceptance unavailable: intake is full"),
        IntakeError::Stopped | IntakeError::OutcomeUnknown(_) => {
            Status::unavailable("durable acceptance unavailable; retry with the same request ID")
        }
    }
}

/// Run the gateway on its own OS thread and multi-thread runtime, like the
/// price API, so maker traffic cannot starve ingest or settlement. The intake
/// writer runs there too, on its own database session.
#[allow(clippy::too_many_arguments)]
pub fn spawn_gateway_thread(
    cfg: GatewayConfig,
    pool: DbPool,
    events: EventWake,
    rpc: Arc<dyn NodeRpcClient>,
    book_tx: mpsc::Sender<BookUpdate>,
    cancel: CancellationToken,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let handle = thread::Builder::new()
        .name("maker-gateway".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(anyhow!("maker gateway runtime: {error}")));
                    return;
                }
            };
            runtime.block_on(async move {
                let addr: SocketAddr = match format!("{}:{}", cfg.bind, cfg.port).parse() {
                    Ok(addr) => addr,
                    Err(error) => {
                        let _ = ready_tx.send(Err(anyhow!("maker gateway address: {error}")));
                        return;
                    }
                };
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(anyhow!("maker gateway bind {addr}: {error}")));
                        return;
                    }
                };
                let (intake, queues) = IntakeSender::new(cfg.cancel_queue, cfg.submit_queue);
                let mut workers = JoinSet::new();
                let intake_pool = pool.clone();
                let intake_book = book_tx.clone();
                let intake_cancel = cancel.clone();
                let round_submits = cfg.round_submits;
                workers.spawn(async move {
                    run_intake(
                        intake_pool.intake_session(),
                        queues,
                        intake_book,
                        round_submits,
                        intake_cancel,
                    )
                    .await;
                    "intake"
                });
                let watcher_pool = pool.clone();
                let watcher_cancel = cancel.clone();
                let watch_interval = cfg.watch_interval;
                workers.spawn(async move {
                    run_watcher(
                        Watcher::new(watcher_pool, RpcChain::new(rpc), book_tx),
                        watch_interval,
                        watcher_cancel,
                    )
                    .await;
                    "watcher"
                });
                let _ = ready_tx.send(Ok(()));
                tracing::info!(%addr, "maker gateway listening");
                let shutdown = cancel.clone();
                let service = MakerGatewayService::new(
                    pool,
                    intake,
                    cfg.markets,
                    events,
                    cfg.stream,
                    cancel.clone(),
                );
                let serving = tonic::transport::Server::builder()
                    .add_service(service.into_server())
                    .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
                        shutdown.cancelled().await
                    });
                supervise_gateway(serving, workers, cancel).await;
            });
        })?;
    Ok((handle, ready_rx))
}

/// Keep the server and both workers supervised until shutdown. Dropping a
/// JoinSet would abort tasks, including an intake whose blocking DB worker
/// may already have committed, so every worker is drained after cancellation.
async fn supervise_gateway<S, E>(
    serving: S,
    mut workers: JoinSet<&'static str>,
    cancel: CancellationToken,
) where
    S: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => {
            if let Err(error) = result {
                tracing::error!(%error, "maker gateway server failed");
            }
        }
        worker = workers.join_next() => {
            match worker {
                Some(Ok(name)) => tracing::error!(name, "maker gateway worker exited early"),
                Some(Err(error)) => tracing::error!(%error, "maker gateway worker panicked"),
                None => tracing::error!("maker gateway workers vanished"),
            }
            cancel.cancel();
            if let Err(error) = serving.await {
                tracing::error!(%error, "maker gateway server failed during shutdown");
            }
        }
    }
    cancel.cancel();
    while let Some(worker) = workers.join_next().await {
        if let Err(error) = worker {
            tracing::error!(%error, "maker gateway worker failed during shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::intake::tests::pswap_note;
    use super::super::proto::maker_gateway_client::MakerGatewayClient;
    use super::*;
    use crate::db::postgres_test::TestDb;
    use crate::maker::MakerUpdate;
    use crate::types::OrderKeys;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    };
    use tonic::transport::Channel;
    use tonic::Code;

    struct Gateway {
        client: MakerGatewayClient<Channel>,
        facts: mpsc::Receiver<BookUpdate>,
        key: String,
        key_id: i64,
        maker_id: MakerId,
        events: EventWake,
        db: TestDb,
        stop: CancellationToken,
    }

    const TEST_STREAM: StreamConfig = StreamConfig {
        buffer: 16,
        heartbeat: std::time::Duration::from_millis(100),
    };

    async fn gateway() -> Gateway {
        gateway_with_stream_limit(MAX_EVENT_STREAMS).await
    }

    async fn gateway_with_stream_limit(stream_limit: usize) -> Gateway {
        let db = TestDb::new().await.unwrap();
        let key = crate::maker::new_api_key();
        let hash = api_key_hash(&key);
        let (maker_id, key_id) = db
            .pool
            .write(move |conn| {
                let maker_id = maker_db::create_maker_tx(conn, "alpha")?.unwrap();
                Ok((
                    maker_id,
                    maker_db::issue_api_key_tx(conn, maker_id, &hash)?.unwrap(),
                ))
            })
            .await
            .unwrap();
        let (intake, queues) = IntakeSender::new(16, 16);
        let (facts_tx, facts) = mpsc::channel(16);
        let stop = CancellationToken::new();
        tokio::spawn(run_intake(
            db.pool.intake_session(),
            queues,
            facts_tx,
            500,
            stop.clone(),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let events = EventWake::default();
        let [x, y, _] = ids();
        let markets = [x, y].map(|id| AccountId::read_from_bytes(&id).unwrap());
        let mut service = MakerGatewayService::new(
            db.pool.clone(),
            intake,
            HashSet::from([market_key(markets[0], markets[1])]),
            events.clone(),
            TEST_STREAM,
            stop.clone(),
        );
        service.event_slots = Arc::new(Semaphore::new(stream_limit));
        let service = service.into_server();
        let shutdown = stop.clone();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
                    shutdown.cancelled().await
                }),
        );
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Gateway {
            client: MakerGatewayClient::new(channel),
            facts,
            key,
            key_id,
            maker_id,
            events,
            db,
            stop,
        }
    }

    fn signed<T>(key: &str, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {key}").parse().unwrap());
        request
    }

    fn command_header(request_id: &str, seq: u64) -> Option<proto::CommandHeader> {
        Some(proto::CommandHeader {
            request_id: request_id.into(),
            seq,
        })
    }

    fn submit(request_id: &str, seq: u64, note: &Note) -> proto::SubmitOrderRequest {
        proto::SubmitOrderRequest {
            header: command_header(request_id, seq),
            note: note.to_bytes(),
        }
    }

    fn ids() -> [Vec<u8>; 3] {
        [
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET,
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
        ]
        .map(|id| AccountId::try_from(id).unwrap().to_bytes())
    }

    fn market(a: &[u8], b: &[u8]) -> Option<proto::Market> {
        Some(proto::Market {
            faucet_a: a.to_vec(),
            faucet_b: b.to_vec(),
        })
    }

    fn direction(offered: &[u8], requested: &[u8]) -> Option<proto::Direction> {
        Some(proto::Direction {
            offered_faucet: offered.to_vec(),
            requested_faucet: requested.to_vec(),
        })
    }

    fn code<T: std::fmt::Debug>(result: Result<Response<T>, Status>) -> Code {
        result.unwrap_err().code()
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn commands_are_authenticated_validated_and_durable() {
        let Gateway {
            mut client,
            mut facts,
            key,
            key_id,
            db,
            stop,
            ..
        } = gateway().await;
        let order = pswap_note(1);

        // Without a valid key nothing is accepted.
        assert_eq!(
            code(client.submit_order(submit("s1", 1, &order)).await),
            Code::Unauthenticated
        );
        assert_eq!(
            code(
                client
                    .submit_order(signed("mmk_wrong", submit("s1", 1, &order)))
                    .await
            ),
            Code::Unauthenticated
        );

        // Submit, exact retry, conflicting reuse.
        let accepted = client
            .submit_order(signed(&key, submit("s1", 1, &order)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(accepted, reply(CommandResult::Accepted, false));
        assert!(matches!(
            facts.recv().await.unwrap().maker_updates.first(),
            Some(MakerUpdate::OrdersAttributed { .. })
        ));
        let retried = client
            .submit_order(signed(&key, submit("s1", 1, &order)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retried, reply(CommandResult::Accepted, true));
        assert_eq!(
            code(
                client
                    .submit_order(signed(&key, submit("s1", 1, &pswap_note(2))))
                    .await
            ),
            Code::AlreadyExists
        );

        // Malformed input is rejected before it reaches the intake.
        let unknown_pair = {
            use miden_protocol::asset::{AssetAmount, FungibleAsset};
            use miden_standards::note::{PswapNote, PswapNoteStorage};
            let creator = AccountId::try_from(
                miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
            )
            .unwrap();
            let [x, _, z] = ids().map(|id| AccountId::read_from_bytes(&id).unwrap());
            Note::from(
                PswapNote::builder()
                    .sender(creator)
                    .storage(
                        PswapNoteStorage::builder()
                            .min_requested_asset(FungibleAsset::new(z, 10).unwrap())
                            .min_fill_step(AssetAmount::new(1).unwrap())
                            .creator_account_id(creator)
                            .build(),
                    )
                    .serial_number(miden_protocol::Word::from([9_u32, 9, 9, 9]))
                    .note_type(miden_protocol::note::NoteType::Private)
                    .offered_asset(FungibleAsset::new(x, 10).unwrap())
                    .build()
                    .unwrap(),
            )
        };
        assert_eq!(
            code(
                client
                    .submit_order(signed(&key, submit("s8", 8, &unknown_pair)))
                    .await
            ),
            Code::InvalidArgument
        );
        let mut garbage = submit("s9", 9, &order);
        garbage.note.push(0);
        assert_eq!(
            code(client.submit_order(signed(&key, garbage)).await),
            Code::InvalidArgument
        );
        assert_eq!(
            code(
                client
                    .submit_order(signed(&key, submit("", 9, &order)))
                    .await
            ),
            Code::InvalidArgument
        );
        let [x, y, z] = ids();
        for (market, direction) in [
            (market(&x, &x), None),
            (market(&x, &y), direction(&x, &z)),
            (None, direction(&x[..14], &y)),
        ] {
            let request = proto::CancelAllRequest {
                header: command_header("bad", 9),
                order_type: 0,
                market,
                direction,
            };
            assert_eq!(
                code(client.cancel_all(signed(&key, request)).await),
                Code::InvalidArgument
            );
        }
        let unknown_type = proto::CancelAllRequest {
            header: command_header("bad", 9),
            order_type: 7,
            market: None,
            direction: None,
        };
        assert_eq!(
            code(client.cancel_all(signed(&key, unknown_type)).await),
            Code::InvalidArgument
        );

        // A scoped cancel-all and a targeted cancel.
        let applied = client
            .cancel_all(signed(
                &key,
                proto::CancelAllRequest {
                    header: command_header("c2", 2),
                    order_type: proto::OrderType::Pswap.into(),
                    market: market(&y, &x),
                    direction: direction(&x, &y),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            applied,
            reply(
                CommandResult::Applied {
                    cutoff: 2,
                    settling: 0
                },
                false
            )
        );
        assert_eq!(
            facts.recv().await.unwrap().maker_updates.first().cloned(),
            Some(MakerUpdate::CutoffRaised {
                maker_id: 1,
                scope: CutoffScope::direction(
                    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap(),
                    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
                ),
                cutoff: 2,
            })
        );
        let lineage_id = OrderKeys::from_note(&order).unwrap().lineage_id;
        assert_eq!(
            code(
                client
                    .cancel_order(signed(
                        &key,
                        proto::CancelOrderRequest {
                            header: command_header("x3", 3),
                            lineage_id: lineage_id[1..].to_vec(),
                        },
                    ))
                    .await
            ),
            Code::InvalidArgument
        );
        let stopped = client
            .cancel_order(signed(
                &key,
                proto::CancelOrderRequest {
                    header: command_header("x3", 3),
                    lineage_id,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            stopped,
            reply(CommandResult::Stopped { settling: 0 }, false)
        );

        // The current view: the submitted order is pending, and the
        // x→y cutoff covers it.
        let state = client
            .get_maker_state(signed(&key, proto::GetMakerStateRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(state.orders.len(), 1);
        assert_eq!(state.orders[0].state(), proto::MakerOrderState::Pending);
        assert!(state.orders[0].cancelled);
        assert_eq!(state.orders[0].note_id, order.id().to_bytes().to_vec());
        assert!(state.next_page_token.is_empty());
        assert_eq!(state.cutoffs.len(), 1);
        assert_eq!(state.cutoffs[0].cutoff, 2);
        assert_eq!(state.cutoffs[0].direction, direction(&x, &y));
        assert_eq!(
            code(
                client
                    .get_maker_state(signed(
                        &key,
                        proto::GetMakerStateRequest {
                            page_token: vec![1, 2, 3],
                            page_size: 0,
                        },
                    ))
                    .await
            ),
            Code::InvalidArgument
        );

        // A lost reply is recovered by request ID.
        let status = client
            .get_command(signed(
                &key,
                proto::GetCommandRequest {
                    request_id: "c2".into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            status,
            reply(
                CommandResult::Applied {
                    cutoff: 2,
                    settling: 0
                },
                true
            )
        );
        assert_eq!(
            code(
                client
                    .get_command(signed(
                        &key,
                        proto::GetCommandRequest {
                            request_id: "never".into(),
                        },
                    ))
                    .await
            ),
            Code::NotFound
        );

        // A revoked key stops working at once.
        db.pool
            .write(move |conn| maker_db::revoke_api_key_tx(conn, key_id))
            .await
            .unwrap();
        assert_eq!(
            code(
                client
                    .submit_order(signed(&key, submit("s4", 4, &pswap_note(4))))
                    .await
            ),
            Code::Unauthenticated
        );
        stop.cancel();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_gateway_thread_starts_and_stops_with_the_solver() {
        let db = TestDb::new().await.unwrap();
        let cancel = CancellationToken::new();
        let cfg = |port| GatewayConfig {
            bind: "127.0.0.1".into(),
            port,
            watch_interval: std::time::Duration::from_millis(50),
            markets: HashSet::new(),
            round_submits: 10,
            submit_queue: 4,
            cancel_queue: 4,
            stream: TEST_STREAM,
        };
        // No node answers here: watcher rounds fail and are retried.
        let rpc = || -> Arc<dyn NodeRpcClient> {
            let endpoint =
                miden_client::rpc::Endpoint::new("http".into(), "127.0.0.1".into(), Some(1));
            Arc::new(miden_client::rpc::GrpcClient::new(&endpoint, 100))
        };
        let (book_tx, _book_rx) = mpsc::channel(1);
        let (thread, ready) = spawn_gateway_thread(
            cfg(0),
            db.pool.clone(),
            EventWake::default(),
            rpc(),
            book_tx.clone(),
            cancel.clone(),
        )
        .unwrap();
        ready.await.unwrap().unwrap();
        cancel.cancel();
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();

        // A bind failure is reported at the readiness gate.
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let (thread, ready) = spawn_gateway_thread(
            cfg(port),
            db.pool.clone(),
            EventWake::default(),
            rpc(),
            book_tx,
            CancellationToken::new(),
        )
        .unwrap();
        assert!(ready.await.unwrap().is_err());
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();
    }

    async fn next(
        stream: &mut tonic::Streaming<proto::StreamMessage>,
    ) -> (Option<proto::MakerEvent>, Option<proto::KeepAlive>) {
        match stream.message().await.unwrap().unwrap().message.unwrap() {
            proto::stream_message::Message::Event(event) => (Some(event), None),
            proto::stream_message::Message::KeepAlive(beat) => (None, Some(beat)),
            proto::stream_message::Message::ReplayComplete(complete) => {
                panic!("unexpected ReplayComplete {complete:?}")
            }
        }
    }

    fn order_status(note_id: u8) -> proto::EventBody {
        proto::EventBody {
            kind: Some(proto::event_body::Kind::OrderStatus(proto::OrderStatus {
                note_id: vec![note_id],
                depth: 0,
                state: proto::OrderState::Live.into(),
                reason: String::new(),
            })),
        }
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn the_event_stream_replays_then_follows_with_heartbeats() {
        let Gateway {
            mut client,
            key,
            maker_id,
            events,
            db,
            stop,
            ..
        } = gateway().await;
        let append = |count: u8| {
            db.pool.write(move |conn| {
                for note in 0..count {
                    super::super::append_event_tx(conn, maker_id, Some(&[7]), &order_status(note))?;
                }
                Ok(())
            })
        };
        append(3).await.unwrap();

        let mut stream = client
            .stream_events(signed(&key, proto::StreamEventsRequest { after_seq: 1 }))
            .await
            .unwrap()
            .into_inner();
        // Replay after the cursor, in order, with typed bodies.
        for seq in [2, 3] {
            let event = next(&mut stream).await.0.unwrap();
            assert_eq!(event.seq, seq);
            assert_eq!(event.lineage_id, vec![7]);
            assert_eq!(event.body, Some(order_status(seq as u8 - 1)));
            assert!(!event.event_id.is_empty() && event.created_at_unix_ms > 0);
        }
        match stream.message().await.unwrap().unwrap().message.unwrap() {
            proto::stream_message::Message::ReplayComplete(complete) => {
                assert_eq!(complete.through_seq, 3)
            }
            other => panic!("expected ReplayComplete, got {other:?}"),
        }
        // A new event arrives once its commit wakes the stream.
        append(1).await.unwrap();
        events.notify();
        assert_eq!(next(&mut stream).await.0.unwrap().seq, 4);
        // The heartbeat carries the cursor.
        let beat = loop {
            if let (_, Some(beat)) = next(&mut stream).await {
                break beat;
            }
        };
        assert_eq!(beat.cursor, 4);
        assert!(beat.server_time_unix_ms > 0);

        // The maker reports its progress and sees how far behind it is.
        let reply = client
            .heartbeat(signed(
                &key,
                proto::HeartbeatRequest {
                    received_through_seq: 2,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.latest_seq, 4);
        assert!(reply.server_time_unix_ms > 0);
        stop.cancel();
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL"]
    async fn event_subscription_limit_releases_on_disconnect() {
        let Gateway {
            mut client,
            key,
            stop,
            db: _db,
            ..
        } = gateway_with_stream_limit(1).await;
        let request = || signed(&key, proto::StreamEventsRequest { after_seq: 0 });
        let mut first = client.stream_events(request()).await.unwrap().into_inner();
        assert!(matches!(
            first.message().await.unwrap().unwrap().message,
            Some(proto::stream_message::Message::ReplayComplete(_))
        ));
        assert_eq!(
            client.stream_events(request()).await.unwrap_err().code(),
            Code::ResourceExhausted
        );
        drop(first);
        let mut released = false;
        for _ in 0..50 {
            match client.stream_events(request()).await {
                Ok(stream) => {
                    drop(stream);
                    released = true;
                    break;
                }
                Err(status) if status.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected stream error: {status}"),
            }
        }
        assert!(released, "disconnect must release the subscription slot");
        stop.cancel();
    }

    #[tokio::test]
    async fn gateway_supervision_cancels_and_drains_after_worker_exit_or_panic() {
        for panic_worker in [false, true] {
            let cancel = CancellationToken::new();
            let mut workers = JoinSet::new();
            workers.spawn(async move {
                if panic_worker {
                    panic!("test worker failure");
                }
                "intake"
            });
            let waiting = cancel.clone();
            workers.spawn(async move {
                waiting.cancelled().await;
                "watcher"
            });
            let server_cancel = cancel.clone();
            let serving = async move {
                server_cancel.cancelled().await;
                Ok::<(), &'static str>(())
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                supervise_gateway(serving, workers, cancel.clone()),
            )
            .await
            .expect("supervision must shut the server and drain the watcher");
            assert!(cancel.is_cancelled());
        }
    }
}

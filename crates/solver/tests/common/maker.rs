//! Maker gateway helpers: register a maker, send commands to the gateway over
//! gRPC with its API key, and read its event feed.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use solver::gateway::proto::{self, maker_gateway_client::MakerGatewayClient};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;
use tonic::Streaming;

/// A port nothing listens on right now, for the gateway to bind.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("no free local port")
}

/// Create a maker and an API key for it directly in the database, as an
/// operator onboarding a maker would. Returns the key.
pub async fn register_maker(db_url: &str, name: &str) -> Result<String> {
    let url = db_url.to_owned();
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut conn = solver::db::postgres_migrations::connect(&url)?;
        let maker = solver::db::maker_db::create_maker_tx(&mut conn, &name)?
            .ok_or_else(|| anyhow!("maker {name} already exists"))?;
        let key = format!("test-key-{name}");
        solver::db::maker_db::issue_api_key_tx(
            &mut conn,
            maker,
            &solver::maker::api_key_hash(&key),
        )?
        .ok_or_else(|| anyhow!("maker {name} vanished"))?;
        Ok(key)
    })
    .await?
}

/// One maker's connection to the gateway. Each command gets the next
/// sequence number.
pub struct MakerClient {
    client: MakerGatewayClient<Channel>,
    auth: MetadataValue<Ascii>,
    seq: u64,
}

impl MakerClient {
    /// Connect, retrying until the gateway listens (it starts with the
    /// solver). Gives up at once if the solver stopped.
    pub async fn connect(
        port: u16,
        api_key: &str,
        solver_stopped: impl Fn() -> bool,
    ) -> Result<Self> {
        let url = format!("http://127.0.0.1:{port}");
        let started = Instant::now();
        let channel = loop {
            match Channel::from_shared(url.clone())?.connect().await {
                Ok(channel) => break channel,
                Err(_) if solver_stopped() => {
                    bail!("the solver stopped before its gateway listened")
                }
                Err(_) if started.elapsed() < Duration::from_secs(120) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(error) => return Err(error).context("maker gateway never listened"),
            }
        };
        Ok(Self {
            client: MakerGatewayClient::new(channel),
            auth: format!("Bearer {api_key}").parse()?,
            seq: 0,
        })
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.auth.clone());
        request
    }

    fn next_header(&mut self, kind: &str) -> proto::CommandHeader {
        self.seq += 1;
        proto::CommandHeader {
            request_id: format!("{kind}-{}", self.seq),
            seq: self.seq,
        }
    }

    /// Submit `note` as an order; returns the gateway's reply.
    pub async fn submit(&mut self, note: &Note) -> Result<proto::CommandReply> {
        let message = proto::SubmitOrderRequest {
            header: Some(self.next_header("submit")),
            note: note.to_bytes(),
            expires_at_unix_ms: None,
        };
        let request = self.request(message);
        Ok(self.client.submit_order(request).await?.into_inner())
    }

    /// The maker's event feed from the first event.
    pub async fn events(&mut self) -> Result<EventFeed> {
        let request = self.request(proto::StreamEventsRequest { after_seq: 0 });
        Ok(EventFeed {
            stream: self.client.stream_events(request).await?.into_inner(),
            unread: VecDeque::new(),
        })
    }
}

/// A maker's event feed. An event read while waiting for another one stays
/// here until a later wait matches it or passes it.
pub struct EventFeed {
    stream: Streaming<proto::StreamMessage>,
    unread: VecDeque<proto::MakerEvent>,
}

impl EventFeed {
    /// Read until an event matches `want`, and return it with every event
    /// before it, in order. `None` when none matched within `max_wait`: the
    /// events read meanwhile are kept for the next wait.
    pub async fn next_match<F>(
        &mut self,
        max_wait: Duration,
        mut want: F,
    ) -> Result<Option<Vec<proto::MakerEvent>>>
    where
        F: FnMut(&proto::event_body::Kind) -> bool,
    {
        let deadline = Instant::now() + max_wait;
        let mut checked = 0;
        loop {
            while let Some(event) = self.unread.get(checked) {
                checked += 1;
                let kind = event.body.as_ref().and_then(|body| body.kind.as_ref());
                if kind.is_some_and(&mut want) {
                    return Ok(Some(self.unread.drain(..checked).collect()));
                }
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            let Ok(message) = tokio::time::timeout(left, self.stream.message()).await else {
                return Ok(None);
            };
            let message =
                message?.ok_or_else(|| anyhow!("event stream ended; saw {:?}", self.unread))?;
            if let Some(proto::stream_message::Message::Event(event)) = message.message {
                self.unread.push_back(event);
            }
        }
    }
}

impl EventFeed {
    /// Every event still unread, and every event that arrives within
    /// `window`, in order.
    pub async fn drain(&mut self, window: Duration) -> Result<Vec<proto::MakerEvent>> {
        self.next_match(window, |_| false).await?;
        Ok(self.unread.drain(..).collect())
    }
}

/// Read the feed until an event matches `want`, or fail after `max_wait`.
/// Every event seen on the way is returned too, in order, for assertions.
pub async fn wait_for_event<F>(
    feed: &mut EventFeed,
    max_wait: Duration,
    want: F,
) -> Result<Vec<proto::MakerEvent>>
where
    F: FnMut(&proto::event_body::Kind) -> bool,
{
    match feed.next_match(max_wait, want).await? {
        Some(seen) => Ok(seen),
        None => bail!("event not seen within {max_wait:?}; saw {:?}", feed.unread),
    }
}

/// Fail if an event matching `unwanted` is in the feed or arrives within
/// `window`. Other events stay in the feed.
pub async fn expect_no_event<F>(feed: &mut EventFeed, window: Duration, unwanted: F) -> Result<()>
where
    F: FnMut(&proto::event_body::Kind) -> bool,
{
    match feed.next_match(window, unwanted).await? {
        Some(seen) => bail!("unexpected event: {:?}", seen.last()),
        None => Ok(()),
    }
}

/// `kind` is an `OrderStatus` with `state` for `note`.
pub fn is_order_status(
    kind: &proto::event_body::Kind,
    note: &Note,
    state: proto::OrderState,
) -> bool {
    matches!(
        kind,
        proto::event_body::Kind::OrderStatus(status)
            if status.note_id == note.id().to_bytes() && status.state == i32::from(state)
    )
}

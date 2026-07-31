//! Async websocket client for the solver's RFQ router — the turnkey integration
//! path for an external DEX ("filler").
//!
//! A [`FillerClient`] owns one authenticated connection. A background task
//! reads server frames into an event queue and writes your outbound messages to
//! the socket, so the two directions never block each other. Messages are miden
//! **binary** frames (see [`crate::protocol`]).
//!
//! ```ignore
//! use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};
//! use miden_protocol::asset::FungibleAsset;
//!
//! let mut client = FillerClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//! client.subscribe(vec![PairSpec { offered: imiden, requested: iusdt }])?;
//! // standing quote: give up to 1_000_000 iMIDEN for 2_000_000 iUSDT (rate + size); refresh before TTL
//! client.quote(FungibleAsset::new(imiden, 1_000_000)?, FungibleAsset::new(iusdt, 2_000_000)?, None)?;
//!
//! while let Some(ev) = client.next_event().await {
//!     match ev {
//!         FillerEvent::Handover(h) => { /* consume h.note on-chain at h.fill_price */ }
//!         FillerEvent::Disconnected => break,
//!         _ => {}
//!     }
//! }
//! ```

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{ClientMsg, PairSpec, PriceRatio, ServerMsg};

/// A note handed over by the solver for the filler to consume on-chain, at the
/// matched price. `note` is a decoded miden [`Note`]; read its swap terms with
/// [`miden_standards::note::PswapNote::try_from`] and build consume args with
/// [`crate::consume::consume_args`].
#[derive(Debug, Clone)]
pub struct Handover {
    /// The PSWAP note to consume.
    pub note: Note,
    /// Requested-token base units to fill.
    pub fill_amount: u64,
    /// The price the match used (your own quote, echoed) — `num/den` =
    /// requested-per-offered. Fill at this rate.
    pub fill_price: PriceRatio,
}

/// An event surfaced from the router connection. The first event after a
/// successful [`FillerClient::connect`] is always [`FillerEvent::AuthOk`]; the
/// last is always [`FillerEvent::Disconnected`].
#[derive(Debug, Clone)]
pub enum FillerEvent {
    /// Handshake accepted — the connection is live.
    AuthOk,
    /// Reserved: the router's request for quotes on these pairs (not currently
    /// emitted — the live flow is quote-driven).
    Ask { pairs: Vec<PairSpec> },
    /// A note to fill. Consume `note` on-chain at `fill_price`.
    Handover(Handover),
    /// A structured error from the router (e.g. a malformed quote was rejected).
    Error { code: String, msg: String },
    /// The socket closed (by either side) or errored. Terminal: the event
    /// stream ends after this.
    Disconnected,
}

impl From<ServerMsg> for FillerEvent {
    fn from(m: ServerMsg) -> Self {
        match m {
            ServerMsg::AuthOk => FillerEvent::AuthOk,
            ServerMsg::Ask { pairs } => FillerEvent::Ask { pairs },
            ServerMsg::Handover { note, fill_amount, fill_price } => {
                FillerEvent::Handover(Handover { note, fill_amount, fill_price })
            }
            ServerMsg::Error { code, msg } => FillerEvent::Error { code, msg },
        }
    }
}

/// The send half of a connection. Cheaply cloneable, so it can be moved into
/// other tasks (e.g. a timer that refreshes quotes) while the main task drains
/// events. Sends are non-blocking: they queue onto the connection task.
#[derive(Clone)]
pub struct FillerSender {
    tx: mpsc::UnboundedSender<ClientMsg>,
}

impl FillerSender {
    /// Send a raw protocol message. Errors only if the connection is gone.
    pub fn send(&self, msg: ClientMsg) -> Result<()> {
        self.tx.send(msg).map_err(|_| anyhow!("router connection closed"))
    }

    /// Declare the pairs this filler can fill. Quotes still gate which orders are
    /// actually offered, per pair.
    pub fn subscribe(&self, pairs: Vec<PairSpec>) -> Result<()> {
        self.send(ClientMsg::Subscribe { pairs })
    }

    /// Post (or refresh) a standing quote: give up to `offered` for `requested`.
    /// The two assets carry both the rate and the max size (like a PSWAP note);
    /// their faucet ids imply the pair. `valid_for_ms` optionally shortens
    /// validity below the server's quote TTL.
    pub fn quote(
        &self,
        offered: FungibleAsset,
        requested: FungibleAsset,
        valid_for_ms: Option<u64>,
    ) -> Result<()> {
        if u64::from(offered.amount()) == 0 || u64::from(requested.amount()) == 0 {
            bail!("quote amounts must be > 0");
        }
        self.send(ClientMsg::Quote { offered, requested, valid_for_ms })
    }
}

/// An authenticated RFQ connection to the solver's router.
pub struct FillerClient {
    sender: FillerSender,
    events: mpsc::UnboundedReceiver<FillerEvent>,
}

impl FillerClient {
    /// Connect to the router at `url` (e.g. `ws://host:port/v1/rfq`) and
    /// authenticate with `token` via the `Authorization: Bearer` header. Returns
    /// once the socket is established; a wrong token fails the upgrade and errors
    /// here. The first [`next_event`](Self::next_event) is [`FillerEvent::AuthOk`].
    pub async fn connect(url: &str, token: &str) -> Result<Self> {
        let mut req = url
            .into_client_request()
            .with_context(|| format!("invalid router url: {url}"))?;
        req.headers_mut().insert(
            AUTHORIZATION,
            format!("Bearer {token}").parse().context("token has invalid header characters")?,
        );

        let (socket, _resp) = tokio_tungstenite::connect_async(req)
            .await
            .context("router websocket connect/upgrade failed (check url and token)")?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<ClientMsg>();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel::<FillerEvent>();
        tokio::spawn(run_connection(socket, out_rx, ev_tx));

        Ok(Self { sender: FillerSender { tx: out_tx }, events: ev_rx })
    }

    /// A cloneable handle for sending from other tasks.
    pub fn sender(&self) -> FillerSender {
        self.sender.clone()
    }

    /// Await the next event, or `None` once the connection is fully closed
    /// (after a final [`FillerEvent::Disconnected`]).
    pub async fn next_event(&mut self) -> Option<FillerEvent> {
        self.events.recv().await
    }

    // ── Convenience pass-throughs to the sender ──────────────────────────────

    /// See [`FillerSender::subscribe`].
    pub fn subscribe(&self, pairs: Vec<PairSpec>) -> Result<()> {
        self.sender.subscribe(pairs)
    }

    /// See [`FillerSender::quote`].
    pub fn quote(
        &self,
        offered: FungibleAsset,
        requested: FungibleAsset,
        valid_for_ms: Option<u64>,
    ) -> Result<()> {
        self.sender.quote(offered, requested, valid_for_ms)
    }
}

/// Background task: writes queued outbound messages to the socket (miden-binary
/// frames) and reads server frames into the event queue. Ends on socket
/// close/error, emitting a final [`FillerEvent::Disconnected`].
async fn run_connection(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut out_rx: mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: mpsc::UnboundedSender<FillerEvent>,
) {
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            // Outbound: miden-serialize to a binary frame and write.
            out = out_rx.recv() => match out {
                Some(msg) => {
                    if sink.send(Message::Binary(msg.to_bytes().into())).await.is_err() {
                        break;
                    }
                }
                None => break, // all senders dropped → close
            },
            // Inbound: miden-deserialize a binary frame and forward as an event.
            item = stream.next() => match item {
                Some(Ok(Message::Binary(b))) => {
                    match ServerMsg::read_from_bytes(&b) {
                        Ok(m) => {
                            if ev_tx.send(FillerEvent::from(m)).is_err() {
                                break; // receiver dropped
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "drop undecodable server frame"),
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {} // ignore text/ping/pong
            },
        }
    }
    let _ = ev_tx.send(FillerEvent::Disconnected);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_msg_maps_to_event() {
        // Variants without a miden Note need no fixtures.
        assert!(matches!(FillerEvent::from(ServerMsg::AuthOk), FillerEvent::AuthOk));
        assert!(matches!(
            FillerEvent::from(ServerMsg::Ask { pairs: vec![] }),
            FillerEvent::Ask { .. }
        ));
        match FillerEvent::from(ServerMsg::Error { code: "x".into(), msg: "y".into() }) {
            FillerEvent::Error { code, msg } => {
                assert_eq!(code, "x");
                assert_eq!(msg, "y");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_queues_a_client_msg() {
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = FillerSender { tx };
        s.subscribe(vec![]).unwrap();
        assert!(matches!(rx.try_recv().unwrap(), ClientMsg::Subscribe { .. }));
    }

    #[tokio::test]
    async fn send_after_close_errors() {
        let (tx, rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = FillerSender { tx };
        drop(rx); // connection task gone
        assert!(s.subscribe(vec![]).is_err());
    }
}

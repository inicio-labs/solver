//! Async websocket client for the solver's RFQ router — the turnkey integration
//! path for an external DEX ("filler").
//!
//! A [`FillerClient`] owns one authenticated connection. A background pump task
//! reads server frames into an event queue and writes your outbound messages to
//! the socket, so the two directions never block each other:
//!
//! ```ignore
//! use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};
//!
//! let mut client = FillerClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//! let pair = PairSpec { offered: imiden_hex, requested: iusdt_hex };
//! client.subscribe(vec![pair.clone()])?;            // pairs I can fill
//! client.quote(&pair, "2.00", 1_000_000, None)?;    // standing quote; refresh before TTL
//!
//! while let Some(ev) = client.next_event().await {
//!     match ev {
//!         FillerEvent::Handover(h) => { /* decode + self-consume on-chain */ }
//!         FillerEvent::Disconnected => break,
//!         _ => {}
//!     }
//! }
//! ```

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{parse_decimal_price, ClientMsg, PairSpec, ServerMsg};

/// A note handed over by the solver for the filler to consume on-chain.
///
/// `note_hex` is the hex-encoded serialized PSWAP note. With the `consume`
/// feature, [`crate::consume::decode_note`] turns it back into a miden note and
/// [`crate::consume::PswapTerms`] reads its swap terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handover {
    pub note_id: String,
    pub fill_amount: u64,
    pub note_hex: String,
    /// The price the solver requires this note be filled at (your own quoted
    /// price, echoed back) — requested-per-offered, per whole token, as a decimal
    /// string. Fill the note at this price, independent of its intrinsic rate.
    pub fill_price: String,
}

/// An event surfaced from the router connection. The first event after a
/// successful [`FillerClient::connect`] is always [`FillerEvent::AuthOk`]; the
/// last is always [`FillerEvent::Disconnected`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillerEvent {
    /// Handshake accepted — the connection is live.
    AuthOk,
    /// The router's standing request: pairs it wants quotes for.
    Ask { pairs: Vec<PairSpec> },
    /// A note to fill. Decode `note_hex` and self-consume on-chain.
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
            ServerMsg::Handover { note_id, fill_amount, note_hex, fill_price } => {
                FillerEvent::Handover(Handover { note_id, fill_amount, note_hex, fill_price })
            }
            ServerMsg::Error { code, msg } => FillerEvent::Error { code, msg },
        }
    }
}

/// The send half of a connection. Cheaply cloneable, so it can be moved into
/// other tasks (e.g. a timer that refreshes quotes) while the main task drains
/// events. Sends are non-blocking: they queue onto the pump task.
#[derive(Clone)]
pub struct FillerSender {
    tx: mpsc::UnboundedSender<ClientMsg>,
}

impl FillerSender {
    /// Send a raw protocol message. Errors only if the connection is gone.
    pub fn send(&self, msg: ClientMsg) -> Result<()> {
        self.tx.send(msg).map_err(|_| anyhow!("router connection closed"))
    }

    /// Declare the pairs this filler can fill. Quotes still gate which orders
    /// are actually offered, per pair.
    pub fn subscribe(&self, pairs: Vec<PairSpec>) -> Result<()> {
        self.send(ClientMsg::Subscribe { pairs })
    }

    /// Post (or refresh) a standing quote for one pair.
    ///
    /// `price` is requested-token per offered-token, **per whole token**, as a
    /// decimal string (e.g. `"2.05"`); it is validated locally before sending,
    /// so a malformed price errors here instead of round-tripping to a server
    /// `Error`. `quantity` is the max requested-token quantity (base units) the
    /// filler will take. `valid_for_ms` optionally shortens validity below the
    /// server's quote TTL.
    pub fn quote(
        &self,
        pair: &PairSpec,
        price: &str,
        quantity: u64,
        valid_for_ms: Option<u64>,
    ) -> Result<()> {
        if parse_decimal_price(price).is_none() {
            bail!("invalid price {price:?}: expected a non-negative decimal like \"2.05\"");
        }
        if quantity == 0 {
            bail!("quantity must be > 0");
        }
        self.send(ClientMsg::Quote {
            pair: pair.clone(),
            price: price.to_string(),
            quantity,
            valid_for_ms,
        })
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
        let bearer = format!("Bearer {token}");
        req.headers_mut().insert(
            "Authorization",
            bearer.parse().context("token has invalid header characters")?,
        );

        let (socket, _resp) = tokio_tungstenite::connect_async(req)
            .await
            .context("router websocket connect/upgrade failed (check url and token)")?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<ClientMsg>();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel::<FillerEvent>();
        tokio::spawn(pump(socket, out_rx, ev_tx));

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

    /// Like [`next_event`](Self::next_event) but gives up after `timeout`,
    /// returning `Ok(None)` on timeout (the connection stays open).
    pub async fn next_event_timeout(&mut self, timeout: Duration) -> Result<Option<FillerEvent>> {
        match tokio::time::timeout(timeout, self.events.recv()).await {
            Ok(ev) => Ok(ev),
            Err(_) => Ok(None),
        }
    }

    // ── Convenience pass-throughs to the sender ──────────────────────────────

    /// See [`FillerSender::subscribe`].
    pub fn subscribe(&self, pairs: Vec<PairSpec>) -> Result<()> {
        self.sender.subscribe(pairs)
    }

    /// See [`FillerSender::quote`].
    pub fn quote(
        &self,
        pair: &PairSpec,
        price: &str,
        quantity: u64,
        valid_for_ms: Option<u64>,
    ) -> Result<()> {
        self.sender.quote(pair, price, quantity, valid_for_ms)
    }
}

/// Background task: writes queued outbound messages to the socket and reads
/// server frames into the event queue. Ends on socket close/error, emitting a
/// final [`FillerEvent::Disconnected`].
async fn pump(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut out_rx: mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: mpsc::UnboundedSender<FillerEvent>,
) {
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            // Outbound: serialize and write. A serialization failure is a bug,
            // not a wire condition — skip the message rather than tear down.
            out = out_rx.recv() => match out {
                Some(msg) => {
                    let txt = match serde_json::to_string(&msg) {
                        Ok(t) => t,
                        Err(e) => { tracing::error!(error = %e, "serialize ClientMsg"); continue; }
                    };
                    if sink.send(Message::Text(txt.into())).await.is_err() {
                        break;
                    }
                }
                None => break, // all senders dropped → close
            },
            // Inbound: decode and forward as an event.
            item = stream.next() => match item {
                Some(Ok(Message::Text(t))) => {
                    match serde_json::from_str::<ServerMsg>(t.as_str()) {
                        Ok(m) => {
                            if ev_tx.send(FillerEvent::from(m)).is_err() {
                                break; // receiver dropped
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "drop undecodable server frame"),
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {} // ignore binary/ping/pong
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
        assert_eq!(FillerEvent::from(ServerMsg::AuthOk), FillerEvent::AuthOk);
        let h = ServerMsg::Handover {
            note_id: "0x1".into(),
            fill_amount: 9,
            note_hex: "ab".into(),
            fill_price: "2.05".into(),
        };
        assert_eq!(
            FillerEvent::from(h),
            FillerEvent::Handover(Handover {
                note_id: "0x1".into(),
                fill_amount: 9,
                note_hex: "ab".into(),
                fill_price: "2.05".into(),
            })
        );
    }

    #[tokio::test]
    async fn sender_validates_before_sending() {
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = FillerSender { tx };
        let pair = PairSpec { offered: "0xaa".into(), requested: "0xbb".into() };

        // Bad price / zero quantity rejected locally — nothing is queued.
        assert!(s.quote(&pair, "abc", 10, None).is_err());
        assert!(s.quote(&pair, "2.0", 0, None).is_err());
        assert!(rx.try_recv().is_err());

        // Valid quote is queued as a ClientMsg::Quote.
        s.quote(&pair, "2.05", 1000, Some(5000)).unwrap();
        match rx.try_recv().unwrap() {
            ClientMsg::Quote { price, quantity, valid_for_ms, .. } => {
                assert_eq!(price, "2.05");
                assert_eq!(quantity, 1000);
                assert_eq!(valid_for_ms, Some(5000));
            }
            other => panic!("expected quote, got {other:?}"),
        }

        // Subscribe is queued as ClientMsg::Subscribe.
        s.subscribe(vec![pair]).unwrap();
        assert!(matches!(rx.try_recv().unwrap(), ClientMsg::Subscribe { .. }));
    }

    #[tokio::test]
    async fn send_after_close_errors() {
        let (tx, rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = FillerSender { tx };
        drop(rx); // pump gone
        assert!(s.subscribe(vec![]).is_err());
    }
}

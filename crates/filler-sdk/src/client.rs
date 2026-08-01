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

use std::time::Duration;

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

    /// The minimal push integration: subscribe to `pairs` and keep a **fresh**
    /// standing quote live for each, hands-free.
    ///
    /// The SDK calls `price(pair)` on every `refresh` tick to get the current
    /// `(offered_amount, requested_amount)` (base units, on the pair's
    /// `offered`/`requested` faucets) and sends it. So your quotes never expire
    /// (keepalive) **and** never go stale-by-omission — the SDK always pushes
    /// whatever `price` returns *now*. Return `None` from `price` to skip a pair
    /// this tick. Set `refresh` to ~half the router's quote TTL.
    ///
    /// Provide a pricing fn here and handle handovers from
    /// [`next_event`](Self::next_event) — that's the whole integration. The
    /// quoting loop runs until the connection drops; keep the returned
    /// [`QuoteTask`] (or drop it to detach) and call [`QuoteTask::abort`] to stop
    /// early. `price` must be cheap/non-blocking — it runs inline on the task.
    pub fn serve_quotes<F>(
        &self,
        pairs: Vec<PairSpec>,
        refresh: Duration,
        price: F,
    ) -> Result<QuoteTask>
    where
        F: Fn(&PairSpec) -> Option<(u64, u64)> + Send + 'static,
    {
        self.subscribe(pairs.clone())?;
        let handle = tokio::spawn(quote_loop(self.sender(), pairs, refresh, price));
        Ok(QuoteTask(handle))
    }
}

/// Handle to the background quoting loop started by
/// [`FillerClient::serve_quotes`]. The loop runs until the connection drops.
/// Dropping this handle detaches the loop (it keeps quoting); call
/// [`abort`](Self::abort) to stop it explicitly.
pub struct QuoteTask(tokio::task::JoinHandle<()>);

impl QuoteTask {
    /// Stop the quoting loop.
    pub fn abort(&self) {
        self.0.abort();
    }
}

/// Re-send a fresh quote for every pair on each `refresh` tick, pricing each via
/// `price`. Ends when the connection drops (a send error).
async fn quote_loop<F>(sender: FillerSender, pairs: Vec<PairSpec>, refresh: Duration, price: F)
where
    F: Fn(&PairSpec) -> Option<(u64, u64)>,
{
    loop {
        for pair in &pairs {
            let Some((offered_amount, requested_amount)) = price(pair) else { continue };
            match (
                FungibleAsset::new(pair.offered, offered_amount),
                FungibleAsset::new(pair.requested, requested_amount),
            ) {
                (Ok(offered), Ok(requested)) => {
                    if sender.quote(offered, requested, None).is_err() {
                        return; // connection gone → stop
                    }
                }
                _ => tracing::warn!("serve_quotes: invalid quote amounts for a pair; skipping"),
            }
        }
        tokio::time::sleep(refresh).await;
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

    #[tokio::test]
    async fn quote_loop_pushes_fresh_and_refreshes() {
        use miden_protocol::account::AccountId;
        // Real testnet faucet ids (valid fungible faucets — no test feature needed).
        let a = AccountId::from_hex("0x4a03c1843860c9b17582c021d563ae").unwrap();
        let b = AccountId::from_hex("0x2458e5446128e6b150b75b8ebd9ce1").unwrap();
        let pair = PairSpec { offered: a, requested: b };

        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let handle = tokio::spawn(quote_loop(
            FillerSender { tx },
            vec![pair],
            Duration::from_millis(20),
            |_p| Some((100, 200)), // priced live each tick
        ));

        // Quotes immediately, with the priced amounts on the pair's faucets.
        tokio::time::sleep(Duration::from_millis(5)).await;
        match rx.try_recv().unwrap() {
            ClientMsg::Quote { offered, requested, .. } => {
                assert_eq!(u64::from(offered.amount()), 100);
                assert_eq!(u64::from(requested.amount()), 200);
                assert_eq!(offered.faucet_id(), a);
                assert_eq!(requested.faucet_id(), b);
            }
            other => panic!("expected Quote, got {other:?}"),
        }
        // And keeps refreshing on the next tick.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(matches!(rx.try_recv().unwrap(), ClientMsg::Quote { .. }));
        handle.abort();
    }
}

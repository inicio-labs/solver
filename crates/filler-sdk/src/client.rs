//! Async websocket client for the solver's RFQ router — the turnkey integration
//! path for an external DEX (a **liquidity provider**, "LP").
//!
//! An [`LpClient`] owns one authenticated, **auto-reconnecting** connection. A
//! background *reader* task decodes server frames into an event queue while a
//! *writer* loop drains your outbound messages to the socket — two independent
//! loops, so a stalled write can never block reads. If the socket drops, the SDK
//! reconnects with backoff and re-authenticates (you see
//! [`LpEvent::Reconnecting`] / [`LpEvent::Reconnected`]); re-post your quotes on
//! `Reconnected` — the quote is the registration. Messages are miden **binary**
//! frames (see [`crate::protocol`]).
//!
//! ```ignore
//! use pswap_filler_sdk::{LpClient, LpEvent, PairSpec};
//! use miden_protocol::asset::FungibleAsset;
//!
//! let mut client = LpClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//! // standing quote: give up to 1_000_000 iMIDEN for 2_000_000 iUSDT (rate + size); refresh before TTL.
//! // The quote's faucet ids imply the pair — no separate subscribe step.
//! client.quote(FungibleAsset::new(imiden, 1_000_000)?, FungibleAsset::new(iusdt, 2_000_000)?, None)?;
//!
//! while let Some(ev) = client.next_event().await {
//!     match ev {
//!         LpEvent::Handover(h) => { /* consume h.note on-chain at h.fill_price */ }
//!         LpEvent::Reconnecting { error, .. } => tracing::warn!(%error, "router link lost; retrying"),
//!         LpEvent::Disconnected { reason } => { tracing::error!(%reason, "gave up"); break }
//!         _ => {}
//!     }
//! }
//! ```

use std::fmt;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use futures_util::stream::{SplitStream, StreamExt};
use futures_util::SinkExt;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::protocol::{ClientMsg, PairSpec, PriceRatio, ServerMsg};

/// The concrete websocket stream type (TCP, optionally TLS-wrapped).
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Backoff bounds for auto-reconnect: first retry after `MIN`, doubling up to
/// `MAX`, reset to `MIN` after each successful reconnect.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// A typed error from the router link. `Clone` so it can ride on events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LpError {
    /// The router rejected the `Bearer` token at the websocket upgrade (HTTP
    /// 401). Terminal — retrying with the same token won't help.
    AuthRejected,
    /// A transport-level failure (connect/upgrade/read/write on the socket).
    Transport(String),
    /// The router closed the connection.
    Closed(String),
    /// An application-level error the router reported (e.g. a malformed quote was
    /// rejected). Non-fatal: the connection stays up.
    Protocol { code: String, msg: String },
}

impl fmt::Display for LpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LpError::AuthRejected => write!(f, "router rejected authentication (check the token)"),
            LpError::Transport(e) => write!(f, "transport error: {e}"),
            LpError::Closed(r) => write!(f, "connection closed: {r}"),
            LpError::Protocol { code, msg } => write!(f, "router error [{code}]: {msg}"),
        }
    }
}

impl std::error::Error for LpError {}

/// A note handed over by the solver for the LP to consume on-chain, at the
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
/// successful [`LpClient::connect`] is always [`LpEvent::AuthOk`]. Transient
/// drops surface as [`LpEvent::Reconnecting`] then [`LpEvent::Reconnected`] (the
/// SDK reconnects for you). The stream ends (`next_event` → `None`) when the
/// client is dropped; a terminal [`LpEvent::Disconnected`] is emitted only when
/// the SDK gives up (e.g. the token is rejected).
// The `Handover` variant carries a typed miden `Note` by value — deliberate (the
// binary protocol's whole point). Events flow one-at-a-time through a channel,
// never stored in bulk, so the variant-size spread costs nothing here.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum LpEvent {
    /// Handshake accepted — the connection is live.
    AuthOk,
    /// Reserved: the router's request for quotes on these pairs (not currently
    /// emitted — the live flow is quote-driven).
    Ask { pairs: Vec<PairSpec> },
    /// A note to fill. Consume `note` on-chain at `fill_price`.
    Handover(Handover),
    /// The router rejected a message we sent (non-fatal — the link stays up).
    Error(LpError),
    /// The link dropped and the SDK is retrying. `error` says why; `attempt`
    /// counts from 1. Followed by [`LpEvent::Reconnected`] once re-established.
    Reconnecting { attempt: u32, error: LpError },
    /// The link was re-established. Re-post your quotes here if you quote
    /// manually (the quote is the registration); [`LpClient::serve_quotes`]
    /// resumes on its own.
    Reconnected,
    /// Terminal: the SDK stopped trying (e.g. authentication was rejected). The
    /// event stream ends after this.
    Disconnected { reason: String },
}

impl From<ServerMsg> for LpEvent {
    fn from(m: ServerMsg) -> Self {
        match m {
            ServerMsg::AuthOk => LpEvent::AuthOk,
            ServerMsg::Ask { pairs } => LpEvent::Ask { pairs },
            ServerMsg::Handover { note, fill_amount, fill_price } => {
                LpEvent::Handover(Handover { note, fill_amount, fill_price })
            }
            ServerMsg::Error { code, msg } => LpEvent::Error(LpError::Protocol { code, msg }),
        }
    }
}

/// The send half of a connection. Cheaply cloneable, so it can be moved into
/// other tasks (e.g. a timer that refreshes quotes) while the main task drains
/// events. Sends are non-blocking: they queue onto the connection task and are
/// written on whichever socket is currently live (including after a reconnect).
#[derive(Clone)]
pub struct LpSender {
    tx: mpsc::UnboundedSender<ClientMsg>,
}

impl LpSender {
    /// Send a raw protocol message. Errors only if the client has been dropped.
    pub fn send(&self, msg: ClientMsg) -> Result<()> {
        self.tx.send(msg).map_err(|_| anyhow!("router connection closed"))
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
        self.send(ClientMsg::Quote {
            offered,
            requested,
            valid_for_ms,
        })
    }
}

/// An authenticated, auto-reconnecting RFQ connection to the solver's router.
pub struct LpClient {
    sender: LpSender,
    events: mpsc::UnboundedReceiver<LpEvent>,
}

impl LpClient {
    /// Connect to the router at `url` (e.g. `ws://host:port/v1/rfq`) and
    /// authenticate with `token` via the `Authorization: Bearer` header. Returns
    /// once the socket is established; a wrong token fails the upgrade and errors
    /// here. The first [`next_event`](Self::next_event) is [`LpEvent::AuthOk`].
    ///
    /// After this, the connection is self-healing: if it drops, the SDK
    /// reconnects with backoff and re-authenticates. Re-post your quotes on
    /// [`LpEvent::Reconnected`] (the quote is the registration);
    /// [`serve_quotes`](Self::serve_quotes) resumes on its own.
    pub async fn connect(url: &str, token: &str) -> Result<Self> {
        // Fail fast on a bad url/token; hand the live socket to the supervisor.
        let socket = connect_and_auth(url, token).await.map_err(|e| anyhow!(e))?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<ClientMsg>();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel::<LpEvent>();
        tokio::spawn(supervise(
            socket,
            url.to_string(),
            token.to_string(),
            out_rx,
            ev_tx,
        ));

        Ok(Self {
            sender: LpSender { tx: out_tx },
            events: ev_rx,
        })
    }

    /// A cloneable handle for sending from other tasks.
    pub fn sender(&self) -> LpSender {
        self.sender.clone()
    }

    /// Await the next event, or `None` once the client is dropped / the SDK has
    /// stopped reconnecting.
    pub async fn next_event(&mut self) -> Option<LpEvent> {
        self.events.recv().await
    }

    // ── Convenience pass-through to the sender ───────────────────────────────

    /// See [`LpSender::quote`].
    pub fn quote(
        &self,
        offered: FungibleAsset,
        requested: FungibleAsset,
        valid_for_ms: Option<u64>,
    ) -> Result<()> {
        self.sender.quote(offered, requested, valid_for_ms)
    }

    /// The minimal push integration: keep a **fresh** standing quote live for
    /// each pair, hands-free. There is no separate subscribe step — **the quote
    /// is the registration** (its faucet ids imply the pair).
    ///
    /// The SDK calls `price(pair)` every `refresh` tick for the current
    /// `(offered_amount, requested_amount)` (base units on the pair's
    /// `offered`/`requested` faucets) and sends it, so your quotes never expire
    /// (keepalive) **and** never go stale-by-omission. Return `None` from `price`
    /// to skip a pair this tick. Set `refresh` to ~half the router's quote TTL.
    /// Across a reconnect the loop keeps ticking, so quoting resumes on its own.
    ///
    /// Provide a pricing fn here and handle handovers from
    /// [`next_event`](Self::next_event) — that's the whole integration. The
    /// quoting loop runs until the client is dropped; keep the returned
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
        let handle = tokio::spawn(quote_loop(self.sender(), pairs, refresh, price));
        Ok(QuoteTask(handle))
    }
}

/// Handle to the background quoting loop started by
/// [`LpClient::serve_quotes`]. The loop runs until the client is dropped.
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
/// `price`. Ends when the client is dropped (a send error).
async fn quote_loop<F>(sender: LpSender, pairs: Vec<PairSpec>, refresh: Duration, price: F)
where
    F: Fn(&PairSpec) -> Option<(u64, u64)>,
{
    loop {
        for pair in &pairs {
            let Some((offered_amount, requested_amount)) = price(pair) else {
                continue;
            };
            match (
                FungibleAsset::new(pair.offered, offered_amount),
                FungibleAsset::new(pair.requested, requested_amount),
            ) {
                (Ok(offered), Ok(requested)) => {
                    if sender.quote(offered, requested, None).is_err() {
                        return; // client dropped → stop
                    }
                }
                _ => tracing::warn!("serve_quotes: invalid quote amounts for a pair; skipping"),
            }
        }
        tokio::time::sleep(refresh).await;
    }
}

// ── Connection supervisor ───────────────────────────────────────────────────

/// Establish a socket to `url` and attach the `Authorization: Bearer` header.
/// A rejected token surfaces as [`LpError::AuthRejected`] (HTTP 401 at upgrade).
async fn connect_and_auth(url: &str, token: &str) -> Result<WsStream, LpError> {
    let mut req = url
        .into_client_request()
        .map_err(|e| LpError::Transport(format!("invalid router url {url}: {e}")))?;
    req.headers_mut().insert(
        AUTHORIZATION,
        format!("Bearer {token}")
            .parse()
            .map_err(|_| LpError::Transport("token has invalid header characters".into()))?,
    );

    match tokio_tungstenite::connect_async(req).await {
        Ok((socket, _resp)) => Ok(socket),
        Err(WsError::Http(resp)) if resp.status().as_u16() == 401 => Err(LpError::AuthRejected),
        Err(e) => Err(LpError::Transport(e.to_string())),
    }
}

/// Why a single connection ended.
enum ConnEnd {
    /// The client was dropped (all senders gone) — stop for good.
    ClientGone,
    /// The socket dropped — reconnect.
    Dropped(LpError),
}

/// Owns the reconnect lifecycle: run a connection, and on an unexpected drop,
/// reconnect (backoff) → re-auth → emit Reconnected → resume. Ends
/// when the client is dropped or the token is permanently rejected.
async fn supervise(
    mut socket: WsStream,
    url: String,
    token: String,
    mut out_rx: mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: mpsc::UnboundedSender<LpEvent>,
) {
    let _ = ev_tx.send(LpEvent::AuthOk); // initial handshake acknowledged

    loop {
        let end = run_connection(socket, &mut out_rx, &ev_tx).await;
        let drop_err = match end {
            ConnEnd::ClientGone => return,
            ConnEnd::Dropped(e) => e,
        };
        tracing::warn!(error = %drop_err, "router link dropped; reconnecting");

        // Reconnect with capped exponential backoff.
        let mut attempt = 1u32;
        let mut delay = RECONNECT_MIN;
        socket = loop {
            if ev_tx.is_closed() {
                return; // client dropped while we were backing off
            }
            let _ = ev_tx.send(LpEvent::Reconnecting {
                attempt,
                error: drop_err.clone(),
            });
            tokio::time::sleep(delay).await;
            match connect_and_auth(&url, &token).await {
                Ok(s) => break s,
                Err(LpError::AuthRejected) => {
                    tracing::error!("router rejected the token on reconnect; giving up");
                    let _ = ev_tx.send(LpEvent::Disconnected {
                        reason: LpError::AuthRejected.to_string(),
                    });
                    return;
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "reconnect attempt failed");
                    attempt += 1;
                    delay = (delay * 2).min(RECONNECT_MAX);
                }
            }
        };

        // Live again — tell the caller so it can re-post quotes.
        tracing::info!("router link re-established");
        let _ = ev_tx.send(LpEvent::Reconnected);
    }
}

/// Drive one live socket: spawn the *reader* (frames → events) as its own task,
/// and run the *writer* (outbound queue → socket) as a separate loop here, so the
/// two directions never block each other. Returns why the connection ended.
async fn run_connection(
    socket: WsStream,
    out_rx: &mut mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: &mpsc::UnboundedSender<LpEvent>,
) -> ConnEnd {
    let (mut sink, stream) = socket.split();

    // Inbound loop lives in its own task; it reports its terminal reason here.
    let (done_tx, mut done_rx) = oneshot::channel::<LpError>();
    let reader = tokio::spawn(reader_loop(stream, ev_tx.clone(), done_tx));

    // Outbound loop: drain the durable queue onto this socket. A `biased` select
    // checks the reader's death signal first so we tear down promptly.
    let end = loop {
        tokio::select! {
            biased;
            reason = &mut done_rx => {
                break ConnEnd::Dropped(
                    reason.unwrap_or_else(|_| LpError::Closed("reader ended".into())),
                );
            }
            msg = out_rx.recv() => match msg {
                Some(msg) => {
                    if sink.send(Message::Binary(msg.to_bytes().into())).await.is_err() {
                        break ConnEnd::Dropped(LpError::Transport("write failed".into()));
                    }
                }
                None => break ConnEnd::ClientGone, // all senders dropped
            },
        }
    };

    reader.abort();
    end
}

/// Inbound loop: decode server frames and forward them as events. Runs as its
/// own task so a stalled write can't block reads. Reports why it stopped via
/// `done_tx` (used by the supervisor to trigger a reconnect).
async fn reader_loop(
    mut stream: SplitStream<WsStream>,
    ev_tx: mpsc::UnboundedSender<LpEvent>,
    done_tx: oneshot::Sender<LpError>,
) {
    let reason = loop {
        match stream.next().await {
            Some(Ok(Message::Binary(b))) => match ServerMsg::read_from_bytes(&b) {
                // The AuthOk frame is an app-level ack; the supervisor already
                // synthesizes AuthOk / Reconnected, so don't double-report it.
                Ok(ServerMsg::AuthOk) => {}
                Ok(m) => {
                    if ev_tx.send(LpEvent::from(m)).is_err() {
                        break LpError::Closed("client dropped".into()); // receiver gone
                    }
                }
                Err(e) => tracing::warn!(error = %e, "drop undecodable server frame"),
            },
            Some(Ok(Message::Close(_))) => break LpError::Closed("router closed the link".into()),
            Some(Ok(_)) => {} // ignore text/ping/pong
            Some(Err(e)) => break LpError::Transport(e.to_string()),
            None => break LpError::Closed("stream ended".into()),
        }
    };
    let _ = done_tx.send(reason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_msg_maps_to_event() {
        // Variants without a miden Note need no fixtures.
        assert!(matches!(LpEvent::from(ServerMsg::AuthOk), LpEvent::AuthOk));
        assert!(matches!(
            LpEvent::from(ServerMsg::Ask { pairs: vec![] }),
            LpEvent::Ask { .. }
        ));
        match LpEvent::from(ServerMsg::Error {
            code: "x".into(),
            msg: "y".into(),
        }) {
            LpEvent::Error(LpError::Protocol { code, msg }) => {
                assert_eq!(code, "x");
                assert_eq!(msg, "y");
            }
            other => panic!("expected Error(Protocol), got {other:?}"),
        }
    }

    #[test]
    fn lp_error_displays_readably() {
        assert_eq!(
            LpError::Protocol {
                code: "bad_quote".into(),
                msg: "nope".into()
            }
            .to_string(),
            "router error [bad_quote]: nope"
        );
        assert!(LpError::AuthRejected.to_string().contains("token"));
    }

    fn sample_faucets() -> (miden_protocol::account::AccountId, miden_protocol::account::AccountId) {
        use miden_protocol::account::AccountId;
        // Real testnet faucet ids (valid fungible faucets — no test feature needed).
        (
            AccountId::from_hex("0x4a03c1843860c9b17582c021d563ae").unwrap(),
            AccountId::from_hex("0x2458e5446128e6b150b75b8ebd9ce1").unwrap(),
        )
    }

    #[tokio::test]
    async fn quote_queues_a_client_msg() {
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = LpSender { tx };
        let (a, b) = sample_faucets();
        s.quote(FungibleAsset::new(a, 1_000).unwrap(), FungibleAsset::new(b, 2_000).unwrap(), None)
            .unwrap();
        assert!(matches!(rx.try_recv().unwrap(), ClientMsg::Quote { .. }));
    }

    #[tokio::test]
    async fn send_after_close_errors() {
        let (tx, rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = LpSender { tx };
        drop(rx); // connection task gone
        let (a, b) = sample_faucets();
        assert!(s
            .quote(FungibleAsset::new(a, 1).unwrap(), FungibleAsset::new(b, 1).unwrap(), None)
            .is_err());
    }

    #[tokio::test]
    async fn quote_loop_pushes_fresh_and_refreshes() {
        use miden_protocol::account::AccountId;
        // Real testnet faucet ids (valid fungible faucets — no test feature needed).
        let a = AccountId::from_hex("0x4a03c1843860c9b17582c021d563ae").unwrap();
        let b = AccountId::from_hex("0x2458e5446128e6b150b75b8ebd9ce1").unwrap();
        let pair = PairSpec {
            offered: a,
            requested: b,
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let handle = tokio::spawn(quote_loop(
            LpSender { tx },
            vec![pair],
            Duration::from_millis(20),
            |_p| Some((100, 200)), // priced live each tick
        ));

        // Quotes immediately, with the priced amounts on the pair's faucets.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let ClientMsg::Quote { offered, requested, .. } = rx.try_recv().unwrap();
        assert_eq!(u64::from(offered.amount()), 100);
        assert_eq!(u64::from(requested.amount()), 200);
        assert_eq!(offered.faucet_id(), a);
        assert_eq!(requested.faucet_id(), b);
        // And keeps refreshing on the next tick.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(matches!(rx.try_recv().unwrap(), ClientMsg::Quote { .. }));
        handle.abort();
    }
}

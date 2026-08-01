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
//! use pswap_lp_sdk::{LpClient, LpEvent, PairSpec};
//! use miden_protocol::asset::FungibleAsset;
//!
//! let mut client = LpClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//! // standing quote: give up to 1_000_000 iMIDEN for 2_000_000 iUSDT (rate + size); refresh before TTL.
//! // The quote's faucet ids imply the pair — no separate subscribe step.
//! client.quote(FungibleAsset::new(imiden, 1_000_000)?, FungibleAsset::new(iusdt, 2_000_000)?, None)?;
//!
//! while let Some(ev) = client.next_event().await {
//!     match ev {
//!         LpEvent::Handover(h) => { /* consume h.note on-chain (it enforces its rate) */ }
//!         LpEvent::Reconnecting { attempt } => tracing::warn!(attempt, "router link lost; retrying"),
//!         LpEvent::Disconnected { reason } => { tracing::error!(%reason, "gave up"); break }
//!         _ => {}
//!     }
//! }
//! ```

use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::protocol::{ClientMsg, PairSpec, ServerMsg};

/// The concrete websocket stream type (TCP, optionally TLS-wrapped).
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Backoff bounds for auto-reconnect: first retry after `MIN`, doubling up to
/// `MAX`. The backoff resets only once a connection has stayed up for
/// `STABLE_UPTIME` — so a router that accepts then instantly drops backs off
/// instead of being hammered (and flooding the event channel) forever.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const STABLE_UPTIME: Duration = Duration::from_secs(10);

/// A typed error from the router link. `Clone` so it can ride on events.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LpError {
    /// The router rejected the `Bearer` token at the websocket upgrade (HTTP
    /// 401). Terminal — retrying with the same token won't help.
    #[error("router rejected authentication (check the token)")]
    AuthRejected,
    /// A transport-level failure (connect/upgrade/read/write on the socket).
    #[error("transport error: {0}")]
    Transport(String),
    /// The client has been dropped (the connection task is gone). Returned by
    /// [`LpSender::send`]/[`quote`](LpSender::quote).
    #[error("connection closed: {0}")]
    Closed(String),
    /// A quote rejected locally before it hit the wire (e.g. a zero amount). The
    /// caller's mistake, not the router's.
    #[error("invalid quote: {0}")]
    InvalidQuote(String),
    /// An application-level error the router reported (e.g. a malformed quote was
    /// rejected). Non-fatal: the connection stays up.
    #[error("router error [{code}]: {msg}")]
    Protocol { code: String, msg: String },
}

/// A note handed over by the solver for the LP to consume on-chain. `note` is a
/// decoded miden [`Note`]; read its swap terms with
/// [`miden_standards::note::PswapNote::try_from`] and build consume args with
/// [`crate::consume::consume_args`]. The note enforces its own on-chain rate, so
/// `note` + `fill_amount` fully specify the fill.
#[derive(Debug, Clone)]
pub struct Handover {
    /// The PSWAP note to consume.
    pub note: Note,
    /// Requested-token base units to fill.
    pub fill_amount: u64,
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
    /// A note to fill. Consume `note` on-chain (it enforces its own rate).
    Handover(Handover),
    /// The router rejected a message we sent (non-fatal — the link stays up).
    Error(LpError),
    /// The link dropped and the SDK is retrying (`attempt` counts from 1; the
    /// reason is logged). Followed by [`LpEvent::Reconnected`] once re-established.
    Reconnecting { attempt: u32 },
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
            ServerMsg::Handover { note, fill_amount } => {
                LpEvent::Handover(Handover { note, fill_amount })
            }
            ServerMsg::Error { code, msg } => LpEvent::Error(LpError::Protocol { code, msg }),
        }
    }
}

/// The send half of a connection. Cheaply cloneable, so it can be moved into
/// other tasks (e.g. a timer that refreshes quotes) while the main task drains
/// events. Sends are non-blocking: they queue onto the connection task.
///
/// `Ok` means *enqueued*, not *delivered*. Anything still queued when the link
/// drops is **discarded on reconnect** (it would otherwise put a stale price on
/// the wire) — so after an [`LpEvent::Reconnected`], re-post your quote. (The
/// hands-free [`LpClient::serve_quotes`] loop already does this for you.)
#[derive(Clone)]
pub struct LpSender {
    tx: mpsc::UnboundedSender<ClientMsg>,
}

impl LpSender {
    /// Send a raw protocol message. Fails with [`LpError::Closed`] only once the
    /// client has been dropped (the connection task is gone).
    pub fn send(&self, msg: ClientMsg) -> Result<(), LpError> {
        self.tx
            .send(msg)
            .map_err(|_| LpError::Closed("client dropped; connection task gone".into()))
    }

    /// Post (or refresh) a standing quote: **give up to `offered` to receive
    /// `requested`** (both from your side — offered = what you give, requested =
    /// what you want). The two assets carry the rate (their ratio) and the max
    /// size (like a PSWAP note); their faucet ids imply the pair. `valid_for_ms`
    /// optionally shortens validity below the server's quote TTL.
    ///
    /// Returns [`LpError::InvalidQuote`] for a zero amount (logged at `warn`), or
    /// [`LpError::Closed`] if the client has been dropped.
    pub fn quote(
        &self,
        offered: FungibleAsset,
        requested: FungibleAsset,
        valid_for_ms: Option<u64>,
    ) -> Result<(), LpError> {
        if u64::from(offered.amount()) == 0 || u64::from(requested.amount()) == 0 {
            let err = LpError::InvalidQuote("quote amounts must be > 0".into());
            tracing::warn!(error = %err, "rejecting quote before send");
            return Err(err);
        }
        self.send(ClientMsg::Quote { offered, requested, valid_for_ms })
    }

    /// True once the client has been dropped (the connection task is gone). Lets
    /// a background loop notice the shutdown even when it isn't currently sending.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
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
    /// once the socket is established. Errors with [`LpError::AuthRejected`] on a
    /// bad token (HTTP 401 at the upgrade) or [`LpError::Transport`] on a bad url /
    /// connect failure. The first [`next_event`](Self::next_event) is
    /// [`LpEvent::AuthOk`].
    ///
    /// After this, the connection is self-healing: if it drops, the SDK
    /// reconnects with backoff and re-authenticates. Re-post your quotes on
    /// [`LpEvent::Reconnected`] (the quote is the registration);
    /// [`serve_quotes`](Self::serve_quotes) resumes on its own.
    pub async fn connect(url: &str, token: &str) -> Result<Self, LpError> {
        // Fail fast on a bad url/token; hand the live socket to the supervisor.
        let socket = connect_and_auth(url, token).await.inspect_err(|e| {
            tracing::warn!(error = %e, "initial connect failed");
        })?;

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
    ) -> Result<(), LpError> {
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
    pub fn serve_quotes<F>(&self, pairs: Vec<PairSpec>, refresh: Duration, price: F) -> QuoteTask
    where
        F: Fn(&PairSpec) -> Option<(u64, u64)> + Send + 'static,
    {
        QuoteTask(tokio::spawn(quote_loop(self.sender(), pairs, refresh, price)))
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
        // Stop promptly on client-drop even if we never send this tick (a price
        // fn that keeps returning None/zero would otherwise loop forever).
        if sender.is_closed() {
            return;
        }
        for pair in &pairs {
            let Some((offered_amount, requested_amount)) = price(pair) else {
                continue;
            };
            match (
                FungibleAsset::new(pair.offered, offered_amount),
                FungibleAsset::new(pair.requested, requested_amount),
            ) {
                (Ok(offered), Ok(requested)) => {
                    // Stop only when the client is gone; a rejected quote (e.g. a
                    // zero amount) is already logged by `quote` — skip and carry on.
                    if let Err(LpError::Closed(_)) = sender.quote(offered, requested, None) {
                        return;
                    }
                }
                _ => tracing::warn!("serve_quotes: could not build a FungibleAsset for a pair; skipping"),
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
    /// The client was dropped (all senders / the event receiver gone) — stop.
    ClientGone,
    /// The socket dropped — reconnect.
    Dropped,
}

/// Owns the reconnect lifecycle: run a connection, and on an unexpected drop,
/// reconnect (backoff) → re-auth → emit `Reconnected` → resume. Ends when the
/// client is dropped or the token is permanently rejected.
async fn supervise(
    mut socket: WsStream,
    url: String,
    token: String,
    mut out_rx: mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: mpsc::UnboundedSender<LpEvent>,
) {
    let _ = ev_tx.send(LpEvent::AuthOk); // initial handshake acknowledged

    // Backoff state persists across drops; it resets only after a connection that
    // stayed up for `STABLE_UPTIME` (see the constant's note).
    let mut attempt = 0u32;
    let mut delay = RECONNECT_MIN;
    loop {
        let started = std::time::Instant::now();
        if let ConnEnd::ClientGone = run_connection(socket, &mut out_rx, &ev_tx).await {
            return;
        }
        // The reader/sender loops already logged *why* the link dropped.
        tracing::warn!("router link dropped; reconnecting");
        if started.elapsed() >= STABLE_UPTIME {
            attempt = 0;
            delay = RECONNECT_MIN;
        }

        socket = loop {
            if ev_tx.is_closed() {
                return; // client dropped while we were backing off
            }
            attempt = attempt.saturating_add(1);
            let _ = ev_tx.send(LpEvent::Reconnecting { attempt });
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RECONNECT_MAX); // grow for the next attempt
            match connect_and_auth(&url, &token).await {
                Ok(s) => break s,
                Err(LpError::AuthRejected) => {
                    let _ = ev_tx.send(LpEvent::Disconnected {
                        reason: LpError::AuthRejected.to_string(),
                    });
                    return; // bad token — retrying won't help
                }
                Err(e) => tracing::warn!(attempt, error = %e, "reconnect attempt failed"),
            }
        };

        // Discard whatever queued during the outage — those quotes were priced
        // before the drop, so flushing them now would put a stale price on the
        // wire. Fresh quotes arrive on the next tick / a manual re-post.
        while out_rx.try_recv().is_ok() {}

        tracing::info!("router link re-established");
        let _ = ev_tx.send(LpEvent::Reconnected);
    }
}

/// Drive one live socket. The two directions run as mirror loops that never block
/// each other — [`reader_loop`] (frames → events) in its own task, [`sender_loop`]
/// (outbound queue → socket) here. Whichever ends first ends the connection.
async fn run_connection(
    socket: WsStream,
    out_rx: &mut mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: &mpsc::UnboundedSender<LpEvent>,
) -> ConnEnd {
    let (sink, stream) = socket.split();
    let mut reader = tokio::spawn(reader_loop(stream, ev_tx.clone()));

    let end = tokio::select! {
        // The read side ended (logged its reason). If the client is gone, stop;
        // otherwise the socket is dead → reconnect.
        _ = &mut reader => {
            if ev_tx.is_closed() { ConnEnd::ClientGone } else { ConnEnd::Dropped }
        }
        end = sender_loop(sink, out_rx, ev_tx) => end,
    };
    reader.abort();
    end
}

/// Inbound loop: decode server frames and forward them as events. Mirror of
/// [`sender_loop`]. Logs *why* it stopped (that's enough — the supervisor only
/// needs to know the link ended).
async fn reader_loop(mut stream: SplitStream<WsStream>, ev_tx: mpsc::UnboundedSender<LpEvent>) {
    loop {
        match stream.next().await {
            Some(Ok(Message::Binary(b))) => match ServerMsg::read_from_bytes(&b) {
                // The AuthOk frame is an app-level ack; the supervisor already
                // synthesizes AuthOk / Reconnected, so don't double-report it.
                Ok(ServerMsg::AuthOk) => {}
                Ok(m) => {
                    if ev_tx.send(LpEvent::from(m)).is_err() {
                        return; // client gone
                    }
                }
                Err(e) => tracing::warn!(error = %e, "drop undecodable server frame"),
            },
            Some(Ok(Message::Close(_))) => return tracing::warn!("router closed the link"),
            Some(Ok(_)) => {} // ignore text/ping/pong
            Some(Err(e)) => return tracing::warn!(error = %e, "read error; dropping link"),
            None => return tracing::warn!("router stream ended"),
        }
    }
}

/// Outbound loop: write queued messages to the socket. Mirror of [`reader_loop`].
/// Returns why it stopped.
async fn sender_loop(
    mut sink: SplitSink<WsStream, Message>,
    out_rx: &mut mpsc::UnboundedReceiver<ClientMsg>,
    ev_tx: &mpsc::UnboundedSender<LpEvent>,
) -> ConnEnd {
    loop {
        tokio::select! {
            // The client (its event receiver) was dropped — tear the link down
            // even if a cloned `LpSender` is still alive somewhere.
            _ = ev_tx.closed() => return ConnEnd::ClientGone,
            msg = out_rx.recv() => match msg {
                Some(msg) => {
                    if let Err(e) = sink.send(Message::Binary(msg.to_bytes().into())).await {
                        tracing::warn!(error = %e, "write error; dropping link");
                        return ConnEnd::Dropped;
                    }
                }
                None => return ConnEnd::ClientGone, // all senders dropped
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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
        assert_eq!(
            LpError::InvalidQuote("quote amounts must be > 0".into()).to_string(),
            "invalid quote: quote amounts must be > 0"
        );
        assert_eq!(LpError::Closed("bye".into()).to_string(), "connection closed: bye");
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
    async fn send_after_close_errors_with_typed_closed() {
        let (tx, rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = LpSender { tx };
        drop(rx); // connection task gone
        let (a, b) = sample_faucets();
        let err = s
            .quote(FungibleAsset::new(a, 1).unwrap(), FungibleAsset::new(b, 1).unwrap(), None)
            .unwrap_err();
        assert!(matches!(err, LpError::Closed(_)), "expected Closed, got {err:?}");
    }

    #[tokio::test]
    async fn quote_rejects_zero_amount_with_typed_error() {
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMsg>();
        let s = LpSender { tx };
        let (a, b) = sample_faucets();
        let err = s
            .quote(FungibleAsset::new(a, 0).unwrap(), FungibleAsset::new(b, 5).unwrap(), None)
            .unwrap_err();
        assert!(matches!(err, LpError::InvalidQuote(_)), "expected InvalidQuote, got {err:?}");
        // A rejected quote never reaches the wire.
        assert!(rx.try_recv().is_err());
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

    #[tokio::test]
    async fn quote_loop_stops_on_client_drop_even_when_never_sending() {
        // A price fn that always returns None never sends — so the loop must
        // notice the client-drop via is_closed(), not via a send error, or it
        // would leak forever (detached task).
        let (tx, rx) = mpsc::unbounded_channel::<ClientMsg>();
        let (a, b) = sample_faucets();
        let handle = tokio::spawn(quote_loop(
            LpSender { tx },
            vec![PairSpec { offered: a, requested: b }],
            Duration::from_millis(10),
            |_p| None,
        ));
        drop(rx); // client gone
        let joined = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "quote_loop must terminate after the client is dropped");
    }

    // ── Resilience: an in-process mock router the SDK connects to ────────────

    struct MockRouter {
        url: String,
        received: Arc<Mutex<Vec<ClientMsg>>>,
        connections: Arc<AtomicUsize>,
    }

    /// Spin up a real (in-process) router. The first `drop_first` accepted
    /// connections send `AuthOk` then drop the socket — an unstable link.
    /// Every later connection stays open and records the quotes it receives.
    async fn mock_router(drop_first: usize) -> MockRouter {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/rfq", listener.local_addr().unwrap());
        let received = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));

        let recv = received.clone();
        let conns = connections.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let idx = conns.fetch_add(1, Ordering::SeqCst);
                let recv = recv.clone();
                tokio::spawn(async move {
                    let Ok(ws) = accept_async(stream).await else { return };
                    let (mut sink, mut stream) = ws.split();
                    let _ = sink.send(Message::Binary(ServerMsg::AuthOk.to_bytes().into())).await;
                    if idx < drop_first {
                        return; // drop the socket → the client's reader sees the link fail
                    }
                    while let Some(Ok(msg)) = stream.next().await {
                        if let Message::Binary(b) = msg {
                            if let Ok(cm) = ClientMsg::read_from_bytes(&b) {
                                recv.lock().unwrap().push(cm);
                            }
                        }
                    }
                });
            }
        });

        MockRouter { url, received, connections }
    }

    fn quote_offer_amounts(msgs: &[ClientMsg]) -> Vec<u64> {
        msgs.iter()
            .map(|m| match m {
                ClientMsg::Quote { offered, .. } => u64::from(offered.amount()),
            })
            .collect()
    }

    /// Q1 (reader `break` on a dead stream), Q2 (no panic), Q3 (reconnect), and
    /// the typed-error/logging ask: a dropped link surfaces `Reconnecting` with a
    /// **typed** `LpError`, then `Reconnected`, and the router sees a 2nd socket.
    #[tokio::test(flavor = "multi_thread")]
    async fn reconnects_after_a_drop() {
        let router = mock_router(1).await; // conn #0 drops; conn #1+ is stable
        let mut client = LpClient::connect(&router.url, "tok").await.unwrap();

        assert!(matches!(client.next_event().await, Some(LpEvent::AuthOk)));

        let mut reconnecting = false;
        let mut reconnected = false;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_secs(3), client.next_event()).await {
                Ok(Some(LpEvent::Reconnecting { attempt })) => {
                    assert!(attempt >= 1);
                    reconnecting = true;
                }
                Ok(Some(LpEvent::Reconnected)) => {
                    reconnected = true;
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("event stream ended unexpectedly"),
                Err(_) => panic!("timed out waiting to reconnect"),
            }
        }
        assert!(reconnecting, "the dropped link should surface Reconnecting");
        assert!(reconnected, "SDK should reconnect after a drop");
        assert!(router.connections.load(Ordering::SeqCst) >= 2, "router should see a 2nd socket");
    }

    /// North star — the SDK must never crash. Force three consecutive drops and
    /// assert the supervisor task rides them out (no panic), reconnects each time,
    /// lands on a stable link, and the client is still usable afterward.
    #[tokio::test(flavor = "multi_thread")]
    async fn survives_repeated_drops_without_crashing() {
        let router = mock_router(3).await; // conns #0..2 drop; #3 is stable
        let mut client = LpClient::connect(&router.url, "tok").await.unwrap();

        let mut reconnects = 0;
        for _ in 0..60 {
            match tokio::time::timeout(Duration::from_secs(6), client.next_event()).await {
                Ok(Some(LpEvent::Reconnected)) => {
                    reconnects += 1;
                    if router.connections.load(Ordering::SeqCst) >= 4 {
                        break; // reached the stable connection
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("stream ended — the SDK gave up unexpectedly"),
                Err(_) => panic!("timed out mid-reconnect"),
            }
        }
        assert!(reconnects >= 3, "should reconnect through every drop, got {reconnects}");
        assert!(router.connections.load(Ordering::SeqCst) >= 4);

        // Still alive and usable — no panic took the supervisor down.
        let (a, b) = sample_faucets();
        let ok = client
            .quote(FungibleAsset::new(a, 1).unwrap(), FungibleAsset::new(b, 1).unwrap(), None)
            .is_ok();
        assert!(ok, "client should still accept sends after recovering");
    }

    /// Priority — never send a stale quote. A quote enqueued during the outage
    /// (priced before the drop) must be discarded on reconnect, not flushed to the
    /// router; only a fresh post-reconnect quote should arrive.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_stale_quote_flushed_after_reconnect() {
        let router = mock_router(1).await; // conn #0 drops; conn #1 is stable
        let mut client = LpClient::connect(&router.url, "tok").await.unwrap();
        let (a, b) = sample_faucets();

        assert!(matches!(client.next_event().await, Some(LpEvent::AuthOk)));

        // Wait for the drop → we're now in backoff, with no live socket draining
        // the outbound queue.
        loop {
            match tokio::time::timeout(Duration::from_secs(3), client.next_event()).await {
                Ok(Some(LpEvent::Reconnecting { .. })) => break,
                Ok(Some(_)) => {}
                _ => panic!("expected Reconnecting"),
            }
        }
        // Enqueue a STALE quote (offered=111) during the outage — it can only
        // buffer, since there is no live socket to write it.
        client
            .quote(FungibleAsset::new(a, 111).unwrap(), FungibleAsset::new(b, 1).unwrap(), None)
            .unwrap();

        // Wait until live again — by now the SDK has drained the stale backlog.
        loop {
            match tokio::time::timeout(Duration::from_secs(3), client.next_event()).await {
                Ok(Some(LpEvent::Reconnected)) => break,
                Ok(Some(_)) => {}
                _ => panic!("expected Reconnected"),
            }
        }
        // Post a FRESH quote (offered=999) now that we're live.
        client
            .quote(FungibleAsset::new(a, 999).unwrap(), FungibleAsset::new(b, 1).unwrap(), None)
            .unwrap();

        // Let the writer deliver to the stable connection.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let offered = quote_offer_amounts(&router.received.lock().unwrap());
        assert!(!offered.contains(&111), "stale quote (111) reached the router: {offered:?}");
        assert!(offered.contains(&999), "fresh quote (999) should be delivered: {offered:?}");
    }

    // A minimal global tracing subscriber that records event messages, so a test
    // can assert the SDK actually *logs* drops/reconnects (no extra dependency).
    struct MsgCapture(Arc<Mutex<Vec<String>>>);
    impl tracing::Subscriber for MsgCapture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct V(String);
            impl tracing::field::Visit for V {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut v = V(String::new());
            event.record(&mut v);
            self.0.lock().unwrap().push(v.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    static LOG_BUF: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();
    fn install_log_capture() -> Arc<Mutex<Vec<String>>> {
        let buf = LOG_BUF.get_or_init(|| Arc::new(Mutex::new(Vec::new()))).clone();
        let _ = tracing::subscriber::set_global_default(MsgCapture(buf.clone())); // once
        buf
    }

    /// Logging ask — a drop and its recovery are logged (the typed `LpError` rides
    /// on the `Reconnecting` event, asserted above; here we prove the boundary logs
    /// fire too).
    #[tokio::test(flavor = "multi_thread")]
    async fn drop_and_recovery_are_logged() {
        let logs = install_log_capture();
        let router = mock_router(1).await;
        let mut client = LpClient::connect(&router.url, "tok").await.unwrap();
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_secs(3), client.next_event()).await {
                Ok(Some(LpEvent::Reconnected)) => break,
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        let text = logs.lock().unwrap().join("\n");
        assert!(text.contains("router link dropped"), "missing drop log; got:\n{text}");
        assert!(text.contains("re-established"), "missing reconnect log; got:\n{text}");
    }
}

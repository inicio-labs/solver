//! Websocket RFQ server thread. Allow-listed DEXes connect, post standing
//! quotes (→ `quotes_tx`, read by the matcher), and receive note handovers
//! (← `route_rx`, produced by the matcher). Runs on its own OS thread with a
//! multi-thread runtime, mirroring `spawn_price_api_thread`. Thin transport:
//! every order decision is made in the matcher, not here.

use anyhow::{anyhow, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::matching::types::DexId;
use crate::types::now_millis;
use pswap_lp_sdk::protocol::{handover_frame, ClientMsg, ServerMsg};
use crate::router::{Pair, Quote, QuotesSnapshot, RouteBatch};

/// Configuration for the router websocket server.
#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub bind: String,
    pub port: u16,
    pub max_connections: usize,
    pub max_msg_bytes: usize,
    pub quote_ttl_ms: u64,
    /// Allow-list of bearer tokens (sourced from env). Empty ⇒ reject everyone.
    pub auth_tokens: Vec<String>,
}

#[derive(Clone)]
struct RouterState {
    cfg: Arc<RouterConfig>,
    quotes_tx: watch::Sender<Arc<QuotesSnapshot>>,
    /// Per-DEX quotes: `dex → (pair → quote)`.
    quotes: Arc<Mutex<HashMap<DexId, HashMap<Pair, Quote>>>>,
    /// Per-DEX outbound sender of pre-serialized wire frames, for routing
    /// handovers (and auth/error replies) to the right connection.
    conns: Arc<Mutex<HashMap<DexId, mpsc::UnboundedSender<Vec<u8>>>>>,
    next_dex: Arc<AtomicUsize>,
    conn_count: Arc<AtomicUsize>,
}

impl RouterState {
    fn new(cfg: RouterConfig, quotes_tx: watch::Sender<Arc<QuotesSnapshot>>) -> Self {
        Self {
            cfg: Arc::new(cfg),
            quotes_tx,
            quotes: Arc::new(Mutex::new(HashMap::new())),
            conns: Arc::new(Mutex::new(HashMap::new())),
            next_dex: Arc::new(AtomicUsize::new(1)),
            conn_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Constant-time check of a provided bearer token against the allow-list.
    fn authorized(&self, provided: Option<&str>) -> bool {
        let Some(tok) = provided else { return false };
        let tb = tok.as_bytes();
        self.cfg
            .auth_tokens
            .iter()
            .any(|allowed| bool::from(allowed.as_bytes().ct_eq(tb)))
    }

    /// Rebuild and broadcast the quotes snapshot to the matcher: grouped by pair,
    /// each list sorted by rate (`supply/demand`) **descending** — most generous
    /// first — which is the order `select_notes` relies on. Sorting here (on quote
    /// change) keeps the matcher tick free of it.
    fn republish(&self) {
        let snap: QuotesSnapshot = {
            let quotes = self.quotes.lock().unwrap();
            let mut by_pair: QuotesSnapshot = HashMap::new();
            for per_pair in quotes.values() {
                for (pair, q) in per_pair {
                    by_pair.entry(*pair).or_default().push(q.clone());
                }
            }
            for list in by_pair.values_mut() {
                // descending supply/demand: `a` before `b` when a's rate is higher.
                list.sort_by(|a, b| {
                    (b.supply as u128 * a.demand as u128).cmp(&(a.supply as u128 * b.demand as u128))
                });
            }
            by_pair
        };
        let _ = self.quotes_tx.send(Arc::new(snap));
    }

    /// Handle one decoded (binary) client message for `dex`. Returns an optional
    /// error to send back to that connection.
    fn handle_client_msg(&self, dex: DexId, bytes: &[u8]) -> Option<ServerMsg> {
        let msg = match ClientMsg::read_from_bytes(bytes) {
            Ok(m) => m,
            Err(e) => {
                return Some(ServerMsg::Error {
                    code: "bad_message".into(),
                    msg: format!("undecodable message: {e}"),
                })
            }
        };
        match msg {
            ClientMsg::Quote { offered, requested, valid_for_ms } => {
                self.register_quote(dex, offered, requested, valid_for_ms)
            }
        }
    }

    /// Build + store the note-centric internal quote from a filler-centric SDK
    /// quote. Returns an error `ServerMsg` if the amounts are invalid.
    fn register_quote(
        &self,
        dex: DexId,
        offered: FungibleAsset,
        requested: FungibleAsset,
        valid_for_ms: Option<u64>,
    ) -> Option<ServerMsg> {
        let supply = u64::from(offered.amount()); // base units the DEX supplies
        let demand = u64::from(requested.amount()); // base units the DEX wants back
        if supply == 0 || demand == 0 {
            return Some(ServerMsg::Error {
                code: "bad_quote".into(),
                msg: "quote amounts must be > 0".into(),
            });
        }
        // Flip to note orientation: a note this DEX fills OFFERS what the DEX wants
        // and REQUESTS what it gives.
        let pair: Pair = (requested.faucet_id(), offered.faucet_id());
        let ttl = valid_for_ms
            .map(|v| v.min(self.cfg.quote_ttl_ms))
            .unwrap_or(self.cfg.quote_ttl_ms);
        let quote = Quote { dex, pair, supply, demand, expires_at: now_millis().saturating_add(ttl) };
        self.quotes.lock().unwrap().entry(dex).or_default().insert(pair, quote);
        self.republish();
        None
    }

    /// Deliver a `RouteBatch` to DEX connections, then **consume** each used quote
    /// so it isn't handed out again until the DEX re-quotes. The note travels as
    /// opaque bytes; the wire frame is built from them via `handover_frame`.
    fn deliver(&self, batch: RouteBatch) {
        {
            let conns = self.conns.lock().unwrap();
            for item in &batch.items {
                let Some(tx) = conns.get(&item.dex) else {
                    tracing::warn!(dex = item.dex, note = %item.note_id, "handover dropped: DEX not connected");
                    continue;
                };
                if tx.send(handover_frame(&item.note_bytes, item.fill)).is_err() {
                    tracing::warn!(dex = item.dex, note = %item.note_id, "handover dropped: writer closed");
                }
            }
        }
        let mut consumed = false;
        {
            let mut quotes = self.quotes.lock().unwrap();
            for item in &batch.items {
                if let Some(m) = quotes.get_mut(&item.dex) {
                    consumed |= m.remove(&item.pair).is_some();
                }
            }
        }
        if consumed {
            self.republish();
        }
    }

    fn deregister(&self, dex: DexId) {
        self.conns.lock().unwrap().remove(&dex);
        self.quotes.lock().unwrap().remove(&dex);
        // Saturating: an unpaired deregister must not underflow to usize::MAX.
        let _ = self
            .conn_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some(n.saturating_sub(1)));
        self.republish(); // drop this DEX's quotes immediately
    }
}

fn bearer_from(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::to_string)
}

/// Build the axum router (used by the server thread AND the ws integration test).
fn build_router(state: RouterState) -> Router {
    Router::new()
        .route("/v1/rfq", get(ws_handler))
        .with_state(state)
}

async fn ws_handler(
    State(state): State<RouterState>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    // Auth: `Authorization: Bearer <t>` or `?token=<t>` (browsers can't set headers).
    let token = bearer_from(&headers).or_else(|| q.get("token").cloned());
    if !state.authorized(token.as_deref()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    if state.conn_count.load(Ordering::Relaxed) >= state.cfg.max_connections {
        return (StatusCode::SERVICE_UNAVAILABLE, "router at capacity").into_response();
    }
    let max_bytes = state.cfg.max_msg_bytes;
    ws.max_message_size(max_bytes)
        .on_upgrade(move |socket| handle_conn(state, socket))
}

async fn handle_conn(state: RouterState, socket: WebSocket) {
    let dex = state.next_dex.fetch_add(1, Ordering::Relaxed) as DexId;
    state.conn_count.fetch_add(1, Ordering::Relaxed);

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.conns.lock().unwrap().insert(dex, outbound_tx.clone());
    let _ = outbound_tx.send(ServerMsg::AuthOk.to_bytes());

    let (mut sink, mut stream) = socket.split();

    // Writer task: drain outbound queue → socket (pre-serialized binary frames).
    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if sink.send(Message::Binary(frame.into())).await.is_err() {
                break;
            }
        }
    });

    // Reader loop: decode client binary frames, reply with any error.
    while let Some(item) = stream.next().await {
        match item {
            Ok(Message::Binary(b)) => {
                if let Some(err) = state.handle_client_msg(dex, &b) {
                    let _ = outbound_tx.send(err.to_bytes());
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {} // ignore text / ping / pong
        }
    }

    writer.abort();
    state.deregister(dex);
    tracing::debug!(dex, "router DEX connection closed");
}

/// Spawn the router websocket server on its own OS thread + multi-thread runtime.
/// Returns the thread handle and a readiness oneshot (Ok once bound).
pub fn spawn_router_thread(
    cfg: RouterConfig,
    quotes_tx: watch::Sender<Arc<QuotesSnapshot>>,
    mut route_rx: mpsc::Receiver<RouteBatch>,
    cancel: CancellationToken,
) -> Result<(thread::JoinHandle<()>, oneshot::Receiver<Result<()>>)> {
    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let handle = thread::Builder::new()
        .name("router".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(anyhow!("router runtime: {e}")));
                    return;
                }
            };
            rt.block_on(async move {
                let state = RouterState::new(cfg, quotes_tx);

                // RouteBatch relay: matcher → DEX connections.
                let relay_state = state.clone();
                let relay_cancel = cancel.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = relay_cancel.cancelled() => break,
                            h = route_rx.recv() => match h {
                                Some(handover) => relay_state.deliver(handover),
                                None => break,
                            },
                        }
                    }
                });

                let app = build_router(state.clone());

                let addr: SocketAddr = match format!("{}:{}", state.cfg.bind, state.cfg.port).parse()
                {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow!("router bind address: {e}")));
                        return;
                    }
                };
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow!("router bind {addr}: {e}")));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                tracing::info!(%addr, "router RFQ websocket listening");
                let shutdown = async move { cancel.cancelled().await };
                if let Err(e) = axum::serve(listener, app).with_graceful_shutdown(shutdown).await {
                    tracing::error!(error = %e, "router server error");
                }
            });
        })
        .context("spawn router thread")?;
    Ok((handle, ready_rx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::types::TokenId;
    use crate::router::RoutedNote;
    use miden_protocol::note::NoteId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    fn tok_a() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
    }
    fn tok_b() -> TokenId {
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
    }

    fn make_state(tokens: Vec<String>) -> (RouterState, watch::Receiver<Arc<QuotesSnapshot>>) {
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: tokens,
        };
        let (tx, rx) = watch::channel(Arc::new(HashMap::new()));
        (RouterState::new(cfg, tx), rx)
    }

    /// Encode a filler-centric `Quote` to wire bytes: the DEX offers `offered.1`
    /// base units of `offered.0` and wants `requested.1` of `requested.0`.
    fn quote_bytes(
        offered: (TokenId, u64),
        requested: (TokenId, u64),
        valid_for_ms: Option<u64>,
    ) -> Vec<u8> {
        ClientMsg::Quote {
            offered: FungibleAsset::new(offered.0, offered.1).unwrap(),
            requested: FungibleAsset::new(requested.0, requested.1).unwrap(),
            valid_for_ms,
        }
        .to_bytes()
    }

    /// Assert a received server frame is a binary `AuthOk`.
    fn expect_auth_ok(msg: WsMessage) {
        match msg {
            WsMessage::Binary(b) => assert!(
                matches!(ServerMsg::read_from_bytes(&b), Ok(ServerMsg::AuthOk)),
                "expected AuthOk, decoded something else"
            ),
            other => panic!("expected binary AuthOk frame, got {other:?}"),
        }
    }

    /// Extract the raw bytes of a received binary frame (panics otherwise).
    fn frame_of(msg: WsMessage) -> Vec<u8> {
        match msg {
            WsMessage::Binary(b) => b.to_vec(),
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    #[test]
    fn authorized_checks_allowlist_constant_time() {
        let (s, _rx) = make_state(vec!["secret".into()]);
        assert!(s.authorized(Some("secret")));
        assert!(!s.authorized(Some("wrong")));
        assert!(!s.authorized(None));
        let (s2, _rx2) = make_state(vec![]);
        assert!(!s2.authorized(Some("anything")), "empty allow-list rejects everyone");
    }

    #[test]
    fn quote_publishes_flipped_base_unit_snapshot() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        // Filler-centric: DEX offers 2_500 of tok_a, wants 1_000 of tok_b.
        let bytes = quote_bytes((tok_a(), 2_500), (tok_b(), 1_000), None);
        assert!(s.handle_client_msg(42, &bytes).is_none(), "valid quote accepted");

        assert!(rx.has_changed().unwrap());
        let snap = rx.borrow_and_update().clone();
        assert_eq!(snap.values().flatten().count(), 1);
        let q = snap.values().flatten().next().unwrap();
        assert_eq!(q.dex, 42);
        // Stored note-centric: the pair FLIPS (a note it fills offers what the DEX
        // wants, requests what it gives).
        assert_eq!(q.pair, (tok_b(), tok_a()));
        // supply/demand are the DEX's two base-unit amounts (rate + capacity).
        assert_eq!((q.supply, q.demand), (2_500, 1_000));
        assert!(q.expires_at > 0);
    }

    #[test]
    fn bad_quotes_return_structured_errors() {
        let (s, _rx) = make_state(vec!["t".into()]);
        // Zero offered amount → structured error (never panic).
        assert!(matches!(
            s.handle_client_msg(1, &quote_bytes((tok_a(), 0), (tok_b(), 5), None)),
            Some(ServerMsg::Error { .. })
        ));
        // Zero requested amount → structured error.
        assert!(matches!(
            s.handle_client_msg(1, &quote_bytes((tok_a(), 5), (tok_b(), 0), None)),
            Some(ServerMsg::Error { .. })
        ));
        // Undecodable bytes → structured error.
        assert!(matches!(
            s.handle_client_msg(1, &[0xFF, 0x00, 0x13]),
            Some(ServerMsg::Error { .. })
        ));
    }

    #[test]
    fn deliver_routes_handover_to_the_right_connection() {
        let (s, _rx) = make_state(vec!["t".into()]);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        s.conns.lock().unwrap().insert(7, out_tx);
        let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 99)).unwrap();

        s.deliver(RouteBatch {
            items: vec![RoutedNote { dex: 7, note_id, fill: 500, pair: (tok_a(), tok_b()), note_bytes: vec![0xDE, 0xAD] }],
        });
        // The router emits the raw RouteBatch wire frame straight from the opaque
        // note bytes — byte-for-byte `handover_frame(note_bytes, fill)`.
        assert_eq!(out_rx.try_recv().unwrap(), handover_frame(&[0xDE, 0xAD], 500));

        // A handover for an unknown DEX is silently dropped (no panic).
        s.deliver(RouteBatch {
            items: vec![RoutedNote { dex: 999, note_id, fill: 1, pair: (tok_a(), tok_b()), note_bytes: vec![] }],
        });
        assert!(out_rx.try_recv().is_err());
    }

    #[test]
    fn handover_consumes_the_quote() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        let (out_tx, _out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        s.conns.lock().unwrap().insert(7, out_tx);
        s.handle_client_msg(7, &quote_bytes((tok_a(), 2_000), (tok_b(), 1_000), None));
        let _ = rx.borrow_and_update();
        let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 1)).unwrap();
        // Delivering a handover against that quote consumes it (not re-hittable).
        s.deliver(RouteBatch {
            items: vec![RoutedNote { dex: 7, note_id, fill: 500, pair: (tok_b(), tok_a()), note_bytes: vec![0xAB] }],
        });
        assert!(rx.borrow_and_update().is_empty(), "used quote is consumed");
    }

    #[test]
    fn deregister_drops_that_dexs_quotes() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        s.conn_count.fetch_add(1, Ordering::Relaxed);
        s.handle_client_msg(5, &quote_bytes((tok_a(), 20), (tok_b(), 10), None));
        let _ = rx.borrow_and_update();
        s.deregister(5);
        assert!(rx.borrow_and_update().is_empty(), "deregistered DEX's quotes are purged");
    }

    #[test]
    fn dex_declared_ttl_is_capped_at_server_ttl() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        let bytes = quote_bytes((tok_a(), 20), (tok_b(), 10), Some(999_999_999));
        s.handle_client_msg(3, &bytes);
        let snap = rx.borrow_and_update().clone();
        // expires_at ≤ now + server quote_ttl_ms (20s), not the DEX's huge value.
        assert!(snap.values().flatten().next().unwrap().expires_at <= now_millis() + 20_000);
    }

    /// Real websocket round-trip: bad token rejected; good token → AuthOk; a
    /// posted quote reaches the matcher's `quotes_rx`; a handover reaches the DEX.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_end_to_end_auth_quote_handover() {
        use futures_util::{SinkExt, StreamExt};
        use std::time::Duration;

        let (quotes_tx, mut quotes_rx) = watch::channel(Arc::new(HashMap::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["secret".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);

        // RouteBatch relay (as in spawn_router_thread).
        let (route_tx, mut route_rx) = mpsc::channel::<RouteBatch>(8);
        {
            let st = state.clone();
            tokio::spawn(async move {
                while let Some(h) = route_rx.recv().await {
                    st.deliver(h);
                }
            });
        }

        // Serve build_router on an ephemeral port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Bad token → upgrade rejected.
        assert!(
            tokio_tungstenite::connect_async(format!("ws://{addr}/v1/rfq?token=nope"))
                .await
                .is_err(),
            "wrong token rejected at upgrade"
        );

        // Good token → connect + AuthOk.
        let (mut ws, _resp) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/v1/rfq?token=secret"))
                .await
                .expect("authed connect");
        expect_auth_ok(ws.next().await.unwrap().unwrap());

        // Post a (filler-centric) quote → it reaches the matcher's quotes_rx.
        let quote = quote_bytes((tok_a(), 2_000), (tok_b(), 1_000), None);
        ws.send(WsMessage::Binary(quote.into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), quotes_rx.changed())
            .await
            .expect("quote propagated")
            .unwrap();
        let snap = quotes_rx.borrow_and_update().clone();
        assert_eq!(snap.values().flatten().count(), 1);
        let dex = snap.values().flatten().next().unwrap().dex;

        // Deliver a handover for that DEX → the client receives the exact frame.
        let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 5)).unwrap();
        route_tx
            .send(RouteBatch {
                items: vec![RoutedNote { dex, note_id, fill: 7, pair: (tok_b(), tok_a()), note_bytes: vec![0xAB] }],
            })
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("handover delivered")
            .unwrap()
            .unwrap();
        assert_eq!(frame_of(msg), handover_frame(&[0xAB], 7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_rejects_when_at_capacity() {
        let (quotes_tx, _rx) = watch::channel(Arc::new(HashMap::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 0, // at capacity immediately
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["t".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });
        let res =
            tokio_tungstenite::connect_async(format!("ws://{addr}/v1/rfq?token=t")).await;
        assert!(res.is_err(), "rejected with 503 when at capacity");
    }

    /// Exercises the real `spawn_router_thread` bootstrap: own OS thread +
    /// multi-thread runtime, readiness signal, serving, and graceful shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_router_thread_serves_and_shuts_down() {
        use futures_util::StreamExt;

        // Grab a free port, then let the router thread bind it.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let (quotes_tx, _qrx) = watch::channel(Arc::new(HashMap::new()));
        let (route_tx, route_rx) = mpsc::channel::<RouteBatch>(8);
        let cancel = CancellationToken::new();
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["s".into()],
        };
        let (thread, ready) =
            spawn_router_thread(cfg, quotes_tx, route_rx, cancel.clone()).unwrap();
        ready.await.unwrap().expect("router bound");

        let (mut ws, _resp) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/v1/rfq?token=s"))
                .await
                .expect("connect to thread-served router");
        expect_auth_ok(ws.next().await.unwrap().unwrap());

        drop(ws);
        drop(route_tx);
        cancel.cancel();
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();
    }

    /// Server-side DEX path: authenticate via the `Authorization: Bearer` header
    /// (browsers can't set headers and use `?token=`; server SDKs use the header).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_auth_via_authorization_header() {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let (quotes_tx, _rx) = watch::channel(Arc::new(HashMap::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["secret".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });

        // Valid bearer header → AuthOk.
        let mut req = format!("ws://{addr}/v1/rfq").into_client_request().unwrap();
        req.headers_mut()
            .insert("Authorization", "Bearer secret".parse().unwrap());
        let (mut ws, _resp) = tokio_tungstenite::connect_async(req)
            .await
            .expect("header-authed connect");
        expect_auth_ok(ws.next().await.unwrap().unwrap());

        // Wrong bearer header → rejected at the upgrade.
        let mut bad = format!("ws://{addr}/v1/rfq").into_client_request().unwrap();
        bad.headers_mut()
            .insert("Authorization", "Bearer nope".parse().unwrap());
        assert!(
            tokio_tungstenite::connect_async(bad).await.is_err(),
            "wrong header token rejected"
        );
    }

    /// Two DEXes connected at once: their quotes coexist in the snapshot, and a
    /// handover addressed to one DEX reaches only that DEX's socket.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_two_dexes_routed_independently() {
        use futures_util::{SinkExt, StreamExt};
        use std::time::Duration;

        let (quotes_tx, mut quotes_rx) = watch::channel(Arc::new(HashMap::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["s".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);
        let (route_tx, mut route_rx) = mpsc::channel::<RouteBatch>(8);
        {
            let st = state.clone();
            tokio::spawn(async move {
                while let Some(h) = route_rx.recv().await {
                    st.deliver(h);
                }
            });
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });

        let url = format!("ws://{addr}/v1/rfq?token=s");
        let (mut ws_a, _) = tokio_tungstenite::connect_async(url.clone()).await.unwrap();
        expect_auth_ok(ws_a.next().await.unwrap().unwrap());
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        expect_auth_ok(ws_b.next().await.unwrap().unwrap());

        // Filler-centric quotes: A offers a / wants b → stored pair FLIPS to (b,a);
        // B offers b / wants a → stored pair (a,b).
        ws_a.send(WsMessage::Binary(
            quote_bytes((tok_a(), 2_000), (tok_b(), 1_000), None).into(),
        ))
        .await
        .unwrap();
        ws_b.send(WsMessage::Binary(
            quote_bytes((tok_b(), 2_000), (tok_a(), 1_000), None).into(),
        ))
        .await
        .unwrap();

        // Wait until both quotes are in the published snapshot.
        let mut snap = quotes_rx.borrow_and_update().clone();
        for _ in 0..20 {
            if snap.len() >= 2 {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(300), quotes_rx.changed()).await;
            snap = quotes_rx.borrow_and_update().clone();
        }
        assert_eq!(snap.len(), 2, "both DEXes' quotes coexist");
        // A's quote flipped to (b,a); B's to (a,b).
        let dex_a = snap.values().flatten().find(|q| q.pair == (tok_b(), tok_a())).unwrap().dex;
        let dex_b = snap.values().flatten().find(|q| q.pair == (tok_a(), tok_b())).unwrap().dex;
        assert_ne!(dex_a, dex_b, "distinct DEX ids");

        // RouteBatch to each DEX → each client receives only its own exact frame.
        let n1 = NoteId::try_from_hex(&format!("0x{:064x}", 1)).unwrap();
        let n2 = NoteId::try_from_hex(&format!("0x{:064x}", 2)).unwrap();
        route_tx
            .send(RouteBatch { items: vec![RoutedNote { dex: dex_a, note_id: n1, fill: 1, pair: (tok_b(), tok_a()), note_bytes: vec![0x01] }] })
            .await
            .unwrap();
        route_tx
            .send(RouteBatch { items: vec![RoutedNote { dex: dex_b, note_id: n2, fill: 2, pair: (tok_a(), tok_b()), note_bytes: vec![0x02] }] })
            .await
            .unwrap();

        let ma = tokio::time::timeout(Duration::from_secs(2), ws_a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mb = tokio::time::timeout(Duration::from_secs(2), ws_b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // ws_a is dex_a → note 01; ws_b is dex_b → note 02.
        assert_eq!(frame_of(ma), handover_frame(&[0x01], 1), "DEX A got its note");
        assert_eq!(frame_of(mb), handover_frame(&[0x02], 2), "DEX B got its note");
    }

    /// DoS guard: a message larger than `max_msg_bytes` closes *that* connection
    /// (the server enforces `ws.max_message_size`) without taking down the router —
    /// a second client connects and authenticates fine afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_oversized_message_closes_only_that_connection() {
        use futures_util::{SinkExt, StreamExt};
        use std::time::Duration;

        let (quotes_tx, _rx) = watch::channel(Arc::new(HashMap::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 128, // tiny cap
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["t".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });

        let url = format!("ws://{addr}/v1/rfq?token=t");
        let (mut ws, _) = tokio_tungstenite::connect_async(url.clone()).await.unwrap();
        expect_auth_ok(ws.next().await.unwrap().unwrap());

        // A message far larger than the 128-byte cap.
        let _ = ws.send(WsMessage::Binary(vec![0u8; 8192].into())).await;

        // The server rejects the oversized frame and drops the connection: the
        // client's stream ends (Close / Err / None) instead of hanging.
        let closed = loop {
            match tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => break true,
                Ok(Some(Ok(_))) => continue, // ignore any other frame
                Err(_) => break false,       // timed out → still open (would be a leak)
            }
        };
        assert!(closed, "oversized message must close the connection");

        // The router itself is unharmed: a fresh client still connects + authenticates.
        let (mut ws2, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        expect_auth_ok(ws2.next().await.unwrap().unwrap());
    }
}

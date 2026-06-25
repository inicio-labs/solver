//! Websocket RFQ server thread. Allow-listed DEXes connect, post standing
//! quotes (→ `quotes_tx`, read by the matcher), and receive note handovers
//! (← `handover_rx`, produced by the matcher). Runs on its own OS thread with a
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
use miden_protocol::account::AccountId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::matching::types::{DexId, TokenId};
use pswap_filler_sdk::protocol::{parse_decimal_price, ClientMsg, PairSpec, ServerMsg};
use crate::router::{Handover, Pair, Quote, QuotesSnapshot};

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

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
struct RouterState {
    cfg: Arc<RouterConfig>,
    quotes_tx: watch::Sender<Arc<QuotesSnapshot>>,
    /// Per-DEX quotes: `dex → (pair → quote)`.
    quotes: Arc<Mutex<HashMap<DexId, HashMap<Pair, Quote>>>>,
    /// Per-DEX outbound sender, for routing handovers to the right connection.
    conns: Arc<Mutex<HashMap<DexId, mpsc::UnboundedSender<ServerMsg>>>>,
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

    /// Rebuild and broadcast the merged quotes snapshot to the matcher.
    fn republish(&self) {
        let snap: Vec<Quote> = {
            let quotes = self.quotes.lock().unwrap();
            quotes.values().flat_map(|m| m.values().cloned()).collect()
        };
        let _ = self.quotes_tx.send(Arc::new(snap));
    }

    /// Handle one decoded client message for `dex`. Returns an optional error to
    /// send back to that connection.
    fn handle_client_msg(&self, dex: DexId, text: &str) -> Option<ServerMsg> {
        let msg: ClientMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                return Some(ServerMsg::Error {
                    code: "bad_message".into(),
                    msg: format!("invalid message: {e}"),
                })
            }
        };
        match msg {
            ClientMsg::Subscribe { .. } => None, // accepted; quotes gate per pair
            ClientMsg::Quote { pair, price, quantity, valid_for_ms } => {
                let parsed_pair = match parse_pair(&pair) {
                    Some(p) => p,
                    None => {
                        return Some(ServerMsg::Error {
                            code: "bad_pair".into(),
                            msg: "invalid pair account id".into(),
                        })
                    }
                };
                let (num, den) = match parse_decimal_price(&price) {
                    Some(r) => r,
                    None => {
                        return Some(ServerMsg::Error {
                            code: "bad_price".into(),
                            msg: format!("invalid price: {price}"),
                        })
                    }
                };
                if quantity == 0 {
                    return Some(ServerMsg::Error {
                        code: "bad_quantity".into(),
                        msg: "quantity must be > 0".into(),
                    });
                }
                let ttl = valid_for_ms
                    .map(|v| v.min(self.cfg.quote_ttl_ms))
                    .unwrap_or(self.cfg.quote_ttl_ms);
                let quote = Quote {
                    dex,
                    pair: parsed_pair,
                    price_num: num,
                    price_den: den,
                    quantity,
                    expires_at: now_millis().saturating_add(ttl),
                };
                self.quotes
                    .lock()
                    .unwrap()
                    .entry(dex)
                    .or_default()
                    .insert(parsed_pair, quote);
                self.republish();
                None
            }
        }
    }

    /// Deliver a matcher handover batch to the appropriate DEX connections.
    fn deliver(&self, handover: Handover) {
        let conns = self.conns.lock().unwrap();
        for item in handover.items {
            let Some(tx) = conns.get(&item.dex) else { continue };
            let _ = tx.send(ServerMsg::Handover {
                note_id: format!("{}", item.note_id),
                fill_amount: item.fill,
                note_hex: hex::encode(&item.note_bytes),
                fill_price: item.fill_price.clone(),
            });
        }
    }

    fn deregister(&self, dex: DexId) {
        self.conns.lock().unwrap().remove(&dex);
        self.quotes.lock().unwrap().remove(&dex);
        self.conn_count.fetch_sub(1, Ordering::Relaxed);
        self.republish(); // drop this DEX's quotes immediately
    }
}

fn parse_pair(spec: &PairSpec) -> Option<Pair> {
    let offered: TokenId = AccountId::from_hex(&spec.offered).ok()?;
    let requested: TokenId = AccountId::from_hex(&spec.requested).ok()?;
    Some((offered, requested))
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

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<ServerMsg>();
    state.conns.lock().unwrap().insert(dex, outbound_tx.clone());
    let _ = outbound_tx.send(ServerMsg::AuthOk);

    let (mut sink, mut stream) = socket.split();

    // Writer task: drain outbound queue → socket.
    let writer = tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            let txt = serde_json::to_string(&msg).unwrap_or_default();
            if sink.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    // Reader loop: decode client messages, reply with any error.
    while let Some(item) = stream.next().await {
        match item {
            Ok(Message::Text(t)) => {
                if let Some(err) = state.handle_client_msg(dex, t.as_str()) {
                    let _ = outbound_tx.send(err);
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {} // ignore binary / ping / pong
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
    mut handover_rx: mpsc::Receiver<Handover>,
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

                // Handover relay: matcher → DEX connections.
                let relay_state = state.clone();
                let relay_cancel = cancel.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = relay_cancel.cancelled() => break,
                            h = handover_rx.recv() => match h {
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
    use crate::router::HandoverPick;
    use miden_protocol::note::NoteId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

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
        let (tx, rx) = watch::channel(Arc::new(Vec::new()));
        (RouterState::new(cfg, tx), rx)
    }

    fn quote_json(pair: (&str, &str), price: &str, quantity: u64) -> String {
        serde_json::json!({
            "type": "quote",
            "pair": { "offered": pair.0, "requested": pair.1 },
            "price": price,
            "quantity": quantity,
        })
        .to_string()
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
    fn parse_pair_valid_and_invalid() {
        let spec = PairSpec { offered: tok_a().to_hex(), requested: tok_b().to_hex() };
        assert_eq!(parse_pair(&spec), Some((tok_a(), tok_b())));
        let bad = PairSpec { offered: "not-hex".into(), requested: tok_b().to_hex() };
        assert!(parse_pair(&bad).is_none());
    }

    #[test]
    fn quote_message_publishes_snapshot() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        let json = quote_json((&tok_a().to_hex(), &tok_b().to_hex()), "2.5", 1_000_000);
        assert!(s.handle_client_msg(42, &json).is_none(), "valid quote accepted");

        assert!(rx.has_changed().unwrap());
        let snap = rx.borrow_and_update().clone();
        assert_eq!(snap.len(), 1);
        let q = &snap[0];
        assert_eq!(q.dex, 42);
        assert_eq!(q.pair, (tok_a(), tok_b()));
        assert_eq!((q.price_num, q.price_den), (25, 10)); // "2.5"
        assert_eq!(q.quantity, 1_000_000);
        assert!(q.expires_at > 0);
    }

    #[test]
    fn bad_quotes_return_structured_errors() {
        let (s, _rx) = make_state(vec!["t".into()]);
        let pa = tok_a().to_hex();
        let pb = tok_b().to_hex();
        assert!(matches!(
            s.handle_client_msg(1, &quote_json((&pa, &pb), "abc", 1)),
            Some(ServerMsg::Error { .. })
        ));
        assert!(matches!(
            s.handle_client_msg(1, &quote_json((&pa, &pb), "2", 0)),
            Some(ServerMsg::Error { .. })
        ));
        assert!(matches!(
            s.handle_client_msg(1, &quote_json(("xx", "yy"), "2", 1)),
            Some(ServerMsg::Error { .. })
        ));
        assert!(matches!(
            s.handle_client_msg(1, "not json"),
            Some(ServerMsg::Error { .. })
        ));
    }

    #[test]
    fn subscribe_is_accepted() {
        let (s, _rx) = make_state(vec!["t".into()]);
        let json = serde_json::json!({
            "type": "subscribe",
            "pairs": [{ "offered": tok_a().to_hex(), "requested": tok_b().to_hex() }],
        })
        .to_string();
        assert!(s.handle_client_msg(1, &json).is_none());
    }

    #[test]
    fn deliver_routes_handover_to_the_right_connection() {
        let (s, _rx) = make_state(vec!["t".into()]);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerMsg>();
        s.conns.lock().unwrap().insert(7, out_tx);
        let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 99)).unwrap();

        s.deliver(Handover {
            items: vec![HandoverPick {
                dex: 7,
                note_id,
                fill: 500,
                note_bytes: vec![0xDE, 0xAD],
                fill_price: "2.5".into(),
            }],
        });
        match out_rx.try_recv().unwrap() {
            ServerMsg::Handover { note_id: nid, fill_amount, note_hex, fill_price } => {
                assert_eq!(fill_amount, 500);
                assert_eq!(note_hex, "dead");
                assert_eq!(nid, format!("{note_id}"));
                assert_eq!(fill_price, "2.5");
            }
            other => panic!("expected handover, got {other:?}"),
        }
        // A handover for an unknown DEX is silently dropped (no panic).
        s.deliver(Handover {
            items: vec![HandoverPick {
                dex: 999,
                note_id,
                fill: 1,
                note_bytes: vec![],
                fill_price: "1".into(),
            }],
        });
        assert!(out_rx.try_recv().is_err());
    }

    #[test]
    fn deregister_drops_that_dexs_quotes() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        s.conn_count.fetch_add(1, Ordering::Relaxed);
        s.handle_client_msg(5, &quote_json((&tok_a().to_hex(), &tok_b().to_hex()), "2", 10));
        let _ = rx.borrow_and_update();
        s.deregister(5);
        assert!(rx.borrow_and_update().is_empty(), "deregistered DEX's quotes are purged");
    }

    #[test]
    fn dex_declared_ttl_is_capped_at_server_ttl() {
        let (s, mut rx) = make_state(vec!["t".into()]);
        let json = serde_json::json!({
            "type": "quote",
            "pair": { "offered": tok_a().to_hex(), "requested": tok_b().to_hex() },
            "price": "2",
            "quantity": 10,
            "valid_for_ms": 999_999_999u64,
        })
        .to_string();
        s.handle_client_msg(3, &json);
        let snap = rx.borrow_and_update().clone();
        // expires_at ≤ now + server quote_ttl_ms (20s), not the DEX's huge value.
        assert!(snap[0].expires_at <= now_millis() + 20_000);
    }

    /// Real websocket round-trip: bad token rejected; good token → AuthOk; a
    /// posted quote reaches the matcher's `quotes_rx`; a handover reaches the DEX.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_end_to_end_auth_quote_handover() {
        use futures_util::{SinkExt, StreamExt};
        use std::time::Duration;
        use tokio_tungstenite::tungstenite::Message as WsMessage;

        let (quotes_tx, mut quotes_rx) = watch::channel(Arc::new(Vec::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["secret".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);

        // Handover relay (as in spawn_router_thread).
        let (handover_tx, mut handover_rx) = mpsc::channel::<Handover>(8);
        {
            let st = state.clone();
            tokio::spawn(async move {
                while let Some(h) = handover_rx.recv().await {
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
        let first = ws.next().await.unwrap().unwrap();
        assert!(first.to_text().unwrap().contains("auth_ok"));

        // Post a quote → it reaches the matcher's quotes_rx.
        let quote = quote_json((&tok_a().to_hex(), &tok_b().to_hex()), "2", 1_000);
        ws.send(WsMessage::Text(quote.into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), quotes_rx.changed())
            .await
            .expect("quote propagated")
            .unwrap();
        let snap = quotes_rx.borrow_and_update().clone();
        assert_eq!(snap.len(), 1);
        let dex = snap[0].dex;

        // Deliver a handover for that DEX → the client receives it.
        let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 5)).unwrap();
        handover_tx
            .send(Handover {
                items: vec![HandoverPick {
                    dex,
                    note_id,
                    fill: 7,
                    note_bytes: vec![0xAB],
                    fill_price: "2".into(),
                }],
            })
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("handover delivered")
            .unwrap()
            .unwrap();
        let txt = msg.to_text().unwrap();
        assert!(txt.contains("handover"), "got: {txt}");
        assert!(txt.contains("\"note_hex\":\"ab\""), "got: {txt}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_rejects_when_at_capacity() {
        let (quotes_tx, _rx) = watch::channel(Arc::new(Vec::new()));
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

        let (quotes_tx, _qrx) = watch::channel(Arc::new(Vec::new()));
        let (handover_tx, handover_rx) = mpsc::channel::<Handover>(8);
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
            spawn_router_thread(cfg, quotes_tx, handover_rx, cancel.clone()).unwrap();
        ready.await.unwrap().expect("router bound");

        let (mut ws, _resp) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/v1/rfq?token=s"))
                .await
                .expect("connect to thread-served router");
        let first = ws.next().await.unwrap().unwrap();
        assert!(first.to_text().unwrap().contains("auth_ok"));

        drop(ws);
        drop(handover_tx);
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

        let (quotes_tx, _rx) = watch::channel(Arc::new(Vec::new()));
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
        let first = ws.next().await.unwrap().unwrap();
        assert!(first.to_text().unwrap().contains("auth_ok"));

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
        use tokio_tungstenite::tungstenite::Message as WsMessage;

        let (quotes_tx, mut quotes_rx) = watch::channel(Arc::new(Vec::new()));
        let cfg = RouterConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            max_connections: 8,
            max_msg_bytes: 16384,
            quote_ttl_ms: 20_000,
            auth_tokens: vec!["s".into()],
        };
        let state = RouterState::new(cfg, quotes_tx);
        let (handover_tx, mut handover_rx) = mpsc::channel::<Handover>(8);
        {
            let st = state.clone();
            tokio::spawn(async move {
                while let Some(h) = handover_rx.recv().await {
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
        assert!(ws_a.next().await.unwrap().unwrap().to_text().unwrap().contains("auth_ok"));
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        assert!(ws_b.next().await.unwrap().unwrap().to_text().unwrap().contains("auth_ok"));

        // A quotes pair (a,b); B quotes the opposite orientation (b,a).
        ws_a.send(WsMessage::Text(
            quote_json((&tok_a().to_hex(), &tok_b().to_hex()), "2", 1_000).into(),
        ))
        .await
        .unwrap();
        ws_b.send(WsMessage::Text(
            quote_json((&tok_b().to_hex(), &tok_a().to_hex()), "2", 1_000).into(),
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
        let dex_ab = snap.iter().find(|q| q.pair == (tok_a(), tok_b())).unwrap().dex;
        let dex_ba = snap.iter().find(|q| q.pair == (tok_b(), tok_a())).unwrap().dex;
        assert_ne!(dex_ab, dex_ba, "distinct DEX ids");

        // Handover to each DEX → each client receives only its own.
        let n1 = NoteId::try_from_hex(&format!("0x{:064x}", 1)).unwrap();
        let n2 = NoteId::try_from_hex(&format!("0x{:064x}", 2)).unwrap();
        handover_tx
            .send(Handover { items: vec![HandoverPick { dex: dex_ab, note_id: n1, fill: 1, note_bytes: vec![0x01], fill_price: "2".into() }] })
            .await
            .unwrap();
        handover_tx
            .send(Handover { items: vec![HandoverPick { dex: dex_ba, note_id: n2, fill: 2, note_bytes: vec![0x02], fill_price: "2".into() }] })
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
        // ws_a quoted (a,b) so it is dex_ab → it must receive note 01; ws_b → 02.
        assert!(ma.to_text().unwrap().contains("\"note_hex\":\"01\""), "DEX A got its note");
        assert!(mb.to_text().unwrap().contains("\"note_hex\":\"02\""), "DEX B got its note");
    }
}

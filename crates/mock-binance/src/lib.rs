//! A local stand-in for Binance Spot public market data.
//!
//! Serves the two endpoints the solver's price feed uses:
//!
//! * `GET /api/v3/exchangeInfo?symbol=ETHUSDT` — one listing, or HTTP 400 with
//!   code `-1121` for an unknown symbol, as Binance answers.
//! * `GET /stream?streams=ethusdt@bookTicker/...` — the combined `bookTicker`
//!   stream. Every `tick`, each `TRADING` market publishes a new update with
//!   the next update ID. All connections receive the same frames, like two
//!   connections to Binance, so their IDs agree.
//!
//! The server pings each connection like Binance does and drops one that does
//! not answer within the pong timeout. Tests can change quotes and statuses,
//! inject raw frames, fail `exchangeInfo` requests or stream handshakes, and
//! drop every connection.
//!
//! Operator endpoints for devnet: `GET /set?symbol=&bid=&ask=` and `GET /markets`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::Instant;

/// One mock market. Prices are decimal strings, sent exactly as given.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Market {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub bid: String,
    pub ask: String,
    pub status: String,
}

impl Market {
    /// A `TRADING` market.
    pub fn new(symbol: &str, base_asset: &str, quote_asset: &str, bid: &str, ask: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            base_asset: base_asset.to_string(),
            quote_asset: quote_asset.to_string(),
            bid: bid.to_string(),
            ask: ask.to_string(),
            status: "TRADING".to_string(),
        }
    }

    fn stream(&self) -> String {
        format!("{}@bookTicker", self.symbol.to_ascii_lowercase())
    }
}

/// Timing of the mock streams.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Interval between updates of every trading market.
    pub tick: Duration,
    /// Interval between server pings on each connection.
    pub ping_interval: Duration,
    /// A connection whose ping is unanswered for this long is closed.
    pub pong_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(60),
        }
    }
}

/// A queued HTTP failure for an `exchangeInfo` request or a stream handshake.
#[derive(Clone, Copy, Debug)]
pub struct Failure {
    pub status: u16,
    pub retry_after_secs: Option<u64>,
}

impl Failure {
    fn response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (
            status,
            Json(json!({ "code": -1003, "msg": "mock failure" })),
        )
            .into_response();
        if let Some(seconds) = self.retry_after_secs {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

struct MarketState {
    market: Market,
    update_id: u64,
}

impl MarketState {
    /// Advance to the next update ID and return its frame.
    fn next_frame(&mut self) -> Frame {
        self.update_id += 1;
        let market = &self.market;
        let data = json!({
            "u": self.update_id,
            "s": market.symbol,
            "b": market.bid,
            "B": "1.00000000",
            "a": market.ask,
            "A": "1.00000000",
        });
        Frame {
            stream: market.stream(),
            text: json!({ "stream": market.stream(), "data": data }).to_string(),
        }
    }
}

#[derive(Clone)]
struct Frame {
    stream: String,
    text: String,
}

struct Shared {
    settings: Settings,
    markets: Mutex<BTreeMap<String, MarketState>>,
    frames: broadcast::Sender<Frame>,
    disconnect: watch::Sender<u64>,
    rest_failures: Mutex<VecDeque<Failure>>,
    stream_failures: Mutex<VecDeque<Failure>>,
    open_connections: AtomicUsize,
    peak_connections: AtomicUsize,
    connections_total: AtomicU64,
    pongs: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl Shared {
    fn tick(&self) {
        let frames: Vec<Frame> = lock(&self.markets)
            .values_mut()
            .filter(|state| state.market.status == "TRADING")
            .map(MarketState::next_frame)
            .collect();
        for frame in frames {
            let _ = self.frames.send(frame);
        }
    }

    /// Change a market's quote from the next tick on.
    fn set_quote(&self, symbol: &str, bid: &str, ask: &str) -> Option<Market> {
        let mut markets = lock(&self.markets);
        let state = markets.get_mut(symbol)?;
        state.market.bid = bid.to_string();
        state.market.ask = ask.to_string();
        Some(state.market.clone())
    }
}

/// A running mock server; stops when dropped.
pub struct MockBinance {
    addr: SocketAddr,
    shared: Arc<Shared>,
    _shutdown: oneshot::Sender<()>,
}

impl MockBinance {
    /// Serve `markets` on an ephemeral loopback port.
    pub async fn start(markets: Vec<Market>, settings: Settings) -> std::io::Result<Self> {
        Self::bind(SocketAddr::from(([127, 0, 0, 1], 0)), markets, settings).await
    }

    /// Serve `markets` on `addr`.
    pub async fn bind(
        addr: SocketAddr,
        markets: Vec<Market>,
        settings: Settings,
    ) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let markets = markets
            .into_iter()
            .map(|market| {
                (
                    market.symbol.clone(),
                    MarketState {
                        market,
                        update_id: 0,
                    },
                )
            })
            .collect();
        let shared = Arc::new(Shared {
            settings,
            markets: Mutex::new(markets),
            frames: broadcast::channel(1024).0,
            disconnect: watch::channel(0).0,
            rest_failures: Mutex::new(VecDeque::new()),
            stream_failures: Mutex::new(VecDeque::new()),
            open_connections: AtomicUsize::new(0),
            peak_connections: AtomicUsize::new(0),
            connections_total: AtomicU64::new(0),
            pongs: AtomicU64::new(0),
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let app = Router::new()
            .route("/api/v3/exchangeInfo", get(exchange_info))
            .route("/stream", get(stream))
            .route("/set", get(set_quote))
            .route("/markets", get(list_markets))
            .with_state(shared.clone());
        let ticker = shared.clone();
        tokio::spawn(async move {
            let server = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .into_future();
            let mut interval = tokio::time::interval(ticker.settings.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let ticks = async {
                loop {
                    interval.tick().await;
                    ticker.tick();
                }
            };
            tokio::select! {
                result = server => if let Err(error) = result {
                    tracing::error!(%error, "mock-binance server failed");
                },
                _ = ticks => {}
            }
            // Close open streams so their clients see the shutdown.
            ticker.disconnect.send_modify(|generation| *generation += 1);
        });
        Ok(Self {
            addr,
            shared,
            _shutdown: shutdown_tx,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Base URL for the stream endpoint, e.g. `ws://127.0.0.1:1234`.
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Base URL for the REST endpoint, e.g. `http://127.0.0.1:1234`.
    pub fn rest_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Change a market's quote from the next tick on.
    pub fn set_quote(&self, symbol: &str, bid: &str, ask: &str) {
        self.shared.set_quote(symbol, bid, ask);
    }

    /// Change a market's listed status; only `TRADING` markets publish updates.
    pub fn set_status(&self, symbol: &str, status: &str) {
        if let Some(state) = lock(&self.shared.markets).get_mut(symbol) {
            state.market.status = status.to_string();
        }
    }

    /// `symbol`'s latest update ID.
    pub fn update_id(&self, symbol: &str) -> Option<u64> {
        lock(&self.shared.markets)
            .get(symbol)
            .map(|state| state.update_id)
    }

    /// Reserve `symbol`'s next update ID, for a hand-written frame.
    pub fn next_update_id(&self, symbol: &str) -> Option<u64> {
        let mut markets = lock(&self.shared.markets);
        let state = markets.get_mut(symbol)?;
        state.update_id += 1;
        Some(state.update_id)
    }

    /// Send `text` verbatim to every connection subscribed to `stream`.
    pub fn send_raw(&self, stream: &str, text: String) {
        let _ = self.shared.frames.send(Frame {
            stream: stream.to_string(),
            text,
        });
    }

    /// Answer the next `exchangeInfo` request with `failure`.
    pub fn fail_rest(&self, failure: Failure) {
        lock(&self.shared.rest_failures).push_back(failure);
    }

    /// Refuse the next stream handshake with `failure`.
    pub fn fail_stream(&self, failure: Failure) {
        lock(&self.shared.stream_failures).push_back(failure);
    }

    /// Close every open stream connection.
    pub fn disconnect_all(&self) {
        self.shared
            .disconnect
            .send_modify(|generation| *generation += 1);
    }

    pub fn open_connections(&self) -> usize {
        self.shared.open_connections.load(Ordering::SeqCst)
    }

    /// Most stream connections open at the same time so far.
    pub fn peak_connections(&self) -> usize {
        self.shared.peak_connections.load(Ordering::SeqCst)
    }

    pub fn connections_total(&self) -> u64 {
        self.shared.connections_total.load(Ordering::SeqCst)
    }

    pub fn pongs(&self) -> u64 {
        self.shared.pongs.load(Ordering::SeqCst)
    }
}

#[derive(Deserialize)]
struct SymbolQuery {
    symbol: Option<String>,
}

fn invalid_symbol() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "code": -1121, "msg": "Invalid symbol." })),
    )
        .into_response()
}

async fn exchange_info(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<SymbolQuery>,
) -> Response {
    if let Some(failure) = lock(&shared.rest_failures).pop_front() {
        return failure.response();
    }
    let Some(symbol) = query.symbol else {
        return invalid_symbol();
    };
    let markets = lock(&shared.markets);
    let Some(state) = markets.get(&symbol) else {
        return invalid_symbol();
    };
    let market = &state.market;
    Json(json!({
        "timezone": "UTC",
        "rateLimits": [],
        "exchangeFilters": [],
        "symbols": [{
            "symbol": market.symbol,
            "status": market.status,
            "baseAsset": market.base_asset,
            "quoteAsset": market.quote_asset,
        }],
    }))
    .into_response()
}

#[derive(Deserialize)]
struct StreamQuery {
    streams: String,
}

async fn stream(
    upgrade: WebSocketUpgrade,
    State(shared): State<Arc<Shared>>,
    Query(query): Query<StreamQuery>,
) -> Response {
    if let Some(failure) = lock(&shared.stream_failures).pop_front() {
        return failure.response();
    }
    let streams: HashSet<String> = query.streams.split('/').map(str::to_string).collect();
    // Subscribe before upgrading, so nothing sent or disconnected in between
    // is missed.
    let frames = shared.frames.subscribe();
    let mut disconnect = shared.disconnect.subscribe();
    disconnect.borrow_and_update();
    upgrade.on_upgrade(move |socket| serve_stream(socket, streams, frames, disconnect, shared))
}

async fn serve_stream(
    mut socket: WebSocket,
    streams: HashSet<String>,
    mut frames: broadcast::Receiver<Frame>,
    mut disconnect: watch::Receiver<u64>,
    shared: Arc<Shared>,
) {
    let open = shared.open_connections.fetch_add(1, Ordering::SeqCst) + 1;
    shared.peak_connections.fetch_max(open, Ordering::SeqCst);
    shared.connections_total.fetch_add(1, Ordering::SeqCst);
    let settings = shared.settings;
    let mut ping = tokio::time::interval_at(
        Instant::now() + settings.ping_interval,
        settings.ping_interval,
    );
    let mut awaiting_pong: Option<Instant> = None;
    loop {
        tokio::select! {
            frame = frames.recv() => match frame {
                Ok(frame) if streams.contains(&frame.stream) => {
                    if socket.send(Message::Text(frame.text.into())).await.is_err() {
                        break;
                    }
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = ping.tick() => {
                if awaiting_pong.is_some_and(|since| since.elapsed() > settings.pong_timeout) {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
                awaiting_pong.get_or_insert_with(Instant::now);
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            message = socket.recv() => match message {
                Some(Ok(Message::Pong(_))) => {
                    shared.pongs.fetch_add(1, Ordering::SeqCst);
                    awaiting_pong = None;
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = disconnect.changed() => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
    shared.open_connections.fetch_sub(1, Ordering::SeqCst);
}

#[derive(Deserialize)]
struct SetQuery {
    symbol: String,
    bid: String,
    ask: String,
}

async fn set_quote(State(shared): State<Arc<Shared>>, Query(query): Query<SetQuery>) -> Response {
    match shared.set_quote(&query.symbol, &query.bid, &query.ask) {
        Some(market) => Json(market).into_response(),
        None => invalid_symbol(),
    }
}

async fn list_markets(State(shared): State<Arc<Shared>>) -> Json<HashMap<String, Market>> {
    Json(
        lock(&shared.markets)
            .iter()
            .map(|(symbol, state)| (symbol.clone(), state.market.clone()))
            .collect(),
    )
}

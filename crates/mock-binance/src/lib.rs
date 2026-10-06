//! A local stand-in for Binance Spot public market data.
//!
//! Serves the two endpoints the solver's price feed uses:
//!
//! * `GET /api/v3/exchangeInfo?symbol=ETHUSDT` — one listing, or HTTP 400 with
//!   code `-1121` for an unknown symbol and `-1100` for an illegal one, as
//!   Binance answers.
//! * `GET /stream?streams=ethusdt@bookTicker/...` — the combined `bookTicker`
//!   stream. Every `tick`, each `TRADING` market publishes a new update with
//!   the next update ID (or, with [`Updates::OnChange`], only when its quote
//!   changed, as Binance does). All connections receive the same frames, like
//!   two connections to Binance, so their IDs agree.
//!
//! The server pings each connection like Binance does, with a payload the
//! pong must echo, and drops one that does not answer within the pong
//! timeout. Tests can change quotes, statuses and the spot flag, inject raw
//! frames, fail `exchangeInfo` requests or stream handshakes, announce a
//! shutdown, and drop every connection.
//!
//! Operator endpoints for devnet: `GET /set?symbol=&bid=&ask=` (loopback
//! clients only) and `GET /markets`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::Instant;

/// Delay between a `serverShutdown` event and the close that follows it.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(200);

/// One mock market. Prices are decimal strings, sent exactly as given.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Market {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub bid: String,
    pub ask: String,
    pub status: String,
    pub spot_trading_allowed: bool,
}

impl Market {
    /// A `TRADING` spot market.
    pub fn new(symbol: &str, base_asset: &str, quote_asset: &str, bid: &str, ask: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            base_asset: base_asset.to_string(),
            quote_asset: quote_asset.to_string(),
            bid: bid.to_string(),
            ask: ask.to_string(),
            status: "TRADING".to_string(),
            spot_trading_allowed: true,
        }
    }

    fn stream(&self) -> String {
        format!("{}@bookTicker", self.symbol.to_ascii_lowercase())
    }
}

/// When a trading market publishes an update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Updates {
    /// Every tick, whether or not the quote changed: keeps devnet quotes fresh.
    #[default]
    EveryTick,
    /// Only when the quote changed, as Binance's `bookTicker` does.
    OnChange,
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
    pub updates: Updates,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(60),
            updates: Updates::EveryTick,
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
    /// The quote of the last published update.
    published: Option<(String, String)>,
}

impl MarketState {
    /// Advance to the next update ID and return its frame.
    fn next_frame(&mut self) -> Frame {
        self.update_id += 1;
        let market = &self.market;
        self.published = Some((market.bid.clone(), market.ask.clone()));
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

    fn changed(&self) -> bool {
        self.published.as_ref() != Some(&(self.market.bid.clone(), self.market.ask.clone()))
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
    shutdown: watch::Sender<u64>,
    rest_failures: Mutex<VecDeque<Failure>>,
    /// Failures for `exchangeInfo` requests about one symbol.
    symbol_failures: Mutex<HashMap<String, VecDeque<Failure>>>,
    stream_failures: Mutex<VecDeque<Failure>>,
    open_connections: AtomicUsize,
    peak_connections: AtomicUsize,
    connections_total: AtomicU64,
    pongs: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonErrorExt::into_inner)
}

/// `PoisonError::into_inner` as a path, for `unwrap_or_else`.
trait PoisonErrorExt<T> {
    fn into_inner(self) -> T;
}

impl<T> PoisonErrorExt<T> for std::sync::PoisonError<T> {
    fn into_inner(self) -> T {
        std::sync::PoisonError::into_inner(self)
    }
}

impl Shared {
    fn tick(&self) {
        let on_change = self.settings.updates == Updates::OnChange;
        let frames: Vec<Frame> = lock(&self.markets)
            .values_mut()
            .filter(|state| state.market.status == "TRADING")
            .filter(|state| !on_change || state.changed())
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
                        published: None,
                    },
                )
            })
            .collect();
        let shared = Arc::new(Shared {
            settings,
            markets: Mutex::new(markets),
            frames: broadcast::channel(1024).0,
            disconnect: watch::channel(0).0,
            shutdown: watch::channel(0).0,
            rest_failures: Mutex::new(VecDeque::new()),
            symbol_failures: Mutex::new(HashMap::new()),
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
            let server = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
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

    /// Change a market's `isSpotTradingAllowed` flag.
    pub fn set_spot_trading_allowed(&self, symbol: &str, allowed: bool) {
        if let Some(state) = lock(&self.shared.markets).get_mut(symbol) {
            state.market.spot_trading_allowed = allowed;
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

    /// Move `symbol`'s update IDs ahead by `count`, as if updates were missed.
    pub fn skip_update_ids(&self, symbol: &str, count: u64) {
        if let Some(state) = lock(&self.shared.markets).get_mut(symbol) {
            state.update_id += count;
        }
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

    /// Answer the next `exchangeInfo` request about `symbol` with `failure`;
    /// requests about other symbols are unaffected.
    pub fn fail_rest_for(&self, symbol: &str, failure: Failure) {
        lock(&self.shared.symbol_failures)
            .entry(symbol.to_string())
            .or_default()
            .push_back(failure);
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

    /// Announce a `serverShutdown` on every open stream connection, then close
    /// it shortly after, as Binance does before a restart.
    pub fn shutdown_all(&self) {
        self.shared
            .shutdown
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

    /// Pongs that echoed a ping's payload.
    pub fn pongs(&self) -> u64 {
        self.shared.pongs.load(Ordering::SeqCst)
    }
}

/// A mock on its own thread and runtime, unaffected by whatever the caller's
/// thread does. Stops when dropped.
pub struct MockThread {
    mock: Arc<MockBinance>,
    _stop: oneshot::Sender<()>,
}

impl MockThread {
    pub fn start(markets: Vec<Market>, settings: Settings) -> Self {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop, stopped) = oneshot::channel::<()>();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("mock runtime");
            runtime.block_on(async move {
                let mock = MockBinance::start(markets, settings)
                    .await
                    .expect("mock bind");
                ready_tx.send(Arc::new(mock)).expect("mock ready");
                let _ = stopped.await;
            });
        });
        Self {
            mock: ready_rx.recv().expect("mock started"),
            _stop: stop,
        }
    }
}

impl Deref for MockThread {
    type Target = MockBinance;

    fn deref(&self) -> &MockBinance {
        &self.mock
    }
}

#[derive(Deserialize)]
struct SymbolQuery {
    symbol: Option<String>,
}

fn api_error(code: i64, msg: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "code": code, "msg": msg })),
    )
        .into_response()
}

fn invalid_symbol() -> Response {
    api_error(-1121, "Invalid symbol.")
}

async fn exchange_info(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<SymbolQuery>,
) -> Response {
    if let Some(failure) = lock(&shared.rest_failures).pop_front() {
        return failure.response();
    }
    let Some(symbol) = query.symbol else {
        return api_error(-1102, "Mandatory parameter 'symbol' was not sent.");
    };
    // Binance's legal range for a symbol excludes lower-case letters.
    if symbol.is_empty()
        || symbol.len() > 50
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || b"-._".contains(&byte)
        })
    {
        return api_error(
            -1100,
            "Illegal characters found in parameter 'symbol'; legal range is '^[\\w\\-._&&[^a-z]]{1,50}$'.",
        );
    }
    if let Some(failure) = lock(&shared.symbol_failures)
        .get_mut(&symbol)
        .and_then(VecDeque::pop_front)
    {
        return failure.response();
    }
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
            "isSpotTradingAllowed": market.spot_trading_allowed,
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
    let mut shutdown = shared.shutdown.subscribe();
    shutdown.borrow_and_update();
    upgrade.on_upgrade(move |socket| {
        serve_stream(socket, streams, frames, disconnect, shutdown, shared)
    })
}

async fn serve_stream(
    mut socket: WebSocket,
    streams: HashSet<String>,
    mut frames: broadcast::Receiver<Frame>,
    mut disconnect: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<u64>,
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
    let mut ping_sequence: u64 = 0;
    // The payload of the unanswered ping, and when it was sent.
    let mut awaiting_pong: Option<(Bytes, Instant)> = None;
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
                if awaiting_pong.as_ref().is_some_and(|(_, since)| since.elapsed() > settings.pong_timeout) {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
                ping_sequence += 1;
                let payload = Bytes::from(ping_sequence.to_be_bytes().to_vec());
                if awaiting_pong.is_none() {
                    awaiting_pong = Some((payload.clone(), Instant::now()));
                }
                if socket.send(Message::Ping(payload)).await.is_err() {
                    break;
                }
            }
            message = socket.recv() => match message {
                Some(Ok(Message::Pong(payload))) => {
                    // Only a pong echoing the pending ping counts, as on Binance.
                    if awaiting_pong.as_ref().is_some_and(|(expected, _)| *expected == payload) {
                        shared.pongs.fetch_add(1, Ordering::SeqCst);
                        awaiting_pong = None;
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = shutdown.changed() => {
                let millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis());
                let event = json!({
                    "stream": "!serverShutdown",
                    "data": { "e": "serverShutdown", "E": millis },
                })
                .to_string();
                if socket.send(Message::Text(event.into())).await.is_err() {
                    break;
                }
                tokio::time::sleep(SHUTDOWN_GRACE).await;
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
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

/// Loopback clients only: on a devnet host bound to a routable address, no one
/// else gets to set the clearing price.
async fn set_quote(
    State(shared): State<Arc<Shared>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(query): Query<SetQuery>,
) -> Response {
    if !peer.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            "quotes can be set from localhost only",
        )
            .into_response();
    }
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

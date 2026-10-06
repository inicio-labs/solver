//! One reader connection, from handshake to exit.
//!
//! The reader subscribes to every symbol through one combined-stream URL, so
//! it sends no control messages. Each text frame is timestamped as soon as the
//! socket yields it, parsed, and stored as the reader's latest observation for
//! its symbol. Polling the socket continuously lets tungstenite answer pings;
//! the reader also flushes after each ping so the pong leaves at once.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::Connector;
use tokio_util::sync::CancellationToken;

use super::feed::{FeedMetrics, LogLimiter};
use super::market::Markets;
use super::rest::rate_limit;
use super::snapshot::Observation;
use super::ticker::parse_frame;

/// A reader's newest observation per subscribed symbol.
pub(super) type ReaderLatest = Vec<Option<Observation>>;

/// Largest accepted WebSocket frame or message; a `bookTicker` frame is ~200 B.
pub(super) const MAX_FRAME_BYTES: usize = 64 * 1024;
/// Bound on sending a pong or a close frame.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// A connection that delivered valid quotes for this long resets the backoff;
/// one that drops sooner keeps backing off, so a flapping endpoint cannot
/// drain the shared connection-attempt budget.
const STABLE_CONNECTION: Duration = Duration::from_secs(60);

pub(super) struct ReaderContext {
    pub(super) index: usize,
    /// Full combined-stream URL.
    pub(super) url: String,
    pub(super) markets: Arc<Markets>,
    pub(super) max_spread_bps: u32,
    pub(super) tls: Arc<rustls::ClientConfig>,
    pub(super) connect_timeout: Duration,
    /// Reconnect when no frame (not even a ping) arrives for this long.
    pub(super) idle_timeout: Duration,
    pub(super) latest: watch::Sender<ReaderLatest>,
    pub(super) metrics: Arc<FeedMetrics>,
    pub(super) cancel: CancellationToken,
}

/// Why a connection ended.
#[derive(Debug)]
pub(super) enum Exit {
    Cancelled,
    /// Planned renewal before Binance's 24-hour connection limit.
    Renewal,
    /// The handshake was refused with HTTP 429 or 418.
    RateLimited(Option<Duration>),
    Failed(String),
}

#[derive(Debug)]
pub(super) struct Outcome {
    pub(super) exit: Exit,
    /// Whether the connection delivered valid quotes for a stable period.
    pub(super) useful: bool,
}

impl Outcome {
    fn failed(exit: Exit) -> Self {
        Self {
            exit,
            useful: false,
        }
    }
}

/// The combined-stream URL subscribing to every symbol of `markets`.
pub(super) fn stream_url(endpoint: &str, markets: &Markets) -> String {
    let streams: Vec<&str> = (0..markets.symbols().len())
        .map(|index| markets.stream_name(index))
        .collect();
    format!(
        "{}/stream?streams={}",
        endpoint.trim_end_matches('/'),
        streams.join("/")
    )
}

fn handshake_exit(error: WsError) -> Exit {
    if let WsError::Http(response) = &error {
        if let Some(retry_after) = rate_limit(response.status(), response.headers()) {
            return Exit::RateLimited(retry_after);
        }
    }
    Exit::Failed(format!("handshake: {error}"))
}

/// Marks a reader connected for as long as it lives, including on unwind.
struct Connected<'a>(&'a AtomicBool);

impl<'a> Connected<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self(flag)
    }
}

impl Drop for Connected<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Run one connection until cancellation, failure, or `lifetime` elapses.
pub(super) async fn run_connection(context: Arc<ReaderContext>, lifetime: Duration) -> Outcome {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let connect = tokio_tungstenite::connect_async_tls_with_config(
        context.url.as_str(),
        Some(config),
        true,
        Some(Connector::Rustls(context.tls.clone())),
    );
    let mut socket = tokio::select! {
        _ = context.cancel.cancelled() => return Outcome::failed(Exit::Cancelled),
        connected = tokio::time::timeout(context.connect_timeout, connect) => match connected {
            Ok(Ok((socket, _response))) => socket,
            Ok(Err(error)) => return Outcome::failed(handshake_exit(error)),
            Err(_) => return Outcome::failed(Exit::Failed("handshake timed out".into())),
        },
    };
    let metrics = &context.metrics;
    let connected = Connected::new(&metrics.connected[context.index]);
    metrics.connections[context.index].fetch_add(1, Ordering::Relaxed);
    let opened = Instant::now();
    let mut delivered = false;
    let mut rejections = LogLimiter::default();
    let mut discards = LogLimiter::default();
    let renewal = tokio::time::sleep(lifetime);
    tokio::pin!(renewal);
    let exit = loop {
        let message = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => break Exit::Cancelled,
            _ = &mut renewal => break Exit::Renewal,
            message = tokio::time::timeout(context.idle_timeout, socket.next()) => message,
        };
        match message {
            Err(_) => break Exit::Failed(format!("no frame for {:?}", context.idle_timeout)),
            Ok(None) => break Exit::Failed("stream ended".into()),
            Ok(Some(Err(error))) => break Exit::Failed(error.to_string()),
            Ok(Some(Ok(Message::Text(text)))) => {
                let received_at = Instant::now();
                metrics.frames.fetch_add(1, Ordering::Relaxed);
                match parse_frame(&text, &context.markets, context.max_spread_bps, received_at) {
                    Ok(observation) => {
                        if let Err(rejection) = observation.mid {
                            metrics.rejected_quotes.fetch_add(1, Ordering::Relaxed);
                            if let Some(suppressed) = rejections.allow() {
                                let symbol = &context.markets.symbols()[observation.symbol];
                                let reason: &'static str = rejection.into();
                                tracing::warn!(
                                    reader = context.index,
                                    %symbol,
                                    update_id = observation.update_id,
                                    reason,
                                    suppressed,
                                    "Binance quote rejected; symbol unusable until a newer valid quote"
                                );
                            }
                        } else {
                            delivered = true;
                        }
                        context
                            .latest
                            .send_modify(|latest| latest[observation.symbol] = Some(observation));
                    }
                    Err(discard) => {
                        metrics.discarded_frames.fetch_add(1, Ordering::Relaxed);
                        if let Some(suppressed) = discards.allow() {
                            let reason: &'static str = discard.into();
                            tracing::warn!(
                                reader = context.index,
                                reason,
                                suppressed,
                                "Binance frame discarded"
                            );
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                // tungstenite queued the pong; send it now.
                match tokio::time::timeout(WRITE_TIMEOUT, socket.flush()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Exit::Failed(format!("pong: {error}")),
                    Err(_) => break Exit::Failed("pong timed out".into()),
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                break Exit::Failed(format!("closed by server: {frame:?}"))
            }
            Ok(Some(Ok(_))) => {}
        }
    };
    drop(connected);
    let _ = tokio::time::timeout(WRITE_TIMEOUT, socket.close(None)).await;
    Outcome {
        exit,
        useful: delivered && opened.elapsed() >= STABLE_CONNECTION,
    }
}

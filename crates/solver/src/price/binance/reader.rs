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
use super::rest::{is_rate_limited, retry_after};
use super::snapshot::Observation;
use super::ticker::{parse_frame, Frame, QuoteLimits};

/// A reader's newest observation per subscribed symbol.
pub(super) type ReaderLatest = Vec<Option<Observation>>;

/// Largest accepted WebSocket frame or message; a `bookTicker` frame is ~200 B.
pub(super) const MAX_FRAME_BYTES: usize = 64 * 1024;
/// Bound on sending a pong or a close frame.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) struct ReaderContext {
    /// Position in the feed's per-reader metrics.
    pub(super) index: usize,
    /// The configured stream endpoint; logs name the reader by it.
    pub(super) endpoint: String,
    /// Full combined-stream URL.
    pub(super) url: String,
    pub(super) markets: Arc<Markets>,
    pub(super) limits: QuoteLimits,
    pub(super) tls: Arc<rustls::ClientConfig>,
    pub(super) connect_timeout: Duration,
    /// Reconnect when no frame at all, not even a ping, arrives for this long.
    pub(super) idle_timeout: Duration,
    /// Reconnect when no quote arrives for this long: a stalled backend keeps
    /// answering pings, and a fresh connection usually lands elsewhere.
    pub(super) data_idle_timeout: Duration,
    /// A connection that delivered a valid quote and lasted this long counts
    /// as stable; see [`Exit::Failed`].
    pub(super) stable_after: Duration,
    /// Repeated warnings are logged at most once per this interval.
    pub(super) log_interval: Duration,
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
    /// Binance announced it is about to close the connection.
    ServerShutdown,
    /// The handshake was refused with HTTP 429 or 418.
    RateLimited(Option<Duration>),
    Failed {
        reason: String,
        /// Whether the connection delivered a valid quote and lasted the
        /// stable period, so its reader's backoff may reset.
        stable: bool,
    },
}

impl Exit {
    fn failed(reason: impl Into<String>) -> Self {
        Self::Failed {
            reason: reason.into(),
            stable: false,
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

fn handshake_exit(error: &WsError) -> Exit {
    match error {
        WsError::Http(response) if is_rate_limited(response.status()) => {
            Exit::RateLimited(retry_after(response.headers()))
        }
        _ => Exit::failed(format!("handshake: {error}")),
    }
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
pub(super) async fn run_connection(context: Arc<ReaderContext>, lifetime: Duration) -> Exit {
    let endpoint = context.endpoint.as_str();
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
        _ = context.cancel.cancelled() => return Exit::Cancelled,
        connected = tokio::time::timeout(context.connect_timeout, connect) => match connected {
            Ok(Ok((socket, _response))) => socket,
            Ok(Err(error)) => return handshake_exit(&error),
            Err(_) => return Exit::failed("handshake timed out"),
        },
    };
    let metrics = &context.metrics;
    let connected = Connected::new(&metrics.connected[context.index]);
    metrics.connections[context.index].fetch_add(1, Ordering::Relaxed);
    let opened = tokio::time::Instant::now();
    let mut last_frame = opened;
    let mut last_quote = opened;
    let mut delivered = false;
    let mut rejections = LogLimiter::new(context.log_interval);
    let mut discards = LogLimiter::new(context.log_interval);
    let renewal = tokio::time::sleep(lifetime);
    tokio::pin!(renewal);
    let exit = loop {
        let frame_deadline = last_frame + context.idle_timeout;
        let quote_deadline = last_quote + context.data_idle_timeout;
        let message = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => break Exit::Cancelled,
            _ = &mut renewal => break Exit::Renewal,
            message = tokio::time::timeout_at(frame_deadline.min(quote_deadline), socket.next()) => message,
        };
        let now = tokio::time::Instant::now();
        match message {
            Err(_) if now >= frame_deadline => {
                break Exit::failed(format!("no frame for {:?}", context.idle_timeout))
            }
            Err(_) => {
                break Exit::failed(format!(
                    "no quote for {:?} on a live connection",
                    context.data_idle_timeout
                ))
            }
            Ok(None) => break Exit::failed("stream ended"),
            Ok(Some(Err(error))) => break Exit::failed(error.to_string()),
            Ok(Some(Ok(Message::Text(text)))) => {
                let received_at = Instant::now();
                last_frame = now;
                metrics.frames[context.index].fetch_add(1, Ordering::Relaxed);
                match parse_frame(&text, &context.markets, context.limits, received_at) {
                    Ok(Frame::Quote(observation)) => {
                        match observation.mid {
                            Ok(_) => {
                                delivered = true;
                                last_quote = now;
                            }
                            Err(rejection) => {
                                metrics.rejected_quotes[context.index]
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Some(suppressed) = rejections.allow() {
                                    let symbol = &context.markets.symbols()[observation.symbol];
                                    tracing::warn!(
                                        endpoint,
                                        %symbol,
                                        update_id = observation.update_id,
                                        reason = %rejection,
                                        suppressed,
                                        "Binance quote rejected; symbol invalid until a newer valid quote"
                                    );
                                }
                            }
                        }
                        context
                            .latest
                            .send_modify(|latest| latest[observation.symbol] = Some(observation));
                    }
                    Ok(Frame::ServerShutdown) => break Exit::ServerShutdown,
                    Err(discard) => {
                        metrics.discarded_frames[context.index].fetch_add(1, Ordering::Relaxed);
                        if let Some(suppressed) = discards.allow() {
                            tracing::warn!(
                                endpoint,
                                reason = %discard,
                                suppressed,
                                "Binance frame discarded"
                            );
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                last_frame = now;
                // tungstenite queued the pong; send it now.
                match tokio::time::timeout(WRITE_TIMEOUT, socket.flush()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Exit::failed(format!("pong: {error}")),
                    Err(_) => break Exit::failed("pong timed out"),
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                break Exit::failed(format!("closed by server: {frame:?}"))
            }
            Ok(Some(Ok(_))) => last_frame = now,
        }
    };
    drop(connected);
    let _ = tokio::time::timeout(WRITE_TIMEOUT, socket.close(None)).await;
    match exit {
        Exit::Failed { reason, .. } => Exit::Failed {
            reason,
            stable: delivered && opened.elapsed() >= context.stable_after,
        },
        other => other,
    }
}

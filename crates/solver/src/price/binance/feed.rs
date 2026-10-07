//! The Binance price feed: one OS thread with its own Tokio runtime.
//!
//! ```text
//! Reader A ─┐
//!           ├─> publisher ─> watch<Arc<PriceSnapshot>> ─┬─> matcher
//! Reader B ─┘                                          └─> price API
//! ```
//!
//! Startup checks every configured market once through `exchangeInfo`. A
//! market Binance rejects, or a lookup that still fails at the validation
//! timeout, fails startup with the full list, so the configuration is fixed
//! before the solver runs. Then two supervised readers keep their own
//! connections to the markets and the publisher merges their observations. A
//! reader that fails or panics restarts alone; any other failure of the feed,
//! including a panic, stops the solver.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use super::market::{Listing, MarketPlan, Markets, Symbol};
use super::reader::{run_connection, stream_url, Exit, ReaderContext, ReaderLatest};
use super::rest::{fetch_listing, LookupError, MAX_RETRY_AFTER};
use super::snapshot::{Offer, PriceSnapshot, SymbolQuote};
use super::ticker::QuoteLimits;

/// The feed keeps this many readers, each on its own configured endpoint;
/// logs and metrics identify a reader by that endpoint.
pub(crate) const READERS: usize = 2;
/// Binance counts connection attempts per IP over five minutes.
const ATTEMPT_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Exponential backoff with jitter, never longer than `max_delay`.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub min_delay: Duration,
    pub max_delay: Duration,
}

/// Delays drawn from a [`RetryPolicy`]: the step doubles from `min_delay`
/// up to `max_delay`, and each delay is a random 50–100% of its step, so two
/// readers that failed together do not retry together, even at the cap.
struct Backoff {
    policy: RetryPolicy,
    step: u32,
}

impl Backoff {
    fn new(policy: RetryPolicy) -> Self {
        Self { policy, step: 0 }
    }

    fn next(&mut self) -> Duration {
        // The step doubles on every failure: min, 2·min, 4·min, … up to max.
        let step = self
            .policy
            .min_delay
            .saturating_mul(2u32.saturating_pow(self.step))
            .min(self.policy.max_delay);
        self.step = self.step.saturating_add(1);
        // The random part: wait 50–100% of the step.
        step.mul_f64(0.5 + rand::random::<f64>() / 2.0)
    }

    fn reset(&mut self) {
        self.step = 0;
    }
}

/// Runtime settings of the feed; the operator-facing documentation of each
/// value is on the `[binance]` configuration section.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// Stream base URL of each reader.
    pub stream_endpoints: [String; READERS],
    pub rest_endpoint: String,
    pub limits: QuoteLimits,
    /// A quote is fresh while younger than this, for clearing and wallet
    /// valuation alike.
    pub quote_ttl: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Reconnect when no frame at all, not even a ping, arrives for this long:
    /// the connection is dead even if the socket has not noticed.
    pub idle_timeout: Duration,
    /// Reconnect when no quote arrives for this long although Binance still
    /// answers pings. A stalled stream server keeps the socket alive but sends
    /// no prices; a new connection usually lands on a healthy one.
    pub data_idle_timeout: Duration,
    /// Longest planned connection; each lasts a random 50–100% of it.
    pub connection_lifetime: Duration,
    pub retry: RetryPolicy,
    /// Connection attempts per five minutes, both readers together; positive.
    pub max_connection_attempts: usize,
    /// At startup, how long failed `exchangeInfo` lookups are retried before
    /// startup fails.
    pub validation_timeout: Duration,
    /// A connection that delivered a valid quote and lasted this long resets
    /// its reader's backoff; one that drops sooner keeps backing off, so a
    /// flapping endpoint cannot drain the connection-attempt budget.
    pub stable_connection: Duration,
    /// How long the feed's tasks get to stop before they are aborted.
    pub shutdown_timeout: Duration,
    /// Repeated warnings (rejected quotes, reconnects) are logged at most once
    /// per this interval, with a count of the ones skipped.
    pub log_interval: Duration,
}

/// Feed counters for `/metrics`. Per-reader counters are indexed like
/// [`FeedConfig::stream_endpoints`].
#[derive(Debug, Default)]
pub struct FeedMetrics {
    /// Each reader's stream endpoint, the label of its metrics.
    pub(crate) endpoints: [String; READERS],
    pub(crate) connected: [AtomicBool; READERS],
    /// Successful handshakes per reader.
    pub(crate) connections: [AtomicU64; READERS],
    pub(crate) frames: [AtomicU64; READERS],
    pub(crate) discarded_frames: [AtomicU64; READERS],
    pub(crate) rejected_quotes: [AtomicU64; READERS],
    pub(crate) reader_panics: AtomicU64,
    /// `serverShutdown` events received, each followed by a prompt reconnect.
    pub(crate) server_shutdowns: AtomicU64,
    pub(crate) publications: AtomicU64,
    /// Updates with the ID of the published quote but different content.
    pub(crate) conflicting_updates: AtomicU64,
    /// Receipt-to-publication delay of the newest quote in the last publication.
    pub(crate) last_publish_delay_us: AtomicU64,
    /// Failed `exchangeInfo` lookups.
    pub(crate) lookup_failures: AtomicU64,
    /// Attempts the shared connection budget delayed.
    pub(crate) budget_waits: AtomicU64,
    /// Symbols the readers subscribe.
    pub(crate) markets_confirmed: AtomicU64,
    /// Configured uses of a symbol that is subscribed but not trading.
    pub(crate) halted_markets: AtomicU64,
}

impl FeedMetrics {
    /// Append the feed's metrics in Prometheus text format: reader health,
    /// market confirmation, publication, and each symbol's quote state. A
    /// quote is `valid` when its newest update passed validation and `fresh`
    /// when it is also younger than the clearing TTL; alerts belong on `fresh`.
    pub(crate) fn render(&self, snapshot: &PriceSnapshot, body: &mut String) {
        use std::fmt::Write;

        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            // The position keeps two readers on one endpoint apart.
            let reader = format!("reader=\"{index}\",endpoint=\"{endpoint}\"");
            let connected = u8::from(self.connected[index].load(Ordering::Relaxed));
            let _ = writeln!(body, "solver_price_feed_connected{{{reader}}} {connected}");
            for (metric, counters) in [
                ("solver_price_feed_connections_total", &self.connections),
                ("solver_price_feed_frames_total", &self.frames),
                (
                    "solver_price_feed_discarded_frames_total",
                    &self.discarded_frames,
                ),
                (
                    "solver_price_feed_rejected_quotes_total",
                    &self.rejected_quotes,
                ),
            ] {
                let _ = writeln!(body, "{metric}{{{reader}}} {}", load(&counters[index]));
            }
        }
        for (metric, counter) in [
            ("solver_price_feed_reader_panics_total", &self.reader_panics),
            (
                "solver_price_feed_server_shutdowns_total",
                &self.server_shutdowns,
            ),
            ("solver_price_feed_publications_total", &self.publications),
            (
                "solver_price_feed_conflicting_updates_total",
                &self.conflicting_updates,
            ),
            (
                "solver_price_feed_lookup_failures_total",
                &self.lookup_failures,
            ),
            ("solver_price_feed_budget_waits_total", &self.budget_waits),
        ] {
            let _ = writeln!(body, "{metric} {}", load(counter));
        }
        for (state, gauge) in [
            ("confirmed", &self.markets_confirmed),
            ("halted", &self.halted_markets),
        ] {
            let _ = writeln!(
                body,
                "solver_price_feed_markets{{state=\"{state}\"}} {}",
                load(gauge)
            );
        }
        let delay = Duration::from_micros(load(&self.last_publish_delay_us));
        let _ = writeln!(
            body,
            "solver_price_feed_publish_delay_seconds {}",
            delay.as_secs_f64()
        );
        let now = Instant::now();
        for (symbol, quote) in snapshot.quotes() {
            let (valid, fresh, age) = match quote {
                SymbolQuote::Valid(quote) => {
                    let age = now.saturating_duration_since(quote.received_at);
                    (1, u8::from(age < snapshot.ttl()), Some(age.as_secs_f64()))
                }
                SymbolQuote::Missing | SymbolQuote::Invalid { .. } => (0, 0, None),
            };
            let _ = writeln!(
                body,
                "solver_price_quote_valid{{symbol=\"{symbol}\"}} {valid}"
            );
            let _ = writeln!(
                body,
                "solver_price_quote_fresh{{symbol=\"{symbol}\"}} {fresh}"
            );
            if let Some(age) = age {
                let _ = writeln!(
                    body,
                    "solver_price_quote_age_seconds{{symbol=\"{symbol}\"}} {age}"
                );
            }
        }
    }
}

impl FeedMetrics {
    /// Counters for readers on `endpoints`.
    pub fn new(endpoints: [String; READERS]) -> Self {
        Self {
            endpoints,
            ..Self::default()
        }
    }
}

/// Failures that stop the feed, and with it the solver.
#[derive(Debug, Error)]
pub(crate) enum FeedError {
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    #[error("HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Binance rejected configured markets: {0}")]
    MarketsRejected(String),
    #[error(
        "Binance did not answer for {pending} symbol(s) within the validation timeout: {error}"
    )]
    BinanceUnreachable { pending: usize, error: String },
    #[error("exchangeInfo lookup for {symbol} cannot succeed: {error}")]
    LookupFailed { symbol: Symbol, error: String },
    #[error("every price receiver is gone")]
    OutputClosed,
    #[error("a reader channel closed")]
    ReaderChannelClosed,
    #[error("{0} stopped unexpectedly")]
    TaskStopped(String),
    #[error("{0} panicked")]
    TaskPanicked(String),
}

/// Allows one log line per `interval` and counts the ones it suppressed.
#[derive(Debug)]
pub(super) struct LogLimiter {
    interval: Duration,
    last: Option<Instant>,
    suppressed: u64,
}

impl LogLimiter {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: None,
            suppressed: 0,
        }
    }

    /// `Some(suppressed since the last line)` when a line may be logged now.
    pub(super) fn allow(&mut self) -> Option<u64> {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < self.interval)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Sleep for `wait`; `false` if `cancel` fired first.
async fn sleep_unless_cancelled(wait: Duration, cancel: &CancellationToken) -> bool {
    cancel
        .run_until_cancelled(tokio::time::sleep(wait))
        .await
        .is_some()
}

/// Connection-attempt budget shared by both readers, and the server-imposed
/// cooldown that also holds back `exchangeInfo` lookups.
struct Gate {
    /// Most connection attempts, both readers together, within `window`
    /// (`binance.max_connection_attempts`).
    limit: usize,
    /// Binance's counting window for connection attempts: five minutes.
    window: Duration,
    state: Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    attempts: VecDeque<tokio::time::Instant>,
    cooldown_until: Option<tokio::time::Instant>,
}

impl Gate {
    fn new(limit: usize, window: Duration) -> Self {
        Self {
            limit,
            window,
            state: Mutex::new(GateState::default()),
        }
    }

    /// Time left on the cooldown, if any.
    fn cooldown_left(&self) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .cooldown_until
            .filter(|until| *until > now)
            .map(|until| until - now)
    }

    /// Take an attempt slot, or say how long until one frees up.
    fn take_slot(&self) -> Result<(), Duration> {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state
            .attempts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= self.window)
        {
            state.attempts.pop_front();
        }
        if state.attempts.len() < self.limit {
            state.attempts.push_back(now);
            return Ok(());
        }
        Err(state.attempts[0] + self.window - now)
    }

    /// Wait out the cooldown; `false` when cancelled.
    async fn wait_cooldown(&self, cancel: &CancellationToken) -> bool {
        while let Some(wait) = self.cooldown_left() {
            if !sleep_unless_cancelled(wait, cancel).await {
                return false;
            }
        }
        !cancel.is_cancelled()
    }

    /// Wait out the cooldown and take an attempt slot; `false` when cancelled.
    async fn acquire_attempt(&self, cancel: &CancellationToken, metrics: &FeedMetrics) -> bool {
        loop {
            if !self.wait_cooldown(cancel).await {
                return false;
            }
            match self.take_slot() {
                Ok(()) => return true,
                Err(wait) => {
                    metrics.budget_waits.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(?wait, "Binance connection budget exhausted; waiting");
                    if !sleep_unless_cancelled(wait, cancel).await {
                        return false;
                    }
                }
            }
        }
    }

    /// Send nothing for at least `wait` (HTTP 429/418 `Retry-After`, or a WAF
    /// block), at most Binance's longest ban of three days.
    fn cool_down(&self, wait: Duration) {
        let until = tokio::time::Instant::now() + wait.min(MAX_RETRY_AFTER);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.cooldown_until = Some(
            state
                .cooldown_until
                .map_or(until, |current| current.max(until)),
        );
    }
}

/// The TLS setup for the WebSocket readers: Rustls with the `ring` provider
/// and the webpki root certificates. Libraries such as tokio-tungstenite can
/// build this themselves, but only from a process-wide default provider, which
/// fails at runtime as soon as any dependency also compiles in `aws-lc-rs`.
/// Naming the provider here keeps the readers independent of that.
fn tls_config() -> Result<Arc<rustls::ClientConfig>, FeedError> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Look every configured symbol up through `exchangeInfo` and resolve the
/// markets. A lookup that failed for a temporary reason (network, timeout,
/// rate limit, Binance 5xx) is retried with backoff, respecting Binance's
/// cooldowns, until the validation timeout; any other failure (a wrong path,
/// a bad request, an unreadable answer) fails startup at once. Binance's own
/// answers are final:
/// any rejected market fails startup, listed with all the others. A market
/// that is temporarily not trading is subscribed with a warning. `None` when
/// cancelled.
async fn confirm_markets(
    plan: &MarketPlan,
    http: &reqwest::Client,
    config: &FeedConfig,
    gate: &Gate,
    metrics: &FeedMetrics,
    cancel: &CancellationToken,
) -> Result<Option<Markets>, FeedError> {
    let deadline = tokio::time::Instant::now() + config.validation_timeout;
    let mut backoff = Backoff::new(config.retry);
    let mut listings: HashMap<Symbol, Option<Listing>> = HashMap::new();
    let mut pending: Vec<Symbol> = plan.symbols().into_iter().collect();
    let unreachable = |pending: usize, error: Option<LookupError>| FeedError::BinanceUnreachable {
        pending,
        error: error.map_or_else(|| "no answer".to_string(), |error| error.to_string()),
    };
    let mut last_error = None;
    while !pending.is_empty() {
        let mut failed = Vec::new();
        let mut symbols = pending.into_iter();
        while let Some(symbol) = symbols.next() {
            let lookup = async {
                if !gate.wait_cooldown(cancel).await {
                    return None;
                }
                cancel
                    .run_until_cancelled(fetch_listing(http, &config.rest_endpoint, &symbol))
                    .await
            };
            match tokio::time::timeout_at(deadline, lookup).await {
                Err(_) => {
                    let pending = failed.len() + 1 + symbols.len();
                    return Err(unreachable(pending, last_error));
                }
                Ok(None) => return Ok(None),
                Ok(Some(Ok(listing))) => {
                    listings.insert(symbol, listing);
                }
                Ok(Some(Err(error))) => {
                    metrics.lookup_failures.fetch_add(1, Ordering::Relaxed);
                    if !error.is_transient() {
                        return Err(FeedError::LookupFailed {
                            symbol,
                            error: error.to_string(),
                        });
                    }
                    if error.is_rate_limited() {
                        gate.cool_down(error.retry_after().unwrap_or_else(|| backoff.next()));
                    }
                    failed.push(symbol);
                    last_error = Some(error);
                }
            }
        }
        pending = failed;
        if pending.is_empty() {
            break;
        }
        let wait = backoff.next();
        if tokio::time::Instant::now() + wait >= deadline {
            return Err(unreachable(pending.len(), last_error));
        }
        if let Some(error) = &last_error {
            tracing::warn!(%error, pending = pending.len(), ?wait, "Binance exchangeInfo lookups failed; retrying");
        }
        if !sleep_unless_cancelled(wait, cancel).await {
            return Ok(None);
        }
    }
    let (markets, issues) = plan.resolve(&listings);
    let (rejected, halted): (Vec<_>, Vec<_>) = issues.iter().partition(|issue| issue.rejects());
    if !rejected.is_empty() {
        let mut list = String::new();
        for issue in &rejected {
            let _ = write!(list, "{}{issue}", if list.is_empty() { "" } else { "; " });
        }
        return Err(FeedError::MarketsRejected(list));
    }
    for issue in &halted {
        tracing::warn!(%issue, "Binance market not trading; subscribed, paused until it trades");
    }
    metrics
        .markets_confirmed
        .store(markets.symbols().len() as u64, Ordering::Relaxed);
    metrics
        .halted_markets
        .store(halted.len() as u64, Ordering::Relaxed);
    tracing::info!(
        symbols = markets.symbols().len(),
        halted = halted.len(),
        "Binance markets confirmed"
    );
    Ok(Some(markets))
}

/// Merge both readers' latest observations and publish on every change.
async fn publish(
    mut book: PriceSnapshot,
    mut readers: [watch::Receiver<ReaderLatest>; READERS],
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) -> Result<(), FeedError> {
    // The newest observation already offered, per reader and symbol, so a
    // conflict is counted once.
    let mut offered: [Vec<Option<(u64, Instant)>>; READERS] =
        std::array::from_fn(|_| vec![None; book.markets().symbols().len()]);
    loop {
        let [first, second] = &mut readers;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            changed = first.changed() => changed.map_err(|_| FeedError::ReaderChannelClosed)?,
            changed = second.changed() => changed.map_err(|_| FeedError::ReaderChannelClosed)?,
        }
        let mut newest: Option<Instant> = None;
        for (reader, offered) in readers.iter_mut().zip(&mut offered) {
            let latest = reader.borrow_and_update();
            for (observation, offered) in latest.iter().zip(offered.iter_mut()) {
                let Some(observation) = observation else {
                    continue;
                };
                let identity = (observation.update_id, observation.received_at);
                if *offered == Some(identity) {
                    continue;
                }
                *offered = Some(identity);
                match book.offer(observation) {
                    Offer::Accepted => newest = newest.max(Some(observation.received_at)),
                    Offer::Conflict => {
                        metrics.conflicting_updates.fetch_add(1, Ordering::Relaxed);
                    }
                    Offer::NotNewer => {}
                }
            }
        }
        let Some(newest) = newest else { continue };
        output
            .send(Arc::new(book.clone()))
            .map_err(|_| FeedError::OutputClosed)?;
        metrics.publications.fetch_add(1, Ordering::Relaxed);
        let delay = u64::try_from(newest.elapsed().as_micros()).unwrap_or(u64::MAX);
        metrics
            .last_publish_delay_us
            .store(delay, Ordering::Relaxed);
    }
}

/// A planned connection lifetime: a random 50–100% of `longest`, so the two
/// readers' renewals stay apart.
fn planned_lifetime(longest: Duration) -> Duration {
    let half = longest / 2;
    half + half.mul_f64(rand::random::<f64>())
}

/// Keep one reader connected: admit each attempt through the shared gate,
/// run the connection as its own task, and finish that task before starting
/// the next. Returns only when cancelled.
#[allow(clippy::too_many_arguments)]
async fn supervise_reader<F, Fut>(
    endpoint: String,
    connect: F,
    gate: Arc<Gate>,
    retry: RetryPolicy,
    longest_lifetime: Duration,
    log_interval: Duration,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) where
    F: Fn(Duration) -> Fut,
    Fut: Future<Output = Exit> + Send + 'static,
{
    let mut backoff = Backoff::new(retry);
    let mut log = LogLimiter::new(log_interval);
    loop {
        if !gate.acquire_attempt(&cancel, &metrics).await {
            return;
        }
        // Aborted with this supervisor, so a connection never outlives it.
        let connection =
            AbortOnDropHandle::new(tokio::spawn(connect(planned_lifetime(longest_lifetime))));
        let reason = match connection.await {
            Ok(Exit::Cancelled) => return,
            Ok(Exit::Renewal) => {
                tracing::info!(%endpoint, "renewing Binance connection");
                backoff.reset();
                continue;
            }
            Ok(Exit::ServerShutdown) => {
                metrics.server_shutdowns.fetch_add(1, Ordering::Relaxed);
                tracing::info!(%endpoint, "Binance announced a shutdown; reconnecting");
                backoff.reset();
                continue;
            }
            Ok(Exit::RateLimited(retry_after)) => {
                // The next attempt waits out the cooldown at the gate.
                let wait = retry_after.unwrap_or_else(|| backoff.next());
                gate.cool_down(wait);
                if let Some(suppressed) = log.allow() {
                    tracing::warn!(
                        %endpoint,
                        ?wait,
                        suppressed,
                        "Binance handshake rate limited; cooling down"
                    );
                }
                continue;
            }
            Ok(Exit::Failed { reason, stable }) => {
                if stable {
                    backoff.reset();
                }
                reason
            }
            Err(error) if error.is_panic() => {
                metrics.reader_panics.fetch_add(1, Ordering::Relaxed);
                let payload = error.into_panic();
                let message = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                tracing::error!(
                    %endpoint,
                    panic = message,
                    "Binance reader task panicked; restarting it"
                );
                "reader task panicked".to_string()
            }
            // The runtime is shutting down.
            Err(_) => return,
        };
        let delay = backoff.next();
        if let Some(suppressed) = log.allow() {
            tracing::warn!(%endpoint, %reason, ?delay, suppressed, "Binance reader disconnected; reconnecting");
        }
        if !sleep_unless_cancelled(delay, &cancel).await {
            return;
        }
    }
}

/// Test hook: a feed configured with this REST endpoint panics at start, so a
/// test can check what a panic on the feed thread does to the solver.
#[cfg(test)]
pub(super) static PANIC_ON_ENDPOINT: Mutex<Option<String>> = Mutex::new(None);

/// Run the feed until `cancel`. `ready` gets the outcome of the startup
/// market check. An error is irrecoverable: the caller stops the solver.
pub(super) async fn run_feed(
    config: FeedConfig,
    plan: MarketPlan,
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), FeedError> {
    #[cfg(test)]
    if PANIC_ON_ENDPOINT
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .as_deref()
        == Some(config.rest_endpoint.as_str())
    {
        panic!("price feed test panic");
    }
    let tls = tls_config()?;
    let http = reqwest::Client::builder()
        .use_preconfigured_tls((*tls).clone())
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout)
        .build()?;
    let gate = Arc::new(Gate::new(config.max_connection_attempts, ATTEMPT_WINDOW));
    let markets = match confirm_markets(&plan, &http, &config, &gate, &metrics, &cancel).await {
        Ok(Some(markets)) => Arc::new(markets),
        Ok(None) => return Ok(()),
        Err(error) => {
            let _ = ready.send(Err(error.to_string()));
            return Err(error);
        }
    };
    let _ = ready.send(Ok(()));
    let book = PriceSnapshot::new(markets.clone(), config.quote_ttl);
    output
        .send(Arc::new(book.clone()))
        .map_err(|_| FeedError::OutputClosed)?;
    if markets.symbols().is_empty() {
        // Nothing to subscribe: connecting would only spend the budget.
        tracing::warn!("no Binance market configured; every pair stays unpriced");
        cancel.cancelled().await;
        return Ok(());
    }

    // Stopping one task stops the others; the solver's token stops them all.
    let stop = cancel.child_token();
    let mut tasks: JoinSet<Result<(), FeedError>> = JoinSet::new();
    let mut names = HashMap::new();
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..READERS)
        .map(|_| watch::channel(vec![None; markets.symbols().len()]))
        .unzip();
    for (index, latest) in senders.into_iter().enumerate() {
        let endpoint = config.stream_endpoints[index].clone();
        let context = Arc::new(ReaderContext {
            index,
            endpoint: endpoint.clone(),
            url: stream_url(&endpoint, &markets),
            markets: markets.clone(),
            limits: config.limits,
            tls: tls.clone(),
            connect_timeout: config.connect_timeout,
            idle_timeout: config.idle_timeout,
            data_idle_timeout: config.data_idle_timeout,
            stable_after: config.stable_connection,
            log_interval: config.log_interval,
            latest,
            metrics: metrics.clone(),
            cancel: stop.clone(),
        });
        let task = format!("Binance reader for {endpoint}");
        let supervisor = supervise_reader(
            endpoint,
            move |lifetime| run_connection(context.clone(), lifetime),
            gate.clone(),
            config.retry,
            config.connection_lifetime,
            config.log_interval,
            metrics.clone(),
            stop.clone(),
        );
        let id = tasks
            .spawn(async move {
                supervisor.await;
                Ok(())
            })
            .id();
        names.insert(id, task);
    }
    let receivers: [watch::Receiver<ReaderLatest>; READERS] = receivers
        .try_into()
        .unwrap_or_else(|_| unreachable!("one receiver per reader"));
    let publisher = publish(book, receivers, output, metrics, stop.clone());
    names.insert(tasks.spawn(publisher).id(), "price publisher".to_string());

    let outcome = tokio::select! {
        biased;
        _ = cancel.cancelled() => Ok(()),
        Some(joined) = tasks.join_next_with_id() => Err(match joined {
            Ok((_, Err(error))) => error,
            Ok((id, Ok(()))) => FeedError::TaskStopped(task_name(&names, &id)),
            Err(error) => FeedError::TaskPanicked(task_name(&names, &error.id())),
        }),
    };
    stop.cancel();
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(config.shutdown_timeout, drain)
        .await
        .is_err()
    {
        tracing::warn!("price feed tasks did not stop in time; aborting them");
        tasks.abort_all();
    }
    outcome
}

fn task_name(names: &HashMap<tokio::task::Id, String>, id: &tokio::task::Id) -> String {
    names
        .get(id)
        .cloned()
        .unwrap_or_else(|| "price feed task".to_string())
}

/// The outcome of the feed's startup market check; `Err` lists what to fix.
pub(crate) type FeedReady = oneshot::Receiver<Result<(), String>>;

/// Start the feed on its own OS thread. The receiver gets the outcome of the
/// startup market check: `Err` lists what to fix in the configuration.
/// However the thread ends — an irrecoverable error, a panic, or after
/// `cancel` — it cancels `cancel`, so a failed feed stops the solver.
pub(crate) fn spawn_price_feed_thread(
    config: FeedConfig,
    plan: MarketPlan,
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) -> anyhow::Result<(thread::JoinHandle<()>, FeedReady)> {
    let shutdown_timeout = config.shutdown_timeout;
    let (ready, ready_rx) = oneshot::channel();
    thread::Builder::new()
        .name("price-feed".into())
        .spawn(move || {
            let _stop_solver = cancel.clone().drop_guard();
            // Built here, so a failure to spawn the thread never drops a
            // runtime on the caller's async thread.
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "cannot build the price-feed runtime; stopping the solver");
                    return;
                }
            };
            let feed = run_feed(config, plan, output, metrics, cancel, ready);
            if let Err(error) = runtime.block_on(feed) {
                tracing::error!(%error, "price feed failed; stopping the solver");
            }
            // Bounded even if a DNS lookup is stuck on a blocking thread.
            runtime.shutdown_timeout(shutdown_timeout);
        })
        .context("spawn price-feed thread")
        .map(|thread| (thread, ready_rx))
}

#[cfg(test)]
mod tests;

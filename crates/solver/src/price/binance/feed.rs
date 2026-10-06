//! The Binance price feed: one OS thread with its own Tokio runtime.
//!
//! ```text
//! Reader A ─┐
//!           ├─> publisher ─> watch<Arc<PriceSnapshot>> ─┬─> matcher
//! Reader B ─┘                                          └─> price API
//! ```
//!
//! Startup first confirms every configured market through `exchangeInfo`,
//! retrying transient failures; until then the published snapshot is empty and
//! every pair is paused, but the solver runs. Then two supervised readers keep
//! their own connections to the same symbols and the publisher merges their
//! observations. A reader that fails or panics restarts alone; any other
//! failure of the feed, including a panic, stops the solver.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use super::market::{MarketPlan, Markets, Symbol};
use super::reader::{run_connection, stream_url, Exit, Outcome, ReaderContext, ReaderLatest};
use super::rest::fetch_listing;
use super::snapshot::{PriceSnapshot, QuoteBook};

/// Reader names in logs and metrics, by index.
pub(crate) const READER_NAMES: [&str; 2] = ["a", "b"];
/// Connection attempts are counted over this window, as Binance limits them.
const ATTEMPT_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Largest accepted `exchangeInfo` body for one symbol.
const MAX_BODY_BYTES: usize = 256 * 1024;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Exponential backoff with jitter, never longer than `max_delay`.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub min_delay: Duration,
    pub max_delay: Duration,
}

/// Delays drawn from a [`RetryPolicy`].
struct Backoff {
    policy: RetryPolicy,
    delays: ExponentialBackoff,
}

impl Backoff {
    fn new(policy: RetryPolicy) -> Self {
        let delays = ExponentialBuilder::default()
            .with_min_delay(policy.min_delay)
            .with_max_delay(policy.max_delay)
            .with_factor(2.0)
            .with_jitter()
            .without_max_times()
            .build();
        Self { policy, delays }
    }

    /// The next delay; jitter can exceed `max_delay`, so it is capped here.
    fn next(&mut self) -> Duration {
        let max = self.policy.max_delay;
        self.delays.next().map_or(max, |delay| delay.min(max))
    }

    fn reset(&mut self) {
        *self = Self::new(self.policy);
    }
}

/// Runtime settings of the feed.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// Stream base URL of reader A and of reader B.
    pub stream_endpoints: [String; 2],
    pub rest_endpoint: String,
    /// Widest accepted spread in bps, inclusive.
    pub max_spread_bps: u32,
    /// A quote is usable while younger than this.
    pub quote_ttl: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    /// Longest planned connection; each lasts a random 50–100% of it.
    pub connection_lifetime: Duration,
    pub retry: RetryPolicy,
    /// Connection attempts per five minutes, both readers together; positive.
    pub max_connection_attempts: usize,
}

/// Feed counters for `/metrics`.
#[derive(Debug, Default)]
pub struct FeedMetrics {
    pub(crate) connected: [AtomicBool; 2],
    /// Successful handshakes per reader.
    pub(crate) connections: [AtomicU64; 2],
    pub(crate) reader_panics: AtomicU64,
    pub(crate) frames: AtomicU64,
    pub(crate) discarded_frames: AtomicU64,
    pub(crate) rejected_quotes: AtomicU64,
    pub(crate) publications: AtomicU64,
    /// Receipt-to-publication delay of the newest quote in the last publication.
    pub(crate) last_publish_delay_us: AtomicU64,
    /// Failed `exchangeInfo` lookups.
    pub(crate) lookup_failures: AtomicU64,
    /// Configured markets Binance did not confirm.
    pub(crate) rejected_markets: AtomicU64,
}

/// Failures that stop the feed, and with it the solver.
#[derive(Debug, Error)]
pub(crate) enum FeedError {
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    #[error("HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("every price receiver is gone")]
    OutputClosed,
    #[error("a reader channel closed")]
    ReaderChannelClosed,
    #[error("{0} stopped unexpectedly")]
    TaskStopped(&'static str),
    #[error("{0} panicked")]
    TaskPanicked(&'static str),
}

/// Allows one log line per [`LOG_INTERVAL`] and counts the ones it suppressed.
#[derive(Debug, Default)]
pub(super) struct LogLimiter {
    last: Option<Instant>,
    suppressed: u64,
}

impl LogLimiter {
    /// `Some(suppressed since the last line)` when a line may be logged now.
    pub(super) fn allow(&mut self) -> Option<u64> {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < LOG_INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Connection-attempt budget shared by both readers, and the server-imposed
/// cooldown that also holds back `exchangeInfo` lookups.
struct Gate {
    limit: usize,
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

    /// How long to wait before the next request; with `attempt`, a free slot
    /// is also required and taken when no wait is needed.
    fn wait_time(&self, attempt: bool) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(until) = state.cooldown_until.filter(|until| *until > now) {
            return Some(until - now);
        }
        if !attempt {
            return None;
        }
        while state
            .attempts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= self.window)
        {
            state.attempts.pop_front();
        }
        if state.attempts.len() < self.limit {
            state.attempts.push_back(now);
            return None;
        }
        Some(state.attempts[0] + self.window - now)
    }

    /// Wait until a request may be sent; `false` when cancelled.
    async fn wait(&self, attempt: bool, cancel: &CancellationToken) -> bool {
        while let Some(wait) = self.wait_time(attempt) {
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = tokio::time::sleep(wait) => {}
            }
        }
        !cancel.is_cancelled()
    }

    /// Send nothing for at least `wait` (HTTP 429/418 `Retry-After`).
    fn cool_down(&self, wait: Duration) {
        let until = tokio::time::Instant::now() + wait;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.cooldown_until = Some(
            state
                .cooldown_until
                .map_or(until, |current| current.max(until)),
        );
    }
}

/// Explicit Rustls setup: the `ring` provider and the webpki root set, not
/// whatever another dependency happens to install process-wide.
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

/// Confirm every planned symbol through `exchangeInfo`. Each pass tries every
/// pending symbol, so one failing symbol does not hold back the others; a
/// transient failure is retried after a backoff, any other rejects only its
/// symbol. `None` when cancelled.
async fn validate_markets(
    plan: &MarketPlan,
    http: &reqwest::Client,
    config: &FeedConfig,
    gate: &Gate,
    metrics: &FeedMetrics,
    cancel: &CancellationToken,
) -> Option<Markets> {
    let mut pending: Vec<Symbol> = plan.symbols().into_iter().collect();
    let mut listings = HashMap::new();
    let mut backoff = Backoff::new(config.retry);
    let mut log = LogLimiter::default();
    while !pending.is_empty() {
        let attempted = pending.len();
        let mut retry = Vec::new();
        let mut last_error = None;
        for symbol in pending {
            if !gate.wait(false, cancel).await {
                return None;
            }
            let lookup = tokio::select! {
                _ = cancel.cancelled() => return None,
                lookup = fetch_listing(http, &config.rest_endpoint, &symbol, MAX_BODY_BYTES) => lookup,
            };
            match lookup {
                Ok(listing) => {
                    listings.insert(symbol, listing);
                }
                Err(error) => {
                    metrics.lookup_failures.fetch_add(1, Ordering::Relaxed);
                    if error.is_transient() {
                        if let Some(wait) = error.retry_after() {
                            gate.cool_down(wait);
                        }
                        last_error = Some(error);
                        retry.push(symbol);
                    } else {
                        tracing::error!(%symbol, %error, "Binance exchangeInfo lookup cannot succeed");
                        listings.insert(symbol, None);
                    }
                }
            }
        }
        if retry.len() < attempted {
            backoff.reset();
        }
        pending = retry;
        if let Some(error) = last_error {
            let delay = backoff.next();
            if let Some(suppressed) = log.allow() {
                tracing::warn!(%error, ?delay, pending = pending.len(), suppressed, "Binance exchangeInfo lookups failed; retrying");
            }
            tokio::select! {
                _ = cancel.cancelled() => return None,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
    let (markets, issues) = plan.resolve(&listings);
    metrics
        .rejected_markets
        .store(issues.len() as u64, Ordering::Relaxed);
    for issue in &issues {
        tracing::error!(%issue, "Binance market rejected; it stays unavailable until restart");
    }
    tracing::info!(
        symbols = markets.symbols().len(),
        rejected = issues.len(),
        "Binance markets confirmed"
    );
    Some(markets)
}

/// Merge both readers' latest observations and publish on every change.
async fn publish(
    mut book: QuoteBook,
    mut readers: [watch::Receiver<ReaderLatest>; 2],
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) -> Result<(), FeedError> {
    loop {
        let [first, second] = &mut readers;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            changed = first.changed() => changed.map_err(|_| FeedError::ReaderChannelClosed)?,
            changed = second.changed() => changed.map_err(|_| FeedError::ReaderChannelClosed)?,
        }
        let mut newest: Option<Instant> = None;
        for reader in &mut readers {
            let latest = reader.borrow_and_update();
            for observation in latest.iter().flatten() {
                if book.offer(observation) {
                    newest = newest.max(Some(observation.received_at));
                }
            }
        }
        let Some(newest) = newest else { continue };
        if output.is_closed() {
            return Err(FeedError::OutputClosed);
        }
        output.send_replace(Arc::new(book.snapshot()));
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
async fn supervise_reader<F, Fut>(
    index: usize,
    connect: F,
    gate: Arc<Gate>,
    retry: RetryPolicy,
    longest_lifetime: Duration,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) where
    F: Fn(Duration) -> Fut,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    let reader = READER_NAMES[index];
    let mut backoff = Backoff::new(retry);
    let mut log = LogLimiter::default();
    loop {
        if !gate.wait(true, &cancel).await {
            return;
        }
        // Aborted with this supervisor, so a connection never outlives it.
        let connection =
            AbortOnDropHandle::new(tokio::spawn(connect(planned_lifetime(longest_lifetime))));
        let (reason, useful) = match connection.await {
            Ok(Outcome {
                exit: Exit::Cancelled,
                ..
            }) => return,
            Ok(Outcome {
                exit: Exit::Renewal,
                ..
            }) => {
                tracing::info!(reader, "renewing Binance connection");
                backoff.reset();
                continue;
            }
            Ok(Outcome {
                exit: Exit::RateLimited(retry_after),
                useful,
            }) => {
                let wait = retry_after.unwrap_or_else(|| backoff.next());
                gate.cool_down(wait);
                (
                    format!("handshake rate limited; cooling down {wait:?}"),
                    useful,
                )
            }
            Ok(Outcome {
                exit: Exit::Failed(reason),
                useful,
            }) => (reason, useful),
            Err(error) if error.is_panic() => {
                metrics.reader_panics.fetch_add(1, Ordering::Relaxed);
                ("reader task panicked".to_string(), false)
            }
            // The runtime is shutting down.
            Err(_) => return,
        };
        if useful {
            backoff.reset();
        }
        let delay = backoff.next();
        if let Some(suppressed) = log.allow() {
            tracing::warn!(reader, %reason, ?delay, suppressed, "Binance reader disconnected; reconnecting");
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Run the feed until `cancel`. An error is irrecoverable: the caller stops
/// the solver.
pub(super) async fn run_feed(
    config: FeedConfig,
    plan: MarketPlan,
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) -> Result<(), FeedError> {
    let tls = tls_config()?;
    let http = reqwest::Client::builder()
        .use_preconfigured_tls((*tls).clone())
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout)
        .build()?;
    let gate = Arc::new(Gate::new(config.max_connection_attempts, ATTEMPT_WINDOW));
    let Some(markets) = validate_markets(&plan, &http, &config, &gate, &metrics, &cancel).await
    else {
        return Ok(());
    };
    let markets = Arc::new(markets);
    let book = QuoteBook::new(markets.clone(), config.quote_ttl);
    output.send_replace(Arc::new(book.snapshot()));
    if markets.symbols().is_empty() {
        tracing::warn!("no Binance market confirmed; every pair stays paused");
        cancel.cancelled().await;
        return Ok(());
    }

    // Stopping one task stops the others; the solver's token stops them all.
    let internal = cancel.child_token();
    let mut tasks: JoinSet<Result<(), FeedError>> = JoinSet::new();
    let mut names = HashMap::new();
    let [(latest_a, readers_a), (latest_b, readers_b)] = config
        .stream_endpoints
        .each_ref()
        .map(|_| watch::channel(vec![None; markets.symbols().len()]));
    for (index, latest) in [latest_a, latest_b].into_iter().enumerate() {
        let context = Arc::new(ReaderContext {
            index,
            url: stream_url(&config.stream_endpoints[index], &markets),
            markets: markets.clone(),
            max_spread_bps: config.max_spread_bps,
            tls: tls.clone(),
            connect_timeout: config.connect_timeout,
            idle_timeout: config.idle_timeout,
            latest,
            metrics: metrics.clone(),
            cancel: internal.clone(),
        });
        let supervisor = supervise_reader(
            index,
            move |lifetime| run_connection(context.clone(), lifetime),
            gate.clone(),
            config.retry,
            config.connection_lifetime,
            metrics.clone(),
            internal.clone(),
        );
        let id = tasks
            .spawn(async move {
                supervisor.await;
                Ok(())
            })
            .id();
        names.insert(id, ["Binance reader a", "Binance reader b"][index]);
    }
    let publisher = publish(
        book,
        [readers_a, readers_b],
        output,
        metrics,
        internal.clone(),
    );
    names.insert(tasks.spawn(publisher).id(), "price publisher");

    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => Ok(()),
        Some(joined) = tasks.join_next_with_id() => Err(match joined {
            Ok((_, Err(error))) => error,
            Ok((id, Ok(()))) => FeedError::TaskStopped(names[&id]),
            Err(error) => FeedError::TaskPanicked(names[&error.id()]),
        }),
    };
    internal.cancel();
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
        tracing::warn!("price feed tasks did not stop in time; aborting them");
        tasks.abort_all();
    }
    result
}

/// Start the feed on its own OS thread. Markets are confirmed in the
/// background, and the snapshot stays empty (every pair paused) until they
/// are. However the thread ends — an irrecoverable error, a panic, or after
/// `cancel` — it cancels `cancel`, so a failed feed stops the solver.
pub(crate) fn spawn_price_feed_thread(
    config: FeedConfig,
    plan: MarketPlan,
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
) -> anyhow::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build price-feed runtime")?;
    thread::Builder::new()
        .name("price-feed".into())
        .spawn(move || {
            let _stop_solver = cancel.clone().drop_guard();
            if let Err(error) = runtime.block_on(run_feed(config, plan, output, metrics, cancel)) {
                tracing::error!(%error, "price feed failed; stopping the solver");
            }
            // Bounded even if a DNS lookup is stuck on a blocking thread.
            runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
        })
        .context("spawn price-feed thread")
}

#[cfg(test)]
mod tests;

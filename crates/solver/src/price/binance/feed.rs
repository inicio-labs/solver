//! The Binance price feed: one OS thread with its own Tokio runtime.
//!
//! ```text
//! Reader A ─┐
//!           ├─> publisher ─> watch<Arc<PriceSnapshot>> ─┬─> matcher
//! Reader B ─┘                                          └─> price API
//! ```
//!
//! Startup confirms the configured markets through `exchangeInfo`. Until the
//! first market is confirmed the published snapshot is empty and every pair is
//! paused, but the solver runs. Once every symbol is resolved, or the
//! validation timeout passes with at least one confirmed, two supervised
//! readers keep their own connections to the confirmed symbols and the
//! publisher merges their observations. Symbols still unresolved keep being
//! retried in the background; when one is confirmed the readers restart with
//! the larger set, carrying the published quotes over. A reader that fails or
//! panics restarts alone; any other failure of the feed, including a panic,
//! stops the solver.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use super::market::{Listing, MarketIssue, MarketPlan, Markets, Symbol};
use super::reader::{run_connection, stream_url, Exit, ReaderContext, ReaderLatest};
use super::rest::fetch_listing;
use super::snapshot::{Offer, PriceSnapshot};
use super::ticker::QuoteLimits;

/// The feed keeps this many readers, each with its own endpoint.
pub(crate) const READERS: usize = 2;
/// Reader names in logs and metrics, by index.
pub(crate) const READER_NAMES: [&str; READERS] = ["a", "b"];
const READER_TASKS: [&str; READERS] = ["Binance reader a", "Binance reader b"];
/// Connection attempts are counted over this window, as Binance limits them.
const ATTEMPT_WINDOW: Duration = Duration::from_secs(5 * 60);
/// A connection that delivered a valid quote and lasted this long resets its
/// reader's backoff; one that drops sooner keeps backing off, so a flapping
/// endpoint cannot drain the shared connection-attempt budget.
pub(super) const STABLE_CONNECTION: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_INTERVAL: Duration = Duration::from_secs(10);

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
        let factor = 1u32.checked_shl(self.step).unwrap_or(u32::MAX);
        let step = self
            .policy
            .min_delay
            .saturating_mul(factor)
            .min(self.policy.max_delay);
        self.step = self.step.saturating_add(1);
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
    /// Stream base URL of reader A and of reader B.
    pub stream_endpoints: [String; READERS],
    pub rest_endpoint: String,
    pub limits: QuoteLimits,
    /// A quote is fresh while younger than this, for clearing and wallet
    /// valuation alike.
    pub quote_ttl: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Reconnect when no frame at all, not even a ping, arrives for this long.
    pub idle_timeout: Duration,
    /// Reconnect when no quote arrives for this long although the connection
    /// stays up: a stalled backend keeps pinging.
    pub data_idle_timeout: Duration,
    /// Longest planned connection; each lasts a random 50–100% of it.
    pub connection_lifetime: Duration,
    pub retry: RetryPolicy,
    /// Connection attempts per five minutes, both readers together; positive.
    pub max_connection_attempts: usize,
    /// How long startup keeps waiting for unresolved symbols before the
    /// readers start with the confirmed ones.
    pub validation_timeout: Duration,
}

/// Feed counters for `/metrics`. Per-reader counters are indexed like
/// [`READER_NAMES`].
#[derive(Debug, Default)]
pub struct FeedMetrics {
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
    /// Symbols `exchangeInfo` has not answered for yet.
    pub(crate) markets_pending: AtomicU64,
    /// Configured uses of a symbol Binance rejected.
    pub(crate) rejected_markets: AtomicU64,
    /// Configured uses of a symbol that is subscribed but not trading.
    pub(crate) halted_markets: AtomicU64,
}

/// Failures that stop the feed, and with it the solver.
#[derive(Debug, Error)]
pub(crate) enum FeedError {
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    #[error("HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("no Binance market could be confirmed within the validation timeout: {0}")]
    NoMarketConfirmed(String),
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

    async fn sleep(wait: Duration, cancel: &CancellationToken) -> bool {
        tokio::select! {
            _ = cancel.cancelled() => false,
            _ = tokio::time::sleep(wait) => true,
        }
    }

    /// Wait out the cooldown; `false` when cancelled.
    async fn wait_cooldown(&self, cancel: &CancellationToken) -> bool {
        while let Some(wait) = self.cooldown_left() {
            if !Self::sleep(wait, cancel).await {
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
                    if !Self::sleep(wait, cancel).await {
                        return false;
                    }
                }
            }
        }
    }

    /// Send nothing for at least `wait` (HTTP 429/418 `Retry-After`, or a WAF
    /// block). Saturates instead of overflowing on an absurd `wait`.
    fn cool_down(&self, wait: Duration) {
        let now = tokio::time::Instant::now();
        let until = now
            .checked_add(wait)
            .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60));
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

/// Confirms the planned symbols through `exchangeInfo`, a pass at a time.
/// Only Binance's own answer about a symbol (unknown, or a listing that does
/// not match) is final; every failed request is retried, so a maintenance
/// page, a wrong path or an outage never rejects a market for good.
struct Validator<'a> {
    plan: &'a MarketPlan,
    http: reqwest::Client,
    rest_endpoint: &'a str,
    gate: Arc<Gate>,
    metrics: Arc<FeedMetrics>,
    listings: HashMap<Symbol, Option<Listing>>,
    /// Symbols without an answer yet.
    pending: Vec<Symbol>,
    backoff: Backoff,
    log: LogLimiter,
    /// Whether every failure of the last pass was one a retry cannot change.
    permanent_only: bool,
    last_error: Option<String>,
}

impl<'a> Validator<'a> {
    fn new(
        plan: &'a MarketPlan,
        http: reqwest::Client,
        config: &'a FeedConfig,
        gate: Arc<Gate>,
        metrics: Arc<FeedMetrics>,
    ) -> Self {
        let pending: Vec<Symbol> = plan.symbols().into_iter().collect();
        metrics
            .markets_pending
            .store(pending.len() as u64, Ordering::Relaxed);
        Self {
            plan,
            http,
            rest_endpoint: &config.rest_endpoint,
            gate,
            metrics,
            listings: HashMap::new(),
            pending,
            backoff: Backoff::new(config.retry),
            log: LogLimiter::default(),
            permanent_only: false,
            last_error: None,
        }
    }

    /// Symbols Binance listed.
    fn confirmed(&self) -> usize {
        self.listings
            .values()
            .filter(|listing| listing.is_some())
            .count()
    }

    /// Look every pending symbol up once. `Some(true)` when a symbol got its
    /// answer, `None` when cancelled. A rate limit ends the pass early.
    async fn pass(&mut self, cancel: &CancellationToken) -> Option<bool> {
        let mut symbols = std::mem::take(&mut self.pending).into_iter();
        let mut retry = Vec::new();
        let mut progress = false;
        let mut permanent_only = true;
        let mut last_error = None;
        while let Some(symbol) = symbols.next() {
            if !self.gate.wait_cooldown(cancel).await {
                return None;
            }
            let lookup = tokio::select! {
                _ = cancel.cancelled() => return None,
                lookup = fetch_listing(&self.http, self.rest_endpoint, &symbol) => lookup,
            };
            let error = match lookup {
                Ok(listing) => {
                    self.listings.insert(symbol, listing);
                    progress = true;
                    continue;
                }
                Err(error) => error,
            };
            self.metrics.lookup_failures.fetch_add(1, Ordering::Relaxed);
            permanent_only &= !error.is_transient();
            retry.push(symbol);
            let rate_limited = error.is_rate_limited();
            if rate_limited {
                // Stop sending for this pass; Binance escalates to a ban.
                let wait = error.retry_after().unwrap_or_else(|| self.backoff.next());
                self.gate.cool_down(wait);
                retry.extend(symbols.by_ref());
            }
            last_error = Some(error);
            if rate_limited {
                break;
            }
        }
        self.pending = retry;
        self.metrics
            .markets_pending
            .store(self.pending.len() as u64, Ordering::Relaxed);
        if self.pending.is_empty() {
            self.backoff.reset();
        }
        self.permanent_only = permanent_only && !self.pending.is_empty();
        if let Some(error) = last_error {
            if let Some(suppressed) = self.log.allow() {
                tracing::warn!(
                    %error,
                    pending = self.pending.len(),
                    suppressed,
                    "Binance exchangeInfo lookups failed; retrying"
                );
            }
            self.last_error = Some(error.to_string());
        }
        Some(progress)
    }

    /// Sleep the next backoff; `false` when cancelled.
    async fn sleep(&mut self, cancel: &CancellationToken) -> bool {
        Gate::sleep(self.backoff.next(), cancel).await
    }

    /// Keep retrying the pending symbols until one gets its answer; `false`
    /// when cancelled. Pends forever once nothing is pending.
    async fn run_until_progress(&mut self, cancel: &CancellationToken) -> bool {
        loop {
            if self.pending.is_empty() {
                std::future::pending::<()>().await;
            }
            if !self.sleep(cancel).await {
                return false;
            }
            match self.pass(cancel).await {
                None => return false,
                Some(true) => return true,
                Some(false) => {}
            }
        }
    }

    /// The confirmed markets and every issue with the configured ones;
    /// pending symbols are left out of both.
    fn resolve(&self) -> (Markets, Vec<MarketIssue>) {
        let (markets, issues) = self.plan.resolve(&self.listings);
        let issues = issues
            .into_iter()
            .filter(|issue| !self.pending.contains(&issue.symbol))
            .collect();
        (markets, issues)
    }
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
    Fut: Future<Output = Exit> + Send + 'static,
{
    let reader = READER_NAMES[index];
    let mut backoff = Backoff::new(retry);
    let mut log = LogLimiter::default();
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
                tracing::info!(reader, "renewing Binance connection");
                backoff.reset();
                continue;
            }
            Ok(Exit::ServerShutdown) => {
                metrics.server_shutdowns.fetch_add(1, Ordering::Relaxed);
                tracing::info!(reader, "Binance announced a shutdown; reconnecting");
                backoff.reset();
                continue;
            }
            Ok(Exit::RateLimited(retry_after)) => {
                // The next attempt waits out the cooldown at the gate.
                let wait = retry_after.unwrap_or_else(|| backoff.next());
                gate.cool_down(wait);
                if let Some(suppressed) = log.allow() {
                    tracing::warn!(
                        reader,
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
                    reader,
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
            tracing::warn!(reader, %reason, ?delay, suppressed, "Binance reader disconnected; reconnecting");
        }
        if !Gate::sleep(delay, &cancel).await {
            return;
        }
    }
}

/// Log and count the issues of one resolution.
fn report_markets(
    markets: &Markets,
    issues: &[MarketIssue],
    pending: usize,
    metrics: &FeedMetrics,
) {
    let (rejected, halted): (Vec<_>, Vec<_>) = issues.iter().partition(|issue| issue.rejects());
    for issue in &rejected {
        tracing::error!(%issue, "Binance market rejected; it stays unavailable until the configuration changes");
    }
    for issue in &halted {
        tracing::warn!(%issue, "Binance market not trading");
    }
    metrics
        .markets_confirmed
        .store(markets.symbols().len() as u64, Ordering::Relaxed);
    metrics
        .rejected_markets
        .store(rejected.len() as u64, Ordering::Relaxed);
    metrics
        .halted_markets
        .store(halted.len() as u64, Ordering::Relaxed);
    tracing::info!(
        symbols = markets.symbols().len(),
        rejected = rejected.len(),
        halted = halted.len(),
        pending,
        "Binance markets confirmed"
    );
}

/// Test hook: a feed configured with this REST endpoint panics at start, so a
/// test can check what a panic on the feed thread does to the solver.
#[cfg(test)]
pub(super) static PANIC_ON_ENDPOINT: Mutex<Option<String>> = Mutex::new(None);

/// Run the feed until `cancel`. An error is irrecoverable: the caller stops
/// the solver.
pub(super) async fn run_feed(
    config: FeedConfig,
    plan: MarketPlan,
    output: watch::Sender<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
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
    let mut validator = Validator::new(&plan, http, &config, gate.clone(), metrics.clone());

    // Startup: resolve every symbol, or as many as the timeout allows.
    let deadline = tokio::time::Instant::now() + config.validation_timeout;
    loop {
        let Some(_progress) = validator.pass(&cancel).await else {
            return Ok(());
        };
        if validator.pending.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            if validator.confirmed() > 0 {
                tracing::warn!(
                    pending = validator.pending.len(),
                    "starting with the confirmed Binance markets; still resolving the rest"
                );
                break;
            }
            if validator.permanent_only {
                let reason = validator.last_error.take().unwrap_or_default();
                return Err(FeedError::NoMarketConfirmed(reason));
            }
        }
        if !validator.sleep(&cancel).await {
            return Ok(());
        }
    }

    // One generation of readers per confirmed market set.
    let mut previous: Option<Arc<PriceSnapshot>> = None;
    loop {
        let (markets, issues) = validator.resolve();
        report_markets(&markets, &issues, validator.pending.len(), &metrics);
        let markets = Arc::new(markets);
        let mut book = PriceSnapshot::new(markets.clone(), config.quote_ttl);
        if let Some(previous) = &previous {
            book.inherit(previous);
        }
        output
            .send(Arc::new(book.clone()))
            .map_err(|_| FeedError::OutputClosed)?;

        // Stopping one task stops the others; the solver's token stops them all.
        let generation = cancel.child_token();
        let mut tasks: JoinSet<Result<(), FeedError>> = JoinSet::new();
        let mut names = HashMap::new();
        if markets.symbols().is_empty() {
            tracing::warn!("no Binance market confirmed; every pair stays paused");
        } else {
            let (senders, receivers): (Vec<_>, Vec<_>) = (0..READERS)
                .map(|_| watch::channel(vec![None; markets.symbols().len()]))
                .unzip();
            for (index, latest) in senders.into_iter().enumerate() {
                let context = Arc::new(ReaderContext {
                    index,
                    url: stream_url(&config.stream_endpoints[index], &markets),
                    markets: markets.clone(),
                    limits: config.limits,
                    tls: tls.clone(),
                    connect_timeout: config.connect_timeout,
                    idle_timeout: config.idle_timeout,
                    data_idle_timeout: config.data_idle_timeout,
                    stable_after: STABLE_CONNECTION,
                    latest,
                    metrics: metrics.clone(),
                    cancel: generation.clone(),
                });
                let supervisor = supervise_reader(
                    index,
                    move |lifetime| run_connection(context.clone(), lifetime),
                    gate.clone(),
                    config.retry,
                    config.connection_lifetime,
                    metrics.clone(),
                    generation.clone(),
                );
                let id = tasks
                    .spawn(async move {
                        supervisor.await;
                        Ok(())
                    })
                    .id();
                names.insert(id, READER_TASKS[index]);
            }
            let receivers: [watch::Receiver<ReaderLatest>; READERS] = receivers
                .try_into()
                .unwrap_or_else(|_| unreachable!("one receiver per reader"));
            let publisher = publish(
                book,
                receivers,
                output.clone(),
                metrics.clone(),
                generation.clone(),
            );
            names.insert(tasks.spawn(publisher).id(), "price publisher");
        }

        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => Ok(false),
            Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => Err(match joined {
                Ok((_, Err(error))) => error,
                Ok((id, Ok(()))) => FeedError::TaskStopped(task_name(&names, &id)),
                Err(error) => FeedError::TaskPanicked(task_name(&names, &error.id())),
            }),
            progress = validator.run_until_progress(&cancel) => Ok(progress),
        };
        generation.cancel();
        let drain = async { while tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
            tracing::warn!("price feed tasks did not stop in time; aborting them");
            tasks.abort_all();
        }
        match outcome {
            Err(error) => return Err(error),
            Ok(false) => return Ok(()),
            Ok(true) => {
                tracing::info!("a Binance market was confirmed late; restarting the readers");
                previous = Some(output.borrow().clone());
            }
        }
    }
}

fn task_name(names: &HashMap<tokio::task::Id, &'static str>, id: &tokio::task::Id) -> &'static str {
    names.get(id).copied().unwrap_or("price feed task")
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

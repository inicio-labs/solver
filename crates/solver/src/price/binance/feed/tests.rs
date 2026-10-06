//! Feed behaviour against a local mock Binance server: validation, both
//! readers, reconnection, invalid quotes, limits, isolation from a blocked
//! thread, and failure ownership.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mock_binance::{Failure, Market, MockBinance, Settings};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::clearing::ReferencePrice;
use crate::price::binance::market::{AssetCode, ClearingMarket, Symbol};
use crate::price::binance::reader::MAX_FRAME_BYTES;
use crate::price::binance::snapshot::{PriceUnavailable, Quote, SymbolQuote};
use crate::price::binance::test_support::{btc, eth, price, usdt};

const TTL: Duration = Duration::from_secs(5);
const WAIT: Duration = Duration::from_secs(10);

/// Clears ETH/USDT through ETHUSDT and values ETH and BTC in USDT.
fn plan() -> MarketPlan {
    let asset = |code: &str| AssetCode::parse(code).unwrap();
    MarketPlan::new(
        [
            (eth(), asset("ETH")),
            (usdt(), asset("USDT")),
            (btc(), asset("BTC")),
        ],
        vec![ClearingMarket {
            name: "ETH-USDT".into(),
            base: eth(),
            quote: usdt(),
            symbol: Symbol::parse("ETHUSDT").unwrap(),
        }],
        asset("USDT"),
    )
    .unwrap()
}

fn markets() -> Vec<Market> {
    vec![
        Market::new("ETHUSDT", "ETH", "USDT", "2718.65", "2718.66"),
        Market::new("BTCUSDT", "BTC", "USDT", "86369.99", "86370.00"),
    ]
}

fn fast() -> Settings {
    Settings {
        tick: Duration::from_millis(20),
        ..Settings::default()
    }
}

fn config_for(stream: &str, rest: &str) -> FeedConfig {
    FeedConfig {
        stream_endpoints: [stream.to_string(), stream.to_string()],
        rest_endpoint: rest.to_string(),
        max_spread_bps: 100,
        quote_ttl: TTL,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(2),
        connection_lifetime: Duration::from_secs(3600),
        retry: RetryPolicy {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(200),
        },
        max_connection_attempts: 1_000,
    }
}

fn config(mock: &MockBinance) -> FeedConfig {
    config_for(&mock.ws_url(), &mock.rest_url())
}

struct Running {
    snapshots: watch::Receiver<Arc<PriceSnapshot>>,
    metrics: Arc<FeedMetrics>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), FeedError>>,
}

impl Running {
    fn start(config: FeedConfig) -> Self {
        let (output, snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
        let metrics = Arc::new(FeedMetrics::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_feed(
            config,
            plan(),
            output,
            metrics.clone(),
            cancel.clone(),
        ));
        Self {
            snapshots,
            metrics,
            cancel,
            task,
        }
    }

    /// Wait until a published snapshot satisfies `ready`.
    async fn until(&mut self, ready: impl Fn(&PriceSnapshot) -> bool) -> Arc<PriceSnapshot> {
        let wait = self.snapshots.wait_for(|snapshot| ready(snapshot));
        let snapshot = tokio::time::timeout(WAIT, wait)
            .await
            .expect("timed out waiting for a snapshot")
            .expect("feed stopped");
        snapshot.clone()
    }

    fn latest(&self) -> Arc<PriceSnapshot> {
        self.snapshots.borrow().clone()
    }

    async fn stop(self) {
        self.cancel.cancel();
        let result = tokio::time::timeout(WAIT, self.task)
            .await
            .expect("feed did not stop");
        result.unwrap().unwrap();
    }
}

fn eth_usdt(snapshot: &PriceSnapshot) -> Result<ReferencePrice, PriceUnavailable> {
    snapshot.pair_price(eth(), usdt(), Instant::now())
}

fn eth_quote(snapshot: &PriceSnapshot) -> Option<Quote> {
    snapshot.quotes().find_map(|(symbol, quote)| match quote {
        SymbolQuote::Usable(quote) if symbol.as_str() == "ETHUSDT" => Some(*quote),
        _ => None,
    })
}

fn eth_update_id(snapshot: &PriceSnapshot) -> Option<u64> {
    eth_quote(snapshot).map(|quote| quote.update_id)
}

async fn eventually(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A mock Binance on its own thread and runtime, unaffected by whatever the
/// test thread does. Stops when dropped.
struct MockThread {
    mock: Arc<MockBinance>,
    _stop: oneshot::Sender<()>,
}

impl MockThread {
    fn start(settings: Settings) -> Self {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop, stopped) = oneshot::channel::<()>();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let mock = MockBinance::start(markets(), settings).await.unwrap();
                ready_tx.send(Arc::new(mock)).unwrap();
                let _ = stopped.await;
            });
        });
        Self {
            mock: ready_rx.recv().unwrap(),
            _stop: stop,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishes_validated_quotes_from_both_readers() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    let snapshot = feed
        .until(|snapshot| {
            eth_usdt(snapshot).is_ok() && snapshot.valuation(btc(), Instant::now()).is_ok()
        })
        .await;
    assert_eq!(eth_usdt(&snapshot), Ok(price("2718.655")));
    let btc_value = snapshot.valuation(btc(), Instant::now()).unwrap();
    assert_eq!(btc_value.price, price("86369.995"));
    assert!(btc_value.fresh);
    // One subscription per reader, both carrying both symbols.
    eventually(|| mock.open_connections() == 2).await;
    assert_eq!(snapshot.quotes().count(), 2);
    let metrics = &feed.metrics;
    eventually(|| {
        metrics
            .connected
            .iter()
            .all(|connected| connected.load(Ordering::Relaxed))
    })
    .await;
    assert_eq!(metrics.rejected_markets.load(Ordering::Relaxed), 0);
    // A quote change reaches the snapshot exactly.
    mock.set_quote("ETHUSDT", "3000", "3000.02");
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("3000.01")))
        .await;
    assert_eq!(mock.peak_connections(), 2);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_symbol_is_reported_alone() {
    let mock = MockBinance::start(vec![markets().remove(0)], fast())
        .await
        .unwrap();
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(
        snapshot.valuation(btc(), Instant::now()),
        Err(PriceUnavailable::NoMarket)
    );
    assert_eq!(feed.metrics.rejected_markets.load(Ordering::Relaxed), 1);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookups_retry_transient_failures_and_honour_retry_after() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.fail_rest(Failure {
        status: 503,
        retry_after_secs: None,
    });
    mock.fail_rest(Failure {
        status: 418,
        retry_after_secs: Some(1),
    });
    let started = Instant::now();
    let mut feed = Running::start(config(&mock));
    // Before validation completes the snapshot is empty and every pair paused.
    assert_eq!(eth_usdt(&feed.latest()), Err(PriceUnavailable::NoMarket));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "Retry-After ignored"
    );
    assert_eq!(feed.metrics.lookup_failures.load(Ordering::Relaxed), 2);
    assert_eq!(feed.metrics.rejected_markets.load(Ordering::Relaxed), 0);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permanent_lookup_failure_rejects_only_its_symbol() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    // The first lookup (BTCUSDT) gets an answer that retrying cannot change.
    mock.fail_rest(Failure {
        status: 404,
        retry_after_secs: None,
    });
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(
        snapshot.valuation(btc(), Instant::now()),
        Err(PriceUnavailable::NoMarket)
    );
    assert_eq!(feed.metrics.lookup_failures.load(Ordering::Relaxed), 1);
    assert_eq!(feed.metrics.rejected_markets.load(Ordering::Relaxed), 1);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_confirmed_market_the_feed_idles() {
    let mock = MockBinance::start(Vec::new(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    tokio::time::timeout(WAIT, feed.snapshots.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(feed.latest().quotes().count(), 0);
    // ETHUSDT for clearing and for ETH's valuation, BTCUSDT for BTC's.
    assert_eq!(feed.metrics.rejected_markets.load(Ordering::Relaxed), 3);
    assert_eq!(mock.connections_total(), 0);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quotes_continue_while_readers_reconnect() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    eventually(|| mock.open_connections() == 2).await;
    for _ in 0..4 {
        let sent = mock.update_id("ETHUSDT").unwrap();
        let connections = mock.connections_total();
        mock.disconnect_all();
        eventually(|| mock.connections_total() >= connections + 2).await;
        // Only the new connections can deliver updates sent after the drop.
        feed.until(|snapshot| eth_update_id(snapshot).is_some_and(|id| id > sent + 2))
            .await;
    }
    // A restart replaces its connection: never more than one per reader.
    assert_eq!(mock.peak_connections(), 2);
    feed.stop().await;
    eventually(|| mock.open_connections() == 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_stream_endpoint_recovers_while_the_other_reader_publishes() {
    let primary = MockBinance::start(markets(), fast()).await.unwrap();
    let flaky = MockBinance::start(markets(), fast()).await.unwrap();
    for _ in 0..3 {
        flaky.fail_stream(Failure {
            status: 503,
            retry_after_secs: None,
        });
    }
    let mut config = config(&primary);
    config.stream_endpoints[0] = flaky.ws_url();
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    let metrics = feed.metrics.clone();
    eventually(|| metrics.connections[0].load(Ordering::Relaxed) == 1).await;
    assert_eq!(flaky.connections_total(), 1);
    assert_eq!(metrics.connections[1].load(Ordering::Relaxed), 1);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rate_limited_handshake_cools_down_both_readers() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    for _ in 0..2 {
        mock.fail_stream(Failure {
            status: 429,
            retry_after_secs: Some(1),
        });
    }
    let started = Instant::now();
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "Retry-After ignored"
    );
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_invalid_quote_pauses_the_pair_until_a_newer_valid_one() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    // Halt regular updates, then send a crossed quote with a newer ID.
    mock.set_status("ETHUSDT", "BREAK");
    let id = mock.next_update_id("ETHUSDT").unwrap();
    let crossed = format!(
        r#"{{"stream":"ethusdt@bookTicker","data":{{"u":{id},"s":"ETHUSDT","b":"2720","B":"1","a":"2719","A":"1"}}}}"#
    );
    mock.send_raw("ethusdt@bookTicker", crossed);
    feed.until(|snapshot| eth_usdt(snapshot) == Err(PriceUnavailable::Unusable))
        .await;
    assert!(feed.metrics.rejected_quotes.load(Ordering::Relaxed) >= 1);
    mock.set_status("ETHUSDT", "TRADING");
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discarded_frames_change_nothing() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    eventually(|| mock.open_connections() == 2).await;
    mock.set_status("ETHUSDT", "BREAK");
    mock.set_status("BTCUSDT", "BREAK");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let quote = eth_quote(&feed.latest());
    let no_update_id = r#"{"stream":"ethusdt@bookTicker","data":{"s":"ETHUSDT"}}"#;
    mock.send_raw("ethusdt@bookTicker", no_update_id.into());
    let metrics = feed.metrics.clone();
    eventually(|| metrics.discarded_frames.load(Ordering::Relaxed) == 2).await;
    assert_eq!(eth_quote(&feed.latest()), quote);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_frame_reconnects_both_readers() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    eventually(|| mock.open_connections() == 2).await;
    let before = mock.connections_total();
    let last_id = eth_update_id(&snapshot).unwrap();
    mock.send_raw("ethusdt@bookTicker", "x".repeat(MAX_FRAME_BYTES + 1));
    eventually(|| mock.connections_total() >= before + 2).await;
    feed.until(|snapshot| eth_update_id(snapshot).is_some_and(|id| id > last_id + 5))
        .await;
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_connection_is_replaced_after_the_idle_timeout() {
    // No pings within the test, and no updates once the markets halt.
    let settings = Settings {
        ping_interval: Duration::from_secs(3600),
        ..fast()
    };
    let mock = MockBinance::start(markets(), settings).await.unwrap();
    let mut config = config(&mock);
    config.idle_timeout = Duration::from_millis(300);
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    mock.set_status("ETHUSDT", "BREAK");
    mock.set_status("BTCUSDT", "BREAK");
    let connections = mock.connections_total();
    eventually(|| mock.connections_total() >= connections + 2).await;
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_disconnected_quote_keeps_its_original_receipt_time() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    eventually(|| mock.open_connections() == 2).await;
    // No further updates; the readers drop and reconnect.
    mock.set_status("ETHUSDT", "BREAK");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let quote = eth_quote(&feed.latest()).unwrap();
    let connections = mock.connections_total();
    mock.disconnect_all();
    eventually(|| mock.connections_total() >= connections + 2).await;
    let snapshot = feed.latest();
    assert_eq!(
        eth_quote(&snapshot),
        Some(quote),
        "reconnecting renewed the quote"
    );
    let expiry = quote.received_at + TTL;
    assert!(snapshot
        .pair_price(eth(), usdt(), expiry - Duration::from_millis(1))
        .is_ok());
    assert_eq!(
        snapshot.pair_price(eth(), usdt(), expiry),
        Err(PriceUnavailable::Stale)
    );
    feed.stop().await;
}

/// The feed runs on its own thread: blocking the caller's thread (as a long
/// synchronous clearing pass blocks the matcher's) neither stops quotes from
/// arriving nor stops pongs, so Binance does not drop the connections. The
/// test runtime is single-threaded, so a feed sharing it would starve.
#[tokio::test]
async fn a_blocked_thread_does_not_starve_the_readers() {
    let MockThread { mock, _stop } = MockThread::start(Settings {
        tick: Duration::from_millis(20),
        ping_interval: Duration::from_millis(100),
        pong_timeout: Duration::from_secs(2),
    });
    let (output, mut snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
    let cancel = CancellationToken::new();
    let thread = spawn_price_feed_thread(
        config(&mock),
        plan(),
        output,
        Arc::new(FeedMetrics::default()),
        cancel.clone(),
    )
    .unwrap();
    tokio::time::timeout(
        WAIT,
        snapshots.wait_for(|snapshot| eth_usdt(snapshot).is_ok()),
    )
    .await
    .unwrap()
    .unwrap();
    eventually(|| mock.open_connections() == 2).await;
    let connections = mock.connections_total();
    let pongs = mock.pongs();

    let blocked_at = Instant::now();
    std::thread::sleep(Duration::from_secs(3));

    let quote = eth_quote(&snapshots.borrow()).unwrap();
    assert!(
        quote.received_at > blocked_at + Duration::from_secs(2),
        "no quote arrived while the thread was blocked"
    );
    assert!(
        mock.pongs() >= pongs + 20,
        "pongs {} -> {}",
        pongs,
        mock.pongs()
    );
    assert_eq!(
        mock.connections_total(),
        connections,
        "a connection was dropped"
    );
    cancel.cancel();
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_interrupts_a_retry_wait() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.fail_rest(Failure {
        status: 500,
        retry_after_secs: None,
    });
    let mut config = config(&mock);
    config.retry = RetryPolicy {
        min_delay: Duration::from_secs(60),
        max_delay: Duration::from_secs(60),
    };
    let feed = Running::start(config);
    let metrics = feed.metrics.clone();
    eventually(|| metrics.lookup_failures.load(Ordering::Relaxed) == 1).await;
    let stopping = Instant::now();
    feed.stop().await;
    assert!(stopping.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_output_is_a_feed_error() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    // Nobody reads the prices: the first publication fails.
    let (output, snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
    drop(snapshots);
    let result = tokio::time::timeout(
        WAIT,
        run_feed(
            config(&mock),
            plan(),
            output,
            Arc::new(FeedMetrics::default()),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(FeedError::OutputClosed)), "{result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_feed_thread_stops_the_solver_when_it_ends() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let (output, snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
    drop(snapshots);
    let cancel = CancellationToken::new();
    let thread = spawn_price_feed_thread(
        config(&mock),
        plan(),
        output,
        Arc::new(FeedMetrics::default()),
        cancel.clone(),
    )
    .unwrap();
    tokio::time::timeout(WAIT, cancel.cancelled())
        .await
        .expect("solver not stopped");
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .unwrap()
        .unwrap();
}

fn supervisor_retry() -> RetryPolicy {
    RetryPolicy {
        min_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_reader_is_restarted() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let metrics = Arc::new(FeedMetrics::default());
    let connect = {
        let (calls, cancel) = (calls.clone(), cancel.clone());
        move |_lifetime: Duration| {
            let (calls, cancel) = (calls.clone(), cancel.clone());
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => panic!("reader bug"),
                    1 => Outcome {
                        exit: Exit::Failed("closed".into()),
                        useful: true,
                    },
                    _ => {
                        cancel.cancel();
                        Outcome {
                            exit: Exit::Cancelled,
                            useful: false,
                        }
                    }
                }
            }
        }
    };
    let gate = Arc::new(Gate::new(10, Duration::from_secs(60)));
    tokio::time::timeout(
        WAIT,
        supervise_reader(
            0,
            connect,
            gate,
            supervisor_retry(),
            Duration::from_secs(60),
            metrics.clone(),
            cancel,
        ),
    )
    .await
    .expect("supervisor did not return after cancellation");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(metrics.reader_panics.load(Ordering::Relaxed), 1);
}

/// Planned renewals reconnect at once, each after a random 50–100% of the
/// longest lifetime, so two readers do not keep renewing together.
#[tokio::test(start_paused = true)]
async fn planned_renewals_are_immediate_and_randomized() {
    let longest = Duration::from_secs(100);
    let lifetimes = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let connect = {
        let (lifetimes, cancel) = (lifetimes.clone(), cancel.clone());
        move |lifetime: Duration| {
            let (lifetimes, cancel) = (lifetimes.clone(), cancel.clone());
            async move {
                let recorded = {
                    let mut lifetimes = lifetimes.lock().unwrap();
                    lifetimes.push(lifetime);
                    lifetimes.len()
                };
                if recorded == 8 {
                    cancel.cancel();
                    return Outcome {
                        exit: Exit::Cancelled,
                        useful: false,
                    };
                }
                tokio::time::sleep(lifetime).await;
                Outcome {
                    exit: Exit::Renewal,
                    useful: false,
                }
            }
        }
    };
    // A backoff would add at least a second; timers round to milliseconds.
    let retry = RetryPolicy {
        min_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(1),
    };
    let started = tokio::time::Instant::now();
    supervise_reader(
        1,
        connect,
        Arc::new(Gate::new(100, Duration::from_secs(60))),
        retry,
        longest,
        Arc::new(FeedMetrics::default()),
        cancel,
    )
    .await;
    let lifetimes = lifetimes.lock().unwrap();
    let renewed: Duration = lifetimes[..7].iter().sum();
    let waited = started.elapsed() - renewed;
    assert!(
        waited < Duration::from_millis(100),
        "a renewal waited {waited:?}"
    );
    assert!(lifetimes
        .iter()
        .all(|lifetime| (longest / 2..=longest).contains(lifetime)));
    assert!(lifetimes.windows(2).any(|pair| pair[0] != pair[1]));
}

#[test]
fn backoff_is_capped_at_the_maximum() {
    let policy = RetryPolicy {
        min_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let mut backoff = Backoff::new(policy);
    let delays: Vec<_> = (0..50).map(|_| backoff.next()).collect();
    assert!(delays.iter().all(|delay| *delay <= policy.max_delay));
    assert_eq!(delays.last(), Some(&policy.max_delay));
    backoff.reset();
    assert!(backoff.next() < policy.max_delay);
}

#[tokio::test(start_paused = true)]
async fn gate_spreads_attempts_and_honours_cooldown() {
    let gate = Gate::new(2, Duration::from_secs(10));
    let cancel = CancellationToken::new();
    let start = tokio::time::Instant::now();
    assert!(gate.wait(true, &cancel).await);
    assert!(gate.wait(true, &cancel).await);
    assert_eq!(start.elapsed(), Duration::ZERO);
    // The third attempt waits for the first to leave the window.
    assert!(gate.wait(true, &cancel).await);
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    // A cooldown delays plain requests too, without using attempt slots.
    gate.cool_down(Duration::from_secs(3));
    assert!(gate.wait(false, &cancel).await);
    assert_eq!(start.elapsed(), Duration::from_secs(13));
    cancel.cancel();
    assert!(!gate.wait(true, &cancel).await);
}

/// Live check against Binance's public endpoints (network required; CI skips
/// it): `cargo test -p solver --lib live_binance -- --ignored --nocapture`.
/// Reader A uses the market-data endpoint and reader B the main one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs network access to Binance"]
async fn live_binance_endpoints_publish_exact_prices() {
    let mut config = config_for(
        "wss://data-stream.binance.vision:443",
        "https://data-api.binance.vision",
    );
    config.stream_endpoints[1] = "wss://stream.binance.com:443".to_string();
    config.max_spread_bps = 50;
    config.connect_timeout = Duration::from_secs(10);
    config.request_timeout = Duration::from_secs(10);
    config.idle_timeout = Duration::from_secs(60);
    let mut feed = Running::start(config);
    let snapshot = feed
        .until(|snapshot| {
            eth_usdt(snapshot).is_ok() && snapshot.valuation(btc(), Instant::now()).is_ok()
        })
        .await;
    let eth = eth_usdt(&snapshot).unwrap();
    let btc = snapshot.valuation(btc(), Instant::now()).unwrap().price;
    println!(
        "ETHUSDT mid {} (update {:?}), BTCUSDT mid {}",
        eth.to_trimmed_decimal(8).unwrap(),
        eth_update_id(&snapshot),
        btc.to_trimmed_decimal(8).unwrap()
    );
    let metrics = feed.metrics.clone();
    eventually(|| {
        metrics
            .connected
            .iter()
            .all(|connected| connected.load(Ordering::Relaxed))
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!(
        "frames {} publications {} discarded {} rejected {}",
        metrics.frames.load(Ordering::Relaxed),
        metrics.publications.load(Ordering::Relaxed),
        metrics.discarded_frames.load(Ordering::Relaxed),
        metrics.rejected_quotes.load(Ordering::Relaxed),
    );
    assert_eq!(metrics.discarded_frames.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.rejected_quotes.load(Ordering::Relaxed), 0);
    feed.stop().await;
}

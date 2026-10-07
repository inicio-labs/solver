//! Feed behaviour against a local mock Binance server: validation, both
//! readers, reconnection, invalid quotes, limits, isolation from a blocked
//! thread, and failure ownership.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mock_binance::{Failure, Market, MockBinance, MockThread, Settings, Updates};
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::price::binance::reader::MAX_FRAME_BYTES;
use crate::price::binance::snapshot::{PriceUnavailable, Quote, SymbolQuote};
use crate::price::binance::test_support::{btc, eth, market, plan, price, usdt};

const TTL: Duration = Duration::from_secs(5);
const WAIT: Duration = Duration::from_secs(10);

/// Clears ETH/USDT through ETHUSDT and values ETH and BTC in USDT.
fn eth_usdt_plan() -> MarketPlan {
    plan(vec![market("ETH-USDT", eth(), usdt(), "ETHUSDT")])
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
        limits: QuoteLimits {
            max_spread_bps: 100,
            min_notional: None,
        },
        quote_ttl: TTL,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(2),
        data_idle_timeout: Duration::from_secs(60),
        connection_lifetime: Duration::from_secs(3600),
        retry: RetryPolicy {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(200),
        },
        max_connection_attempts: 1_000,
        validation_timeout: Duration::from_secs(30),
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
        Self::start_with_plan(config, eth_usdt_plan())
    }

    fn start_with_plan(config: FeedConfig, plan: MarketPlan) -> Self {
        let (output, snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
        let metrics = Arc::new(FeedMetrics::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_feed(
            config,
            plan,
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

    fn counter(&self, counter: impl Fn(&FeedMetrics) -> &AtomicU64) -> u64 {
        counter(&self.metrics).load(Ordering::Relaxed)
    }

    async fn stop(self) {
        self.cancel.cancel();
        let result = tokio::time::timeout(WAIT, self.task)
            .await
            .expect("feed did not stop");
        result.unwrap().unwrap();
    }
}

/// Whole USDT per whole ETH on the ETH/USDT clearing pair.
fn eth_usdt(snapshot: &PriceSnapshot) -> Result<Decimal, PriceUnavailable> {
    snapshot.market_price(eth(), usdt(), Instant::now())
}

fn btc_value(snapshot: &PriceSnapshot) -> Result<Decimal, PriceUnavailable> {
    snapshot
        .valuation(btc(), Instant::now())
        .map(|valued| valued.price)
}

fn eth_quote(snapshot: &PriceSnapshot) -> Option<Quote> {
    snapshot.quotes().find_map(|(symbol, quote)| match quote {
        SymbolQuote::Valid(quote) if symbol.as_str() == "ETHUSDT" => Some(*quote),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishes_validated_quotes_from_both_readers() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut feed = Running::start(config(&mock));
    let snapshot = feed
        .until(|snapshot| eth_usdt(snapshot).is_ok() && btc_value(snapshot).is_ok())
        .await;
    assert_eq!(eth_usdt(&snapshot), Ok(price("2718.655")));
    let valued = snapshot.valuation(btc(), Instant::now()).unwrap();
    assert_eq!(valued.price, price("86369.995"));
    assert!(valued.fresh);
    // One subscription per reader, both carrying both symbols.
    eventually(|| mock.open_connections() == 2).await;
    assert_eq!(snapshot.quotes().count(), 2);
    let metrics = &feed.metrics;
    eventually(|| {
        metrics
            .connected
            .iter()
            .all(|connected| connected.load(Ordering::Relaxed))
            && metrics
                .frames
                .iter()
                .all(|frames| frames.load(Ordering::Relaxed) > 0)
    })
    .await;
    assert_eq!(feed.counter(|m| &m.rejected_markets), 0);
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 2);
    assert_eq!(feed.counter(|m| &m.markets_pending), 0);
    // A quote change reaches the snapshot exactly.
    mock.set_quote("ETHUSDT", "3000", "3000.02");
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("3000.01")))
        .await;
    assert_eq!(mock.peak_connections(), 2);
    // Both readers see the same update IDs, so nothing conflicts.
    assert_eq!(feed.counter(|m| &m.conflicting_updates), 0);
    feed.stop().await;
}

/// Either reader alone carries prices, and the publisher takes the highest
/// update ID across both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_reader_alone_supplies_prices_and_the_highest_id_wins() {
    let first = MockBinance::start(markets(), fast()).await.unwrap();
    let second = MockBinance::start(markets(), fast()).await.unwrap();
    second.set_quote("ETHUSDT", "3000", "3000.02");
    // Reader B's server is ahead: its quotes win while it publishes.
    second.skip_update_ids("ETHUSDT", 1_000);
    let mut config = config(&first);
    config.stream_endpoints[1] = second.ws_url();
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("3000.01")))
        .await;
    // Equal-ID frames never happen across two independent servers, and the
    // lower IDs from the first server are not conflicts either.
    assert_eq!(feed.counter(|m| &m.conflicting_updates), 0);
    // Now the first server jumps ahead; its reader alone must move the price.
    second.set_status("ETHUSDT", "BREAK");
    first.skip_update_ids("ETHUSDT", 10_000);
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("2718.655")))
        .await;
    // And the other way round, with reader A's server silent.
    first.set_status("ETHUSDT", "BREAK");
    second.set_status("ETHUSDT", "TRADING");
    second.skip_update_ids("ETHUSDT", 100_000);
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("3000.01")))
        .await;
    for index in 0..READERS {
        assert!(feed.metrics.frames[index].load(Ordering::Relaxed) > 0);
    }
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_symbol_is_reported_alone() {
    let mock = MockBinance::start(vec![markets().remove(0)], fast())
        .await
        .unwrap();
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(btc_value(&snapshot), Err(PriceUnavailable::NoMarket));
    assert_eq!(feed.counter(|m| &m.rejected_markets), 1);
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 1);
    feed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spot_restricted_symbol_is_rejected_and_a_halted_one_subscribed() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.set_spot_trading_allowed("BTCUSDT", false);
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(btc_value(&snapshot), Err(PriceUnavailable::NoMarket));
    assert_eq!(feed.counter(|m| &m.rejected_markets), 1);
    feed.stop().await;

    // A halt is temporary: the symbol is subscribed and resumes on its own.
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.set_status("BTCUSDT", "BREAK");
    let mut feed = Running::start(config(&mock));
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(btc_value(&snapshot), Err(PriceUnavailable::NoQuote));
    assert_eq!(feed.counter(|m| &m.rejected_markets), 0);
    assert_eq!(feed.counter(|m| &m.halted_markets), 1);
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 2);
    mock.set_status("BTCUSDT", "TRADING");
    feed.until(|snapshot| btc_value(snapshot) == Ok(price("86369.995")))
        .await;
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
    // While the lookups are held back, nothing has been published: every
    // pair is paused on the empty default snapshot.
    let metrics = feed.metrics.clone();
    eventually(|| metrics.lookup_failures.load(Ordering::Relaxed) == 2).await;
    assert!(!feed.snapshots.has_changed().unwrap());
    assert_eq!(eth_usdt(&feed.latest()), Err(PriceUnavailable::NoMarket));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "Retry-After ignored"
    );
    assert_eq!(feed.counter(|m| &m.lookup_failures), 2);
    assert_eq!(feed.counter(|m| &m.rejected_markets), 0);
    feed.stop().await;
}

/// A wrong answer from the endpoint (a 404 here, or a maintenance page) is
/// retried; only Binance's own answer about a symbol rejects a market.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_endpoint_error_is_retried_not_treated_as_a_rejection() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.fail_rest(Failure {
        status: 404,
        retry_after_secs: None,
    });
    let mut feed = Running::start(config(&mock));
    feed.until(|snapshot| eth_usdt(snapshot).is_ok() && btc_value(snapshot).is_ok())
        .await;
    assert_eq!(feed.counter(|m| &m.lookup_failures), 1);
    assert_eq!(feed.counter(|m| &m.rejected_markets), 0);
    feed.stop().await;
}

/// After the validation timeout the readers start with what is confirmed; a
/// symbol confirmed later is added by restarting them, without losing the
/// quotes already published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_market_confirmed_late_is_added_without_a_restart_of_the_solver() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    // BTCUSDT's lookups fail for a while; ETHUSDT's succeed at once.
    for _ in 0..12 {
        mock.fail_rest_for(
            "BTCUSDT",
            Failure {
                status: 503,
                retry_after_secs: None,
            },
        );
    }
    let mut config = config(&mock);
    config.validation_timeout = Duration::from_millis(300);
    let mut feed = Running::start(config);
    let snapshot = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    assert_eq!(btc_value(&snapshot), Err(PriceUnavailable::NoMarket));
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 1);
    assert_eq!(feed.counter(|m| &m.markets_pending), 1);
    let connections = mock.connections_total();
    let snapshot = feed.until(|snapshot| btc_value(snapshot).is_ok()).await;
    // The restart carried ETH's quote over.
    assert!(eth_usdt(&snapshot).is_ok());
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 2);
    assert_eq!(feed.counter(|m| &m.markets_pending), 0);
    assert!(mock.connections_total() >= connections + 2);
    eventually(|| mock.open_connections() == 2).await;
    feed.stop().await;
}

/// When nothing can be confirmed and every failure is one a retry cannot
/// change (a wrong path here), the feed stops the solver instead of running
/// without prices forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_confirmed_for_a_permanent_reason_is_a_feed_error() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut config = config(&mock);
    config.rest_endpoint = format!("{}/nowhere", mock.rest_url());
    config.validation_timeout = Duration::from_millis(300);
    let feed = Running::start(config);
    let result = tokio::time::timeout(WAIT, feed.task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(FeedError::NoMarketConfirmed(_))),
        "{result:?}"
    );
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
    assert_eq!(feed.counter(|m| &m.rejected_markets), 3);
    assert_eq!(feed.counter(|m| &m.markets_confirmed), 0);
    // The feed stays alive (an exit would stop the solver) and opens nothing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!feed.task.is_finished());
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

/// With one endpoint refusing every handshake for a while, the other reader
/// alone keeps the prices flowing; the failing reader recovers later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_stream_endpoint_recovers_while_the_other_reader_publishes() {
    let primary = MockBinance::start(markets(), fast()).await.unwrap();
    let flaky = MockBinance::start(markets(), fast()).await.unwrap();
    for _ in 0..12 {
        flaky.fail_stream(Failure {
            status: 503,
            retry_after_secs: None,
        });
    }
    let mut config = config(&primary);
    config.stream_endpoints[0] = flaky.ws_url();
    let mut feed = Running::start(config);
    let first = feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    let first_id = eth_update_id(&first).unwrap();
    // Prices keep moving on reader B alone while reader A is still refused.
    feed.until(|snapshot| eth_update_id(snapshot).is_some_and(|id| id > first_id + 5))
        .await;
    assert_eq!(feed.metrics.frames[0].load(Ordering::Relaxed), 0);
    assert_eq!(flaky.connections_total(), 0);
    assert!(feed.metrics.frames[1].load(Ordering::Relaxed) > 5);
    let metrics = feed.metrics.clone();
    eventually(|| metrics.connections[0].load(Ordering::Relaxed) == 1).await;
    assert_eq!(flaky.connections_total(), 1);
    assert_eq!(metrics.connections[1].load(Ordering::Relaxed), 1);
    feed.stop().await;
}

/// One reader's 429 holds back the other reader's next handshake too: the
/// cooldown is shared, as Binance's limits are per IP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rate_limited_handshake_cools_down_both_readers() {
    let limited = MockBinance::start(markets(), fast()).await.unwrap();
    let healthy = MockBinance::start(markets(), fast()).await.unwrap();
    limited.fail_stream(Failure {
        status: 429,
        retry_after_secs: Some(2),
    });
    let mut config = config(&healthy);
    config.stream_endpoints[0] = limited.ws_url();
    let started = Instant::now();
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    let metrics = feed.metrics.clone();
    eventually(|| metrics.connections[1].load(Ordering::Relaxed) == 1).await;
    // Reader B is dropped and must wait out reader A's cooldown to reconnect.
    healthy.disconnect_all();
    let dropped = Instant::now();
    eventually(|| metrics.connections[1].load(Ordering::Relaxed) == 2).await;
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "reader B reconnected {:?} after the start, inside reader A's cooldown",
        started.elapsed()
    );
    assert!(dropped.elapsed() >= Duration::from_millis(500));
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
    feed.until(|snapshot| eth_usdt(snapshot) == Err(PriceUnavailable::Invalid))
        .await;
    let rejected: u64 = feed
        .metrics
        .rejected_quotes
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();
    assert!(rejected >= 1);
    mock.set_status("ETHUSDT", "TRADING");
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    feed.stop().await;
}

/// With a minimum notional, a one-lot top of book cannot set the price.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thin_book_is_invalid_under_a_minimum_notional() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut config = config(&mock);
    // The mock displays 1.0 on each side: 2718 USDT of ETH, 86370 of BTC.
    config.limits.min_notional = Some(price("5000"));
    let mut feed = Running::start(config);
    let snapshot = feed
        .until(|snapshot| {
            btc_value(snapshot).is_ok() && eth_usdt(snapshot) != Err(PriceUnavailable::NoQuote)
        })
        .await;
    assert_eq!(eth_usdt(&snapshot), Err(PriceUnavailable::Invalid));
    // A deeper quote at a higher price clears the floor.
    mock.set_quote("ETHUSDT", "6000", "6000.02");
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("6000.01")))
        .await;
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
    // Wait until the last regular update has been published.
    let last = mock.update_id("ETHUSDT").unwrap();
    feed.until(|snapshot| eth_update_id(snapshot) == Some(last))
        .await;
    let quote = eth_quote(&feed.latest());
    let no_update_id = r#"{"stream":"ethusdt@bookTicker","data":{"s":"ETHUSDT"}}"#;
    mock.send_raw("ethusdt@bookTicker", no_update_id.into());
    let metrics = feed.metrics.clone();
    eventually(|| {
        metrics
            .discarded_frames
            .iter()
            .all(|counter| counter.load(Ordering::Relaxed) == 1)
    })
    .await;
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
    // The replaced connections were closed first: never two per reader.
    assert_eq!(mock.peak_connections(), 2);
    eventually(|| mock.open_connections() == 2).await;
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
    assert_eq!(mock.peak_connections(), 2);
    feed.stop().await;
}

/// A connection that keeps answering pings but delivers no quote is replaced
/// after the data-idle timeout, since a stalled backend keeps pinging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinging_but_quoteless_connection_is_replaced() {
    let settings = Settings {
        ping_interval: Duration::from_millis(100),
        updates: Updates::OnChange,
        ..fast()
    };
    let mock = MockBinance::start(markets(), settings).await.unwrap();
    let mut config = config(&mock);
    config.idle_timeout = Duration::from_secs(5);
    config.data_idle_timeout = Duration::from_millis(600);
    let mut feed = Running::start(config);
    // One change gives each reader a quote; then the market is quiet.
    eventually(|| mock.open_connections() == 2).await;
    mock.set_quote("ETHUSDT", "2800", "2800.02");
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("2800.01")))
        .await;
    let connections = mock.connections_total();
    let pongs = mock.pongs();
    eventually(|| mock.connections_total() >= connections + 2).await;
    assert!(
        mock.pongs() > pongs,
        "the connection was not kept alive by pings"
    );
    assert_eq!(mock.peak_connections(), 2);
    feed.stop().await;
}

/// Binance announces a shutdown before closing; the readers reconnect at once
/// instead of backing off after the close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_shutdown_reconnects_without_backoff() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut config = config(&mock);
    config.retry = RetryPolicy {
        min_delay: Duration::from_secs(5),
        max_delay: Duration::from_secs(5),
    };
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    eventually(|| mock.open_connections() == 2).await;
    let connections = mock.connections_total();
    let announced = Instant::now();
    mock.shutdown_all();
    eventually(|| mock.connections_total() >= connections + 2).await;
    assert!(
        announced.elapsed() < Duration::from_secs(2),
        "a backoff was applied"
    );
    assert_eq!(feed.counter(|m| &m.server_shutdowns), 2);
    let discarded: u64 = feed
        .metrics
        .discarded_frames
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();
    assert_eq!(discarded, 0, "the event was counted as a discarded frame");
    let sent = mock.update_id("ETHUSDT").unwrap();
    feed.until(|snapshot| eth_update_id(snapshot).is_some_and(|id| id > sent))
        .await;
    feed.stop().await;
}

/// A quiet market expires exactly at the TTL and resumes on the next change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_market_goes_stale_at_the_ttl() {
    let settings = Settings {
        updates: Updates::OnChange,
        ..fast()
    };
    let mock = MockBinance::start(markets(), settings).await.unwrap();
    let mut config = config(&mock);
    config.quote_ttl = Duration::from_millis(400);
    let mut feed = Running::start(config);
    eventually(|| mock.open_connections() == 2).await;
    mock.set_quote("ETHUSDT", "2800", "2800.02");
    let snapshot = feed
        .until(|snapshot| eth_usdt(snapshot) == Ok(price("2800.01")))
        .await;
    let received = eth_quote(&snapshot).unwrap().received_at;
    let expiry = received + Duration::from_millis(400);
    assert!(snapshot
        .pair_price(eth(), usdt(), expiry - Duration::from_millis(1))
        .is_ok());
    assert_eq!(
        snapshot.pair_price(eth(), usdt(), expiry),
        Err(PriceUnavailable::Stale)
    );
    // Nothing is republished while the market is quiet...
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(eth_usdt(&feed.latest()), Err(PriceUnavailable::Stale));
    // ...and the next change makes it fresh again.
    mock.set_quote("ETHUSDT", "2900", "2900.02");
    feed.until(|snapshot| eth_usdt(snapshot) == Ok(price("2900.01")))
        .await;
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
    let last = mock.update_id("ETHUSDT").unwrap();
    feed.until(|snapshot| eth_update_id(snapshot) == Some(last))
        .await;
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
    feed.stop().await;
}

/// Planned renewals through real connections: each reader replaces its
/// connection without a backoff, closing the old one first, and prices keep
/// flowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planned_renewals_replace_connections_without_a_gap() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let mut config = config(&mock);
    config.connection_lifetime = Duration::from_millis(400);
    config.retry = RetryPolicy {
        min_delay: Duration::from_secs(5),
        max_delay: Duration::from_secs(5),
    };
    let mut feed = Running::start(config);
    feed.until(|snapshot| eth_usdt(snapshot).is_ok()).await;
    let started = Instant::now();
    eventually(|| mock.connections_total() >= 8).await;
    // Four renewals per reader in well under the 5 s a backoff would cost.
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(mock.peak_connections(), 2);
    let sent = mock.update_id("ETHUSDT").unwrap();
    feed.until(|snapshot| eth_update_id(snapshot).is_some_and(|id| id > sent))
        .await;
    feed.stop().await;
}

/// The feed runs on its own thread: blocking the caller's thread (as a long
/// synchronous clearing pass blocks the matcher's) neither stops quotes from
/// arriving nor stops pongs, so Binance does not drop the connections. The
/// test runtime is single-threaded, so a feed sharing it would starve.
#[tokio::test]
async fn a_blocked_thread_does_not_starve_the_readers() {
    let mock = MockThread::start(
        markets(),
        Settings {
            tick: Duration::from_millis(20),
            ping_interval: Duration::from_millis(100),
            pong_timeout: Duration::from_secs(2),
            updates: Updates::EveryTick,
        },
    );
    let (output, mut snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
    let cancel = CancellationToken::new();
    let thread = spawn_price_feed_thread(
        config(&mock),
        eth_usdt_plan(),
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

/// An absurd `Retry-After` from a lookup or a handshake must not end the feed
/// (and with it the solver); it is capped and waited out like any ban.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_absurd_retry_after_does_not_stop_the_feed() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.fail_rest(Failure {
        status: 418,
        retry_after_secs: Some(u64::MAX),
    });
    let feed = Running::start(config(&mock));
    let metrics = feed.metrics.clone();
    eventually(|| metrics.lookup_failures.load(Ordering::Relaxed) == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!feed.task.is_finished(), "the feed ended on the header");
    feed.stop().await;

    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    mock.fail_stream(Failure {
        status: 429,
        retry_after_secs: Some(u64::MAX),
    });
    let feed = Running::start(config(&mock));
    eventually(|| mock.connections_total() >= 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!feed.task.is_finished(), "the feed ended on the header");
    feed.stop().await;
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
            eth_usdt_plan(),
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
        eth_usdt_plan(),
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

/// A panic on the feed thread stops the solver too; the drop guard does not
/// depend on an error being returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_feed_thread_stops_the_solver_when_it_panics() {
    let mock = MockBinance::start(markets(), fast()).await.unwrap();
    let config = config(&mock);
    *PANIC_ON_ENDPOINT.lock().unwrap() = Some(config.rest_endpoint.clone());
    let (output, _snapshots) = watch::channel(Arc::new(PriceSnapshot::default()));
    let cancel = CancellationToken::new();
    let thread = spawn_price_feed_thread(
        config,
        eth_usdt_plan(),
        output,
        Arc::new(FeedMetrics::default()),
        cancel.clone(),
    )
    .unwrap();
    tokio::time::timeout(WAIT, cancel.cancelled())
        .await
        .expect("solver not stopped");
    let joined = tokio::task::spawn_blocking(move || thread.join())
        .await
        .unwrap();
    assert!(joined.is_err(), "the thread did not panic");
    *PANIC_ON_ENDPOINT.lock().unwrap() = None;
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
                    1 => Exit::Failed {
                        reason: "closed".into(),
                        stable: true,
                    },
                    _ => {
                        cancel.cancel();
                        Exit::Cancelled
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

/// Records when each fake connection is attempted, under paused time.
fn recording_connect(
    exits: Vec<fn() -> Exit>,
    attempts: Arc<Mutex<Vec<tokio::time::Instant>>>,
    cancel: CancellationToken,
) -> impl Fn(Duration) -> std::pin::Pin<Box<dyn Future<Output = Exit> + Send>> {
    let exits = Arc::new(exits);
    let calls = Arc::new(AtomicUsize::new(0));
    move |_lifetime| {
        let (exits, calls, attempts, cancel) = (
            exits.clone(),
            calls.clone(),
            attempts.clone(),
            cancel.clone(),
        );
        Box::pin(async move {
            attempts.lock().unwrap().push(tokio::time::Instant::now());
            let call = calls.fetch_add(1, Ordering::SeqCst);
            match exits.get(call) {
                Some(exit) => exit(),
                None => {
                    cancel.cancel();
                    Exit::Cancelled
                }
            }
        })
    }
}

fn unstable() -> Exit {
    Exit::Failed {
        reason: "dropped".into(),
        stable: false,
    }
}

fn stable() -> Exit {
    Exit::Failed {
        reason: "dropped".into(),
        stable: true,
    }
}

/// Short connections keep the backoff growing; a stable one resets it.
#[tokio::test(start_paused = true)]
async fn only_a_stable_connection_resets_the_backoff() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let connect = recording_connect(
        vec![unstable, unstable, unstable, stable, unstable],
        attempts.clone(),
        cancel.clone(),
    );
    let retry = RetryPolicy {
        min_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(64),
    };
    supervise_reader(
        0,
        connect,
        Arc::new(Gate::new(100, Duration::from_secs(60))),
        retry,
        Duration::from_secs(3600),
        Arc::new(FeedMetrics::default()),
        cancel,
    )
    .await;
    let attempts = attempts.lock().unwrap();
    let gaps: Vec<Duration> = attempts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    // Steps 1 s, 2 s, 4 s with jitter in [50%, 100%) after the three
    // unstable connections...
    assert!(gaps[0] < Duration::from_secs(1), "{gaps:?}");
    assert!(
        gaps[1] >= Duration::from_secs(1) && gaps[1] < Duration::from_secs(2),
        "{gaps:?}"
    );
    assert!(
        gaps[2] >= Duration::from_secs(2) && gaps[2] < Duration::from_secs(4),
        "{gaps:?}"
    );
    // ...then the stable one starts the schedule over.
    assert!(gaps[3] < Duration::from_secs(1), "{gaps:?}");
    assert!(
        gaps[4] >= Duration::from_secs(1) && gaps[4] < Duration::from_secs(2),
        "{gaps:?}"
    );
}

/// Two readers share one attempt budget: the third attempt in the window
/// waits for a slot, whichever reader makes it.
#[tokio::test(start_paused = true)]
async fn the_attempt_budget_is_shared_by_both_readers() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let gate = Arc::new(Gate::new(2, Duration::from_secs(10)));
    let retry = RetryPolicy {
        min_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(100),
    };
    let metrics = Arc::new(FeedMetrics::default());
    let supervisors = (0..READERS).map(|index| {
        supervise_reader(
            index,
            recording_connect(vec![unstable, unstable], attempts.clone(), cancel.clone()),
            gate.clone(),
            retry,
            Duration::from_secs(3600),
            metrics.clone(),
            cancel.clone(),
        )
    });
    futures_util::future::join_all(supervisors).await;
    let mut attempts = attempts.lock().unwrap().clone();
    attempts.sort();
    let start = attempts[0];
    assert!(attempts.len() >= 4, "{attempts:?}");
    assert_eq!(attempts[1] - start, Duration::ZERO);
    assert!(
        attempts[2] - start >= Duration::from_secs(10),
        "{attempts:?}"
    );
    assert!(
        attempts[3] - start >= Duration::from_secs(10),
        "{attempts:?}"
    );
    assert!(metrics.budget_waits.load(Ordering::Relaxed) >= 2);
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
                    return Exit::Cancelled;
                }
                tokio::time::sleep(lifetime).await;
                Exit::Renewal
            }
        }
    };
    // A backoff would add at least half a second; timers round to milliseconds.
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

/// Delays never exceed the cap and keep their jitter at the cap, so two
/// readers that failed together do not retry in lockstep.
#[test]
fn backoff_is_capped_and_stays_jittered() {
    let policy = RetryPolicy {
        min_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let mut backoff = Backoff::new(policy);
    let delays: Vec<_> = (0..50).map(|_| backoff.next()).collect();
    assert!(delays.iter().all(|delay| *delay <= policy.max_delay));
    assert!(delays[0] < policy.min_delay);
    let capped = &delays[10..];
    assert!(capped
        .iter()
        .all(|delay| *delay >= policy.max_delay / 2 && *delay < policy.max_delay));
    assert!(capped.windows(2).any(|pair| pair[0] != pair[1]));
    backoff.reset();
    assert!(backoff.next() < policy.min_delay);
}

#[tokio::test(start_paused = true)]
async fn gate_spreads_attempts_and_honours_cooldown() {
    let gate = Gate::new(2, Duration::from_secs(10));
    let metrics = FeedMetrics::default();
    let cancel = CancellationToken::new();
    let start = tokio::time::Instant::now();
    assert!(gate.acquire_attempt(&cancel, &metrics).await);
    assert!(gate.acquire_attempt(&cancel, &metrics).await);
    assert_eq!(start.elapsed(), Duration::ZERO);
    // The third attempt waits for the first to leave the window.
    assert!(gate.acquire_attempt(&cancel, &metrics).await);
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert_eq!(metrics.budget_waits.load(Ordering::Relaxed), 1);
    // A cooldown delays plain requests too, without using attempt slots...
    gate.cool_down(Duration::from_secs(3));
    assert!(gate.wait_cooldown(&cancel).await);
    assert_eq!(start.elapsed(), Duration::from_secs(13));
    // ...so the next attempt finds a free slot at once.
    assert!(gate.acquire_attempt(&cancel, &metrics).await);
    assert_eq!(start.elapsed(), Duration::from_secs(13));
    // An absurd cooldown saturates instead of panicking.
    gate.cool_down(Duration::from_secs(u64::MAX));
    assert!(gate.cooldown_left().is_some());
    cancel.cancel();
    assert!(!gate.acquire_attempt(&cancel, &metrics).await);
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
    config.limits.max_spread_bps = 50;
    config.connect_timeout = Duration::from_secs(10);
    config.request_timeout = Duration::from_secs(10);
    config.idle_timeout = Duration::from_secs(60);
    let mut feed = Running::start(config);
    let snapshot = feed
        .until(|snapshot| eth_usdt(snapshot).is_ok() && btc_value(snapshot).is_ok())
        .await;
    let eth = eth_usdt(&snapshot).unwrap();
    let btc = btc_value(&snapshot).unwrap();
    println!(
        "ETHUSDT mid {} (update {:?}), BTCUSDT mid {}",
        eth,
        eth_update_id(&snapshot),
        btc
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
    let sum = |counters: &[AtomicU64; READERS]| -> u64 {
        counters
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    };
    println!(
        "frames {} publications {} discarded {} rejected {} conflicts {}",
        sum(&metrics.frames),
        metrics.publications.load(Ordering::Relaxed),
        sum(&metrics.discarded_frames),
        sum(&metrics.rejected_quotes),
        metrics.conflicting_updates.load(Ordering::Relaxed),
    );
    assert_eq!(sum(&metrics.discarded_frames), 0);
    assert_eq!(sum(&metrics.rejected_quotes), 0);
    assert_eq!(metrics.conflicting_updates.load(Ordering::Relaxed), 0);
    feed.stop().await;
}

//! `mock-binance` — serve mock Binance Spot market data for devnet/local runs.
//!
//! Point the solver's `[binance]` endpoints at it:
//!
//! ```toml
//! [binance]
//! stream_endpoints = ["ws://127.0.0.1:8089", "ws://127.0.0.1:8089"]
//! rest_endpoint = "http://127.0.0.1:8089"
//! ```
//!
//! Markets are `SYMBOL=BASE/QUOTE:BID/ASK`, e.g.
//! `--market ETHUSDT=ETH/USDT:2718.65/2718.66`. Change a quote at runtime with
//! `GET /set?symbol=ETHUSDT&bid=2700&ask=2700.01`; `GET /markets` lists them.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use mock_binance::{Market, MockBinance, Settings};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(
    name = "mock-binance",
    about = "Mock Binance Spot exchangeInfo + bookTicker streams"
)]
struct Cli {
    /// Address to bind.
    #[arg(long, default_value = "127.0.0.1:8089")]
    bind: SocketAddr,
    /// A market `SYMBOL=BASE/QUOTE:BID/ASK` (repeatable).
    #[arg(long = "market", required = true)]
    markets: Vec<String>,
    /// Milliseconds between updates of every market.
    #[arg(long, default_value_t = 250)]
    tick_ms: u64,
}

fn parse_market(spec: &str) -> Result<Market> {
    let parse = || -> Option<Market> {
        let (symbol, rest) = spec.split_once('=')?;
        let (assets, quote) = rest.split_once(':')?;
        let (base_asset, quote_asset) = assets.split_once('/')?;
        let (bid, ask) = quote.split_once('/')?;
        Some(Market::new(symbol, base_asset, quote_asset, bid, ask))
    };
    parse().ok_or_else(|| anyhow!("market {spec:?} is not SYMBOL=BASE/QUOTE:BID/ASK"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let cli = Cli::parse();
    let markets = cli
        .markets
        .iter()
        .map(|spec| parse_market(spec))
        .collect::<Result<Vec<_>>>()?;
    let settings = Settings {
        tick: Duration::from_millis(cli.tick_ms.max(1)),
        ..Settings::default()
    };
    let server = MockBinance::bind(cli.bind, markets, settings)
        .await
        .with_context(|| format!("bind {}", cli.bind))?;
    tracing::info!(addr = %server.addr(), "mock-binance listening");
    tokio::signal::ctrl_c().await.context("wait for Ctrl-C")?;
    Ok(())
}

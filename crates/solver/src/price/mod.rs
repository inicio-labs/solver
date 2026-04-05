use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

use crate::types::TokenId;
use crate::matching::price_feed::{PriceFeed, UsdCents};

/// Price data: mapping from token (faucet) ID to USD price in cents.
pub type PriceSnapshot = HashMap<TokenId, UsdCents>;

/// Trait abstracting the price service.
pub trait PriceClient: Send {
    /// Fetch latest prices for the given tokens.
    fn fetch_prices(
        &self,
        tokens: &[TokenId],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PriceSnapshot>> + Send + '_>>;
}

/// Mock price client that returns configurable static prices.
pub struct MockPriceClient {
    prices: PriceSnapshot,
}

impl MockPriceClient {
    pub fn new(prices: PriceSnapshot) -> Self {
        Self { prices }
    }
}

impl PriceClient for MockPriceClient {
    fn fetch_prices(
        &self,
        _tokens: &[TokenId],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PriceSnapshot>> + Send + '_>>
    {
        let prices = self.prices.clone();
        Box::pin(async move { Ok(prices) })
    }
}

/// Run the price fetching loop.
///
/// Periodically polls the price service and broadcasts updates via the watch channel.
pub async fn run_price_feed(
    client: impl PriceClient,
    tokens: Vec<TokenId>,
    price_tx: watch::Sender<PriceSnapshot>,
    interval: Duration,
) {
    loop {
        match client.fetch_prices(&tokens).await {
            Ok(prices) => {
                let _ = price_tx.send(prices);
            }
            Err(e) => {
                eprintln!("[price] fetch error: {e}");
                // watch channel retains last value, so matcher uses stale price
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// PriceFeed adapter that reads from a watch channel snapshot.
///
/// Created by snapshotting the watch channel at the start of each matching run.
/// Implements the matching engine's `PriceFeed` trait.
#[derive(Clone)]
pub struct WatchPriceFeed {
    prices: PriceSnapshot,
}

impl WatchPriceFeed {
    /// Create an empty feed.
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
        }
    }

    /// Create a new feed from the current watch channel value.
    pub fn from_watch(rx: &watch::Receiver<PriceSnapshot>) -> Self {
        Self {
            prices: rx.borrow().clone(),
        }
    }

    /// Create a feed from a static price map.
    pub fn from_map(prices: PriceSnapshot) -> Self {
        Self { prices }
    }

    /// Set the price for a token (convenience for building feeds incrementally).
    pub fn set_price_cents(&mut self, token: TokenId, price: UsdCents) {
        self.prices.insert(token, price);
    }
}

impl Default for WatchPriceFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceFeed for WatchPriceFeed {
    fn usd_price_cents(&self, token: TokenId) -> UsdCents {
        *self.prices.get(&token).unwrap_or(&1)
    }
}

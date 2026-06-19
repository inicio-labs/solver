use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

use crate::db::{self, DbPool};
use crate::types::TokenId;
use crate::matching::price_feed::{PriceFeed, UsdCents};

/// Price data: mapping from token (faucet) ID to USD price in cents.
pub type PriceSnapshot = HashMap<TokenId, UsdCents>;

/// Trait abstracting the price service.
#[async_trait]
pub trait PriceClient: Send {
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PriceSnapshot>;
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

#[async_trait]
impl PriceClient for MockPriceClient {
    async fn fetch_prices(&self, _tokens: &[TokenId]) -> Result<PriceSnapshot> {
        Ok(self.prices.clone())
    }
}

/// Forwarding impl so a `Box<dyn PriceClient + Send>` satisfies `P:
/// PriceClient`. Lets `start` accept an injected (boxed) price client —
/// production `HttpPriceClient`, tests `MockPriceClient` — without making the
/// price plumbing (`run_price_feed`, `spawn_core_services`) generic over a
/// trait object. (`PriceClient: Send` as a supertrait does NOT make bare
/// `dyn PriceClient` a `Send` type, so `+ Send` is required; and the
/// `#[async_trait]` Send future borrows `&self` across the await, so the
/// boxed object must also be `Sync` — both concrete clients are.)
#[async_trait]
impl PriceClient for Box<dyn PriceClient + Send + Sync> {
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PriceSnapshot> {
        (**self).fetch_prices(tokens).await
    }
}

/// Run the price fetching loop.
///
/// Periodically polls the price service and broadcasts updates via the watch channel.
pub async fn run_price_feed(
    client: impl PriceClient,
    pool: DbPool,
    price_tx: watch::Sender<PriceSnapshot>,
    interval: Duration,
) {
    loop {
        let tokens = match db::load_registered_tokens(&pool) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "failed to load tokens from DB");
                vec![]
            }
        };
        match client.fetch_prices(&tokens).await {
            Ok(prices) => {
                let _ = price_tx.send(prices);
            }
            Err(e) => {
                tracing::warn!(error = %e, "price fetch failed; matcher continues with last good snapshot");
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
    pub fn new() -> Self {
        Self { prices: HashMap::new() }
    }

    pub fn from_watch(rx: &watch::Receiver<PriceSnapshot>) -> Self {
        Self { prices: rx.borrow().clone() }
    }
}

impl Default for WatchPriceFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceFeed for WatchPriceFeed {
    fn price_cents(&self, token: TokenId) -> Option<UsdCents> {
        self.prices.get(&token).copied()
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use super::*;

    impl WatchPriceFeed {
        pub fn from_map(prices: PriceSnapshot) -> Self {
            Self { prices }
        }

        pub fn set_price_cents(&mut self, token: TokenId, price: UsdCents) {
            self.prices.insert(token, price);
        }
    }
}

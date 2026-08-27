use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

use crate::matching::price_feed::{PriceFeed, UsdCents};
use crate::price::coingecko::{read_symbol_map, SharedSymbolMap};
use crate::types::TokenId;

/// Matcher-facing price snapshot: token (faucet) ID → USD price in whole cents.
/// This integer representation is the fund-critical path's source of truth.
pub type PriceSnapshot = HashMap<TokenId, UsdCents>;

/// Full-precision price for a token (the public price-query API only). Kept
/// OUT of the matcher path so floats never enter settlement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceData {
    /// Price in the quote currency at full precision (CoinGecko `precision=full`).
    pub usd: f64,
}

/// Token → full-precision price, broadcast on a side channel for the price API.
pub type PreciseSnapshot = HashMap<TokenId, PriceData>;

/// Trait abstracting the price service. Returns full-precision values; the
/// matcher's cents snapshot is derived in [`run_price_feed`].
#[async_trait]
pub trait PriceClient: Send {
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PreciseSnapshot>;
}

/// Mock price client that returns configurable static prices. Constructed from a
/// cents map (back-compat) which it stores as full-precision USD.
pub struct MockPriceClient {
    prices: PreciseSnapshot,
}

impl MockPriceClient {
    pub fn new(prices: PriceSnapshot) -> Self {
        let prices = prices
            .into_iter()
            .map(|(t, cents)| (t, PriceData { usd: cents as f64 / 100.0 }))
            .collect();
        Self { prices }
    }
}

#[async_trait]
impl PriceClient for MockPriceClient {
    async fn fetch_prices(&self, _tokens: &[TokenId]) -> Result<PreciseSnapshot> {
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
    async fn fetch_prices(&self, tokens: &[TokenId]) -> Result<PreciseSnapshot> {
        (**self).fetch_prices(tokens).await
    }
}

/// Derive the matcher's cents snapshot from full-precision USD. Identical
/// rounding to the previous fetch-edge behaviour (`round(usd*100)`), so the
/// matcher sees the same integer prices it always has.
fn to_cents(precise: &PreciseSnapshot) -> PriceSnapshot {
    precise
        .iter()
        .map(|(t, d)| (*t, (d.usd * 100.0).round() as UsdCents))
        .collect()
}

/// Run the price fetching loop. The token set comes from the in-memory
/// `symbol_map` (hydrated at boot, kept current by admin write-through), so the
/// loop never reads the DB. Each successful poll broadcasts the cents snapshot
/// (matcher) and the precise snapshot (price API) and bumps `last_price_update`.
/// A failed poll keeps the last good snapshots and does NOT advance the
/// timestamp, so the API can detect staleness.
pub async fn run_price_feed(
    client: impl PriceClient,
    symbol_map: SharedSymbolMap,
    price_tx: watch::Sender<PriceSnapshot>,
    precise_tx: watch::Sender<PreciseSnapshot>,
    last_price_update: Arc<AtomicI64>,
    interval: Duration,
) {
    loop {
        // Guard drops at the end of this statement, so the lock is never held across the await.
        let tokens: Vec<TokenId> = read_symbol_map(&symbol_map).keys().copied().collect();
        match client.fetch_prices(&tokens).await {
            Ok(precise) => {
                let _ = price_tx.send(to_cents(&precise));
                let _ = precise_tx.send(precise);
                if let Ok(d) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    last_price_update.store(d.as_secs() as i64, Ordering::Relaxed);
                }
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

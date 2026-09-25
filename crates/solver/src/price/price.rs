use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::matching::price_feed::{PriceFeed, UsdCents};
use crate::clearing::{ReferencePrice, Wide};
use crate::price::{read_token_map, SharedTokenMap};
use crate::types::TokenId;

/// Legacy-matcher snapshot: token (faucet) ID → USD price in whole cents.
pub type PriceSnapshot = HashMap<TokenId, UsdCents>;

/// One token's wallet-API price and independently parsed exact clearing price.
/// Only `exact_reference` may enter clearing arithmetic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceData {
    /// Price in the quote currency at full precision (CoinGecko `precision=full`).
    pub usd: f64,
    /// Original provider number, parsed without `f64`. Clearing requires this;
    /// `None` keeps API-only prices usable but never admits them to settlement.
    pub exact_reference: Option<ReferencePrice>,
    /// Provider's own last-update timestamp, not the local HTTP fetch time.
    /// Clearing requires it; API-only and legacy prices may omit it.
    pub source_updated_at_unix_ms: Option<u64>,
    /// Local observation time assigned to every token in one completed fetch.
    /// Zero means this value has not been published by the price-feed loop.
    pub observed_at_unix_ms: u64,
}

/// Token → price data, published once for the wallet API and clearing.
pub type PreciseSnapshot = HashMap<TokenId, PriceData>;

/// Trait abstracting the price service. The legacy matcher's cents snapshot is
/// derived in [`run_price_feed`]; exact clearing uses `exact_reference` only.
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
            .map(|(t, cents)| {
                (
                    t,
                    PriceData {
                        usd: cents as f64 / 100.0,
                        exact_reference: (cents > 0).then_some(ReferencePrice {
                            numerator: Wide::from(cents),
                            denominator: Wide::from(100u64),
                        }),
                        source_updated_at_unix_ms: None,
                        observed_at_unix_ms: 0,
                    },
                )
            })
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
/// `token_map` (hydrated at boot, kept current by admin write-through), so the
/// loop never reads the DB. Each successful poll publishes the current prices
/// twice — a whole-cents map for the legacy matcher and one snapshot retaining
/// exact references for clearing plus f64 prices for the wallet API — and bumps
/// `last_price_update`. A failed poll keeps the last good prices and does NOT
/// advance the timestamp, so the API can detect staleness.
pub async fn run_price_feed(
    client: impl PriceClient,
    token_map: SharedTokenMap,
    price_tx: watch::Sender<PriceSnapshot>,
    precise_tx: watch::Sender<PreciseSnapshot>,
    last_price_update: Arc<AtomicI64>,
    interval: Duration,
) {
    loop {
        // Guard drops at the end of this statement, so the lock is never held across the await.
        let tokens: Vec<TokenId> = read_token_map(&token_map).keys().copied().collect();
        match client.fetch_prices(&tokens).await {
            Ok(mut precise) => {
                let observed_at_unix_ms = crate::types::now_millis();
                for data in precise.values_mut() {
                    data.observed_at_unix_ms = observed_at_unix_ms;
                }
                let _ = price_tx.send(to_cents(&precise));
                let _ = precise_tx.send(precise);
                last_price_update.store((observed_at_unix_ms / 1_000) as i64, Ordering::Relaxed);
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

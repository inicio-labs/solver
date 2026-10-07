//! Prices. Binance Spot `bookTicker` midpoints are the only source, for
//! internal clearing, swap guidance and wallet valuation alike (ADR 0004).
//! This is the one path to the price types: `crate::price::{..}`.

mod binance;

#[cfg(test)]
pub(crate) use binance::test_support;
pub(crate) use binance::{
    spawn_price_feed_thread, ClearingMarket, FeedConfig, MarketPlan, PriceUnavailable, QuoteLimits,
    RetryPolicy, Valued,
};
pub use binance::{AssetCode, FeedMetrics, MarketError, PriceSnapshot, Symbol};

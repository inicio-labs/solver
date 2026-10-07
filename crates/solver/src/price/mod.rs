//! Prices. Binance Spot `bookTicker` midpoints are the only source, for
//! internal clearing, swap guidance and wallet valuation alike (ADR 0004).
//! This is the one path to the price types: `crate::price::{..}`.

mod binance;
mod precision;

#[cfg(test)]
pub(crate) use binance::test_support;
pub(crate) use binance::{
    parse_positive_decimal, spawn_price_feed_thread, ClearingMarket, MarketPlan, PriceUnavailable,
    Valued,
};
pub use binance::{AssetCode, BinanceConfig, FeedMetrics, MarketError, PriceSnapshot, Symbol};
pub use precision::PricePrecision;

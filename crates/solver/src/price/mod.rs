//! Prices. Binance Spot `bookTicker` midpoints are the only source, for
//! internal clearing, swap guidance and wallet valuation alike (ADR 0004).

pub(crate) mod binance;

pub use binance::{
    AssetCode, ClearingMarket, FeedConfig, FeedMetrics, MarketError, MarketPlan, PriceSnapshot,
    PriceUnavailable, RetryPolicy, Symbol,
};

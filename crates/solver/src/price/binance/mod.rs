//! Binance Spot `bookTicker` prices (ADR 0004).
//!
//! [`market`] maps configured faucets and pairs to Binance symbols and checks
//! them against `exchangeInfo` listings; [`ticker`] parses and validates one
//! stream frame; [`snapshot`] merges both readers' observations and answers
//! freshness-checked price lookups; [`feed`] runs the readers and publisher on
//! their own thread.

pub(crate) mod feed;
pub(crate) mod market;
mod reader;
mod rest;
pub(crate) mod snapshot;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod ticker;

pub(crate) use feed::{spawn_price_feed_thread, READER_NAMES};
pub use feed::{FeedConfig, FeedMetrics, RetryPolicy};
pub use market::{AssetCode, ClearingMarket, MarketError, MarketPlan, Symbol};
pub use snapshot::{PriceSnapshot, PriceUnavailable};
pub(crate) use snapshot::{SymbolQuote, Valued};

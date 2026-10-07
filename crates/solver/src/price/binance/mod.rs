//! Binance Spot `bookTicker` prices (ADR 0004).
//!
//! [`market`] maps configured faucets and pairs to Binance symbols and checks
//! them against `exchangeInfo` listings; [`ticker`] parses and validates one
//! stream frame; [`snapshot`] merges both readers' observations and answers
//! freshness-checked price lookups; [`feed`] runs the readers and publisher on
//! their own thread. The crate reaches these through `crate::price`.

mod feed;
mod market;
mod reader;
mod rest;
mod snapshot;
#[cfg(test)]
pub(crate) mod test_support;
mod ticker;

pub use feed::FeedMetrics;
pub(crate) use feed::{spawn_price_feed_thread, FeedConfig, RetryPolicy};
pub use market::{AssetCode, MarketError, Symbol};
pub(crate) use market::{ClearingMarket, MarketPlan};
pub use snapshot::PriceSnapshot;
pub(crate) use snapshot::{PriceUnavailable, Valued};
pub(crate) use ticker::{parse_positive_decimal, QuoteLimits};

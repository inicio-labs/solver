//! Matcher module. Implementation in [`matcher`]; this file only wires the
//! submodule and re-exports its public surface.

pub(crate) mod clearing_book;
mod maker_index;
mod error;
mod matcher;
pub(crate) use clearing_book::ClearingBootstrap;
pub use error::MatcherError;
pub use matcher::*;

/// With time paused the matcher ticks at 0 s, 1 s, 2 s and so on: these
/// return half a second after the first tick, or one tick later.
#[cfg(test)]
pub(crate) mod test_ticks {
    use std::time::Duration;

    pub(crate) async fn after_first_tick() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    pub(crate) async fn after_next_tick() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

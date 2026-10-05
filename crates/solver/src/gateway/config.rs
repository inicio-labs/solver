//! Configuration for the maker gateway and its event streams.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct StreamConfig {
    /// Messages a subscriber may leave unread before it is disconnected.
    pub buffer: usize,
    /// Keep-alive interval, which also re-checks the API key.
    pub heartbeat: Duration,
}

pub struct GatewayConfig {
    pub bind: String,
    pub port: u16,
    /// Market keys of the pairs this solver clears; submits for any other
    /// pair are rejected, since nothing would ever match them.
    pub markets: HashSet<Vec<u8>>,
    /// How often the maker-note watcher looks for new blocks.
    pub watch_interval: Duration,
    /// Dedicated Miden client store for maker-note sync state.
    pub maker_store_path: PathBuf,
    pub round_submits: usize,
    pub submit_queue: usize,
    pub cancel_queue: usize,
    pub stream: StreamConfig,
}

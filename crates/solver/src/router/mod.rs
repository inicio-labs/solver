//! External liquidity routing: hand unmatched notes to allow-listed external
//! DEXes that quote for them, so they self-consume on-chain.
//!
//! - [`select`] — the pure, decimal-correct note-selection math (`select_notes`).
//! - shared channel payloads ([`QuotesSnapshot`], [`Handover`]) below.
//! - (server thread + matcher pass are wired in `router::server` / `matcher`).
//!
//! The websocket wire protocol lives in the standalone `pswap-filler-sdk` crate
//! (`pswap_filler_sdk::protocol`) — the same definition external fillers import,
//! so the contract can't drift between the two sides.

pub mod select;
pub mod server;

pub use select::{format_price, select_notes, Decimals, NoteView, Pair, Pick, Quote};
pub use server::{spawn_router_thread, RouterConfig};

use crate::matching::types::{Amount, DexId, OrderId};

/// Latest standing quotes from all connected DEXes — one entry per
/// `(dex, pair)`. Published by the router on the `quotes` watch channel and
/// read (filtered by freshness) by the matcher each tick.
pub type QuotesSnapshot = Vec<Quote>;

/// A batch of notes the matcher hands to the router for delivery to DEXes.
/// Sent on the `handover` mpsc channel with `try_send` (never blocks the tick).
#[derive(Clone, Debug)]
pub struct Handover {
    pub items: Vec<HandoverPick>,
}

/// One note to deliver to one DEX over its websocket connection.
#[derive(Clone, Debug)]
pub struct HandoverPick {
    pub dex: DexId,
    pub note_id: OrderId,
    /// requested-token amount the DEX should fill.
    pub fill: Amount,
    /// Serialized PSWAP note bytes for the DEX to consume on-chain.
    pub note_bytes: Vec<u8>,
    /// The price the solver requires this note be filled at (the winning DEX
    /// quote's price), as a decimal string — requested-per-offered, per whole
    /// token. Echoed to the DEX in the handover.
    pub fill_price: String,
}

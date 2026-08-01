//! # PSWAP Filler SDK
//!
//! Client SDK for external DEXes (**"fillers"**) that fill Miden PSWAP orders
//! routed by the solver's RFQ websocket. A filler connects, declares the pairs
//! it can fill, posts standing quotes (as `offered`/`requested` asset amounts),
//! and receives **handovers** — miden [`Note`](miden_protocol::note::Note)s it
//! consumes on-chain (on its own gas) at the matched price.
//!
//! ## Isolation
//!
//! A filler depends on **only** `pswap-filler-sdk` — never the solver crate, its
//! `miden-client`, `diesel`, `axum`, database, or any internal module. The wire
//! protocol ([`protocol`]) is the single shared source of truth: the solver's
//! router and this SDK both build on it, so the two can never drift.
//!
//! The protocol is **miden's own binary serialization over WebSocket binary
//! frames**, so miden types (`AccountId`, `FungibleAsset`, `Note`) travel
//! natively — no serde, no hex, no string parsing. That means `miden-protocol` /
//! `miden-standards` are dependencies, but the SDK stays independent of the
//! solver and of `miden-client` (the filler runs the consume tx with its own
//! client).
//!
//! ## Quick start
//!
//! Provide a pricing fn + handle handovers — that's the whole push
//! integration. [`serve_quotes`](client::LpClient::serve_quotes) keeps a
//! fresh quote live for each pair (keepalive + no stale-by-omission); you just
//! return the current `(offered_amount, requested_amount)` when asked. The
//! connection is auto-reconnecting, so the loop below survives transient drops.
//!
//! ```ignore
//! use std::time::Duration;
//! use pswap_filler_sdk::{LpClient, LpEvent, PairSpec};
//! use pswap_filler_sdk::consume::{consume_args, PswapNote};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut client = LpClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//!     let _q = client.serve_quotes(
//!         vec![PairSpec { offered: imiden, requested: iusdt }],
//!         Duration::from_secs(10),                 // ~half the router's quote TTL
//!         |_pair| Some((1_000_000, 2_000_000)),    // your live price: (offer, request) base units
//!     );
//!
//!     while let Some(ev) = client.next_event().await {
//!         match ev {
//!             LpEvent::Handover(h) => {
//!                 let pswap = PswapNote::try_from(&h.note)?;   // what am I getting / paying?
//!                 // ... your risk check against `pswap` (the note's fixed terms) ...
//!                 let _args = consume_args(0, h.fill_amount)?; // then self-consume on-chain
//!             }
//!             LpEvent::Disconnected { reason } => break,       // SDK gave up (e.g. bad token)
//!             _ => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod consume;
pub mod protocol;

// Ergonomic top-level re-exports — the common path needs only these.
pub use client::{Handover, LpClient, LpError, LpEvent, LpSender, QuoteTask};
pub use protocol::{ClientMsg, PairSpec, ServerMsg};

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
//! ```ignore
//! use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};
//! use pswap_filler_sdk::consume::{consume_args, PswapNote};
//! use miden_protocol::asset::FungibleAsset;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut client = FillerClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//!     client.subscribe(vec![PairSpec { offered: imiden, requested: iusdt }])?;
//!     // give up to 1 iMIDEN for 2 iUSDT (rate + size); refresh before the TTL.
//!     client.quote(FungibleAsset::new(imiden, 1_000_000)?, FungibleAsset::new(iusdt, 2_000_000)?, None)?;
//!
//!     while let Some(ev) = client.next_event().await {
//!         match ev {
//!             FillerEvent::Handover(h) => {
//!                 let pswap = PswapNote::try_from(&h.note)?;   // what am I getting / paying?
//!                 // ... your pricing/risk check against `pswap` and `h.fill_price` ...
//!                 let _args = consume_args(0, h.fill_amount)?; // then self-consume on-chain
//!             }
//!             FillerEvent::Disconnected => break,
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
pub use client::{FillerClient, FillerEvent, FillerSender, Handover};
pub use protocol::{ClientMsg, PairSpec, PriceRatio, ServerMsg};

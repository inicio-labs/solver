//! # PSWAP Filler SDK
//!
//! Client SDK for external DEXes (**"fillers"**) that fill Miden PSWAP orders
//! routed by the solver's RFQ websocket. A filler connects, declares the pairs
//! it can fill, posts standing `{price, quantity}` quotes, and receives
//! **handovers** — serialized PSWAP notes it consumes on-chain (on its own gas)
//! at the note's fixed rate.
//!
//! ## Dependency isolation (the whole point of this crate)
//!
//! This is a **standalone crate in the solver repo**, depended on one-way by the
//! solver. A filler adds **only** `pswap-filler-sdk` — never the solver, its
//! `miden-client`, `diesel`, `axum`, database, or any internal module. The
//! default build is small and pure: serde + tokio + a websocket client.
//!
//! The wire protocol ([`protocol`]) is the single shared source of truth: the
//! solver's router and this SDK both build on it, so the two can never drift.
//!
//! ## Features
//!
//! - **default** — the async client ([`client`]) and the wire protocol
//!   ([`protocol`]). **Zero miden / solver dependencies.**
//! - **`consume`** — opt-in on-chain helpers ([`consume`]) to decode a
//!   handed-over note and read its PSWAP terms. Pulls in `miden-protocol` +
//!   `miden-standards` (but **not** `miden-client`: the filler runs the consume
//!   transaction with its own client). Omit it if you only want the raw bytes.
//!
//! ## Quick start
//!
//! ```ignore
//! use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut client = FillerClient::connect("ws://solver:8090/v1/rfq", "my-token").await?;
//!     let pair = PairSpec { offered: imiden_hex, requested: iusdt_hex };
//!     client.quote(&pair, "2.00", 1_000_000, None)?; // refresh before the TTL
//!
//!     while let Some(ev) = client.next_event().await {
//!         match ev {
//!             FillerEvent::Handover(h) => {
//!                 // with feature "consume":
//!                 // let note  = pswap_filler_sdk::consume::decode_note(&h.note_hex)?;
//!                 // let terms = pswap_filler_sdk::consume::PswapTerms::from_note(&note)?;
//!                 // ... your pricing/risk check, then self-consume on-chain ...
//!             }
//!             FillerEvent::Disconnected => break,
//!             _ => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod protocol;

#[cfg(feature = "consume")]
pub mod consume;

// Ergonomic top-level re-exports — the common path needs only these.
pub use client::{FillerClient, FillerEvent, FillerSender, Handover};
pub use protocol::{parse_decimal_price, ClientMsg, PairSpec, ServerMsg};

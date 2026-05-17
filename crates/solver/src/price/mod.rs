//! Price module. The `coingecko` submodule and the core price-feed
//! implementation ([`price`]) live in sibling files; this file only wires
//! the submodules and re-exports their public surface so callers keep using
//! `crate::price::{...}`.

pub mod coingecko;
pub use coingecko::{read_symbol_map, write_symbol_map, HttpPriceClient, SharedSymbolMap};

mod price;
pub use price::*;

//! Price module. The `coingecko` submodule and the core price-feed
//! implementation ([`price`]) live in sibling files; this file only wires
//! the submodules and re-exports their public surface so callers keep using
//! `crate::price::{...}`.

pub mod coingecko;
pub use coingecko::{
    build_http_price_client, build_http_price_client_with_base, read_symbol_map, write_symbol_map,
    HttpPriceClient, SharedSymbolMap, COINGECKO_BASE,
};

mod price;
pub use price::*;

//! Price module. The oracle-agnostic token map ([`token_map`]), the
//! `coingecko` adapter, and the core price-feed implementation ([`price`]) live
//! in sibling files; this file only wires the submodules and re-exports their
//! public surface so callers keep using `crate::price::{...}`.

mod token_map;
pub use token_map::{read_token_map, write_token_map, SharedTokenMap};

pub mod coingecko;
pub use coingecko::{
    build_http_price_client, build_http_price_client_with_base, HttpPriceClient, COINGECKO_BASE,
};

mod price;
pub use price::*;

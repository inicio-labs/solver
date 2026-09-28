//! Matcher module. Implementation in [`matcher`]; this file only wires the
//! submodule and re-exports its public surface.

pub(crate) mod clearing_book;
mod matcher;
pub(crate) use clearing_book::ClearingBootstrap;
pub use matcher::*;

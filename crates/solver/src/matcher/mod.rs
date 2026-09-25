//! Matcher module. Implementation in [`matcher`]; this file only wires the
//! submodule and re-exports its public surface.

mod matcher;
mod clearing_book;
pub(crate) use clearing_book::ClearingBootstrap;
pub use matcher::*;

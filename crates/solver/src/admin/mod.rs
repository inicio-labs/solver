//! Admin HTTP module. Implementation in [`admin`]; this file only wires the
//! submodule and re-exports its public surface.

mod admin;
pub use admin::*;

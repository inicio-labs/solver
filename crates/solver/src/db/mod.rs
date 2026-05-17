//! DB module. The `models`/`schema` submodules and the core DB
//! implementation ([`db`]) live in sibling files; this file only wires the
//! submodules and re-exports the public surface so callers keep using
//! `crate::db::{...}`.

pub mod models;
pub mod schema;

mod db;
pub use db::*;

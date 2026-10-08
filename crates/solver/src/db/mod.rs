//! PostgreSQL application database. The two upstream Miden client stores
//! remain SQLite and are not part of this module.

pub mod error;
pub mod maker_db;
pub mod postgres_db;
pub mod postgres_migrations;
pub mod postgres_models;
pub mod postgres_pool;
pub mod postgres_schema;
#[cfg(test)]
pub mod postgres_test;

pub use error::{DbError, DbResult};
pub use postgres_pool::{IntakeSession, PgPool as DbPool};

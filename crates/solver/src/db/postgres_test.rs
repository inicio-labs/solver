//! Per-test PostgreSQL schemas: every test gets its own schema inside the
//! database named by `SOLVER_TEST_DATABASE_URL`, dropped when it ends.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use diesel::connection::SimpleConnection;
use diesel::PgConnection;

use super::{postgres_migrations, DbPool};

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

/// A fresh, uniquely named schema.
pub struct TestSchema {
    pub name: String,
    /// The test database URL with `search_path` set to this schema.
    pub url: String,
    /// A session on `url`: unqualified names resolve to this schema.
    pub conn: PgConnection,
    /// A session on the plain test URL, for qualified admin statements.
    pub admin: PgConnection,
}

impl TestSchema {
    /// An empty schema, with no migrations applied.
    pub fn new() -> Result<Self> {
        let base_url = std::env::var("SOLVER_TEST_DATABASE_URL")
            .context("set SOLVER_TEST_DATABASE_URL for PostgreSQL tests")?;
        let mut admin = postgres_migrations::connect(&base_url)?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!("solver_test_{}_{nonce}_{sequence}", std::process::id());
        admin.batch_execute(&format!("CREATE SCHEMA {name}"))?;
        let separator = if base_url.contains('?') { '&' } else { '?' };
        let url = format!("{base_url}{separator}options=-csearch_path%3D{name}");
        let conn = postgres_migrations::connect(&url)?;
        Ok(Self {
            name,
            url,
            conn,
            admin,
        })
    }

    /// A schema with every migration applied.
    pub fn migrated() -> Result<Self> {
        let mut schema = Self::new()?;
        postgres_migrations::migrate(&mut schema.conn)?;
        Ok(schema)
    }
}

impl Drop for TestSchema {
    fn drop(&mut self) {
        let _ = self.conn.batch_execute("ROLLBACK");
        let _ = self
            .admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.name));
    }
}

/// A migrated schema with a solver pool open on it.
pub struct TestDb {
    pub pool: DbPool,
    _schema: TestSchema,
}

impl TestDb {
    pub async fn new() -> Result<Self> {
        let schema = tokio::task::spawn_blocking(TestSchema::migrated).await??;
        let pool = DbPool::open(
            schema.url.clone(),
            schema.url.clone(),
            4,
            "solver/test".into(),
        )
        .await?;
        Ok(Self {
            pool,
            _schema: schema,
        })
    }
}

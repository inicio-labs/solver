//! Per-test PostgreSQL schema and pool for runtime-path tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use diesel::connection::SimpleConnection;

use super::{postgres_migrations, DbPool};

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

pub struct TestDb {
    pub pool: DbPool,
    base_url: String,
    schema: String,
}

impl TestDb {
    pub async fn new() -> Result<Self> {
        let base_url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
        let setup_url = base_url.clone();
        let (schema, url) = tokio::task::spawn_blocking(move || -> Result<_> {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
            let schema = format!("solver_test_{}_{}_{}", std::process::id(), nonce, sequence);
            let mut admin = postgres_migrations::connect(&setup_url)?;
            admin.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
            let separator = if setup_url.contains('?') { '&' } else { '?' };
            let url = format!("{setup_url}{separator}options=-csearch_path%3D{schema}");
            let mut conn = postgres_migrations::connect(&url)?;
            postgres_migrations::migrate(&mut conn)?;
            Ok((schema, url))
        })
        .await??;
        let pool = DbPool::open(url.clone(), url.clone(), 4, "solver/test".into()).await?;
        Ok(Self {
            pool,
            base_url,
            schema,
        })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let base = self.base_url.clone();
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(mut conn) = postgres_migrations::connect(&base) {
                let _ = conn.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
            }
        })
        .join();
    }
}

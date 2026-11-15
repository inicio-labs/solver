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
    pub fn url(&self) -> &str {
        &self._schema.url
    }

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

/// A settlement input filling `parent` with `amount` exactly as the executor
/// does, and the payback and remainder notes the transaction creates.
pub fn fill(
    tx: u8,
    parent: &miden_protocol::note::Note,
    amount: u64,
) -> (
    super::postgres_models::SettlementInputRow,
    miden_protocol::note::Note,
    Option<miden_protocol::note::Note>,
) {
    use miden_protocol::crypto::utils::Serializable;
    use miden_protocol::note::Note;

    let pswap = miden_standards::note::PswapNote::try_from(parent).unwrap();
    let requested = pswap.storage().requested_faucet_id();
    let (payback, remainder) = pswap
        .execute(
            super::postgres_db::test_consumer(),
            None,
            Some(miden_protocol::asset::FungibleAsset::new(requested, amount).unwrap()),
        )
        .unwrap();
    let remainder = remainder.map(Note::from);
    let row = super::postgres_models::SettlementInputRow {
        tx_id: vec![tx; 32],
        parent_note_id: parent.id().to_bytes().to_vec(),
        child_note_id: remainder.as_ref().map(|note| note.id().to_bytes().to_vec()),
        child_note_data: remainder.as_ref().map(Serializable::to_bytes),
        fill_amount: Some(i64::try_from(amount).unwrap()),
    };
    (row, payback, remainder)
}

/// Reserve `inputs` in a prepared settlement attempt `tx`.
pub fn prepare(
    conn: &mut PgConnection,
    tx: u8,
    inputs: &[super::postgres_models::SettlementInputRow],
) -> Result<()> {
    use diesel::Connection;

    let attempt = super::postgres_models::SettlementAttemptRow {
        tx_id: vec![tx; 32],
        tx_result: vec![tx],
        status: "prepared".into(),
    };
    conn.transaction::<_, super::DbError, _>(|conn| {
        super::postgres_db::prepare_settlement_tx(conn, &attempt, inputs, None)
    })?;
    Ok(())
}

/// Revert every migration newer than `version` (as `revert_last` names it,
/// e.g. `20261003000001`), so a test reaches that migration without
/// counting the ones after it.
pub fn revert_after(conn: &mut PgConnection, version: &str) -> Result<()> {
    loop {
        let applied = postgres_migrations::applied_versions(conn)?;
        anyhow::ensure!(applied.contains(version), "{version} is not applied");
        if applied.iter().next_back().map(String::as_str) == Some(version) {
            return Ok(());
        }
        postgres_migrations::revert_last(conn)?;
    }
}

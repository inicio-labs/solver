//! PostgreSQL schema setup for the operator and read-only verification for the
//! solver. Neither Miden client SQLite store is changed by these migrations.

use std::collections::BTreeSet;

use diesel::migration::MigrationSource;
use diesel::pg::{Pg, PgConnection};
use diesel::prelude::*;
use diesel::sql_types::{Nullable, Text};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

use super::error::{DbError, DbResult};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations_postgres");

#[derive(QueryableByName)]
struct AppliedVersion {
    #[diesel(sql_type = Text)]
    version: String,
}

#[derive(QueryableByName)]
struct HistoryTable {
    #[diesel(sql_type = Nullable<Text>)]
    name: Option<String>,
}

fn expected_versions() -> DbResult<BTreeSet<String>> {
    let migrations = MigrationSource::<Pg>::migrations(&MIGRATIONS)
        .map_err(|error| DbError::Migration(error.to_string()))?;
    Ok(migrations
        .into_iter()
        .map(|migration| migration.name().version().to_string())
        .collect())
}

fn applied_versions(conn: &mut PgConnection) -> DbResult<BTreeSet<String>> {
    Ok(
        diesel::sql_query("SELECT version FROM __diesel_schema_migrations ORDER BY version")
            .load::<AppliedVersion>(conn)
            .map_err(DbError::MissingMigrationHistory)?
            .into_iter()
            .map(|row| row.version)
            .collect(),
    )
}

/// Compare the database's applied migrations with this binary's.
fn schema_mismatch(expected: &BTreeSet<String>, applied: &BTreeSet<String>) -> Option<DbError> {
    let missing: Vec<String> = expected.difference(applied).cloned().collect();
    let unsupported: Vec<String> = applied.difference(expected).cloned().collect();
    (!missing.is_empty() || !unsupported.is_empty()).then_some(DbError::SchemaMismatch {
        missing,
        unsupported,
    })
}

/// Apply pending PostgreSQL schema changes with the operator's migration role.
/// This may create the Diesel migration-history table and must not run at
/// ordinary solver startup. A database already ahead of this binary is
/// refused rather than migrated.
pub fn migrate(conn: &mut PgConnection) -> DbResult<Vec<String>> {
    let history =
        diesel::sql_query("SELECT to_regclass('__diesel_schema_migrations')::text AS name")
            .get_result::<HistoryTable>(conn)?;
    if history.name.is_some() {
        let unsupported: Vec<String> = applied_versions(conn)?
            .difference(&expected_versions()?)
            .cloned()
            .collect();
        if !unsupported.is_empty() {
            return Err(DbError::SchemaMismatch {
                missing: Vec::new(),
                unsupported,
            });
        }
    }

    let applied = conn
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| DbError::Migration(error.to_string()))?
        .into_iter()
        .map(|version| version.to_string())
        .collect();
    verify(conn)?;
    Ok(applied)
}

/// Check, without DDL, that the applied migration history equals the set
/// compiled into this binary. Each migration creates its own tables, so an
/// exact history match also means the schema this binary expects is present.
pub fn verify(conn: &mut PgConnection) -> DbResult<()> {
    match schema_mismatch(&expected_versions()?, &applied_versions(conn)?) {
        Some(mismatch) => Err(mismatch),
        None => Ok(()),
    }
}

pub fn connect(database_url: &str) -> DbResult<PgConnection> {
    Ok(PgConnection::establish(database_url)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result};
    use diesel::connection::SimpleConnection;
    use diesel::sql_types::BigInt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

    struct SchemaFixture {
        conn: PgConnection,
        name: String,
    }

    impl SchemaFixture {
        fn new() -> Result<Self> {
            let url = std::env::var("SOLVER_TEST_DATABASE_URL")
                .context("set SOLVER_TEST_DATABASE_URL for PostgreSQL tests")?;
            let mut conn = connect(&url)?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!("solver_pg_{}_{}_{}", std::process::id(), nonce, sequence);
            conn.batch_execute(&format!("CREATE SCHEMA {name}; SET search_path TO {name}"))?;
            Ok(Self { conn, name })
        }
    }

    impl Drop for SchemaFixture {
        fn drop(&mut self) {
            let _ = self.conn.batch_execute("ROLLBACK");
            let _ = self.conn.batch_execute(&format!(
                "SET search_path TO public; DROP SCHEMA {} CASCADE",
                self.name
            ));
        }
    }

    #[derive(QueryableByName)]
    struct PriorityRow {
        #[diesel(sql_type = BigInt)]
        priority_seq: i64,
    }

    fn rejects(conn: &mut PgConnection, statement: &str) -> Result<()> {
        conn.batch_execute("SAVEPOINT rejected_statement")?;
        let result = conn.batch_execute(statement);
        conn.batch_execute("ROLLBACK TO SAVEPOINT rejected_statement")?;
        conn.batch_execute("RELEASE SAVEPOINT rejected_statement")?;
        assert!(
            result.is_err(),
            "statement unexpectedly succeeded: {statement}"
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn migration_history_matches_exactly_without_startup_ddl() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;

        assert!(
            verify(conn).is_err(),
            "an unmigrated schema must be rejected"
        );
        assert_eq!(migrate(conn)?.len(), expected_versions()?.len());
        assert!(
            migrate(conn)?.is_empty(),
            "operator migration is repeatable"
        );

        conn.batch_execute("BEGIN READ ONLY")?;
        let verified_read_only = verify(conn);
        conn.batch_execute("ROLLBACK")?;
        verified_read_only?;

        conn.batch_execute("BEGIN; DELETE FROM __diesel_schema_migrations")?;
        let older = verify(conn).expect_err("an older schema must be rejected");
        assert!(older.to_string().contains("missing migrations"));
        conn.batch_execute("ROLLBACK")?;

        conn.batch_execute(
            "BEGIN; INSERT INTO __diesel_schema_migrations (version) VALUES ('2099-01-01-000001')",
        )?;
        let newer = verify(conn).expect_err("a newer schema must be rejected");
        assert!(newer.to_string().contains("unsupported applied migrations"));
        let migration_error =
            migrate(conn).expect_err("the migration command must reject a newer schema");
        assert!(migration_error
            .to_string()
            .contains("unsupported applied migrations"));
        conn.batch_execute("ROLLBACK")?;
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn baseline_constraints_and_priority_inheritance() -> Result<()> {
        let mut fixture = SchemaFixture::new()?;
        let conn = &mut fixture.conn;
        migrate(conn)?;
        conn.batch_execute("BEGIN")?;

        let first: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, raw_data, arrival_unix)
             VALUES (decode('01', 'hex'), decode('11', 'hex'), 42)
             RETURNING priority_seq",
        )
        .get_result(conn)?;
        assert!(first.priority_seq > 0);

        let child: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, raw_data, arrival_unix, priority_seq)
             VALUES (decode('02', 'hex'), decode('22', 'hex'), 42, $1)
             RETURNING priority_seq",
        )
        .bind::<BigInt, _>(first.priority_seq)
        .get_result(conn)?;
        assert_eq!(child.priority_seq, first.priority_seq);

        let next: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, raw_data, arrival_unix)
             VALUES (decode('03', 'hex'), decode('33', 'hex'), 43)
             RETURNING priority_seq",
        )
        .get_result(conn)?;
        assert!(next.priority_seq > first.priority_seq);

        rejects(
            conn,
            "UPDATE orders SET status = 'invalid' WHERE note_id = decode('01', 'hex')",
        )?;
        rejects(
            conn,
            "UPDATE orders SET arrival_unix = -1 WHERE note_id = decode('01', 'hex')",
        )?;
        rejects(
            conn,
            "UPDATE sync_state SET last_fetched_block = -1 WHERE id = 1",
        )?;
        // The price API reads decimals as a u8.
        conn.batch_execute(
            "INSERT INTO registered_tokens (token_id, decimals) VALUES (decode('aa', 'hex'), 255)",
        )?;
        for decimals in [-1, 256] {
            rejects(
                conn,
                &format!("UPDATE registered_tokens SET decimals = {decimals}"),
            )?;
        }

        conn.batch_execute(
            "INSERT INTO settlement_attempts (tx_id, tx_result, status)
             VALUES (decode('fe', 'hex'), decode('ab', 'hex'), 'prepared')",
        )?;
        // Resolved attempts are deleted, never stored as confirmed/submitted.
        for status in ["invalid", "confirmed", "submitted"] {
            rejects(
                conn,
                &format!("UPDATE settlement_attempts SET status = '{status}'"),
            )?;
        }
        conn.batch_execute(
            "INSERT INTO settlement_inputs (tx_id, parent_note_id)
             VALUES (decode('fe', 'hex'), decode('01', 'hex'))",
        )?;
        rejects(
            conn,
            "INSERT INTO settlement_inputs (tx_id, parent_note_id)
             VALUES (decode('fe', 'hex'), decode('99', 'hex'))",
        )?;
        rejects(
            conn,
            "INSERT INTO settlement_inputs (tx_id, parent_note_id, child_note_id)
             VALUES (decode('fe', 'hex'), decode('02', 'hex'), decode('c1', 'hex'))",
        )?;
        conn.batch_execute(
            "INSERT INTO settlement_inputs (tx_id, parent_note_id, child_note_id, child_note_data)
             VALUES (decode('fe', 'hex'), decode('02', 'hex'), decode('c1', 'hex'), decode('d1', 'hex'))",
        )?;
        rejects(
            conn,
            "INSERT INTO settlement_inputs (tx_id, parent_note_id, child_note_id, child_note_data)
             VALUES (decode('fe', 'hex'), decode('03', 'hex'), decode('c1', 'hex'), decode('d2', 'hex'))",
        )?;
        // Deleting a resolved attempt removes its inputs with it.
        conn.batch_execute("DELETE FROM settlement_attempts WHERE tx_id = decode('fe', 'hex')")?;
        #[derive(QueryableByName)]
        struct Count {
            #[diesel(sql_type = BigInt)]
            count: i64,
        }
        let inputs: Count =
            diesel::sql_query("SELECT count(*)::bigint AS count FROM settlement_inputs")
                .get_result(conn)?;
        assert_eq!(inputs.count, 0);
        conn.batch_execute("ROLLBACK")?;
        Ok(())
    }
}

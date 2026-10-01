//! PostgreSQL schema setup for the operator and read-only verification for the
//! solver. Neither Miden client SQLite store is changed by these migrations.

use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use diesel::migration::MigrationSource;
use diesel::pg::{Pg, PgConnection};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

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

#[derive(QueryableByName)]
struct BaselineTables {
    #[diesel(sql_type = BigInt)]
    present: i64,
}

fn expected_versions() -> Result<BTreeSet<String>> {
    let migrations = MigrationSource::<Pg>::migrations(&MIGRATIONS)
        .map_err(|error| anyhow::anyhow!("load embedded PostgreSQL migrations: {error}"))?;
    Ok(migrations
        .into_iter()
        .map(|migration| migration.name().version().to_string())
        .collect())
}

fn applied_versions(conn: &mut PgConnection) -> Result<BTreeSet<String>> {
    Ok(
        diesel::sql_query("SELECT version FROM __diesel_schema_migrations ORDER BY version")
            .load::<AppliedVersion>(conn)
            .context("read PostgreSQL migration history (is the database initialized?)")?
            .into_iter()
            .map(|row| row.version)
            .collect(),
    )
}

/// Apply pending PostgreSQL schema changes with the operator's migration role.
/// This may create the Diesel migration-history table and must not run at
/// ordinary solver startup.
pub fn migrate(conn: &mut PgConnection) -> Result<Vec<String>> {
    let history =
        diesel::sql_query("SELECT to_regclass('__diesel_schema_migrations')::text AS name")
            .get_result::<HistoryTable>(conn)
            .context("locate PostgreSQL migration history")?;
    if history.name.is_some() {
        let expected = expected_versions()?;
        let applied = applied_versions(conn)?;
        let unsupported: Vec<_> = applied.difference(&expected).collect();
        if !unsupported.is_empty() {
            bail!("database contains migrations unknown to this solver binary: {unsupported:?}");
        }
    }

    let applied = conn
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| anyhow::anyhow!("apply PostgreSQL solver migrations: {error}"))?
        .into_iter()
        .map(|version| version.to_string())
        .collect();
    verify(conn)?;
    Ok(applied)
}

/// Check the complete migration history without issuing any DDL. A pending
/// check alone cannot detect a database upgraded beyond this solver binary.
pub fn verify(conn: &mut PgConnection) -> Result<()> {
    let expected = expected_versions()?;
    let applied = applied_versions(conn)?;

    if applied != expected {
        let missing: Vec<_> = expected.difference(&applied).collect();
        let unsupported: Vec<_> = applied.difference(&expected).collect();
        bail!(
            "PostgreSQL schema mismatch: missing migrations {missing:?}; unsupported applied migrations {unsupported:?}. Run migrate-db or use a compatible solver binary"
        );
    }
    let tables: BaselineTables = diesel::sql_query(
        "SELECT count(*)::bigint AS present
         FROM unnest(ARRAY[
             '__diesel_schema_migrations', 'sync_state', 'notes', 'orders',
             'settlement_attempts', 'settlement_inputs', 'registered_tokens'
         ]) AS expected(table_name)
         JOIN pg_class AS c ON c.oid = to_regclass(expected.table_name)
         WHERE c.relnamespace = (
             SELECT relnamespace FROM pg_class WHERE oid = to_regclass('sync_state')
         ) AND c.relkind IN ('r', 'p')",
    )
    .get_result(conn)
    .context("verify PostgreSQL baseline tables")?;
    if tables.present != 7 {
        bail!("PostgreSQL baseline tables are missing or resolve to different schemas");
    }
    Ok(())
}

pub fn connect(database_url: &str) -> Result<PgConnection> {
    PgConnection::establish(database_url).context("connect to PostgreSQL application database")
}

#[cfg(test)]
mod tests {
    use super::*;
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

        conn.batch_execute("BEGIN; DROP TABLE registered_tokens")?;
        let missing = verify(conn).expect_err("a missing baseline table must be rejected");
        assert!(missing.to_string().contains("baseline tables"));
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
            .contains("unknown to this solver binary"));
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
        conn.batch_execute(
            "INSERT INTO notes (note_id, account_id, raw_data) VALUES
             (decode('01', 'hex'), decode('aa', 'hex'), decode('11', 'hex')),
             (decode('02', 'hex'), decode('aa', 'hex'), decode('22', 'hex')),
             (decode('03', 'hex'), decode('aa', 'hex'), decode('33', 'hex'))",
        )?;

        let first: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, account_id, requested_asset, requested_amount,
              offered_asset, offered_amount, arrival_unix)
             VALUES (decode('01', 'hex'), decode('aa', 'hex'), decode('10', 'hex'), 1,
                     decode('20', 'hex'), 2, 42)
             RETURNING priority_seq",
        )
        .get_result(conn)?;
        assert!(first.priority_seq > 0);

        let child: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, account_id, requested_asset, requested_amount,
              offered_asset, offered_amount, arrival_unix, priority_seq)
             VALUES (decode('02', 'hex'), decode('aa', 'hex'), decode('10', 'hex'), 1,
                     decode('20', 'hex'), 2, 42, $1)
             RETURNING priority_seq",
        )
        .bind::<BigInt, _>(first.priority_seq)
        .get_result(conn)?;
        assert_eq!(child.priority_seq, first.priority_seq);

        let next: PriorityRow = diesel::sql_query(
            "INSERT INTO orders (note_id, account_id, requested_asset, requested_amount,
              offered_asset, offered_amount, arrival_unix)
             VALUES (decode('03', 'hex'), decode('aa', 'hex'), decode('10', 'hex'), 1,
                     decode('20', 'hex'), 2, 43)
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
            "UPDATE sync_state SET last_fetched_block = -1 WHERE id = 1",
        )?;
        rejects(
            conn,
            "INSERT INTO orders (note_id, account_id, requested_asset, requested_amount,
              offered_asset, offered_amount, arrival_unix)
             VALUES (decode('99', 'hex'), decode('aa', 'hex'), decode('10', 'hex'), 1,
                     decode('20', 'hex'), 2, 42)",
        )?;

        conn.batch_execute(
            "INSERT INTO settlement_attempts (tx_id, tx_result, status, created_at_unix)
             VALUES (decode('fe', 'hex'), decode('ab', 'hex'), 'prepared', 42)",
        )?;
        rejects(conn, "UPDATE settlement_attempts SET status = 'invalid'")?;
        conn.batch_execute(
            "INSERT INTO settlement_inputs (tx_id, parent_note_id, payback_note_id)
             VALUES (decode('fe', 'hex'), decode('01', 'hex'), decode('b1', 'hex'))",
        )?;
        rejects(
            conn,
            "INSERT INTO settlement_inputs
              (tx_id, parent_note_id, payback_note_id, child_note_id)
             VALUES (decode('fe', 'hex'), decode('02', 'hex'), decode('b2', 'hex'), decode('c1', 'hex'))",
        )?;
        conn.batch_execute(
            "INSERT INTO settlement_inputs
              (tx_id, parent_note_id, payback_note_id, child_note_id, child_note_data)
             VALUES (decode('fe', 'hex'), decode('02', 'hex'), decode('b2', 'hex'),
                     decode('c1', 'hex'), decode('d1', 'hex'))",
        )?;
        rejects(
            conn,
            "INSERT INTO settlement_inputs
              (tx_id, parent_note_id, payback_note_id, child_note_id, child_note_data)
             VALUES (decode('fe', 'hex'), decode('03', 'hex'), decode('b3', 'hex'),
                     decode('c1', 'hex'), decode('d2', 'hex'))",
        )?;
        conn.batch_execute("ROLLBACK")?;
        Ok(())
    }
}

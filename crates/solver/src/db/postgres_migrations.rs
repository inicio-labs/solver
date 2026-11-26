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

pub(crate) fn applied_versions(conn: &mut PgConnection) -> DbResult<BTreeSet<String>> {
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
    super::postgres_pool::with_ownership_lock(conn, migrate_locked)
}

fn migrate_locked(conn: &mut PgConnection) -> DbResult<Vec<String>> {
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

/// Roll back the newest applied migration with its `down.sql`, so an older
/// solver binary can start again. Refuses to revert the baseline, whose
/// `down.sql` drops every application table.
pub fn revert_last(conn: &mut PgConnection) -> DbResult<String> {
    super::postgres_pool::with_ownership_lock(conn, |conn| {
        let applied = applied_versions(conn)?;
        let baseline = expected_versions()?.into_iter().next();
        if applied.len() <= 1 && applied.iter().next() == baseline.as_ref() {
            return Err(DbError::Migration(
                "refusing to revert the baseline migration: it drops every application table"
                    .into(),
            ));
        }
        conn.revert_last_migration(MIGRATIONS)
            .map(|version| version.to_string())
            .map_err(|error| DbError::Migration(error.to_string()))
    })
}

/// The live-at-accept escape hatch (ADR 0008): with the solver stopped, close
/// every order that went live at accept and still lacks its note. Refuses
/// while a solver owns the database.
pub fn cancel_unconfirmed(conn: &mut PgConnection) -> DbResult<super::maker_db::HatchReport> {
    verify(conn)?;
    super::postgres_pool::with_ownership_lock(conn, |conn| {
        conn.transaction(super::maker_db::cancel_unconfirmed_tx)
    })
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

/// Views a solver session reads directly, so its own role needs `SELECT` on
/// each. A view reached only through another view is checked against that
/// view's owner instead.
const DIRECTLY_READ_VIEWS: [&str; 3] = ["live_orders", "eligible_orders", "eligible_lineages"];

/// Check that this session's role can read the views the solver reads
/// directly. A migration that adds one needs a grant of its own unless the
/// deployment's default grants cover it. Without this check a missing grant
/// passes startup, since hydration reads `live_orders` and reaches
/// `eligible_orders` as that view's owner, and then fails the first ingest
/// or settlement write with "permission denied".
pub fn verify_access(conn: &mut PgConnection) -> DbResult<()> {
    for relation in DIRECTLY_READ_VIEWS {
        diesel::sql_query(format!("SELECT 1 FROM {relation} LIMIT 0"))
            .execute(conn)
            .map_err(|source| DbError::Unreadable { relation, source })?;
    }
    Ok(())
}

pub fn connect(database_url: &str) -> DbResult<PgConnection> {
    Ok(PgConnection::establish(database_url)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_test::TestSchema;
    use anyhow::Result;
    use diesel::connection::SimpleConnection;
    use diesel::sql_types::BigInt;

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

    /// Reverting the live-at-accept migration keeps the grants on
    /// `live_orders`: the previous binary reads it at startup.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_revert_keeps_the_grants_on_live_orders() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let (schema, conn) = (fixture.name.clone(), &mut fixture.conn);
        conn.batch_execute(&format!("GRANT SELECT ON {schema}.live_orders TO PUBLIC"))?;
        #[derive(QueryableByName)]
        struct Acl {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            acl: Option<String>,
        }
        let public_reads = |conn: &mut PgConnection| -> Result<bool> {
            let acl = diesel::sql_query(format!(
                "SELECT relacl::text AS acl FROM pg_class WHERE oid = '{schema}.live_orders'::regclass"
            ))
            .get_result::<Acl>(conn)?
            .acl
            .unwrap_or_default();
            // PUBLIC's entry has an empty grantee: "=r/owner".
            Ok(acl.contains("\"=r/") || acl.contains(",=r/") || acl.contains("{=r/"))
        };
        assert!(public_reads(conn)?);
        crate::db::postgres_test::revert_after(conn, "20261114000001")?;
        assert_eq!(revert_last(conn)?, "20261114000001");
        assert!(public_reads(conn)?, "the revert dropped the grant");
        Ok(())
    }

    /// The watcher-index migration adds the partial index and the `orders`
    /// vacuum settings. Its revert removes both and leaves any other
    /// per-table setting, such as one an operator set by hand, as it is.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn the_watcher_index_migration_reverts_cleanly() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let (schema, conn) = (fixture.name.clone(), &mut fixture.conn);
        #[derive(QueryableByName)]
        struct Shape {
            #[diesel(sql_type = diesel::sql_types::Bool)]
            indexed: bool,
            #[diesel(sql_type = Nullable<Text>)]
            options: Option<String>,
        }
        let shape = |conn: &mut PgConnection| -> Result<Shape> {
            Ok(diesel::sql_query(format!(
                "SELECT to_regclass('{schema}.orders_live_status_idx') IS NOT NULL AS indexed,
                        reloptions::text AS options
                 FROM pg_class WHERE oid = '{schema}.orders'::regclass"
            ))
            .get_result(conn)?)
        };
        let migrated = shape(conn)?;
        assert!(migrated.indexed);
        assert_eq!(
            migrated.options.as_deref(),
            Some(
                "{autovacuum_vacuum_scale_factor=0.01,autovacuum_vacuum_threshold=1000,\
                 vacuum_index_cleanup=on,log_autovacuum_min_duration=0}"
            )
        );
        conn.batch_execute("ALTER TABLE orders SET (autovacuum_analyze_scale_factor = 0.01)")?;
        crate::db::postgres_test::revert_after(conn, "20261119000001")?;
        assert_eq!(revert_last(conn)?, "20261119000001");
        let reverted = shape(conn)?;
        assert!(!reverted.indexed);
        assert_eq!(
            reverted.options.as_deref(),
            Some("{autovacuum_analyze_scale_factor=0.01}")
        );
        Ok(())
    }

    /// The self-trade storage migration reverts cleanly: the trigger, the
    /// function, the view, the columns and the indexes go, and orders keep
    /// being stored.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn the_stp_storage_migration_reverts_cleanly() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let (schema, conn) = (fixture.name.clone(), &mut fixture.conn);
        crate::db::postgres_test::revert_after(conn, "20261125000001")?;
        #[derive(QueryableByName)]
        struct Left {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }
        let left = |conn: &mut PgConnection| -> Result<i64> {
            Ok(diesel::sql_query(format!(
                "SELECT (SELECT count(*) FROM pg_trigger
                         WHERE tgrelid = '{schema}.orders'::regclass
                           AND tgname = 'orders_fill_maker_id')
                      + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                         WHERE n.nspname = '{schema}' AND p.proname = 'orders_fill_maker_id')
                      + (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE n.nspname = '{schema}'
                           AND c.relname IN ('eligible_lineages', 'maker_lineages_seq_idx',
                                             'maker_lineages_pending_group_idx', 'orders_maker_open_idx'))
                      + (SELECT count(*) FROM information_schema.columns
                         WHERE table_schema = '{schema}'
                           AND (table_name, column_name) IN (
                               ('orders', 'maker_id'), ('maker_stops', 'source'),
                               ('maker_stops', 'caused_by_lineage'),
                               ('maker_lineages', 'offered_amount'),
                               ('maker_lineages', 'requested_amount'))) AS count"
            ))
            .get_result::<Left>(conn)?
            .count)
        };
        assert_eq!(left(conn)?, 1 + 1 + 4 + 5);
        let lineage_options = |conn: &mut PgConnection| -> Result<Option<String>> {
            #[derive(QueryableByName)]
            struct Options {
                #[diesel(sql_type = Nullable<Text>)]
                options: Option<String>,
            }
            Ok(diesel::sql_query(format!(
                "SELECT reloptions::text AS options FROM pg_class
                 WHERE oid = '{schema}.maker_lineages'::regclass"
            ))
            .get_result::<Options>(conn)?
            .options)
        };
        assert_eq!(
            lineage_options(conn)?.as_deref(),
            Some(
                "{autovacuum_vacuum_scale_factor=0.01,autovacuum_vacuum_threshold=1000,\
                 vacuum_index_cleanup=on,log_autovacuum_min_duration=0}"
            )
        );
        // Amounts come in pairs; only a self-trade stop names its cause.
        conn.batch_execute("BEGIN")?;
        rejects(
            conn,
            "INSERT INTO makers (name) VALUES ('m');
             INSERT INTO maker_commands (maker_id, request_id, seq, kind, payload, result)
             SELECT maker_id, 'r', 1, 'submit', '\\x00', '' FROM makers;
             INSERT INTO maker_lineages (lineage_id, maker_id, request_id, root_seq, note_id,
                                         offered_amount)
             SELECT '\\x01', maker_id, 'r', 1, '\\x02', 5 FROM makers",
        )?;
        for source in ["'maker'", "NULL"] {
            rejects(
                conn,
                &format!(
                    "INSERT INTO makers (name) VALUES ('m');
                     INSERT INTO maker_stops (maker_id, lineage_id, source, caused_by_lineage)
                     SELECT maker_id, '\\x01', {source}, '\\x02' FROM makers"
                ),
            )?;
        }
        conn.batch_execute("ROLLBACK")?;
        assert_eq!(revert_last(conn)?, "20261125000001");
        assert_eq!(left(conn)?, 0);
        assert_eq!(lineage_options(conn)?, None);
        conn.batch_execute(
            "INSERT INTO orders (note_id, raw_data, arrival_unix, status, priority_seq, lineage_id)
             VALUES ('\\x01', '\\x01', 1, 'active', 1, '\\x02')",
        )?;
        Ok(())
    }

    /// A runtime role that was granted the old view but not the new one
    /// passes the history check and can still read the book (`live_orders`
    /// reaches `eligible_orders` as its owner). The access check names the
    /// view it cannot read.
    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn a_role_that_cannot_read_a_view_is_refused_by_name() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let (schema, conn) = (fixture.name.clone(), &mut fixture.conn);
        let role = format!("{schema}_r");
        // Rolled back at the end, so the role never outlives the test.
        conn.batch_execute(&format!(
            "BEGIN; CREATE ROLE {role} NOLOGIN;
             GRANT USAGE ON SCHEMA {schema} TO {role};
             GRANT SELECT ON {schema}.__diesel_schema_migrations, {schema}.live_orders TO {role}"
        ))?;
        let as_role = |conn: &mut PgConnection| -> Result<DbResult<()>> {
            conn.batch_execute(&format!("SAVEPOINT as_role; SET LOCAL ROLE {role}"))?;
            let checked = verify(conn).and_then(|()| {
                diesel::sql_query("SELECT 1 FROM live_orders LIMIT 0").execute(conn)?;
                verify_access(conn)
            });
            conn.batch_execute("ROLLBACK TO SAVEPOINT as_role; RELEASE SAVEPOINT as_role")?;
            Ok(checked)
        };
        let refused = as_role(conn)?.expect_err("eligible_orders is not granted");
        assert!(
            matches!(
                refused,
                DbError::Unreadable {
                    relation: "eligible_orders",
                    ..
                }
            ),
            "{refused:?}"
        );
        conn.batch_execute(&format!(
            "GRANT SELECT ON {schema}.eligible_orders TO {role}"
        ))?;
        let refused = as_role(conn)?.expect_err("eligible_lineages is not granted");
        assert!(
            matches!(
                refused,
                DbError::Unreadable {
                    relation: "eligible_lineages",
                    ..
                }
            ),
            "{refused:?}"
        );
        conn.batch_execute(&format!(
            "GRANT SELECT ON {schema}.eligible_lineages TO {role}"
        ))?;
        as_role(conn)??;
        conn.batch_execute("ROLLBACK")?;
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a local PostgreSQL service"]
    fn migration_history_matches_exactly_without_startup_ddl() -> Result<()> {
        let mut fixture = TestSchema::new()?;
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
        let mut fixture = TestSchema::new()?;
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

//! PostgreSQL connection ownership and the blocking boundary for Diesel.
//!
//! The writer is one physical session for this pool's entire lifetime. Its
//! session advisory lock protects the shared application database, while a
//! Tokio mutex serializes individual transactions. Read concurrency is bounded
//! before a blocking worker is spawned.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use super::postgres_migrations;
use crate::types::BookUpdate;

/// "SOLVERV1" as a positive signed 64-bit PostgreSQL advisory-lock key.
/// Ownership is scoped by the database, not by the configured solver account.
pub const APPLICATION_LOCK_KEY: i64 = 0x534F_4C56_4552_5631;

const LATENCY_BUCKET_US: [u64; 8] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000,
];
// A server-side statement timeout does not bound a client waiting for a lost
// network reply. A timed-out blocking operation keeps its permit while the
// coordinated solver shuts down for a supervised restart.
const DEFAULT_OPERATION_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Default)]
struct PoolTelemetry {
    read_total: AtomicU64,
    read_errors: AtomicU64,
    read_wait_us: AtomicU64,
    read_wait_buckets: [AtomicU64; 9],
    read_duration_us: AtomicU64,
    read_duration_buckets: [AtomicU64; 9],
    write_total: AtomicU64,
    write_errors: AtomicU64,
    writer_wait_us: AtomicU64,
    writer_wait_buckets: [AtomicU64; 9],
    write_duration_us: AtomicU64,
    write_duration_buckets: [AtomicU64; 9],
    lock_timeouts: AtomicU64,
    statement_timeouts: AtomicU64,
    deadlocks: AtomicU64,
}

/// Monotonic totals and current pool occupancy for an external metrics scraper.
#[derive(Debug, Clone, Copy)]
pub struct PoolTelemetrySnapshot {
    pub read_total: u64,
    pub read_errors: u64,
    pub read_wait_us: u64,
    pub read_wait_buckets: [u64; 9],
    pub read_duration_us: u64,
    pub read_duration_buckets: [u64; 9],
    pub write_total: u64,
    pub write_errors: u64,
    pub writer_wait_us: u64,
    pub writer_wait_buckets: [u64; 9],
    pub write_duration_us: u64,
    pub write_duration_buckets: [u64; 9],
    pub lock_timeouts: u64,
    pub statement_timeouts: u64,
    pub deadlocks: u64,
    pub read_connections: u32,
    pub read_idle_connections: u32,
    pub writer_busy: bool,
    pub ownership_lost: bool,
    pub fatal_shutdown_requested: bool,
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn record_latency(buckets: &[AtomicU64; 9], duration_us: u64) {
    for (index, bound) in LATENCY_BUCKET_US.iter().enumerate() {
        if duration_us <= *bound {
            buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }
    buckets[8].fetch_add(1, Ordering::Relaxed);
}

fn load_buckets(buckets: &[AtomicU64; 9]) -> [u64; 9] {
    std::array::from_fn(|index| buckets[index].load(Ordering::Relaxed))
}

// `diesel::r2d2::Error` is Diesel's own enum; a failed pool checkout surfaces
// as the r2d2 crate's `PoolError`. Both mean the connection is unusable.
// Message matching below relies on `lc_messages = 'C'` from `configure_session`.
fn database_error_class(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<r2d2::Error>().is_some()
        || error.downcast_ref::<r2d2::PoolError>().is_some()
    {
        return "connection";
    }
    match error.downcast_ref::<DieselError>() {
        Some(DieselError::DatabaseError(DatabaseErrorKind::ClosedConnection, _)) => "connection",
        Some(DieselError::DatabaseError(_, info))
            if info.message().contains("deadlock detected") =>
        {
            "deadlock"
        }
        Some(DieselError::DatabaseError(_, info)) if info.message().contains("lock timeout") => {
            "lock_timeout"
        }
        Some(DieselError::DatabaseError(_, info))
            if info.message().contains("statement timeout") =>
        {
            "statement_timeout"
        }
        Some(DieselError::DatabaseError(_, _)) => "database",
        Some(DieselError::NotFound) => "not_found",
        Some(_) => "diesel",
        None => "other",
    }
}

impl PoolTelemetry {
    fn record_error(&self, class: &str) {
        let counter = match class {
            "lock_timeout" => Some(&self.lock_timeouts),
            "statement_timeout" => Some(&self.statement_timeouts),
            "deadlock" => Some(&self.deadlocks),
            _ => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(QueryableByName)]
struct LockResult {
    #[diesel(sql_type = Bool)]
    acquired: bool,
}

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct DatabaseIdentity {
    #[diesel(sql_type = Text)]
    database_name: String,
    #[diesel(sql_type = Text)]
    schema_name: String,
    #[diesel(sql_type = Nullable<Text>)]
    server_address: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    server_port: Option<i32>,
}

fn database_identity(conn: &mut PgConnection) -> Result<DatabaseIdentity> {
    diesel::sql_query(
        "SELECT current_database()::text AS database_name,
                n.nspname::text AS schema_name,
                inet_server_addr()::text AS server_address,
                inet_server_port() AS server_port
         FROM pg_class AS c
         JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE c.oid = 'sync_state'::regclass",
    )
    .get_result::<DatabaseIdentity>(conn)
    .context("resolve PostgreSQL application database and schema")
}

// Public is the normal application schema and uses the documented fixed key.
// An explicitly selected non-public schema is a separate application dataset;
// derive a stable key so per-schema PostgreSQL integration tests can run in
// parallel without weakening single-owner protection within any one schema.
fn schema_lock_key(schema: &str) -> i64 {
    if schema == "public" {
        return APPLICATION_LOCK_KEY;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in b"solver/schema/".iter().chain(schema.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash as i64
}

#[derive(QueryableByName)]
struct SettingResult {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(QueryableByName)]
struct WriterHealth {
    #[diesel(sql_type = Integer)]
    backend_pid: i32,
    #[diesel(sql_type = Bool)]
    owns_advisory_lock: bool,
}

fn writer_health(conn: &mut PgConnection, lock_key: i64) -> diesel::QueryResult<WriterHealth> {
    diesel::sql_query(
        "SELECT pg_backend_pid() AS backend_pid,
                EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE pid = pg_backend_pid() AND locktype = 'advisory'
                      AND granted AND mode = 'ExclusiveLock'
                      AND classid::bigint = $1 AND objid::bigint = $2
                      AND objsubid = 1
                ) AS owns_advisory_lock",
    )
    .bind::<BigInt, _>(((lock_key as u64 >> 32) & 0xffff_ffff) as i64)
    .bind::<BigInt, _>((lock_key as u64 & 0xffff_ffff) as i64)
    .get_result(conn)
}

fn configure_session(conn: &mut PgConnection, application_name: &str) -> diesel::QueryResult<()> {
    conn.batch_execute(
        "SET default_transaction_isolation = 'read committed';
         SET lc_messages = 'C';
         SET statement_timeout = '10s';
         SET lock_timeout = '2s';
         SET idle_in_transaction_session_timeout = '10s'",
    )?;
    let setting = diesel::sql_query("SELECT set_config('application_name', $1, false) AS value")
        .bind::<Text, _>(application_name)
        .get_result::<SettingResult>(conn)?;
    if setting.value != application_name {
        return Err(diesel::result::Error::QueryBuilderError(Box::new(
            std::io::Error::other("PostgreSQL application_name was not applied"),
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct ReadCustomizer {
    application_name: String,
}

impl r2d2::CustomizeConnection<PgConnection, r2d2::Error> for ReadCustomizer {
    fn on_acquire(&self, conn: &mut PgConnection) -> std::result::Result<(), r2d2::Error> {
        configure_session(conn, &self.application_name).map_err(r2d2::Error::QueryError)
    }
}

#[derive(Clone)]
pub struct PgPool {
    writer: Arc<Mutex<PgConnection>>,
    lock_key: i64,
    writer_backend_pid: i32,
    ownership_lost: Arc<AtomicBool>,
    fatal_db: CancellationToken,
    operation_deadline: Duration,
    admin_order: Arc<Mutex<()>>,
    publish_order: Arc<Mutex<()>>,
    readers: r2d2::Pool<ConnectionManager<PgConnection>>,
    read_slots: Arc<Semaphore>,
    telemetry: Arc<PoolTelemetry>,
}

impl PgPool {
    /// Open and verify the sole writer before any solver worker starts. The
    /// writer session must never be silently replaced: losing it loses the
    /// PostgreSQL-held ownership lock and requires a whole-solver restart.
    pub async fn open(
        writer_url: String,
        reader_url: String,
        read_pool_size: u32,
        application_name: String,
    ) -> Result<Self> {
        if read_pool_size == 0 {
            bail!("PostgreSQL read_pool_size must be at least one");
        }
        let (writer, writer_backend_pid, readers, lock_key) =
            tokio::time::timeout(DEFAULT_OPERATION_DEADLINE, tokio::task::spawn_blocking(move || -> Result<_> {
                let mut writer = postgres_migrations::connect(&writer_url)?;
                configure_session(&mut writer, &application_name)
                    .context("configure PostgreSQL writer session")?;
                postgres_migrations::verify(&mut writer)?;
                let writer_identity = database_identity(&mut writer)?;
                let lock_key = schema_lock_key(&writer_identity.schema_name);
                let lock = diesel::sql_query("SELECT pg_try_advisory_lock($1) AS acquired")
                    .bind::<BigInt, _>(lock_key)
                    .get_result::<LockResult>(&mut writer)
                    .context("acquire PostgreSQL solver ownership")?;
                if !lock.acquired {
                    bail!("another solver already owns this PostgreSQL application database");
                }
                let health = writer_health(&mut writer, lock_key)
                    .context("verify PostgreSQL writer owns its advisory lock")?;
                if !health.owns_advisory_lock {
                    bail!("PostgreSQL writer did not retain the application advisory lock");
                }
                let writer_backend_pid = health.backend_pid;

                let readers = r2d2::Pool::builder()
                    .max_size(read_pool_size)
                    .connection_timeout(Duration::from_secs(5))
                    .test_on_check_out(true)
                    .connection_customizer(Box::new(ReadCustomizer {
                        application_name: format!("{application_name}/read"),
                    }))
                    .build(ConnectionManager::<PgConnection>::new(reader_url))
                    .context("open PostgreSQL read pool")?;
                {
                    let mut reader = readers.get().context("check PostgreSQL read pool")?;
                    postgres_migrations::verify(&mut reader)?;
                    let reader_identity = database_identity(&mut reader)?;
                    if reader_identity != writer_identity {
                        bail!("PostgreSQL reader target differs from writer application database/schema: writer={writer_identity:?}, reader={reader_identity:?}");
                    }
                }
                Ok((writer, writer_backend_pid, readers, lock_key))
            }))
            .await
            .context("PostgreSQL startup exceeded thirty seconds; restart the whole solver")?
            .context("PostgreSQL startup worker stopped")??;

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            lock_key,
            writer_backend_pid,
            ownership_lost: Arc::new(AtomicBool::new(false)),
            fatal_db: CancellationToken::new(),
            operation_deadline: DEFAULT_OPERATION_DEADLINE,
            admin_order: Arc::new(Mutex::new(())),
            publish_order: Arc::new(Mutex::new(())),
            readers,
            read_slots: Arc::new(Semaphore::new(read_pool_size as usize)),
            telemetry: Arc::new(PoolTelemetry::default()),
        })
    }

    pub fn telemetry_snapshot(&self) -> PoolTelemetrySnapshot {
        let state = self.readers.state();
        let load = Ordering::Relaxed;
        PoolTelemetrySnapshot {
            read_total: self.telemetry.read_total.load(load),
            read_errors: self.telemetry.read_errors.load(load),
            read_wait_us: self.telemetry.read_wait_us.load(load),
            read_wait_buckets: load_buckets(&self.telemetry.read_wait_buckets),
            read_duration_us: self.telemetry.read_duration_us.load(load),
            read_duration_buckets: load_buckets(&self.telemetry.read_duration_buckets),
            write_total: self.telemetry.write_total.load(load),
            write_errors: self.telemetry.write_errors.load(load),
            writer_wait_us: self.telemetry.writer_wait_us.load(load),
            writer_wait_buckets: load_buckets(&self.telemetry.writer_wait_buckets),
            write_duration_us: self.telemetry.write_duration_us.load(load),
            write_duration_buckets: load_buckets(&self.telemetry.write_duration_buckets),
            lock_timeouts: self.telemetry.lock_timeouts.load(load),
            statement_timeouts: self.telemetry.statement_timeouts.load(load),
            deadlocks: self.telemetry.deadlocks.load(load),
            read_connections: state.connections,
            read_idle_connections: state.idle_connections,
            writer_busy: self.writer.try_lock().is_err(),
            ownership_lost: self.ownership_lost.load(Ordering::Acquire),
            fatal_shutdown_requested: self.fatal_db.is_cancelled(),
        }
    }

    /// A database failure requiring coordinated shutdown and startup hydration.
    pub fn fatal_token(&self) -> CancellationToken {
        self.fatal_db.clone()
    }

    /// Reads never request a solver shutdown. The read pool holds no
    /// ownership state, so a failed read is reported to its caller and the
    /// pool reconnects on the next checkout. Only the writer can be fatal.
    pub async fn read<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
    {
        if self.fatal_db.is_cancelled() {
            bail!("PostgreSQL operation failed critically; restart the whole solver");
        }
        let operation_name = std::any::type_name::<F>();
        self.telemetry.read_total.fetch_add(1, Ordering::Relaxed);
        let waiting_since = Instant::now();
        let permit = tokio::time::timeout(
            Duration::from_secs(5),
            self.read_slots.clone().acquire_owned(),
        )
        .await;
        let wait_us = elapsed_us(waiting_since);
        self.telemetry
            .read_wait_us
            .fetch_add(wait_us, Ordering::Relaxed);
        record_latency(&self.telemetry.read_wait_buckets, wait_us);
        let permit = match permit {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                self.telemetry.read_errors.fetch_add(1, Ordering::Relaxed);
                return Err(error).context("PostgreSQL read pool closed");
            }
            Err(_) => {
                self.telemetry.read_errors.fetch_add(1, Ordering::Relaxed);
                bail!("PostgreSQL read pool wait exceeded five seconds");
            }
        };
        let readers = self.readers.clone();
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.operation_deadline,
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let mut conn = readers
                    .get()
                    .context("check out PostgreSQL read connection")?;
                operation(&mut conn)
            }),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(error).context("PostgreSQL read worker stopped"),
            // The blocking worker keeps its permit until libpq returns, so a
            // lost reply costs one read slot until then, not the solver.
            Err(_) => Err(anyhow::anyhow!(
                "PostgreSQL read exceeded {:?}",
                self.operation_deadline
            )),
        };
        let duration_us = elapsed_us(started);
        self.telemetry
            .read_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
        record_latency(&self.telemetry.read_duration_buckets, duration_us);
        if let Err(error) = &result {
            self.telemetry.read_errors.fetch_add(1, Ordering::Relaxed);
            let class = database_error_class(error);
            self.telemetry.record_error(class);
            tracing::warn!(%error, class, operation = operation_name, duration_us, "PostgreSQL read failed");
        } else if duration_us > 100_000 {
            tracing::warn!(
                operation = operation_name,
                duration_us,
                "slow PostgreSQL read"
            );
        }
        result
    }

    /// Own exactly one application write transaction. The operation permit
    /// stays with the blocking worker even if its async caller is cancelled.
    ///
    /// Only losing the writer session is fatal: a dropped connection, a lost
    /// advisory lock, a worker panic, or a deadline with an uncertain commit.
    /// Lock and statement timeouts roll back one transaction on a healthy
    /// session; they are returned to the caller, who re-feeds or retries.
    pub async fn write<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
    {
        let operation_name = std::any::type_name::<F>();
        self.telemetry.write_total.fetch_add(1, Ordering::Relaxed);
        if self.ownership_lost.load(Ordering::Acquire) || self.fatal_db.is_cancelled() {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            bail!("PostgreSQL writer is no longer safe; restart the whole solver");
        }
        let waiting_since = Instant::now();
        let mut writer =
            match tokio::time::timeout(Duration::from_secs(30), self.writer.clone().lock_owned())
                .await
            {
                Ok(writer) => writer,
                Err(_) => {
                    let wait_us = elapsed_us(waiting_since);
                    self.telemetry
                        .writer_wait_us
                        .fetch_add(wait_us, Ordering::Relaxed);
                    record_latency(&self.telemetry.writer_wait_buckets, wait_us);
                    self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
                    self.fatal_db.cancel();
                    bail!(
                        "PostgreSQL writer wait exceeded thirty seconds; restart the whole solver"
                    );
                }
            };
        let wait_us = elapsed_us(waiting_since);
        self.telemetry
            .writer_wait_us
            .fetch_add(wait_us, Ordering::Relaxed);
        record_latency(&self.telemetry.writer_wait_buckets, wait_us);
        if self.ownership_lost.load(Ordering::Acquire) || self.fatal_db.is_cancelled() {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            bail!("PostgreSQL writer is no longer safe; restart the whole solver");
        }
        let lost_in_worker = self.ownership_lost.clone();
        let lost_on_panic = self.ownership_lost.clone();
        let fatal_in_worker = self.fatal_db.clone();
        let expected_pid = self.writer_backend_pid;
        let lock_key = self.lock_key;
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.operation_deadline,
            tokio::task::spawn_blocking(move || {
                let result = writer.transaction(|conn| operation(conn));
                if result.is_err() {
                    // A broken session or missing ownership lock must never
                    // permit an in-process retry on a replacement session.
                    match writer_health(&mut writer, lock_key) {
                        Ok(health)
                            if health.backend_pid == expected_pid && health.owns_advisory_lock => {}
                        _ => {
                            lost_in_worker.store(true, Ordering::Release);
                            fatal_in_worker.cancel();
                        }
                    }
                }
                result
            }),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                lost_on_panic.store(true, Ordering::Release);
                self.fatal_db.cancel();
                Err(error).context("PostgreSQL writer worker stopped")
            }
            Err(_) => {
                lost_on_panic.store(true, Ordering::Release);
                self.fatal_db.cancel();
                Err(anyhow::anyhow!(
                    "PostgreSQL write exceeded {:?}; commit outcome is uncertain; restart the whole solver",
                    self.operation_deadline
                ))
            }
        };
        let duration_us = elapsed_us(started);
        self.telemetry
            .write_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
        record_latency(&self.telemetry.write_duration_buckets, duration_us);
        if let Err(error) = &result {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            let class = database_error_class(error);
            self.telemetry.record_error(class);
            if class == "connection" {
                self.ownership_lost.store(true, Ordering::Release);
                self.fatal_db.cancel();
            }
            tracing::error!(%error, class, operation = operation_name, duration_us, "PostgreSQL write failed");
        } else if duration_us > 100_000 {
            tracing::warn!(
                operation = operation_name,
                duration_us,
                "slow PostgreSQL write"
            );
        }
        result
    }

    /// Commit a book-changing transaction and publish its update to the
    /// matcher in commit order. The matcher applies updates as deltas and
    /// recreates any order named `active`, so a stale activation delivered
    /// after a later removal would put a consumed order back in its book.
    ///
    /// The publish guard spans commit and send. Plain `write` calls do not
    /// take it, so a full matcher channel delays only other publishers, and
    /// the matcher never takes it, so waiting for capacity cannot deadlock.
    pub async fn write_book<F>(&self, sender: &mpsc::Sender<BookUpdate>, operation: F) -> Result<()>
    where
        F: FnOnce(&mut PgConnection) -> Result<BookUpdate> + Send + 'static,
    {
        let _publish = self.publish_order.lock().await;
        let update = self.write(operation).await?;
        if !update.is_empty() {
            sender
                .send(update)
                .await
                .context("matcher stopped: book update receiver closed")?;
        }
        Ok(())
    }

    /// Serialize each admin token mutation and its synchronous symbol-cache
    /// update. Once the operation starts, an HTTP caller dropping its future
    /// cannot abandon the cache update after the database commit. Subscription
    /// delivery belongs after this method returns, outside both guards.
    pub async fn admin_write<T, F, C>(&self, operation: F, update_cache: C) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
        C: FnOnce(&T) + Send + 'static,
    {
        let admin_guard = self.admin_order.clone().lock_owned().await;
        let pool = self.clone();
        tokio::spawn(async move {
            let _admin_guard = admin_guard;
            let result = pool.write(operation).await?;
            update_cache(&result);
            Ok(result)
        })
        .await
        .context("PostgreSQL admin mutation worker stopped")?
    }

    /// Readiness proves the read pool answers and the original writer backend
    /// still holds its advisory lock. The schema was verified at `open` and
    /// cannot change under a running binary, so it is not re-checked here.
    ///
    /// A probe must never queue behind application writes or stop the
    /// solver: a busy writer is reported healthy (every failed write already
    /// re-verifies ownership), and a failed probe only returns an error.
    pub async fn readiness_check(&self) -> Result<()> {
        if self.ownership_lost.load(Ordering::Acquire) || self.fatal_db.is_cancelled() {
            bail!("PostgreSQL writer is no longer safe; restart the whole solver");
        }
        self.read(|conn| {
            diesel::sql_query("SELECT 1").execute(conn)?;
            Ok(())
        })
        .await
        .context("PostgreSQL read pool is unavailable")?;
        let Ok(mut writer) = self.writer.clone().try_lock_owned() else {
            return Ok(());
        };
        let expected_pid = self.writer_backend_pid;
        let lock_key = self.lock_key;
        let health = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || writer_health(&mut writer, lock_key)),
        )
        .await
        .context("PostgreSQL writer ownership probe timed out")?
        .context("PostgreSQL writer ownership probe stopped")?
        .context("query PostgreSQL writer ownership")?;
        if health.backend_pid != expected_pid || !health.owns_advisory_lock {
            self.ownership_lost.store(true, Ordering::Release);
            self.fatal_db.cancel();
            bail!("PostgreSQL writer lost its original ownership session or advisory lock");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_migrations;
    use crate::db::postgres_schema::sync_state;
    use miden_protocol::crypto::utils::{Deserializable, SliceReader};
    use std::sync::atomic::AtomicI64;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

    struct PoolFixture {
        admin: PgConnection,
        name: String,
        url: String,
    }

    impl PoolFixture {
        fn new() -> Result<Self> {
            let base_url = std::env::var("SOLVER_TEST_DATABASE_URL")?;
            let mut admin = postgres_migrations::connect(&base_url)?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!("solver_pool_{}_{}_{}", std::process::id(), nonce, sequence);
            admin.batch_execute(&format!("CREATE SCHEMA {name}"))?;
            let separator = if base_url.contains('?') { '&' } else { '?' };
            let url = format!("{base_url}{separator}options=-csearch_path%3D{name}");
            let fixture = Self { admin, name, url };
            let mut schema_conn = postgres_migrations::connect(&fixture.url)?;
            postgres_migrations::migrate(&mut schema_conn)?;
            Ok(fixture)
        }
    }

    impl Drop for PoolFixture {
        fn drop(&mut self) {
            let _ = self
                .admin
                .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.name));
        }
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn ownership_and_post_commit_publication() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let url = fixture.url.clone();

        let pool = PgPool::open(url.clone(), url.clone(), 2, "solver/test".into()).await?;
        pool.readiness_check().await?;
        match PgPool::open(url.clone(), url.clone(), 1, "solver/second".into()).await {
            Ok(_) => bail!("a second solver acquired the application ownership lock"),
            Err(error) => assert!(error.to_string().contains("another solver already owns")),
        }
        let other_schema = PoolFixture::new()?;
        let independent = PgPool::open(
            other_schema.url.clone(),
            other_schema.url.clone(),
            1,
            "solver/independent".into(),
        )
        .await?;
        independent.readiness_check().await?;

        pool.write(|conn| {
            diesel::update(sync_state::table.find(1_i16))
                .set(sync_state::last_fetched_block.eq(1_i64))
                .execute(conn)?;
            Ok(())
        })
        .await?;
        let initial = pool
            .read(|conn| {
                Ok(sync_state::table
                    .find(1_i16)
                    .select(sync_state::last_fetched_block)
                    .first::<i64>(conn)?)
            })
            .await?;
        assert_eq!(initial, 1);

        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(BookUpdate::default()).await?;
        let order_id = crate::types::OrderId::read_from(&mut SliceReader::new(&[0_u8; 32]))?;
        let pending_pool = pool.clone();
        let publication = tokio::spawn(async move {
            pending_pool
                .write_book(&sender, move |conn| {
                    diesel::update(sync_state::table.find(1_i16))
                        .set(sync_state::last_fetched_block.eq(2_i64))
                        .execute(conn)?;
                    Ok(BookUpdate {
                        removed: vec![order_id],
                        active: Vec::new(),
                    })
                })
                .await
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let value = pool
                .read(|conn| {
                    Ok(sync_state::table
                        .find(1_i16)
                        .select(sync_state::last_fetched_block)
                        .first::<i64>(conn)?)
                })
                .await?;
            if value == 2 {
                break;
            }
            if Instant::now() >= deadline {
                bail!("first write did not commit before its channel send");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !publication.is_finished(),
            "channel send should still be waiting"
        );

        // A full matcher channel cannot hold the database writer after commit.
        tokio::time::timeout(
            Duration::from_secs(2),
            pool.write(|conn| {
                diesel::update(sync_state::table.find(1_i16))
                    .set(sync_state::last_fetched_block.eq(3_i64))
                    .execute(conn)?;
                Ok(())
            }),
        )
        .await??;
        receiver.recv().await.context("missing filler update")?;
        publication.await??;
        let sent = receiver.recv().await.context("missing committed update")?;
        assert_eq!(sent.removed.len(), 1);

        let cache = Arc::new(AtomicI64::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let first_pool = pool.clone();
        let first_cache = cache.clone();
        let first_admin = tokio::spawn(async move {
            first_pool
                .admin_write(
                    move |conn| {
                        let _ = started_tx.send(());
                        std::thread::sleep(Duration::from_millis(80));
                        diesel::update(sync_state::table.find(1_i16))
                            .set(sync_state::last_fetched_block.eq(4_i64))
                            .execute(conn)?;
                        Ok(4_i64)
                    },
                    move |value| first_cache.store(*value, Ordering::Release),
                )
                .await
        });
        started_rx.await?;
        first_admin.abort();
        let second_cache = cache.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            pool.admin_write(
                |conn| {
                    diesel::update(sync_state::table.find(1_i16))
                        .set(sync_state::last_fetched_block.eq(5_i64))
                        .execute(conn)?;
                    Ok(5_i64)
                },
                move |value| second_cache.store(*value, Ordering::Release),
            ),
        )
        .await??;
        assert_eq!(cache.load(Ordering::Acquire), 5);

        let lock_key = pool.lock_key;
        pool.write(move |conn| {
            let unlocked = diesel::sql_query("SELECT pg_advisory_unlock($1) AS acquired")
                .bind::<BigInt, _>(lock_key)
                .get_result::<LockResult>(conn)?;
            assert!(unlocked.acquired);
            Ok(())
        })
        .await?;
        assert!(pool.readiness_check().await.is_err());
        assert!(pool.write(|_| Ok(())).await.is_err());

        drop(receiver);
        drop(pool);
        PgPool::open(url.clone(), url, 1, "solver/restarted".into()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn book_updates_arrive_in_commit_order() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let url = fixture.url.clone();
        let pool = PgPool::open(url.clone(), url, 1, "solver/publish-order".into()).await?;
        let first_id = crate::types::OrderId::read_from(&mut SliceReader::new(&[1_u8; 32]))?;
        let second_id = crate::types::OrderId::read_from(&mut SliceReader::new(&[2_u8; 32]))?;

        // A full channel parks the first publisher after its commit.
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(BookUpdate::default()).await?;
        let first_pool = pool.clone();
        let first_sender = sender.clone();
        let first = tokio::spawn(async move {
            first_pool
                .write_book(&first_sender, move |conn| {
                    diesel::update(sync_state::table.find(1_i16))
                        .set(sync_state::last_fetched_block.eq(1_i64))
                        .execute(conn)?;
                    Ok(BookUpdate {
                        removed: vec![first_id],
                        active: Vec::new(),
                    })
                })
                .await
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.telemetry_snapshot().write_total == 0 {
            if Instant::now() >= deadline {
                bail!("first publisher never committed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The second publisher cannot commit and overtake the first send.
        let second_pool = pool.clone();
        let second_sender = sender.clone();
        let second = tokio::spawn(async move {
            second_pool
                .write_book(&second_sender, move |conn| {
                    diesel::update(sync_state::table.find(1_i16))
                        .set(sync_state::last_fetched_block.eq(2_i64))
                        .execute(conn)?;
                    Ok(BookUpdate {
                        removed: vec![second_id],
                        active: Vec::new(),
                    })
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            pool.telemetry_snapshot().write_total,
            1,
            "second publisher must wait for the first to publish"
        );

        receiver.recv().await.context("missing filler update")?;
        let delivered = receiver.recv().await.context("missing first update")?;
        assert_eq!(delivered.removed, vec![first_id]);
        let delivered = receiver.recv().await.context("missing second update")?;
        assert_eq!(delivered.removed, vec![second_id]);
        first.await??;
        second.await??;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn closed_matcher_after_commit_leaves_state_for_restart() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let url = fixture.url.clone();
        let pool = PgPool::open(url.clone(), url.clone(), 1, "solver/first".into()).await?;
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let order_id = crate::types::OrderId::read_from(&mut SliceReader::new(&[0_u8; 32]))?;
        let error = pool
            .write_book(&sender, move |conn| {
                diesel::update(sync_state::table.find(1_i16))
                    .set(sync_state::last_fetched_block.eq(17_i64))
                    .execute(conn)?;
                Ok(BookUpdate {
                    removed: vec![order_id],
                    active: Vec::new(),
                })
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("matcher stopped"));
        drop(pool);

        let restarted = PgPool::open(url.clone(), url, 1, "solver/restarted".into()).await?;
        let cursor = restarted
            .read(|conn| {
                Ok(sync_state::table
                    .find(1_i16)
                    .select(sync_state::last_fetched_block)
                    .first::<i64>(conn)?)
            })
            .await?;
        assert_eq!(cursor, 17);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn terminated_writer_session_fails_closed() -> Result<()> {
        let mut fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/terminated-writer".into(),
        )
        .await?;
        let pid = pool.writer_backend_pid;
        #[derive(QueryableByName)]
        struct Termination {
            #[diesel(sql_type = Bool)]
            terminated: bool,
        }
        let terminated = diesel::sql_query("SELECT pg_terminate_backend($1) AS terminated")
            .bind::<Integer, _>(pid)
            .get_result::<Termination>(&mut fixture.admin)?;
        assert!(terminated.terminated);
        assert!(pool.write(|_| Ok(())).await.is_err());
        let second = pool.write(|_| Ok(())).await.unwrap_err();
        assert!(second.to_string().contains("no longer safe"));
        assert!(pool.fatal_token().is_cancelled());
        assert!(pool.readiness_check().await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn reader_must_target_the_writer_database_and_schema() -> Result<()> {
        let writer = PoolFixture::new()?;
        let reader = PoolFixture::new()?;
        let error = match PgPool::open(
            writer.url.clone(),
            reader.url.clone(),
            1,
            "solver/wrong-reader".into(),
        )
        .await
        {
            Ok(_) => bail!("reader in another schema was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("reader target differs"));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn exhausted_read_pool_returns_a_bounded_error() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/read-exhaustion".into(),
        )
        .await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let occupied_pool = pool.clone();
        let occupied = tokio::spawn(async move {
            occupied_pool
                .read(move |_| {
                    let _ = started_tx.send(());
                    std::thread::sleep(Duration::from_secs(6));
                    Ok(())
                })
                .await
        });
        started_rx.await?;
        let error = pool.read(|_| Ok(())).await.unwrap_err();
        assert!(error.to_string().contains("read pool wait exceeded"));
        occupied.await??;
        assert_eq!(pool.telemetry_snapshot().read_errors, 1);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn lost_write_reply_fails_closed_but_committed_state_is_recoverable() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let mut pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/lost-write-reply".into(),
        )
        .await?;
        pool.operation_deadline = Duration::from_millis(100);
        let error = pool
            .write(|conn| {
                diesel::update(sync_state::table.find(1_i16))
                    .set(sync_state::last_fetched_block.eq(42_i64))
                    .execute(conn)?;
                std::thread::sleep(Duration::from_millis(300));
                Ok(())
            })
            .await
            .expect_err("the client must stop waiting for the write reply");
        assert!(error.to_string().contains("commit outcome is uncertain"));
        assert!(pool.fatal_token().is_cancelled());
        assert!(pool.write(|_| Ok(())).await.is_err());

        // The blocking operation is deliberately allowed to finish. A fresh
        // solver must read durable state instead of assuming the timeout
        // rolled the transaction back.
        tokio::time::sleep(Duration::from_millis(350)).await;
        let url = fixture.url.clone();
        let stored = tokio::task::spawn_blocking(move || -> Result<i64> {
            let mut conn = postgres_migrations::connect(&url)?;
            Ok(sync_state::table
                .find(1_i16)
                .select(sync_state::last_fetched_block)
                .first(&mut conn)?)
        })
        .await??;
        assert_eq!(stored, 42);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn lost_read_reply_is_not_fatal() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let mut pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            2,
            "solver/lost-read-reply".into(),
        )
        .await?;
        pool.operation_deadline = Duration::from_millis(100);
        let error = pool
            .read(|_| {
                std::thread::sleep(Duration::from_millis(300));
                Ok(())
            })
            .await
            .expect_err("the client must stop waiting for the read reply");
        assert!(error.to_string().contains("read exceeded"));
        assert!(!pool.fatal_token().is_cancelled());
        pool.readiness_check().await?;
        pool.write(|_| Ok(())).await?;
        tokio::time::sleep(Duration::from_millis(350)).await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn readiness_does_not_wait_for_a_busy_writer() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/busy-writer".into(),
        )
        .await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let busy_pool = pool.clone();
        let busy = tokio::spawn(async move {
            busy_pool
                .write(move |_| {
                    let _ = started_tx.send(());
                    std::thread::sleep(Duration::from_millis(500));
                    Ok(())
                })
                .await
        });
        started_rx.await?;
        tokio::time::timeout(Duration::from_millis(200), pool.readiness_check()).await??;
        busy.await??;
        assert!(!pool.fatal_token().is_cancelled());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn row_lock_timeout_is_not_fatal() -> Result<()> {
        let mut fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/lock-timeout".into(),
        )
        .await?;
        fixture.admin.batch_execute(&format!(
            "SET search_path TO {}; BEGIN; SELECT id FROM sync_state WHERE id = 1 FOR UPDATE",
            fixture.name
        ))?;
        let blocked = pool
            .write(|conn| {
                diesel::update(sync_state::table.find(1_i16))
                    .set(sync_state::last_fetched_block.eq(7_i64))
                    .execute(conn)?;
                Ok(())
            })
            .await;
        fixture.admin.batch_execute("ROLLBACK")?;
        let error = blocked.expect_err("the writer must not wait indefinitely on a row lock");
        assert!(error.to_string().contains("lock timeout"));
        assert_eq!(pool.telemetry_snapshot().lock_timeouts, 1);
        // One rolled-back transaction on a healthy session is the caller's
        // problem, not the solver's.
        assert!(!pool.fatal_token().is_cancelled());
        pool.readiness_check().await?;
        pool.write(|conn| {
            diesel::update(sync_state::table.find(1_i16))
                .set(sync_state::last_fetched_block.eq(8_i64))
                .execute(conn)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn statement_timeout_is_not_fatal() -> Result<()> {
        let fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/statement-timeout".into(),
        )
        .await?;
        let error = pool
            .write(|conn| {
                conn.batch_execute("SET LOCAL statement_timeout = '50ms'; SELECT pg_sleep(0.2)")?;
                Ok(())
            })
            .await
            .expect_err("a timed-out statement must fail the transaction");
        assert!(error.to_string().contains("statement timeout"));
        assert_eq!(pool.telemetry_snapshot().statement_timeouts, 1);
        assert!(!pool.fatal_token().is_cancelled());
        pool.write(|_| Ok(())).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn writer_terminated_during_transaction_fails_closed() -> Result<()> {
        let mut fixture = PoolFixture::new()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/mid-transaction-termination".into(),
        )
        .await?;
        let pid = pool.writer_backend_pid;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let blocked_pool = pool.clone();
        let blocked = tokio::spawn(async move {
            blocked_pool
                .write(move |conn| {
                    let _ = started_tx.send(());
                    conn.batch_execute("SELECT pg_sleep(5)")?;
                    Ok(())
                })
                .await
        });
        started_rx.await?;
        #[derive(QueryableByName)]
        struct Termination {
            #[diesel(sql_type = Bool)]
            terminated: bool,
        }
        let terminated = diesel::sql_query("SELECT pg_terminate_backend($1) AS terminated")
            .bind::<Integer, _>(pid)
            .get_result::<Termination>(&mut fixture.admin)?;
        assert!(terminated.terminated);
        assert!(blocked.await?.is_err());
        assert!(pool.fatal_token().is_cancelled());
        assert!(pool.ownership_lost.load(Ordering::Acquire));
        assert!(pool.write(|_| Ok(())).await.is_err());
        Ok(())
    }
}

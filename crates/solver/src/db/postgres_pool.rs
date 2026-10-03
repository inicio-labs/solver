//! PostgreSQL connection ownership and the blocking boundary for Diesel.
//!
//! The writer is one physical session for this pool's entire lifetime. Its
//! session advisory lock protects the shared application database, while a
//! Tokio mutex serializes individual transactions. Read concurrency is bounded
//! before a blocking worker is spawned.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::connection::{AnsiTransactionManager, SimpleConnection, TransactionManager};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::error::{DbError, DbResult};
use super::postgres_migrations;
use super::postgres_schema::sync_state;
use crate::types::BookUpdate;

/// "SOLVERV1" as a positive signed 64-bit PostgreSQL advisory-lock key.
/// Ownership is scoped by the database, not by the configured solver account.
pub const APPLICATION_LOCK_KEY: i64 = 0x534F_4C56_4552_5631;

/// Upper bounds of the latency histogram buckets; a last `+Inf` bucket
/// counts every sample.
pub const LATENCY_BUCKET_US: [u64; 8] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000,
];
// A server-side statement timeout does not bound a client waiting for a lost
// network reply. A timed-out blocking operation keeps its permit while the
// coordinated solver shuts down for a supervised restart.
const DEFAULT_OPERATION_DEADLINE: Duration = Duration::from_secs(30);
/// How long a read waits for a free read slot before reporting the pool busy.
const READ_SLOT_WAIT: Duration = Duration::from_secs(5);
/// How long a new read connection may take to open.
const READ_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on the `/readyz` writer-ownership probe.
const READINESS_PROBE_DEADLINE: Duration = Duration::from_secs(5);
/// Longest pause between writer reconnect attempts.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
/// A cumulative latency histogram (Prometheus style).
#[derive(Default)]
struct Latency {
    sum_us: AtomicU64,
    buckets: [AtomicU64; 9],
}

#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub sum_us: u64,
    /// Cumulative counts per `LATENCY_BUCKET_US` bound, then `+Inf`.
    pub buckets: [u64; 9],
}

impl Latency {
    /// Record the time since `started`; returns it in microseconds.
    fn record(&self, started: Instant) -> u64 {
        let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        for (bucket, bound) in self.buckets.iter().zip(LATENCY_BUCKET_US) {
            if us <= bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.buckets[8].fetch_add(1, Ordering::Relaxed);
        us
    }

    fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            sum_us: self.sum_us.load(Ordering::Relaxed),
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
        }
    }
}

#[derive(Default)]
struct PoolTelemetry {
    read_total: AtomicU64,
    read_errors: AtomicU64,
    read_wait: Latency,
    read_duration: Latency,
    write_total: AtomicU64,
    write_errors: AtomicU64,
    writer_wait: Latency,
    write_duration: Latency,
    lock_timeouts: AtomicU64,
    statement_timeouts: AtomicU64,
    deadlocks: AtomicU64,
    writer_reconnects: AtomicU64,
}

/// Monotonic totals and current pool occupancy for an external metrics scraper.
#[derive(Debug, Clone, Copy)]
pub struct PoolTelemetrySnapshot {
    pub read_total: u64,
    pub read_errors: u64,
    pub read_wait: LatencySnapshot,
    pub read_duration: LatencySnapshot,
    pub write_total: u64,
    pub write_errors: u64,
    pub writer_wait: LatencySnapshot,
    pub write_duration: LatencySnapshot,
    pub lock_timeouts: u64,
    pub statement_timeouts: u64,
    pub deadlocks: u64,
    pub writer_reconnects: u64,
    pub read_connections: u32,
    pub read_idle_connections: u32,
    pub writer_busy: bool,
    pub fatal_shutdown_requested: bool,
}

/// Run blocking database work on a worker thread with a client-side deadline.
/// After the deadline the worker keeps running until libpq returns.
async fn blocking<T, F>(operation: &'static str, deadline: Duration, work: F) -> DbResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> DbResult<T> + Send + 'static,
{
    match tokio::time::timeout(deadline, tokio::task::spawn_blocking(work)).await {
        Ok(Ok(result)) => result,
        Ok(Err(panicked)) => Err(DbError::WorkerStopped(panicked)),
        Err(_) => Err(DbError::Deadline {
            operation,
            deadline,
        }),
    }
}

#[derive(Debug, Clone, Copy)]
enum Access {
    Read,
    Write,
}

/// Telemetry class of a database error, logged as its snake_case name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum ErrorClass {
    Connection,
    Deadlock,
    LockTimeout,
    StatementTimeout,
    Database,
    NotFound,
    Diesel,
    Other,
}

impl ErrorClass {
    /// Message matching relies on `lc_messages = 'C'` from
    /// `configure_session`; Diesel has no error kind for these SQLSTATEs.
    fn of(error: &DbError) -> Self {
        match error {
            DbError::Connect(_) | DbError::ReadPool(_) => Self::Connection,
            DbError::Query(DieselError::DatabaseError(DatabaseErrorKind::ClosedConnection, _)) => {
                Self::Connection
            }
            DbError::Query(DieselError::DatabaseError(_, info)) => {
                let message = info.message();
                if message.contains("deadlock detected") {
                    Self::Deadlock
                } else if message.contains("lock timeout") {
                    Self::LockTimeout
                } else if message.contains("statement timeout") {
                    Self::StatementTimeout
                } else {
                    Self::Database
                }
            }
            DbError::Query(DieselError::NotFound) => Self::NotFound,
            DbError::Query(_) => Self::Diesel,
            _ => Self::Other,
        }
    }
}

impl PoolTelemetry {
    /// Count a failed operation; returns its class for the log line.
    fn record_error(&self, errors: &AtomicU64, error: &DbError) -> &'static str {
        errors.fetch_add(1, Ordering::Relaxed);
        let class = ErrorClass::of(error);
        let counter = match class {
            ErrorClass::LockTimeout => &self.lock_timeouts,
            ErrorClass::StatementTimeout => &self.statement_timeouts,
            ErrorClass::Deadlock => &self.deadlocks,
            _ => return class.into(),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        class.into()
    }
}

/// A server backend, identified by pid and start time: a pid alone can be
/// reused by another session after the database restarts.
#[derive(QueryableByName, Clone, Debug, PartialEq, Eq)]
struct Backend {
    #[diesel(sql_type = Integer)]
    pid: i32,
    #[diesel(sql_type = Nullable<Text>)]
    started: Option<String>,
}

#[derive(QueryableByName)]
struct LockResult {
    #[diesel(sql_type = Bool)]
    acquired: bool,
    #[diesel(sql_type = Integer)]
    backend_pid: i32,
    #[diesel(sql_type = Nullable<Text>)]
    backend_start: Option<String>,
}

/// Try the session advisory lock. `Some(this backend)` when this session now
/// holds it; a granted session-level lock needs no further check.
fn try_lock(conn: &mut PgConnection, lock_key: i64) -> diesel::QueryResult<Option<Backend>> {
    let lock = diesel::sql_query(
        "SELECT pg_try_advisory_lock($1) AS acquired, pg_backend_pid() AS backend_pid,
                (SELECT backend_start::text FROM pg_stat_activity
                 WHERE pid = pg_backend_pid()) AS backend_start",
    )
    .bind::<BigInt, _>(lock_key)
    .get_result::<LockResult>(conn)?;
    Ok(lock.acquired.then_some(Backend {
        pid: lock.backend_pid,
        started: lock.backend_start,
    }))
}

/// `pg_locks` shows a bigint advisory key as two 32-bit halves.
fn lock_key_parts(lock_key: i64) -> (i64, i64) {
    let key = lock_key as u64;
    ((key >> 32) as i64, (key & 0xffff_ffff) as i64)
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

fn database_identity(conn: &mut PgConnection) -> diesel::QueryResult<DatabaseIdentity> {
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
struct CurrentSchema {
    #[diesel(sql_type = Nullable<Text>)]
    schema: Option<String>,
}

/// Run schema maintenance while holding the solver's ownership lock for the
/// schema this session migrates, so it cannot change tables under a running
/// solver and no solver can start until it finishes.
pub(crate) fn with_ownership_lock<T>(
    conn: &mut PgConnection,
    maintenance: impl FnOnce(&mut PgConnection) -> DbResult<T>,
) -> DbResult<T> {
    let schema = diesel::sql_query("SELECT current_schema()::text AS schema")
        .get_result::<CurrentSchema>(conn)?
        .schema
        .unwrap_or_else(|| "public".into());
    let lock_key = schema_lock_key(&schema);
    if try_lock(conn, lock_key)?.is_none() {
        return Err(DbError::AlreadyOwned);
    }
    let result = maintenance(conn);
    diesel::sql_query("SELECT pg_advisory_unlock($1)")
        .bind::<BigInt, _>(lock_key)
        .execute(conn)?;
    result
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
    .bind::<BigInt, _>(lock_key_parts(lock_key).0)
    .bind::<BigInt, _>(lock_key_parts(lock_key).1)
    .get_result(conn)
}

#[derive(QueryableByName)]
struct OptionalText {
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

#[derive(QueryableByName)]
struct Epoch {
    #[diesel(sql_type = BigInt)]
    owner_epoch: i64,
}

/// The ID of the current transaction, if it has written anything. Read just
/// before COMMIT so a lost commit reply can be resolved afterwards.
fn current_transaction_id(conn: &mut PgConnection) -> diesel::QueryResult<Option<String>> {
    Ok(
        diesel::sql_query("SELECT pg_current_xact_id_if_assigned()::text AS value")
            .get_result::<OptionalText>(conn)?
            .value,
    )
}

/// Claim the next ownership epoch. Called once, right after the advisory
/// lock is first acquired and before any worker starts.
fn claim_owner_epoch(conn: &mut PgConnection) -> diesel::QueryResult<i64> {
    Ok(diesel::sql_query(
        "UPDATE sync_state SET owner_epoch = owner_epoch + 1 WHERE id = 1
         RETURNING owner_epoch",
    )
    .get_result::<Epoch>(conn)?
    .owner_epoch)
}

/// Where a write transaction failed, which decides whether it may have
/// committed and whether the solver can continue.
enum WriteFailure<T> {
    /// Rolled back on a live session that still owns the database.
    Failed(DbError),
    /// The session is alive but no longer holds the ownership lock.
    OwnershipLost,
    /// The session was lost before COMMIT was sent: nothing was committed.
    LostBeforeCommit(DbError),
    /// The session was lost while committing. `xid` decides the outcome; a
    /// transaction that wrote nothing has none and committed nothing.
    LostAtCommit {
        error: DbError,
        value: T,
        xid: Option<String>,
    },
}

/// Run `operation` in one transaction as explicit steps — begin, operate,
/// record the transaction ID, commit — so a failure can be attributed to the
/// step it happened in.
fn run_write_transaction<T>(
    conn: &mut PgConnection,
    operation: impl FnOnce(&mut PgConnection) -> DbResult<T>,
    lock_key: i64,
    expected_pid: i32,
) -> Result<T, WriteFailure<T>> {
    let staged = AnsiTransactionManager::begin_transaction(conn)
        .map_err(DbError::from)
        .and_then(|()| {
            let value = operation(conn)?;
            let xid = current_transaction_id(conn)?;
            Ok((value, xid))
        });
    let (error, committing) = match staged {
        Ok((value, xid)) => match AnsiTransactionManager::commit_transaction(conn) {
            Ok(()) => return Ok(value),
            Err(error) => (DbError::from(error), Some((value, xid))),
        },
        Err(error) => {
            let _ = AnsiTransactionManager::rollback_transaction(conn);
            (error, None)
        }
    };
    match writer_health(conn, lock_key) {
        Ok(health) if health.backend_pid == expected_pid && health.owns_advisory_lock => {
            Err(WriteFailure::Failed(error))
        }
        Ok(_) => Err(WriteFailure::OwnershipLost),
        Err(_) => Err(match committing {
            Some((value, xid)) => WriteFailure::LostAtCommit { error, value, xid },
            None => WriteFailure::LostBeforeCommit(error),
        }),
    }
}

/// A reconnect attempt either may succeed later (`Retry`) or must never be
/// retried because another solver is or was the owner (`Stop`).
enum Reconnect {
    Retry(DbError),
    Stop(DbError),
}

impl<E: Into<DbError>> From<E> for Reconnect {
    fn from(error: E) -> Self {
        Self::Retry(error.into())
    }
}

/// One attempt to replace a lost writer session: connect, re-take the
/// advisory lock, and confirm the ownership epoch is still ours.
fn reconnect_once(
    config: &WriterConfig,
    previous: &Backend,
) -> Result<(PgConnection, Backend), Reconnect> {
    let lock_key = config.lock_key;
    let mut conn = postgres_migrations::connect(&config.url)?;
    configure_session(&mut conn, &config.application_name)?;
    let Some(backend) = try_lock(&mut conn, lock_key)? else {
        // After a network cut our previous backend can outlive the client
        // and keep the lock until it notices; that one is safe to end. It is
        // matched by pid and start time, never by a reused pid alone.
        let holder = diesel::sql_query(
            "SELECT l.pid, a.backend_start::text AS started
             FROM pg_locks AS l LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
             WHERE l.locktype = 'advisory' AND l.granted
               AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND l.classid::bigint = $1 AND l.objid::bigint = $2 AND l.objsubid = 1
             LIMIT 1",
        )
        .bind::<BigInt, _>(lock_key_parts(lock_key).0)
        .bind::<BigInt, _>(lock_key_parts(lock_key).1)
        .get_result::<Backend>(&mut conn)
        .optional()?;
        return Err(match holder {
            Some(holder) if holder == *previous && holder.started.is_some() => {
                diesel::sql_query("SELECT pg_terminate_backend($1)")
                    .bind::<Integer, _>(holder.pid)
                    .execute(&mut conn)?;
                Reconnect::Retry(DbError::OwnershipLost)
            }
            Some(_) => Reconnect::Stop(DbError::LockTakenAfterDisconnect),
            None => Reconnect::Retry(DbError::OwnershipLost),
        });
    };
    let current: i64 = sync_state::table
        .find(1_i16)
        .select(sync_state::owner_epoch)
        .first(&mut conn)?;
    if current != config.owner_epoch {
        return Err(Reconnect::Stop(DbError::OwnerEpochMoved {
            ours: config.owner_epoch,
            current,
        }));
    }
    Ok((conn, backend))
}

/// Whether the transaction a lost session was committing took effect. Only
/// called once the ownership lock is re-held, so that session has ended and
/// its transaction is final.
fn transaction_committed(conn: &mut PgConnection, xid: &str) -> Result<bool, Reconnect> {
    let status = diesel::sql_query("SELECT pg_xact_status($1::xid8)::text AS value")
        .bind::<Text, _>(xid)
        .get_result::<OptionalText>(conn)?
        .value;
    match status.as_deref() {
        Some("committed") => Ok(true),
        Some("aborted") => Ok(false),
        _ => Err(Reconnect::Stop(DbError::CommitOutcomeUnknown {
            xid: xid.to_owned(),
            status,
        })),
    }
}

/// Replace the lost writer session in place and, if a commit was in flight,
/// learn its outcome. Retries until the database answers: a restart or
/// failover pauses writes instead of stopping the solver. Only a condition
/// that makes continuing unsafe (another owner, unknown commit outcome)
/// returns an error.
fn recover_writer(
    writer: &mut Writer,
    config: &WriterConfig,
    xid: Option<&str>,
) -> DbResult<Option<bool>> {
    let mut backoff = Duration::from_millis(100);
    for tries in 1_u64.. {
        let attempt = reconnect_once(config, &writer.backend).and_then(|(mut fresh, backend)| {
            let committed = xid
                .map(|xid| transaction_committed(&mut fresh, xid))
                .transpose()?;
            Ok((fresh, backend, committed))
        });
        match attempt {
            Ok((fresh, backend, committed)) => {
                writer.conn = fresh;
                writer.backend = backend;
                return Ok(committed);
            }
            Err(Reconnect::Stop(error)) => return Err(error),
            Err(Reconnect::Retry(error)) => {
                tracing::warn!(%error, tries, "PostgreSQL writer reconnect failed; writes wait, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
            }
        }
    }
    unreachable!("the reconnect loop only exits by returning")
}

/// What a lost writer session needs to come back as the same owner.
struct WriterConfig {
    url: String,
    application_name: String,
    lock_key: i64,
    owner_epoch: i64,
}

// `idle_session_timeout = 0` overrides any server or role default: the writer
// session holds the ownership lock and must stay open however long it idles.
fn configure_session(conn: &mut PgConnection, application_name: &str) -> diesel::QueryResult<()> {
    conn.batch_execute(
        "SET default_transaction_isolation = 'read committed';
         SET statement_timeout = '10s';
         SET lock_timeout = '2s';
         SET idle_in_transaction_session_timeout = '10s';
         SET idle_session_timeout = 0",
    )?;
    // Server-side TCP keepalives on our own sessions: once a client vanishes
    // (network cut, crash, killed host), the server ends its backend within
    // about 25 s, releasing the writer's ownership lock, so a restarted solver
    // can start instead of finding the lock held by a session nobody owns.
    // `idle_session_timeout = 0` keeps a live idle writer; these end only a
    // dead one. Ordinary roles may set them; unsupported platforms only warn.
    // `tcp_user_timeout` (Linux servers) also covers a backend blocked
    // sending to a dead client, where keepalive probes do not run.
    for setting in [
        "SET tcp_keepalives_idle = 10; SET tcp_keepalives_interval = 5; SET tcp_keepalives_count = 3",
        "SET tcp_user_timeout = 25000",
    ] {
        if let Err(error) = conn.batch_execute(setting) {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!(%error, setting, "cannot set server TCP keepalives; a dead writer session may hold the ownership lock until the server's own keepalive ends it")
            });
        }
    }
    // English messages let `ErrorClass` tell lock, statement and deadlock
    // timeouts apart in metrics. Only a superuser, or a role granted
    // `SET ON PARAMETER lc_messages` (PostgreSQL 15+), may set it; without
    // that grant the solver runs normally and counts them as `database`.
    if let Err(error) = conn.batch_execute("SET lc_messages = 'C'") {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(%error, "cannot set lc_messages; timeout metrics will be unclassified")
        });
    }
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

/// The single writer session and its backend pid. Both change together, only
/// on reconnect, and only under the writer mutex.
struct Writer {
    conn: PgConnection,
    backend: Backend,
}

#[derive(Clone)]
pub struct PgPool {
    writer: Arc<Mutex<Writer>>,
    writer_config: Arc<WriterConfig>,
    /// Cancelled once the writer is unsafe; requests a whole-solver stop.
    fatal_db: CancellationToken,
    operation_deadline: Duration,
    publish_order: Arc<Mutex<()>>,
    readers: r2d2::Pool<ConnectionManager<PgConnection>>,
    read_slots: Arc<Semaphore>,
    /// Caps public API reads below `read_slots`, so wallet traffic always
    /// leaves at least one read connection for the solver pipeline.
    public_slots: Arc<Semaphore>,
    telemetry: Arc<PoolTelemetry>,
}

impl PgPool {
    /// Open and verify the sole writer before any solver worker starts, take
    /// the ownership lock, and claim a new owner epoch. A writer session lost
    /// later is replaced only after re-taking the lock under the same epoch.
    pub async fn open(
        writer_url: String,
        reader_url: String,
        read_pool_size: u32,
        application_name: String,
    ) -> DbResult<Self> {
        if read_pool_size == 0 {
            return Err(DbError::InvalidReadPoolSize);
        }
        let (writer, writer_backend, readers, writer_config) =
            blocking("startup", DEFAULT_OPERATION_DEADLINE, move || {
                let mut writer = postgres_migrations::connect(&writer_url)?;
                configure_session(&mut writer, &application_name)?;
                postgres_migrations::verify(&mut writer)?;
                let writer_identity = database_identity(&mut writer)?;
                let lock_key = schema_lock_key(&writer_identity.schema_name);
                let writer_backend =
                    try_lock(&mut writer, lock_key)?.ok_or(DbError::AlreadyOwned)?;
                let owner_epoch = claim_owner_epoch(&mut writer)?;

                let readers = r2d2::Pool::builder()
                    .max_size(read_pool_size)
                    .connection_timeout(READ_CONNECT_TIMEOUT)
                    .test_on_check_out(true)
                    .connection_customizer(Box::new(ReadCustomizer {
                        application_name: format!("{application_name}/read"),
                    }))
                    .build(ConnectionManager::<PgConnection>::new(reader_url))?;
                {
                    let mut reader = readers.get()?;
                    postgres_migrations::verify(&mut reader)?;
                    let reader_identity = database_identity(&mut reader)?;
                    if reader_identity != writer_identity {
                        return Err(DbError::ReaderTargetMismatch {
                            writer: format!("{writer_identity:?}"),
                            reader: format!("{reader_identity:?}"),
                        });
                    }
                }
                let writer_config = WriterConfig {
                    url: writer_url,
                    application_name,
                    lock_key,
                    owner_epoch,
                };
                Ok((writer, writer_backend, readers, writer_config))
            })
            .await?;

        Ok(Self {
            writer: Arc::new(Mutex::new(Writer {
                conn: writer,
                backend: writer_backend,
            })),
            writer_config: Arc::new(writer_config),
            fatal_db: CancellationToken::new(),
            operation_deadline: DEFAULT_OPERATION_DEADLINE,
            publish_order: Arc::new(Mutex::new(())),
            readers,
            read_slots: Arc::new(Semaphore::new(read_pool_size as usize)),
            public_slots: Arc::new(Semaphore::new((read_pool_size as usize - 1).max(1))),
            telemetry: Arc::new(PoolTelemetry::default()),
        })
    }

    pub fn telemetry_snapshot(&self) -> PoolTelemetrySnapshot {
        let state = self.readers.state();
        let load = Ordering::Relaxed;
        PoolTelemetrySnapshot {
            read_total: self.telemetry.read_total.load(load),
            read_errors: self.telemetry.read_errors.load(load),
            read_wait: self.telemetry.read_wait.snapshot(),
            read_duration: self.telemetry.read_duration.snapshot(),
            write_total: self.telemetry.write_total.load(load),
            write_errors: self.telemetry.write_errors.load(load),
            writer_wait: self.telemetry.writer_wait.snapshot(),
            write_duration: self.telemetry.write_duration.snapshot(),
            lock_timeouts: self.telemetry.lock_timeouts.load(load),
            statement_timeouts: self.telemetry.statement_timeouts.load(load),
            deadlocks: self.telemetry.deadlocks.load(load),
            writer_reconnects: self.telemetry.writer_reconnects.load(load),
            read_connections: state.connections,
            read_idle_connections: state.idle_connections,
            writer_busy: self.writer.try_lock().is_err(),
            fatal_shutdown_requested: self.fatal_db.is_cancelled(),
        }
    }

    /// A database failure requiring coordinated shutdown and startup hydration.
    pub fn fatal_token(&self) -> CancellationToken {
        self.fatal_db.clone()
    }

    /// A read for the public price API. It waits for a public slot first, so
    /// a traffic burst queues here instead of taking every read connection
    /// from ingest, the executor and `/readyz`.
    pub async fn read_public<T, F>(&self, operation: F) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> DbResult<T> + Send + 'static,
    {
        let _slot = self.read_slot(&self.public_slots).await?;
        self.read(operation).await
    }

    /// A slot from `slots`, or `ReadPoolBusy` after `READ_SLOT_WAIT`. The
    /// semaphores are never closed, so running out of time is the only error.
    async fn read_slot(&self, slots: &Arc<Semaphore>) -> DbResult<OwnedSemaphorePermit> {
        match tokio::time::timeout(READ_SLOT_WAIT, slots.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => {
                self.telemetry.read_errors.fetch_add(1, Ordering::Relaxed);
                Err(DbError::ReadPoolBusy(READ_SLOT_WAIT))
            }
        }
    }

    /// Reads never request a solver shutdown. The read pool holds no
    /// ownership state, so a failed read is reported to its caller and the
    /// pool reconnects on the next checkout. Only the writer can be fatal.
    pub async fn read<T, F>(&self, operation: F) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> DbResult<T> + Send + 'static,
    {
        if self.fatal_db.is_cancelled() {
            return Err(DbError::WriterUnsafe);
        }
        let operation_name = std::any::type_name::<F>();
        self.telemetry.read_total.fetch_add(1, Ordering::Relaxed);
        let waiting_since = Instant::now();
        let permit = self.read_slot(&self.read_slots).await;
        self.telemetry.read_wait.record(waiting_since);
        let permit = permit?;
        let readers = self.readers.clone();
        let started = Instant::now();
        // The blocking worker keeps its permit until libpq returns, so a lost
        // reply costs one read slot until then, not the solver.
        let result = blocking("read", self.operation_deadline, move || {
            let _permit = permit;
            let mut conn = readers.get()?;
            operation(&mut conn)
        })
        .await;
        self.finish(Access::Read, started, operation_name, result)
    }

    /// Own exactly one application write transaction. The operation permit
    /// stays with the blocking worker even if its async caller is cancelled.
    ///
    /// A lost writer session is replaced in place (see `recover`): a write
    /// lost before COMMIT returns an error, one lost during COMMIT returns its
    /// real outcome from `pg_xact_status`. Only a lost ownership lock, another
    /// owner, a worker panic, an unrecoverable session, or a deadline with the
    /// session still busy stop the solver. Lock and statement timeouts roll
    /// back one transaction; the caller re-feeds or retries.
    pub async fn write<T, F>(&self, operation: F) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> DbResult<T> + Send + 'static,
    {
        let operation_name = std::any::type_name::<F>();
        self.telemetry.write_total.fetch_add(1, Ordering::Relaxed);
        if self.writer_unsafe() {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(DbError::WriterUnsafe);
        }
        // No timeout of its own: the holder is a write bounded by the
        // operation deadline, or a reconnect that waits for the database to
        // come back. Either stops the solver itself if it cannot finish safely.
        let waiting_since = Instant::now();
        let writer = tokio::select! {
            writer = self.writer.clone().lock_owned() => Some(writer),
            _ = self.fatal_db.cancelled() => None,
        };
        self.telemetry.writer_wait.record(waiting_since);
        let Some(mut writer) = writer else {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(DbError::WriterUnsafe);
        };
        if self.writer_unsafe() {
            self.telemetry.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(DbError::WriterUnsafe);
        }
        let expected_pid = writer.backend.pid;
        let lock_key = self.writer_config.lock_key;
        let started = Instant::now();
        let attempt = tokio::time::timeout(
            self.operation_deadline,
            tokio::task::spawn_blocking(move || {
                let outcome =
                    run_write_transaction(&mut writer.conn, operation, lock_key, expected_pid);
                (writer, outcome)
            }),
        )
        .await;
        let result = async {
            match attempt {
                Ok(Ok((_, Ok(value)))) => Ok(value),
                Ok(Ok((_, Err(WriteFailure::Failed(error))))) => Err(error),
                Ok(Ok((_, Err(WriteFailure::OwnershipLost)))) => {
                    Err(self.stop(DbError::OwnershipLost))
                }
                Ok(Ok((writer, Err(WriteFailure::LostBeforeCommit(error))))) => self
                    .recover(writer, None)
                    .await
                    .and(Err(DbError::LostBeforeCommit(Box::new(error)))),
                Ok(Ok((writer, Err(WriteFailure::LostAtCommit { error, value, xid })))) => {
                    match self.recover(writer, xid).await? {
                        // A transaction that wrote nothing has no ID and no effect;
                        // its result was computed from a consistent snapshot.
                        Some(true) | None => Ok(value),
                        Some(false) => Err(DbError::CommitDidNotApply(Box::new(error))),
                    }
                }
                Ok(Err(panicked)) => Err(self.stop(DbError::WriterPanicked(panicked))),
                // The blocking worker still holds the session, so it cannot be
                // replaced from here; a whole-solver restart recovers.
                Err(_) => Err(self.stop(DbError::WriteDeadline(self.operation_deadline))),
            }
        }
        .await;
        self.finish(Access::Write, started, operation_name, result)
    }

    /// Shared tail of `read` and `write`: duration, error counters, logs.
    fn finish<T>(
        &self,
        access: Access,
        started: Instant,
        operation: &str,
        result: DbResult<T>,
    ) -> DbResult<T> {
        let telemetry = &self.telemetry;
        let (errors, latency) = match access {
            Access::Read => (&telemetry.read_errors, &telemetry.read_duration),
            Access::Write => (&telemetry.write_errors, &telemetry.write_duration),
        };
        let duration_us = latency.record(started);
        if let Err(error) = &result {
            let class = telemetry.record_error(errors, error);
            match access {
                Access::Read => {
                    tracing::warn!(%error, class, operation, duration_us, "PostgreSQL read failed")
                }
                Access::Write => {
                    tracing::error!(%error, class, operation, duration_us, "PostgreSQL write failed")
                }
            }
        } else if duration_us > 100_000 {
            tracing::warn!(?access, operation, duration_us, "slow PostgreSQL operation");
        }
        result
    }

    fn writer_unsafe(&self) -> bool {
        self.fatal_db.is_cancelled()
    }

    /// Mark the writer unsafe and request a whole-solver stop.
    fn stop(&self, error: DbError) -> DbError {
        self.fatal_db.cancel();
        error
    }

    /// Replace a lost writer session while still holding the writer mutex,
    /// so no other write runs in between. Returns whether the in-flight
    /// transaction `xid` committed. Any failure here stops the solver.
    async fn recover(
        &self,
        mut writer: tokio::sync::OwnedMutexGuard<Writer>,
        xid: Option<String>,
    ) -> DbResult<Option<bool>> {
        let previous_pid = writer.backend.pid;
        tracing::warn!(previous_pid, "PostgreSQL writer session lost; reconnecting");
        let config = self.writer_config.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            recover_writer(&mut writer, &config, xid.as_deref())
                .map(|committed| (writer.backend.pid, committed))
        })
        .await;
        match recovered {
            Ok(Ok((pid, committed))) => {
                self.telemetry
                    .writer_reconnects
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    previous_pid,
                    pid,
                    ?committed,
                    "PostgreSQL writer session reconnected"
                );
                Ok(committed)
            }
            Ok(Err(error)) => Err(self.stop(error)),
            Err(panicked) => Err(self.stop(DbError::WriterPanicked(panicked))),
        }
    }

    /// Commit a book-changing transaction and publish its update to the
    /// matcher in commit order. The matcher applies updates as deltas and
    /// recreates any order named `active`, so a stale activation delivered
    /// after a later removal would put a consumed order back in its book.
    ///
    /// The publish guard spans commit and send. Plain `write` calls do not
    /// take it, so a full matcher channel delays only other publishers, and
    /// the matcher never takes it, so waiting for capacity cannot deadlock.
    pub async fn write_book<F>(
        &self,
        sender: &mpsc::Sender<BookUpdate>,
        operation: F,
    ) -> DbResult<()>
    where
        F: FnOnce(&mut PgConnection) -> DbResult<BookUpdate> + Send + 'static,
    {
        let _publish = self.publish_order.lock().await;
        let update = self.write(operation).await?;
        if !update.is_empty() {
            sender
                .send(update)
                .await
                .map_err(|_| DbError::MatcherStopped)?;
        }
        Ok(())
    }

    /// Readiness proves the read pool answers and the original writer backend
    /// still holds its advisory lock. The schema was verified at `open` and
    /// cannot change under a running binary, so it is not re-checked here.
    ///
    /// A probe must never queue behind application writes or stop the
    /// solver: a busy writer is reported healthy (every failed write already
    /// re-verifies ownership), and a failed probe only returns an error.
    pub async fn readiness_check(&self) -> DbResult<()> {
        // `read` already refuses once the writer is unsafe.
        self.read(|conn| {
            diesel::sql_query("SELECT 1").execute(conn)?;
            Ok(())
        })
        .await?;
        let Ok(mut writer) = self.writer.clone().try_lock_owned() else {
            return Ok(());
        };
        let expected_pid = writer.backend.pid;
        let lock_key = self.writer_config.lock_key;
        // A dead session is replaced by the next write; only a live session
        // without the lock is fatal here.
        let health = blocking("readiness probe", READINESS_PROBE_DEADLINE, move || {
            Ok(writer_health(&mut writer.conn, lock_key)?)
        })
        .await?;
        if health.backend_pid != expected_pid || !health.owns_advisory_lock {
            return Err(self.stop(DbError::OwnershipLost));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres_migrations;
    use crate::db::postgres_schema::sync_state;
    use crate::db::postgres_test::TestSchema;
    use anyhow::{bail, Context, Result};
    use miden_protocol::crypto::utils::{Deserializable, SliceReader};
    use std::time::Instant;

    #[derive(QueryableByName)]
    struct Keepalives {
        #[diesel(sql_type = Text)]
        idle: String,
        #[diesel(sql_type = Text)]
        interval: String,
        #[diesel(sql_type = Text)]
        count: String,
        #[diesel(sql_type = Text)]
        user_timeout: String,
    }

    /// A dead client's writer session must not hold the ownership lock for
    /// hours: the server probes our sessions and ends a vanished one quickly.
    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn solver_sessions_let_the_server_detect_a_dead_client() -> Result<()> {
        let schema = TestSchema::migrated()?;
        let pool = PgPool::open(
            schema.url.clone(),
            schema.url.clone(),
            1,
            "solver/test".into(),
        )
        .await?;
        let settings = pool
            .write(|conn| {
                Ok(diesel::sql_query(
                    // Raw values in base units (seconds; ms for the user
                    // timeout), independent of how the server formats them.
                    "SELECT max(setting) FILTER (WHERE name = 'tcp_keepalives_idle') AS idle,
                            max(setting) FILTER (WHERE name = 'tcp_keepalives_interval') AS interval,
                            max(setting) FILTER (WHERE name = 'tcp_keepalives_count') AS count,
                            max(setting) FILTER (WHERE name = 'tcp_user_timeout') AS user_timeout
                     FROM pg_settings",
                )
                .get_result::<Keepalives>(conn)?)
            })
            .await?;
        // The test database is reached over TCP; a Unix socket reads as 0.
        assert_eq!(settings.idle, "10");
        assert_eq!(settings.interval, "5");
        assert_eq!(settings.count, "3");
        // Linux servers apply it; servers without TCP_USER_TIMEOUT keep 0.
        assert!(
            matches!(settings.user_timeout.as_str(), "25000" | "0"),
            "tcp_user_timeout = {}",
            settings.user_timeout
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    fn migrations_refuse_to_run_while_a_solver_owns_the_schema() -> Result<()> {
        let mut schema = TestSchema::migrated()?;
        let mut solver = postgres_migrations::connect(&schema.url)?;
        let lock_key = schema_lock_key(&schema.name);
        assert!(try_lock(&mut solver, lock_key)?.is_some());
        assert!(matches!(
            postgres_migrations::migrate(&mut schema.conn),
            Err(DbError::AlreadyOwned)
        ));
        drop(solver);
        postgres_migrations::migrate(&mut schema.conn)?;
        // Maintenance releases the lock, so a solver can start afterwards.
        assert!(try_lock(&mut postgres_migrations::connect(&schema.url)?, lock_key)?.is_some());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn ownership_and_post_commit_publication() -> Result<()> {
        let fixture = TestSchema::migrated()?;
        let url = fixture.url.clone();

        let pool = PgPool::open(url.clone(), url.clone(), 2, "solver/test".into()).await?;
        pool.readiness_check().await?;
        match PgPool::open(url.clone(), url.clone(), 1, "solver/second".into()).await {
            Ok(_) => bail!("a second solver acquired the application ownership lock"),
            Err(error) => assert!(matches!(error, DbError::AlreadyOwned), "{error:?}"),
        }
        let other_schema = TestSchema::migrated()?;
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

        let lock_key = pool.writer_config.lock_key;
        pool.write(move |conn| {
            let unlocked = diesel::sql_query(
                "SELECT pg_advisory_unlock($1) AS acquired, pg_backend_pid() AS backend_pid,
                        NULL::text AS backend_start",
            )
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
        let fixture = TestSchema::migrated()?;
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
        let fixture = TestSchema::migrated()?;
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
        assert!(matches!(error, DbError::MatcherStopped), "{error:?}");
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
    async fn reader_must_target_the_writer_database_and_schema() -> Result<()> {
        let writer = TestSchema::migrated()?;
        let reader = TestSchema::migrated()?;
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
        assert!(
            matches!(error, DbError::ReaderTargetMismatch { .. }),
            "{error:?}"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn exhausted_read_pool_returns_a_bounded_error() -> Result<()> {
        let fixture = TestSchema::migrated()?;
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
        assert!(matches!(error, DbError::ReadPoolBusy(_)), "{error:?}");
        occupied.await??;
        assert_eq!(pool.telemetry_snapshot().read_errors, 1);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn lost_write_reply_fails_closed_but_committed_state_is_recoverable() -> Result<()> {
        let fixture = TestSchema::migrated()?;
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
        assert!(matches!(error, DbError::WriteDeadline(_)), "{error:?}");
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
        let fixture = TestSchema::migrated()?;
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
        assert!(
            matches!(
                error,
                DbError::Deadline {
                    operation: "read",
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(!pool.fatal_token().is_cancelled());
        pool.readiness_check().await?;
        pool.write(|_| Ok(())).await?;
        tokio::time::sleep(Duration::from_millis(350)).await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn readiness_does_not_wait_for_a_busy_writer() -> Result<()> {
        let fixture = TestSchema::migrated()?;
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
        let mut fixture = TestSchema::migrated()?;
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
        assert!(matches!(error, DbError::Query(_)), "{error:?}");
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
        let fixture = TestSchema::migrated()?;
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
        assert!(matches!(error, DbError::Query(_)), "{error:?}");
        assert_eq!(pool.telemetry_snapshot().statement_timeouts, 1);
        assert!(!pool.fatal_token().is_cancelled());
        pool.write(|_| Ok(())).await?;
        Ok(())
    }

    #[derive(QueryableByName)]
    struct Flag {
        #[diesel(sql_type = Bool)]
        value: bool,
    }

    /// End a backend from the admin session and wait until it is gone.
    fn terminate(admin: &mut PgConnection, pid: i32) -> Result<()> {
        diesel::sql_query("SELECT pg_terminate_backend($1) AS value")
            .bind::<Integer, _>(pid)
            .get_result::<Flag>(admin)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let alive = diesel::sql_query(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1) AS value",
            )
            .bind::<Integer, _>(pid)
            .get_result::<Flag>(admin)?
            .value;
            if !alive {
                return Ok(());
            }
            anyhow::ensure!(Instant::now() < deadline, "backend {pid} did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn cursor(pool_url: &str) -> Result<i64> {
        let mut conn = postgres_migrations::connect(pool_url)?;
        Ok(sync_state::table
            .find(1_i16)
            .select(sync_state::last_fetched_block)
            .first(&mut conn)?)
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn lost_idle_writer_reconnects_and_keeps_running() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/idle-writer".into(),
        )
        .await?;
        let first_pid = pool.writer.lock().await.backend.pid;
        terminate(&mut fixture.admin, first_pid)?;

        // Nothing was in flight: the write that finds the dead session reports
        // an ordinary error after reconnecting, and the next one succeeds.
        let error = pool.write(|_| Ok(())).await.unwrap_err();
        assert!(matches!(error, DbError::LostBeforeCommit(_)), "{error:?}");
        assert!(!pool.fatal_token().is_cancelled());
        assert_ne!(pool.writer.lock().await.backend.pid, first_pid);
        pool.write(|conn| {
            diesel::update(sync_state::table.find(1_i16))
                .set(sync_state::last_fetched_block.eq(3_i64))
                .execute(conn)?;
            Ok(())
        })
        .await?;
        pool.readiness_check().await?;
        assert_eq!(cursor(&fixture.url)?, 3);
        assert_eq!(pool.telemetry_snapshot().writer_reconnects, 1);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn writer_lost_mid_transaction_writes_nothing_and_recovers() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/mid-transaction".into(),
        )
        .await?;
        let pid = pool.writer.lock().await.backend.pid;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let blocked_pool = pool.clone();
        let blocked = tokio::spawn(async move {
            blocked_pool
                .write(move |conn| {
                    diesel::update(sync_state::table.find(1_i16))
                        .set(sync_state::last_fetched_block.eq(11_i64))
                        .execute(conn)?;
                    let _ = started_tx.send(());
                    conn.batch_execute("SELECT pg_sleep(5)")?;
                    Ok(())
                })
                .await
        });
        started_rx.await?;
        terminate(&mut fixture.admin, pid)?;
        assert!(blocked.await?.is_err());
        assert!(!pool.fatal_token().is_cancelled());
        assert_eq!(
            cursor(&fixture.url)?,
            0,
            "the interrupted update rolled back"
        );
        pool.write(|_| Ok(())).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn writer_lost_during_commit_reports_the_real_outcome() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/lost-commit".into(),
        )
        .await?;
        // A deferred trigger ends the writer's own backend inside COMMIT, so
        // the client loses the session exactly while committing.
        fixture.admin.batch_execute(&format!(
            "SET search_path TO {schema};
             CREATE FUNCTION end_own_session() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NULL; END $$;
             CREATE CONSTRAINT TRIGGER end_at_commit AFTER UPDATE ON sync_state
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION end_own_session();",
            schema = fixture.name
        ))?;
        let error = pool
            .write(|conn| {
                diesel::update(sync_state::table.find(1_i16))
                    .set(sync_state::last_fetched_block.eq(9_i64))
                    .execute(conn)?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::CommitDidNotApply(_)), "{error:?}");
        assert!(!pool.fatal_token().is_cancelled());
        assert_eq!(cursor(&fixture.url)?, 0);

        fixture
            .admin
            .batch_execute("DROP TRIGGER end_at_commit ON sync_state")?;
        pool.write(|conn| {
            diesel::update(sync_state::table.find(1_i16))
                .set(sync_state::last_fetched_block.eq(9_i64))
                .execute(conn)?;
            Ok(())
        })
        .await?;
        assert_eq!(cursor(&fixture.url)?, 9);
        Ok(())
    }

    #[test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    fn commit_outcome_is_read_back_by_transaction_id() -> Result<()> {
        let fixture = TestSchema::migrated()?;
        let mut conn = postgres_migrations::connect(&fixture.url)?;
        let mut xid_of = |finish: &str| -> Result<String> {
            conn.batch_execute(
                "BEGIN; UPDATE sync_state SET last_fetched_block = last_fetched_block + 1",
            )?;
            let xid =
                current_transaction_id(&mut conn)?.context("a writing transaction has an ID")?;
            conn.batch_execute(finish)?;
            Ok(xid)
        };
        let committed = xid_of("COMMIT")?;
        let aborted = xid_of("ROLLBACK")?;
        let mut check = postgres_migrations::connect(&fixture.url)?;
        assert!(matches!(
            transaction_committed(&mut check, &committed),
            Ok(true)
        ));
        assert!(matches!(
            transaction_committed(&mut check, &aborted),
            Ok(false)
        ));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn another_owner_after_disconnect_stops_the_solver() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/lock-taken".into(),
        )
        .await?;
        terminate(&mut fixture.admin, pool.writer.lock().await.backend.pid)?;
        let acquired = diesel::sql_query("SELECT pg_try_advisory_lock($1) AS value")
            .bind::<BigInt, _>(pool.writer_config.lock_key)
            .get_result::<Flag>(&mut fixture.admin)?
            .value;
        assert!(acquired, "the lost writer released the ownership lock");

        let error = pool.write(|_| Ok(())).await.unwrap_err();
        assert!(
            matches!(error, DbError::LockTakenAfterDisconnect),
            "{error:?}"
        );
        assert!(pool.fatal_token().is_cancelled());
        assert!(pool.write(|_| Ok(())).await.is_err());
        diesel::sql_query("SELECT pg_advisory_unlock($1) AS value")
            .bind::<BigInt, _>(pool.writer_config.lock_key)
            .get_result::<Flag>(&mut fixture.admin)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
    async fn a_new_owner_epoch_after_disconnect_stops_the_solver() -> Result<()> {
        let mut fixture = TestSchema::migrated()?;
        let pool = PgPool::open(
            fixture.url.clone(),
            fixture.url.clone(),
            1,
            "solver/epoch-moved".into(),
        )
        .await?;
        terminate(&mut fixture.admin, pool.writer.lock().await.backend.pid)?;
        // Another solver started, did work, and exited while we were away.
        fixture.admin.batch_execute(&format!(
            "UPDATE {}.sync_state SET owner_epoch = owner_epoch + 1",
            fixture.name
        ))?;
        let error = pool.write(|_| Ok(())).await.unwrap_err();
        assert!(
            matches!(error, DbError::OwnerEpochMoved { .. }),
            "{error:?}"
        );
        assert!(pool.fatal_token().is_cancelled());
        Ok(())
    }
}

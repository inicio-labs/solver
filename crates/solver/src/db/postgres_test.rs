//! Per-test PostgreSQL schemas: every test gets its own schema inside the
//! database named by `SOLVER_TEST_DATABASE_URL`, dropped when it ends.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use diesel::connection::SimpleConnection;
use diesel::{Connection, PgConnection};
use miden_protocol::account::AccountId;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::block::BlockNumber;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteType};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2, ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
};
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};

use super::maker_db::{self, AcceptMode, CommandOutcome};
use crate::maker::{CommandHeader, MakerCommand, MakerId};
use crate::types::OrderKeys;

use super::{postgres_migrations, DbError, DbPool};

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

/// Which way an order trades on the test pair: a sell offers faucet A for
/// faucet B, a buy offers B for A.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Sell,
    Buy,
}

/// A private PSWAP order offering `offered` for `requested`.
pub fn order(serial: u32, side: Side, offered: u64, requested: u64) -> Note {
    order_of(serial, side, offered, requested, NoteType::Private)
}

pub fn order_of(
    serial: u32,
    side: Side,
    offered: u64,
    requested: u64,
    note_type: NoteType,
) -> Note {
    let a = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let b = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
    match side {
        Side::Sell => pswap_order(serial, (a, offered), (b, requested), note_type),
        Side::Buy => pswap_order(serial, (b, offered), (a, requested), note_type),
    }
}

/// A private sell on another pair: faucet A for a third faucet.
pub fn order_on_other_pair(serial: u32, offered: u64, requested: u64) -> Note {
    let a = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let c = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2.try_into().unwrap();
    pswap_order(serial, (a, offered), (c, requested), NoteType::Private)
}

fn pswap_order(
    serial: u32,
    (offered_faucet, offered): (AccountId, u64),
    (requested_faucet, requested): (AccountId, u64),
    note_type: NoteType,
) -> Note {
    let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
        .try_into()
        .unwrap();
    PswapNote::builder()
        .sender(creator)
        .storage(
            PswapNoteStorage::builder()
                .min_requested_asset(FungibleAsset::new(requested_faucet, requested).unwrap())
                .min_fill_step(AssetAmount::new(1).unwrap())
                .creator_account_id(creator)
                .build(),
        )
        .serial_number(Word::from([serial, 0, 0, 0]))
        .note_type(note_type)
        .offered_asset(FungibleAsset::new(offered_faucet, offered).unwrap())
        .build()
        .unwrap()
        .into()
}

pub fn id(note: &Note) -> Vec<u8> {
    note.id().to_bytes().to_vec()
}

pub fn lineage_of(note: &Note) -> Vec<u8> {
    OrderKeys::from_note(note).unwrap().lineage_id
}

pub fn maker(conn: &mut PgConnection, name: &str) -> Result<MakerId> {
    Ok(maker_db::create_maker_tx(conn, name)?.unwrap())
}

/// Run one command in its own transaction, live at accept on with a fresh
/// watcher block when `live` is set.
pub fn run(
    conn: &mut PgConnection,
    maker_id: MakerId,
    seq: u64,
    command: &MakerCommand,
    live: bool,
) -> Result<CommandOutcome> {
    let accept = if live {
        AcceptMode::Fresh(BlockNumber::from(7))
    } else {
        AcceptMode::Off
    };
    let header = CommandHeader::new(maker_id, format!("r{seq}"), seq).unwrap();
    Ok(conn.transaction::<_, DbError, _>(|conn| {
        maker_db::execute_command_tx(conn, &header, command, accept)
    })?)
}

pub fn submit(
    conn: &mut PgConnection,
    maker_id: MakerId,
    seq: u64,
    note: &Note,
    live: bool,
) -> Result<CommandOutcome> {
    run(
        conn,
        maker_id,
        seq,
        &MakerCommand::submit(note.clone())?,
        live,
    )
}

/// Cancel `note`'s lineage with its maker's `CancelOrder` at `seq`.
pub fn cancel(conn: &mut PgConnection, maker_id: MakerId, seq: u64, note: &Note) -> Result<()> {
    let lineage_id = lineage_of(note);
    run(
        conn,
        maker_id,
        seq,
        &MakerCommand::CancelOrder { lineage_id },
        false,
    )?;
    Ok(())
}

/// Reserve `parents` in settlement `tx`, each filled by `amount`.
pub fn reserve(conn: &mut PgConnection, tx: u8, parents: &[&Note], amount: u64) -> Result<()> {
    let inputs: Vec<_> = parents
        .iter()
        .map(|parent| fill(tx, parent, amount).0)
        .collect();
    prepare(conn, tx, &inputs)
}

/// Every event of `maker_id`, decoded, in feed order.
pub fn events(
    conn: &mut PgConnection,
    maker_id: MakerId,
) -> Result<Vec<crate::gateway::proto::event_body::Kind>> {
    use prost::Message as _;
    Ok(maker_db::read_events_tx(conn, maker_id, 0, 1000)?
        .into_iter()
        .map(|event| {
            crate::gateway::proto::EventBody::decode(event.payload.as_slice())
                .unwrap()
                .kind
                .unwrap()
        })
        .collect())
}

/// Each SettlementResolved of `maker_id`: (input note, status, reason).
pub fn resolutions(
    conn: &mut PgConnection,
    maker_id: MakerId,
) -> Result<Vec<(Vec<u8>, crate::gateway::proto::ResultingStatus, String)>> {
    use crate::gateway::proto::event_body::Kind;
    Ok(events(conn, maker_id)?
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::SettlementResolved(resolved) => {
                let result = resolved.result.unwrap();
                let status = result.status();
                Some((result.note_id, status, result.reason))
            }
            _ => None,
        })
        .collect())
}

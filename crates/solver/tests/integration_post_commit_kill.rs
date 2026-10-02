//! A process kill after a database commit but before matcher publication must
//! release PostgreSQL ownership and let a new solver hydrate committed state.

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use diesel::prelude::*;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::{Note, NoteType};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
};
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};
use solver::db::postgres_models::NewOrderRow;
use solver::db::postgres_schema::orders;
use solver::db::{postgres_db, postgres_migrations, DbError, DbPool};
use solver::types::BookUpdate;
use tokio::sync::mpsc;

use common::PgSchema;

fn order_note(serial_number: Word) -> Result<Note> {
    let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
    let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
    let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
    Ok(PswapNote::builder()
        .sender(creator)
        .serial_number(serial_number)
        .note_type(NoteType::Public)
        .storage(
            PswapNoteStorage::builder()
                .min_requested_asset(FungibleAsset::new(requested, 10)?)
                .min_fill_step(AssetAmount::new(1)?)
                .creator_account_id(creator)
                .build(),
        )
        .offered_asset(FungibleAsset::new(offered, 10)?)
        .build()?
        .into())
}

// Spawned by the parent test, not by normal test discovery. A full channel
// pins write_book after its commit and before its matcher send; the parent
// observes the committed row and kills this process at that exact boundary.
#[tokio::test]
#[ignore = "helper for process_kill_after_commit_restores_book"]
async fn crash_worker() -> Result<()> {
    let Ok(url) = std::env::var("SOLVER_CRASH_WORKER_URL") else {
        return Ok(());
    };
    let pool = DbPool::open(url.clone(), url, 1, "solver/crash-worker".into()).await?;
    let (book_tx, _book_rx) = mpsc::channel(1);
    book_tx.send(BookUpdate::default()).await?;
    pool.write_book(&book_tx, move |conn| {
        // Active orders load in FIFO order.
        let parent = postgres_db::load_active_orders_tx(conn)?
            .into_iter()
            .next()
            .ok_or(DbError::Corrupt("crash fixture has no active parent"))?;
        let id = parent.id();
        diesel::update(orders::table.find(id.to_bytes()))
            .set(orders::status.eq("onchain_nullified"))
            .execute(conn)?;
        Ok(BookUpdate {
            removed: vec![id],
            active: Vec::new(),
        })
    })
    .await?;
    bail!("matcher publication unexpectedly completed before the process kill")
}

#[tokio::test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn process_kill_after_commit_restores_book() -> Result<()> {
    let pg = PgSchema::new().await?;
    let mut rng = RandomCoin::new(Word::default());
    let parent = order_note(rng.draw_word())?;
    let survivor = order_note(rng.draw_word())?;
    let parent_id = parent.id().to_bytes();
    let survivor_id = survivor.id();
    let parent_order = NewOrderRow::ingested(&parent, 10)?;
    let survivor_order = NewOrderRow::ingested(&survivor, 11)?;
    let pool = DbPool::open(
        pg.url.clone(),
        pg.url.clone(),
        1,
        "solver/crash-fixture".into(),
    )
    .await?;
    pool.write(move |conn| {
        postgres_db::insert_orders_batch_tx(conn, &[parent_order, survivor_order], 1)?;
        Ok(())
    })
    .await?;
    drop(pool);

    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", "crash_worker", "--ignored", "--nocapture"])
        .env("SOLVER_CRASH_WORKER_URL", &pg.url)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn solver crash worker")?;
    let observed: Result<()> = async {
        let started = Instant::now();
        loop {
            let url = pg.url.clone();
            let id = parent_id.clone();
            let status = tokio::task::spawn_blocking(move || -> Result<String> {
                let mut conn = postgres_migrations::connect(&url)?;
                Ok(orders::table
                    .find(&id)
                    .select(orders::status)
                    .first(&mut conn)?)
            })
            .await??;
            if status == "onchain_nullified" {
                if child.try_wait()?.is_some() {
                    bail!("crash worker exited before the forced process kill");
                }
                return Ok(());
            }
            if let Some(exit) = child.try_wait()? {
                bail!("crash worker exited before commit: {exit}");
            }
            if started.elapsed() >= Duration::from_secs(15) {
                bail!("crash worker did not commit the parent removal");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    .await;
    if let Err(error) = observed {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    child.kill().context("force-stop solver after commit")?;
    let killed = child.wait().context("reap solver crash worker")?;
    assert!(!killed.success(), "crash worker was not force-stopped");

    let restarted = DbPool::open(
        pg.url.clone(),
        pg.url.clone(),
        1,
        "solver/crash-restarted".into(),
    )
    .await
    .context("new solver could not reacquire PostgreSQL ownership")?;
    restarted.readiness_check().await?;
    let restored = restarted.read(postgres_db::load_active_orders_tx).await?;
    assert_eq!(restored.len(), 1, "startup hydration used stale book state");
    assert_eq!(restored[0].id(), survivor_id);
    assert_ne!(restored[0].id().to_bytes(), parent_id);
    Ok(())
}

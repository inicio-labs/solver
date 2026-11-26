//! PostgreSQL tests for self-trade prevention at submit (ADR 0009): the
//! storage it reads.

use std::collections::{BTreeSet, HashSet};

use anyhow::Result;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Binary, Nullable};
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteType};

use super::maker_db;
use super::postgres_db::{confirm_settlement_tx, insert_orders_batch_tx, test_consumer};
use super::postgres_models::NewOrderRow;
use super::postgres_schema::{eligible_lineages, eligible_orders, maker_lineages};
use super::postgres_test::{
    self, id, lineage_of, maker, order, order_of, run, submit, Side, TestSchema,
};
use super::DbError;
use crate::maker::{CutoffScope, MakerCommand};
use crate::types::OrderKeys;

/// The maker copied onto `note`'s order row.
fn row_maker(conn: &mut PgConnection, note: &Note) -> Result<Option<i64>> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Nullable<BigInt>)]
        maker_id: Option<i64>,
    }
    Ok(
        diesel::sql_query("SELECT maker_id FROM orders WHERE note_id = $1")
            .bind::<Binary, _>(id(note))
            .get_result::<Row>(conn)?
            .maker_id,
    )
}

/// The claim stores the submitted amounts, and every path that stores a
/// maker order's row (accept, activation, a remainder at confirmation)
/// gets the row its maker. A user's order has none.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn every_maker_row_gets_its_maker_and_the_claim_its_amounts() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (at_accept, confirmed, user) = (
        order(1, Side::Sell, 50, 100),
        order(2, Side::Buy, 70, 30),
        order_of(3, Side::Sell, 50, 100, NoteType::Public),
    );

    submit(conn, alpha, 1, &at_accept, true)?;
    assert_eq!(row_maker(conn, &at_accept)?, Some(alpha));

    submit(conn, alpha, 2, &confirmed, false)?;
    let amounts: (Option<i64>, Option<i64>) = maker_lineages::table
        .find(lineage_of(&confirmed))
        .select((
            maker_lineages::offered_amount,
            maker_lineages::requested_amount,
        ))
        .first(conn)?;
    assert_eq!(amounts, (Some(70), Some(30)));
    let submission = maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .find(|s| s.note.id() == confirmed.id())
        .unwrap();
    conn.transaction::<_, DbError, _>(|conn| maker_db::activate_tx(conn, &[submission], 1))?;
    assert_eq!(row_maker(conn, &confirmed)?, Some(alpha));

    conn.transaction::<_, DbError, _>(|conn| {
        insert_orders_batch_tx(conn, &[NewOrderRow::parsed(&user, 1)?], 1)
    })?;
    assert_eq!(row_maker(conn, &user)?, None);

    // A partial fill's remainder, stored when its settlement confirms.
    let (input, _, remainder) = postgres_test::fill(9, &at_accept, 40);
    let remainder = remainder.expect("a partial fill");
    postgres_test::prepare(conn, 9, &[input])?;
    conn.transaction::<_, DbError, _>(|conn| {
        confirm_settlement_tx(
            conn,
            &[9; 32],
            &HashSet::new(),
            BlockNumber::from(8),
            test_consumer(),
            None,
        )
    })?;
    assert_eq!(row_maker(conn, &remainder)?, Some(alpha));
    Ok(())
}

/// The migration gives every open maker order row that already exists its
/// maker (active, settling, waiting), and leaves a finished one alone; and
/// it can be reverted and applied again with that data in place.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_migration_backfills_the_maker_of_open_rows() -> Result<()> {
    use super::postgres_migrations::migrate;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    postgres_test::revert_after(conn, "20261119000001")?;
    let alpha = maker(conn, "alpha")?;
    let notes: Vec<Note> = (1..=4).map(|s| order(s, Side::Sell, 50, 100)).collect();
    for (index, note) in notes.iter().enumerate() {
        let seq = 1 + index as i64;
        let keys = OrderKeys::from_note(note)?;
        diesel::sql_query(
            "INSERT INTO maker_commands (maker_id, request_id, seq, kind, payload, result)
             VALUES ($1, 'r' || $2, $2, 'submit', '\\x00', '{\"result\":\"accepted\"}')",
        )
        .bind::<BigInt, _>(alpha)
        .bind::<BigInt, _>(seq)
        .execute(conn)?;
        diesel::sql_query(
            "INSERT INTO maker_lineages (lineage_id, maker_id, request_id, root_seq, note_id,
                                         market, direction, state)
             VALUES ($3, $1, 'r' || $2, $2, $4, $5, $6, 'activated')",
        )
        .bind::<BigInt, _>(alpha)
        .bind::<BigInt, _>(seq)
        .bind::<Binary, _>(&keys.lineage_id)
        .bind::<Binary, _>(id(note))
        .bind::<Binary, _>(&keys.market)
        .bind::<Binary, _>(&keys.direction)
        .execute(conn)?;
    }
    let rows: Vec<_> = notes
        .iter()
        .map(|note| NewOrderRow::parsed(note, 1))
        .collect::<std::result::Result<_, _>>()?;
    conn.transaction::<_, DbError, _>(|conn| insert_orders_batch_tx(conn, &rows, 1))?;
    for (note, status) in notes[1..].iter().zip(["settling", "waiting", "stopped"]) {
        diesel::sql_query("UPDATE orders SET status = $1 WHERE note_id = $2")
            .bind::<diesel::sql_types::Text, _>(status)
            .bind::<Binary, _>(id(note))
            .execute(conn)?;
    }
    let expected = [Some(alpha), Some(alpha), Some(alpha), None];
    for round in 0..2 {
        if round == 1 {
            postgres_test::revert_after(conn, "20261119000001")?;
        }
        migrate(conn)?;
        let makers = notes
            .iter()
            .map(|note| row_maker(conn, note))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(makers, expected, "round {round}");
    }
    Ok(())
}

/// `eligible_lineages` admits a lineage exactly when `eligible_orders`
/// admits its rows: the same stops and cutoffs, by maker, market and
/// direction. Each of those clauses keeps one order eligible here.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_lineage_view_admits_what_the_order_view_admits() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let beta = maker(conn, "beta")?;
    let side = |index: usize| {
        if index % 2 == 0 {
            Side::Sell
        } else {
            Side::Buy
        }
    };
    // Alpha: sells and buys at seqs 10, 12, ..., 20.
    let mut notes: Vec<Note> = (0..6)
        .map(|index| order(1 + index as u32, side(index), 50, 100))
        .collect();
    for (index, note) in notes.iter().enumerate() {
        submit(conn, alpha, 10 + 2 * index as u64, note, true)?;
    }
    // Beta: a sell on another pair at 1, a buy at 2 and a sell at 4.
    notes.push(order(7, Side::Sell, 50, 100));
    notes.push(order(8, Side::Buy, 50, 100));
    notes.push(postgres_test::order_on_other_pair(9, 50, 100));
    for (index, seq) in [(6, 4), (7, 2), (8, 1)] {
        submit(conn, beta, seq, &notes[index], true)?;
    }
    let market = OrderKeys::from_note(&notes[0])?.market;
    let sells = OrderKeys::from_note(&notes[0])?.direction;
    run(
        conn,
        alpha,
        40,
        &MakerCommand::CancelOrder {
            lineage_id: lineage_of(&notes[5]),
        },
        false,
    )?;
    // Alpha's sells below 15 on this market: seqs 10 and 14. Not its buy
    // at 12 (direction), not beta's sell at 4 (maker).
    run(
        conn,
        alpha,
        15,
        &MakerCommand::CancelAll {
            scope: CutoffScope {
                market: market.clone(),
                direction: sells,
            },
        },
        false,
    )?;
    // Beta's whole market below 3: its buy at 2. Not its order on the other
    // pair at 1 (market).
    run(
        conn,
        beta,
        3,
        &MakerCommand::CancelAll {
            scope: CutoffScope {
                market,
                direction: Vec::new(),
            },
        },
        false,
    )?;
    let lineages: BTreeSet<Vec<u8>> = eligible_lineages::table
        .select(eligible_lineages::lineage_id)
        .load(conn)?
        .into_iter()
        .collect();
    let rows: BTreeSet<Vec<u8>> = eligible_orders::table
        .filter(eligible_orders::lineage_id.is_not_null())
        .select(eligible_orders::lineage_id.assume_not_null())
        .load(conn)?
        .into_iter()
        .collect();
    assert_eq!(lineages, rows);
    let expected: BTreeSet<Vec<u8>> = [1, 3, 4, 6, 8]
        .iter()
        .map(|index| lineage_of(&notes[*index]))
        .collect();
    assert_eq!(lineages, expected);
    Ok(())
}

/// The startup count finds open lineages without amounts and open maker
/// rows without their maker, and nothing else.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_startup_count_finds_what_the_check_cannot_see() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let notes: Vec<Note> = (1..=7).map(|s| order(s, Side::Sell, 50, 100)).collect();
    // 0 and 1 go live at accept (still pending), 2 waits for its note, 3 to
    // 6 are activated by the watcher.
    for (index, note) in notes.iter().enumerate() {
        submit(conn, alpha, 1 + index as u64, note, index < 2)?;
    }
    let activated: Vec<_> = maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .filter(|s| notes[3..].iter().any(|note| note.id() == s.note.id()))
        .collect();
    conn.transaction::<_, DbError, _>(|conn| maker_db::activate_tx(conn, &activated, 1))?;
    let user = order_of(8, Side::Sell, 50, 100, NoteType::Public);
    conn.transaction::<_, DbError, _>(|conn| {
        insert_orders_batch_tx(conn, &[NewOrderRow::parsed(&user, 1)?], 1)
    })?;
    let ready = maker_db::stp_readiness_tx(conn)?;
    assert_eq!(
        (ready.lineages_without_amounts, ready.orders_without_maker),
        (0, 0)
    );
    // As if claimed or stored before the migration. Without amounts: 0 and
    // 2 (pending), 4 (an open row) and 5 (its row finished). Without its
    // maker: 1 (open), 6 (stopped) and the user's order (no claim).
    let ids = |indexes: &[usize]| indexes.iter().map(|i| id(&notes[*i])).collect::<Vec<_>>();
    diesel::update(
        maker_lineages::table.filter(maker_lineages::note_id.eq_any(ids(&[0, 2, 4, 5]))),
    )
    .set((
        maker_lineages::offered_amount.eq(None::<i64>),
        maker_lineages::requested_amount.eq(None::<i64>),
    ))
    .execute(conn)?;
    for (note, status) in [(&notes[5], "executed"), (&notes[6], "stopped")] {
        diesel::sql_query("UPDATE orders SET status = $1 WHERE note_id = $2")
            .bind::<diesel::sql_types::Text, _>(status)
            .bind::<Binary, _>(id(note))
            .execute(conn)?;
    }
    diesel::sql_query("UPDATE orders SET maker_id = NULL WHERE note_id = ANY($1)")
        .bind::<diesel::sql_types::Array<Binary>, _>(ids(&[1, 6]))
        .execute(conn)?;
    let ready = maker_db::stp_readiness_tx(conn)?;
    assert_eq!(
        (ready.lineages_without_amounts, ready.orders_without_maker),
        (3, 1)
    );
    // A cancelled lineage is no longer open.
    run(
        conn,
        alpha,
        9,
        &MakerCommand::CancelOrder {
            lineage_id: lineage_of(&notes[2]),
        },
        false,
    )?;
    let ready = maker_db::stp_readiness_tx(conn)?;
    assert_eq!(ready.lineages_without_amounts, 2);
    Ok(())
}

/// The check compares a late submit with every newer lineage of its maker,
/// in any state, so lineage history is kept: no code deletes a claim.
#[test]
fn no_code_deletes_a_lineage_claim() {
    fn sources(dir: &std::path::Path, found: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                sources(&path, found);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                found.push((path.display().to_string(), text));
            }
        }
    }
    let mut files = Vec::new();
    sources(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let forbidden = [
        ["delete", "(maker_lineages"].concat(),
        ["DELETE FROM ", "maker_lineages"].concat(),
    ];
    for (path, text) in files {
        let text = text.to_ascii_uppercase();
        for pattern in &forbidden {
            assert!(
                !text.contains(&pattern.to_ascii_uppercase()),
                "{path} deletes from maker_lineages"
            );
        }
    }
}

/// A stop row: lineage, source and cause.
type StopRow = (Vec<u8>, Option<String>, Option<Vec<u8>>);

/// Every stop row: lineage, source and cause.
fn stop_rows(conn: &mut PgConnection) -> Result<Vec<StopRow>> {
    use super::postgres_schema::maker_stops;
    Ok(maker_stops::table
        .select((
            maker_stops::lineage_id,
            maker_stops::source,
            maker_stops::caused_by_lineage,
        ))
        .order(maker_stops::lineage_id)
        .load(conn)?)
}

/// Every stop is written with who wrote it, once: a later stop of the same
/// lineage changes nothing and is not reported as new.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn every_stop_records_its_source_once() -> Result<()> {
    use super::maker_db::{Close, Stop, StopSource};
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let notes: Vec<Note> = (1..=4).map(|s| order(s, Side::Sell, 50, 100)).collect();
    for (index, note) in notes.iter().enumerate() {
        submit(conn, alpha, 1 + index as u64, note, true)?;
    }
    run(
        conn,
        alpha,
        10,
        &MakerCommand::CancelOrder {
            lineage_id: lineage_of(&notes[0]),
        },
        false,
    )?;
    let capped = maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .find(|s| s.note.id() == notes[1].id())
        .unwrap();
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::close_unconfirmed_tx(conn, [&capped], Close::NoteNotReceived)
    })?;
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::lock_makers_ordered_tx(conn, &[alpha])?;
        maker_db::cancel_self_trades_tx(conn, &[id(&notes[2]), id(&notes[0])])
    })?;
    conn.transaction::<_, DbError, _>(maker_db::cancel_unconfirmed_tx)?;
    let row = |note: &Note, source: &str| (lineage_of(note), Some(source.to_owned()), None);
    let mut expected = vec![
        row(&notes[0], "maker"),
        row(&notes[1], "cap"),
        row(&notes[2], "stp_reservation"),
        row(&notes[3], "escape_hatch"),
    ];
    expected.sort();
    assert_eq!(stop_rows(conn)?, expected);

    // A second stop, by anyone, keeps the first and is not new.
    let again = conn.transaction::<_, DbError, _>(|conn| {
        let first = lineage_of(&notes[0]);
        let other = lineage_of(&notes[3]);
        maker_db::stop_lineages_tx(
            conn,
            &[
                Stop {
                    maker_id: alpha,
                    lineage_id: &first,
                    source: StopSource::StpOnSubmit,
                    caused_by: Some(&other),
                },
                Stop {
                    maker_id: alpha,
                    lineage_id: &first,
                    source: StopSource::Maker,
                    caused_by: None,
                },
            ],
        )
    })?;
    assert!(again.is_empty());
    assert_eq!(stop_rows(conn)?, expected);

    // The same new stop twice in one call (the hatch can name a lineage
    // twice): one row, returned once.
    let fresh = order(5, Side::Sell, 50, 100);
    submit(conn, alpha, 20, &fresh, true)?;
    let lineage = lineage_of(&fresh);
    let twice = conn.transaction::<_, DbError, _>(|conn| {
        let stop = Stop {
            maker_id: alpha,
            lineage_id: &lineage,
            source: StopSource::Maker,
            caused_by: None,
        };
        maker_db::stop_lineages_tx(conn, &[stop, stop])
    })?;
    assert_eq!(twice, [lineage.clone()].into_iter().collect());
    expected.push(row(&fresh, "maker"));

    // A self-trade stop at submit records the newer order that caused it.
    let older = order(6, Side::Sell, 50, 100);
    submit(conn, alpha, 21, &older, true)?;
    let (lineage, cause) = (lineage_of(&older), lineage_of(&notes[3]));
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::stop_lineages_tx(
            conn,
            &[Stop {
                maker_id: alpha,
                lineage_id: &lineage,
                source: StopSource::StpOnSubmit,
                caused_by: Some(&cause),
            }],
        )
    })?;
    expected.push((lineage, Some("stp_on_submit".to_owned()), Some(cause)));
    expected.sort();
    assert_eq!(stop_rows(conn)?, expected);
    Ok(())
}

/// Stops are written in chunks: every chunk's rows are stored and returned,
/// and a repeat in a later chunk is not new.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn stops_past_one_chunk_are_all_written_once() -> Result<()> {
    use super::maker_db::{Stop, StopSource};
    use super::postgres_db::INSERT_CHUNK_ROWS;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    // No claims needed: a stop row refers to its maker only.
    let mut lineages: Vec<Vec<u8>> = (0..=INSERT_CHUNK_ROWS as u32)
        .map(|n| n.to_be_bytes().to_vec())
        .collect();
    lineages.push(lineages[0].clone());
    let stops: Vec<Stop> = lineages
        .iter()
        .map(|lineage_id| Stop {
            maker_id: alpha,
            lineage_id,
            source: StopSource::Maker,
            caused_by: None,
        })
        .collect();
    let stopped =
        conn.transaction::<_, DbError, _>(|conn| maker_db::stop_lineages_tx(conn, &stops))?;
    assert_eq!(stopped.len(), INSERT_CHUNK_ROWS + 1);
    assert_eq!(stop_rows(conn)?.len(), INSERT_CHUNK_ROWS + 1);
    Ok(())
}

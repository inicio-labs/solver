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
    self, ask, bid, history, id, lineage_of, maker, order, order_of, run, submit, Side, TestSchema,
};
use super::DbError;
use crate::maker::{CutoffScope, MakerCommand, MakerId};
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

/// Resolve settlement `tx`: committed, or voided because the node never
/// received it, with `consumed` spent elsewhere meanwhile.
fn resolve(conn: &mut PgConnection, tx: u8, commit: bool, consumed: &[&Note]) -> Result<()> {
    use super::postgres_db::{finish_discarded_settlement_tx, live_book_update_tx, NodeAnswer};
    let consumed: HashSet<_> = consumed.iter().map(|note| note.id()).collect();
    conn.transaction::<_, DbError, _>(|conn| {
        if commit {
            // As the executor writes it: the update passes the liveness rule.
            let confirmed = confirm_settlement_tx(
                conn,
                &[tx; 32],
                &consumed,
                BlockNumber::from(8),
                test_consumer(),
                None,
            )?;
            live_book_update_tx(conn, confirmed.update)?;
        } else {
            let released =
                finish_discarded_settlement_tx(conn, &[tx; 32], &consumed, &NodeAnswer::NotSent)?;
            live_book_update_tx(conn, released.update)?;
        }
        Ok(())
    })?;
    Ok(())
}

/// A settlement result that says CANCELLED gives the reason the order was
/// stopped while the settlement held it: the maker's own cancel or cutoff,
/// a newer crossing order, or the escape hatch; a stop wins over a cutoff.
/// Other results give none.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_cancelled_settlement_result_says_why() -> Result<()> {
    use super::maker_db::{Stop, StopSource};
    use super::postgres_schema::maker_stops;
    use crate::gateway::proto::ResultingStatus::{Cancelled, FullyFilled, Live, SpentElsewhere};
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let notes: Vec<Note> = (1..=9).map(|s| order(s, Side::Sell, 50, 100)).collect();
    // Seqs 10, 20, ..., 90, each in its own settlement; 7 fills whole.
    for (index, note) in notes.iter().enumerate() {
        submit(conn, alpha, 10 * (1 + index as u64), note, true)?;
        let amount = if index == 7 { 100 } else { 40 };
        postgres_test::reserve(conn, 10 + index as u8, &[note], amount)?;
    }
    // 0, 7 and 8: their maker's CancelOrder.
    for (seq, index) in [(100, 0), (110, 7), (120, 8)] {
        postgres_test::cancel(conn, alpha, seq, &notes[index])?;
    }
    // The maker's cutoff on sells below 55 covers 0 to 4; 1 has nothing
    // else.
    run(
        conn,
        alpha,
        55,
        &MakerCommand::CancelAll {
            scope: CutoffScope {
                market: OrderKeys::from_note(&notes[1])?.market,
                direction: OrderKeys::from_note(&notes[1])?.direction,
            },
        },
        false,
    )?;
    // 2: a newer crossing order; 3: the escape hatch's stop; 4: a stop
    // from before stops recorded their source. 5: not stopped. 6 is the
    // newer order, still settling.
    let (two, newer) = (lineage_of(&notes[2]), lineage_of(&notes[6]));
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::stop_lineages_tx(
            conn,
            &[
                Stop {
                    maker_id: alpha,
                    lineage_id: &two,
                    source: StopSource::StpOnSubmit,
                    caused_by: Some(&newer),
                },
                Stop {
                    maker_id: alpha,
                    lineage_id: &lineage_of(&notes[3]),
                    source: StopSource::EscapeHatch,
                    caused_by: None,
                },
                Stop {
                    maker_id: alpha,
                    lineage_id: &lineage_of(&notes[4]),
                    source: StopSource::Maker,
                    caused_by: None,
                },
            ],
        )
    })?;
    diesel::update(maker_stops::table.filter(maker_stops::lineage_id.eq(lineage_of(&notes[4]))))
        .set(maker_stops::source.eq(None::<String>))
        .execute(conn)?;
    for (index, commit, consumed) in [
        (0, true, None),
        (1, false, None),
        (2, true, None),
        (3, false, None),
        (4, true, None),
        (5, true, None),
        (7, true, None),
        (8, false, Some(&notes[8])),
    ] {
        let consumed: Vec<&Note> = consumed.into_iter().collect();
        resolve(conn, 10 + index as u8, commit, &consumed)?;
    }
    let reasons: Vec<_> = postgres_test::resolutions(conn, alpha)?
        .into_iter()
        .map(|(note, status, reason)| {
            let index = notes.iter().position(|n| id(n) == note).unwrap();
            (index, status, reason)
        })
        .collect();
    assert_eq!(
        reasons,
        [
            (0, Cancelled, "maker_cancel".to_owned()),
            (1, Cancelled, "maker_cancel".to_owned()),
            (2, Cancelled, "self_trade_prevention".to_owned()),
            (3, Cancelled, "note_not_received".to_owned()),
            (4, Cancelled, String::new()),
            (5, Live, String::new()),
            (7, FullyFilled, String::new()),
            (8, SpentElsewhere, String::new()),
        ]
    );
    // A lineage neither stopped nor cut off has no reason at all.
    assert!(maker_db::cancelled_lineages(conn, &[&lineage_of(&notes[5])])?.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// The check at submit (ADR 0009): intake rounds with the switch on.

use super::maker_db::{AcceptGate, StpCancel};
use crate::gateway::intake::{intake_round_tx, IntakeRound};
use crate::gateway::proto::OrderState;
use crate::maker::{CommandHeader as Header, MakerUpdate};

fn sub(maker_id: MakerId, seq: u64, note: &Note) -> (Header, MakerCommand) {
    (
        Header::new(maker_id, format!("r{seq}"), seq).unwrap(),
        MakerCommand::submit(note.clone()).unwrap(),
    )
}

/// A submit of `note` that expires at `at` (Unix ms).
fn sub_expiring(maker_id: MakerId, seq: u64, note: &Note, at: u64) -> (Header, MakerCommand) {
    let mut command = sub(maker_id, seq, note);
    if let MakerCommand::Submit {
        expires_at_unix_ms, ..
    } = &mut command.1
    {
        *expires_at_unix_ms = Some(at);
    }
    command
}

fn cancel_cmd(maker_id: MakerId, seq: u64, note: &Note) -> (Header, MakerCommand) {
    (
        Header::new(maker_id, format!("r{seq}"), seq).unwrap(),
        MakerCommand::CancelOrder {
            lineage_id: lineage_of(note),
        },
    )
}

/// One intake round with the check on: live at accept on (a fresh watcher
/// block) or off. Cancels must come first, as intake orders them.
fn stp_round(
    conn: &mut PgConnection,
    commands: &[(Header, MakerCommand)],
    live: bool,
) -> Result<IntakeRound> {
    round(conn, commands, live, true)
}

/// One intake round, with the check on or off.
fn round(
    conn: &mut PgConnection,
    commands: &[(Header, MakerCommand)],
    live: bool,
    check: bool,
) -> Result<IntakeRound> {
    let epoch = postgres_test::epoch(conn)?;
    let gate = if live {
        AcceptGate::new(epoch, || Some(BlockNumber::from(7)))
    } else {
        AcceptGate::epoch_only(epoch)
    };
    Ok(conn
        .transaction::<_, DbError, _>(|conn| intake_round_tx(conn, commands, Some(&gate), check))?)
}

fn cancelled(note: &Note) -> (Vec<u8>, OrderState, String) {
    (
        id(note),
        OrderState::Cancelled,
        "self_trade_prevention".into(),
    )
}

fn went_live(note: &Note) -> (Vec<u8>, OrderState, String) {
    (id(note), OrderState::Live, String::new())
}

fn lineage_state(conn: &mut PgConnection, note: &Note) -> Result<String> {
    Ok(maker_lineages::table
        .find(lineage_of(note))
        .select(maker_lineages::state)
        .first(conn)?)
}

/// The source and cause of `note`'s stop, if it has one.
fn stop_of(conn: &mut PgConnection, note: &Note) -> Result<Option<(String, Option<Vec<u8>>)>> {
    use super::postgres_schema::maker_stops;
    Ok(maker_stops::table
        .filter(maker_stops::lineage_id.eq(lineage_of(note)))
        .select((
            maker_stops::source.assume_not_null(),
            maker_stops::caused_by_lineage,
        ))
        .first(conn)
        .optional()?)
}

fn in_book(conn: &mut PgConnection, note: &Note) -> Result<bool> {
    Ok(super::postgres_db::load_live_orders_tx(conn)?
        .iter()
        .any(|order| order.id() == note.id()))
}

/// A new order cancels the maker's older crossing orders in every state
/// they can be open in, each as the table in ADR 0009 says: pending or
/// waiting for its note (CANCELLED, lineage closed if pending), live
/// (CANCELLED), in a settlement (no event now). It goes live itself, and
/// the matcher learns of every loser.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_new_order_cancels_older_crossing_orders_in_every_open_state() -> Result<()> {
    use super::postgres_db::mark_orders_onchain_nullified_tx;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    // Serials run against seqs, so lineage IDs sort the other way round from
    // seqs: the events must still come in seq order.
    let (pending, live, waiting, settling, retired) = (
        ask(60, 100),
        ask(50, 99),
        ask(40, 98),
        ask(30, 97),
        ask(20, 96),
    );
    let (filled, same_side, above) = (ask(10, 95), bid(5, 90), ask(6, 120));
    // Pending: accepted, its note not in a block yet (live at accept off).
    stp_round(conn, &[sub(alpha, 1, &pending)], false)?;
    stp_round(conn, &[sub(alpha, 2, &live)], true)?;
    stp_round(conn, &[sub(alpha, 3, &waiting)], true)?;
    postgres_test::set_status(conn, &waiting, "waiting")?;
    stp_round(conn, &[sub(alpha, 4, &settling)], true)?;
    postgres_test::reserve(conn, 9, &[&settling], 40)?;
    // Retired by the watcher while its settlement is unresolved (decision N).
    stp_round(conn, &[sub(alpha, 5, &retired)], true)?;
    postgres_test::reserve(conn, 8, &[&retired], 40)?;
    conn.transaction::<_, DbError, _>(|conn| {
        mark_orders_onchain_nullified_tx(conn, &[id(&retired)])
    })?;
    // Filled on the confirmed path: not open, so never cancelled.
    stp_round(conn, &[sub(alpha, 6, &filled)], false)?;
    let landed = maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .find(|s| s.note.id() == filled.id())
        .unwrap();
    conn.transaction::<_, DbError, _>(|conn| maker_db::activate_tx(conn, &[landed], 1))?;
    let (input, _, rest) = postgres_test::fill(7, &filled, 95);
    assert!(rest.is_none(), "a full fill");
    postgres_test::prepare(conn, 7, &[input])?;
    resolve(conn, 7, true, &[])?;
    stp_round(
        conn,
        &[sub(alpha, 7, &same_side), sub(alpha, 8, &above)],
        true,
    )?;
    let before = postgres_test::statuses(conn, alpha)?.len();

    let new = bid(7, 100);
    let round = stp_round(conn, &[sub(alpha, 10, &new)], true)?;
    assert!(
        round.outcomes[0].1.live.is_some(),
        "the new order goes live"
    );
    let mut cancelled_lineages: Vec<Vec<u8>> = round
        .self_trade
        .updates
        .iter()
        .map(|update| match update {
            MakerUpdate::LineageCancelled { lineage_id, .. } => lineage_id.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    cancelled_lineages.sort();
    let losers = [&pending, &live, &waiting, &settling, &retired];
    let mut expected: Vec<Vec<u8>> = losers.iter().map(|note| lineage_of(note)).collect();
    expected.sort();
    assert_eq!(cancelled_lineages, expected);
    assert!(round.self_trade.appended);
    // CANCELLED for the pending, live and waiting ones, in seq order; then
    // LIVE. Nothing for the settling one and the retired parent.
    assert_eq!(
        postgres_test::statuses(conn, alpha)?[before..],
        [
            cancelled(&pending),
            cancelled(&live),
            cancelled(&waiting),
            went_live(&new),
        ]
    );
    assert_eq!(lineage_state(conn, &pending)?, "rejected");
    for note in losers {
        let (source, caused_by) = stop_of(conn, note)?.expect("stopped");
        assert_eq!(source, "stp_on_submit");
        assert_eq!(caused_by, Some(lineage_of(&new)));
        assert!(!in_book(conn, note)?);
    }
    assert_eq!(stop_of(conn, &filled)?, None);
    for note in [&same_side, &above] {
        assert_eq!(stop_of(conn, note)?, None);
        assert!(in_book(conn, note)?);
    }
    assert_eq!(
        round.self_trade.cancels,
        [StpCancel {
            maker_id: alpha,
            entering: false
        }; 5]
    );
    // The settling one and the retired parent: their settlements commit
    // partial fills; the remainders never enter the book, and the results
    // say why.
    resolve(conn, 9, true, &[])?;
    resolve(conn, 8, true, &[])?;
    for parent in [&settling, &retired] {
        let (_, _, remainder) = postgres_test::fill(0, parent, 40);
        let remainder = remainder.unwrap();
        assert_eq!(
            postgres_test::status(conn, &remainder)?.as_deref(),
            Some("stopped")
        );
        assert!(!in_book(conn, &remainder)?);
    }
    let reasons: Vec<_> = postgres_test::resolutions(conn, alpha)?
        .into_iter()
        .filter(|(note, ..)| *note != id(&filled))
        .collect();
    let cancelled_result = |note: &Note| {
        (
            id(note),
            crate::gateway::proto::ResultingStatus::Cancelled,
            "self_trade_prevention".to_owned(),
        )
    };
    assert_eq!(
        reasons,
        [cancelled_result(&settling), cancelled_result(&retired)]
    );
    Ok(())
}

/// Resolve settlement `tx` holding `parent`: voided, with `parent` spent
/// outside the solver.
fn void_spent(conn: &mut PgConnection, tx: u8, parent: &Note) -> Result<()> {
    use super::postgres_db::{finish_discarded_settlement_tx, live_book_update_tx, NodeAnswer};
    conn.transaction::<_, DbError, _>(|conn| {
        let consumed: HashSet<_> = [parent.id()].into_iter().collect();
        let released =
            finish_discarded_settlement_tx(conn, &[tx; 32], &consumed, &NodeAnswer::NotSent)?;
        live_book_update_tx(conn, released.update)?;
        Ok(())
    })?;
    Ok(())
}

/// A submit that reaches us after a newer crossing order of its maker gets
/// CANCELLED and never LIVE, whatever happened to the newer one since:
/// pending, live, settling, cancelled, filled, lost to an even newer order,
/// or past its expiry. Its lineage is closed, so nothing follows when its
/// note is spent, and it still cancels its own older crossing orders.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_late_order_loses_to_a_newer_crossing_order_in_any_state() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let states = [
        "pending",
        "live",
        "settling",
        "cancelled",
        "filled",
        "lost",
        "expired",
    ];
    for (index, state) in states.iter().enumerate() {
        let maker_id = maker(conn, state)?;
        let serial = 100 * (index as u32 + 1);
        // An older bid of its own, a newer bid that crosses the late ask.
        let (oldest, late, newer) = (bid(serial, 100), ask(serial + 1, 100), bid(serial + 2, 101));
        stp_round(conn, &[sub(maker_id, 1, &oldest)], true)?;
        let newer_submit = if *state == "expired" {
            sub_expiring(maker_id, 10, &newer, 1)
        } else {
            sub(maker_id, 10, &newer)
        };
        stp_round(conn, &[newer_submit], *state != "pending")?;
        match *state {
            "settling" => postgres_test::reserve(conn, serial as u8, &[&newer], 40)?,
            "cancelled" => {
                stp_round(conn, &[cancel_cmd(maker_id, 11, &newer)], true)?;
            }
            "filled" => {
                let (input, _, remainder) = postgres_test::fill(serial as u8, &newer, 100);
                assert!(remainder.is_none(), "a full fill");
                postgres_test::prepare(conn, serial as u8, &[input])?;
                resolve(conn, serial as u8, true, &[])?;
            }
            "lost" => {
                // An ask at 1.005: it crosses the newer bid (1.01), not the
                // oldest (1.00).
                let newest = order(serial + 3, Side::Sell, 200, 201);
                stp_round(conn, &[sub(maker_id, 20, &newest)], true)?;
                assert!(stop_of(conn, &newer)?.is_some());
            }
            _ => {}
        }
        let before = postgres_test::statuses(conn, maker_id)?.len();
        let round = stp_round(conn, &[sub(maker_id, 5, &late)], true)?;
        assert!(round.outcomes[0].1.live.is_none(), "{state}");
        assert_eq!(
            round.outcomes[0].1.reply,
            crate::maker::CommandReply::Committed(crate::maker::CommandResult::Accepted),
            "{state}: the reply stays Accepted"
        );
        assert_eq!(
            postgres_test::statuses(conn, maker_id)?[before..],
            [cancelled(&oldest), cancelled(&late)],
            "{state}"
        );
        assert_eq!(
            stop_of(conn, &late)?.unwrap(),
            ("stp_on_submit".to_owned(), Some(lineage_of(&newer))),
            "{state}"
        );
        assert_eq!(lineage_state(conn, &late)?, "rejected", "{state}");
        assert_eq!(postgres_test::status(conn, &late)?, None, "{state}");
        assert_eq!(
            round.self_trade.cancels,
            [
                StpCancel {
                    maker_id,
                    entering: false
                },
                StpCancel {
                    maker_id,
                    entering: true
                }
            ],
            "{state}"
        );
        // Its note is never watched: nothing follows when it is spent.
        assert!(maker_db::pending_submissions_tx(conn)?
            .iter()
            .all(|s| s.note.id() != late.id()));
    }
    Ok(())
}

/// A submit its maker cancelled first, or a cutoff covers, still cancels
/// its older crossing orders, never goes live and gets no event of its own,
/// even when a newer crossing order would have beaten it. It is counted.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_cancelled_or_cut_off_submit_still_cancels_its_older_crossing_orders() -> Result<()> {
    use super::maker_db::DeadCause;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (older, dead, newer) = (ask(1, 100), bid(2, 101), ask(3, 101));
    stp_round(conn, &[sub(alpha, 5, &older)], true)?;
    stp_round(conn, &[sub(alpha, 20, &newer)], true)?;
    // The cancel of `dead` (seq 11) comes before its submit (seq 10).
    stp_round(conn, &[cancel_cmd(alpha, 11, &dead)], true)?;
    let before = postgres_test::statuses(conn, alpha)?.len();
    let round = stp_round(conn, &[sub(alpha, 10, &dead)], true)?;
    assert!(round.outcomes[0].1.live.is_none());
    assert_eq!(
        postgres_test::statuses(conn, alpha)?[before..],
        [cancelled(&older)]
    );
    assert_eq!(
        round.self_trade.dead_on_arrival,
        [(alpha, DeadCause::Cancelled)]
    );
    // Closed: the watcher never tells of it, even if its note is spent.
    assert_eq!(lineage_state(conn, &dead)?, "rejected");
    assert_eq!(
        round.self_trade.cancels,
        [StpCancel {
            maker_id: alpha,
            entering: false
        }]
    );
    assert!(in_book(conn, &newer)?);
    assert_eq!(stop_of(conn, &dead)?.unwrap().0, "maker");

    // Under a cutoff on bids only, so the older ask is open.
    let beta = maker(conn, "beta")?;
    let (older, cut) = (ask(11, 100), bid(12, 101));
    stp_round(conn, &[sub(beta, 5, &older)], true)?;
    let keys = OrderKeys::from_note(&cut)?;
    stp_round(
        conn,
        &[(
            Header::new(beta, "c".into(), 12).unwrap(),
            MakerCommand::CancelAll {
                scope: CutoffScope {
                    market: keys.market,
                    direction: keys.direction,
                },
            },
        )],
        true,
    )?;
    let before = postgres_test::statuses(conn, beta)?.len();
    let round = stp_round(conn, &[sub(beta, 10, &cut)], true)?;
    assert!(round.outcomes[0].1.live.is_none());
    assert_eq!(
        postgres_test::statuses(conn, beta)?[before..],
        [cancelled(&older)]
    );
    assert_eq!(
        round.self_trade.dead_on_arrival,
        [(beta, DeadCause::CutOff)]
    );
    Ok(())
}

/// Two crossing submits in one round, in either order, with live at accept
/// on or off: only the newer one can go live; the older one gets CANCELLED
/// and never LIVE.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn of_two_crossing_submits_in_one_round_only_the_newer_goes_live() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let mut serial = 0;
    for live in [true, false] {
        for newer_first in [true, false] {
            let maker_id = maker(conn, &format!("m{live}{newer_first}"))?;
            serial += 10;
            let (old, new) = (ask(serial, 100), bid(serial + 1, 100));
            let mut round = vec![sub(maker_id, 1, &old), sub(maker_id, 2, &new)];
            if newer_first {
                round.reverse();
            }
            let result = stp_round(conn, &round, live)?;
            let live_ids: Vec<_> = result
                .outcomes
                .iter()
                .filter_map(|(_, outcome)| outcome.live.as_ref().map(|order| order.id()))
                .collect();
            let expected_live = if live { vec![new.id()] } else { Vec::new() };
            assert_eq!(live_ids, expected_live);
            let mut events = vec![cancelled(&old)];
            if live {
                events.push(went_live(&new));
            }
            assert_eq!(postgres_test::statuses(conn, maker_id)?, events);
            assert_eq!(
                result.self_trade.updates,
                [MakerUpdate::LineageCancelled {
                    maker_id,
                    lineage_id: lineage_of(&old)
                }]
            );
            assert_eq!(lineage_state(conn, &old)?, "rejected");
        }
    }
    Ok(())
}

/// The check ignores expiry: an old order past its expiry is cancelled
/// like any other, and a submit with less than the settlement buffer left
/// still takes part. An order the matcher chose before a newer
/// crossing order arrived fails its reservation, and that is counted.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn expiry_plays_no_part_and_a_chosen_order_fails_its_reservation() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let expiring = |seq: u64, note: &Note, at: u64| sub_expiring(alpha, seq, note, at);
    let (expired, chosen) = (ask(1, 100), ask(2, 99));
    stp_round(conn, &[expiring(1, &expired, 1)], true)?;
    stp_round(conn, &[sub(alpha, 2, &chosen)], true)?;
    let soon = crate::types::now_millis() + 10_000;
    let new = bid(3, 100);
    // Less than the settlement buffer left: it still takes part.
    stp_round(conn, &[expiring(3, &new, soon)], true)?;
    assert_eq!(
        postgres_test::statuses(conn, alpha)?[2..4],
        [cancelled(&expired), cancelled(&chosen)]
    );
    // The matcher had chosen `chosen` before the bid arrived: its
    // reservation is refused, and counted because the input has a stop.
    // The counter is process-wide and only grows.
    let before = crate::executor::metrics::reservation_stopped_inputs();
    assert!(postgres_test::reserve(conn, 9, &[&chosen], 40).is_err());
    assert!(crate::executor::metrics::reservation_stopped_inputs() > before);
    assert!(super::postgres_db::has_stopped_input_tx(
        conn,
        &[id(&chosen)]
    )?);
    assert!(!super::postgres_db::has_stopped_input_tx(
        conn,
        &[id(&new)]
    )?);
    Ok(())
}

/// A loser held by a settlement hears nothing until that settlement
/// resolves; then its result says FULLY_FILLED, CANCELLED (the input, or the
/// remainder, stored stopped) or SPENT_ELSEWHERE.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_settling_loser_is_reported_by_its_settlement() -> Result<()> {
    use crate::gateway::proto::ResultingStatus::{Cancelled, FullyFilled, SpentElsewhere};
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (full, voided, spent) = (ask(1, 100), ask(2, 99), ask(3, 98));
    for (seq, note) in [(1, &full), (2, &voided), (3, &spent)] {
        stp_round(conn, &[sub(alpha, seq, note)], true)?;
    }
    let (input, _, _) = postgres_test::fill(1, &full, 100);
    postgres_test::prepare(conn, 1, &[input])?;
    postgres_test::reserve(conn, 2, &[&voided], 40)?;
    postgres_test::reserve(conn, 3, &[&spent], 40)?;
    let new = bid(4, 100);
    let round = stp_round(conn, &[sub(alpha, 10, &new)], true)?;
    assert_eq!(round.self_trade.updates.len(), 3);
    assert_eq!(
        postgres_test::statuses(conn, alpha)?[3..],
        [went_live(&new)],
        "nothing yet"
    );

    resolve(conn, 1, true, &[])?;
    resolve(conn, 2, false, &[])?;
    void_spent(conn, 3, &spent)?;
    assert_eq!(
        postgres_test::resolutions(conn, alpha)?,
        [
            (id(&full), FullyFilled, String::new()),
            (id(&voided), Cancelled, "self_trade_prevention".to_owned()),
            (id(&spent), SpentElsewhere, String::new()),
        ]
    );
    assert_eq!(
        postgres_test::status(conn, &voided)?.as_deref(),
        Some("stopped")
    );
    assert!(!in_book(conn, &voided)?);
    Ok(())
}

/// A round whose only loser is in a settlement appends no event (live at
/// accept off: its winner waits for its note), so it wakes no stream.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_round_whose_only_loser_is_settling_appends_nothing() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let old = ask(1, 100);
    stp_round(conn, &[sub(alpha, 1, &old)], true)?;
    postgres_test::reserve(conn, 1, &[&old], 40)?;
    let events = postgres_test::events(conn, alpha)?.len();
    let round = stp_round(conn, &[sub(alpha, 2, &bid(2, 100))], false)?;
    assert_eq!(round.self_trade.updates.len(), 1);
    assert!(!round.self_trade.appended);
    assert_eq!(postgres_test::events(conn, alpha)?.len(), events);
    Ok(())
}

/// The newer read covers only lineages above the round's lowest claim: an
/// exact retry claims nothing and reads nothing, and a late submit reads
/// only the lineages after it.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_newer_read_starts_at_the_rounds_lowest_claim() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    // Asks at seqs 10..=19, none crossing another.
    for seq in 10..20 {
        stp_round(conn, &[sub(alpha, seq, &ask(seq as u32, 100))], true)?;
    }
    let retry = stp_round(conn, &[sub(alpha, 12, &ask(12, 100))], true)?;
    assert!(retry.self_trade.newer_rows.is_empty());
    let late = stp_round(conn, &[sub(alpha, 15, &ask(99, 100))], true)?;
    // A conflict on seq 15: not claimed, nothing read.
    assert!(late.self_trade.newer_rows.is_empty());
    let late = stp_round(conn, &[sub(alpha, 165, &ask(98, 100))], true)?;
    assert!(late.self_trade.newer_rows.is_empty(), "165 is the newest");
    let late = stp_round(conn, &[sub(alpha, 1, &ask(97, 100))], true)?;
    assert_eq!(late.self_trade.newer_rows, [(alpha, 11)]);
    Ok(())
}

/// A round makes two reads per (maker, market) and a fixed number of
/// writes for its losers: its statements grow only with its own commands.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_rounds_cost_does_not_grow_with_its_losers() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let statements = Arc::new(AtomicUsize::new(0));
    let counted = statements.clone();
    conn.set_instrumentation(move |event: diesel::connection::InstrumentationEvent<'_>| {
        if matches!(
            event,
            diesel::connection::InstrumentationEvent::StartQuery { .. }
        ) {
            counted.fetch_add(1, Ordering::Relaxed);
        }
    });
    let cost = |conn: &mut PgConnection, round: &[(Header, MakerCommand)]| -> Result<usize> {
        let start = statements.load(Ordering::Relaxed);
        stp_round(conn, round, false)?;
        Ok(statements.load(Ordering::Relaxed) - start)
    };
    // One loser, then fifty, each lost to one ask.
    let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
    cost(conn, &[sub(alpha, 1, &bid(1, 200))])?;
    for seq in 1..=50 {
        cost(conn, &[sub(beta, seq, &bid(100 + seq as u32, 200))])?;
    }
    let one = cost(conn, &[sub(alpha, 100, &ask(2, 100))])?;
    let fifty = cost(conn, &[sub(beta, 100, &ask(3, 100))])?;
    assert_eq!(one, fifty, "losers cost the same in any number");
    // One submit, then two, then fifty, none crossing.
    let gamma = maker(conn, "gamma")?;
    let asks = |from: u64, count: u64| -> Vec<(Header, MakerCommand)> {
        (from..from + count)
            .map(|seq| sub(gamma, seq, &ask(1000 + seq as u32, 100)))
            .collect()
    };
    let one = cost(conn, &asks(1, 1))?;
    let two = cost(conn, &asks(10, 2))?;
    let fifty = cost(conn, &asks(100, 50))?;
    assert_eq!(fifty - one, 49 * (two - one));
    Ok(())
}

/// Equal prices cross. The same side, another maker and a user's order are
/// never touched.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn only_the_makers_own_opposite_crossing_orders_are_touched() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let (alpha, beta) = (maker(conn, "alpha")?, maker(conn, "beta")?);
    // The new order is a bid at 1.00. An older bid of the maker is on its
    // side; another maker's ask and a user's ask would cross it.
    let (equal, same_side, others) = (ask(1, 100), bid(2, 90), ask(3, 50));
    let user = order_of(4, Side::Sell, 100, 50, NoteType::Public);
    stp_round(conn, &[sub(alpha, 1, &equal)], true)?;
    stp_round(conn, &[sub(alpha, 2, &same_side)], true)?;
    stp_round(conn, &[sub(beta, 1, &others)], true)?;
    conn.transaction::<_, DbError, _>(|conn| {
        insert_orders_batch_tx(conn, &[NewOrderRow::parsed(&user, 1)?], 1)
    })?;
    let round = stp_round(conn, &[sub(alpha, 3, &bid(5, 100))], true)?;
    assert_eq!(
        round.self_trade.updates,
        [MakerUpdate::LineageCancelled {
            maker_id: alpha,
            lineage_id: lineage_of(&equal)
        }]
    );
    for note in [&same_side, &others, &user] {
        assert!(in_book(conn, note)?);
    }
    Ok(())
}

/// A maker's own cancel and a crossing submit racing for one order give one
/// stop and at most one event. A pending order stopped while a watcher
/// round that already read it is in flight is stored stopped, with no event.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn races_with_the_makers_cancel_and_the_watcher_stop_once() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    // The maker's cancel first: the submit finds the order stopped.
    let (first, second) = (ask(1, 100), ask(2, 100));
    stp_round(conn, &[sub(alpha, 1, &first)], true)?;
    stp_round(conn, &[cancel_cmd(alpha, 3, &first)], true)?;
    let round = stp_round(conn, &[sub(alpha, 4, &bid(3, 100))], true)?;
    assert!(round.self_trade.cancels.is_empty());
    assert_eq!(stop_of(conn, &first)?.unwrap().0, "maker");
    // The stop first: the maker's later cancel adds nothing.
    stp_round(conn, &[sub(alpha, 5, &second)], true)?;
    stp_round(conn, &[sub(alpha, 6, &bid(4, 100))], true)?;
    let events = postgres_test::statuses(conn, alpha)?.len();
    stp_round(conn, &[cancel_cmd(alpha, 7, &second)], true)?;
    assert_eq!(postgres_test::statuses(conn, alpha)?.len(), events);
    assert_eq!(stop_of(conn, &second)?.unwrap().0, "stp_on_submit");
    let cancels_of = |conn: &mut PgConnection, note: &Note| -> Result<usize> {
        Ok(postgres_test::statuses(conn, alpha)?
            .iter()
            .filter(|(id_, state, _)| *id_ == id(note) && *state == OrderState::Cancelled)
            .count())
    };
    assert_eq!(cancels_of(conn, &first)?, 0);
    assert_eq!(cancels_of(conn, &second)?, 1);

    // The watcher read a pending order, then a crossing submit stopped it.
    let beta = maker(conn, "beta")?;
    let pending = ask(10, 100);
    stp_round(conn, &[sub(beta, 1, &pending)], false)?;
    let read = maker_db::pending_submissions_tx(conn)?
        .into_iter()
        .find(|s| s.note.id() == pending.id())
        .unwrap();
    stp_round(conn, &[sub(beta, 2, &bid(11, 100))], false)?;
    let events = postgres_test::statuses(conn, beta)?.len();
    conn.transaction::<_, DbError, _>(|conn| {
        let activated = maker_db::activate_tx(conn, &[read], 1)?;
        super::postgres_db::live_book_update_tx(conn, activated.update)?;
        Ok(())
    })?;
    assert_eq!(
        postgres_test::status(conn, &pending)?.as_deref(),
        Some("stopped")
    );
    assert_eq!(postgres_test::statuses(conn, beta)?.len(), events);
    Ok(())
}

/// A watcher round that read an order before a crossing submit stopped it
/// adds nothing: the cap closing an order that went live at accept, and a
/// pending order whose note it found spent, both stay at the one CANCELLED.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_stale_watcher_read_adds_nothing_after_a_stop() -> Result<()> {
    use super::maker_db::Close;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let read = |conn: &mut PgConnection, note: &Note| -> Result<_> {
        Ok(maker_db::pending_submissions_tx(conn)?
            .into_iter()
            .find(|s| s.note.id() == note.id())
            .unwrap())
    };

    // The cap.
    let alpha = maker(conn, "alpha")?;
    let unconfirmed = ask(1, 100);
    stp_round(conn, &[sub(alpha, 1, &unconfirmed)], true)?;
    let stale = read(conn, &unconfirmed)?;
    stp_round(conn, &[sub(alpha, 2, &bid(2, 100))], true)?;
    let events = postgres_test::statuses(conn, alpha)?;
    conn.transaction::<_, DbError, _>(|conn| {
        maker_db::close_unconfirmed_tx(conn, [&stale], Close::NoteNotReceived)
    })?;
    assert_eq!(postgres_test::statuses(conn, alpha)?, events);
    assert_eq!(stop_of(conn, &unconfirmed)?.unwrap().0, "stp_on_submit");

    // The note found spent.
    let beta = maker(conn, "beta")?;
    let pending = ask(3, 100);
    stp_round(conn, &[sub(beta, 1, &pending)], false)?;
    let stale = read(conn, &pending)?;
    stp_round(conn, &[sub(beta, 2, &bid(4, 100))], false)?;
    let events = postgres_test::statuses(conn, beta)?;
    assert!(events.contains(&cancelled(&pending)));
    conn.transaction::<_, DbError, _>(|conn| maker_db::reject_tx(conn, &[stale]))?;
    assert_eq!(postgres_test::statuses(conn, beta)?, events);
    assert_eq!(lineage_state(conn, &pending)?, "rejected");
    Ok(())
}

/// The owner-epoch guard is taken with the check on and live at accept off.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_check_takes_the_owner_epoch_guard() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let gate = AcceptGate::epoch_only(postgres_test::epoch(conn)? + 1);
    let refused = conn.transaction::<_, DbError, _>(|conn| {
        intake_round_tx(conn, &[sub(alpha, 1, &ask(1, 100))], Some(&gate), true)
    });
    assert!(
        matches!(refused, Err(DbError::IntakeEpochMoved { .. })),
        "{refused:?}"
    );
    Ok(())
}

/// With the check off, crossing orders stay as before.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn with_the_check_off_nothing_changes() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (old, new) = (ask(1, 100), bid(2, 100));
    for (seq, note) in [(1, &old), (2, &new)] {
        let round = round(conn, &[sub(alpha, seq, note)], true, false)?;
        assert!(round.self_trade.updates.is_empty());
    }
    assert!(in_book(conn, &old)? && in_book(conn, &new)?);
    assert_eq!(
        postgres_test::statuses(conn, alpha)?,
        [went_live(&old), went_live(&new)]
    );
    Ok(())
}

/// Wait until session `pid` waits on a lock.
fn wait_for_lock_wait(url: &str, pid: i32) -> Result<()> {
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;
    let mut watch = super::postgres_migrations::connect(url)?;
    let started = std::time::Instant::now();
    while !diesel::select(sql::<Bool>(&format!(
        "EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {pid} AND wait_event_type = 'Lock')"
    )))
    .get_result::<bool>(&mut watch)?
    {
        anyhow::ensure!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "session {pid} never waited on a lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

/// A crossing submit and a reservation of the order it cancels serialize on
/// the maker's lock and never deadlock, in either order. Each order is run
/// on purpose: the first session writes, and commits only once the second
/// waits on its lock. Reservation first: the order settles and hears
/// nothing now. Submit first: the order gets CANCELLED and the reservation
/// fails.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_crossing_submit_and_a_reservation_of_its_loser_serialize() -> Result<()> {
    use super::postgres_migrations::connect;
    use super::postgres_models::SettlementAttemptRow;
    use diesel::dsl::sql;
    use diesel::sql_types::Integer;
    let mut fixture = TestSchema::migrated()?;
    let alpha = maker(&mut fixture.conn, "alpha")?;
    let epoch = postgres_test::epoch(&mut fixture.conn)?;
    for (k, reservation_first) in [(0_u8, true), (1, false), (2, true), (3, false)] {
        let (quote, crossing) = (ask(10 + u32::from(k), 100), bid(1000 + u32::from(k), 100));
        let seq = 10 * u64::from(k);
        stp_round(&mut fixture.conn, &[sub(alpha, seq + 1, &quote)], true)?;
        let (input, _, _) = postgres_test::fill(k, &quote, 40);
        let attempt = SettlementAttemptRow {
            tx_id: vec![k; 32],
            tx_result: vec![k],
            status: "prepared".into(),
        };
        let commands = [sub(alpha, seq + 2, &crossing)];
        let reserve = |conn: &mut PgConnection| {
            super::postgres_db::prepare_settlement_tx(
                conn,
                &attempt,
                std::slice::from_ref(&input),
                None,
            )
        };
        let submit = |conn: &mut PgConnection| {
            let gate = AcceptGate::new(epoch, || Some(BlockNumber::from(7)));
            intake_round_tx(conn, &commands, Some(&gate), true).map(|_| ())
        };
        let (mut leader, mut follower) = (connect(&fixture.url)?, connect(&fixture.url)?);
        let follower_pid: i32 =
            diesel::select(sql::<Integer>("pg_backend_pid()")).get_result(&mut follower)?;
        let url = fixture.url.clone();
        let (wrote, written) = std::sync::mpsc::channel();
        let second = std::thread::scope(|scope| -> Result<Result<(), DbError>> {
            let first = scope.spawn(|| {
                leader.transaction::<_, DbError, _>(|conn| {
                    if reservation_first {
                        reserve(conn)?;
                    } else {
                        submit(conn)?;
                    }
                    wrote.send(()).expect("the test waits");
                    wait_for_lock_wait(&url, follower_pid)
                        .map_err(|_| DbError::Corrupt("the second session never waited"))
                })
            });
            written.recv()?;
            let second = follower.transaction::<_, DbError, _>(|conn| {
                if reservation_first {
                    submit(conn)
                } else {
                    reserve(conn)
                }
            });
            first.join().expect("first session panicked")?;
            Ok(second)
        })?;
        let conn = &mut fixture.conn;
        let told = postgres_test::statuses(conn, alpha)?.contains(&cancelled(&quote));
        assert_eq!(
            stop_of(conn, &quote)?.unwrap().0,
            "stp_on_submit",
            "round {k}"
        );
        if reservation_first {
            second?;
            assert!(!told, "round {k}: a settling loser hears nothing now");
            assert_eq!(
                postgres_test::status(conn, &quote)?.as_deref(),
                Some("settling")
            );
        } else {
            assert!(
                matches!(
                    second,
                    Err(DbError::Settlement(
                        crate::types::SettlementError::InputOrderNotActive
                    ))
                ),
                "round {k}: {second:?}"
            );
            assert!(told, "round {k}: a live loser hears CANCELLED");
        }
    }
    Ok(())
}

/// The newer read starts at the round's lowest claim, not its highest: a
/// late submit is compared with a newer order even when the same round
/// also claims a higher seq.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_late_submit_beside_a_higher_one_still_sees_the_newer_order() -> Result<()> {
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let (newer, late, unrelated) = (ask(1, 100), bid(2, 101), bid(3, 50));
    stp_round(conn, &[sub(alpha, 7, &newer)], true)?;
    stp_round(conn, &[cancel_cmd(alpha, 8, &newer)], true)?;
    let round = stp_round(
        conn,
        &[sub(alpha, 5, &late), sub(alpha, 9, &unrelated)],
        true,
    )?;
    assert!(round.outcomes[0].1.live.is_none(), "the late bid lost");
    assert!(round.outcomes[1].1.live.is_some());
    assert_eq!(
        postgres_test::statuses(conn, alpha)?.last(),
        Some(&went_live(&unrelated))
    );
    assert!(postgres_test::statuses(conn, alpha)?.contains(&cancelled(&late)));
    // Newer than 5: the cancelled ask (7); the round's own 9 is not counted.
    assert_eq!(round.self_trade.newer_rows, [(alpha, 1)]);
    Ok(())
}

/// With the check on, a submit with more than 1,000 of its maker's orders
/// on the pair above its seq is refused before it is claimed, the same on
/// every retry; 1,000 is fine, another pair's history does not count, an
/// earlier accepted submit still replays, and with the check off nothing is
/// refused.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn a_submit_far_below_its_makers_history_is_refused() -> Result<()> {
    use crate::maker::{CommandReply, CommandResult};
    use maker_db::MAX_NEWER_LINEAGES;
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    let alpha = maker(conn, "alpha")?;
    let keys = OrderKeys::from_note(&ask(1, 100))?;
    let reply = |round: IntakeRound| round.outcomes.into_iter().next().unwrap().1.reply;

    let early = ask(1, 100);
    assert_eq!(
        reply(stp_round(conn, &[sub(alpha, 1, &early)], false)?),
        CommandReply::Committed(CommandResult::Accepted)
    );
    // Another pair's history does not count.
    history(
        conn,
        alpha,
        b"other pair",
        &keys.direction,
        2_002,
        MAX_NEWER_LINEAGES + 1,
    )?;
    let elsewhere = ask(2, 100);
    assert_eq!(
        reply(stp_round(conn, &[sub(alpha, 3, &elsewhere)], false)?),
        CommandReply::Committed(CommandResult::Accepted)
    );

    // 1,001 orders on this pair at seqs 10,002..12,002.
    history(
        conn,
        alpha,
        &keys.market,
        &keys.direction,
        10_002,
        MAX_NEWER_LINEAGES + 1,
    )?;
    let (refused, kept) = (ask(3, 100), ask(4, 100));
    // Stored: the retry replays the refusal, never recomputes it.
    for expected in [
        CommandReply::Committed(CommandResult::SeqBelowHistory),
        CommandReply::Replayed(CommandResult::SeqBelowHistory),
    ] {
        let outcome = stp_round(conn, &[sub(alpha, 10_001, &refused)], false)?;
        assert_eq!(reply(outcome), expected);
    }
    let claims: i64 = maker_lineages::table
        .find(lineage_of(&refused))
        .count()
        .get_result(conn)?;
    assert_eq!(claims, 0, "a refused submit claims nothing");
    // Exactly 1,000 above: claimed.
    assert_eq!(
        reply(stp_round(conn, &[sub(alpha, 10_003, &kept)], false)?),
        CommandReply::Committed(CommandResult::Accepted)
    );
    // An exact retry of the early submit still gets its stored reply.
    assert_eq!(
        reply(stp_round(conn, &[sub(alpha, 1, &early)], false)?),
        CommandReply::Replayed(CommandResult::Accepted)
    );
    // With the check off, nothing is refused.
    let off = round(conn, &[sub(alpha, 9_999, &ask(5, 100))], false, false)?;
    assert_eq!(reply(off), CommandReply::Committed(CommandResult::Accepted));
    Ok(())
}

/// The runbook's soak check 1, run as the runbook prints it: it finds each
/// crossed pair of a maker's open orders, a parent retired while its
/// settlement is unresolved included, and none the rule prevented.
#[test]
#[ignore = "requires SOLVER_TEST_DATABASE_URL"]
fn the_runbooks_real_miss_check_finds_crossed_pairs() -> Result<()> {
    use super::postgres_db::mark_orders_onchain_nullified_tx;
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        misses: i64,
    }
    let runbook = include_str!("../../../../docs/postgres-runbook.md");
    let start = runbook
        .find("WITH open_lineages AS (")
        .expect("check 1 in the runbook");
    let end = start + runbook[start..].find(">= :switch_on;").unwrap() + ">= :switch_on".len();
    let check = &runbook[start..end];
    let misses = |conn: &mut PgConnection, switch_on: &str| -> Result<i64> {
        let check = check.replace(":switch_on", &format!("'{switch_on}'::timestamptz"));
        Ok(
            diesel::sql_query(format!("SELECT count(*) AS misses FROM ({check}) found"))
                .get_result::<Count>(conn)?
                .misses,
        )
    };
    let mut fixture = TestSchema::migrated()?;
    let conn = &mut fixture.conn;
    // With the check off: a crossed pair of live orders.
    let alpha = maker(conn, "alpha")?;
    round(conn, &[sub(alpha, 1, &ask(1, 100))], true, false)?;
    round(conn, &[sub(alpha, 2, &bid(2, 101))], true, false)?;
    assert_eq!(misses(conn, "-infinity")?, 1);
    assert_eq!(misses(conn, "infinity")?, 0, "both from before switch-on");
    // And one whose ask is a parent the watcher retired while its
    // settlement is unresolved.
    let beta = maker(conn, "beta")?;
    let retired = ask(3, 100);
    round(conn, &[sub(beta, 1, &retired)], true, false)?;
    postgres_test::reserve(conn, 9, &[&retired], 40)?;
    conn.transaction::<_, DbError, _>(|conn| {
        mark_orders_onchain_nullified_tx(conn, &[id(&retired)])
    })?;
    round(conn, &[sub(beta, 2, &bid(4, 101))], true, false)?;
    assert_eq!(misses(conn, "-infinity")?, 2);
    // With the check on, the rule leaves no crossed pair.
    let gamma = maker(conn, "gamma")?;
    stp_round(conn, &[sub(gamma, 1, &ask(5, 100))], true)?;
    stp_round(conn, &[sub(gamma, 2, &bid(6, 101))], true)?;
    assert_eq!(misses(conn, "-infinity")?, 2);
    Ok(())
}

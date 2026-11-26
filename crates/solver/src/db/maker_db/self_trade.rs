//! Self-trade prevention at submit (ADR 0009): the database side of one
//! intake round. After the round's cancels and claims, read the maker's
//! orders on each pair its submits name, let [`decide`] pick the losers,
//! stop them, then let the winners go live at accept in arrival order.
//!
//! The round already holds every maker lock it touches
//! (`prelock_command_makers_tx`), and this step writes only stop rows,
//! lineage states and events, never an order row; only a winner's go-live
//! inserts its own new row, as without the check.

use std::collections::{BTreeMap, HashMap, HashSet};

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Binary, Nullable, Text};
use prost::Message as _;

use miden_protocol::crypto::utils::Deserializable;
use miden_protocol::note::Note;

use super::{
    append_events_tx, go_live_at_accept_tx, order_status, settle_lineages, status_body,
    stop_lineages_tx, AcceptFallback, AcceptMode, EventKind, LineageState, NewEvent, Stop,
    StopSource,
};
use crate::db::error::DbResult;
use crate::db::postgres_schema::maker_stops;
use crate::gateway::proto;
use crate::gateway::self_trade::{decide, Lineage};
use crate::maker::{CommandHeader, LineageId, MakerId, MakerUpdate};
use crate::types::{BookOrder, OrderKeys, OrderStatus};

/// A submit this round claimed, in arrival order.
#[derive(Clone, Copy)]
pub struct Submitted<'a> {
    /// Its position among the round's commands.
    pub index: usize,
    pub header: &'a CommandHeader,
    pub note: &'a Note,
    pub keys: &'a OrderKeys,
    /// The submitted (offered, requested) amounts.
    pub amounts: (u64, u64),
}

/// Why a claimed submit was not open when the round read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum DeadCause {
    /// Its maker's `CancelOrder` came first.
    Cancelled,
    /// A `CancelAll` of its maker covers it.
    CutOff,
}

/// An order the step cancelled, for `/metrics`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StpCancel {
    pub maker_id: MakerId,
    /// One of the round's own submits, which lost to a newer crossing order.
    pub entering: bool,
}

/// What the self-trade step of one round did.
#[derive(Debug, Default)]
pub struct SelfTradeRound {
    /// Winners that went live at accept, by command index.
    pub live: Vec<(usize, BookOrder)>,
    /// Winners that could have gone live at accept and did not.
    pub fallbacks: Vec<(usize, AcceptFallback)>,
    /// `LineageCancelled` for every loser, for the matcher.
    pub updates: Vec<MakerUpdate>,
    /// Whether the step appended events (its CANCELLED and the winners'
    /// LIVE), so the streams are woken after commit.
    pub appended: bool,
    /// Every loser this round stopped.
    pub cancels: Vec<StpCancel>,
    /// Claimed submits that were not open, by cause.
    pub dead_on_arrival: Vec<(MakerId, DeadCause)>,
    /// Newer lineages read beyond the round's own claims, per maker: a
    /// maker whose seq counter went backwards makes each round read its
    /// history.
    pub newer_rows: Vec<(MakerId, u64)>,
}

/// One open maker order, from the round's read.
#[derive(QueryableByName, Debug)]
struct OpenRow {
    #[diesel(embed)]
    lineage: NewerRow,
    /// The order row's note, or the submitted note while none exists.
    #[diesel(sql_type = Binary)]
    note_id: Vec<u8>,
    #[diesel(sql_type = Nullable<BigInt>)]
    depth: Option<i64>,
    /// The row's status, `settling` for a parent retired while its
    /// settlement is unresolved, or `pending` while no row exists.
    #[diesel(sql_type = Text)]
    status: String,
}

/// One lineage newer than the round's lowest claim, in any state.
#[derive(QueryableByName, Debug)]
struct NewerRow {
    #[diesel(sql_type = Binary)]
    lineage_id: Vec<u8>,
    #[diesel(sql_type = Nullable<Binary>)]
    direction: Option<Vec<u8>>,
    #[diesel(sql_type = BigInt)]
    root_seq: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    offered_amount: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    requested_amount: Option<i64>,
}

impl NewerRow {
    /// The lineage as the rule sees it; `None` when it was claimed before
    /// amounts were stored, so it never crosses.
    fn lineage(&self, maker_id: MakerId, market: &[u8]) -> DbResult<Option<Lineage>> {
        let (Some(direction), Some(offered), Some(requested)) =
            (&self.direction, self.offered_amount, self.requested_amount)
        else {
            return Ok(None);
        };
        Ok(Some(Lineage {
            lineage_id: self.lineage_id.clone(),
            maker_id,
            market: market.to_vec(),
            direction: direction.clone(),
            root_seq: u64::try_from(self.root_seq)?,
            offered: u64::try_from(offered)?,
            requested: u64::try_from(requested)?,
        }))
    }
}

/// The maker's open orders on `market`: rows active, settling or waiting; a
/// parent the watcher retired while its settlement is unresolved; and
/// accepted lineages without a row yet, which include this round's claims.
/// The maker's stops and cutoffs are applied (`eligible_lineages`). No
/// expiry filter: expiry is the matcher's, and an order chosen before its
/// cutoff may still be reserved.
const OPEN: &str = "\
SELECT l.lineage_id, l.direction, l.root_seq, l.offered_amount, l.requested_amount,
       o.note_id, o.depth, o.status
FROM orders o
JOIN eligible_lineages l ON l.lineage_id = o.lineage_id
WHERE o.maker_id = $1 AND o.market = $2
  AND o.status IN ('active', 'settling', 'waiting')
UNION ALL
SELECT l.lineage_id, l.direction, l.root_seq, l.offered_amount, l.requested_amount,
       o.note_id, o.depth, 'settling'
FROM settlement_inputs si
JOIN orders o ON o.note_id = si.parent_note_id
JOIN eligible_lineages l ON l.lineage_id = o.lineage_id
WHERE l.maker_id = $1 AND l.market = $2
  AND o.status NOT IN ('active', 'settling', 'waiting')
UNION ALL
SELECT l.lineage_id, l.direction, l.root_seq, l.offered_amount, l.requested_amount,
       l.note_id, 0, 'pending'
FROM eligible_lineages l
WHERE l.maker_id = $1 AND l.market = $2
  AND l.state = 'pending' AND l.accept_block IS NULL";

/// Every lineage of the maker on `market` above the round's lowest claimed
/// seq, in any state: the table, not the view, so a cancelled one counts.
const NEWER: &str = "\
SELECT lineage_id, direction, root_seq, offered_amount, requested_amount
FROM maker_lineages
WHERE maker_id = $1 AND market = $2 AND root_seq > $3";

/// The most of a maker's orders on a pair a new submit may sit below. A
/// submit with more above it is refused before it is claimed, so a round's
/// newer read stays small whatever the maker's history. A maker that takes
/// its seqs from one rising counter only ever sits below the few orders it
/// has in flight, so 1,000 is far above any honest gap, and still a small
/// read.
pub const MAX_NEWER_LINEAGES: i64 = 1_000;

/// Whether more than [`MAX_NEWER_LINEAGES`] of `maker_id`'s lineages on
/// `market` have a seq above `seq`. Reads at most one more than the limit,
/// along `maker_lineages_seq_idx`.
pub(in crate::db) fn far_below_history_tx(
    conn: &mut PgConnection,
    maker_id: MakerId,
    market: &[u8],
    seq: u64,
) -> DbResult<bool> {
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        newer: i64,
    }
    let count: Count = diesel::sql_query(
        "SELECT count(*) AS newer FROM (
             SELECT 1 FROM maker_lineages
             WHERE maker_id = $1 AND market = $2 AND root_seq > $3
             LIMIT $4) above",
    )
    .bind::<BigInt, _>(maker_id)
    .bind::<Binary, _>(market)
    .bind::<BigInt, _>(i64::try_from(seq)?)
    .bind::<BigInt, _>(MAX_NEWER_LINEAGES + 1)
    .get_result(conn)?;
    Ok(count.newer > MAX_NEWER_LINEAGES)
}

/// Where a loser stood when the round read it.
enum Standing {
    /// Accepted, no order row yet: told about its submitted note.
    Pending,
    /// An order row the maker sees live: its note and depth.
    Shown(Vec<u8>, u32),
    /// Held by a settlement: that settlement's result reports it.
    Settling,
}

/// Run the self-trade step of an intake round whose `submitted` claims are
/// written (ADR 0009); then put the winners live at accept unless `accept`
/// is off.
///
/// Its statements do not grow with its losers (below [`INSERT_CHUNK_ROWS`]
/// of them): two reads per (maker, market), one stop-cause read if a submit
/// was dead, one stop insert per [`INSERT_CHUNK_ROWS`] losers, one lineage
/// update, at most one note read, two per maker with events, then each
/// winner's go-live as without the check. The door check runs per submit
/// before this, in its claim.
///
/// [`INSERT_CHUNK_ROWS`]: crate::db::postgres_db::INSERT_CHUNK_ROWS
pub fn self_trade_round_tx(
    conn: &mut PgConnection,
    submitted: &[Submitted],
    accept: AcceptMode,
) -> DbResult<SelfTradeRound> {
    let mut round = SelfTradeRound::default();
    if submitted.is_empty() {
        return Ok(round);
    }
    let mut claimed = Vec::with_capacity(submitted.len());
    let mut lowest: BTreeMap<(MakerId, &[u8]), i64> = BTreeMap::new();
    for submit in submitted {
        let maker_id = submit.header.maker_id;
        let seq = i64::try_from(submit.header.seq)?;
        let (offered, requested) = submit.amounts;
        claimed.push(Lineage {
            lineage_id: submit.keys.lineage_id.clone(),
            maker_id,
            market: submit.keys.market.clone(),
            direction: submit.keys.direction.clone(),
            root_seq: submit.header.seq,
            offered,
            requested,
        });
        let group = lowest.entry((maker_id, &submit.keys.market)).or_insert(seq);
        *group = (*group).min(seq);
    }
    let claimed_ids: HashSet<&LineageId> = claimed.iter().map(|l| &l.lineage_id).collect();

    // The read: two statements per (maker, market), plain parameters.
    let mut known: HashMap<LineageId, Lineage> = HashMap::new();
    let mut open: HashMap<LineageId, OpenRow> = HashMap::new();
    for (&(maker_id, market), &seq) in &lowest {
        let rows: Vec<OpenRow> = diesel::sql_query(OPEN)
            .bind::<BigInt, _>(maker_id)
            .bind::<Binary, _>(market)
            .load(conn)?;
        for row in rows {
            let Some(found) = row.lineage.lineage(maker_id, market)? else {
                continue;
            };
            known.insert(found.lineage_id.clone(), found);
            // A lineage can have two open rows (a parent settling and a
            // remainder stored early): keep the settling one, so its
            // settlement reports the loss.
            let lineage_id = row.lineage.lineage_id.clone();
            if row.status == OrderStatus::Settling.as_str() || !open.contains_key(&lineage_id) {
                open.insert(lineage_id, row);
            }
        }
        let newer: Vec<NewerRow> = diesel::sql_query(NEWER)
            .bind::<BigInt, _>(maker_id)
            .bind::<Binary, _>(market)
            .bind::<BigInt, _>(seq)
            .load(conn)?;
        let beyond = newer
            .iter()
            .filter(|row| !claimed_ids.contains(&row.lineage_id))
            .count() as u64;
        if beyond > 0 {
            round.newer_rows.push((maker_id, beyond));
        }
        for row in newer {
            if let Some(found) = row.lineage(maker_id, market)? {
                known.entry(found.lineage_id.clone()).or_insert(found);
            }
        }
    }

    let open_ids: HashSet<LineageId> = open.keys().cloned().collect();
    let known: Vec<Lineage> = known.into_values().collect();
    let decision = decide(&claimed, &known, &open_ids);

    // Claimed submits that were not open: cancelled or cut off first.
    let dead: Vec<&Lineage> = claimed
        .iter()
        .filter(|l| !open_ids.contains(&l.lineage_id))
        .collect();
    let dead_ids: Vec<&LineageId> = dead.iter().map(|l| &l.lineage_id).collect();
    if !dead.is_empty() {
        let makers: Vec<MakerId> = dead.iter().map(|l| l.maker_id).collect();
        // Both key columns, so the primary key serves it.
        let stopped: HashSet<(MakerId, Vec<u8>)> = maker_stops::table
            .filter(maker_stops::maker_id.eq_any(&makers))
            .filter(maker_stops::lineage_id.eq_any(&dead_ids))
            .select((maker_stops::maker_id, maker_stops::lineage_id))
            .load::<(MakerId, Vec<u8>)>(conn)?
            .into_iter()
            .collect();
        for lineage in dead {
            let cause = if stopped.contains(&(lineage.maker_id, lineage.lineage_id.clone())) {
                DeadCause::Cancelled
            } else {
                DeadCause::CutOff
            };
            round.dead_on_arrival.push((lineage.maker_id, cause));
        }
    }

    // The losers: stop rows, then lineage states, then events.
    let stops: Vec<Stop> = decision
        .losers
        .iter()
        .map(|(lineage_id, loss)| Stop {
            maker_id: loss.maker_id,
            lineage_id,
            source: StopSource::StpOnSubmit,
            caused_by: Some(&loss.caused_by),
        })
        .collect();
    // Every loser was open when read (`eligible_lineages`: no stop row),
    // under the round's maker locks, so no stop of anyone else's can land in
    // between: all of these are new.
    let stopped = stop_lineages_tx(conn, &stops)?;
    debug_assert_eq!(stopped.len(), stops.len(), "every self-trade stop is new");
    round.updates = decision
        .losers
        .iter()
        .map(|(lineage_id, loss)| MakerUpdate::LineageCancelled {
            maker_id: loss.maker_id,
            lineage_id: lineage_id.clone(),
        })
        .collect();
    let mut newly: Vec<(&LineageId, MakerId, i64, Standing)> = Vec::new();
    for (lineage_id, loss) in &decision.losers {
        let row = &open[lineage_id];
        let standing = match row.status.as_str() {
            "pending" => Standing::Pending,
            status if status == OrderStatus::Settling.as_str() => Standing::Settling,
            _ => Standing::Shown(row.note_id.clone(), u32::try_from(row.depth.unwrap_or(0))?),
        };
        round.cancels.push(StpCancel {
            maker_id: loss.maker_id,
            entering: claimed_ids.contains(lineage_id),
        });
        newly.push((lineage_id, loss.maker_id, row.lineage.root_seq, standing));
    }
    let pending: Vec<&LineageId> = newly
        .iter()
        .filter(|(.., standing)| matches!(standing, Standing::Pending))
        .map(|(lineage_id, ..)| *lineage_id)
        .collect();
    // Closed: pending losers, and dead submits, so the watcher never tells
    // the maker of either again (a dead submit gets no event of its own).
    let closing: Vec<&LineageId> = pending
        .iter()
        .copied()
        .chain(dead_ids.iter().copied())
        .collect();
    settle_lineages(conn, &closing, LineageState::Rejected)?;
    let notes = submitted_notes(conn, submitted, &pending)?;
    // In seq order per maker, whatever order the lineage IDs sort in.
    newly.sort_by_key(|(lineage_id, maker_id, seq, _)| (*maker_id, *seq, *lineage_id));
    let mut events: BTreeMap<MakerId, Vec<NewEvent>> = BTreeMap::new();
    for (lineage_id, maker_id, _, standing) in &newly {
        let (state, reason) = (
            proto::OrderState::Cancelled,
            StopSource::StpOnSubmit.reason(),
        );
        let body = match standing {
            Standing::Pending => order_status(&notes[*lineage_id], state, reason),
            Standing::Shown(note_id, depth) => status_body(note_id.clone(), *depth, state, reason),
            Standing::Settling => continue,
        };
        events.entry(*maker_id).or_default().push((
            Some(lineage_id.as_slice()),
            EventKind::OrderStatus,
            body.encode_to_vec(),
        ));
    }
    for (maker_id, events) in &events {
        append_events_tx(conn, *maker_id, events)?;
        round.appended = true;
    }

    // Then the winners go live, in arrival order.
    let by_lineage: HashMap<&LineageId, &Submitted> = submitted
        .iter()
        .map(|submit| (&submit.keys.lineage_id, submit))
        .collect();
    for winner in &decision.winners {
        let submit = by_lineage[winner];
        match accept {
            AcceptMode::Off => {}
            AcceptMode::Stale => {
                round
                    .fallbacks
                    .push((submit.index, AcceptFallback::WatcherStale));
            }
            AcceptMode::Fresh(block) => {
                let live = go_live_at_accept_tx(
                    conn,
                    block,
                    submit.header.maker_id,
                    &submit.keys.lineage_id,
                    submit.note,
                )?;
                if let Some(order) = live {
                    round.appended = true;
                    round.live.push((submit.index, order));
                }
            }
        }
    }
    Ok(round)
}

/// The submitted note of each of `pending`: the round's own submits carry
/// it, the others' come from the submit the maker stored.
fn submitted_notes(
    conn: &mut PgConnection,
    submitted: &[Submitted],
    pending: &[&LineageId],
) -> DbResult<HashMap<LineageId, Note>> {
    let mut notes: HashMap<LineageId, Note> = submitted
        .iter()
        .filter(|submit| pending.contains(&&submit.keys.lineage_id))
        .map(|submit| (submit.keys.lineage_id.clone(), submit.note.clone()))
        .collect();
    let stored: Vec<&LineageId> = pending
        .iter()
        .copied()
        .filter(|lineage_id| !notes.contains_key(*lineage_id))
        .collect();
    if stored.is_empty() {
        return Ok(notes);
    }
    #[derive(QueryableByName)]
    struct Payload {
        #[diesel(sql_type = Binary)]
        lineage_id: Vec<u8>,
        #[diesel(sql_type = Binary)]
        payload: Vec<u8>,
    }
    let rows: Vec<Payload> = diesel::sql_query(
        "SELECT l.lineage_id, c.payload
         FROM maker_lineages l
         JOIN maker_commands c ON c.maker_id = l.maker_id AND c.request_id = l.request_id
         WHERE l.lineage_id = ANY($1)",
    )
    .bind::<diesel::sql_types::Array<Binary>, _>(&stored)
    .load(conn)?;
    for row in rows {
        notes.insert(row.lineage_id, Note::read_from_bytes(&row.payload)?);
    }
    Ok(notes)
}

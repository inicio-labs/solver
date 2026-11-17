//! Per-maker counters for orders that go live at accept (ADR 0008), for
//! `/metrics`. Makers are few, so every counter is labelled by maker ID. Each
//! is counted after the transaction that did it commits.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use crate::db::maker_db::AcceptFallback;
use crate::maker::MakerId;

/// Orders that went live at accept.
pub(crate) const LIVE_AT_ACCEPT: &str = "solver_maker_live_at_accept_total";
/// Unconfirmed orders a node refusal named as unknown notes.
pub(crate) const UNKNOWN_NOTES_NAMED: &str = "solver_maker_unknown_notes_named_total";
/// Unconfirmed orders a failed settlement sent to `waiting`.
pub(crate) const SENT_TO_WAITING: &str = "solver_maker_sent_to_waiting_total";
/// Orders back in the book from `waiting` once their note landed.
pub(crate) const WAITING_RETURNED: &str = "solver_maker_waiting_returned_total";
/// Orders the cap cancelled: `note_not_received`.
pub(crate) const CAP_CANCELS: &str = "solver_maker_cap_cancels_total";
const ACCEPT_FALLBACKS: &str = "solver_maker_accept_fallbacks_total";
const UNCONFIRMED_ACTIVATIONS: &str = "solver_maker_unconfirmed_activations_total";

type Key = (&'static str, MakerId, &'static str, &'static str);

static COUNTS: LazyLock<Mutex<BTreeMap<Key, u64>>> = LazyLock::new(Mutex::default);

/// Service-wide gateway numbers (no maker label): a reconnect herd shows as a
/// full intake queue, intake-full replies and a jump in open streams.
static INTAKE_DEPTH: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static INTAKE_FULL: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static OPEN_STREAMS: AtomicU64 = AtomicU64::new(0);

/// The two intake queues.
#[derive(Clone, Copy)]
pub(crate) enum Queue {
    Submit = 0,
    Cancel = 1,
}

const QUEUE_NAMES: [&str; 2] = ["submit", "cancel"];

/// Commands waiting in each intake queue, as the intake writer last saw them.
pub(crate) fn record_intake_depth(submits: usize, cancels: usize) {
    INTAKE_DEPTH[Queue::Submit as usize].store(submits as u64, Ordering::Relaxed);
    INTAKE_DEPTH[Queue::Cancel as usize].store(cancels as u64, Ordering::Relaxed);
}

/// A command refused at once because its intake queue was full (the maker
/// gets UNAVAILABLE "intake is full").
pub(crate) fn count_intake_full(queue: Queue) {
    INTAKE_FULL[queue as usize].fetch_add(1, Ordering::Relaxed);
}

/// Held by each running event stream; counts it open until dropped.
pub(crate) struct OpenStream(());

impl OpenStream {
    pub(crate) fn open() -> Self {
        OPEN_STREAMS.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        OPEN_STREAMS.fetch_sub(1, Ordering::Relaxed);
    }
}

fn render_service_wide(body: &mut String) {
    for (index, queue) in QUEUE_NAMES.iter().enumerate() {
        let _ = writeln!(
            body,
            "solver_maker_intake_queue_depth{{queue=\"{queue}\"}} {}\n\
             solver_maker_intake_full_total{{queue=\"{queue}\"}} {}",
            INTAKE_DEPTH[index].load(Ordering::Relaxed),
            INTAKE_FULL[index].load(Ordering::Relaxed),
        );
    }
    let _ = writeln!(
        body,
        "solver_maker_open_streams {}",
        OPEN_STREAMS.load(Ordering::Relaxed)
    );
}

fn add(metric: &'static str, maker_id: MakerId, label: (&'static str, &'static str)) {
    *COUNTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry((metric, maker_id, label.0, label.1))
        .or_default() += 1;
}

pub(crate) fn count(metric: &'static str, maker_id: MakerId) {
    add(metric, maker_id, ("", ""));
}

/// A private submit that took the confirmed path although the switch is on.
pub(crate) fn count_fallback(maker_id: MakerId, reason: AcceptFallback) {
    add(ACCEPT_FALLBACKS, maker_id, ("reason", reason.into()));
}

/// An unconfirmed order whose note was found: `via` the watcher's round,
/// its exact `lookup`, or our own `settlement`.
pub(crate) fn count_activation(maker_id: MakerId, via: &'static str) {
    add(UNCONFIRMED_ACTIVATIONS, maker_id, ("via", via));
}

pub(crate) fn render(body: &mut String) {
    render_service_wide(body);
    let counts = COUNTS.lock().unwrap_or_else(|e| e.into_inner());
    for ((metric, maker_id, label, value), count) in counts.iter() {
        let _ = if label.is_empty() {
            writeln!(body, "{metric}{{maker_id=\"{maker_id}\"}} {count}")
        } else {
            writeln!(
                body,
                "{metric}{{maker_id=\"{maker_id}\",{label}=\"{value}\"}} {count}"
            )
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_wide_numbers_parse_and_move() {
        let open = OpenStream::open();
        count_intake_full(Queue::Submit);
        record_intake_depth(7, 1);
        let mut body = String::new();
        render_service_wide(&mut body);
        assert_eq!(crate::obs::exposition_errors(&body), Vec::<String>::new());
        assert!(
            body.contains("solver_maker_intake_queue_depth{queue=\"submit\"} 7\n"),
            "{body}"
        );
        assert!(
            body.contains("solver_maker_intake_queue_depth{queue=\"cancel\"} 1\n"),
            "{body}"
        );
        assert!(
            body.contains("solver_maker_intake_full_total{queue=\"submit\"} "),
            "{body}"
        );
        let streams = |body: &str| -> u64 {
            body.lines()
                .find_map(|l| l.strip_prefix("solver_maker_open_streams "))
                .unwrap()
                .parse()
                .unwrap()
        };
        let with = streams(&body);
        drop(open);
        let mut after = String::new();
        render_service_wide(&mut after);
        assert_eq!(streams(&after) + 1, with);
    }

    #[test]
    fn counters_render_per_maker_and_label() {
        // Counters are process-wide: use a maker ID no other test uses.
        let maker = 9_000_001;
        count(CAP_CANCELS, maker);
        count(CAP_CANCELS, maker);
        count_fallback(maker, AcceptFallback::WatcherStale);
        count_activation(maker, "lookup");
        let mut body = String::new();
        render(&mut body);
        for line in [
            "solver_maker_cap_cancels_total{maker_id=\"9000001\"} 2",
            "solver_maker_accept_fallbacks_total{maker_id=\"9000001\",reason=\"watcher_stale\"} 1",
            "solver_maker_unconfirmed_activations_total{maker_id=\"9000001\",via=\"lookup\"} 1",
        ] {
            assert!(body.lines().any(|l| l == line), "{line} missing:\n{body}");
        }
    }
}

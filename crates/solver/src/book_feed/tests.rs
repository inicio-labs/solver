use miden_protocol::note::NoteId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};

use super::*;

/// A matcher-side feed and the receiving end of its batches.
fn feed() -> (BookFeed, mpsc::UnboundedReceiver<BookBatch>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (BookFeed::new(tx), rx)
}

fn note(seed: u64) -> NoteId {
    NoteId::try_from_hex(&format!("0x{seed:064x}")).unwrap()
}

fn pair() -> Pair {
    (
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap(),
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap(),
    )
}

/// An order offering `offered` for `requested` on [`pair`].
fn entry(seed: u64, requested: u64, offered: u64) -> BookEntry {
    BookEntry {
        id: note(seed),
        pair: pair(),
        rate: RateKey::new(requested, offered),
        priority: seed,
        min_fill_step: 0,
    }
}

fn levels(copy: &BookCopy) -> Vec<(Amount, Amount, Amount)> {
    copy.snapshot()
        .get(&pair())
        .map(|levels| {
            levels
                .iter()
                .map(|level| (level.rate.requested, level.rate.offered, level.volume))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn levels_merge_equal_rates_and_drop_when_empty() {
    let mut copy = BookCopy::with_levels();
    copy.insert(entry(1, 200, 100)); // 2 B per A
    copy.insert(entry(2, 100, 50)); // the same rate
    copy.insert(entry(3, 7, 7)); // better
    assert_eq!(levels(&copy), [(7, 7, 7), (200, 100, 150)]);

    copy.remove(&note(1));
    assert_eq!(levels(&copy), [(7, 7, 7), (200, 100, 50)]);
    copy.remove(&note(2));
    copy.remove(&note(3));
    assert!(copy.snapshot().is_empty());
    assert_eq!(copy.digest(pair()), 0, "an empty pair has no digest");
}

#[test]
fn the_digest_does_not_depend_on_arrival_order() {
    let mut one = BookCopy::default();
    one.insert(entry(1, 10, 5));
    one.insert(entry(2, 30, 10));
    let mut two = BookCopy::default();
    two.insert(entry(2, 30, 10));
    two.insert(entry(1, 10, 5));
    assert_eq!(one.digest(pair()), two.digest(pair()));
    assert_ne!(one.digest(pair()), 0);
}

#[test]
fn every_field_moves_the_digest() {
    let base = entry(2, 30, 10);
    let (offered, requested) = base.pair;
    let changed = [
        BookEntry {
            id: note(3),
            ..base
        },
        BookEntry {
            pair: (requested, offered),
            ..base
        },
        // The same ratio with other amounts: the copy keeps raw amounts.
        BookEntry {
            rate: RateKey::new(60, 20),
            ..base
        },
        BookEntry {
            rate: RateKey::new(31, 10),
            ..base
        },
        BookEntry {
            rate: RateKey::new(30, 11),
            ..base
        },
        BookEntry {
            priority: 3,
            ..base
        },
        BookEntry {
            min_fill_step: 1,
            ..base
        },
    ];
    let digest = |entry: BookEntry| {
        let mut copy = BookCopy::default();
        copy.insert(entry);
        copy.digest(entry.pair)
    };
    for other in changed {
        assert_ne!(digest(base), digest(other), "{other:?}");
    }
}

#[test]
fn a_live_state_replaces_the_one_held_for_its_id() {
    let mut copy = BookCopy::with_levels();
    copy.insert(entry(1, 200, 100));
    copy.insert(BookEntry {
        rate: RateKey::new(30, 10),
        ..entry(1, 200, 100)
    });
    assert_eq!(levels(&copy), [(30, 10, 10)]);
    let mut fresh = BookCopy::default();
    fresh.insert(BookEntry {
        rate: RateKey::new(30, 10),
        ..entry(1, 200, 100)
    });
    assert_eq!(copy.digest(pair()), fresh.digest(pair()));
}

#[test]
fn batches_from_the_feed_apply_without_mismatch() {
    let (mut matcher, mut rx) = feed();
    let mut copy = BookCopy::with_levels();
    matcher.live_entry(entry(1, 200, 100));
    matcher.live_entry(entry(2, 7, 7));
    matcher.flush();
    let applied = copy.drain(&mut rx);
    assert_eq!(applied.mismatched, 0);
    assert_eq!(applied.touched, HashSet::from([pair()]));
    // Taken off and put back within one step: sent once, live.
    matcher.gone(note(1));
    matcher.live_entry(entry(1, 200, 100));
    matcher.gone(note(2));
    matcher.flush();
    let batch = rx.try_recv().unwrap();
    assert!(rx.try_recv().is_err(), "one batch per step");
    assert_eq!(batch.orders.len(), 2);
    assert!(batch.orders.contains(&OrderState::Live(entry(1, 200, 100))));
    assert!(batch.orders.contains(&OrderState::Gone(note(2))));
    assert_eq!(copy.apply(&batch).mismatched, 0);
    assert_eq!(levels(&copy), [(200, 100, 100)]);
    assert_eq!(copy.digest(pair()), matcher.copy.digest(pair()));
    // A step that touched nothing sends nothing.
    matcher.flush();
    assert!(rx.try_recv().is_err());
}

#[test]
fn a_wrong_or_missing_digest_is_reported() {
    let (mut matcher, mut rx) = feed();
    matcher.live_entry(entry(1, 200, 100));
    matcher.flush();
    let mut batch = rx.try_recv().unwrap();
    let before = digest_mismatches();
    batch.digests[0].1 ^= 1;
    assert_eq!(BookCopy::with_levels().apply(&batch).mismatched, 1);
    // A pair the batch changes must come with a digest.
    batch.digests.clear();
    assert_eq!(BookCopy::with_levels().apply(&batch).mismatched, 1);
    assert!(digest_mismatches() >= before + 2);
}

#[test]
fn a_pair_out_of_step_counts_each_batch_and_is_noted_until_back() {
    let mut copy = BookCopy::with_levels();
    let wrong = |seed| BookBatch {
        orders: vec![OrderState::Live(entry(seed, 200, 100))],
        digests: vec![(pair(), 1)],
    };
    // Two orders on one pair, both wrong: one batch, counted once.
    let both = BookBatch {
        orders: vec![
            OrderState::Live(entry(1, 200, 100)),
            OrderState::Live(entry(2, 200, 100)),
        ],
        digests: vec![(pair(), 1)],
    };
    assert_eq!(copy.apply(&both).mismatched, 1);
    assert_eq!(copy.apply(&wrong(3)).mismatched, 1);
    assert_eq!(copy.out_of_step, HashSet::from([pair()]));
    let right = BookBatch {
        orders: vec![OrderState::Gone(note(3))],
        digests: vec![(
            pair(),
            copy.digest(pair()).wrapping_sub(entry(3, 200, 100).hash()),
        )],
    };
    assert_eq!(copy.apply(&right).mismatched, 0);
    assert!(copy.out_of_step.is_empty());
}

#[test]
fn a_large_step_leaves_no_large_sets_behind() {
    let (mut matcher, _rx) = feed();
    for seed in 1..=5_000 {
        matcher.live_entry(entry(seed, 200, 100));
    }
    matcher.flush();
    assert!(matcher.touched_orders.capacity() <= 2 * SHRINK_TO);
}

#[test]
fn gone_for_an_order_never_held_changes_nothing() {
    let mut copy = BookCopy::with_levels();
    let applied = copy.apply(&BookBatch {
        orders: vec![OrderState::Gone(note(9))],
        digests: Vec::new(),
    });
    assert!(applied.touched.is_empty());
    assert_eq!(applied.mismatched, 0);
    assert!(copy.snapshot().is_empty());
}

#[test]
fn a_copy_without_levels_has_no_snapshot() {
    let mut copy = BookCopy::default();
    copy.insert(entry(1, 200, 100));
    assert!(copy.snapshot().is_empty());
    assert_ne!(copy.digest(pair()), 0);
}

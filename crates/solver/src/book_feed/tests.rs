use miden_protocol::note::NoteId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};

use super::*;

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
    // Any field change moves it.
    let mut other = BookCopy::default();
    other.insert(entry(1, 10, 5));
    other.insert(BookEntry {
        min_fill_step: 1,
        ..entry(2, 30, 10)
    });
    assert_ne!(one.digest(pair()), other.digest(pair()));
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
fn batches_from_the_matcher_apply_without_mismatch() {
    let mut matcher = BookCopy::default();
    let mut copy = BookCopy::with_levels();
    let first = matcher.batch_for_tests(vec![
        OrderState::Live(entry(1, 200, 100)),
        OrderState::Live(entry(2, 7, 7)),
    ]);
    let applied = copy.apply(&first);
    assert_eq!(applied.mismatched, 0);
    assert_eq!(applied.touched, HashSet::from([pair()]));
    let second = matcher.batch_for_tests(vec![OrderState::Gone(note(2))]);
    assert_eq!(copy.apply(&second).mismatched, 0);
    assert_eq!(levels(&copy), [(200, 100, 100)]);
    assert_eq!(copy.digest(pair()), matcher.digest(pair()));
}

#[test]
fn a_wrong_or_missing_digest_is_reported() {
    let mut matcher = BookCopy::default();
    let mut batch = matcher.batch_for_tests(vec![OrderState::Live(entry(1, 200, 100))]);
    let before = digest_mismatches();
    batch.digests[0].1 ^= 1;
    assert_eq!(BookCopy::with_levels().apply(&batch).mismatched, 1);
    // A pair the batch changes must come with a digest.
    batch.digests.clear();
    assert_eq!(BookCopy::with_levels().apply(&batch).mismatched, 1);
    assert!(digest_mismatches() >= before + 2);
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

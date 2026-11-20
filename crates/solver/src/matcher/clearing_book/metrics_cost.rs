//! What the matcher's metrics cost per tick, against the tick's own clearing
//! work on a large book; and what counting a gateway call costs. Numbers are
//! from the dev profile (dependencies optimised, this crate not), so the
//! ratios are what count.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use miden_protocol::account::AccountId;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::note::{Note, NoteType};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PRIVATE_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_3, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_WITH_CALLBACKS,
    ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
};
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};

use super::*;
use crate::maker::MakerTag;

/// A sell of 10 `base` asking `ask` `quote`.
fn sell(serial: u64, base: TokenId, quote: TokenId, ask: u64, maker: Option<MakerId>) -> BookOrder {
    let creator: AccountId = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
        .try_into()
        .unwrap();
    let note: Note = PswapNote::builder()
        .sender(creator)
        .storage(
            PswapNoteStorage::builder()
                .min_requested_asset(FungibleAsset::new(quote, ask).unwrap())
                .min_fill_step(AssetAmount::new(1).unwrap())
                .creator_account_id(creator)
                .build(),
        )
        .serial_number(Word::from([serial as u32, (serial >> 32) as u32, 0, 11]))
        .note_type(NoteType::Private)
        .offered_asset(FungibleAsset::new(base, 10).unwrap())
        .build()
        .unwrap()
        .into();
    BookOrder {
        priority_seq: serial,
        arrival_unix: 1,
        note: Arc::new(note),
        maker: maker.map(|maker_id| MakerTag {
            maker_id,
            root_seq: serial,
            expires_at_unix_ms: None,
        }),
    }
}

/// Microseconds per call of `f`, after one warm-up call.
fn per_call<F: FnMut()>(rounds: u32, mut f: F) -> f64 {
    f();
    let start = Instant::now();
    for _ in 0..rounds {
        f();
    }
    start.elapsed().as_secs_f64() * 1e6 / f64::from(rounds)
}

#[test]
#[ignore = "timing report; run with --nocapture"]
fn matcher_metrics_cost_per_tick() {
    let quote: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let bases: Vec<TokenId> = [
        ACCOUNT_ID_PRIVATE_FUNGIBLE_FAUCET,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_3,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_WITH_CALLBACKS,
    ]
    .into_iter()
    .map(|id| id.try_into().unwrap())
    .collect();
    // Four configured pairs; users' orders sit on a fifth, unconfigured one.
    let (configured, unconfigured) = (&bases[..4], bases[4]);
    let plan: HashSet<(TokenId, TokenId)> = configured
        .iter()
        .flat_map(|&base| [(base, quote), (quote, base)])
        .collect();
    let config = ClearingConfig::default();
    let price = BatchPrice::from_ratio(2, 1).unwrap();

    for (makers, per_pair, in_flight) in [(10, 20, 20), (50, 100, 100), (50, 400, 200)] {
        let mut book = ClearingBook::default();
        let mut serial = 1u64;
        for &base in configured {
            for maker in 1..=makers {
                for level in 0..per_pair {
                    // Level 0 is eligible at the price; the ladder sits above.
                    let ask = if level == 0 { 18 } else { 20 + level };
                    book.insert_or_skip(&sell(serial, base, quote, ask, Some(maker)));
                    serial += 1;
                }
            }
        }
        for _ in 0..1_000 {
            book.insert_or_skip(&sell(serial, unconfigured, quote, 18, None));
            serial += 1;
        }
        let sent: Vec<NoteId> = book.orders.keys().copied().take(in_flight).collect();
        for id in &sent {
            book.deactivate(*id);
        }
        book.track_matched(&sent, 1);

        let clearing = per_call(20, || {
            for &base in configured {
                book.build_pair_batch(base, quote, price, &config).unwrap();
            }
        });
        // Every tick: the matched-order clock and its minimum.
        let mut now = 2;
        let clock = per_call(200, || {
            now += 1;
            book.track_matched(&sent, now);
            std::hint::black_box(book.oldest_matched_since());
        });
        // At most once a second (about one tick in five).
        let gauges = per_call(200, || {
            std::hint::black_box(book.live_orders_per_maker());
            std::hint::black_box(book.orders_outside(&plan));
        });
        let per_tick = clock + gauges / 5.0;
        println!(
            "{makers:>3} makers x {per_pair:>3}/pair x 4 pairs + 1000 on an unconfigured pair = {:>6} orders, \
             {in_flight} in a settlement: clearing {clearing:.0} us; clock {clock:.1} us; gauges {gauges:.1} us \
             => {per_tick:.1} us per tick = {:.2}% of clearing",
            book.orders.len(),
            per_tick / clearing * 100.0
        );
    }
}

#[test]
#[ignore = "timing report; run with --nocapture"]
fn gateway_call_counter_cost() {
    use crate::gateway::metrics::Calls;
    let calls: &'static Calls = Box::leak(Box::default());
    let total = 1_000_000_i64;
    let alone = Instant::now();
    for i in 0..total {
        calls.add("submit_order", tonic::Code::Ok, Some(1 + i % 50));
    }
    let alone_ns = alone.elapsed().as_nanos() as f64 / total as f64;
    let threads = 8;
    let together = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            scope.spawn(move || {
                for i in 0..total / threads {
                    calls.add("heartbeat", tonic::Code::Ok, Some(1 + (i + thread) % 50));
                }
            });
        }
    });
    let together_ns = together.elapsed().as_nanos() as f64 / total as f64;
    let mut body = String::new();
    let render_us = per_call(100, || {
        body.clear();
        calls.render(&mut body);
    });
    println!(
        "gateway call counter: {alone_ns:.0} ns per call alone, {together_ns:.0} ns with {threads} threads at once; \
         {} series render in {render_us:.0} us",
        body.lines().count()
    );
}

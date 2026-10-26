//! Cost of self-trade prevention at scale (ADR 0005, "Cost"): one tick's
//! `build_pair_batch` over every pair, with the rule on and off. Numbers are
//! from the dev profile, so only the ratio and the growth matter.

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

/// `pairs` directed as (base, quote); every pair shares the same quote so one
/// maker's orders on other pairs are the realistic "noise" `decide` walks.
fn tokens() -> (TokenId, Vec<TokenId>) {
    let quote: TokenId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let bases: Vec<TokenId> = [
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    ]
    .into_iter()
    .map(|id| id.try_into().unwrap())
    .collect();
    (quote, bases)
}

fn order(
    serial: u64,
    buy: bool,
    base: TokenId,
    quote: TokenId,
    priority: u64,
    maker: Option<(MakerId, u64)>,
) -> BookOrder {
    let creator: AccountId = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
        .try_into()
        .unwrap();
    // Price 2 quote per base; every order is eligible (crossed) at that price.
    let (offered, requested) = if buy {
        (
            FungibleAsset::new(quote, 22).unwrap(),
            FungibleAsset::new(base, 10).unwrap(),
        )
    } else {
        (
            FungibleAsset::new(base, 10).unwrap(),
            FungibleAsset::new(quote, 18).unwrap(),
        )
    };
    let note: Note = PswapNote::builder()
        .sender(creator)
        .storage(
            PswapNoteStorage::builder()
                .min_requested_asset(requested)
                .min_fill_step(AssetAmount::new(1).unwrap())
                .creator_account_id(creator)
                .build(),
        )
        .serial_number(Word::from([
            serial as u32,
            (serial >> 32) as u32,
            u32::from(buy),
            7,
        ]))
        .note_type(NoteType::Private)
        .offered_asset(offered)
        .build()
        .unwrap()
        .into();
    BookOrder {
        priority_seq: priority,
        arrival_unix: 1,
        note: Arc::new(note),
        maker: maker.map(|(maker_id, root_seq)| MakerTag {
            maker_id,
            root_seq,
            expires_at_unix_ms: None,
        }),
    }
}

/// `makers` makers, each alternating buy/sell `per_pair` orders on every pair
/// (all crossed), plus `users` user orders per side per pair.
fn big_book(makers: usize, per_pair: usize, users: usize) -> (ClearingBook, Vec<TokenId>, TokenId) {
    let (quote, bases) = tokens();
    let mut book = ClearingBook::default();
    let mut priority = 1u64;
    let mut serial = 1u64;
    for &base in &bases {
        for maker in 0..makers as MakerId {
            for n in 0..per_pair as u64 {
                book.insert_or_skip(&order(
                    serial,
                    n % 2 == 0,
                    base,
                    quote,
                    priority,
                    Some((maker + 1, n + 1)),
                ));
                serial += 1;
                priority += 1;
            }
        }
        for n in 0..users {
            for buy in [true, false] {
                book.insert_or_skip(&order(serial, buy, base, quote, priority, None));
                serial += 1;
                priority += 1;
                let _ = n;
            }
        }
    }
    (book, bases, quote)
}

fn tick(book: &ClearingBook, bases: &[TokenId], quote: TokenId, on: bool) -> (usize, usize) {
    let config = ClearingConfig {
        self_trade_prevention: on,
        ..ClearingConfig::default()
    };
    let price = BatchPrice::from_ratio(2, 1).unwrap();
    let (mut admitted, mut sit_outs) = (0, 0);
    for &base in bases {
        let (batch, decisions) = book.build_pair_batch(base, quote, price, &config).unwrap();
        admitted += batch.orders().count();
        sit_outs += decisions.iter().map(|d| d.sit_out.len()).sum::<usize>();
    }
    (admitted, sit_outs)
}

#[test]
#[ignore = "timing report; run with --nocapture"]
fn self_trade_prevention_cost_at_scale() {
    for (makers, per_pair, users) in [(5, 20, 50), (50, 100, 50), (50, 1000, 50), (200, 100, 100)] {
        let built = Instant::now();
        let (book, bases, quote) = big_book(makers, per_pair, users);
        let orders = book.orders.len();
        let build_ms = built.elapsed().as_millis();
        // Warm once, then time a few ticks each way.
        tick(&book, &bases, quote, false);
        let rounds = 5;
        let off = Instant::now();
        for _ in 0..rounds {
            tick(&book, &bases, quote, false);
        }
        let off_us = off.elapsed().as_micros() / rounds;
        let on = Instant::now();
        let mut result = (0, 0);
        for _ in 0..rounds {
            result = tick(&book, &bases, quote, true);
        }
        let on_us = on.elapsed().as_micros() / rounds;
        println!(
            "makers {makers:>3} × {per_pair:>4}/pair × {} pairs + {users} users/side = {orders:>6} orders \
             (built in {build_ms} ms): tick off {off_us:>7} µs, on {on_us:>7} µs, \
             admitted {} sit-outs {}",
            bases.len(),
            result.0,
            result.1
        );
        // Every maker alternates sides, so its newest order (a sell when
        // per_pair is even) wins alone: one kept order per maker per pair.
        assert_eq!(result.1, makers * (per_pair - 1) * bases.len());
    }
}

/// A sell of 10 `base` asking `ask` `quote` (eligible at price 2 when
/// `ask <= 20`).
fn sell(
    serial: u64,
    base: TokenId,
    quote: TokenId,
    ask: u64,
    priority: u64,
    maker: Option<(MakerId, u64)>,
) -> BookOrder {
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
        .serial_number(Word::from([serial as u32, (serial >> 32) as u32, 0, 9]))
        .note_type(NoteType::Private)
        .offered_asset(FungibleAsset::new(base, 10).unwrap())
        .build()
        .unwrap()
        .into();
    BookOrder {
        priority_seq: priority,
        arrival_unix: 1,
        note: Arc::new(note),
        maker: maker.map(|(maker_id, root_seq)| MakerTag {
            maker_id,
            root_seq,
            expires_at_unix_ms: None,
        }),
    }
}

/// Makers quoting normally (one side, never crossed) on five pairs: per pair
/// one ask the walk reaches and the rest of the ladder above the price.
#[test]
#[ignore = "timing report; run with --nocapture"]
fn self_trade_prevention_cost_with_one_sided_makers_on_many_pairs() {
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
    for (makers, per_pair) in [(10, 20), (50, 100), (50, 400)] {
        let mut book = ClearingBook::default();
        let (mut serial, mut priority) = (1u64, 1u64);
        for &base in &bases {
            for maker in 1..=makers as MakerId {
                for level in 0..per_pair as u64 {
                    // Level 0 asks 18 (eligible at 2); the ladder asks 21 and up.
                    let ask = if level == 0 { 18 } else { 20 + level };
                    book.insert_or_skip(&sell(
                        serial,
                        base,
                        quote,
                        ask,
                        priority,
                        Some((maker, serial)),
                    ));
                    serial += 1;
                    priority += 1;
                }
            }
        }
        let orders = book.orders.len();
        tick(&book, &bases, quote, true);
        let rounds = 5;
        let off = Instant::now();
        for _ in 0..rounds {
            tick(&book, &bases, quote, false);
        }
        let off_us = off.elapsed().as_micros() / rounds;
        let on = Instant::now();
        let mut result = (0, 0);
        for _ in 0..rounds {
            result = tick(&book, &bases, quote, true);
        }
        let on_us = on.elapsed().as_micros() / rounds;
        println!(
            "one-sided: makers {makers:>3} × {per_pair:>4}/pair × {} pairs = {orders:>6} orders: \
             tick off {off_us:>6} µs, on {on_us:>7} µs, admitted {}, sit-outs {}",
            bases.len(),
            result.0,
            result.1
        );
        assert_eq!(result, (makers * bases.len(), 0), "no maker is crossed");
    }
}

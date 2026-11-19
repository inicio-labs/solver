use super::*;
use crate::maker::MakerTag;
use crate::price::PairRules;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
use miden_protocol::note::{Note, NoteType};
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
};
use miden_protocol::Word;
use miden_standards::note::{PswapNote, PswapNoteStorage};
use ruint::aliases::U256;

const SELL: bool = true;
const BUY: bool = false;
const BAND: u16 = 100;

type Terms = (bool, u64, u64, u64);

const DISPLACEMENT_303: &[Terms] = &[
    (SELL, 6, 17, 17),
    (SELL, 17, 52, 16),
    (BUY, 19, 6, 6),
    (BUY, 29, 9, 0),
];
const DISPLACEMENT_12: &[Terms] = &[
    (SELL, 6, 71, 0),
    (SELL, 20, 242, 0),
    (BUY, 88, 7, 7),
    (BUY, 144, 12, 0),
];

struct Book {
    base: TokenId,
    quote: TokenId,
    orders: Vec<BookOrder>,
    live: ClearingBook,
}

impl Book {
    fn new(terms: &[Terms]) -> Self {
        Self::with_makers(terms, &[])
    }

    fn with_makers(terms: &[Terms], makers: &[(u64, MakerTag)]) -> Self {
        let base = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
        let quote = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let mut live = ClearingBook::default();
        let mut rng = RandomCoin::new(Word::default());
        let mut orders = Vec::new();
        for (index, &(sell, offered, requested, minimum)) in terms.iter().enumerate() {
            let priority_seq = index as u64 + 1;
            let maker = makers
                .iter()
                .find_map(|&(seq, tag)| (seq == priority_seq).then_some(tag));
            let (offered_token, requested_token) = if sell { (base, quote) } else { (quote, base) };
            let note: Note = PswapNote::builder()
                .sender(creator)
                .storage(
                    PswapNoteStorage::builder()
                        .min_requested_asset(
                            FungibleAsset::new(requested_token, requested).unwrap(),
                        )
                        .creator_account_id(creator)
                        .min_fill_step(AssetAmount::new(minimum).unwrap())
                        .build(),
                )
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(FungibleAsset::new(offered_token, offered).unwrap())
                .build()
                .unwrap()
                .into();
            let order = BookOrder {
                priority_seq,
                arrival_unix: priority_seq,
                note: Arc::new(note),
                maker,
            };
            live.insert_or_skip(&order);
            orders.push(order);
        }
        Self {
            base,
            quote,
            orders,
            live,
        }
    }

    fn walk(&self, mid: BatchPrice, band: u16) -> Result<Option<BoundedPrice>, ClearingError> {
        let config = ClearingConfig::default();
        self.live.bounded_price(
            self.base,
            self.quote,
            mid,
            band,
            config.protocol_fee_ppm,
            config.max_orders_per_side,
            config.self_trade_prevention,
        )
    }

    fn fixed(&self, price: BatchPrice, enabled: bool, route: ClearRoute) -> Option<Cleared> {
        clear_pair_at(
            &self.live,
            self.base,
            self.quote,
            price,
            &config(enabled),
            route,
        )
        .unwrap_or_else(|failure| panic!("{} ({})", failure.message, failure.error))
    }

    fn bounded(&self, mid: BatchPrice, band: u16, enabled: bool) -> Option<Cleared> {
        clear_pair_bounded(
            &self.live,
            self.base,
            self.quote,
            mid,
            band,
            &config(enabled),
        )
    }

    fn tick(
        &mut self,
        mid: &str,
        band_bps: u16,
        received_at: Instant,
        enabled: bool,
    ) -> Option<ExecutionBatch> {
        let snapshot = PriceSnapshot::for_tests(
            &[(self.base, self.quote, mid, received_at)],
            &[],
            Duration::from_secs(30),
        )
        .with_pair_rules(
            self.base,
            self.quote,
            PairRules {
                band_bps,
                quote_ttl: None,
                reference_amount: None,
            },
        );
        let runtime = ClearingRuntime {
            bootstrap: oneshot::channel().1,
            prices: watch::channel(Arc::new(snapshot)).1,
            config: config(enabled),
            maker_settlement_buffer_ms: 0,
            routing: None,
        };
        let (exec_tx, mut exec_rx) = mpsc::channel(2);
        internal_clear(&mut self.live, &runtime, &exec_tx).unwrap();
        let sent = exec_rx.try_recv().ok();
        assert!(matches!(
            exec_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        sent
    }

    fn source(&self, priority: u64) -> &BookOrder {
        &self.orders[priority as usize - 1]
    }

    fn assert_execution(
        &self,
        execution: &ExecutionBatch,
        fills: &[(u64, u64)],
        group_ends: &[usize],
        cancels: &[(u64, u64)],
    ) {
        assert_eq!(execution.group_ends, group_ends);
        assert_eq!(execution.filled_notes.len(), fills.len());
        for (filled, &(priority, payment)) in execution.filled_notes.iter().zip(fills) {
            let source = self.source(priority);
            assert_eq!(filled.note_id, source.id());
            assert_eq!(filled.priority_seq, priority);
            assert_eq!(filled.requested_filled, payment);
            assert_eq!(filled.arrival_unix, source.arrival_unix);
            assert!(Arc::ptr_eq(&filled.note, &source.note));
            let expected_cancels: Vec<_> = cancels
                .iter()
                .filter_map(|&(first_fill, cancel)| {
                    (first_fill == priority).then(|| self.source(cancel))
                })
                .collect();
            assert_eq!(filled.cancel_on_reserve.len(), expected_cancels.len());
            for (cancelled, source) in filled.cancel_on_reserve.iter().zip(expected_cancels) {
                assert_eq!(cancelled.id(), source.id());
                assert_eq!(cancelled.priority_seq, source.priority_seq);
                assert_eq!(cancelled.arrival_unix, source.arrival_unix);
                assert_eq!(cancelled.maker, source.maker);
                assert!(Arc::ptr_eq(&cancelled.note, &source.note));
            }
        }
    }

    fn assert_clear(&self, cleared: Cleared, price: BatchPrice, fills: &[(u64, u64)]) {
        assert_eq!(cleared.price, price);
        self.assert_execution(&cleared.execution, fills, &[], &[]);
    }

    fn assert_witness(
        &mut self,
        mid: BatchPrice,
        walk_price: BatchPrice,
        mid_decimal: &str,
        mid_fills: &[(u64, u64)],
        walk_repair_fills: &[(u64, u64)],
        mid_released: PairAmounts,
    ) {
        assert_eq!(self.walk(mid, BAND).unwrap().unwrap().price, walk_price);
        let before = fallback_count("nothing_at_price");
        for enabled in [false, true] {
            assert!(self
                .fixed(walk_price, enabled, ClearRoute::DistinctWalk)
                .is_none());
            let fixed_mid = self.fixed(mid, enabled, ClearRoute::TerminalMid).unwrap();
            assert_eq!(fixed_mid.released, mid_released);
            self.assert_clear(fixed_mid, mid, mid_fills);
            let bounded = self.bounded(mid, BAND, enabled).unwrap();
            assert_eq!(bounded.released, mid_released);
            self.assert_clear(bounded, mid, mid_fills);
        }
        assert!(fallback_count("nothing_at_price") >= before + 2);

        // The frozen route would treat this same walk as terminal and take
        // the repair. It can displace a note paid by the legacy fallback.
        self.assert_clear(
            self.fixed(walk_price, true, ClearRoute::TerminalMid)
                .unwrap(),
            walk_price,
            walk_repair_fills,
        );
        let sent = self.tick(mid_decimal, BAND, Instant::now(), true).unwrap();
        self.assert_execution(&sent, mid_fills, &[mid_fills.len()], &[]);
    }
}

fn price(numerator: u64, denominator: u64) -> BatchPrice {
    BatchPrice::from_ratio(numerator, denominator).unwrap()
}

fn config(enabled: bool) -> ClearingConfig {
    ClearingConfig {
        integer_completion_repair: enabled,
        ..ClearingConfig::default()
    }
}

fn fallback_count(reason: &str) -> u64 {
    bounded_fallbacks()
        .find(|(label, _)| *label == reason)
        .unwrap()
        .1
}

#[test]
fn walk_repair_does_not_replace_the_303_mid_fallback() {
    Book::new(DISPLACEMENT_303).assert_witness(
        price(303, 100),
        price(52, 17),
        "3.03",
        &[(1, 18), (4, 6)],
        &[(1, 18), (2, 28), (4, 9), (3, 6)],
        PairAmounts { base: 6, quote: 19 },
    );
}

#[test]
fn walk_repair_does_not_displace_the_buyer_at_the_12_mid_fallback() {
    Book::new(DISPLACEMENT_12).assert_witness(
        price(12, 1),
        price(121, 10),
        "12",
        &[(1, 72), (4, 6)],
        &[(1, 72), (2, 13), (3, 7)],
        PairAmounts { base: 6, quote: 72 },
    );
}

#[test]
fn executable_distinct_walk_can_upgrade_at_that_price() {
    let book = Book::new(&[(SELL, 10, 14, 0), (BUY, 5, 3, 0), (BUY, 300, 200, 200)]);
    let mid = price(151, 100);
    let walk = price(3, 2);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    book.assert_clear(
        book.bounded(mid, BAND, false).unwrap(),
        walk,
        &[(1, 3), (2, 2)],
    );
    book.assert_clear(
        book.bounded(mid, BAND, true).unwrap(),
        walk,
        &[(1, 5), (2, 3)],
    );
}

#[test]
fn executable_distinct_walk_stays_when_repair_declines() {
    let book = Book::new(&[(SELL, 10, 15, 0), (BUY, 15, 10, 0)]);
    let mid = price(151, 100);
    let walk = price(3, 2);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    for enabled in [false, true] {
        book.assert_clear(
            book.bounded(mid, BAND, enabled).unwrap(),
            walk,
            &[(1, 15), (2, 10)],
        );
    }
}

#[test]
fn walk_skip_uses_terminal_mid_upgrade_even_when_payments_match() {
    let book = Book::new(&[
        (SELL, 154, 227, 0),
        (BUY, 11, 7, 0),
        (BUY, 2970, 2000, 2000),
    ]);
    let mid = price(3, 2);
    let walk = price(297, 200);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    assert!(book.fixed(walk, false, ClearRoute::DistinctWalk).is_none());
    assert!(book.fixed(walk, true, ClearRoute::DistinctWalk).is_none());
    book.assert_clear(
        book.fixed(walk, true, ClearRoute::TerminalMid).unwrap(),
        walk,
        &[(1, 11), (2, 7)],
    );
    book.assert_clear(
        book.bounded(mid, BAND, false).unwrap(),
        mid,
        &[(1, 9), (2, 6)],
    );
    book.assert_clear(
        book.bounded(mid, BAND, true).unwrap(),
        mid,
        &[(1, 11), (2, 7)],
    );
}

#[test]
fn both_prices_skip_but_only_the_mid_can_rescue() {
    let book = Book::new(&[(SELL, 8, 11, 0), (BUY, 2, 1, 0), (BUY, 1490, 1000, 1000)]);
    let mid = price(3, 2);
    let walk = price(149, 100);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    assert!(book.fixed(walk, false, ClearRoute::DistinctWalk).is_none());
    assert!(book.fixed(walk, true, ClearRoute::DistinctWalk).is_none());
    assert!(book.bounded(mid, BAND, false).is_none());
    book.assert_clear(
        book.fixed(walk, true, ClearRoute::TerminalMid).unwrap(),
        walk,
        &[(1, 2), (2, 1)],
    );
    book.assert_clear(
        book.bounded(mid, BAND, true).unwrap(),
        mid,
        &[(1, 2), (2, 1)],
    );
}

#[test]
fn walk_only_rescue_does_not_dispatch_or_deactivate() {
    let mut book = Book::new(&[(SELL, 8, 12, 0), (BUY, 2, 1, 0)]);
    let mid = price(149, 100);
    let walk = price(3, 2);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    assert!(book.fixed(walk, true, ClearRoute::DistinctWalk).is_none());
    assert!(book.fixed(mid, true, ClearRoute::TerminalMid).is_none());
    book.assert_clear(
        book.fixed(walk, true, ClearRoute::TerminalMid).unwrap(),
        walk,
        &[(1, 2), (2, 1)],
    );
    let original_levels = book.live.levels();
    for enabled in [false, true] {
        assert!(book.bounded(mid, BAND, enabled).is_none());
        assert!(book.tick("1.49", BAND, Instant::now(), enabled).is_none());
        assert_eq!(book.live.levels(), original_levels);
    }
}

#[test]
fn zero_band_and_equal_walk_allow_terminal_tail_repair() {
    let mid = price(6, 4);
    for band in [0, BAND] {
        let mut book = Book::new(&[(SELL, 8, 11, 0), (BUY, 2, 1, 0)]);
        if band != 0 {
            assert_eq!(book.walk(mid, band).unwrap().unwrap().price, price(3, 2));
            assert!(book.bounded(mid, band, false).is_none());
            book.assert_clear(
                book.bounded(mid, band, true).unwrap(),
                price(3, 2),
                &[(1, 2), (2, 1)],
            );
        }
        assert!(book.tick("1.5", band, Instant::now(), false).is_none());
        let sent = book.tick("1.5", band, Instant::now(), true).unwrap();
        book.assert_execution(&sent, &[(1, 2), (2, 1)], &[2], &[]);

        let solver = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE
            .try_into()
            .unwrap();
        let seller = PswapNote::try_from(sent.filled_notes[0].note.as_ref()).unwrap();
        let buyer = PswapNote::try_from(sent.filled_notes[1].note.as_ref()).unwrap();
        let (_, seller_remainder) = seller
            .execute(
                solver,
                None,
                Some(FungibleAsset::new(book.quote, 2).unwrap()),
            )
            .unwrap();
        let remaining = seller_remainder.unwrap();
        assert_eq!(remaining.offered_asset().amount().as_u64(), 7);
        assert_eq!(remaining.storage().min_requested_amount(), 9);
        let (_, buyer_remainder) = buyer
            .execute(
                solver,
                None,
                Some(FungibleAsset::new(book.base, 1).unwrap()),
            )
            .unwrap();
        assert!(buyer_remainder.is_none());
    }
}

fn maker(maker_id: i64, root_seq: u64) -> MakerTag {
    MakerTag {
        maker_id,
        root_seq,
        expires_at_unix_ms: None,
    }
}

#[test]
fn empty_walk_and_walk_error_keep_the_terminal_mid() {
    // Rule B benches the older maker ask during pricing. At the mid the
    // maker bid is ineligible and the ask can trade with the user.
    let book = Book::with_makers(
        &[
            (SELL, 500_000, 997_000, 0),
            (BUY, 998_100, 500_000, 0),
            (BUY, 1_000_200, 500_000, 0),
        ],
        &[(1, maker(7, 1)), (2, maker(7, 2))],
    );
    let mid = price(2, 1);
    assert!(book.walk(mid, 30).unwrap().is_none());
    assert!(matches!(
        book.walk(mid, 10_000),
        Err(ClearingError::InvalidConfig)
    ));
    // The 500,000-base comparison fill is partial for the user's bid.
    // Its payment is ceil(500,000 × 1,000,000 / 1,000,200) = 499,901.
    for (band, reason) in [(30, "walk_found_nothing"), (10_000, "walk_error")] {
        let before = fallback_count(reason);
        for enabled in [false, true] {
            book.assert_clear(
                book.bounded(mid, band, enabled).unwrap(),
                mid,
                &[(1, 1_000_000), (3, 499_901)],
            );
        }
        assert!(fallback_count(reason) >= before + 2);
    }
}

#[test]
fn terminal_source_error_keeps_its_admission_failure() {
    let book = Book::new(&[(SELL, 100, 150, 0), (BUY, 150, 100, 0)]);
    let mid = BatchPrice::new(U256::ONE << 250, U256::ONE).unwrap();
    assert!(matches!(
        book.walk(mid, BAND),
        Err(ClearingError::ArithmeticOverflow)
    ));
    let before = fallback_count("walk_error");
    for enabled in [false, true] {
        let fixed = clear_pair_at(
            &book.live,
            book.base,
            book.quote,
            mid,
            &config(enabled),
            ClearRoute::TerminalMid,
        );
        let Err(error) = fixed else {
            panic!("expected a terminal admission error");
        };
        assert_eq!(error.message, "clearing admission failed");
        assert!(matches!(error.error, ClearingError::ArithmeticOverflow));
        assert!(book.bounded(mid, BAND, enabled).is_none());
    }
    assert!(fallback_count("walk_error") >= before + 2);
}

#[test]
fn stale_price_pauses_even_a_repairable_tail() {
    let mut book = Book::new(&[(SELL, 8, 11, 0), (BUY, 2, 1, 0)]);
    let original = book.live.levels();
    let received_at = Instant::now() - Duration::from_secs(31);
    assert!(book.tick("1.5", BAND, received_at, true).is_none());
    assert_eq!(book.live.levels(), original);
    let sent = book.tick("1.5", BAND, Instant::now(), true).unwrap();
    book.assert_execution(&sent, &[(1, 2), (2, 1)], &[2], &[]);
}

#[test]
fn both_prices_decline_a_non_crossing_executable_minimum() {
    let mut book = Book::new(&[(SELL, 8, 11, 11), (BUY, 2, 1, 0), (BUY, 300, 200, 200)]);
    let mid = price(151, 100);
    let walk = price(3, 2);
    assert_eq!(book.walk(mid, BAND).unwrap().unwrap().price, walk);
    for enabled in [false, true] {
        assert!(book.fixed(walk, enabled, ClearRoute::TerminalMid).is_none());
        assert!(book.fixed(mid, enabled, ClearRoute::TerminalMid).is_none());
        assert!(book.bounded(mid, BAND, enabled).is_none());
        assert!(book.tick("1.51", BAND, Instant::now(), enabled).is_none());
    }
}

#[test]
fn required_planner_resource_limit_dispatches_nothing() {
    let book = Book::new(&[(SELL, 8, 11, 0), (BUY, 2, 1, 0)]);
    for enabled in [false, true] {
        let config = ClearingConfig {
            max_total_intervals: 1,
            ..config(enabled)
        };
        let mid = price(3, 2);
        let (batch, _) = book
            .live
            .build_pair_batch(book.base, book.quote, mid, &config)
            .unwrap();
        assert!(matches!(
            PairMatcher::new(&batch, &config).clear(),
            Ok(ClearingOutcome::Skipped(SkipReason::ResourceLimit))
        ));
        assert!(matches!(
            clear_pair_at(
                &book.live,
                book.base,
                book.quote,
                mid,
                &config,
                ClearRoute::TerminalMid
            ),
            Ok(None)
        ));
        assert!(
            clear_pair_bounded(&book.live, book.base, book.quote, mid, BAND, &config).is_none()
        );
    }
}

#[test]
fn fallback_dispatch_uses_mid_cancellations_and_leaves_walk_sit_out_active() {
    let mut terms = DISPLACEMENT_12.to_vec();
    terms.extend([(BUY, 12, 1, 1), (BUY, 121, 10, 10)]);
    for enabled in [false, true] {
        let mut book = Book::with_makers(
            &terms,
            &[
                (1, maker(7, 2)),
                (5, maker(7, 1)),
                (2, maker(8, 2)),
                (6, maker(8, 1)),
            ],
        );
        let mid = price(12, 1);
        let walk = price(121, 10);
        let proposal = book.walk(mid, BAND).unwrap().unwrap();
        assert_eq!(proposal.price, walk);
        assert!(proposal.sit_outs.contains(&book.source(6).id()));
        assert!(book
            .fixed(walk, enabled, ClearRoute::DistinctWalk)
            .is_none());

        let original = book.live.levels();
        let cleared = book.bounded(mid, BAND, enabled).unwrap();
        assert_eq!(cleared.price, mid);
        book.assert_execution(&cleared.execution, &[(1, 72), (4, 6)], &[], &[(1, 5)]);
        assert_eq!(book.live.levels(), original);

        let sent = book.tick("12", BAND, Instant::now(), enabled).unwrap();
        book.assert_execution(&sent, &[(1, 72), (4, 6)], &[2], &[(1, 5)]);
        let mut expected_remaining = ClearingBook::default();
        for priority in [2, 3, 6] {
            expected_remaining.insert_or_skip(book.source(priority));
        }
        assert_eq!(book.live.levels(), expected_remaining.levels());
    }
}

#[test]
fn terminal_repair_can_attach_sit_outs_to_a_newly_filled_maker() {
    let mut book = Book::with_makers(
        &[(SELL, 8, 11, 0), (BUY, 2, 1, 0), (SELL, 10, 14, 0)],
        &[(2, maker(7, 2)), (3, maker(7, 1))],
    );
    assert!(book.tick("1.5", BAND, Instant::now(), false).is_none());
    let sent = book.tick("1.5", BAND, Instant::now(), true).unwrap();
    book.assert_execution(&sent, &[(1, 2), (2, 1)], &[2], &[(2, 3)]);
    assert!(book.live.levels().is_empty());
    assert!(book.tick("1.5", BAND, Instant::now(), true).is_none());
}

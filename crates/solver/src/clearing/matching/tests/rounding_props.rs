//! Tests for covering a rounding shortfall by raising one partial order
//! (`PairMatcher::cover_rounding`): real monitor cases, the raise itself, and
//! random books that must always settle.

use super::*;

/// One maker selling base at exactly `ask_e6` (quote units per base unit,
/// x1e6) and one user offering `quote` for at least `base`, cleared at the
/// maker's ask.
fn maker_and_user(ask_e6: u64, quote: u64, base: u64) -> (ClearingOutcome, Vec<Order>) {
    let (base_id, quote_id) = assets();
    let maker_offered = 1_000_000_000u64;
    let maker_requested = (u128::from(maker_offered) * u128::from(ask_e6) / 1_000_000) as u64;
    let mut coin = RandomCoin::new(Word::default());
    let orders = vec![
        order(
            FungibleAsset::new(base_id, maker_offered).unwrap(),
            FungibleAsset::new(quote_id, maker_requested).unwrap(),
            0,
            1,
            &mut coin,
        ),
        order(
            FungibleAsset::new(quote_id, quote).unwrap(),
            FungibleAsset::new(base_id, base).unwrap(),
            0,
            2,
            &mut coin,
        ),
    ];
    let price = BatchPrice::from_ratio(maker_requested, maker_offered).unwrap();
    let outcome = clear(&batch(price, &orders), &ClearingConfig::default());
    (outcome, orders)
}

/// (payment, release) of one order in an accepted plan.
fn executed(outcome: &ClearingOutcome, order: &Order) -> Option<(u64, u64)> {
    let ClearingOutcome::Accepted(plan) = outcome else {
        return None;
    };
    plan.candidate
        .executions
        .iter()
        .find(|e| e.order_id == order.id())
        .map(|e| (e.payment.amount().as_u64(), e.release.amount().as_u64()))
}

/// The monitor's USDT -> WBTC run: 94,023 µUSDT for at least 139 sats, maker
/// ask 673.042338. Paying the maker floor(P * B) left its note one sat short;
/// the user must now be filled completely.
#[test]
fn monitor_order_is_filled_completely() {
    let (outcome, orders) = maker_and_user(673_042_338, 94_023, 139);
    let (paid, released) = executed(&outcome, &orders[1]).expect("the batch settles");
    assert_eq!(released, 94_023, "the whole order is spent");
    assert!(paid >= 139, "paid {paid}");
}

/// Real leftovers from monitor timeouts: quote left for 1 base unit at the
/// maker's ask. Each was skipped as insolvent on every tick; now each fills.
#[test]
fn monitor_leftovers_fill() {
    for (quote, ask_e6) in [
        (677, 673_042_338),
        (695, 690_972_222),
        (21, 20_018_758),
        (39, 38_219_376),
    ] {
        let (outcome, orders) = maker_and_user(ask_e6, quote, 1);
        assert_eq!(
            executed(&outcome, &orders[1]).map(|(paid, _)| paid),
            Some(1),
            "{quote} @ {ask_e6}: {outcome:?}"
        );
    }
}

/// Sorted batch prepared at `fee_ppm` (the module's `batch` helper uses 0).
fn batch_with_fee<'a>(price: BatchPrice, fee_ppm: u32, orders: &'a [Order]) -> PairBatch<'a> {
    let (base, _) = assets();
    let (mut sells, mut buys): (Vec<_>, Vec<_>) = orders
        .iter()
        .partition(|order| order.offered_asset().faucet_id() == base);
    let key = |order: &&Order| {
        (
            RateKey::new(
                order.requested_asset().amount().as_u64(),
                order.offered_asset().amount().as_u64(),
            ),
            order.priority_sequence(),
        )
    };
    sells.sort_unstable_by_key(key);
    buys.sort_unstable_by_key(key);
    let prepare = |orders: Vec<&'a Order>, side| {
        orders
            .into_iter()
            .map(|o| {
                o.prepare_if_eligible(side, price, fee_ppm)
                    .unwrap()
                    .unwrap()
            })
            .collect()
    };
    PairBatch::new(
        price,
        prepare(sells, OrderSide::SellBase),
        prepare(buys, OrderSide::BuyBase),
    )
}

/// A seller offering 10 base for 7 quote, prepared at 1 quote per base: a
/// partial fill f pays ceil(7f / 10) and gives out floor(10 * pay / 7). Fill
/// 5 pays 4 and gives 5; fill 6 pays 5 and gives 7; fill 9 pays 7 and gives
/// all 10.
fn sellers_10_for_7(steps: &[u64]) -> Vec<Order> {
    let (base, quote) = assets();
    let mut coin = RandomCoin::new(Word::default());
    steps
        .iter()
        .enumerate()
        .map(|(index, &step)| {
            order(
                FungibleAsset::new(base, 10).unwrap(),
                FungibleAsset::new(quote, 7).unwrap(),
                step,
                index as u64 + 1,
                &mut coin,
            )
        })
        .collect()
}

fn prepared(orders: &[Order]) -> Vec<MatchOrder<'_>> {
    let price = BatchPrice::from_ratio(1, 1).unwrap();
    orders
        .iter()
        .map(|o| {
            o.prepare_if_eligible(OrderSide::SellBase, price, 0)
                .unwrap()
                .unwrap()
        })
        .collect()
}

fn fills(values: &[u64]) -> Vec<U256> {
    values.iter().map(|&value| U256::from(value)).collect()
}

#[test]
fn the_first_partial_is_raised_to_the_smallest_covering_fill() {
    let orders = sellers_10_for_7(&[0, 0]);
    let sellers = prepared(&orders);
    let full = sellers[0].fill_interval().maximum;
    let raise = |allocations: &mut Vec<U256>, short| {
        PairMatcher::raise_first_partial(&sellers, allocations, short, 0, WholeNote::AsFullFill)
            .unwrap()
    };

    // One base short: 5 -> 6 (gives 7). Fill 5 gives only 5.
    let mut allocations = fills(&[5, 5]);
    raise(&mut allocations, 1);
    assert_eq!(allocations, fills(&[6, 5]), "first partial, smallest fill");

    // Two short: 6 gives exactly 5 + 2.
    let mut allocations = fills(&[5, 5]);
    raise(&mut allocations, 2);
    assert_eq!(allocations, fills(&[6, 5]));

    // A full first order is skipped; the next partial is raised.
    let mut allocations = vec![full, U256::from(5u64)];
    raise(&mut allocations, 1);
    assert_eq!(allocations, vec![full, U256::from(6u64)]);

    // An unfilled order is skipped too.
    let mut allocations = fills(&[0, 5]);
    raise(&mut allocations, 1);
    assert_eq!(allocations, fills(&[0, 6]));

    // Nothing short: nothing changes.
    let mut allocations = fills(&[5, 5]);
    assert!(!raise(&mut allocations, 0));
    assert_eq!(allocations, fills(&[5, 5]));
}

/// A raise whose note would give out everything it offers becomes a full fill
/// (paid at the batch price) when asked to, else stays the smallest fill (paid
/// at the order's own limit).
#[test]
fn a_raise_that_spends_the_whole_note_can_be_a_full_fill() {
    let orders = sellers_10_for_7(&[0]);
    let sellers = prepared(&orders);
    let full = sellers[0].fill_interval().maximum;

    let mut allocations = fills(&[5]);
    let whole =
        PairMatcher::raise_first_partial(&sellers, &mut allocations, 5, 0, WholeNote::AsFullFill)
            .unwrap();
    assert!(whole);
    assert_eq!(allocations, vec![full]);

    let mut allocations = fills(&[5]);
    let whole = PairMatcher::raise_first_partial(
        &sellers,
        &mut allocations,
        5,
        0,
        WholeNote::AsSmallestFill,
    )
    .unwrap();
    assert!(!whole);
    assert_eq!(
        allocations,
        fills(&[9]),
        "fill 9 already pays the whole ask"
    );
}

#[test]
fn fill_releasing_contract() {
    let orders = sellers_10_for_7(&[0, 5]);
    let sellers = prepared(&orders);
    let (plain, stepped) = (&sellers[0], &sellers[1]);
    let f = |o: &MatchOrder<'_>, current: u64, needed: u64| {
        o.fill_releasing(U256::from(current), needed, 0)
            .unwrap()
            .map(|fill| u64::try_from(fill).unwrap())
    };
    assert_eq!(f(plain, 5, 0), Some(5), "nothing needed: keep the fill");
    assert_eq!(f(plain, 8, 6), Some(8), "never lowers the current fill");
    assert_eq!(f(plain, 5, 6), Some(6));
    assert_eq!(
        f(plain, 5, 10),
        Some(9),
        "smallest fill giving out everything"
    );
    assert_eq!(f(plain, 5, 11), None, "more than the note offers");
    // min_fill_step 5 quote = comparison fill 7.
    assert_eq!(f(stepped, 0, 1), Some(7), "never below the order's minimum");
}

/// A base unit worth a third of a quote unit, fee 3000 ppm. Raising the user
/// seller leaves the buyers one quote short, so the buyers' side must be
/// raised too: the maker buyer gives out 3 quote and the user sells all 9.
#[test]
fn the_buyers_side_is_raised_when_quote_is_short() {
    let (base, quote) = assets();
    let mut coin = RandomCoin::new(Word::default());
    let asset = |faucet, amount| FungibleAsset::new(faucet, amount).unwrap();
    let orders = [
        order(asset(base, 9), asset(quote, 3), 1, 1, &mut coin),
        order(asset(quote, 506), asset(base, 1356), 678, 2, &mut coin),
        order(asset(quote, 55), asset(base, 149), 0, 3, &mut coin),
    ];
    let price = BatchPrice::from_ratio(1000, 2991).unwrap();
    let config = ClearingConfig {
        protocol_fee_ppm: 3000,
        ..ClearingConfig::default()
    };
    let outcome = clear(&batch_with_fee(price, 3000, &orders), &config);
    assert_eq!(
        executed(&outcome, &orders[0]),
        Some((3, 9)),
        "user sells all 9 base"
    );
    assert_eq!(
        executed(&outcome, &orders[2]),
        Some((9, 3)),
        "maker gives out 3 quote"
    );
}

/// The only seller is filled completely, so nothing on the short side can be
/// raised; the shave still settles the batch, taking it from the maker.
#[test]
fn shave_still_runs_when_no_seller_can_be_raised() {
    let (base, quote) = assets();
    let mut coin = RandomCoin::new(Word::default());
    let asset = |faucet, amount| FungibleAsset::new(faucet, amount).unwrap();
    let orders = vec![
        // A maker bidding exactly the batch price, then a user buying base.
        order(
            asset(quote, 3_339_468_000),
            asset(base, 9_000_000_000),
            0,
            1,
            &mut coin,
        ),
        order(
            asset(quote, 502_500),
            asset(base, 1_346_982),
            0,
            2,
            &mut coin,
        ),
        // The only seller: a user.
        order(
            asset(base, 2_693_965),
            asset(quote, 994_999),
            0,
            3,
            &mut coin,
        ),
    ];
    let price = BatchPrice::new(U256::from(92_763u64), U256::from(250_000u64)).unwrap();
    let outcome = clear(&batch(price, &orders), &ClearingConfig::default());
    let released = |order: &Order| executed(&outcome, order).map(|(_, released)| released);
    assert_eq!(released(&orders[2]), Some(2_693_965), "seller spent whole");
    assert_eq!(
        released(&orders[1]),
        Some(502_500),
        "user buyer spent whole"
    );
    assert_eq!(
        released(&orders[0]),
        Some(497_101),
        "the maker takes the shave"
    );
}

// ---- Random books: users and makers on both sides, prices like real pairs. ----

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

struct Book {
    orders: Vec<Order>,
    price: BatchPrice,
    fee_ppm: u32,
}

/// Quote units per base unit, x1e6 (WBTC, ETH, PAXG, stable, base worth less).
const PROFILES: [u64; 5] = [673_042_338, 20_018_758, 38_219_376, 1_000_300, 371_200];

fn mul_ppm(amount: u64, price_e6: u64) -> u128 {
    u128::from(amount) * u128::from(price_e6)
}

fn generate(rng: &mut Rng, step_pct: u64) -> Option<Book> {
    let (base, quote) = assets();
    let mut coin = RandomCoin::new(Word::from([rng.next() as u32, 0, 0, 0]));
    let price_e6 = PROFILES[rng.below(PROFILES.len() as u64) as usize];
    let fee_ppm = [0u32, 0, 50, 500][rng.below(4) as usize];
    let mut orders = Vec::new();
    let mut makers = Vec::new();
    let mut seq = 1u64;
    let mut push = |orders: &mut Vec<Order>,
                    rng: &mut Rng,
                    offered: FungibleAsset,
                    requested: FungibleAsset| {
        let step = if rng.chance(step_pct) {
            rng.range(1, requested.amount().as_u64())
        } else {
            0
        };
        let order = order(offered, requested, step, seq, &mut coin);
        seq += 1;
        orders.push(order);
    };
    // Base units a user trades: about $0.10 to $1,000 worth, at least 1.
    let user_base = |rng: &mut Rng| -> u64 {
        let usd_micro = 10u64.pow(rng.range(5, 9) as u32) * rng.range(1, 9);
        (usd_micro * 1_000_000 / price_e6).max(1)
    };
    for _ in 0..rng.range(0, 3) {
        // Maker selling base at the reference price plus 0-20 bps.
        let ask = price_e6 + price_e6 * rng.range(0, 20) / 10_000;
        let offered = 10u64.pow(rng.range(3, 9) as u32) * rng.range(1, 9);
        let requested = u64::try_from(mul_ppm(offered, ask).div_ceil(1_000_000))
            .ok()?
            .max(1);
        push(
            &mut orders,
            rng,
            FungibleAsset::new(base, offered).ok()?,
            FungibleAsset::new(quote, requested).ok()?,
        );
        makers.push(orders.len() - 1);
    }
    for _ in 0..rng.range(0, 3) {
        // Maker buying base at the reference price minus 0-20 bps.
        let bid = price_e6 - price_e6 * rng.range(0, 20) / 10_000;
        let requested = 10u64.pow(rng.range(3, 9) as u32) * rng.range(1, 9);
        let offered = u64::try_from(mul_ppm(requested, bid) / 1_000_000)
            .ok()?
            .max(1);
        push(
            &mut orders,
            rng,
            FungibleAsset::new(quote, offered).ok()?,
            FungibleAsset::new(base, requested).ok()?,
        );
        makers.push(orders.len() - 1);
    }
    let slippage_bps = |rng: &mut Rng| [0u64, 10, 50, 50, 200][rng.below(5) as usize];
    for _ in 0..rng.range(0, 3) {
        // User buying base: offers enough quote for its base at the
        // reference price plus slippage.
        let requested = user_base(rng);
        let limit = price_e6 + price_e6 * slippage_bps(rng) / 10_000;
        let offered = u64::try_from(mul_ppm(requested, limit).div_ceil(1_000_000))
            .ok()?
            .max(1);
        push(
            &mut orders,
            rng,
            FungibleAsset::new(quote, offered).ok()?,
            FungibleAsset::new(base, requested).ok()?,
        );
    }
    for _ in 0..rng.range(0, 3) {
        // User selling base: asks for quote at the reference price minus slippage.
        let offered = user_base(rng);
        let limit = price_e6 - price_e6 * slippage_bps(rng) / 10_000;
        let requested = u64::try_from(mul_ppm(offered, limit) / 1_000_000)
            .ok()?
            .max(1);
        push(
            &mut orders,
            rng,
            FungibleAsset::new(base, offered).ok()?,
            FungibleAsset::new(quote, requested).ok()?,
        );
    }
    // Band walk: clear at one maker's exact fee-inclusive limit. Otherwise a
    // mid-like price within 10 bps of the reference.
    let band = !makers.is_empty() && rng.chance(70);
    let price = if band {
        let maker = &orders[makers[rng.below(makers.len() as u64) as usize]];
        let offered = maker.offered_asset().amount().as_u64();
        let requested = maker.requested_asset().amount().as_u64();
        if maker.offered_asset().faucet_id() == base {
            BatchPrice::new(
                U256::from(requested) * U256::from(1_000_000u64),
                U256::from(offered) * U256::from(1_000_000 - fee_ppm),
            )
        } else {
            BatchPrice::new(
                U256::from(offered) * U256::from(1_000_000u64),
                U256::from(requested) * U256::from(1_000_000 + fee_ppm),
            )
        }
        .ok()?
    } else {
        let p =
            price_e6 + price_e6 * rng.range(0, 10) / 10_000 - price_e6 * rng.range(0, 10) / 10_000;
        BatchPrice::from_ratio(p, 1_000_000).ok()?
    };
    Some(Book {
        orders,
        price,
        fee_ppm,
    })
}

fn eligible_batch<'a>(book: &'a Book) -> Option<PairBatch<'a>> {
    let (base, _) = assets();
    let key = |order: &&Order| {
        (
            RateKey::new(
                order.requested_asset().amount().as_u64(),
                order.offered_asset().amount().as_u64(),
            ),
            order.priority_sequence(),
        )
    };
    let (mut sells, mut buys): (Vec<_>, Vec<_>) = book
        .orders
        .iter()
        .partition(|order| order.offered_asset().faucet_id() == base);
    sells.sort_unstable_by_key(key);
    buys.sort_unstable_by_key(key);
    let sells: Vec<_> = sells
        .into_iter()
        .filter_map(|o| {
            o.prepare_if_eligible(OrderSide::SellBase, book.price, book.fee_ppm)
                .unwrap()
        })
        .collect();
    let buys: Vec<_> = buys
        .into_iter()
        .filter_map(|o| {
            o.prepare_if_eligible(OrderSide::BuyBase, book.price, book.fee_ppm)
                .unwrap()
        })
        .collect();
    (!sells.is_empty() && !buys.is_empty()).then(|| PairBatch::new(book.price, sells, buys))
}

/// PSWAP validity of every execution, and exact solvency of the plan.
fn check_plan(batch: &PairBatch<'_>, plan: &SettlementPlan) -> Result<(), String> {
    let c = &plan.candidate;
    if c.released.base < c.paid.base || c.released.quote < c.paid.quote {
        return Err("plan not solvent".into());
    }
    for e in &c.executions {
        let order = batch
            .orders()
            .find(|o| o.order().id() == e.order_id)
            .unwrap();
        let note = order.order();
        let interval = order.fill_interval();
        if e.comparison_fill < interval.minimum || e.comparison_fill > interval.maximum {
            return Err("fill outside the order's interval".into());
        }
        let expected = note
            .pswap_note()
            .calculate_offered_for_requested(e.payment.amount().as_u64())
            .unwrap();
        if e.release.amount().as_u64() != expected {
            return Err("release differs from the PSWAP payout".into());
        }
        let requested = note.requested_asset().amount().as_u64();
        let step = note
            .pswap_note()
            .storage()
            .min_fill_step()
            .as_u64()
            .min(requested);
        if e.payment.amount().as_u64() < step {
            return Err("payment below min_fill_step".into());
        }
    }
    Ok(())
}

/// Random books at two `min_fill_step` levels: every accepted plan is valid,
/// and no book is skipped as insolvent (rounding is always covered or shaved).
#[test]
fn random_books_settle_with_valid_plans() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for step_pct in [15, 60] {
        let mut books = 0;
        while books < 1_000 {
            let Some(book) = generate(&mut rng, step_pct) else {
                continue;
            };
            let Some(batch) = eligible_batch(&book) else {
                continue;
            };
            books += 1;
            let config = ClearingConfig {
                protocol_fee_ppm: book.fee_ppm,
                ..ClearingConfig::default()
            };
            match clear(&batch, &config) {
                ClearingOutcome::Accepted(plan) => {
                    if let Err(error) = check_plan(&batch, &plan) {
                        panic!("{error} at {:?}: {plan:?}", book.price);
                    }
                }
                ClearingOutcome::Skipped(reason @ SkipReason::Insolvent { .. }) => {
                    panic!("skipped as insolvent at {:?}: {reason:?}", book.price)
                }
                ClearingOutcome::Skipped(_) => {}
            }
        }
    }
}

/// `min_fill_step` can leave two partial orders on a side: priority gives the
/// first one less so the second reaches its minimum. The raise goes to the
/// first in priority, not to the last order with a fill.
#[test]
fn the_raise_goes_to_the_first_of_two_partials() {
    let orders = sellers_10_for_7(&[0, 5]);
    let sellers = prepared(&orders);
    let map = ReachableFillMap::build(&sellers, 1_000, 1_000_000).unwrap();
    let mut allocations = map
        .allocate_by_priority(&sellers, U256::from(12u64))
        .unwrap();
    assert_eq!(allocations, fills(&[5, 7]), "both orders partial");

    PairMatcher::raise_first_partial(&sellers, &mut allocations, 1, 0, WholeNote::AsFullFill)
        .unwrap();
    assert_eq!(allocations, fills(&[6, 7]));
}

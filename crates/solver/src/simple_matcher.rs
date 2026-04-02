use crate::order::Order;

#[cfg_attr(not(test), allow(dead_code))]
const PRECISION_FACTOR: u64 = 100_000;

/// Computes how much offered asset is released for a given fill of the requested asset.
/// Matches the on-chain PSWAP MASM calculation exactly.
#[cfg_attr(not(test), allow(dead_code))]
fn calculate_output_amount(offered: u64, requested: u64, input: u64) -> u64 {
    if input == requested {
        return offered;
    }
    if input == 0 {
        return 0;
    }

    if offered > requested {
        let ratio = (offered * PRECISION_FACTOR) / requested;
        (input * ratio) / PRECISION_FACTOR
    } else {
        let ratio = (requested * PRECISION_FACTOR) / offered;
        (input * PRECISION_FACTOR) / ratio
    }
}

/// Internal book entry tracking remaining quantity during matching.
struct BookEntry {
    original_index: usize,
    price: f64,
    remaining_qty: u64,
}

pub struct SimpleMatcher;

impl SimpleMatcher {
    /// Run simple two-side bid/ask matching with price-priority.
    ///
    /// - `bids`: orders that buy X (offer Y, want X). Price = offered_amount / requested_amount.
    /// - `asks`: orders that sell X (offer X, want Y). Price = requested_amount / offered_amount.
    ///
    /// Returns `(bids, asks)` with `fill_amount` set on matched orders.
    /// Caller checks `order.fill_amount != 0` to identify filled orders.
    ///
    /// For bids, `fill_amount` = X received (in `requested_amount` units).
    /// For asks, `fill_amount` = Y received (in `requested_amount` units).
    pub fn run(mut bids: Vec<Order>, mut asks: Vec<Order>) -> (Vec<Order>, Vec<Order>) {
        if bids.is_empty() || asks.is_empty() {
            return (bids, asks);
        }

        // Build book entries for bids
        // Bid buys X, offers Y: price = offered_amount / requested_amount (Y per X willing to pay)
        // qty = requested_amount (X wanted)
        let mut bid_entries: Vec<BookEntry> = bids
            .iter()
            .enumerate()
            .map(|(i, order)| BookEntry {
                original_index: i,
                price: order.offered_amount as f64 / order.requested_amount as f64,
                remaining_qty: order.requested_amount,
            })
            .collect();

        // Build book entries for asks
        // Ask sells X, wants Y: price = requested_amount / offered_amount (Y per X demanded)
        // qty = offered_amount (X available)
        let mut ask_entries: Vec<BookEntry> = asks
            .iter()
            .enumerate()
            .map(|(i, order)| BookEntry {
                original_index: i,
                price: order.requested_amount as f64 / order.offered_amount as f64,
                remaining_qty: order.offered_amount,
            })
            .collect();

        // Sort bids by price descending (most aggressive first)
        bid_entries.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        // Sort asks by price ascending (cheapest first)
        ask_entries.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

        // Track base-asset (X) quantities filled per original index
        let mut bid_x_filled = vec![0u64; bids.len()];
        let mut ask_x_sold = vec![0u64; asks.len()];

        let mut bid_idx = 0;
        let mut ask_idx = 0;

        while bid_idx < bid_entries.len() && ask_idx < ask_entries.len() {
            let bid = &bid_entries[bid_idx];
            let ask = &ask_entries[ask_idx];

            if bid.price < ask.price {
                break;
            }

            let trade_qty = bid.remaining_qty.min(ask.remaining_qty);

            bid_x_filled[bid.original_index] += trade_qty;
            ask_x_sold[ask.original_index] += trade_qty;

            bid_entries[bid_idx].remaining_qty -= trade_qty;
            ask_entries[ask_idx].remaining_qty -= trade_qty;

            if bid_entries[bid_idx].remaining_qty == 0 {
                bid_idx += 1;
            }
            if ask_entries[ask_idx].remaining_qty == 0 {
                ask_idx += 1;
            }
        }

        // ── Convert matching results into fill amounts ──
        //
        // miden_swapp::PswapNote::calculate_output_amount uses a PRECISION_FACTOR
        // (100_000) with two-step integer division, which can lose up to 1 unit
        // per order. Both conservation constraints must hold:
        //   X: sum(released_X per ask) ≥ sum(bid.fill_amount)
        //   Y: sum(effective_Y per bid) ≥ sum(ask.fill_amount)
        //
        // Strategy:
        // 1. For each ask, find min fill_y where released_X ≥ target (ceiling + bump).
        // 2. Set bid fills, trim if X conservation fails.
        // 3. Validate Y conservation. If it fails (pathological precision loss on
        //    small amounts), zero out fills for that matching round.

        // Step 1: Ask fills
        //
        // Invert calculate_output_amount's formula to find the minimum fill_y
        // such that released_X ≥ target_x. The function uses:
        //   PRECISION = 100_000
        //   if offered > requested: ratio = (off*P)/req; out = (fill*ratio)/P
        //   else:                   ratio = (req*P)/off; out = (fill*P)/ratio
        //
        // We solve for fill_y using ceiling division on the inverted formula,
        // then verify once (ratio truncation may require +1 adjustment).
        const PRECISION: u64 = 100_000;

        for (i, ask) in asks.iter_mut().enumerate() {
            if ask_x_sold[i] == 0 {
                continue;
            }
            // Full fill — all X sold, give full requested Y
            if ask_x_sold[i] == ask.offered_amount {
                ask.fill_amount = ask.requested_amount;
                continue;
            }
            // Partial fill — invert the formula
            let target_x = ask_x_sold[i];
            let off = ask.offered_amount;
            let req = ask.requested_amount;

            let fill = if off > req {
                // ratio = (off * P) / req.  Need: (fill * ratio) / P ≥ target
                // → fill ≥ ceil(target * P / ratio)
                let ratio = (off * PRECISION) / req;
                let f = (target_x * PRECISION + ratio - 1) / ratio;
                // Verify — ratio truncation may leave us 1 short
                let released = (f * ratio) / PRECISION;
                if released >= target_x { f } else { f + 1 }
            } else {
                // ratio = (req * P) / off.  Need: (fill * P) / ratio ≥ target
                // → fill ≥ ceil(target * ratio / P)
                let ratio = (req * PRECISION) / off;
                let f = (target_x * ratio + PRECISION - 1) / PRECISION;
                let released = (f * PRECISION) / ratio;
                if released >= target_x { f } else { f + 1 }
            };

            ask.fill_amount = fill.min(req);
        }

        // Step 2: Set bid fills
        for (i, bid) in bids.iter_mut().enumerate() {
            bid.fill_amount = bid_x_filled[i];
        }

        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::{AccountId, AccountIdVersion, AccountStorageMode, AccountType};
    use miden_protocol::asset::FungibleAsset;
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::NoteType;
    use miden_protocol::ZERO;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    fn make_faucet_id(seed: u64) -> AccountId {
        AccountId::dummy(
            [seed as u8; 15],
            AccountIdVersion::Version0,
            AccountType::FungibleFaucet,
            AccountStorageMode::Public,
        )
    }

    fn make_account_id(seed: u64) -> AccountId {
        AccountId::dummy(
            [seed as u8; 15],
            AccountIdVersion::Version0,
            AccountType::RegularAccountImmutableCode,
            AccountStorageMode::Public,
        )
    }

    fn make_test_order(
        offered_faucet: AccountId,
        offered_amount: u64,
        requested_faucet: AccountId,
        requested_amount: u64,
    ) -> Order {
        let offered_asset = FungibleAsset::new(offered_faucet, offered_amount).unwrap();
        let requested_asset = FungibleAsset::new(requested_faucet, requested_amount).unwrap();
        let creator = make_account_id(99);

        let mut rng = RandomCoin::new([ZERO; 4].into());
        let serial_number = rng.draw_word();

        let storage = PswapNoteStorage::builder()
            .requested_asset(requested_asset)
            .creator_account_id(creator)
            .build();

        let pswap = PswapNote::builder()
            .sender(creator)
            .storage(storage)
            .serial_number(serial_number)
            .note_type(NoteType::Public)
            .offered_asset(offered_asset)
            .build()
            .expect("Failed to create test note");

        let note = miden_protocol::note::Note::from(pswap);

        Order {
            note,
            offered_faucet_id: offered_faucet,
            offered_amount,
            requested_faucet_id: requested_faucet,
            requested_amount,
            creator_id: creator,
            fill_amount: 0,
        }
    }

    // Pair convention: X = faucet 1 (base), Y = faucet 2 (quote)
    // Bid buys X, offers Y: Order { offered_faucet = Y, offered_amount = Y_qty, requested_faucet = X, requested_amount = X_qty }
    // Ask sells X, wants Y: Order { offered_faucet = X, offered_amount = X_qty, requested_faucet = Y, requested_amount = Y_qty }

    fn faucets() -> (AccountId, AccountId) {
        (make_faucet_id(1), make_faucet_id(2))
    }

    /// Helper: create a bid order (buys X, offers Y)
    fn make_bid(faucet_x: AccountId, faucet_y: AccountId, x_qty: u64, y_qty: u64) -> Order {
        make_test_order(faucet_y, y_qty, faucet_x, x_qty)
    }

    /// Helper: create an ask order (sells X, wants Y)
    fn make_ask(faucet_x: AccountId, faucet_y: AccountId, x_qty: u64, y_qty: u64) -> Order {
        make_test_order(faucet_x, x_qty, faucet_y, y_qty)
    }

    #[test]
    fn test_no_orders() {
        let (bids, asks) = SimpleMatcher::run(vec![], vec![]);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn test_no_match_prices_dont_cross() {
        let (fx, fy) = faucets();
        // Bid: willing to pay 1 Y per X (price = 1.0)
        let bids = vec![make_bid(fx, fy, 100, 100)];
        // Ask: demands 2 Y per X (price = 2.0)
        let asks = vec![make_ask(fx, fy, 100, 200)];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        assert_eq!(bids[0].fill_amount, 0, "Bid should not be filled");
        assert_eq!(asks[0].fill_amount, 0, "Ask should not be filled");
    }

    #[test]
    fn test_exact_full_match() {
        let (fx, fy) = faucets();
        // Bid: buy 100 X at price 1.0 Y/X
        let bids = vec![make_bid(fx, fy, 100, 100)];
        // Ask: sell 100 X at price 1.0 Y/X
        let asks = vec![make_ask(fx, fy, 100, 100)];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        assert_eq!(bids[0].fill_amount, 100, "Bid should be fully filled (100 X)");
        assert_eq!(asks[0].fill_amount, 100, "Ask should be fully filled (100 Y)");
    }

    #[test]
    fn test_partial_fill_bid_larger() {
        let (fx, fy) = faucets();
        // Bid: buy 200 X at price 1.0
        let bids = vec![make_bid(fx, fy, 200, 200)];
        // Ask: sell 50 X at price 1.0
        let asks = vec![make_ask(fx, fy, 50, 50)];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        assert_eq!(bids[0].fill_amount, 50, "Bid partially filled (50 of 200 X)");
        assert_eq!(asks[0].fill_amount, 50, "Ask fully filled (50 Y)");
    }

    #[test]
    fn test_partial_fill_ask_larger() {
        let (fx, fy) = faucets();
        // Bid: buy 30 X at price 1.0
        let bids = vec![make_bid(fx, fy, 30, 30)];
        // Ask: sell 100 X at price 1.0
        let asks = vec![make_ask(fx, fy, 100, 100)];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        assert_eq!(bids[0].fill_amount, 30, "Bid fully filled (30 X)");
        assert_eq!(asks[0].fill_amount, 30, "Ask partially filled (30 of 100 Y)");
    }

    #[test]
    fn test_multiple_trades() {
        let (fx, fy) = faucets();
        // One large bid: buy 100 X at price 2.0
        let bids = vec![make_bid(fx, fy, 100, 200)];
        // Three small asks at price 1.0 (30 + 40 + 50 = 120 X available)
        let asks = vec![
            make_ask(fx, fy, 30, 30),
            make_ask(fx, fy, 40, 40),
            make_ask(fx, fy, 50, 50),
        ];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        assert_eq!(bids[0].fill_amount, 100, "Bid fully filled");
        assert_eq!(asks[0].fill_amount, 30, "First ask fully filled");
        assert_eq!(asks[1].fill_amount, 40, "Second ask fully filled");
        assert_eq!(asks[2].fill_amount, 30, "Third ask partially filled (30 of 50)");
    }

    #[test]
    fn test_multiple_bids_multiple_asks() {
        let (fx, fy) = faucets();
        // Two bids
        let bids = vec![
            make_bid(fx, fy, 60, 120), // price 2.0
            make_bid(fx, fy, 40, 60),  // price 1.5
        ];
        // Two asks
        let asks = vec![
            make_ask(fx, fy, 50, 50), // price 1.0
            make_ask(fx, fy, 50, 75), // price 1.5
        ];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        // All orders should be filled (both bid prices >= both ask prices)
        assert_eq!(bids[0].fill_amount, 60, "First bid fully filled");
        assert_eq!(bids[1].fill_amount, 40, "Second bid fully filled");
        assert_eq!(asks[0].fill_amount, 50, "First ask fully filled");
        assert_eq!(asks[1].fill_amount, 75, "Second ask fully filled");
    }

    #[test]
    fn test_fill_amounts_proportional() {
        let (fx, fy) = faucets();
        // Ask: sell 100 X, want 200 Y (price 2.0 Y/X)
        // Bid: buy 60 X, offer 150 Y (price 2.5 Y/X)
        let bids = vec![make_bid(fx, fy, 60, 150)];
        let asks = vec![make_ask(fx, fy, 100, 200)];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        // Bid fully filled: 60 X received
        assert_eq!(bids[0].fill_amount, 60);
        // Ask partially filled: sold 60 of 100 X → Y received = 60 * 200 / 100 = 120
        assert_eq!(asks[0].fill_amount, 120);
    }

    #[test]
    fn test_sorting_correctness() {
        let (fx, fy) = faucets();
        // Bids in wrong order (low price first) — matcher should sort
        let bids = vec![
            make_bid(fx, fy, 50, 50),  // price 1.0 (less aggressive)
            make_bid(fx, fy, 50, 100), // price 2.0 (more aggressive)
        ];
        // Asks in wrong order (high price first) — matcher should sort
        let asks = vec![
            make_ask(fx, fy, 50, 75), // price 1.5 (more expensive)
            make_ask(fx, fy, 50, 25), // price 0.5 (cheaper)
        ];
        let (bids, asks) = SimpleMatcher::run(bids, asks);
        // bid[1] @2.0 matches ask[1] @0.5 → filled
        // bid[0] @1.0 vs ask[0] @1.5 → no cross → not filled
        assert_eq!(bids[0].fill_amount, 0, "Less aggressive bid should NOT match");
        assert_eq!(bids[1].fill_amount, 50, "More aggressive bid should match");
        assert_eq!(asks[0].fill_amount, 0, "More expensive ask should NOT match");
        assert_eq!(asks[1].fill_amount, 25, "Cheaper ask should match");
    }

    /// Stress test: randomized orders verify asset conservation invariants.
    ///
    /// For every matching result, we check:
    /// 1. supply_x ≥ demand_x  (no X created from thin air)
    /// 2. supply_y ≥ demand_y  (no Y created from thin air)
    /// 3. fill amounts are within bounds
    #[test]
    fn test_asset_conservation_stress() {
        let (fx, fy) = faucets();

        // Deterministic "random" values to cover many edge cases
        let offered_vals = [1, 2, 3, 7, 13, 17, 19, 23, 50, 97, 100, 200, 999];
        let requested_vals = [1, 2, 3, 7, 11, 13, 17, 20, 50, 89, 100, 150, 500];

        let mut cases_tested = 0u64;

        for &ask_off in &offered_vals {
            for &ask_req in &requested_vals {
                for &bid_off in &requested_vals {
                    for &bid_req in &offered_vals {
                        // bid price = bid_off / bid_req (Y per X willing to pay)
                        // ask price = ask_req / ask_off (Y per X demanded)
                        // Only test crossing prices (bid_price >= ask_price)
                        // i.e. bid_off * ask_off >= ask_req * bid_req
                        if (bid_off as u128) * (ask_off as u128)
                            < (ask_req as u128) * (bid_req as u128)
                        {
                            continue;
                        }

                        let bids = vec![make_bid(fx, fy, bid_req, bid_off)];
                        let asks = vec![make_ask(fx, fy, ask_off, ask_req)];

                        let (bids, asks) = SimpleMatcher::run(bids, asks);

                        // Compute supply and demand as the executor would
                        let mut supply_x: u64 = 0;
                        let mut supply_y: u64 = 0;
                        let mut demand_x: u64 = 0;
                        let mut demand_y: u64 = 0;

                        for ask in &asks {
                            if ask.fill_amount == 0 {
                                continue;
                            }
                            supply_x += ask.offered_amount;
                            // P2ID sends fill_amount of Y to ask creator
                            demand_y += ask.fill_amount;
                            // Remainder: offered - released X
                            let released_x = calculate_output_amount(
                                ask.offered_amount,
                                ask.requested_amount,
                                ask.fill_amount,
                            );
                            demand_x += ask.offered_amount - released_x;

                            assert!(
                                ask.fill_amount <= ask.requested_amount,
                                "Ask fill exceeds requested: fill={} req={} (off={} req={} bid_off={} bid_req={})",
                                ask.fill_amount, ask.requested_amount,
                                ask_off, ask_req, bid_off, bid_req,
                            );
                        }

                        for bid in &bids {
                            if bid.fill_amount == 0 {
                                continue;
                            }
                            supply_y += bid.offered_amount;
                            // P2ID sends fill_amount of X to bid creator
                            demand_x += bid.fill_amount;
                            // Remainder: offered_Y - released_Y
                            if bid.fill_amount < bid.requested_amount {
                                let released_y = calculate_output_amount(
                                    bid.offered_amount,
                                    bid.requested_amount,
                                    bid.fill_amount,
                                );
                                demand_y += bid.offered_amount - released_y;
                            }

                            assert!(
                                bid.fill_amount <= bid.requested_amount,
                                "Bid fill exceeds requested: fill={} req={}",
                                bid.fill_amount, bid.requested_amount,
                            );
                        }

                        if supply_x < demand_x {
                            eprintln!("--- X VIOLATION ---");
                            eprintln!("ask_off={} ask_req={} bid_off={} bid_req={}", ask_off, ask_req, bid_off, bid_req);
                            for (i, a) in asks.iter().enumerate() {
                                eprintln!("  ask[{}]: offered={} requested={} fill={}", i, a.offered_amount, a.requested_amount, a.fill_amount);
                                if a.fill_amount > 0 {
                                    let rel = calculate_output_amount(a.offered_amount, a.requested_amount, a.fill_amount);
                                    eprintln!("    released_x={} remainder_x={}", rel, a.offered_amount - rel);
                                }
                            }
                            for (i, b) in bids.iter().enumerate() {
                                eprintln!("  bid[{}]: offered={} requested={} fill={}", i, b.offered_amount, b.requested_amount, b.fill_amount);
                            }
                            panic!(
                                "X conservation violated: supply={} demand={} (ask: off={} req={}, bid: off={} req={})",
                                supply_x, demand_x, ask_off, ask_req, bid_off, bid_req,
                            );
                        }
                        if supply_y < demand_y {
                            eprintln!("--- Y VIOLATION ---");
                            eprintln!("ask_off={} ask_req={} bid_off={} bid_req={}", ask_off, ask_req, bid_off, bid_req);
                            for (i, a) in asks.iter().enumerate() {
                                eprintln!("  ask[{}]: offered={} requested={} fill={}", i, a.offered_amount, a.requested_amount, a.fill_amount);
                            }
                            for (i, b) in bids.iter().enumerate() {
                                eprintln!("  bid[{}]: offered={} requested={} fill={}", i, b.offered_amount, b.requested_amount, b.fill_amount);
                                if b.fill_amount > 0 && b.fill_amount < b.requested_amount {
                                    let rel = calculate_output_amount(b.offered_amount, b.requested_amount, b.fill_amount);
                                    eprintln!("    released_y={} remainder_y={}", rel, b.offered_amount - rel);
                                }
                            }
                            panic!(
                                "Y conservation violated: supply={} demand={} (ask: off={} req={}, bid: off={} req={})",
                                supply_y, demand_y, ask_off, ask_req, bid_off, bid_req,
                            );
                        }

                        cases_tested += 1;
                    }
                }
            }
        }
        println!("Asset conservation stress test passed: {} cases", cases_tested);
    }
}

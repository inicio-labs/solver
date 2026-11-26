# 7. Bounded clearing price

- **Status:** Accepted, implemented in the matcher and the price API (`/v2/pair-price` and `/v2/swap-eta` quote with the same walk; see the price API's [Pairs with a price band](../price-api.md#pairs-with-a-price-band)).
- **Date:** 2026-10-29
- **Deciders:** Vaibhav Jindal.
- **Scope:** PSWAP batch clearing in the 0.17 solver, per configured pair.
- **Related:** [ADR 0004](0004-binance-bookticker-prices.md) (the Binance mid), [ADR 0005](0005-maker-self-trade-prevention.md) (self-trade prevention at admission, removed; replaced by [ADR 0009](0009-self-trade-prevention-at-submit.md)), [market maker guide](../market-maker-guide.md), issue [#62](https://github.com/inicio-labs/solver/issues/62) (deferred items).

## Context

Each round clears every pair at one price: the Binance mid `M` (ADR 0004). A sell is eligible when its limit is at or below `M` after the fee, a buy when its limit is at or above it. A maker quoting a spread around the mid therefore never trades with users: its ask is above `M`, its bid below. Only users who happen to meet each other at the mid trade, and makers cannot use our flow to rebalance inventory or close arbitrage.

We want makers to have leeway while users stay protected: the batch price may move away from the mid, but only inside a fixed band, and only as far as the book requires.

## Decision

### The rule

Each pair has a band of `band_bps` basis points on each side of the mid (`[[pairs]] band_bps`, at most 100; 0 keeps today's rule). Per pair, per round:

```text
L = M × (1 − band),  H = M × (1 + band)
P = the price in [L, H] that trades the most volume;
    among those, the one closest to M
```

Nothing crossing inside the band means no trade at a bounded price (see the fallback). Every fully filled order trades at `P`. A partly filled PSWAP order is still paid at its own note ratio, never worse than its limit.

### How P is found: one walk over the book

The book already keeps each side of a pair sorted best first: sells by ascending limit, buys by descending limit. The walk pairs the best sell with the best buy, takes the smaller of their remaining sizes, and moves past whichever is used up. It stops when:
- a side runs out;
- `max_orders_per_side` orders have counted on a side;
- the next sell is above `H`, or the next buy is below `L`;
- the next sell's limit is above the next buy's limit.

The volume reached is the most any price in the band can trade. It trades at every price between the last pair's limits `[s*, d*]`, so

```text
P = M    if s* ≤ M ≤ d*
P = s*   if M < s*      (more buyers: the cheapest ask that clears)
P = d*   if M > d*      (more sellers: the highest bid that clears)
```

`P` is always inside the band and never needs clipping. When `P` is a limit, it is a real order's limit.

Limits are fee-inclusive and exact: a sell offering `X` for `Y` trades at `P ≥ Y·10⁶ / (X·(10⁶ − fee))`, a buy at `P ≤ X·10⁶ / (Y·(10⁶ + fee))`, the same thresholds as admission. Prices are ratios compared by cross-multiplication in 256 bits. Sizes are in quote at the mid without dividing, so the smallest orders of an 18-decimal token still count. An overflow is an error, never a panic.

Cost per pair and round: one index seek per side plus at most `2 × max_orders_per_side` steps.

### Self-trade prevention while pricing (rule B, removed)

Rule B sat out the older order of every bid and ask of one maker that clashed inside the band, so a maker's own crossed pair could not set the price (a tiny ask at +25 bps and a tiny bid at +26 bps would otherwise set `P` at +25 bps for every buyer). It is removed: a maker's orders that cross at their submitted prices are cancelled at submit ([ADR 0009](0009-self-trade-prevention-at-submit.md)), so the walk does not look at makers. The one exception is accepted: a pair that rounding leaves slightly crossed on a nearly filled remainder can set the price, as it can already fill itself ([#93](https://github.com/inicio-labs/solver/issues/93)).

### Admission: unchanged

Admission and matching run at the chosen price exactly as before. Since ADR 0005 was removed, admission does not look at makers: their crossing orders are cancelled at submit (ADR 0009).

### Fallback to the mid

```text
band_bps = 0                                  → today's rule at M
the walk finds nothing                        → today's rule at M
admission + matching at P trade nothing
  (no cross, insolvent, resource limit, error) → today's rule at M
otherwise                                     → P
```

When `P = M` matching runs once. The fallback keeps every pair trading wherever today's rule trades.

### Configuration and observability

- `[[pairs]] band_bps`: default 0, at most 100 (1%).
- `[[pairs]] quote_ttl_ms`: the pair's own quote freshness, else `binance.quote_ttl_ms`. Both the matcher and the price API judge a pair by it.
- `[[pairs]] reference_amount`: the size `/v2/pair-price` quotes at on a pair with a band, in whole `asset_y` tokens; required when `band_bps` is above 0.
- Startup logs each pair's band, TTL and reference amount.
- The price API runs the same walk on its mirror of the book's levels, with one probe order added: the reference amount for `/v2/pair-price`, the order being checked for `/v2/swap-eta`, which is then judged at that price. The matcher's walk does not look at makers either, so both walk the same orders; a level counts as one order toward the cap (issue #64).
- Metrics: `solver_matcher_bounded_off_mid_total`, `solver_matcher_bounded_fallback_total{reason}` (`walk_found_nothing`, `nothing_at_price`, `walk_error`).
- A pair that clears off the mid logs the mid, `P`, the distance in bps, `s*`, `d*`, the walk's volume, its stop reason and the order that set the price.

## Consequences

- Makers can sell to users above the mid and buy below it, up to the band, when the book needs them. Users never pay outside the band, and the price moves only as far as the book requires.
- Pairs without a band behave exactly as before.
- **Accepted costs of the simple fallback.** It keeps `P` whenever something trades there, even if the mid would trade more. On random books in the reference model (3,000 per set-up, about half with a self-crossed maker), 39 to 56 trade less than today's rule, almost all because a maker's stale crossed order no longer trades. In some books the price moves with no extra volume, on average 8 bps and at most 19. A stricter fallback (keep `P` only if it trades strictly more than the mid) removes both but runs matching twice; it was not chosen.
- **Partial fills** are still paid at the order's own limit until protocol#4041 lands.
- **Known gaps, deferred to #62:**
  - no collar: with thin makers the batch price can reach the band edge through a small extra ask placed there;
  - an all-or-nothing order can leave the walk on the mid while nothing executes there;

## Alternatives considered

- **Mid only (today).** Gives makers no leeway.
- **Midpoint of the max-volume range** (some derivatives venues). Moves the price half-way into the range even when the mid trades the same volume.
- **Multi-step call-auction tie-breaks** (surplus, then market pressure, then a reference price, as some cash equity auctions do). More rules, and on our simulated books no better for users than "closest to the mid".
- **Most surplus (CoW style).** The surplus depends on how far orders' limits are from the price, so wide limits pull the price toward them.
- **No self-trade prevention while pricing.** Simpler, and identical on normal books, but it let a maker set the price with its own crossed pair. Chosen once self-trade prevention at submit (ADR 0009) cancelled such pairs before they reach the book (see rule B, removed).
- **Strict fallback (strictly more volume than the mid).** Safer on all-or-nothing books, but more complex; the simple fallback was chosen.

## Verification

- A Python reference model of this design: hand-written cases for blocking, wrong sit-outs or cancels, and the band's edges, plus property runs on random books. Prices never leave the band, no fill crosses its own limit, no maker fills on both sides, and nothing freezes where today trades.
- Rust tests: the walk against a brute-force oracle on 2,000 random books (every candidate price), the named books from the design review, matcher tests for the band and both fallbacks, and every existing matcher test unchanged with band 0. Four deliberate mutations of the walk each fail several tests.
- An integration test on the mock chain (`integration_bounded_price`): a maker's ask 20 bps above the mid never settles without a band and settles at its own limit with one, with exact payback amounts.
- Scenarios on the mock chain with real orders and the price API (`integration_bounded_price_scenarios`): both band edges, an ask just past the edge, a Binance move into the band, a remainder in the next batch, the fallback to the mid with all-or-nothing orders, a fee, a banded and an unbanded pair, a partial fill at its own limit, and a random book checked for limits, the band and conservation.

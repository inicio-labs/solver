# 9. Self-trade prevention at submit

- **Status:** Accepted, implemented, always on. It replaced [ADR 0005](0005-maker-self-trade-prevention.md) after a soak with both rules running ([runbook](../postgres-runbook.md#self-trade-prevention-at-submit)); ADR 0005's rule and both switches (`engine.maker_self_trade_prevention`, `engine.maker_cancel_crossing_on_submit`) are removed.
- **Date:** 2026-11-25
- **Deciders:** Vaibhav Jindal. Maker requirements from Jordy (Pragma), 2026-10-23 and 2026-11-23.
- **Scope:** maker orders submitted through the maker gateway (ADR 0003). Users' orders are never touched.
- **Related:** [ADR 0003](0003-market-maker-v1-gateway.md) (lineages, stops, cutoffs), [ADR 0005](0005-maker-self-trade-prevention.md) (the rule this replaces), [ADR 0008](0008-maker-orders-live-at-accept.md) (live at accept), [market maker guide](../market-maker-guide.md#12-self-trade-prevention), [runbook](../postgres-runbook.md#self-trade-prevention-at-submit), issues [#71](https://github.com/inicio-labs/solver/issues/71) (pending lineages whose note never lands), [#76](https://github.com/inicio-labs/solver/issues/76) (the reason on a settlement result), [#78](https://github.com/inicio-labs/solver/issues/78) (expiry events), [#80](https://github.com/inicio-labs/solver/issues/80) (stopped rows stored `stopped`), [#93](https://github.com/inicio-labs/solver/issues/93) (rounding on nearly filled remainders, a known limitation).

## Context

ADR 0005 decided in the matcher, every round, among a maker's eligible orders: the side of the newest order trades, the others sit out, and they are cancelled only when a trading order is reserved. It costs every maker on every pair each round, keeps state in the executor (sit-outs travel with the settlement), and a maker learns of a cancel only at a reservation. Makers asked for the simpler rule exchanges use: a new order cancels the older crossing one.

## Decision

> For one maker on one pair, when a bid and an ask cross at the prices the maker submitted (bid ≥ ask), the one with the **lower seq is cancelled**. This holds whatever order they arrive in, and whatever has happened to the higher one since.

- **Which orders are cancelled is what a replay of the maker's commands in seq order gives** (gaps allowed). The rule uses only seqs and prices, never arrival time.
- **Checked when a `SubmitOrder` is accepted**, in the intake round that claims it. That is the only moment a new pair can appear: a remainder or a returning order keeps its lineage's seq and original price.
- **The higher order counts in any state:** pending, live, settling, cancelled by its maker, filled, expired, or itself beaten by an even newer order.
- **The lower one is cancelled only if it is open:** pending, active, waiting, settling, or a parent the watcher retired while its settlement is unresolved. A filled order can't be undone.
- **A cancel is permanent,** with the whole lineage, like the maker's own `CancelOrder`: a stop row with `source = 'stp_on_submit'` and `caused_by_lineage`: the lowest-seq newer crossing order seen when it was stopped (Limits, 8).
- **"Dead" means only that the maker cancelled it** (a stop row or a cutoff). A dead submit still cancels its older crossing orders; it never goes live and gets no event of its own: its lineage is closed (`rejected`), so the watcher never reports it either.
- **The rule never looks at the clock.** Expiry stays the matcher's (`maker_settlement_buffer_ms` before it). An old order near or past its expiry that a newer one crosses gets the normal CANCELLED until #78.
- **Cross test:** two orders in opposite directions on one pair cross when `offered_x × offered_y ≥ requested_x × requested_y`, in u128, at the submitted amounts. Lineages claimed before the amounts were stored never cross.
- **One maker ID is one group.**

### The round

```
 cancels ─► claims ─► read ─► decide ─► write ─► commit ─► publish
 (as       (no go     OPEN +   pure      losers,  one tx    LineageCancelled,
 before)    live)     NEWER              then               replies,
                                         winners            wake streams
```

1. The round's cancels, then its claims, as before, but a claimed submit does not go live yet. A replay, `already_registered` or a conflict takes no further part. A submit with more than 1,000 of its maker's orders on the pair above its seq is refused before its claim (`FAILED_PRECONDITION`, stored as its result), so step 2 reads at most about 1,000 rows of history per claim.
2. For each (maker, market) the claims name, two reads in the round's transaction: **OPEN** (the maker's open orders on the pair, through `eligible_lineages`: rows active, settling or waiting; a retired parent with an unresolved settlement; accepted lineages without a row, which include this round's claims) and **NEWER** (every lineage above the round's lowest claimed seq, from the table, in any state).
3. `gateway::self_trade::decide`: for each claimed submit and each crossing lineage of its maker, the lower seq loses if it is open. Pure; property-tested against a seq-order replay.
4. The losers' stop rows, then their lineage states (pending losers, and dead submits, are closed `rejected`), then their events. A round's statement count does not grow with its losers (below 1,000 of them: stop rows are written 1,000 per statement). Then the winners go live at accept (ADR 0008) in arrival order; with live at accept off they stay pending until their note lands.
5. After the commit: `LineageCancelled` to the matcher for every loser, after the round's cancels and before its new orders; then the replies; then the event streams are woken if the round appended any event. A stream awake for another reason can read a CANCELLED before its submit's reply, as with LIVE (ADR 0008).

### What happens to a loser

| The loser is | Effects |
|---|---|
| A pending lineage, including the entering submit (it arrived after a newer crossing order) | Stop. Lineage `rejected`. CANCELLED `self_trade_prevention` naming its submitted note. Never LIVE. A submit's reply stays `accepted`. |
| A row `active` or `waiting`, including one near or past its expiry | Stop. CANCELLED naming that row's note and depth. The row stays as it is, hidden by the views, until #80. A `waiting` row is stored `stopped` when its note lands. |
| `settling` | Stop. No event now. Its `SettlementResolved` reports it: CANCELLED (reason `self_trade_prevention`), FULLY_FILLED or SPENT_ELSEWHERE. |

Only stops that are new have effects: if the maker's own cancel got there first, nothing more happens.

### Storage (migration 2026-11-25-000001)

- `maker_lineages.offered_amount`, `requested_amount`: written once by the claim. Old lineages keep NULL.
- `orders.maker_id`: copied from the claim by a `BEFORE INSERT` trigger on every insert path. With private notes only, the claim always comes first.
- Indexes for OPEN (`orders (maker_id, market)` on open rows; `maker_lineages (maker_id, market)` on pending rows without an accept block) and NEWER (`maker_lineages (maker_id, market, root_seq)`). `maker_lineages` gets the vacuum settings `orders` has (1% + 1000 dead rows, index pass on): every claim leaves `pending` once, and the table is never pruned.
- `maker_stops.source` (`maker`, `cap`, `escape_hatch`, `stp_reservation`, `stp_on_submit`) and `caused_by_lineage`. Every stop row is written by `stop_lineages_tx`.
- View `eligible_lineages`: `maker_lineages` minus the maker's stops and cutoffs. Read directly by intake, so the startup access check covers it.
- **History:** `maker_lineages` rows are never deleted. The newer read needs them; a test pins that no code deletes one.

### Locks

Intake takes the round's maker locks up front, in ID order. The step writes stop rows, lineage states and events, never another order's row, so ADR 0006's order holds. A reservation takes the same maker lock: either it sees the stop and fails (counted), or the step sees the order `settling`. Any round with a submit takes the owner-epoch share lock first.

### Limits

Where the result differs from a replay in seq order, or the rule leaves a gap:

1. A cancel sent with a lower seq than its own submit stops that order for good.
2. A fill can't be undone: an older order can trade before a late newer one arrives.
3. **Known limitation:** rounding on a nearly filled remainder can leave it slightly across the maker's other side, and this rule doesn't cancel it ([#93](https://github.com/inicio-labs/solver/issues/93)). With ADR 0005 removed nothing does, so the two can fill each other. Rule B of the bounded price (ADR 0007) still keeps such a pair from setting a band price.
4. Lineages claimed before the storage migration never cross. ADR 0005 covered the pairs open before the switch-on until they drained; it was removed only after that (runbook).
5. Who gets the credit when a maker cancel and a crossing submit race (the final state is the same).
6. The same note resent with a new request ID and a higher seq gets `already_registered` and keeps its first seq.
7. A seq counter that goes backwards makes the maker's new crossing orders lose to its own history. Far behind (more than 1,000 of its orders on the pair above a submit), the submit is refused with `FAILED_PRECONDITION`, where a replay would accept it, and cancel it if a newer one crosses it. `solver_maker_stp_newer_rows_total` counts what rounds read.
8. Which newer order a stop names (`caused_by_lineage`) can differ when orders arrive out of seq order: the lowest-seq newer crossing order seen when it was stopped; one that arrives later does not replace it.
9. With live at accept off, a crossing re-quote takes the replaced order out 3 to 4 s before the new one is in, and for good if the new note fails.
10. A stop on an order the matcher chose but did not reserve yet fails that reservation once (`solver_executor_reservation_stopped_input_total`).
11. Pending lineages whose note never lands stay open until cancelled (#71).
12. A submit with less than the settlement buffer to its expiry is accepted and never trades, but still cancels the maker's older crossing orders (`solver_maker_submits_near_expiry_total`).

## Consequences

- ADR 0005 ran beside this rule during the soak, then was removed with both switches. Its stop source, `stp_reservation`, stays readable on old rows. A periodic read-only count of crossed open pairs, `solver_maker_crossed_open_pairs`, must stay 0 (runbook).
- A maker can be cancelled by its own late retry: it must take every seq from one rising counter (guide, section 6).
- Makers' clients see what the market maker guide lists under "What your client must handle" (section 12).

## Evidence

- `gateway/self_trade/tests.rs`: 20,000 random runs (two makers, two pairs, out-of-order arrival, retries, cancels, cut-offs, orders finishing between rounds): every lineage that did not finish is stopped exactly as a seq-order replay stops it, no round leaves a crossed pair open, every round cancels only open orders, each naming the lowest-seq newer crossing order it read, and lets exactly its still-open claims go live; the cases of this ADR's rule and tables in every arrival order and split into rounds; six deliberate breaks caught; the cross test checked against an independent price comparison on 20,000 random sizes.
- `db/stp_tests.rs`: every case of the tables above in PostgreSQL, races with the maker's cancel, the watcher and a reservation (two sessions, both orders of winning), and a round's statement count independent of its losers.
- The change, in eight PRs: [#96](https://github.com/inicio-labs/solver/pull/96) (private notes only), [#97](https://github.com/inicio-labs/solver/pull/97) (storage), [#98](https://github.com/inicio-labs/solver/pull/98) (stops), [#99](https://github.com/inicio-labs/solver/pull/99) (the rule), [#100](https://github.com/inicio-labs/solver/pull/100) (settlement reason), [#101](https://github.com/inicio-labs/solver/pull/101) (the check), [#102](https://github.com/inicio-labs/solver/pull/102) (docs), [#103](https://github.com/inicio-labs/solver/pull/103) (mock-chain scenario).

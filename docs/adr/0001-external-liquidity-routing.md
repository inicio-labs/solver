# 1. External liquidity routing — RFQ websocket to other DEXes

- **Status:** Accepted (implemented)
- **Date:** 2026-06-24
- **Deciders:** solver team
- **Related:** [docs/external-liquidity-routing.md](../external-liquidity-routing.md) (as-built reference), ADR [0002](0002-filler-sdk.md) (the filler SDK)

## Context

Orders the matcher can't cross internally — e.g. an `IMIDEN→IUSDT` note with no
opposing `IUSDT→IMIDEN` — sit unmatched in the in-memory order book. On the live
devnet this is the *common* case: flow is one-directional, so genuine liquidity idles.
We want to offload that residual to **external DEXes** without giving up custody,
without a price negotiation, and without destabilising the internal matcher.

Two PSWAP properties shape the design:

1. A PSWAP note's rate is **fixed on-chain** and fills are permissionless — so this is
   *liquidity discovery* ("how much can you take?"), not price negotiation.
2. "Handing over" a note is sending its **public serialized bytes**, not transferring
   custody — the DEX consumes on its own gas; if nobody fills, the note stays ours.

## Decision

Add an opt-in (`router_enabled`, default off) external pass driven by the matcher over
the one in-memory order book:

1. **Central-book model.** The matcher's `OrderBook` is the single source of truth and
   the decision hub (internal cross → executor, else external → a DEX whose quote it
   clears). No DB poll, no second copy of the book.
2. **Standing-quote RFQ.** Each allow-listed DEX posts a `{price, quantity}` quote per
   registered pair and refreshes it before a TTL. The matcher matches its idle notes
   against the cached quotes every tick. (No per-order `Ask`/reply round-trip.)
3. **Permissionless handover.** The matcher hands the DEX the serialized note bytes;
   the DEX **self-consumes on-chain**. The existing ingest `consumed_notes` →
   `consumed_rx` path detects the fill and drops the note — no executor change.
4. **Park / unpark for in-flight.** A handed-over note is removed from the rate index
   (counter decremented) but its `Order` is kept; reactivation after an in-flight TTL
   is O(expiring) via a time-ordered `park_queue`. See ADR rationale below.
5. **Willingness-only selection (no oracle, no decimals).** A PSWAP note's rate is
   fixed on-chain, so the maker's price is guaranteed however the note fills.
   `select_notes` is therefore a pure base-unit **willingness** cross — does the DEX's
   quote accept the note's rate (`requested·price_den ≤ price_num·offered`)? Exact
   `u128`, no oracle prices, no `10^decimals` scaling: the solver is a matchmaker here,
   not a price authority over a rate the chain already binds.
6. **Router = transport only**, on its own OS thread + multi-thread runtime (mirrors
   `spawn_price_api_thread`); two `Send` channels to the matcher (`watch` quotes in,
   `mpsc` handovers out via `try_send`).
7. **Allow-list auth** via `SOLVER_ROUTER_TOKENS` (env), constant-time check at the
   websocket upgrade.
8. **Handover carries only the note + fill amount.** The DEX receives the serialized
   note (which binds its own rate on-chain) and the requested-token `fill_amount` —
   nothing else is needed to self-consume. An earlier draft also echoed the DEX's
   quoted `fill_price` for a future overfill protocol, but the note already binds the
   rate and no overfill protocol exists yet, so it was dropped as dead wire weight (to
   be re-derived from the quote if/when overfill lands). See ADR
   [0002](0002-filler-sdk.md) and the as-built doc.

## Reasoning / alternatives considered

- **Standing quotes vs per-order Ask/reply.** Because terms are fixed on-chain, a DEX
  only needs to express price + capacity once; re-asking per note adds latency and
  chat for no information gain. Rejected per-order RFQ.
- **Park/unpark vs a new in-flight flag on the matching gates.** A bespoke flag would
  mean editing the proven `best_order`/`has_orders`/`active_pair_count` paths and risk
  counter drift. Park = reuse the existing index-remove + counter-decrement, keep the
  struct. A parked note is *automatically* invisible to matching (it isn't in the
  index), so the gates need **zero change**. This is the riskiest area, so we minimised
  new logic by construction. (Removing `active_pair_count` entirely was considered and
  rejected — it turns an O(1) gate into a scan.)
- **Pure matchmaker, not a price authority.** Since a note's rate is fixed on-chain, the
  solver neither gains nor risks value by routing any *willing* note — the maker gets
  their exact rate regardless. So routing does **not** re-check the quote or the note
  against an oracle. An earlier draft measured a retention "edge" against oracle mid and
  rejected off-market quotes (`router_min_export_edge_bps` / `router_quote_max_deviation_bps`);
  that was value-retention + anti-cherry-pick *policy*, not safety, and it was removed for
  v1 (the oracle-price and per-token-decimals dependencies go with it). It can return when
  real external DEXes and real surplus make retention worth policing.
- **`try_send` for handovers.** The matcher tick is fund-critical and runs on the
  main-thread LocalSet; it must never block on a slow DEX socket. A full channel drops
  the handover (counted), and the note reactivates via TTL — safe degradation.
- **DEX self-consumes (no executor change).** Keeps custody/gas with the DEX and avoids
  touching the settlement path in v1.

## Consequences

- **Flow leak (accepted).** A trusted DEX gets a near-real-time, byte-level view of our
  unmatched residual and could quote-to-receive-bytes, never consume, and cherry-pick on
  reactivation. Acceptable only under genuine trust in every token holder. Partial
  mitigation shipped: a reactivated note is not re-offered to the same DEX. Per-DEX
  outstanding-handover cap + indicative→firm two-stage are documented follow-ups.
- **Every willing note is routable.** v1 hands off any note a connected DEX will fill; it
  does **not** retain surplus for internal crossing (that retention policy was removed
  with the oracle gates). Multi-DEX competition is the intended recapture lever (deferred).
- **No custody risk.** A handover is bytes; worst case from a misbehaving DEX is wasted
  in-flight windows bounded by the TTL.
- **Blast radius contained to the order book + matcher pass**, both behind ≥95% line /
  100% function coverage, with a property test asserting `active_order_count()`
  invariance across park→unpark and park→consume.
- **Deferred:** mock-mirror-as-DEX on-chain e2e; event-driven Ask-on-pair-update;
  multi-DEX residual split; an overfill protocol (let a DEX fill beyond the note's
  own rate, re-introducing an explicit fill price on the handover).

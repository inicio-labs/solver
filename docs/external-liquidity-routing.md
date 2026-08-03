# External Liquidity Routing (RFQ to other DEXes)

**STATUS: IMPLEMENTED.** Router, matcher external pass, decimal-correct selection,
config, and the filler SDK are built and tested. Opt-in via `router_enabled`
(default off).

**Owner:** solver team · **Audience:** solver team + operators (DEX/filler
integrators want [filler-integration.md](filler-integration.md)).

> This doc describes the **as-built** system. It supersedes the original design
> sketch and the in-tree planning notes; where they disagree, the code wins.

---

## 1. TL;DR

When the internal matcher can't cross an order against another user, that order is
idle liquidity in our book. We offer it to **allow-listed external DEXes ("fillers")**
over a websocket RFQ: each DEX posts a standing `{price, quantity}` quote per pair, the
matcher matches its idle notes against those quotes, and hands over the **serialized
note bytes**. The DEX **self-consumes on-chain at the note's fixed rate**, on its own
gas. Our existing ingest path detects the on-chain fill and drops the note.

Two PSWAP properties keep this far simpler than UniswapX/0x/CoW:

1. **Terms are fixed on-chain**, so there's no price negotiation — RFQ collapses to
   *liquidity discovery* ("how much of this fixed-rate note can you take?").
2. **Handover is a notification, not custody** — we send note id + bytes; the DEX
   consumes on its own gas. If nobody fills, the note stays ours.

Net: no signed orders, no exclusivity bonds, no escrow, no price auction — a read-only
selector + a websocket fan-out + the fill-watcher we already had.

---

## 2. Architecture

The **router is only websocket transport.** Every order decision is made in the
matcher, over the one in-memory order book. There is no second copy of the book and no
DB poll in this path.

```
 DEX ──SUBSCRIBE/QUOTE──▶ router thread ──quotes_tx (watch)──▶ matcher (book owner)
 DEX ◀──HANDOVER──────── router thread ◀──handover_tx (mpsc, try_send)── matcher
 DEX self-consumes on-chain ─▶ ingest sees nullifier ─▶ consumed_rx ─▶ matcher drops note
```

### 2.1 Components

- **Router thread** (`crates/solver/src/router/server.rs`) — its own OS thread + a
  multi-thread tokio runtime, mirroring `spawn_price_api_thread`. Holds DEX
  connections + their latest quotes; merges quotes onto a `watch` channel for the
  matcher; routes handovers back to the right connection by `DexId`. Thin and `Send`;
  a blocked/slow DEX socket can never stall the matcher tick.
- **Matcher external pass** (`crates/solver/src/matcher/matcher.rs`) — after internal
  matching, on **every** tick: reactivate expired in-flight notes, read the cached
  quotes + oracle mid, run `select_notes`, **park** each pick, reserve its quantity,
  and `try_send` a handover. No `.await`, no socket I/O on the tick.
- **Selection math** (`crates/solver/src/router/select.rs`) — `select_notes` is a pure,
  read-only, exact-integer function (the correctness core; see §4).
- **Order book park/unpark** (`crates/solver/src/matching/order_book.rs`) — how a note
  is taken out of internal matching while it's in flight to a DEX, without disturbing
  the matching gates (see §3).
- **LP SDK** (`crates/lp-sdk`, `pswap-lp-sdk`) — the client DEXes (liquidity providers)
  use, and the home of the shared wire protocol (see §6).

### 2.2 Two channels (matcher ↔ router)

| channel | dir | type | purpose |
|---|---|---|---|
| `quotes_tx/rx` | router→matcher | `watch<Arc<Vec<Quote>>>` | latest standing quotes `(dex,pair)→{price,qty,expires_at}` |
| `handover_tx/rx` | matcher→router | `mpsc<Handover>` | picks (note id + fill + bytes) back to the DEX, via `try_send` |

Wired in `pipeline.rs` (channels) and `start.rs` (router thread, readiness gate,
shutdown join).

---

## 3. In-flight = park / unpark

A note handed to a DEX must be invisible to internal matching until it's either
consumed or times out — **without** corrupting the matching gates. We get that by
reusing the book's own index machinery rather than inventing a parallel flag path.

- **`park(id, dex, now)`** = remove the note from the rate index and decrement the
  per-pair counter (exactly what `remove_order` already does), **but keep its `Order`
  struct** in `orders`. A parked note is therefore not indexed and not counted —
  identical to a removed one from the matcher's perspective. Consequence: `has_orders`,
  `best_order`, and `apply_match` need **no change** — a parked note simply isn't there
  to be returned.
- **Reactivation is O(expiring), never O(book).** The book keeps
  `parked: HashMap<OrderId,(DexId,parked_at)>` + a time-ordered
  `park_queue: VecDeque<(parked_at, OrderId)>`. Parking happens in tick-time order, so
  `parked_at` is monotonic and the queue is sorted for free.
  `reactivate_parked_older_than(ttl, now)` pops the front while
  `parked_at + ttl ≤ now` and stops at the first still-fresh entry (usually 0 pops); a
  popped id no longer in `parked` is a consumed tombstone, skipped. It re-indexes the
  surviving struct via the existing add path (note rejoins the back of its rate FIFO —
  fair; it was "away") and returns `(id, dex)` so the matcher releases the reservation.
- **Exits from PARKED:** (a) **consume** — the DEX self-consumes → `consumed_rx` → drop
  from `orders` (settled); (b) **no-show** — not consumed within `router_inflight_ttl_ms`
  → reactivation re-indexes it (matchable + re-routable, but **not** re-offered to the
  same DEX); (c) **rollback** — if the handover `try_send` is dropped (full/closed
  channel), the note never reached the DEX, so it is **immediately unparked**
  (`OrderBook::unpark`) with its reservation released and **no** re-offer penalty — a
  dropped delivery costs nothing. The DEX-no-show penalty in (b) applies only to notes
  that were actually delivered.
- **One park-aware conditional:** the `consumed_rx` removal of an *already-parked* note
  must not decrement the counter again (it was decremented at park) — just drop it from
  `orders`. `add_user_order` is idempotent on note id as general defence.

A note is in exactly one of {indexed · parked · gone} by construction, which is what
delivers "internal matching and external routing never touch the same note." A property
test asserts `active_order_count()` invariance across park→unpark and park→consume.

---

## 4. Export predicate (the #1 correctness item)

A note offers `o_raw` of token `O` (decimals `d_O`, oracle cents `c_O`) and requests
`r_raw` of token `R` (decimals `d_R`, oracle cents `c_R`). **All comparisons are exact
`u128`, never float**, mirroring the matcher's own cross-multiplication. Common-
denominator USD-cents:

```
offered_usd   ∝  o_raw · c_O · 10^d_R
requested_usd ∝  r_raw · c_R · 10^d_O
```

A note is **exportable** iff all hold:

1. **Data gate** — both tokens priced (`c_O,c_R` known) and both decimals known, else
   skip (mirrors "missing price ⇒ not matchable").
2. **Oracle-edge gate (vs MID, not the quote):**
   `o_raw · c_O · 10^d_R · 10_000 ≥ r_raw · c_R · 10^d_O · (10_000 + min_edge_bps)`.
   The note must give the consumer ≥ `min_edge_bps` more USD value than it takes, **at
   oracle mid**. Measuring against mid (not the DEX's quote) means an in-band
   manipulated quote can't move the export decision.
3. **DEX-willingness gate** — the DEX's quote price is on the profitable side of the
   note's fixed rate (the DEX has said it will take notes at least this generous).
4. **Off-market guard** — reject the whole quote if its implied price deviates from
   oracle mid by `> router_quote_max_deviation_bps`.

**Ordering:** hand over **marginal-eligible-first** (smallest `offered_usd −
requested_usd` surplus first), accumulating `Σ fill ≤ quote.quantity − reserved`. This
retains the most-generous notes for internal crossing (where the solver captures the
surplus) and gives away the least first.

> **The decimals trap (why this is exact, not "rate vs price").** Internally the matcher
> is decimals-blind — it works on devnet only because the tokens happen to share 8
> decimals. Export is decimals-**correct**. Worked example: IMIDEN \$2 / 8-dec, IUSDT
> \$1 / 6-dec; a parity note (offer `1e8` IMIDEN, request `2e6` IUSDT) has
> `offered_usd = 1e8·200·10^6` and `requested_usd = 2e6·100·10^8` → equal → margin 0 →
> **not exported** for any `edge_bps>0` (correct: a fair note isn't given away). The raw
> ratio `r_raw/o_raw = 0.02` differs from the price `2.0` by exactly `10^(d_O−d_R)=100×`
> — comparing raw-rate to price directly mis-routes by 100×. The integer formula above
> is the guard.

---

## 5. Configuration

All knobs live in `[engine]` of `solver.toml` with `#[serde(default)]`. The feature is
**off by default**; set `router_enabled = true` to turn it on.

| field | default | meaning |
|---|---|---|
| `router_enabled` | `false` | master switch for the router + matcher external pass |
| `router_bind` | `"127.0.0.1"` | websocket bind address (`"0.0.0.0"` to expose) |
| `router_port` | `8090` | websocket port (path is `/v1/rfq`) |
| `router_max_connections` | `64` | max concurrent DEX connections |
| `router_max_msg_bytes` | `16384` | max inbound websocket message size |
| `router_quote_ttl_ms` | `20000` | how long a standing quote stays selectable |
| `router_inflight_ttl_ms` | `30000` | how long a handed-over note waits before reactivating; set above realistic consume latency |
| `router_min_export_edge_bps` | `50` | min edge over oracle mid before a note is exported (`>0` retains internal liquidity) |
| `router_quote_max_deviation_bps` | `200` | reject a DEX quote deviating more than this from oracle mid |

**Auth tokens are NOT in config.** The allow-list comes from the
**`SOLVER_ROUTER_TOKENS`** env var (comma-separated), like `SOLVER_ADMIN_TOKEN`. If
`router_enabled` is true but the var is empty, the router starts but **rejects every
connection** and logs a warning.

```toml
[engine]
router_enabled = true
router_bind = "0.0.0.0"
router_port = 8090
router_min_export_edge_bps = 50
router_quote_max_deviation_bps = 200
```

```bash
# One token per DEX (comma-separated). Rotate by editing + restarting.
export SOLVER_ROUTER_TOKENS="dex-acme-7f3c…,dex-globex-9a21…"
```

---

## 6. The RFQ protocol & SDK

The wire protocol (a compact **binary** encoding — miden `Serializable`, over binary
websocket frames on `/v1/rfq`) lives in **`crates/lp-sdk/src/protocol.rs`** — the single
shared definition. The solver depends on `pswap-lp-sdk` for it one-way, so the two sides
can't drift. (Grepping the solver for the protocol types finds nothing locally; they're
in the SDK.)

Messages: `Quote{offered, requested, valid_for_ms?}` (client→) and `AuthOk` ·
`Ask{pairs}` · `Handover{note, fill_amount}` · `Error{code, msg}` (→client). A quote is
**filler-centric** and carries typed `FungibleAsset`s: `offered` is what the DEX gives,
`requested` is what it wants — their faucet ids imply the pair and their amounts carry
both the rate (the ratio) and the max size, so there is **no separate `subscribe` step
and no decimal price string** (the quote is the registration). The router flips this to
the note-centric orientation internally. Auth is `Authorization: Bearer` (or `?token=`
for browsers), checked constant-time at the upgrade. The `Handover` carries the typed
PSWAP `note` (which binds its own rate on-chain) plus the requested-token `fill_amount` —
enough to self-consume. Full field-by-field reference and a reference filler are in
[filler-integration.md](filler-integration.md).

The SDK ships a `consume` feature (off by default) that decodes a handed-over note and
reads its terms using `miden-protocol`/`miden-standards` only — never `miden-client`, so
it can't constrain a DEX's miden version.

---

## 7. Operator runbook

1. **Enable** — set `router_enabled = true`; pick `router_bind`/`router_port`. Behind a
   reverse proxy, terminate TLS there and forward to the bind address.
2. **Issue tokens** — generate one opaque random token per DEX; put them in
   `SOLVER_ROUTER_TOKENS` (comma-separated); share each token with its DEX over a secure
   channel. Rotate by editing the var and restarting.
3. **Tune retention** — `router_min_export_edge_bps` is the lever: higher keeps more
   liquidity for internal crossing, lower exports more aggressively. `>0` always.
4. **Tune safety** — `router_quote_max_deviation_bps` rejects off-market quotes;
   `router_inflight_ttl_ms` must exceed realistic DEX consume latency or notes bounce
   back too soon.
5. **Verify readiness** — on boot, `start.rs` gates on the router's bind readiness
   oneshot; a router that can't bind fails startup loudly.
6. **Watch** — handover/quote activity is logged; a `handover_tx` full/closed drop is
   logged and the batch is **rolled back** (notes unparked, reservations released, so
   they stay eligible next tick) — not fatal.

---

## 8. Security model & accepted risks

- **Allow-list only.** Only holders of a `SOLVER_ROUTER_TOKENS` value can connect.
  There is no open registration.
- **Flow leak (explicitly accepted).** A trusted DEX gets a near-real-time, byte-level
  view of our unmatched residual: it can quote-to-receive-bytes, never consume, let the
  TTL reactivate, and cherry-pick. This is acceptable **only** under genuine trust in
  every token holder. Partial mitigation in place: on reactivation a note is **not
  re-offered to the same DEX**. A per-DEX outstanding-handover cap and an
  indicative→firm two-stage are documented follow-ups, not built.
- **No custody risk.** The solver never holds DEX funds or keys; a handover is bytes.
  Worst case from a misbehaving DEX is wasted in-flight windows (bounded by the TTL),
  not loss.
- **Off-market protection.** Export decisions are measured against the solver's own
  oracle mid, so an in-band manipulated quote can't change *which* notes leave; an
  out-of-band one is rejected outright.

---

## 9. Testing

- **`select_notes` (unit + property):** the asymmetric-decimals boundary (6-dec IUSDT
  vs 8-dec IMIDEN), the `edge_bps` margin, DEX-willingness, marginal-first ordering +
  quantity cap, reserved-budget, stale-quote skip, off-market reject, overflow inputs,
  and a 400-case invariant proptest.
- **Order book:** `active_order_count()` invariance across park→unpark and park→consume;
  parked notes skipped by the matching gates; idempotent re-add.
- **Matcher:** route-to-DEX, internal-match unaffected, reactivation-then-consume,
  backpressure (a full `handover_tx` doesn't stall the tick).
- **Router (real websocket):** auth reject/accept (header + `?token=`), quote→snapshot,
  off-market/bad-quote errors, handover delivery, capacity 503, two-DEX independence,
  thread bootstrap + graceful shutdown.
- **Seamless end-to-end:** `crates/solver/tests/integration_lp_sdk.rs` drives the
  **real router thread** through the **public SDK** (`LpClient`): bad token rejected
  → `AuthOk` → quote reaches the matcher → handover surfaces as an `LpEvent::Handover`
  carrying the decoded note + fill amount.

Changed files carry ≥95% line + 100% function coverage.

---

## 10. Deferred (not built)

- **mock-mirror as a DEX** for a devnet/MockChain e2e with a real on-chain consume.
- **Event-driven Ask-on-pair-update** (we kept standing-quotes only, by decision).
- **Per-DEX outstanding-handover cap** and **indicative→firm two-stage** (flow-leak
  hardening).
- **Multi-DEX split** of a pair's residual across all clearing quotes (v1 = best-price
  single winner).

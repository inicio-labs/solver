# Filler Integration Guide — PSWAP external liquidity

**Audience:** external DEXes ("fillers") integrating with a Miden PSWAP solver to
receive and fill order flow it can't cross internally.
**SDK:** [`pswap-filler-sdk`](../crates/filler-sdk) (Rust). **Transport:** one
websocket (miden-binary frames). **Auth:** a bearer token the operator issues you.

The SDK's rustdoc (`cargo doc -p pswap-filler-sdk --open`) is the API reference;
this guide covers the semantics you can't read off the types.

---

## 1. Mental model

The solver runs an internal order book of PSWAP notes (on-chain swap orders). When
it can't cross an order against another user — e.g. an `IMIDEN→IUSDT` order with no
opposing `IUSDT→IMIDEN` — that note is **idle liquidity**, and it offers it to you.

Three facts make this simpler than a normal RFQ/AMM integration:

1. **Terms are fixed on-chain.** A PSWAP note already encodes its rate (offer `X`,
   request `Y`). You fill it *at that rate or better for the maker, never worse*. So
   there is **no price negotiation** — your "quote" just declares *how much* you'll
   take and *at what rate you stop being interested*.
2. **A "handover" is a note, not custody.** The solver sends you the decoded `Note`.
   You consume it **on-chain, on your own gas, with your own keys**. The solver never
   holds your funds and never signs for you.
3. **Standing quotes, not request/response.** You keep a quote live and refresh it
   before it expires; you are *not* asked per-order. The solver matches its idle notes
   against your standing quote and pushes you the ones that clear it. (The SDK's
   `serve_quotes` handles the refresh for you.)

### How the solver talks to your DEX

```mermaid
sequenceDiagram
    autonumber
    participant D as Your DEX<br/>(pswap-filler-sdk)
    participant R as Solver router<br/>(websocket thread)
    participant M as Matcher<br/>(in-memory order book)
    participant C as Miden chain

    D->>R: connect /v1/rfq (Authorization: Bearer)
    R-->>D: AuthOk
    D->>R: Quote { offered, requested }
    Note right of D: standing — serve_quotes refreshes before the TTL
    R->>M: quotes_tx (watch) — latest quotes
    Note over M: each tick, select_notes():<br/>does an idle note clear your quote<br/>AND beat the oracle edge?
    M->>M: park the note (out of internal matching)
    M->>R: handover_tx (try_send)
    R-->>D: Handover { note, fill_amount, fill_price }
    D->>D: PswapNote::try_from(&note) + your policy check
    D->>C: consume the note on-chain (your gas, your keys)
    C-->>M: nullifier observed → consumed_rx
    M->>M: drop the note (settled)
    Note over D,M: not consumed within the in-flight TTL?<br/>the matcher unparks and re-routes it
```

If you never consume a handed-over note, nothing breaks: after the in-flight TTL the
solver reactivates it and matches it elsewhere.

---

## 2. Install

```toml
[dependencies]
pswap-filler-sdk = { git = "<solver-repo-url>", package = "pswap-filler-sdk" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
```

The SDK pulls `miden-protocol` / `miden-standards` (the binary protocol carries miden
types natively). It does **not** pull the solver crate or `miden-client` — you bring
your own client for the consume transaction, so nothing conflicts with your stack.

---

## 3. Connect

```rust
use pswap_filler_sdk::{LpClient, LpEvent};

let mut client = LpClient::connect("ws://solver-host:8090/v1/rfq", "your-token").await?;
assert!(matches!(client.next_event().await, Some(LpEvent::AuthOk)));
```

- The SDK sends `Authorization: Bearer <token>` on the upgrade. A **wrong/missing token
  fails the connection** (HTTP 401) — `connect` returns `Err`, no session opens.
- On success the **first event is always `AuthOk`**. The connection then **auto-reconnects**:
  a transient drop surfaces as `Reconnecting` then `Reconnected` (the SDK re-authenticates
  for you), and the event stream only ends (`next_event` → `None`) when
  you drop the client. A terminal `Disconnected { reason }` is emitted only if the SDK
  gives up — today that means the token was rejected on reconnect.
- Default port **8090**, path **`/v1/rfq`**. Tokens are bearer secrets — treat them like
  API keys; don't share or log them.

---

## 4. Quote

The one-call, hands-free path is `serve_quotes` — keep a fresh quote live per pair.
There's no subscribe step: **the quote is the registration** (its faucet ids imply the
pair).

```rust
use std::time::Duration;
use pswap_filler_sdk::PairSpec;

let _q = client.serve_quotes(
    vec![PairSpec { offered: imiden, requested: iusdt }],   // AccountIds, (offered, requested)
    Duration::from_secs(10),                                // ~half the router's quote TTL
    |_pair| Some((1_000_000, 2_000_000)),                   // your live price (base units)
);
```

The SDK calls your pricing fn **every tick** for the current amounts and re-sends the
quote — so it never expires (keepalive) **and** never goes stale-by-omission (it always
pushes what your fn returns *now*). Return `None` to skip a pair this tick. Drop the
returned `QuoteTask` to detach, or `abort()` to stop.

For manual control you can also `client.quote(offered, requested, valid_for_ms)` with
`FungibleAsset`s directly, or send from a cloned `client.sender()`.

### What a quote means (base units, not a human price)

A quote is two amounts on the pair's faucets: **`offered`** (base units of the pair's
`offered` token — what you *receive*) and **`requested`** (base units of the `requested`
token — what you *pay*).

- **Their ratio is your rate** — the worst price you'll accept. The solver only hands
  you notes whose fixed on-chain rate is at least this generous to you.
- **The amounts are your max size** — the solver packs notes up to them.

> **Work in base units — the token's on-chain units, exactly like a PSWAP note.** Do
> **not** compute a "human price per whole token" or pre-scale by decimals — you did
> that in the old string-price protocol; you don't now. A quote is structurally a PSWAP
> counter-order: "I give `offered` for `requested`." The solver compares base-unit
> ratios directly (and applies each token's decimals only for its oracle off-market
> check — see [external-liquidity-routing.md](external-liquidity-routing.md)).

`PairSpec { offered, requested }` and its reverse are distinct pairs. The pair you quote
is the pair you fill — no separate registration.

---

## 5. Receive handovers & consume

```rust
use pswap_filler_sdk::consume::{consume_args, PswapNote};

while let Some(ev) = client.next_event().await {
    match ev {
        LpEvent::Handover(h) => {
            // h.note        : Note       — the PSWAP note to consume (decoded)
            // h.fill_amount : u64        — requested-token base units to fill
            // h.fill_price  : PriceRatio — { num, den }, requested-per-offered (your matched quote)
            let pswap = PswapNote::try_from(&h.note)?;   // what you receive / pay
            // pswap.offered_asset()               — you RECEIVE this
            // pswap.storage().requested_asset()   — you PAY this (pro-rata for a partial fill)
            // pswap.storage().creator_account_id()— maker the requested asset settles back to

            // Your policy check: is the note's rate good for you, given live prices and
            // inventory (cross-check against h.fill_price)? You decide — the rate is fixed.

            let args = consume_args(0, h.fill_amount)?;  // (account_fill, note_fill) → Word
            // ... feed h.note + args into YOUR miden-client transaction (below) ...
        }
        LpEvent::Error(e) => eprintln!("router error: {e}"),  // typed LpError (e.g. rejected quote)
        LpEvent::Reconnecting { attempt, error } => eprintln!("link lost (try {attempt}): {error}"),
        LpEvent::Disconnected { reason } => break,             // SDK gave up (see §6)
        _ => {}                                                // AuthOk, Reconnected, Ask
    }
}
```

- **`fill_price`** is a `PriceRatio { num, den }` (requested-per-offered) — **your own
  matched quote, echoed**: *"fill this note at `num/den`."* Cross-check it against your
  live price. (Forward-looking: a PSWAP note enforces its own fixed rate on-chain today,
  so `consume_args` fills at that rate and `fill_price` equals or beats it; it becomes
  the *binding* rate when the overfill protocol change ships — read it now to be ready.)
- **`consume_args(account_fill, note_fill)`** builds the note args for a fill.
  `note_fill` is requested-token base units from the note (partial fills allowed —
  `fill_amount` may be below the note's requested amount); `account_fill` is the
  account-side amount (`0` for a pure note-side fill).

### Self-consume on-chain (your code, your client)

The SDK stops at the note + args — running the transaction is yours (your keystore, your
gas). With `miden-client` that is roughly:

```rust
// PSEUDO — uses YOUR miden-client, not the SDK:
// let request = TransactionRequestBuilder::new()
//     .input_notes([(h.note, Some(args))])
//     .build()?;
// let tx = your_client.new_transaction(your_account_id, request).await?;
// your_client.submit_transaction(tx).await?;
```

The note's payback (the requested asset) settles to the **creator**; the offered asset
lands in **your** account. Once your consume confirms on-chain, the solver's ingest sees
the nullifier and drops the note — no message back from you needed.

### If you don't fill

A handover is an *offer*, not an obligation. Ignore it and after `router_inflight_ttl_ms`
(default 30 s) the solver reactivates the note and routes it elsewhere. Repeatedly taking
handovers and not consuming them is visible to the operator and grounds for de-listing.

---

## 6. Operational notes

- **Reconnect is automatic.** The SDK reconnects with capped backoff and re-authenticates
  on its own — watch for `Reconnecting`/`Reconnected`. Your standing quotes don't survive a
  drop, but `serve_quotes` resumes pushing on the next tick; if you quote manually, re-post
  on `Reconnected` (the quote is the registration). The SDK only stops (terminal
  `Disconnected { reason }`) when the token is rejected.
- **Idempotent handovers.** Dedupe by the note's id — a reactivated note can be offered
  again (not to the DEX that already held it, within the same in-flight window).
- **Message size.** The server caps inbound messages (default 16 KiB). Quotes are tiny.
- **One connection, many pairs.** Serve as many pairs as you fill over one socket.
  Multiple connections with the same token work and are routed independently.

---

## 7. Protocol reference (binary)

miden `Serializable`/`Deserializable` over WebSocket **binary** frames at `GET /v1/rfq`.
The SDK encodes/decodes it; you never touch the wire. (Binary isn't human-readable — log
the decoded structs, not the frames.)

**Client → server** (`ClientMsg`):
- `Quote { offered: FungibleAsset, requested: FungibleAsset, valid_for_ms: Option<u64> }`
  — standing quote; resend to refresh. The faucet ids imply the pair, so this is also the
  registration — there is no separate subscribe message.

**Server → client** (`ServerMsg`):
- `AuthOk` — handshake accepted (first frame).
- `Handover { note: Note, fill_amount: u64, fill_price: PriceRatio }` — a note to fill.
- `Error { code: String, msg: String }` — a message was rejected.
- `Ask { pairs }` — reserved (a future pull/quote-on-demand mode); not emitted today.

`PairSpec`/`FungibleAsset`/`Note` are miden types, serialized in miden's binary format.

---

## 8. FAQ / gotchas

**Do I request per order?** No. You keep a standing quote (`serve_quotes`); the solver
pushes matching notes. You never reply per order.

**My quote is live but I get no handovers.** The solver only exports a note when (a) your
quote's rate clears the note's fixed rate, (b) the note also beats oracle mid by the
operator's edge (it keeps the most-generous notes for internal crossing), and (c) your
quote is within the off-market band vs oracle mid. A correct-but-tighter quote getting no
flow is normal — you'll get fills when clearing notes appear.

**Why was my quote rejected with an `Error`?** Most common: a zero amount, or amounts
that don't form a valid `FungibleAsset` (the SDK's `quote` rejects zero amounts locally).

**Are partial fills possible?** Yes — `fill_amount` can be below the note's requested
amount. Use `consume_args(0, fill_amount)`.

**What if two of us quote the same pair?** The solver picks per its own policy (v1: best
rate wins). Tighten your rate to win more flow.

**Does the solver take a spread?** No — it routes at the note's terms; the export edge is
a retention threshold, not a fee. Your margin is yours.

**Decimals?** Quote in **base units** (the note's own units); never pre-scale by decimals
— the solver applies each token's on-chain decimals for its own comparisons.

---

See also: [external-liquidity-routing.md](external-liquidity-routing.md) (solver-side
architecture, export math, config, runbook) and the crate rustdoc.

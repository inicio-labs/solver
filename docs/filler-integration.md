# Filler Integration Guide — PSWAP external liquidity

**Audience:** external DEXes ("fillers") integrating with a Miden PSWAP solver to
receive and fill order flow it can't cross internally.
**SDK:** [`pswap-filler-sdk`](../crates/filler-sdk) (Rust). **Transport:** one
websocket. **Auth:** a bearer token the solver operator issues you.

---

## 1. Mental model

The solver runs an internal order book of PSWAP notes (on-chain swap orders). When
it can't cross an order against another user — e.g. an `IMIDEN→IUSDT` order with no
opposing `IUSDT→IMIDEN` — that note is **idle liquidity**. Instead of letting it sit,
the solver offers it to you.

Three facts make this simpler than a normal RFQ/AMM integration:

1. **Terms are fixed on-chain.** A PSWAP note already encodes its rate (offer `X`,
   request `Y`). You can only fill it *at that rate or better for the maker, never
   worse*. So there is **no price negotiation** — your "quote" is just you declaring
   *how much* you'll take and *at what price you stop being interested*.
2. **A "handover" is just bytes, not custody.** The solver sends you the note's id +
   serialized bytes. You consume it **on-chain, on your own gas, with your own keys**.
   The solver never holds your funds and never signs for you.
3. **Standing quotes, not request/response.** You post a quote once and refresh it
   before it expires. You are *not* asked per-order and you do *not* reply per-order.
   The solver matches its idle notes against your standing quote and pushes you the
   ones that clear it.

```
  you ──SUBSCRIBE{pairs}──▶ solver router        (pairs you can fill)
  you ──QUOTE{pair,price,quantity}──▶ router      (standing; refresh before TTL)
                          router ──HANDOVER{note_id, fill_amount, note_hex, fill_price}──▶ you
  you: decode note → check terms → consume on-chain (your client, your gas)
  solver observes the on-chain fill → drops the note from its book
```

If you never consume a handed-over note, nothing breaks: after an in-flight TTL the
solver simply reactivates it and matches it elsewhere.

---

## 2. Install

```toml
[dependencies]
pswap-filler-sdk = { git = "<solver-repo-url>", package = "pswap-filler-sdk" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
```

### Feature flags

| feature | adds | pulls in |
|---|---|---|
| *(default)* | `FillerClient` + the wire `protocol` | serde, tokio, tungstenite — **no miden** |
| `consume` | `consume::{decode_note, PswapTerms, consume_args}` | `miden-protocol`, `miden-standards` (**not** `miden-client`) |

Enable `consume` **only** if you want the SDK to decode the note and read its terms
for you. If you already run a miden stack and just want the raw bytes, stay on the
default build — it has zero miden dependencies, so the SDK never constrains your
miden version.

```toml
pswap-filler-sdk = { git = "...", package = "pswap-filler-sdk", features = ["consume"] }
```

You bring your own `miden-client` for the actual consume transaction either way — the
SDK deliberately does not pull it in, so it can't conflict with yours.

---

## 3. Connect & authenticate

```rust
use pswap_filler_sdk::{FillerClient, FillerEvent};

let mut client = FillerClient::connect("ws://solver-host:8090/v1/rfq", "your-token").await?;
assert!(matches!(client.next_event().await, Some(FillerEvent::AuthOk)));
```

- The SDK sends `Authorization: Bearer <token>` on the upgrade. A **wrong or missing
  token fails the connection** (HTTP 401) — `connect` returns `Err`, no session opens.
- On success the **first event is always `AuthOk`**; the **last is always
  `Disconnected`**.
- The default port is **8090**, path **`/v1/rfq`**. Confirm host/port/token with the
  operator.
- Tokens are bearer secrets — treat them like API keys. Anyone holding one sees your
  pair's idle order flow, so don't share or log them.

---

## 4. Subscribe & quote

```rust
use pswap_filler_sdk::PairSpec;

// Hex account ids, in the note's (offered, requested) orientation.
let pair = PairSpec { offered: imiden_hex.into(), requested: iusdt_hex.into() };

client.subscribe(vec![pair.clone()])?;            // pairs you can fill
client.quote(&pair, "2.00", 1_000_000, None)?;    // standing quote
```

### Pairs

A `PairSpec` is two **hex account ids** (faucet ids) in **`(offered, requested)`**
orientation — the same orientation as the note. `offered` is the token the note pays
*out* (and you receive); `requested` is the token the note wants *in* (and you pay).
A pair and its reverse are distinct: `(IMIDEN, IUSDT)` ≠ `(IUSDT, IMIDEN)`.

`subscribe` declares which pairs you can fill. Quotes still gate per pair — subscribing
without quoting gets you nothing.

### Quotes (the important part)

```rust
client.quote(&pair, price /* &str */, quantity /* u64 */, valid_for_ms /* Option<u64> */)?;
```

- **`price`** — `requested`-token per `offered`-token, **per whole token**, as a
  decimal string (e.g. `"2.00"`). This is the **worst price you'll accept**: the solver
  only hands you notes whose on-chain rate is at least this generous to you. Validated
  client-side, so a malformed price errors locally instead of round-tripping.
- **`quantity`** — the **max `requested`-token amount (base units)** you'll take across
  all fills against this quote. The solver packs notes up to this budget.
- **`valid_for_ms`** — optional; shortens validity below the server's quote TTL. `None`
  uses the server TTL (default 20 s).

**Quotes are standing.** Post once; **refresh before expiry** (a good keepalive is
≈ TTL/2). A stale quote is silently ignored by the matcher. Disconnecting purges your
quotes immediately.

> ### Price units — read this twice
> Price is **per whole token, `requested` per `offered`** — the human price, not a
> base-unit ratio. If IMIDEN is \$2 and IUSDT is \$1, the parity price for
> `(IMIDEN, IUSDT)` is `"2.0"` regardless of either token's decimals. Do **not**
> pre-scale by decimals; the solver applies decimals itself. (Internally it compares
> exact integers using each token's on-chain decimals — see
> [external-liquidity-routing.md](external-liquidity-routing.md) §"Export predicate".)

---

## 5. Receive handovers & consume

Loop on events. A `Handover` is a note for you to fill:

```rust
while let Some(ev) = client.next_event().await {
    match ev {
        FillerEvent::Handover(h) => {
            // h.note_id     : String  — the note's id (for your logs / dedupe)
            // h.fill_amount : u64     — requested-token base units to fill
            // h.note_hex    : String  — hex-encoded serialized PSWAP note
            // h.fill_price  : String  — the price to fill at (your quoted X, echoed)
            handle_handover(h).await?;
        }
        FillerEvent::Withdrawn { note_id } => {
            // Stop trying to consume this note — it settled internally or was pulled.
        }
        FillerEvent::Ask { .. } => { /* solver re-advertised the pairs it wants */ }
        FillerEvent::Error { code, msg } => eprintln!("router error {code}: {msg}"),
        FillerEvent::Disconnected => break,   // reconnect (see §7)
        FillerEvent::AuthOk => {}
    }
}
```

### `fill_price` — the price to fill at

`fill_price` is **your own quoted price `X`, echoed back** (requested-per-offered, per
whole token): the solver is saying *"fill this note at `X`."* It's a string so it stays
exact (e.g. `"2.00"`) — parse it with `parse_decimal_price` if you need the rational.

> **Forward-looking, be aware.** Today a PSWAP note enforces its own fixed rate
> on-chain, and `consume_args` fills at that rate — so right now `fill_price` is the
> agreed price/floor for your records and equals (or is more generous than) the note's
> rate. It becomes the *binding* fill rate once the **overfill** protocol change ships
> (a note that lets the consumer settle above its intrinsic rate). Design your handler
> to read `fill_price` now so you're ready when that lands.

### Decoding & checking terms (feature `consume`)

```rust
use pswap_filler_sdk::consume::{decode_note, PswapTerms, consume_args};

let note  = decode_note(&h.note_hex)?;          // hex → miden Note
let terms = PswapTerms::from_note(&note)?;      // what you receive / pay
// terms.offered_faucet / offered_amount  — you RECEIVE this
// terms.requested_faucet / requested_amount — you PAY this (pro-rata for partial)
// terms.creator — the maker the requested asset settles back to

// Your policy check: is terms.offered_amount / terms.requested_amount good for you,
// given live prices and your inventory? You decide — the note's rate is fixed.

let args = consume_args(h.fill_amount)?;        // note args for a (partial) fill
```

`fill_amount` may be **less than** `requested_amount` (a partial fill). `consume_args`
builds the note args for exactly that fill; pass the full `requested_amount` for a
complete fill.

### Self-consume on-chain (your code, your client)

The SDK stops at decode + args — running the transaction is yours, because it needs
your keystore and gas. With `miden-client` that is roughly:

```rust
// PSEUDO — uses YOUR miden-client setup, not the SDK:
// let request = TransactionRequestBuilder::new()
//     .with_authenticated_input_notes([(note, Some(args))])
//     .build()?;
// let tx = your_client.new_transaction(your_account_id, request).await?;
// your_client.submit_transaction(tx).await?;
```

The note's payback (the requested asset) settles to the **creator**; the offered asset
lands in **your** account. Once your consume confirms on-chain, the solver's ingest
sees the nullifier and drops the note from its book — no message back from you needed.

### If you don't fill

A handover is an *offer*, not an obligation. Ignore it and after
`router_inflight_ttl_ms` (default 30 s) the solver reactivates the note and routes it
elsewhere. Repeatedly taking handovers and not consuming them is visible to the
operator and is grounds for de-listing your token.

---

## 6. A complete reference filler

```rust
use std::time::Duration;
use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url   = "ws://solver-host:8090/v1/rfq";
    let token = std::env::var("SOLVER_FILLER_TOKEN")?;
    let pair  = PairSpec { offered: imiden_hex(), requested: iusdt_hex() };

    loop {
        let mut client = match FillerClient::connect(url, &token).await {
            Ok(c) => c,
            Err(e) => { eprintln!("connect failed: {e}; retrying in 5s"); sleep5().await; continue; }
        };

        client.subscribe(vec![pair.clone()])?;
        client.quote(&pair, "2.00", 1_000_000, None)?;

        // Refresh the quote every ~10s (TTL/2) from a cloned sender.
        let refresher = client.sender();
        let p = pair.clone();
        let keepalive = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if refresher.quote(&p, "2.00", 1_000_000, None).is_err() { break; }
            }
        });

        while let Some(ev) = client.next_event().await {
            match ev {
                FillerEvent::Handover(h) => {
                    if let Err(e) = try_fill(&h).await {   // your decode+policy+consume
                        eprintln!("fill {} failed: {e}", h.note_id);
                    }
                }
                FillerEvent::Error { code, msg } => eprintln!("router error {code}: {msg}"),
                FillerEvent::Disconnected => break,
                _ => {}
            }
        }
        keepalive.abort();      // connection dropped → reconnect from the top
    }
}
```

`FillerClient::sender()` returns a cheap, cloneable `FillerSender` you can move into a
timer task to refresh quotes while the main task drains events.

---

## 7. Operational notes

- **Reconnect on `Disconnected`.** Quotes do not survive a disconnect; re-subscribe and
  re-quote after reconnecting (the loop above does this). Back off on repeated connect
  failures.
- **Idempotent handovers.** Dedupe by `note_id` — a reactivated note can be offered
  again (though not to the DEX that already held it, in the same in-flight window).
- **Message size.** The server caps inbound messages (default 16 KiB). Quotes/subscribes
  are tiny; this won't bite normal use.
- **One connection, many pairs.** Subscribe and quote as many pairs as you fill over a
  single socket. Multiple connections with the same token also work and are routed
  independently.
- **Timeouts.** `next_event_timeout(Duration)` returns `Ok(None)` on timeout without
  closing the connection — handy for driving your own keepalive cadence.

---

## 8. Protocol reference

JSON, `type`-tagged, over a single websocket at `GET /v1/rfq`. The SDK encodes/decodes
all of this for you; this section is for debugging or a non-Rust client.

### Client → server

| `type` | fields | meaning |
|---|---|---|
| `subscribe` | `pairs: [{offered, requested}]` | pairs you can fill |
| `quote` | `pair: {offered, requested}`, `price: string`, `quantity: u64`, `valid_for_ms?: u64` | standing quote; resend to refresh |

### Server → client

| `type` | fields | meaning |
|---|---|---|
| `auth_ok` | — | handshake accepted (first frame) |
| `ask` | `pairs: [{offered, requested}]` | pairs the solver wants quotes for |
| `handover` | `note_id: string`, `fill_amount: u64`, `note_hex: string`, `fill_price: string` | a note to fill, at `fill_price` |
| `withdrawn` | `note_id: string` | stop trying to consume this note |
| `error` | `code: string`, `msg: string` | a message was rejected (e.g. bad price) |

`pair` is `{ "offered": "0x<hex account id>", "requested": "0x<hex account id>" }`.
`price` and `fill_price` are decimal strings, `requested` per `offered`, per whole token
(`fill_price` is your quoted `price` echoed for the handed-over note). `quantity` and
`fill_amount` are `requested`-token **base units**. `note_hex` is the hex of the
serialized PSWAP note (optionally `0x`-prefixed).

Example quote frame:

```json
{ "type": "quote",
  "pair": { "offered": "0xabc…", "requested": "0xdef…" },
  "price": "2.00", "quantity": 1000000 }
```

---

## 9. FAQ / gotchas

**Do I send an Ask or request per order?** No. You post a standing quote; the solver
pushes matching notes. `Ask` is informational (the pairs the solver wants).

**My quote is "valid" but I get no handovers.** The solver only exports a note when (a)
your quote price clears the note's fixed rate, (b) the note also beats oracle mid by the
operator's edge (it keeps the most-generous notes for internal crossing), and (c) your
quote is within the off-market band vs oracle mid. A correct quote that's simply tighter
than the available notes is normal — you'll get fills when notes that clear it appear.

**Why was my quote rejected with an `error`?** Most common: malformed `price` (the SDK
catches this before sending), `quantity = 0`, or an unparseable pair account id.

**Are partial fills possible?** Yes — `fill_amount` can be below the note's
`requested_amount`. Use `consume_args(fill_amount)`.

**What if two of us quote the same pair?** The solver picks per its own policy (v1: best
price wins the pair). Tighten your price to win more flow.

**Does the solver take a spread?** No — it routes at the note's terms; `min_export_edge`
is purely a retention threshold, not a fee. Your margin is yours.

**Decimals?** Quote the human price per whole token. Never pre-scale by decimals — the
solver applies each token's on-chain decimals when it compares.

---

See also: [external-liquidity-routing.md](external-liquidity-routing.md) (the solver-side
architecture, export math, config, and operator runbook) and the crate docs
(`cargo doc -p pswap-filler-sdk --features consume --open`).

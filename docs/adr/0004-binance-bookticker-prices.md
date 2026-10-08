# 4. Binance bookTicker prices for PSWAP batch clearing

- **Status:** Accepted; implementation in progress
- **Date:** 2026-10-06
- **Scope:** Internal PSWAP batch clearing, swap guidance, and wallet token valuation in the 0.17 solver. Binance is the only price source.
- **Related:** [Binance Spot WebSocket streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [market-data-only endpoints](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md), [exchange information](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information), [price API](../price-api.md)

## Context

Batch clearing needs a reference price for each supported direct pair. At launch the Miden book may be thin or one-sided, so its own orders may not provide a useful clearing reference. The solver needs a current external reference for each supported direct pair without making a remote request on every matching tick.

The matcher and current price task are spawned on the same single-threaded Tokio `LocalSet` in `pipeline.rs`. Pair clearing performs synchronous computation. While that computation runs, another task on the same thread cannot read a socket or answer its ping. The price feed must remain responsive independently of matching work.

The reference price is an input to the existing order eligibility and allocation logic. It does not replace a PSWAP note's limit, the configured fee rule, minimum-fill handling, or the final integer-asset solvency check.

## Decision

### Price and market mapping

Use Binance Spot's public `<symbol>@bookTicker` stream for configured direct markets. Each message supplies an order-book update ID, symbol, best bid and ask prices, and their displayed quantities. Parse the price strings exactly. For Binance's `QUOTE per BASE` orientation, set the whole-token reference price to:

```text
P = (bid + ask) / 2
```

Compute this midpoint once. If the solver's pair uses the reverse orientation, use `1 / P`; do not average the reciprocals of bid and ask. Convert the resulting whole-token ratio to base units with the validated decimals of both Miden assets:

```text
quote base units per base base unit = P × 10^quote_decimals / 10^base_decimals
```

Keep the arithmetic exact and checked. Read the price strings as exact decimals (`rust_decimal`), compute `P` from them, and convert `P` once into the existing `BatchPrice` ratio of base units. A reversed pair uses that ratio with its two sides swapped, so `1 / P` is never rounded. Do not turn the pair price into two fictitious USD token prices or pass it through floating point.

The operator configures each Binance symbol against its Miden faucet pair. On startup, validate the symbol, its base and quote assets, and trading status through public `exchangeInfo`. Derive direct or reverse orientation from that mapping; there is no separate operator-supplied orientation flag. Token names alone do not establish the mapping. Only directly listed and approved markets are supported. A pair without a valid direct market remains unavailable for internal clearing while other pairs continue.

Each faucet maps to a Binance asset code and each clearing pair names its approved symbol:

```toml
[[pairs]]
name = "USDT-ETH"
asset_x_faucet_id = "0x…"
asset_x_binance_asset = "USDT"
asset_y_faucet_id = "0x…"
asset_y_binance_asset = "ETH"
binance_symbol = "ETHUSDT"
```

`exchangeInfo` must list `ETHUSDT` with exactly these two assets and status `TRADING`. Here the pair's base (`asset_x`, USDT) is Binance's quote asset, so the pair price is `1 / P`. Query each symbol on its own: a multi-symbol `exchangeInfo` request fails as a whole (HTTP 400, code `-1121`) when one symbol is unknown, which would hide which configured market is wrong.

At the external-data boundary require positive bid, ask, and displayed quantities; `bid <= ask`; a known symbol; and a valid update ID. Require the spread to satisfy a configurable maximum, compared with exact arithmetic:

```text
spread_bps = 10_000 × (ask - bid) / ((bid + ask) / 2)
```

Do not round the spread down before comparison. Existing order eligibility applies each order's limit and fee margin to the selected `P`. The feed does not introduce another fee formula.

### One pricing thread, two reader tasks

Run one dedicated pricing OS thread with a Tokio runtime. Spawn two supervised reader tasks on that runtime. Each reader owns its own WebSocket connection and independently subscribes to the same configured `bookTicker` symbols. Both stay live; either can deliver the next update. One publisher on the pricing runtime merges their latest observations and sends a pair-price snapshot to the matcher and swap guidance API.

```text
Reader A ─┐
          ├─> publisher ─> in-memory pair snapshot ─┬─> matcher
Reader B ─┘                                        └─> swap guidance API
```

Use one combined stream per reader rather than a connection per symbol. Default both connections to the public market-data endpoint `wss://data-stream.binance.vision:443`; allow separately configured endpoints, including `wss://stream.binance.com:443`. This uses no Binance account, user-data stream, or API key. Two sockets improve tolerance to an individual connection failure; they do not imply independent provider infrastructure, network paths, processes, or machines.

Each reader publishes its latest observation per configured symbol, including the original local receipt time and an unusable marker when a newer identifiable quote fails validation. A bounded latest-value `watch` snapshot avoids a historical tick queue. The publisher owns one per-symbol high-water update ID and one output `watch::Sender<Arc<PriceSnapshot>>`:

- Accept a higher ID from either reader and publish its validated state.
- Ignore an equal or lower ID, without renewing the quote's age.
- On a higher ID with an identifiable but crossed, too-wide, or otherwise invalid quote, mark only that symbol unavailable and retain the new high-water ID. An older good quote must not revive it.
- Discard a frame whose symbol or ID cannot be identified reliably, without changing published state.

If several messages arrive before publication, retain the latest state for each symbol. Do not average the two connections' prices. A routine reader reconnect must not reset the publisher's ID high-water. A suspected exchange sequence reset needs explicit investigation and recovery; silently clearing the high-water could allow an old quote to win.

### Receipt time, TTL, and the batch cutoff

The JSON `bookTicker` payload has an update ID but no exchange event timestamp. Capture a monotonic local `received_at` when the socket delivers the message, before parsing or publication. Freshness means time since local receipt, not time since Binance created the quote.

At the beginning of each matching tick, clone the published snapshot's `Arc`, release the `watch::Ref`, and capture one monotonic matching time. A pair is usable only if it has a valid quote, its receipt time is not in the future, and:

```text
matching_time - received_at < configured_TTL
```

At exactly the TTL boundary the quote is stale. The matcher fixes the accepted pair price for that batch. Later feed updates cannot reprice its already selected fills or an in-flight settlement. A missing, invalid, or stale pair price skips only that pair; its orders stay in the book. The swap guidance API reads the same snapshot and freshness rule at request time, but does not promise the next batch's price.

On socket disconnect, keep the last valid quote with its original expiry. Disconnect and reconnect do not refresh it. Once it expires, pause that pair until a newer valid quote arrives. A newer identifiable invalid market quote is different: it makes the symbol unavailable when processed, even if its preceding good quote was still inside TTL. A genuine newer update with unchanged bid and ask can renew receipt time because `bookTicker` also reports quantity changes.

### Recovery and failure ownership

The feed uses public REST only to validate configured markets, not for each quote or batch. The pricing worker can report runtime and channel readiness with an empty price snapshot; an unavailable Binance endpoint does not hold the whole solver's startup hostage. Temporary DNS, HTTP, and WebSocket failures are retried inside the feed with cancellable, capped exponential backoff and jitter. HTTP 429 or 418 respects `Retry-After` as a minimum wait. The two readers share a connection-attempt budget, and planned renewals are staggered. A successful TCP handshake alone does not reset retry delay; require a useful subscription or valid update.

A reader close, returned error, unexpected exit, or ordinary unwinding task panic restarts only that reader. Its sibling continues to publish. The supervisor observes and finishes old tasks before replacement; dropping a Tokio `JoinHandle` alone would detach a live task. An irrecoverable feed runtime, publisher, or critical output-channel failure is reported to the solver supervisor for coordinated shutdown. Cancellation interrupts socket reads, metadata requests, and retry waits, and close/join work is bounded.

Unsupported or non-trading symbols and bad operator mappings are reported individually. Malformed messages are handled at the feed boundary, with rate-limited logs. No price is fabricated during an outage. A quiet market can expire without another `bookTicker` update, and a delayed upstream update can look newly received: local TTL does not prove exchange-origin freshness.

### Implementation boundaries

Keep price state in memory. The pricing thread needs no database handle, quote table, event journal, or replay; it starts empty after a restart. Existing token metadata and order/settlement persistence remain separate. The swap guidance API currently fetches token metadata from PostgreSQL; changing its price source does not remove those metadata reads.

### One price source

Binance is the solver's only price source: the clearing path, swap guidance, and the wallet price endpoints all read it. Wallet valuation (`/v1/price`, `/v1/prices`) prices each mapped token by the midpoint of `<ASSET><VALUATION_QUOTE>` (default quote `USDT`, e.g. `ETHUSDT`), validated through `exchangeInfo` like any other market; the valuation quote asset itself is worth exactly one. Responses report `vs_currency` as the quote asset (`usdt`) and `source` as `binance`. Valuation markets use the same readers, publisher, and TTL, but they never price a clearing pair: a pair without its own approved direct symbol does not clear. Devnet, localnet, and tests run the same code against a local mock Binance server (`exchangeInfo` plus the combined `bookTicker` stream).

Reuse `tokio-tungstenite` for WebSocket transport, `reqwest` for public metadata HTTP, Tokio timers and task supervision, the solver's cancellation token, Serde, and existing exact arithmetic. Use a retry strategy helper (`backon`'s exponential builder, used only as a delay iterator) for backoff and jitter, while the feed owns error classification and Binance cooldown policy. Do not nest retry engines. The reviewed Binance Rust SDK introduces callback/runtime and reconnect behavior that would still need our own supervision; for these public endpoints the existing transport libraries make a smaller adapter.

Use bounded frames, HTTP bodies, and connection/close timeouts. Keep the WebSocket polled and flushed so pong responses are sent. Respect Binance's stream, control-message, connection-attempt, and connection-lifetime limits. Explicitly configure the Rustls crypto provider and trusted roots rather than depending on another dependency's incidental TLS setup. Validate external messages before publishing; hold no `watch` borrow across an await or a matching pass.

## Why this arrangement

- **Two live readers instead of one:** a connection can fail without waiting for detection and reconnection before the other supplies quotes. One shared publisher provides a deterministic price per symbol.
- **A pricing thread instead of placing both readers on the main `LocalSet`:** the current single-threaded matcher can occupy that thread during synchronous clearing. Two Tokio tasks on that same thread would also stop being polled. One extra pricing thread isolates socket progress without changing the matcher's runtime or note-client ownership.
- **Public `bookTicker` instead of polling token USD prices:** it supplies the live direct market's best bid and ask and avoids a remote request in the batch path. It is a reference price, not proof that the on-chain asset has the same basis or that makers can quote without inventory skew.
- **Midpoint instead of one side of the spread:** it supplies one symmetric reference for both sides of the batch. Choosing the midpoint does not override order limits or guarantee any order will be eligible.
- **Latest-value snapshots instead of queuing every tick:** clearing needs the newest accepted quote at batch start, not the history of intermediate quote updates. Keep one state per symbol to bound memory.
- **A static direct-market mapping instead of automatic cross pricing:** assets that each trade against USDT do not necessarily have a liquid direct market against each other. Cross pricing would need a separate two-leg timing and freshness policy.

## Consequences and limits

- A market with no approved, valid, fresh direct quote cannot clear internally. Other markets and already submitted settlements continue.
- Two sockets remain in one process and usually share Binance infrastructure and the same network path. A process abort, machine outage, shared network failure, or blocked pricing runtime affects both.
- Binance's midpoint can differ from Miden's fair price because of bridging costs, latency, and maker inventory. This policy favors a stable external anchor at launch; later book-derived or bounded price discovery requires a new decision.
- JSON `bookTicker` cannot establish the exchange generation time. Configured TTL controls receipt age only. Quote age, stale-pair skips, invalidations, reconnects, and publication delay must be observable.
- TTL, spread limit, approved symbols, timeouts, and retry budgets require deployment configuration. The ADR sets their meaning, not universal numerical values. Registering a token at runtime does not automatically authorize and subscribe a new Binance market in V1.

## Verification before rollout

1. Validate configured symbols and direct/reversed orientation against `exchangeInfo`, including unequal asset decimals and unsupported pairs.
2. Property-test exact midpoint, reciprocal orientation, overflow, spread boundary, and integer-unit conversion. Verify `P_forward × P_reverse = 1` before conversion to base units.
3. Exercise the exact TTL boundary, future receipt times, quiet symbols, disconnect with original expiry, duplicate IDs, and newer invalid quotes followed by older good quotes.
4. Run two live readers against the selected endpoints and confirm compatible IDs, publication while either reader restarts, repeated handshake-failure recovery, and bounded task/socket count.
5. Block or slow synchronous matching and verify that both readers can still receive messages and answer pings. Verify the matcher freezes one snapshot per tick and the price API uses the same source.
6. Inject HTTP 5xx, 429/418, socket closure, cancellation during backoff, reader panic, and publisher failure. A transient external failure must not shut down the solver; a critical internal failure must reach its supervisor.
7. Check startup with no quote, per-pair pauses, connection rotation, frame limits, bounded memory, and a sustained run before enabling all launch pairs.

The isolated price-feed design tests performed before this ADR exercised arithmetic, ordering, TTL, local WebSocket/HTTP recovery, and ordinary Tokio reader-task isolation. They did not run the production dual-reader worker or a Miden settlement. Integration and rollout checks above remain required.

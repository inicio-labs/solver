# 2. Filler SDK — a standalone crate that owns the wire protocol

- **Status:** Accepted (implemented). **Updated 2026-07-30** — the wire protocol
  moved from JSON/serde to **miden-native binary**, superseding the original
  "serde-only, zero-miden default" design (see *Update* below).
- **Date:** 2026-06-24
- **Deciders:** solver team
- **Related:** ADR [0001](0001-external-liquidity-routing.md) (the routing feature), [docs/filler-integration.md](../filler-integration.md) (integration guide)

## Context

External DEXes ("fillers") need to integrate with the solver's RFQ router (ADR
[0001](0001-external-liquidity-routing.md)): connect, post quotes, receive
handovers, and consume notes on-chain. We want integration to be **turnkey** while
ensuring a DEX takes on **only the SDK, not the solver's internals** (`miden-client`,
`diesel`, `axum`, the database, internal modules). We also must not let the wire
protocol drift between the two sides.

## Decision

Ship **`pswap-lp-sdk`** as a standalone crate inside the solver repo:

1. **Separate crate, depended on one-way.** The solver depends on the SDK; the SDK
   never depends on the solver. A DEX adds only `pswap-lp-sdk` — no solver crate,
   no `miden-client`, no db/http.
2. **The SDK owns the wire protocol.** `protocol` (`ClientMsg`/`ServerMsg`/`PairSpec`)
   lives in the SDK; the solver's router imports it from there. One definition ⇒ the
   two sides cannot drift.
3. **Miden-native binary wire.** Messages serialize with miden's `Serializable`/
   `Deserializable` over WebSocket **binary** frames, so miden types (`AccountId`,
   `FungibleAsset`, `Note`) travel natively — no serde, no hex, no decimal-string
   prices. The SDK depends on `miden-protocol`/`miden-standards`, but stays independent
   of the solver crate and of `miden-client`.
4. **Typed handover.** `ServerMsg::Handover` carries a decoded `Note` (not hex bytes) +
   `fill_amount`. The note enforces its own on-chain rate, so those two fully specify
   the fill. On-chain helpers (`consume_args`, re-exported `PswapNote`) are always
   available — no feature gate. The DEX runs the consume tx with its own client/gas.
5. **Hands-free push.** `LpClient::serve_quotes(pairs, refresh, price_fn)` keeps a
   fresh quote live per pair: it calls the pricing fn each tick and re-sends, so quotes
   never expire (keepalive) and never go stale-by-omission. The connection is
   auto-reconnecting (backoff + re-auth), so serve_quotes survives drops. There is
   no subscribe step — the quote is the registration (its faucet ids imply the pair).

## Update (2026-07-30): why binary miden-native replaced serde-only

The original design made the `protocol` module **serde-only with zero miden deps** (hex
account ids, decimal-string prices), gating the miden helpers behind a `consume`
feature. Its one real benefit was **version-decoupling**: an external DEX on a different
`miden-protocol` version wouldn't collide with ours, and a non-Rust DEX could speak the
JSON. We reversed it because **fillers ship in lockstep with the solver** (internal,
all-Rust, versioned together), so there is no version skew to protect against and no
non-Rust filler to serve. Given that, coupling to miden types is free, and it **deletes
the whole serde-adapter + stringly-typed layer** (no `parse_decimal_price`, no
hex↔`AccountId`, no `note_hex`, no `serde`/`serde_json`), unifies serialization on
miden's own format, and hands the filler a typed `Note`.

## Reasoning / alternatives

- **Separate crate vs a solver module.** A module forces DEXes onto the whole solver.
  A crate gives them a small, self-contained dependency. (Unchanged.)
- **SDK owns the protocol vs each side defining it.** One definition in the SDK, imported
  by the router, makes drift impossible. (Unchanged.)
- **Miden-native (now) vs miden-free (original).** Miden-free buys version-decoupling +
  non-Rust portability at the cost of a stringly-typed wire and serde glue. Neither
  benefit applies to internal, versioned-together, all-Rust fillers — so miden-native
  wins on type-safety, less code, and one serialization story.
- **Binary vs JSON.** Binary lets miden types serialize natively and carries the note as
  a typed field (no hex-in-JSON), at the cost of human-readability and non-Rust
  portability — both moot here. The WS handshake (URL, `Authorization: Bearer`) stays
  HTTP regardless.
- **Still isolate from the solver crate.** That is the isolation that matters: a filler
  must never pull `diesel`/`axum`/db. Miden types are fine — the filler already runs
  miden to consume the note.

## Consequences

- A DEX integrates against a small surface (the SDK + miden, which it has anyway),
  independent of the solver internals.
- The wire protocol has a single home and cannot drift.
- **Rust-only (firmly).** A non-Rust DEX would have to reimplement miden's binary
  serialization; accepted, since any filler that can *consume* a note already runs miden
  (Rust or WASM).
- We give up cross-version decoupling from a filler's own miden build — acceptable
  because fillers are versioned with the solver.
- A handover is `note` + `fill_amount` only. The note enforces its own on-chain rate, so
  no separate price rides along; if the overfill path later makes the binding fill rate
  differ from the note's intrinsic rate, re-add an echoed price then.
- Verified: `cargo test -p pswap-lp-sdk` green; the crate builds independent of the
  solver crate and `miden-client`.

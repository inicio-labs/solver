# 2. Filler SDK — a standalone crate that owns the wire protocol

- **Status:** Accepted (implemented)
- **Date:** 2026-06-24
- **Deciders:** solver team
- **Related:** ADR [0001](0001-external-liquidity-routing.md) (the routing feature), [docs/filler-integration.md](../filler-integration.md) (integration guide)

## Context

External DEXes ("fillers") need to integrate with the solver's RFQ router (ADR
[0001](0001-external-liquidity-routing.md)): connect, subscribe, post quotes, receive
handovers, and consume notes on-chain. We want integration to be **turnkey** — "no
front work" — while ensuring a DEX takes on **only the SDK's dependencies, not the
solver's** (`miden-client`, `diesel`, `axum`, the database, internal modules). We also
must not let the wire protocol drift between the two sides.

## Decision

Ship **`pswap-filler-sdk`** as a standalone crate inside the solver repo:

1. **Separate crate, depended on one-way.** The solver depends on the SDK; the SDK
   never depends on the solver. A DEX adds only `pswap-filler-sdk`.
2. **The SDK owns the wire protocol.** The miden-free, serde-only `protocol` module
   (`ClientMsg`/`ServerMsg`/`PairSpec`/`parse_decimal_price`) lives in the SDK; the
   solver's router imports it from there. One definition ⇒ the two sides cannot drift.
3. **Lean default build.** Default features pull only serde + tokio + a websocket
   client — **zero miden**. `FillerClient::connect(url, token)` →
   `subscribe`/`quote`/`next_event` (`FillerEvent`), with a background pump task so
   send/receive never block each other.
4. **`consume` feature, opt-in.** On-chain helpers (`decode_note`, `PswapTerms`,
   `consume_args`) sit behind a feature flag that pulls **only** `miden-protocol` +
   `miden-standards` — never `miden-client`. The DEX runs the consume transaction with
   its own client/keystore/gas.
5. **Decode at the DEX end.** The handover carries the note **bytes** (authoritative)
   plus `fill_price`; the SDK decodes the bytes at the DEX's end (feature-gated) rather
   than the solver pre-decoding structured terms onto the wire.

## Reasoning / alternatives considered

- **Separate crate vs a module in the solver.** A module would force DEXes to depend on
  the whole solver (and its `miden-client`/`diesel`/`axum`). A crate is the only way to
  give them a small, version-stable dependency.
- **SDK owns the protocol vs the solver owning it (SDK mirrors).** If each side defined
  the messages, they would drift. Putting the one definition in the SDK and having the
  solver depend on it makes drift impossible and keeps the protocol miden-free.
- **Feature-gate `consume` vs always-on decode.** Always-on decode would pull
  `miden-protocol` into every build, defeating the lean default. Gating keeps the
  decision/pricing path miden-free; a DEX only pays for decode when it wants it — and
  it already runs miden for the consume transaction anyway.
- **Don't pull `miden-client` into the SDK.** Decoding a note needs only
  `miden-protocol`/`miden-standards`. Pulling the client would pin the DEX to our
  client version and drag in a heavy git dependency. The DEX brings its own client.
- **SDK-side decode vs solver-side structured terms.** Sending decoded terms on the
  wire was considered; instead the bytes stay authoritative and the SDK decodes them,
  so the server stays dumb transport and there is one source of truth (the bytes). A
  DEX can re-derive terms at consume time to verify.
- **Keep the note bytes (don't replace with terms).** Consuming a note on-chain needs
  the full `Note` object; terms are a lossy projection — enough to *decide*, not to
  *consume*. So the handover carries bytes; `fill_price` rides alongside.

## Consequences

- A DEX integrates against a tiny dependency surface; the default build has no miden,
  so the SDK never constrains the DEX's miden version.
- The wire protocol has a single home and cannot drift between solver and SDK.
- **Rust-only.** A non-Rust DEX runs a thin Rust sidecar or reimplements the
  (documented, JSON-over-websocket) protocol. Accepted for v1.
- **`fill_price` is forward-compatible.** It is carried now and becomes the binding
  fill rate when the overfill protocol ships; until then the chain settles at the
  note's intrinsic rate.
- Verified: default + `consume` build/test green with no miden/diesel/axum/tonic in the
  default dependency tree; a seamless e2e drives the real router thread through the
  public `FillerClient`.

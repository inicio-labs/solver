# CLAUDE.md — Integrate `pswap-lp-sdk` (Miden PSWAP liquidity)

> **For the partner:** copy this file into the root of the repo where you're building your
> integration and rename it `CLAUDE.md` (or `AGENTS.md`). It tells your coding agent exactly
> how to use the SDK correctly. The human-readable guide is
> [`filler-integration.md`](./filler-integration.md); the SDK rustdoc is the API reference
> (`cargo doc -p pswap-lp-sdk --open`).

## Your task

Build a small always-on service that provides liquidity to a Miden PSWAP solver: keep a
standing **quote** live over one websocket, and **consume** the notes the solver hands you,
on-chain, using our own `miden-client`. The SDK (`pswap-lp-sdk`) handles the wire protocol,
auth, reconnect, and quote refresh. We own: pricing, the risk check, and the consume tx.

## 10-second model

- We post a **quote** = a standing counter-order: *"I'll give up to X of token A to get Y of
  token B."*
- The solver pushes us **handovers** = miden `Note`s that match our quote.
- We **consume** each note on-chain (our gas, our keys) to execute the swap. No custody, no
  per-order request/reply, no message back — the chain settles it.

## Setup

```toml
[dependencies]
pswap-lp-sdk = { git = "<solver-repo-url>", package = "pswap-lp-sdk" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
# plus YOUR miden-client for the consume transaction
```

Config from the operator (out-of-band): websocket URL, a **bearer token** (load from a
secret — never hardcode/log/commit), the **faucet `AccountId`s** for each token, and our
**filling account** `AccountId`.

## Canonical shape (follow this)

```rust
use std::time::Duration;
use miden_client::transaction::TransactionRequestBuilder;
use miden_protocol::asset::FungibleAsset;
use pswap_lp_sdk::consume::{consume_args, PswapNote};
use pswap_lp_sdk::{LpClient, LpEvent, PairSpec};

let mut client = LpClient::connect(url, &token).await?;   // auto-reconnects after this

// Keep a fresh quote live; `price` returns CURRENT (give_amount, want_amount) in BASE UNITS.
let _quotes = client.serve_quotes(
    vec![PairSpec { offered: give_faucet, requested: want_faucet }],
    Duration::from_secs(10),                 // ≈ half the operator's quote TTL
    move |_pair| Some(live_price()),         // Option<(u64, u64)>; None = skip this tick
);

while let Some(ev) = client.next_event().await {
    match ev {
        LpEvent::Handover(h) => {
            let pswap = PswapNote::try_from(&h.note)?;  // terms; pswap.offered_asset() = we RECEIVE
            if !accept(&pswap, h.fill_amount) { continue; }          // our risk check
            let args = consume_args(0, h.fill_amount)?;              // (account_fill, note_fill)
            let req = TransactionRequestBuilder::new()
                .input_notes([(h.note.clone(), Some(args))])
                .build()?;
            if let Err(e) = our_client.submit_new_transaction(our_account_id, req).await {
                tracing::warn!(note = ?h.note.id(), %e, "consume failed; skip");   // do NOT retry
            }
        }
        LpEvent::Reconnecting { attempt } => tracing::warn!(attempt, "link lost; retrying"),
        LpEvent::Reconnected => {}   // re-post only if quoting manually; serve_quotes self-heals
        LpEvent::Error(e) => tracing::warn!(%e, "router rejected our message"),
        LpEvent::Disconnected { reason } => { tracing::error!(%reason, "gave up"); break }
        LpEvent::AuthOk | LpEvent::Ask { .. } => {}
    }
}
```

## HARD RULES (do not violate)

1. **Orientation — the #1 money bug.** In a **quote**, `offered` = the token WE GIVE,
   `requested` = the token WE WANT. A handed-over **note** is the *mirror*:
   `note.offered_asset()` = what we RECEIVE, `note.requested_asset()` = what we PAY. Same
   word, opposite sides. To *buy iMIDEN paying iUSDT*: `offered = iUSDT` (give),
   `requested = iMIDEN` (want). If unsure, STOP and ask the human — do not guess.
2. **Base units only.** All amounts are the token's smallest on-chain unit. Never pre-scale
   by decimals or compute a "human price." The ratio of the two amounts is the rate.
3. **Idempotent consume.** Dedupe handovers by `h.note.id()`. Never submit two txs for one
   note id. If a consume fails ("already nullified" / RPC error), **drop it — do not retry**
   a note that may already be consumed.
4. **Handle every `LpEvent` arm.** Especially `Handover`, `Reconnecting`, `Disconnected`.
   `Disconnected` is terminal (stream ends).
5. **The note carries the rate.** `note` + `fill_amount` fully specify the fill; the rate is
   fixed on-chain. Our `accept(...)` decides *whether* to fill, not the price.
6. **Never log or hardcode the token.** Load from a secret store.
7. **Keep the `QuoteTask`.** Bind `serve_quotes`'s return (`let _quotes = …`); dropping it
   detaches the quoting loop. Keep the `price` closure cheap and non-panicking (it runs inline).
8. **We run the consume tx, not the SDK.** Use OUR `miden-client`. The SDK never holds funds
   or signs.

## API you will use (exact)

- `LpClient::connect(url: &str, token: &str) -> Result<LpClient, LpError>` — errors
  `AuthRejected` (bad token) or `Transport` (bad url/connect).
- `LpClient::serve_quotes(pairs: Vec<PairSpec>, refresh: Duration, price: Fn(&PairSpec) ->
  Option<(u64 /*offered/give*/, u64 /*requested/want*/)>) -> QuoteTask` — hands-free quoting.
- `LpClient::quote(offered: FungibleAsset, requested: FungibleAsset, valid_for_ms: Option<u64>)
  -> Result<(), LpError>` — manual quote (rare; prefer `serve_quotes`).
- `LpClient::next_event().await -> Option<LpEvent>` — event loop; `None` = SDK stopped.
- `Handover { note: Note, fill_amount: u64 }` (in `LpEvent::Handover`).
- `consume::consume_args(account_fill: u64, note_fill: u64) -> Result<Word, LpError>` — pass
  `account_fill = 0` and `note_fill = h.fill_amount` for a note-side fill.
- `consume::PswapNote::try_from(&Note)` — `.offered_asset()` (we receive), `.storage()
  .requested_asset()` (we pay), `.storage().creator_account_id()` (settles back to maker).
- `LpError`: `AuthRejected | Transport(String) | Closed(String) | InvalidQuote(String) |
  Protocol { code, msg } | Consume(String)`.

## Testing / definition of done

- Unit-test `price` and `accept` — assert the quote **gives the token we intend to give**.
- Drive `LpClient` against a local mock websocket (adapt `mock_router` from the SDK's
  `client.rs` tests) to exercise connect → quote → injected `Handover` → our handler, with no
  chain.
- On **testnet** first: run with `accept = false`, log handovers, confirm they offer the token
  we asked for (orientation sanity), then do **one real fill** and verify the right asset
  landed in our account.
- Confirm auto-reconnect: kill the network mid-run → see `Reconnecting` → `Reconnected`,
  quoting resumes, no crash.
- Metrics: handovers received vs consumed (a growing gap risks de-listing); alert on
  `Disconnected`.

Done = quotes stay live across reconnects, every `LpEvent` arm handled, consume is idempotent,
and a testnet fill delivered the expected asset.

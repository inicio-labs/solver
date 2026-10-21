# Briefing for coding agents

You are helping a market maker integrate with the Inicio solver on the Miden testnet. Work from this folder (`maker-kit/` in the inicio-labs/solver repository); the rest of the repository is the solver itself and you do not need it. Read in this order: `onboarding.md` (environment), `../docs/market-maker-guide.md` (protocol), `../crates/solver/proto/maker/v1/gateway.proto` (contract), then `example/src/` (a working client).

## The system in one paragraph

The maker locks the tokens it offers in a **PSWAP note** (a Miden note that offers token A for token B and can be partly filled), creates and commits it from its own Miden account, then registers it with the solver's **maker gateway** (`SubmitOrder`, gRPC over TLS, API key in `authorization: Bearer`). The solver matches each pair in batches at one price, the Binance mid, and settles fills in its own on-chain transaction that consumes the maker's note and creates a **payback note** for the maker (and a **remainder note** after a partial fill, which keeps trading as the same order). The maker follows an ordered **event feed** (`StreamEvents`) and collects paybacks by consuming them from its account.

## Endpoints and versions

- Gateway: `https://35-175-40-181.sslip.io` (gRPC, TLS). Price API: same host, HTTPS JSON.
- Miden testnet RPC `https://rpc.testnet.miden.io`, remote prover `https://tx-prover.testnet.miden.io`.
- Test-token faucet API: `POST https://faucet-api.forkchoice.xyz/api/mint` `{token, address, amount, note_type}`.
- SDK pins (exact): `miden-client` and `miden-client-sqlite-store` `=0.17.5`, `miden-protocol` and `miden-standards` `=0.17.1`. Client releases before 0.17.3 cannot consume USDCX, the fee token.
- Token IDs and decimals: `onboarding.md` or `GET /v2/pairs`. Use base units (integers) everywhere; never floats.

## Rules that the protocol depends on

1. **Save every command before sending it** (request ID, sequence, payload; for a submit also the note bytes and lineage ID). On a lost reply, resend the identical command; never mint a new request ID for it. `ALREADY_EXISTS` means a request ID or sequence was reused for something different: a bug, never success.
2. **One sequence counter per maker**, shared by every process and key. `CancelAll` cancels orders whose submit sequence is lower than the cancel's.
3. **Persist the event cursor together with the event's effect**, and reconnect `StreamEvents` from it. Events are numbered contiguously per maker.
4. **Book a fill only on `SettlementResolved` with `committed = true`.** `SettlementPending` is a reservation, not a fill.
5. **Cancel by lineage ID** (47 bytes: creator account ID ‖ serial words with `serial[3] − depth`). Compute it once from the original note; it is the same for all remainders. See `example/src/wallet.rs::lineage_id`.
6. **A cancel stops matching; it does not return funds.** Reclaim the note (consume it from your own account) after any settlement counted in the cancel's `settling` has resolved.
7. **Keep the original note bytes** of every order. You need them to reclaim, and to rebuild payback and remainder notes for private paybacks (guide, section 10).

## Miden pitfalls (learned on this testnet)

- **Prover timeout:** `RemoteTransactionProver::new(url)` gives up after 10 s. Use `.with_timeout(Duration::from_secs(45))`.
- **Expiry:** a transaction that moves USDCX (e.g. claiming a USDCX note) expires 20 blocks (~60 s) after its reference block. Call `client.sync_state()` immediately before building every transaction, and keep proving under that window.
- **One transaction per account at a time.** A second transaction built on the same account state is rejected by the mempool. Serialize per account; scale with more accounts.
- **Consume incoming notes one per transaction** and skip failures: a note you cannot consume (a time lock, or another account's note that shares your 32-bit note tag) fails a whole batch.
- **Never consume your own PSWAP order notes when claiming** unless you mean to reclaim them; filter by `PswapNote::script_root()`.
- **Fees:** every transaction pays ~120 base units of USDCX. Monitor the balance.
- `miden_client::Client` is `!Send`: use a current-thread Tokio runtime with a `LocalSet`, as the example does.

## Working on the example

- Build: `cd example && cargo build --release` (protox compiles the proto; no system `protoc`).
- Config: `maker.toml` (from `maker.toml.example`); the API key lives in `api-key.txt`, never in code or config.
- State: `wallet/journal.json` holds the account ID, the sequence counter, every command with its reply, every order with its note bytes, and the event cursor.
- Modules: `gateway.rs` (gRPC client), `wallet.rs` (Miden account, notes, claims), `journal.rs` (durable state), `config.rs`, `main.rs` (commands).
- Not implemented, by design: pricing logic, rebuilding private remainders after partial fills (needed to reclaim them), multi-order quoting loops. The guide covers each.

## Verifying changes

A change is done when, against the live testnet: an order goes `LIVE`, `orders` lists it, `cancel` returns `stopped settling=0`, `reclaim` returns the tokens, an exact resend of a command returns the stored reply (`replayed`), and `events` after a restart resumes from the saved cursor without gaps.

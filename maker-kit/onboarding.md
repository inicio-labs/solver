# Inicio Solver: Market Maker Onboarding (Miden testnet)

This document gives you everything needed to quote on the Inicio solver on the Miden testnet: endpoints, credentials, tokens, how to fund your account, and how orders trade and settle. It comes with an integration kit, the `maker-kit/` folder of https://github.com/inicio-labs/solver (branch `vaibhav/pswap-017-integration`): a working example client to build on, next to the protocol reference and the gRPC contract.

## 1. Access

| Item | Value |
|---|---|
| Maker gateway (gRPC over TLS) | `https://35-175-40-181.sslip.io`, service `solver.maker.v1.MakerGateway` |
| API key | Issued to you by Inicio Labs with your maker name. Send it on every call as `authorization: Bearer <API_KEY>`. |
| Price API (HTTPS, JSON) | `https://35-175-40-181.sslip.io` (see `price-api.md`) |
| Test-token faucet | Web: `https://faucets.forkchoice.xyz` · API: `https://faucet-api.forkchoice.xyz` |
| Miden network | Testnet. RPC `https://rpc.testnet.miden.io` · remote prover `https://tx-prover.testnet.miden.io` |
| Solver account | `0xf2ffdbcabb56d4511c23b524022aa2` (consumes your notes in settlements) |
| Miden SDK | `miden-client` 0.17.5, `miden-protocol` and `miden-standards` 0.17.1 |

Keep the API key out of source control. We can issue more keys for the same maker (they share orders and events) and revoke a key at any time; revoking a key does not cancel your orders.

## 2. Tokens and pairs

| Token | Faucet ID | Decimals |
|---|---|---|
| IETH | `0xe33498f1176661512b5ac4ded86ec7` | 8 |
| IUSDT | `0x094698fc4bc531d136db0866f38902` | 8 |
| IBTC | `0xaf8444c4a7c091114666adba25c5d0` | 8 |
| IMIDEN | `0x750052a4af10cd51141f52c0ec2ada` | 8 |
| USDCX | `0x4cbdcaffe75f0a317482224dae6436` | 6 |

| Pair | Priced from (Binance) |
|---|---|
| IETH / IUSDT | ETHUSDT |
| IBTC / IUSDT | BTCUSDT |
| IETH / IBTC | ETHBTC |
| IMIDEN / IUSDT | SOLUSDT |
| IMIDEN / IBTC | SOLBTC |
| USDCX / IUSDT | USDCUSDT |
| USDCX / IBTC | BTCUSDC |

Every pair trades in both directions. On testnet, IMIDEN is priced as SOL and USDCX as USDC. `GET /v2/pairs` on the price API returns this list in JSON; build from it rather than from this table.

## 3. Funding your account

**Test tokens (IETH, IBTC, IUSDT, IMIDEN).** Mint from our faucet as often as you like, up to 1,000,000 tokens per request. Amounts are base units (1,000,000 tokens with 8 decimals = `100000000000000`). The address can be hex (`0x…`) or bech32 (`mtst1…`).

```bash
curl -X POST https://faucet-api.forkchoice.xyz/api/mint \
  -H 'content-type: application/json' \
  -d '{"token":"IUSDT","address":"<your account>","amount":100000000000000,"note_type":"public"}'
```

The reply carries the transaction and note IDs. The tokens arrive as a public note, usually within 30 s; consume it with your wallet (`maker claim` in the kit, run again if a token has not arrived yet). `GET https://faucet-api.forkchoice.xyz/api/tokens` lists the tokens and their limits.

**USDCX, the network fee token.** Every Miden transaction you send (creating an order, reclaiming one, claiming a payback) pays a fee in USDCX, about 120 base units (0.00012 USDCX). Registering a new account with the network funds it once with USDCX (1 USDCX at the time of writing, several thousand transactions; `maker init` does this). For more, use the Miden testnet faucet at `https://faucet.testnet.miden.io`, or ask us and we will send a larger amount.

## 4. How your orders trade

1. **You create an order note.** An order is a PSWAP note offering an amount of one token for an amount of another, created and committed on chain by your own account. We recommend private order notes with public paybacks.
2. **You register it.** `SubmitOrder` with the note's bytes. The reply `accepted` means stored, not yet live. Once the solver sees the note committed and unspent, you receive `OrderStatus LIVE`.
3. **The solver matches it.** Each pair clears in batches at one price, the Binance mid for that pair, every few seconds. Your order is eligible when its limit is at or better than that price after the fee. `/v2/pair-price` returns that threshold as `fillPrice`.
4. **Settlement.** The solver consumes your note in an on-chain transaction. You receive `SettlementPending`, then `SettlementResolved` with the fill. A fill pays you at least your order's own price. A full fill pays the clearing price less the fee. A partial fill leaves a remainder note that keeps trading as the same order (same lineage ID).
5. **You collect.** Each fill creates a payback note addressed to your account. Consume it to move the tokens into your account.

| Typical testnet timing | |
|---|---|
| Create and prove an order note | 3 to 8 s |
| Note transaction sent → `LIVE` | about 4 s |
| Matched → settlement confirmed on chain | about 8 s (median), 25 s (slow cases) |
| Block time | about 3 s |

**Fees.** The solver's clearing fee is 50 parts per million, taken only from price improvement; it never takes you below your own limit. Network fees (USDCX) are paid by whoever sends a transaction: you for your orders, reclaims and claims; the solver for settlements.

**Cancelling.** `CancelOrder` stops one order and all its future remainders; `CancelAll` stops everything older in a scope (all, one market, or one direction). A cancel stops matching; your tokens stay in the note until you reclaim it. Reclaim after any settlement counted in the cancel's `settling` has resolved.

**Self-trade prevention.** Your buy and your sell on the same pair never fill each other. If they cross, the side of your newest order trades and your older crossing orders sit out; once the newer order is reserved for a settlement, the older ones are cancelled by the solver and you receive `OrderStatus CANCELLED` (reason `self_trade_prevention`) for each. Treat it like your own cancel: the order is final, reclaim the note. Only orders you submit as private notes are reliably covered. Section 12 of the guide has the rule and examples.

**Expiry.** Optional, per order, enforced by the solver off chain. There is no expiry event, and expiry does not return funds; reclaim the note.

## 5. Integration kit

Clone it with `git clone -b vaibhav/pswap-017-integration https://github.com/inicio-labs/solver` and work in its `maker-kit/` folder with your own editor or coding agent. Paths below are relative to `maker-kit/`:

| Path | Contents |
|---|---|
| `CLAUDE.md` | Instructions for a coding agent: architecture, rules and testnet pitfalls. Start here. |
| `README.md` | Build and run the example, step by step. |
| `onboarding.md` | This document. |
| `../docs/market-maker-guide.md` | The full maker gateway protocol: commands, idempotency, events, settlement accounting, errors. |
| `../docs/price-api.md` | The price API: `/v1/price`, `/v2/pairs`, `/v2/pair-price`, `/v2/swap-eta`. |
| `../crates/solver/proto/maker/v1/gateway.proto` | The gRPC contract. |
| `example/` | A Rust command-line maker: create and fund a wallet, place orders, follow events, cancel, reclaim, claim paybacks. |

To start with Claude Code: run `claude` in `maker-kit/` and ask, for example, *"Read CLAUDE.md, then build the example and walk me through placing and cancelling one order."*

## 6. Testnet notes

- **Remote prover timeout.** miden-client gives up on the remote prover after 10 s by default, which is too short for many transactions; set about 45 s (`RemoteTransactionProver::with_timeout`), under the expiry below.
- **Transaction expiry.** A transaction that moves USDCX, such as claiming a USDCX note, expires 20 blocks (about 60 s) after the block it was built on, because the token's transfer policy sets that limit. Sync your client right before building each transaction.
- **One account, one transaction at a time.** Two transactions built on the same account state conflict; the second is rejected. Serialize the transactions of one account, or use several accounts.
- **Claim notes one at a time,** skipping any that fail. One note that cannot be consumed fails a whole multi-note transaction.
- **Liquidity on testnet.** Inicio runs a reference liquidity bot that answers user orders. Your quotes compete with it and with each other.
- **Maintenance.** The solver may restart for upgrades. Reconnect the event stream from your saved cursor; nothing is lost, and your orders stay live while you are disconnected.

## 7. Before you trade

Run the checklist in section 14 of `market-maker-guide.md`: submit, cancel, cancel-all, retry with the same request ID, reconnect the event stream from a saved cursor, follow one order to a committed fill and claim the payback.

Questions: your Inicio Labs contact.

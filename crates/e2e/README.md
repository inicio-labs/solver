# e2e — solver devnet end-to-end harness

A standalone crate (kept out of the solver / consume-script crates and out of
the normal test build) that exercises the **whole solver pipeline against a live
Miden devnet**: it provisions real on-chain accounts, funds the solver, creates
matchable PSWAP orders, then runs the solver in-process and verifies it
ingests → matches → settles them on-chain.

It talks to devnet via `ClientBuilder::for_devnet()`, which wires the devnet RPC
(`rpc.devnet.miden.io`) **and** the remote prover (`tx-prover.devnet.miden.io`),
so proofs are offloaded to the network — no heavy local proving.

All state lives under `./e2e/` (SQLite stores, keystores, `provisioned.json`,
the generated `solver.devnet.toml`). These are gitignored.

## Commands

```bash
# 1. Create + deploy the solver wallet and two fungible faucets (MTA, MTB) on
#    devnet, fund the solver's inventory buffer, and write artifacts + config.
#    Prints the SOLVER ACCOUNT ID + faucet ids.
cargo run -p e2e --release -- provision

# 2a. SELF-DRIVING: create N opposing PSWAP pairs (user1: MTA->MTB, user2:
#     MTB->MTA) that the solver can match.
cargo run -p e2e --release -- load --rounds 1

# 3. Run the solver in-process against devnet (deterministic fixed prices — no
#    CoinGecko key needed). Reports the solver balance delta = captured spread.
cargo run -p e2e --release -- run --secs 180

# 2b. HAND-OFF (you drive): mint tokens to your own wallet so you can create
#     P2ID / PSWAP notes yourself, then `fund` makes the solver consume any
#     committed notes targeting it (mints AND your P2ID).
cargo run -p e2e --release -- mint --to 0x<your_wallet> --token a --amount 10000000
cargo run -p e2e --release -- fund --amount 50000000
```

## The two flows

* **Self-driving (CI-style):** `provision` → `load` → `run`. Fully automated;
  asserts settlement via the solver's on-chain balance delta (a positive Δ on
  both tokens == opposing PSWAPs matched and settled, spread captured).
* **Hand-off (you create the orders):** `provision` gives you the solver account
  id + faucet ids. Mint MTA/MTB to your wallet(s) (`e2e mint`, since the faucet
  keys live in `e2e/operator_keystore`), create P2ID notes to the solver and/or
  opposing PSWAPs, then `e2e fund` (solver consumes the P2ID) and `e2e run`.

## Design notes

* **Two client contexts.** The *solver* context (`e2e/solver_*.sqlite3` +
  `e2e/solver_keystore`) owns the solver account — its executor store IS the one
  the running solver opens, so the runtime finds the account + key. The
  *operator* context (`e2e/operator.sqlite3` + `e2e/operator_keystore`) owns the
  faucets and the user wallets that mint and create orders.
* **The matcher pairs two opposing user orders** (it is not a single-order
  inventory filler), so `load` always creates A→B and B→A pairs. Each offers
  more than the counterparty requests, leaving a spread the solver captures.
* **Fixed prices in `run`.** `run` injects a `MockPriceClient` (both tokens at
  $1.00) via the solver's `make_price_client` seam, so matching is deterministic
  and needs no CoinGecko key. The faucet `external_symbol`s in the generated
  config (`tether`/`ethereum`) only matter if you run the real `solver-bin`
  against CoinGecko instead.
* The solver buffer (provisioned inventory) lets the executor bridge fills
  during settlement.

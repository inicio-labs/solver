# Inicio maker kit (Miden testnet)

Everything needed to quote on the Inicio solver as a market maker: the onboarding guide, a working example maker in Rust, and pointers to the protocol guide, the price API reference and the gRPC contract elsewhere in this repository.

```bash
git clone -b vaibhav/pswap-017-integration https://github.com/inicio-labs/solver
cd solver/maker-kit
```

| Path | What it is |
|---|---|
| [`onboarding.md`](onboarding.md) | Endpoints, credentials, tokens, funding, how orders trade. **Read first.** |
| [`../docs/market-maker-guide.md`](../docs/market-maker-guide.md) | The maker gateway protocol in full. |
| [`../docs/price-api.md`](../docs/price-api.md) | Prices, pairs and order checks over HTTPS. |
| [`../crates/solver/proto/maker/v1/gateway.proto`](../crates/solver/proto/maker/v1/gateway.proto) | The gRPC contract (`solver.maker.v1.MakerGateway`). |
| [`example/`](example/) | `maker`, a command-line maker built on miden-client 0.17.3. |
| [`CLAUDE.md`](CLAUDE.md) | Briefing for a coding agent working in this kit. |

## Run the example

Requirements: Rust (the toolchain is pinned in `example/rust-toolchain.toml`; `rustup` installs it), about 4 GB of free disk for the first build, and your API key.

```bash
cd example
cp maker.toml.example maker.toml
printf '%s' 'mmk_…your key…' > api-key.txt && chmod 600 api-key.txt
cargo build --release
alias maker=./target/release/maker
```

**1. Create and fund a wallet.** `init` creates a wallet account, stores its key under `wallet/`, and registers it with the network, which sends it USDCX for fees (1 USDCX at the time of writing).

```bash
maker init
```

Mint test tokens to the printed address (any amount up to 1,000,000 tokens per request, as often as you like):

```bash
ADDR=0x…   # from `maker init` or `maker address`
for T in IETH IUSDT IBTC IMIDEN; do
  curl -s -X POST https://faucet-api.forkchoice.xyz/api/mint -H 'content-type: application/json' \
    -d "{\"token\":\"$T\",\"address\":\"$ADDR\",\"amount\":100000000000,\"note_type\":\"public\"}"; echo
done
```

Wait about 30 s for the notes to commit, then move everything into the account (run `claim` again if a token has not arrived yet):

```bash
maker claim
maker balance
```

**2. Check the price.** Quote around the solver's clearing price:

```bash
curl -s "https://35-175-40-181.sslip.io/v2/pair-price?offered_faucet=0xe33498f1176661512b5ac4ded86ec7&requested_faucet=0x094698fc4bc531d136db0866f38902"
```

`fillPrice` is the highest ask that fills now (here: IUSDT per IETH when selling IETH).

**3. Place an order and follow it.**

```bash
maker quote --offer IETH:0.1 --request IUSDT:248 --expires-in 600
maker events            # replays your feed, shows OrderStatus LIVE
maker orders            # what the solver can match now
```

`quote` creates the order note on chain (private, public paybacks), records it in `wallet/journal.json`, and submits it. Add `--public` for a public order note.

**4. Cancel and take the tokens back.**

```bash
maker cancel <lineage-hex>     # printed by `quote`
maker reclaim <lineage-hex>    # only for an order with no fill yet
```

**5. After a fill,** `maker events` shows `SettlementPending` and `SettlementResolved`. Then `maker claim` consumes the payback note and `maker balance` shows the tokens.

`maker events --follow` stays connected and reconnects from the saved cursor. `maker help` lists every command.

## Notes

- The example keeps its state in `wallet/` (keys, the Miden store, the journal). Back it up; the keys control your funds.
- It is a reference, not a trading engine: one order per command, no pricing logic, no rebuilding of private remainders (see the guide, section 10).
- Every Miden transaction pays about 120 base units of USDCX. `maker balance` shows what is left.

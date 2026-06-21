# mock-mirror

A devnet/testnet **liquidity harness** for the solver. On a fresh network there
are no natural counter-orders, so the solver never matches. This daemon watches
for user PSWAP notes and posts *favorable* counter-orders from a single account,
so the solver's full **match → settle** pipeline runs end-to-end.

It is **funded externally** — it never mints. You mint the public faucets to its
address; it spends that inventory to post counters. (Trade proceeds also arrive
back as P2ID notes and are tracked on sync.)

## Use

```bash
# 1. Create the mock account; prints its address (hex + bech32 mdev…).
mock-mirror provision mock.toml

# 2. Fund the printed mdev… address from your faucet (IBTC/IUSDT/IETH/IMIDEN),
#    then set [mock] account_id = "0x…" in mock.toml.

# 3. Run the mirror loop.
mock-mirror run mock.toml
```

See [mock.toml.example](mock.toml.example) for the full config. Each tick:
1. **Mirror** — for every new user PSWAP, build a strictly-favorable counter
   (offers the user's requested token, asks slightly less of the offered token
   so the solver keeps `spread_bps`), capped at `max_mirrors_per_tick`.
2. **Monitor** — warn (never mint) when a tracked token's balance falls below
   its `low_water`, signalling the operator to top it up.

The counter math (`favorable_counter`) is pure and unit-tested: full + partial
fills stay matchable and clear the solver's strict cross-product gate; dust is
skipped.

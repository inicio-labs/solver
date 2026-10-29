# Miden Price API — dApp Integration

Read-only HTTP API for swap UIs:

- `/v1/price`, `/v1/prices`: a token's **price in USDT** (the exact Binance
  Spot midpoint of its `<ASSET>USDT` market) plus its **on-chain decimals**.
- `/v2/pair-price`: a pair's expected batch price, to build an order's ask
  from.
- `/v2/swap-eta`: whether the order a wallet is about to sign fills now,
  partly, or not, and why.
- `/v1/swap-eta`: the original swap check, frozen for wallets built against
  it. New integrations use `/v2`.

Field names are snake_case on `/v1/price(s)` and camelCase on the swap
endpoints.

## Base URL

```
https://35-175-40-181.sslip.io        # devnet — HTTPS, browser-ready (use this)
http://35.175.40.181:8080             # same API over plain HTTP (server-side / curl only)
```

> Use the **HTTPS** URL from any browser dApp — it has a real Let's Encrypt cert,
> so there's no mixed-content block from an `https://` page. (`sslip.io` is just a
> DNS hostname that resolves to the box IP; a custom domain can replace it later.)
> The plain-HTTP `:8080` URL works for server-to-server / curl but an HTTPS page
> will block it as mixed content.

**CORS:** enabled (any origin, `GET`) — browser `fetch()` / extensions work.

---

## Endpoints

### `GET /v1/price/{faucet_id}`

One token's price. `faucet_id` is the **hex** faucet account id (`0x…`).

Query params (optional):

| Param | Values | Meaning |
|---|---|---|
| `precision` | `full` (default) \| `0`–`18` | decimal places of the **price number**. `full` = exact. |
| `allow_stale` | `true` | return `200` + `"stale":true` instead of `503` when the token's quote is stale (at least the solver's quote TTL old — the same TTL the solver clears with). |

**200 response:**

```json
{
  "faucet_id": "0xb3722d97036169910fc0eeaccce29b",
  "ticker": "IBTC",
  "vs_currency": "usdt",
  "price": "10",
  "precision": "full",
  "decimals": 8,
  "as_of": 1781906150,
  "stale": false,
  "source": "binance"
}
```

| Field | Type | Meaning |
|---|---|---|
| `faucet_id` | string | canonical hex id of the token's faucet |
| `ticker` | string | on-chain token symbol (e.g. `IBTC`); left out when unknown |
| `vs_currency` | string | quote currency (`usdt`) |
| `price` | **string** | price of **ONE WHOLE token** in `vs_currency`. Exact decimal string (no float rounding); USDT itself is `"1"`. |
| `precision` | string | precision applied to `price` (`full` or `0`–`18`) |
| `decimals` | number \| null | the token's **on-chain decimals** (here: `8`) |
| `as_of` | number | unix epoch seconds the solver received this quote; for the quote asset itself (`USDT`, always `"1"`) it is the request time |
| `stale` | bool | `true` if the quote is at least the solver's quote TTL old; never `true` for the quote asset itself |
| `source` | string | price source label (`binance`) |

> **Valuing an amount.** On-chain amounts are in **base units**.
> `value = (amount / 10^decimals) * price`.
> e.g. `250000000` base units of IBTC → `250000000 / 10^8 = 2.5` IBTC → `2.5 × 10 = 25` USDT.

### `GET /v1/prices?ids=a,b,c`

Batch. Returns an object keyed by `faucet_id`; unknown, unpriced and (unless
`allow_stale=true`) stale ids are **omitted**. Max 50 ids.

```json
{
  "0xb3722d97036169910fc0eeaccce29b": { "ticker":"IBTC","price":"10","decimals":8, ... },
  "0x3ae73d7f166f723132e3acbba75e75": { "ticker":"IETH","price":"5","decimals":8, ... }
}
```

### Swapping with `/v2`

Only pairs configured as a Binance clearing market (for example `ETHUSDT`)
can swap; both tokens having a `/v1/price` is not enough. `GET /v2/pairs`
lists them: fetch it once for the token pickers.

1. **Price.** `GET /v2/pair-price` gives `fillPrice`, the price to build the
   ask from: the expected batch price per whole offered token, less the fee.
2. **Ask.** With the user's slippage as a fraction (`0.005` = 0.5%), in
   BigInt or a decimal library, never floats:
   `requested_amount = floor(offered_amount × fillPrice × (1 − slippage) × 10^requestedDecimals / 10^offeredDecimals)`
3. **Verdict.** `GET /v2/swap-eta` with both amounts, **before the user
   signs**. Off market: go back to step 1 for a fresh price.

**What the user receives.** A full fill pays the batch price less the fee
(`expectedRequestedAmount`), whatever the slippage. On a pair without a
price band the batch price is the Binance mid; on a pair with one it can be
a quote in the book, inside the band ([Pairs with a price
band](#pairs-with-a-price-band)). A partial fill pays the order's own rate,
so there the user gets exactly what they asked. Slippage protects against
the price moving before the next batch; a lower ask also queues ahead of
orders on the same side that ask more.

**Reading the verdict.** Take the first row that matches:

| Outcome | Answer | Show |
|---|---|---|
| Settlements paused; the order waits | `settlementsRunning: false` | "Settlement delayed" |
| No price for the pair | `none`, reason `no_market` / `no_price` | "This pair isn't traded" / "Prices are unavailable right now; try again shortly" |
| Can't fill because of the price | `off_market` (`none`, reason `price`) | "The price moved." Get a fresh `/v2/pair-price` and rebuild the ask |
| Not enough orders at the current price | `none`, reason `liquidity` | "Not enough orders at the current price right now" |
| Can fill | `at_market` + `full` | "You receive ≈ `expectedRequestedAmount`, at least `requestedAmount`" |
| Can fill partly | `at_market` + `partial` | "Only `fillableOfferedAmount` fills now; the rest waits" |
| Close: needs a small price move | `tolerated` + `full` or `partial` | "May take longer: fills when the price moves slightly" |

Once the order is in the book, asking again counts it twice: as volume ahead
of itself, and in the expected price. An order-status endpoint for the "not
filling soon, cancel?" prompt is planned.

### Pairs with a price band

A pair can have a price band (`band_bps` in the solver's configuration). On
such a pair every round still clears at one price for all its full fills,
but that price is not always the Binance mid: it is the price inside
`mid ± band` that trades the most volume, and among those the one closest to
the mid. When more users buy than sell, a maker's ask above the mid can set
it; when more sell than buy, a bid below. Users never pay outside the band.

Both swap endpoints quote that price as `expectedPrice`, from the solver's
own rule run on its book:

- `/v2/pair-price` adds one order of the pair's reference size in the asked
  direction, so the answer is the price a typical order gets now;
- `/v2/swap-eta` adds the order being checked, so the answer is for its own
  size and limit, and the verdict is judged at that price.

On a pair without a band, `expectedPrice` equals `marketPrice` and every
other field is as before.

The expected price is an estimate for the next batch, on the book as it is
now. Not modelled: a maker's own crossed orders, which the solver leaves out
while pricing; all-or-nothing orders; the solver's fallback to the mid when
nothing can trade at the band price; the per-side order cap; and the Binance
mid moving before the batch. Show it as "≈", with `requestedAmount` as the
guaranteed minimum, and ask again right before the user signs.

### `GET /v2/pairs` — what the solver swaps

The tokens the solver swaps, each listed once, and the pairs between them.
It changes only when the solver restarts with a new configuration, so it is
cacheable for five minutes (`Cache-Control: public, max-age=300`). Prices,
fees and freshness come from `/v2/pair-price`. While the solver is still
confirming its Binance markets at startup (up to a minute) it answers `503`
`not_ready`.

```json
{
  "tokens": [
    { "faucetId": "0x…", "ticker": "ETH",  "decimals": 18, "binanceAsset": "ETH" },
    { "faucetId": "0x…", "ticker": "USDT", "decimals": 6,  "binanceAsset": "USDT" },
    { "faucetId": "0x…", "ticker": "BTC",  "decimals": 8,  "binanceAsset": "BTC" }
  ],
  "pairs": [
    { "name": "ETH-USDT", "binanceSymbol": "ETHUSDT", "faucetIds": ["0x…", "0x…"] },
    { "name": "BTC-USDT", "binanceSymbol": "BTCUSDT", "faucetIds": ["0x…", "0x…"] }
  ]
}
```

| Field | Meaning |
|---|---|
| `tokens` | every token in at least one pair, in configuration order. A token the solver only values (`/v1/price`) but does not swap is not listed |
| `faucetId` | hex faucet id, as every other endpoint takes it |
| `ticker`, `decimals` | the token's on-chain symbol and decimals. Startup requires them, so they are set in practice; the type allows `null` |
| `binanceAsset` | the Binance asset code the token is priced as (usually the ticker; a wrapped token may differ) |
| `pairs` | every pair the solver clears internally, in configuration order: configured, and confirmed by Binance at startup. Whether a pair is trading right now is `/v2/pair-price`'s answer (`503` when it is not) |
| `name` | the pair's name in the solver's configuration |
| `binanceSymbol` | the Binance market that prices the pair |
| `faucetIds` | the pair's two tokens, unordered: a pair swaps in both directions. Each is in `tokens` |

A "to" picker for a chosen token lists the other token of every pair that
contains it. Then `/v2/pair-price` gives the price in the chosen direction.

### `GET /v2/pair-price` — the price to build an ask from

| Param | Meaning |
|---|---|
| `offered_faucet`, `requested_faucet` | hex faucet ids; the order offers the first and requests the second |

**200 response** — offering ETH (18 decimals) for USDT (6 decimals), mid 2500, fee 0.1%:

```json
{
  "offeredFaucet": "0x…", "requestedFaucet": "0x…",
  "marketPrice": "2500", "expectedPrice": "2500", "fillPrice": "2497.5",
  "feePpm": 1000, "offeredDecimals": 18, "requestedDecimals": 6,
  "asOf": 1791470000
}
```

Prices are per offered token, so they depend on the direction: offering USDT
for ETH on the same market gives `marketPrice` and `expectedPrice` `"0.0004"`
and `fillPrice` `"0.000399600399600399"` (ETH per USDT; invert them for
display). With a band and a maker selling ETH at 2501.63 USDT, the same
request gives `expectedPrice` `"0.000399339630560874"`: 2501.63 USDT plus
the fee per ETH.

| Field | Meaning |
|---|---|
| `marketPrice` | the Binance mid, in whole requested tokens per whole offered token |
| `expectedPrice` | the batch price an order of the pair's reference size gets now, in whole requested tokens per whole offered token, rounded down at 18 decimals: `marketPrice` on a pair without a band ([Pairs with a price band](#pairs-with-a-price-band)) |
| `fillPrice` | the price to build an ask from, per whole offered token: `expectedPrice` after the fee, selling the pair's base token × (1 − fee), buying it ÷ (1 + fee); rounded down at 18 decimals. An ask at it is eligible at the expected price; a full fill pays the batch price less the fee |
| `feePpm` | the clearing fee in parts per million (`1000` = 0.1%; may be `0`) |
| `offeredDecimals`, `requestedDecimals` | the tokens' on-chain decimals |
| `asOf` | unix seconds the solver received the Binance quote |

Errors: see [Status codes](#status-codes). It follows the solver's own
freshness rule, so a missing or stale quote is `503` `no_price`.

### `GET /v2/swap-eta` — will this order fill?

What the solver would do **now** with the order the wallet is about to sign,
by the solver's own rules, plus how long it takes.

| Param | Required | Meaning |
|---|---|---|
| `offered_faucet`, `requested_faucet` | yes | hex faucet ids; the order offers the first and requests the second |
| `offered_amount`, `requested_amount` | yes | the order, in base units |
| `min_fill_step` | no | the note's smallest partial fill, in requested-token base units; `0` or omitted means none, as in PSWAP. If only part of the order can fill and that part is smaller, `fillStatus` is `none` (reason `liquidity`). Send `requested_amount` for all-or-nothing |

**200 response** — offering 1 ETH for 2485.0125 USDT (step 2 with 0.5% slippage), with buyers for 3.2 ETH (8000 USDT) in the book:

```json
{
  "offeredFaucet": "0x…", "requestedFaucet": "0x…",
  "offeredAmount": "1000000000000000000", "requestedAmount": "2485012500",
  "priceBand": "at_market",
  "fillStatus": "full",
  "reason": null,
  "fillableOfferedAmount": "1000000000000000000", "fillableRequestedAmount": "2485012500",
  "maxOfferedAmount": "3200000000000000000",
  "expectedRequestedAmount": "2497500000",
  "feePpm": 1000, "feeAmount": "2500000",
  "marketPrice": "2500", "expectedPrice": "2500", "fillPrice": "2497.5",
  "settlementsRunning": true,
  "estimatedSeconds": 14, "median24hSeconds": 11
}
```

The whole order fills. `fillableRequestedAmount` is at the order's own rate,
the minimum (2485.0125 USDT); a full fill pays `expectedRequestedAmount`
(2497.5 USDT).

Amounts are **base units** (strings); prices are whole requested tokens per
whole offered token.

| Field | Meaning |
|---|---|
| `offeredAmount`, `requestedAmount` | the order, as asked |
| `priceBand` | judged against `expectedPrice`. `at_market`: fills at it · `tolerated`: fills once the price moves up to 0.5% (set per deployment) in the order's favour; its fill fields are judged at the price where it starts to fill · `off_market`: further than that |
| `fillStatus`, `reason` | `full`, `partial` or `none`; for `none`, why: `price`, `liquidity`, `no_market` or `no_price` |
| `fillableOfferedAmount`, `fillableRequestedAmount` | how much of the order fills now, at the order's own rate |
| `maxOfferedAmount` | the most of the offered token an order at this rate fills now, after the orders ahead of it; not capped by `offeredAmount` ("max you can swap now") |
| `expectedRequestedAmount` | what a full fill pays: `expectedPrice` less the fee, never less than `requestedAmount` |
| `feePpm`, `feeAmount` | the clearing fee, and what it takes from that full fill (requested token). It only comes out of the surplus over the ask, so it is smaller, down to `0`, for an order that is not at market |
| `marketPrice` | as on `/v2/pair-price` |
| `expectedPrice`, `fillPrice` | as on `/v2/pair-price`, for this order: the batch price with it in the book |
| `settlementsRunning` | `false` while the solver cannot settle (missing fee funds, node or database down) or has just restarted: orders wait |
| `estimatedSeconds` | next-batch ETA when settlements are running and the order is `at_market` and fills fully or partly; otherwise `null` |
| `median24hSeconds` | the pair's median settlement time over the last 24 h |

Without a price, `priceBand`, `maxOfferedAmount`, `expectedRequestedAmount`,
`feeAmount`, `marketPrice`, `expectedPrice` and `fillPrice` are `null`, and the answer is still
`200` with reason `no_market` or `no_price`. Off market, `maxOfferedAmount` is
`null`. Optional fields are `null`, never omitted.

The verdict is advisory: nothing is reserved, so two wallets can be told
`full` for the same liquidity. It counts the solver's own book only. Not
modelled: external liquidity routing, the per-side order cap of one batch,
and other orders' own minimum fills (an all-or-nothing order counts in full
even when it could not fill against this one); on a pair with a band, also
what [Pairs with a price band](#pairs-with-a-price-band) lists. Errors: see
[Status codes](#status-codes).

### `GET /v1/swap-eta` — the original check (frozen)

Kept unchanged for wallets built against it; new integrations use
`/v2/pair-price` and `/v2/swap-eta`. Same parameters (both amounts
required); it answers:

| Field | Meaning |
|---|---|
| `offeredFaucet`, `requestedFaucet`, `offeredAmount`, `requestedAmount` | the order, as asked |
| `canFill` | the order's rate crosses the **best** opposite level's own rate, and that level holds at least `requestedAmount` |
| `offMarket` | the order asks more than its offer is worth at the Binance mid by more than `swap_offmarket_tolerance_bps`; `null` without a fresh price |
| `estimatedSeconds` | next-batch ETA when `canFill`; otherwise `null` |
| `marketPrice` | the Binance mid; `null` without a fresh price |
| `median24hSeconds` | the pair's median settlement time over the last 24 h |

It looks at the top of the book only and ignores the clearing fee, so it can
differ from what the solver does; `/v2/swap-eta` follows the solver's own
rule. Its `marketPrice` and `offMarket` refer to the Binance mid: on a pair
with a band the batch price can be up to the band away from it.

### Status codes

| Code | When |
|---|---|
| `200` | OK |
| `400` | `bad_faucet_id`: missing or malformed faucet id; `bad_precision`; `batch_too_large`; on the swap endpoints also `bad_request` (the two faucets are equal) and `bad_amount` (missing, zero or non-numeric amount, or too large to price) |
| `404` | `unknown_faucet`: not registered with the solver |
| `408` | the request took longer than `price_query_timeout_ms` (empty body) |
| `503` | `no_market`: no Binance market is configured for the token or pair — nothing to wait for; `no_price`: its market has no valid quote right now (none yet, stale on `/v2/pair-price`, or the newest was crossed, too wide or too thin); `stale` (`/v1/price` only): the quote is at least the quote TTL old (use `?allow_stale=true` to override); `overloaded`: more than `price_query_max_inflight` requests in flight, retry shortly; `not_ready` (`/v2/pairs` only): the solver is still confirming its Binance markets at startup |

Without a price, both swap-eta endpoints still answer `200`: `/v2` with reason
`no_market` or `no_price`, `/v1` with `null` market fields.

Error body: `{"error":"unknown_faucet","message":"…"}` (`408` has none); the
`error` codes above are stable, the `message` is for people. `allow_stale` does
not override `no_price`.

---

## Tokens (devnet)

Query by the **hex** id. `decimals = 8` for all four.

| Token | Price (USDT, mock) | Faucet id (hex — use this) | Faucet id (bech32) |
|---|---|---|---|
| IBTC | 10 | `0xb3722d97036169910fc0eeaccce29b` | `mdev1azehytvhqdsknyg0crh2en8znvp3zmga` |
| IUSDT | 1 | `0x9f0c6ec13c4ed2b1076a2990a9fc29` | `mdev1az0scmkp838d9vg8dg5ep20u9y2s8ymm` |
| IETH | 5 | `0x3ae73d7f166f723132e3acbba75e75` | `mdev1aqaww0tlzehhyvfjuwkthf67w5djl28w` |
| IMIDEN | 2 | `0x2a7afa87c3623a117132a9bca24fea` | `mdev1aq484758cd3r5yt3x25megj0ag46wp8a` |

---

## Examples

```bash
# single
curl http://35.175.40.181:8080/v1/price/0xb3722d97036169910fc0eeaccce29b
# rounded to 2 dp
curl "http://35.175.40.181:8080/v1/price/0xb3722d97036169910fc0eeaccce29b?precision=2"
# batch
curl "http://35.175.40.181:8080/v1/prices?ids=0xb3722d97036169910fc0eeaccce29b,0x3ae73d7f166f723132e3acbba75e75"
```

```js
const PRICE_API = "http://35.175.40.181:8080";

// fetch one token's price record
export async function getPrice(faucetIdHex) {
  const r = await fetch(`${PRICE_API}/v1/price/${faucetIdHex}`);
  if (!r.ok) throw new Error(`price ${r.status}`);
  return r.json(); // { price, decimals, ticker, vs_currency, as_of, stale, ... }
}

// value of a base-unit amount in the quote currency (USDT)
export function quoteValue({ price, decimals }, amountBaseUnits) {
  return (Number(amountBaseUnits) / 10 ** decimals) * Number(price);
}

// example: value of a swap leg
const ibtc = await getPrice("0xb3722d97036169910fc0eeaccce29b");
const usdt = quoteValue(ibtc, 250000000); // 2.5 IBTC -> 25
```

---

## Notes

- **Read-only, public, cached** (`Cache-Control: max-age=1`; both `swap-eta` versions are `no-store`). Concurrency-limited; excess → `503`.
- `price` is per **whole token** (not per base unit) — combine with `decimals` as shown.
- Prices come from Binance Spot `bookTicker` midpoints. On devnet the faucet tokens are priced by a mock Binance server, so the values in the table above are fixed test prices.
- Endpoint accepts the **hex** faucet id today. (Bech32 `mdev…` acceptance can be added on request.)

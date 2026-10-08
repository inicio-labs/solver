# Miden Price API — dApp Integration

Read-only HTTP API. Given a token's **faucet id**, returns its **price in USDT**
(the exact Binance Spot midpoint of its `<ASSET>USDT` market) plus the token's
**on-chain decimals** — enough to render a swap quote unambiguously.

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
| `ticker` | string \| null | on-chain token symbol (e.g. `IBTC`); `null` if unknown |
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

### Swapping: three steps for the wallet

1. **Price.** `GET /v1/pair-price` gives the pair's `fillPrice`: the Binance
   mid less the clearing fee, the best price that fills right now.
2. **Ask.** The wallet builds the order with the user's slippage (BigInt or a
   decimal library, never floats):
   `requested_amount = floor(offered_amount × fillPrice × (1 − slippage) × 10^requestedDecimals / 10^offeredDecimals)`
3. **Verdict.** `GET /v1/swap-eta` with both amounts says whether that exact
   order fills now, partly, or not, and why. If it is off market, go back to
   step 1 for a fresh price.

Slippage here protects the user against the mid moving before the next
batch. It does not buy more liquidity: the solver fills every order at the
mid less the fee, never at a resting order's own price.

### `GET /v1/pair-price` — the price to build an ask from

| Param | Meaning |
|---|---|
| `offered_faucet`, `requested_faucet` | hex faucet ids; the order offers the first and requests the second |

**200 response** — selling ETH (18 decimals) for USDT (6 decimals), mid 2500, fee 0.1%:

```json
{
  "offeredFaucet": "0x…", "requestedFaucet": "0x…",
  "marketPrice": "2500", "fillPrice": "2497.5", "feePpm": 1000,
  "offeredDecimals": 18, "requestedDecimals": 6,
  "asOf": 1791470000
}
```

| Field | Meaning |
|---|---|
| `marketPrice` | the Binance mid of the pair's clearing market, in whole requested tokens per whole offered token |
| `fillPrice` | the mid after the clearing fee, as this side pays it: the best ask that fills now |
| `feePpm` | the clearing fee |
| `offeredDecimals`, `requestedDecimals` | the tokens' on-chain decimals, to turn prices into base units; `null` until fetched |
| `asOf` | unix seconds the solver received this quote |

It follows the solver's own freshness rule: `503` `no_market` when the pair
has no clearing market, `503` `no_price` when its quote is missing, invalid
or stale. `404` unknown faucet, `400` bad or equal faucet ids.

### `GET /v1/swap-eta` — will this order fill?

What the solver would do **now** with the order the wallet is about to
sign: where its price sits, how much of it the live book fills, and how long
it takes. It applies the solver's own rule (fill at the mid less the fee) to
the live book.

| Param | Required | Meaning |
|---|---|---|
| `offered_faucet`, `requested_faucet` | yes | hex faucet ids; the order offers the first and requests the second |
| `offered_amount`, `requested_amount` | yes | the order, in base units |
| `min_fill_step` | no | the note's smallest partial fill. A wallet that does not allow partial fills sends the requested amount; a smaller partial answer becomes `none` |

**200 response** — selling 1 ETH for 2485.0125 USDT (step 2 with 0.5% slippage), with 3.2 ETH of buyers in the book:

```json
{
  "offeredFaucet": "0x…", "requestedFaucet": "0x…",
  "offeredAmount": "1000000000000000000", "requestedAmount": "2485012500",
  "priceBand": "at_market",
  "fillStatus": "full",
  "reason": null,
  "fillableOfferedAmount": "1000000000000000000", "fillableRequestedAmount": "2485012500",
  "availableOfferedAmount": "3200000000000000000",
  "expectedRequestedAmount": "2497500000",
  "feePpm": 1000, "feeAmount": "2500000",
  "marketPrice": "2500", "fillPrice": "2497.5",
  "acceptingOrders": true,
  "canFill": true, "offMarket": false,
  "estimatedSeconds": 14, "median24hSeconds": 11
}
```

Amounts are **base units** (strings); prices are whole requested tokens per
whole offered token. Optional fields are `null`, never omitted.

| Field | Meaning |
|---|---|
| `offeredAmount`, `requestedAmount` | the order, as asked |
| `priceBand` | `at_market`: fills at today's price · `tolerated`: fills once the price moves at most `swap_offmarket_tolerance_bps` (default 0.5%) the order's way · `off_market`: further than that · `null`: no fresh price |
| `fillStatus` | `full`, `partial` or `none` |
| `reason` | only for `none`: `price` (off market), `liquidity` (priced fine, nothing left in the book for it), `no_market` (the pair has no clearing market), `no_price` (no fresh price right now) |
| `fillableOfferedAmount`, `fillableRequestedAmount` | how much of the order the book fills now; a partial fill pays the order's own ratio |
| `availableOfferedAmount` | how much of the offered token the book takes now at this order's price, not capped by the order's size: "max you can swap now". `null` off market or without a price |
| `expectedRequestedAmount` | what a **full** fill pays now: the market value less the fee, never less than `requestedAmount`. Show "you receive ≈ expected, at least requested" |
| `feePpm`, `feeAmount` | the clearing fee, and its amount on a full fill now (requested token) |
| `marketPrice`, `fillPrice` | as in `/v1/pair-price`, for showing "the market is now X" |
| `acceptingOrders` | `false` while the solver cannot settle (it is recovering from missing fee funds, or the node or database being down): orders wait |
| `estimatedSeconds` | next-batch ETA for an `at_market` order that fills fully or partly while `acceptingOrders`; otherwise `null` |
| `median24hSeconds` | the pair's median settlement time over the last 24 h |
| `canFill`, `offMarket` | kept for older wallets: `at_market` and `full`; `priceBand == off_market` |

Suggested wallet copy:

| Answer | Show |
|---|---|
| `at_market` + `full` | no warning |
| `at_market` + `partial` | "Only `fillableOfferedAmount` can fill now; the rest waits" |
| `tolerated` | "May take longer: fills when the price moves slightly" |
| `none` + `price` | "The price moved." Get a fresh `/v1/pair-price` and rebuild the ask |
| `none` + `liquidity` | "Not enough orders at the current price right now" |
| `acceptingOrders: false` | "Settlement delayed" |

The verdict is advisory: nothing is reserved, so two wallets can be told
`full` for the same liquidity. It counts the solver's own book only, not
external liquidity routing, and ignores the per-side order cap of one batch.
Errors: `400` bad or missing amounts or faucet ids, `404` unknown faucet.
Without a price the answer is still `200`, with `reason` `no_market` or
`no_price`.

### Status codes

| Code | When |
|---|---|
| `200` | OK |
| `400` | malformed faucet id, bad `precision`, or too many ids |
| `404` | faucet not registered with the solver |
| `503` | `no_market`: registered but no Binance market is configured for the token — nothing to wait for; `no_price`: its market has no valid quote right now (none yet, or the newest was crossed, too wide or too thin); `stale`: the quote is at least the quote TTL old (use `?allow_stale=true` to override) |

Error body: `{"error":"unknown_faucet","message":"…"}`; the `error` codes above are
stable, the `message` is for people. `allow_stale` does not override `no_price`.

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

- **Read-only, public, cached** (`Cache-Control: max-age=1`). Concurrency-limited; excess → `503`.
- `price` is per **whole token** (not per base unit) — combine with `decimals` as shown.
- Prices come from Binance Spot `bookTicker` midpoints. On devnet the faucet tokens are priced by a mock Binance server, so the values in the table above are fixed test prices.
- Endpoint accepts the **hex** faucet id today. (Bech32 `mdev…` acceptance can be added on request.)

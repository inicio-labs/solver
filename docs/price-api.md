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

### `GET /v1/swap-eta` — swap quote

What the solver would do **now** with an order the wallet is about to sign:
where its price sits, how much of it the live book fills, the price we
suggest, and how long it takes. The solver fills every order at the Binance
mid less the clearing fee; this endpoint applies that same rule to the live
book.

| Param | Required | Meaning |
|---|---|---|
| `offered_faucet`, `requested_faucet` | yes | hex faucet ids; the order offers the first and requests the second |
| `offered_amount` | one of the two | base units the order offers |
| `requested_amount` | one of the two | base units the order requests. Leave one amount out and the solver fills it in at the suggested price ("sell 1 ETH" → how much USDT to ask) |
| `min_fill_step` | no | the note's smallest partial fill; a smaller partial answer becomes `none` |

**200 response** — selling 1 ETH (18 decimals) for USDT (6 decimals), mid 2500, fee 0.1%, `requested_amount` left out:

```json
{
  "offeredFaucet": "0x…", "requestedFaucet": "0x…",
  "offeredAmount": "1000000000000000000", "requestedAmount": "2492505000",
  "priceBand": "at_market",
  "fillStatus": "full",
  "reason": null,
  "fillableOfferedAmount": "1000000000000000000", "fillableRequestedAmount": "2492505000",
  "expectedRequestedAmount": "2497500000",
  "feePpm": 1000, "feeAmount": "2500000",
  "marketPrice": "2500", "fillPrice": "2497.5",
  "suggestedPrice": "2492.505", "suggestedRequestedAmount": "2492505000",
  "acceptingOrders": true,
  "canFill": true, "offMarket": false,
  "estimatedSeconds": 14, "median24hSeconds": 11
}
```

Amounts are **base units** (strings); prices are whole requested tokens per
whole offered token. Optional fields are `null`, never omitted.

| Field | Meaning |
|---|---|
| `offeredAmount`, `requestedAmount` | the order, with a left-out amount filled in |
| `priceBand` | `at_market`: fills at today's price · `tolerated`: fills once the price moves at most `swap_offmarket_tolerance_bps` (default 0.5%) the order's way · `off_market`: further than that · `null`: no fresh price |
| `fillStatus` | `full`, `partial` or `none` |
| `reason` | only for `none`: `price` (off market), `liquidity` (priced fine, nothing left in the book for it), `no_market` (the pair has no clearing market), `no_price` (no fresh price right now) |
| `fillableOfferedAmount`, `fillableRequestedAmount` | how much of the order the book fills now; a partial fill pays the order's own ratio |
| `expectedRequestedAmount` | what a **full** fill pays now: the market value less the fee, never less than `requestedAmount`. Show "you receive ≈ expected, at least requested" |
| `feePpm`, `feeAmount` | the clearing fee, and its amount on a full fill now (requested token) |
| `marketPrice` | the Binance mid |
| `fillPrice` | the mid after the fee: the best price that fills now |
| `suggestedPrice`, `suggestedRequestedAmount` | the fill price less `swap_suggest_buffer_bps` (default 0.2%), and the amount to request at it, so the order still fills after a small move. Use it as is; do not apply slippage on top |
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
| `none` + `price` | "Price too far from market. Use `suggestedPrice`?" |
| `none` + `liquidity` | "Not enough liquidity right now" |
| `acceptingOrders: false` | "Settlement delayed" |

The quote is advisory: nothing is reserved, so two wallets can be told
`full` for the same liquidity. It counts the solver's own book only, not
external liquidity routing, and ignores the per-side order cap of one batch.
Errors: `400` bad or missing amounts, or an amount too small to price;
`404` unknown faucet; `503` `no_market` / `no_price` when an amount is left
out and there is no price to fill it in.

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

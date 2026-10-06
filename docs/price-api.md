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
| `allow_stale` | `true` | return `200` + `"stale":true` instead of `503` when the token's quote is stale. |

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
| `as_of` | number | unix epoch seconds the solver received this quote |
| `stale` | bool | `true` if the quote is at least the solver's quote TTL old |
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

### Status codes

| Code | When |
|---|---|
| `200` | OK |
| `400` | malformed faucet id, bad `precision`, or too many ids |
| `404` | faucet not registered with the solver |
| `503` | registered but no price (no configured Binance market, or no valid quote yet), or price stale (use `?allow_stale=true` to override) |

Error body: `{"error":"unknown_faucet","message":"…"}`.

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

// USDT value of a base-unit amount
export function usdValue({ price, decimals }, amountBaseUnits) {
  return (Number(amountBaseUnits) / 10 ** decimals) * Number(price);
}

// example: value of a swap leg
const ibtc = await getPrice("0xb3722d97036169910fc0eeaccce29b");
const usd  = usdValue(ibtc, 250000000); // 2.5 IBTC -> 25
```

---

## Notes

- **Read-only, public, cached** (`Cache-Control: max-age=1`). Concurrency-limited; excess → `503`.
- `price` is per **whole token** (not per base unit) — combine with `decimals` as shown.
- Prices come from Binance Spot `bookTicker` midpoints. On devnet the faucet tokens are priced by a mock Binance server, so the values below are fixed test prices.
- Endpoint accepts the **hex** faucet id today. (Bech32 `mdev…` acceptance can be added on request.)

# Miden Price API — dApp Integration

Read-only HTTP API. Given a token's **faucet id**, returns its **USD price** plus
the token's **on-chain decimals** — enough to render a swap quote unambiguously.

## Base URL

```
http://35.175.40.181:8080        # devnet, current (HTTP)
```

> HTTPS (`https://<subdomain>`) is being set up — switch the base URL to it once
> live. Browsers block `http://` calls from an `https://` page (mixed content),
> so for an HTTPS frontend use the HTTPS base URL when ready.

**CORS:** enabled (any origin, `GET`) — browser `fetch()` / extensions work.

---

## Endpoints

### `GET /v1/price/{faucet_id}`

One token's price. `faucet_id` is the **hex** faucet account id (`0x…`).

Query params (optional):

| Param | Values | Meaning |
|---|---|---|
| `precision` | `full` (default) \| `0`–`18` | decimal places of the **price number**. `full` = exact. |
| `allow_stale` | `true` | return `200` + `"stale":true` instead of `503` when the feed is stale. |

**200 response:**

```json
{
  "faucet_id": "0xb3722d97036169910fc0eeaccce29b",
  "ticker": "IBTC",
  "vs_currency": "usd",
  "price": "10",
  "precision": "full",
  "decimals": 8,
  "as_of": 1781906150,
  "stale": false,
  "source": "coingecko"
}
```

| Field | Type | Meaning |
|---|---|---|
| `faucet_id` | string | canonical hex id of the token's faucet |
| `ticker` | string \| null | on-chain token symbol (e.g. `IBTC`); `null` if unknown |
| `vs_currency` | string | quote currency (`usd`) |
| `price` | **string** | USD price of **ONE WHOLE token**. String to avoid float rounding. |
| `precision` | string | precision applied to `price` (`full` or `0`–`18`) |
| `decimals` | number \| null | the token's **on-chain decimals** (here: `8`) |
| `as_of` | number | unix epoch seconds the price was last refreshed |
| `stale` | bool | `true` if the feed is older than the staleness window |
| `source` | string | price source label |

> **Valuing an amount.** On-chain amounts are in **base units**.
> `usd = (amount / 10^decimals) * price`.
> e.g. `250000000` base units of IBTC → `250000000 / 10^8 = 2.5` IBTC → `2.5 × $10 = $25`.

### `GET /v1/prices?ids=a,b,c`

Batch. Returns an object keyed by `faucet_id`; unknown/unpriced ids are **omitted**
(CoinGecko-style). Max 50 ids.

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
| `503` | registered but no price yet, or price stale (use `?allow_stale=true` to override) |

Error body: `{"error":"unknown_faucet","message":"…"}`.

---

## Tokens (devnet)

Query by the **hex** id. `decimals = 8` for all four.

| Token | Price | Faucet id (hex — use this) | Faucet id (bech32) |
|---|---|---|---|
| IBTC | $10 | `0xb3722d97036169910fc0eeaccce29b` | `mdev1azehytvhqdsknyg0crh2en8znvp3zmga` |
| IUSDT | $1 | `0x9f0c6ec13c4ed2b1076a2990a9fc29` | `mdev1az0scmkp838d9vg8dg5ep20u9y2s8ymm` |
| IETH | $5 | `0x3ae73d7f166f723132e3acbba75e75` | `mdev1aqaww0tlzehhyvfjuwkthf67w5djl28w` |
| IMIDEN | $2 | `0x2a7afa87c3623a117132a9bca24fea` | `mdev1aq484758cd3r5yt3x25megj0ag46wp8a` |

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

// USD value of a base-unit amount
export function usdValue({ price, decimals }, amountBaseUnits) {
  return (Number(amountBaseUnits) / 10 ** decimals) * Number(price);
}

// example: value of a swap leg
const ibtc = await getPrice("0xb3722d97036169910fc0eeaccce29b");
const usd  = usdValue(ibtc, 250000000); // 2.5 IBTC -> 25
```

---

## Notes

- **Read-only, public, cached** (`Cache-Control` ~5s). Concurrency-limited; excess → `503`.
- `price` is per **whole token** (not per base unit) — combine with `decimals` as shown.
- Prices are devnet mock values right now; a production deployment reads CoinGecko.
- Endpoint accepts the **hex** faucet id today. (Bech32 `mdev…` acceptance can be added on request.)

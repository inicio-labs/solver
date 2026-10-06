-- Token prices come only from Binance markets configured in solver.toml
-- (ADR 0004); the per-token CoinGecko id is no longer used.
ALTER TABLE registered_tokens DROP COLUMN external_symbol;

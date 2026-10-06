-- Restores the column empty; the CoinGecko ids it held are not recovered.
ALTER TABLE registered_tokens ADD COLUMN external_symbol TEXT;

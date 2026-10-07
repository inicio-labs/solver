-- Token prices come only from the Binance markets configured in solver.toml
-- (ADR 0004); the per-token external symbol is not used.
ALTER TABLE registered_tokens DROP COLUMN external_symbol;

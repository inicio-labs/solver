-- Reverting the baseline removes the whole application database. Only run
-- this on a deployment that has never settled an order, or after restoring
-- the Miden client stores to a matching checkpoint (see docs/postgres-runbook.md).
DROP TABLE IF EXISTS settlement_inputs;
DROP TABLE IF EXISTS settlement_attempts;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS registered_tokens;
DROP TABLE IF EXISTS sync_state;

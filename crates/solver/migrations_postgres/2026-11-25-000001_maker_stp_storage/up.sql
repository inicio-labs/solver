-- Self-trade prevention at submit (ADR 0009), storage only: nothing reads
-- these until the check is switched on. Diesel runs each migration in one
-- transaction, so this lock timeout ends with it.
SET LOCAL lock_timeout = '30s';
-- The backfill and index builds scan whole tables: a timeout set on the
-- role or database must not cancel them (the solver is stopped).
SET LOCAL statement_timeout = 0;

-- The prices the maker submitted, written once by the claim. Two orders of
-- one maker on one pair, in opposite directions, cross when offered_a *
-- offered_b >= requested_a * requested_b. Lineages claimed before this migration keep NULL and never
-- cross.
ALTER TABLE maker_lineages
    ADD COLUMN offered_amount BIGINT CHECK (offered_amount > 0),
    ADD COLUMN requested_amount BIGINT CHECK (requested_amount > 0),
    ADD CONSTRAINT maker_lineages_amounts_together
        CHECK ((offered_amount IS NULL) = (requested_amount IS NULL));
-- Every lineage of a maker on a pair after a given submit sequence, in any
-- state: the newer orders a late submit is compared with.
CREATE INDEX maker_lineages_seq_idx ON maker_lineages (maker_id, market, root_seq);
-- Accepted submits whose order row does not exist yet.
CREATE INDEX maker_lineages_pending_group_idx ON maker_lineages (maker_id, market)
    WHERE state = 'pending' AND accept_block IS NULL;
-- Every claim leaves the pending indexes once (activated, rejected, or
-- flagged at accept, which sets `accept_block`), a dead entry in the indexes
-- the check reads each round;
-- and the table keeps every claim, so the default trigger (20% dead) comes
-- later and later. Vacuum at 1% + 1000 dead rows, with the index pass, as
-- `orders` does, and log each run.
ALTER TABLE maker_lineages SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    vacuum_index_cleanup = on,
    log_autovacuum_min_duration = 0
);

-- The maker of a maker order, copied from its claim on every insert, so the
-- maker's open orders on a pair are found by index. A maker order is a
-- private note and its claim is always written first; a public order has no
-- claim and keeps NULL.
ALTER TABLE orders ADD COLUMN maker_id BIGINT;
CREATE FUNCTION orders_fill_maker_id() RETURNS trigger
    LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    -- The claim is the only source: NULL without one.
    NEW.maker_id := (SELECT maker_id FROM maker_lineages WHERE lineage_id = NEW.lineage_id);
    RETURN NEW;
END $$;
CREATE TRIGGER orders_fill_maker_id BEFORE INSERT ON orders
    FOR EACH ROW EXECUTE FUNCTION orders_fill_maker_id();
-- Rows stored before the trigger: only the open ones are ever read by maker.
UPDATE orders o SET maker_id = l.maker_id
FROM maker_lineages l
WHERE l.lineage_id = o.lineage_id
  AND o.status IN ('active', 'settling', 'waiting');
CREATE INDEX orders_maker_open_idx ON orders (maker_id, market)
    WHERE status IN ('active', 'settling', 'waiting') AND maker_id IS NOT NULL;

-- Who stopped a lineage, and the newer order that did when it was a
-- self-trade. NULL for stops written before this migration.
ALTER TABLE maker_stops
    ADD COLUMN source TEXT
        CHECK (source IN ('maker', 'cap', 'escape_hatch', 'stp_reservation', 'stp_on_submit')),
    ADD COLUMN caused_by_lineage BYTEA,
    ADD CONSTRAINT maker_stops_cause_is_self_trade
        CHECK (caused_by_lineage IS NULL OR source IS NOT DISTINCT FROM 'stp_on_submit');

-- Every claim the maker's cutoffs and stops admit, whatever its state: the
-- same rule as `eligible_orders`, for lineages. History stays: no code
-- deletes a `maker_lineages` row.
CREATE VIEW eligible_lineages AS
SELECT l.lineage_id, l.maker_id, l.request_id, l.root_seq, l.note_id, l.state,
       l.market, l.direction, l.expires_at_unix_ms, l.accept_block,
       l.offered_amount, l.requested_amount
FROM maker_lineages l
WHERE NOT EXISTS (
      SELECT 1 FROM maker_cutoffs c
      WHERE c.maker_id = l.maker_id
        AND l.root_seq < c.cutoff
        AND (c.market = '' OR c.market = l.market)
        AND (c.direction = '' OR c.direction = l.direction))
  AND NOT EXISTS (
      SELECT 1 FROM maker_stops s
      WHERE s.maker_id = l.maker_id AND s.lineage_id = l.lineage_id);

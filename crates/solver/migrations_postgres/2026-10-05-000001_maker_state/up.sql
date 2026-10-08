-- GetMakerState (ADR 0003) pages through one maker's lineages, and checks a
-- pending submission against market- and direction-scoped cutoffs before its
-- order row exists. Claims written before this migration have no keys; they
-- match only cutoffs that cover every market.
ALTER TABLE maker_lineages
    ADD COLUMN market BYTEA,
    ADD COLUMN direction BYTEA;
CREATE INDEX maker_lineages_maker_idx ON maker_lineages (maker_id, lineage_id);

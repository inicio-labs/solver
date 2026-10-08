DROP INDEX maker_lineages_maker_idx;
ALTER TABLE maker_lineages
    DROP COLUMN direction,
    DROP COLUMN market;

-- The requested-asset amount each settlement input is filled with (the
-- payback amount). With the stored parent and remainder notes it gives every
-- fill detail a maker needs to rebuild its payback and remainder notes.
-- Attempts prepared before this migration have none and are never reported.
ALTER TABLE settlement_inputs
    ADD COLUMN fill_amount BIGINT CHECK (fill_amount > 0);

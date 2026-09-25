DROP TRIGGER orders_assign_priority_seq;
DROP INDEX orders_priority_seq_unique;
DROP TABLE order_priority_counter;
ALTER TABLE orders DROP COLUMN priority_seq;

-- Fetches by TID in the order of the TIDs -- AFTER triggers', a foreign
-- key's check of each row a statement wrote, a TID scan's -- served from
-- windows of rows taken ahead of them (docs/design.md §2.8)
SHOW lance.fetch_ahead;
CREATE TABLE fk_parent (id int PRIMARY KEY);
INSERT INTO fk_parent SELECT generate_series(1, 1000);
CREATE TABLE fk_child (id int NOT NULL, parent int REFERENCES fk_parent, note text) USING lance;

-- a statement's rows, each checked, through windows of 16 to 1024 rows
INSERT INTO fk_child SELECT g, g % 1000 + 1, 'n' || g FROM generate_series(1, 5000) g;
SELECT count(*), sum(parent) FROM fk_child;
-- one row among them without its parent: the statement fails
INSERT INTO fk_child
SELECT g, CASE WHEN g = 7777 THEN 5000 ELSE g % 1000 + 1 END, NULL FROM generate_series(5001, 10000) g;
SELECT count(*) FROM fk_child;

-- a deferred key: the rows deleted before the commit are not checked
ALTER TABLE fk_child ALTER CONSTRAINT fk_child_parent_fkey DEFERRABLE INITIALLY DEFERRED;
BEGIN;
INSERT INTO fk_child
SELECT g, CASE WHEN g % 500 = 0 THEN 5000 ELSE g % 1000 + 1 END, NULL FROM generate_series(10001, 15000) g;
DELETE FROM fk_child WHERE parent = 5000;
COMMIT;
SELECT count(*) FROM fk_child;
-- the rows given their parent before the commit pass: updated in place,
-- and given new TIDs
BEGIN;
INSERT INTO fk_child
SELECT g, CASE WHEN g % 500 = 0 THEN 5000 ELSE g % 1000 + 1 END, NULL FROM generate_series(15001, 20000) g;
UPDATE fk_child SET parent = 1 WHERE parent = 5000;
COMMIT;
SET lance.update_in_place = off;
BEGIN;
INSERT INTO fk_child
SELECT g, CASE WHEN g % 500 = 0 THEN 5000 ELSE g % 1000 + 1 END, NULL FROM generate_series(20001, 25000) g;
UPDATE fk_child SET parent = 2 WHERE parent = 5000;
COMMIT;
RESET lance.update_in_place;
SELECT count(*), count(*) FILTER (WHERE id > 15000 AND parent IN (1, 2) AND id % 500 = 0) FROM fk_child;
-- a row left without its parent: the commit fails
BEGIN;
INSERT INTO fk_child
SELECT g, CASE WHEN g = 25555 THEN 5000 ELSE g % 1000 + 1 END, NULL FROM generate_series(25001, 30000) g;
COMMIT;
-- a savepoint's rows rolled back go unchecked
BEGIN;
INSERT INTO fk_child SELECT g, g % 1000 + 1, NULL FROM generate_series(30001, 32000) g;
SAVEPOINT s;
INSERT INTO fk_child SELECT g, 5000, NULL FROM generate_series(32001, 33000) g;
ROLLBACK TO SAVEPOINT s;
INSERT INTO fk_child SELECT g, g % 1000 + 1, NULL FROM generate_series(33001, 34000) g;
COMMIT;
SELECT count(*), max(id) FROM fk_child;

-- a trigger that deletes the next row before the key's check of it: the
-- check, after the change, finds the row gone, as heap's does
CREATE TABLE fk_next (id int NOT NULL, parent int REFERENCES fk_parent) USING lance;
CREATE FUNCTION fk_next_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM fk_next WHERE id = NEW.id + 1 AND parent = 5000;
    RETURN NULL;
END$$;
CREATE TRIGGER fk_next_after AFTER INSERT ON fk_next FOR EACH ROW EXECUTE FUNCTION fk_next_fn();
INSERT INTO fk_next SELECT g, CASE WHEN g % 100 = 0 THEN 5000 ELSE 1 END FROM generate_series(1, 300) g;
SELECT count(*), count(*) FILTER (WHERE parent = 5000) FROM fk_next;
DROP TABLE fk_next;
DROP FUNCTION fk_next_fn();

-- a TID scan, the transaction's deletions left out, with and without windows
CREATE TEMP TABLE child_tids AS SELECT array_agg(ctid ORDER BY ctid) AS a FROM fk_child;
BEGIN;
DELETE FROM fk_child WHERE id % 7 = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM fk_child WHERE ctid = ANY ((SELECT a FROM child_tids)::tid[]);
SELECT count(*), sum(id), count(note) FROM fk_child WHERE ctid = ANY ((SELECT a FROM child_tids)::tid[]);
SELECT count(*), sum(id), count(note) FROM fk_child;
SELECT count(*), sum(id), count(note) FROM fk_child WHERE ctid = ANY ((SELECT a[1:1000] FROM child_tids)::tid[]);
SET LOCAL lance.fetch_ahead = 0;
SELECT count(*), sum(id), count(note) FROM fk_child WHERE ctid = ANY ((SELECT a[1:1000] FROM child_tids)::tid[]);
ROLLBACK;
DROP TABLE fk_child, fk_parent;

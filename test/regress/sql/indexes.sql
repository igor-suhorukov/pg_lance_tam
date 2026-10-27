-- PostgreSQL's own indexes over a lance table: their entries are the rows'
-- TIDs, which are the rows' stable row ids (docs/design.md §2.6)
CREATE TABLE people (id int NOT NULL, name text, age int) USING lance;
INSERT INTO people SELECT g, 'p' || g, g % 90 FROM generate_series(1, 5000) g;
CREATE INDEX people_age ON people (age);
CREATE UNIQUE INDEX people_id ON people (id);
ANALYZE people;

SET enable_seqscan = off;
SET lance.enable_customscan = off;
EXPLAIN (COSTS OFF) SELECT name FROM people WHERE id = 4321;
SELECT name FROM people WHERE id = 4321;
EXPLAIN (COSTS OFF) SELECT count(*) FROM people WHERE age = 33;
SELECT count(*) FROM people WHERE age = 33;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM people WHERE age BETWEEN 10 AND 12;
SELECT count(*) FROM people WHERE age BETWEEN 10 AND 12;
RESET enable_indexscan;

-- writes keep the indexes; a deleted row's entry finds nothing
UPDATE people SET age = 200 WHERE id = 10;
SELECT id, age FROM people WHERE age = 200;
DELETE FROM people WHERE id = 11;
SELECT count(*) FROM people WHERE id = 11;
INSERT INTO people VALUES (6000, 'late', 201);
SELECT id FROM people WHERE age = 201;

-- the unique index refuses a duplicate, in a statement and across them
INSERT INTO people VALUES (6000, 'dup', 1);
INSERT INTO people VALUES (7000, 'a', 1), (7000, 'b', 1);
BEGIN;
INSERT INTO people VALUES (8000, 'x', 1);
INSERT INTO people VALUES (8000, 'y', 1);
ROLLBACK;
SELECT count(*) FROM people WHERE id IN (7000, 8000);

-- ON CONFLICT, through the unique index
INSERT INTO people VALUES (6000, 'conflict', 1) ON CONFLICT (id) DO NOTHING;
INSERT INTO people VALUES (6000, 'upserted', 5) ON CONFLICT (id) DO UPDATE SET name = excluded.name;
SELECT id, name, age FROM people WHERE id = 6000;
-- ... and a command that would change a row it inserted itself
INSERT INTO people VALUES (9000, 'a', 1), (9000, 'b', 2) ON CONFLICT (id) DO UPDATE SET name = excluded.name;
RESET enable_seqscan;
RESET lance.enable_customscan;

-- CREATE INDEX CONCURRENTLY and REINDEX CONCURRENTLY: the index built in
-- one transaction and validated against the table in another
CREATE INDEX CONCURRENTLY people_name ON people (name);
SELECT indisvalid FROM pg_index WHERE indexrelid = 'people_name'::regclass;
REINDEX INDEX CONCURRENTLY people_name;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT id FROM people WHERE name = 'p77';
RESET enable_seqscan;
RESET lance.enable_customscan;
DROP INDEX CONCURRENTLY people_name;
CREATE INDEX CONCURRENTLY people_lance ON people USING lance_index (age);

-- VACUUM compacts the dataset; the row ids, and so the indexes, stay
SELECT (lance.table_info('people')->>'fragments')::int > 1 AS many_fragments;
VACUUM people;
SELECT (lance.table_info('people')->>'fragments')::int AS fragments;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT name FROM people WHERE id = 4321;
SELECT id, name FROM people WHERE id = 6000;
RESET enable_seqscan;
RESET lance.enable_customscan;
REINDEX TABLE people;
SELECT count(*) FROM people;

-- foreign keys, both ways, between lance tables and heap
CREATE TABLE teams (id int PRIMARY KEY, name text) USING lance;
CREATE TABLE members (id int, team int REFERENCES teams (id) ON DELETE CASCADE, name text) USING lance;
INSERT INTO teams VALUES (1, 'one'), (2, 'two');
INSERT INTO members VALUES (10, 1, 'x'), (11, 2, 'y');
INSERT INTO members VALUES (12, 3, 'z');
BEGIN;
INSERT INTO teams VALUES (3, 'three');
INSERT INTO members VALUES (12, 3, 'z');
UPDATE members SET team = 1 WHERE id = 12;
COMMIT;
DELETE FROM teams WHERE id = 2;
SELECT id, team FROM members ORDER BY id;
UPDATE teams SET id = 5 WHERE id = 1;
CREATE TABLE heap_ref (team int REFERENCES teams (id));
INSERT INTO heap_ref VALUES (1);
INSERT INTO heap_ref VALUES (4);
DROP TABLE heap_ref, members, teams;

-- MERGE
CREATE TABLE stock (item int PRIMARY KEY, qty int) USING lance;
INSERT INTO stock VALUES (1, 10), (2, 20), (3, 30);
CREATE TEMP TABLE delivery (item int, qty int);
INSERT INTO delivery VALUES (1, 5), (3, -30), (4, 7);
MERGE INTO stock s USING delivery d ON s.item = d.item
WHEN MATCHED AND s.qty + d.qty <= 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET qty = s.qty + d.qty
WHEN NOT MATCHED THEN INSERT VALUES (d.item, d.qty);
SELECT * FROM stock ORDER BY item;
DROP TABLE stock;
DROP TABLE people;

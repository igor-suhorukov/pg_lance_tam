-- ALTER TABLE ... ADD COLUMN: a column of the dataset's, each row it has
-- taking the default PostgreSQL gives the rows that predate the column, or
-- NULL (docs/design.md §2.15)
CREATE TABLE evo (id int NOT NULL, label varchar(10)) USING lance;
INSERT INTO evo SELECT g, 'r' || g FROM generate_series(1, 2000) g;
DELETE FROM evo WHERE id % 10 = 0;
ALTER TABLE evo ADD COLUMN n int;
ALTER TABLE evo ADD COLUMN d int DEFAULT 7, ADD COLUMN t text DEFAULT 'x',
    ADD COLUMN v vector(3) DEFAULT '[1,2,3]';
SELECT count(*), count(n), sum(d), min(t), max(t), count(v) FROM evo;
SELECT id, n, d, t, v FROM evo WHERE id = 1;
INSERT INTO evo (id, label, n, d, t, v) VALUES (5000, 'new', 1, NULL, NULL, '[4,5,6]');
UPDATE evo SET n = id WHERE id <= 3;
SELECT id, label, n, d, t, v FROM evo WHERE id <= 3 OR id = 5000 ORDER BY id;
-- the new columns are Lance's: a filter on one is Lance's to apply
EXPLAIN (COSTS OFF) SELECT id FROM evo WHERE d = 7 AND n = 2;
SELECT id FROM evo WHERE d = 7 AND n = 2;
-- a vector column added is searched as any other
SELECT id FROM evo ORDER BY v <-> '[4,5,6]' LIMIT 1;

-- constraints over a new column see the rows' value
ALTER TABLE evo ADD COLUMN c int DEFAULT 5 CHECK (c < 3);
ALTER TABLE evo ADD COLUMN nn int NOT NULL;
ALTER TABLE evo ADD COLUMN nd int NOT NULL DEFAULT 0;
SELECT count(*), sum(nd) FROM evo;
-- a volatile default would rewrite the table
ALTER TABLE evo ADD COLUMN r float8 DEFAULT random();

-- a dropped column's name, given again: a column of its own
ALTER TABLE evo DROP COLUMN n;
ALTER TABLE evo ADD COLUMN n text DEFAULT 'again';
SELECT id, n FROM evo WHERE id IN (1, 5000) ORDER BY id;

-- in a transaction: after its writes, which it commits first; and undone
-- by its rollback
BEGIN;
INSERT INTO evo (id, label) VALUES (6000, 'tx');
ALTER TABLE evo ADD COLUMN late int DEFAULT 1;
INSERT INTO evo (id, label, late) VALUES (6001, 'tx2', 2);
SELECT id, late FROM evo WHERE id >= 6000 ORDER BY id;
ROLLBACK;
SELECT count(*) FROM evo WHERE id >= 6000;
BEGIN;
ALTER TABLE evo ADD COLUMN late int DEFAULT 1;
INSERT INTO evo (id, label, late) VALUES (6001, 'tx2', 2);
COMMIT;
SELECT id, late FROM evo WHERE id IN (1, 6001) ORDER BY id;
-- a savepoint rolled back takes its column along
BEGIN;
SAVEPOINT s;
ALTER TABLE evo ADD COLUMN gone int DEFAULT 3;
ROLLBACK TO s;
INSERT INTO evo (id, label) VALUES (7001, 'after');
COMMIT;
SELECT id, label, late FROM evo WHERE id = 7001;
-- ... and in one after the transaction's writes it is refused
BEGIN;
INSERT INTO evo (id, label) VALUES (7002, 'sp');
SAVEPOINT s;
ALTER TABLE evo ADD COLUMN sp int;
ROLLBACK;
SELECT count(*) FROM evo WHERE id = 7002;

-- the table's PostgreSQL indexes over the new columns, and VACUUM's
-- compaction with them
CREATE INDEX evo_d ON evo (d);
VACUUM evo;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT count(*) FROM evo WHERE d = 7;
RESET enable_seqscan;
RESET lance.enable_customscan;

-- ALTER COLUMN ... TYPE: the changes PostgreSQL makes in its catalog alone,
-- when the dataset's column keeps its Arrow type
ALTER TABLE evo ALTER COLUMN label TYPE varchar(30);
ALTER TABLE evo ALTER COLUMN label TYPE text;
INSERT INTO evo (id, label) VALUES (8000, 'a label longer than ten');
SELECT label FROM evo WHERE id = 8000;
ALTER TABLE evo ALTER COLUMN d TYPE bigint;
ALTER TABLE evo ALTER COLUMN t TYPE varchar(1);
SELECT count(*) FROM evo;
DROP TABLE evo;

-- DROP COLUMN drops the dataset's column too: a NOT NULL one needs no
-- value after it, and its name is free again
CREATE TABLE dropme (a int NOT NULL, b int NOT NULL, c text) USING lance;
INSERT INTO dropme VALUES (1, 2, 'x');
ALTER TABLE dropme DROP COLUMN b;
INSERT INTO dropme VALUES (3, 'y');
SELECT * FROM dropme ORDER BY a;
ALTER TABLE dropme ADD COLUMN b text DEFAULT 'bee';
SELECT * FROM dropme ORDER BY a;
ALTER TABLE dropme DROP COLUMN a, DROP COLUMN c, DROP COLUMN b;
DROP TABLE dropme;

-- NOT NULL is PostgreSQL's: dropped, the column takes NULLs
CREATE TABLE nn (a int NOT NULL, b text) USING lance;
INSERT INTO nn VALUES (1, 'x');
INSERT INTO nn VALUES (NULL, 'y');
ALTER TABLE nn ALTER COLUMN a DROP NOT NULL;
INSERT INTO nn VALUES (NULL, 'y');
SELECT a, b FROM nn ORDER BY b;
DROP TABLE nn;

-- a virtual generated column is PostgreSQL's to compute: no column of the
-- dataset's; a stored one is the dataset's
CREATE TABLE gen (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL NOT NULL,
                  s int GENERATED ALWAYS AS (a + 1) STORED) USING lance;
INSERT INTO gen (a) VALUES (1), (2);
INSERT INTO gen (a) VALUES (NULL);
ALTER TABLE gen ADD COLUMN c int GENERATED ALWAYS AS (a * 10) VIRTUAL;
SELECT a, b, s, c FROM gen ORDER BY a;
DROP TABLE gen;

-- a materialized view cannot be lance's, even as the default: REFRESH
-- rewrites it
SET default_table_access_method = lance;
CREATE MATERIALIZED VIEW mv AS SELECT 1 AS one;
CREATE MATERIALIZED VIEW mv2 USING lance AS SELECT 1 AS one;
CREATE MATERIALIZED VIEW mv USING heap AS SELECT 1 AS one;
REFRESH MATERIALIZED VIEW mv;
SELECT one FROM mv;
DROP MATERIALIZED VIEW mv;
RESET default_table_access_method;

-- a table made in the transaction that adds to it
BEGIN;
CREATE TABLE fresh (a int) USING lance;
INSERT INTO fresh VALUES (1), (2);
ALTER TABLE fresh ADD COLUMN b text DEFAULT 'b';
INSERT INTO fresh VALUES (3, 'c');
COMMIT;
SELECT * FROM fresh ORDER BY a;
DROP TABLE fresh;

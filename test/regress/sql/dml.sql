-- Writes and a transaction's own view of them (docs/design.md §2.7)
CREATE TABLE acct (id int NOT NULL, owner text, balance numeric(12, 2)) USING lance;
INSERT INTO acct SELECT g, 'u' || g, g * 10 FROM generate_series(1, 1000) g;
SELECT count(*), sum(balance) FROM acct;

-- a transaction's later commands see its earlier ones
BEGIN;
UPDATE acct SET balance = balance + 1 WHERE id <= 10;
SELECT sum(balance) FROM acct WHERE id <= 10;
DELETE FROM acct WHERE id BETWEEN 11 AND 20;
SELECT count(*) FROM acct;
INSERT INTO acct VALUES (2000, 'new', 0);
SELECT id, owner FROM acct WHERE id = 2000;
UPDATE acct SET owner = 'newer' WHERE id = 2000;
SELECT id, owner FROM acct WHERE id >= 2000;
DELETE FROM acct WHERE id = 2000;
SELECT count(*) FROM acct WHERE id >= 2000;
COMMIT;
SELECT count(*), sum(balance) FROM acct;

-- savepoints take back what came after them
BEGIN;
INSERT INTO acct VALUES (3000, 'kept', 1);
SAVEPOINT s1;
INSERT INTO acct VALUES (3001, 'gone', 1);
DELETE FROM acct WHERE id = 1;
SELECT count(*) FROM acct WHERE id IN (1, 3000, 3001);
ROLLBACK TO SAVEPOINT s1;
SELECT count(*) FROM acct WHERE id IN (1, 3000, 3001);
SAVEPOINT s2;
UPDATE acct SET owner = 'sub' WHERE id = 2;
RELEASE SAVEPOINT s2;
COMMIT;
SELECT id, owner FROM acct WHERE id IN (1, 2, 3000, 3001) ORDER BY id;

-- RETURNING, and an error rolling a statement back
UPDATE acct SET balance = 0 WHERE id = 3 RETURNING id, balance;
DELETE FROM acct WHERE id = 4 RETURNING id, owner;
INSERT INTO acct VALUES (5000, 'r', 5) RETURNING *;
BEGIN;
INSERT INTO acct VALUES (6000, 'x', 1);
INSERT INTO acct VALUES (NULL, 'y', 1);
ROLLBACK;
SELECT count(*) FROM acct WHERE id = 6000;

-- one command reading the table it writes sees the table as it was
INSERT INTO acct SELECT id + 10000, owner, balance FROM acct WHERE id <= 5;
SELECT count(*) FROM acct WHERE id > 10000;
UPDATE acct SET id = id + 1 WHERE id > 10000;
SELECT array_agg(id ORDER BY id) FROM acct WHERE id > 10000;

-- UPDATE ... FROM a join naming a row twice changes it once
CREATE TEMP TABLE twice (k int);
INSERT INTO twice VALUES (7), (7);
UPDATE acct SET balance = balance + 100 FROM twice WHERE acct.id = twice.k;
SELECT id, balance FROM acct WHERE id = 7;

-- TRUNCATE, and rows after it in the same transaction
BEGIN;
TRUNCATE acct;
SELECT count(*) FROM acct;
INSERT INTO acct VALUES (1, 'after', 1);
SELECT * FROM acct;
ROLLBACK;
SELECT count(*) FROM acct;
TRUNCATE acct;
SELECT count(*) FROM acct;
INSERT INTO acct VALUES (1, 'after', 1);
SELECT * FROM acct;

-- COPY in and out
COPY acct FROM stdin;
10	ten	10.00
11	eleven	11.00
\.
COPY (SELECT * FROM acct ORDER BY id) TO stdout;

-- CREATE TABLE AS, with options
CREATE TABLE acct2 USING lance AS SELECT * FROM acct;
SELECT count(*) FROM acct2;
SELECT * FROM acct2 ORDER BY id;

-- TID scans
SELECT count(*) FROM acct WHERE ctid = (SELECT ctid FROM acct WHERE id = 10);

-- cursors read forward
BEGIN;
DECLARE c CURSOR FOR SELECT id FROM acct ORDER BY id;
FETCH 2 FROM c;
CLOSE c;
COMMIT;

DROP TABLE acct, acct2;

-- a temporary table whose rows go at commit, its index rebuilt then
CREATE TEMP TABLE scratch (x int) USING lance ON COMMIT DELETE ROWS;
CREATE INDEX ON scratch (x);
BEGIN;
INSERT INTO scratch VALUES (1), (2);
SELECT count(*) FROM scratch;
COMMIT;
SELECT count(*) FROM scratch;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT count(*) FROM scratch WHERE x = 1;
RESET enable_seqscan;
RESET lance.enable_customscan;
DROP TABLE scratch;

-- a parallel index build: each participant reads its share of the
-- fragments; one in the transaction that wrote the rows reads them alone
CREATE TABLE par (a int, b int) USING lance WITH (max_rows_per_file = 1000);
CREATE TABLE badopt (a int) USING lance WITH (max_rows_per_file = 'many');
INSERT INTO par SELECT g % 10, g FROM generate_series(1, 10000) g;
SELECT (lance.table_info('par')->>'fragments')::int AS fragments;
SET min_parallel_table_scan_size = '1kB';
SET max_parallel_maintenance_workers = 2;
CREATE INDEX par_a ON par (a);
BEGIN;
INSERT INTO par SELECT g % 10, g FROM generate_series(10001, 20000) g;
CREATE INDEX par_b ON par (b);
COMMIT;
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT count(*) FROM par WHERE a = 3;
SELECT count(*) FROM par WHERE b BETWEEN 9990 AND 10010;
RESET enable_seqscan;
RESET lance.enable_customscan;
DROP TABLE par;

-- ANALYZE: a sample of all the rows, or a row of each of a sample of the
-- table's virtual blocks of 256 rows; and of a partitioned table's lance
-- partitions
CREATE TABLE big (id int, g int) USING lance;
INSERT INTO big SELECT i, i % 7 FROM generate_series(1, 392 * 256) i;
ANALYZE big;
SELECT reltuples, relpages FROM pg_class WHERE relname = 'big';
SELECT n_distinct FROM pg_stats WHERE tablename = 'big' AND attname = 'g';
SET default_statistics_target = 1;
ANALYZE big;
SELECT reltuples FROM pg_class WHERE relname = 'big';
SELECT n_distinct FROM pg_stats WHERE tablename = 'big' AND attname = 'g';
RESET default_statistics_target;
CREATE TABLE parted (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE parted_1 PARTITION OF parted FOR VALUES FROM (0) TO (50000) USING lance;
CREATE TABLE parted_2 PARTITION OF parted FOR VALUES FROM (50000) TO (100000) USING lance;
INSERT INTO parted SELECT i, (i % 3)::text FROM generate_series(0, 99999) i;
ANALYZE parted;
SELECT tablename, inherited, n_distinct FROM pg_stats
WHERE tablename LIKE 'parted%' AND attname = 'b' ORDER BY tablename, inherited;
DROP TABLE big, parted;

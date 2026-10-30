-- The index entries of rows no snapshot will see again (docs/design.md §2.6):
-- a rollback's rows, the rows a commit deleted.  A fetch that finds no such
-- row has the index mark the entry dead; an open transaction's rows, and the
-- rows an old snapshot still reads, keep their entries.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS dblink;
RESET client_min_messages;
SELECT dblink_connect('other', 'dbname=' || current_database());
SELECT dblink_exec('other', 'SET enable_seqscan = off');
SELECT dblink_exec('other', 'SET enable_bitmapscan = off');
SELECT dblink_exec('other', 'SET lance.enable_customscan = off');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET lance.enable_customscan = off;
-- the entries of an index of one leaf page, and those marked dead
CREATE FUNCTION entries(idx text, OUT items bigint, OUT dead bigint) LANGUAGE sql AS $$
    SELECT count(*), count(*) FILTER (WHERE i.dead) FROM bt_page_items(idx, 1) i
$$;

-- a rollback's keys again: each uniqueness check finds the row gone and has
-- its entry marked
CREATE TABLE d_pk (id int PRIMARY KEY, v text) USING lance;
INSERT INTO d_pk VALUES (0, 'committed');
BEGIN;
INSERT INTO d_pk SELECT g, 'rolled back' FROM generate_series(1, 100) g;
ROLLBACK;
SELECT * FROM entries('d_pk_pkey');
INSERT INTO d_pk SELECT g, 'again' FROM generate_series(1, 100) g;
SELECT * FROM entries('d_pk_pkey');
SELECT count(*), min(v), max(v) FROM d_pk WHERE id BETWEEN 1 AND 100;
INSERT INTO d_pk VALUES (5, 'a duplicate');
DROP TABLE d_pk;

-- an index scan has the entries of a rollback's rows, and of the rows a
-- commit deleted, marked
CREATE TABLE d_scan (id int NOT NULL, v text) USING lance;
CREATE INDEX d_scan_id ON d_scan (id);
INSERT INTO d_scan VALUES (0, 'committed');
BEGIN;
INSERT INTO d_scan SELECT g, 'rolled back' FROM generate_series(1, 100) g;
ROLLBACK;
SELECT * FROM entries('d_scan_id');
SELECT count(v) FROM d_scan WHERE id BETWEEN 1 AND 100;
SELECT * FROM entries('d_scan_id');
DELETE FROM d_scan WHERE id = 0;
SELECT count(v) FROM d_scan WHERE id = 0;
SELECT * FROM entries('d_scan_id');
DROP TABLE d_scan;

-- a parallel operation cannot tell: its scans go on, and mark nothing
CREATE TABLE d_par (id int NOT NULL, v text) USING lance;
CREATE INDEX d_par_id ON d_par (id);
INSERT INTO d_par VALUES (0, 'committed');
BEGIN;
INSERT INTO d_par SELECT g, 'rolled back' FROM generate_series(1, 100) g;
ROLLBACK;
SET debug_parallel_query = on;
EXPLAIN (COSTS OFF) SELECT count(v) FROM d_par WHERE id BETWEEN 1 AND 100;
SELECT count(v) FROM d_par WHERE id BETWEEN 1 AND 100;
RESET debug_parallel_query;
SELECT * FROM entries('d_par_id');
SELECT count(v) FROM d_par WHERE id BETWEEN 1 AND 100;
SELECT * FROM entries('d_par_id');
DROP TABLE d_par;

-- the rows an old snapshot still reads keep their entries, until it is gone
CREATE TABLE d_old (id int NOT NULL, v text) USING lance;
CREATE INDEX d_old_id ON d_old (id);
INSERT INTO d_old SELECT g, 'x' FROM generate_series(1, 50) g;
SELECT dblink_exec('other', 'BEGIN ISOLATION LEVEL REPEATABLE READ');
SELECT * FROM dblink('other', 'SELECT count(*) FROM d_old') AS r(n bigint);
DELETE FROM d_old WHERE id <= 25;
SELECT count(v) FROM d_old WHERE id <= 25;
SELECT * FROM entries('d_old_id');
SELECT * FROM dblink('other', 'SELECT count(v) FROM d_old WHERE id <= 25') AS r(n bigint);
SELECT dblink_exec('other', 'COMMIT');
-- PostgreSQL moves its horizon on when RecentXmin moves: a transaction id spent
SELECT pg_current_xact_id() IS NOT NULL AS spent;
SELECT count(v) FROM d_old WHERE id <= 25;
SELECT * FROM entries('d_old_id');
DROP TABLE d_old;

-- an open transaction's rows keep their entries: they are there when it
-- commits, and marked once it has rolled back
CREATE TABLE d_open (id int NOT NULL, v text) USING lance;
CREATE INDEX d_open_id ON d_open (id);
INSERT INTO d_open VALUES (0, 'committed');
SELECT dblink_exec('other', 'BEGIN');
SELECT dblink_exec('other', 'INSERT INTO d_open SELECT g, ''open'' FROM generate_series(1, 30) g');
SELECT count(v) FROM d_open WHERE id BETWEEN 1 AND 30;
SELECT * FROM entries('d_open_id');
SELECT dblink_exec('other', 'COMMIT');
SELECT count(v) FROM d_open WHERE id BETWEEN 1 AND 30;
SELECT * FROM entries('d_open_id');
SELECT dblink_exec('other', 'BEGIN');
SELECT dblink_exec('other', 'INSERT INTO d_open SELECT g, ''open'' FROM generate_series(31, 60) g');
SELECT count(v) FROM d_open WHERE id BETWEEN 31 AND 60;
SELECT * FROM entries('d_open_id');
SELECT dblink_exec('other', 'ROLLBACK');
SELECT count(v) FROM d_open WHERE id BETWEEN 31 AND 60;
SELECT * FROM entries('d_open_id');
DROP TABLE d_open;

-- the transaction's own deletions, and its rows under a savepoint rolled
-- back, are no one else's: no entry marked before it ends
CREATE TABLE d_own (id int NOT NULL, v text) USING lance;
CREATE INDEX d_own_id ON d_own (id);
INSERT INTO d_own SELECT g, 'x' FROM generate_series(1, 20) g;
BEGIN;
DELETE FROM d_own WHERE id <= 10;
SELECT count(v) FROM d_own WHERE id <= 10;
SELECT * FROM entries('d_own_id');
ROLLBACK;
SELECT count(v) FROM d_own WHERE id <= 10;
BEGIN;
SAVEPOINT s;
INSERT INTO d_own SELECT g, 'sub' FROM generate_series(100, 109) g;
ROLLBACK TO SAVEPOINT s;
SELECT count(v) FROM d_own WHERE id >= 100;
SELECT * FROM entries('d_own_id');
COMMIT;
SELECT count(v) FROM d_own WHERE id >= 100;
SELECT * FROM entries('d_own_id');
DROP TABLE d_own;

-- VACUUM removes the entries of dead rows from the indexes, marked or not
CREATE TABLE v_pk (id int PRIMARY KEY, v text) USING lance;
INSERT INTO v_pk VALUES (0, 'committed');
BEGIN;
INSERT INTO v_pk SELECT g, 'rolled back' FROM generate_series(1, 100) g;
ROLLBACK;
INSERT INTO v_pk SELECT g, 'again' FROM generate_series(1, 100) g;
SELECT * FROM entries('v_pk_pkey');
VACUUM v_pk;
SELECT * FROM entries('v_pk_pkey');
SELECT reltuples FROM pg_class WHERE relname = 'v_pk_pkey';
SELECT count(*), min(v), max(v) FROM v_pk WHERE id BETWEEN 1 AND 100;
DELETE FROM v_pk WHERE id <= 50;
SELECT * FROM entries('v_pk_pkey');
VACUUM (INDEX_CLEANUP OFF) v_pk;
SELECT * FROM entries('v_pk_pkey');
VACUUM v_pk;
SELECT * FROM entries('v_pk_pkey');
SELECT count(*) FROM v_pk WHERE id BETWEEN 51 AND 100;
INSERT INTO v_pk VALUES (51, 'a duplicate');
INSERT INTO v_pk VALUES (1, 'a key free again');
DROP TABLE v_pk;

-- rows updated in place keep their TIDs and entries, until a commit deletes them
CREATE TABLE v_place (id int PRIMARY KEY, v int) USING lance;
INSERT INTO v_place SELECT g, g FROM generate_series(1, 40) g;
UPDATE v_place SET v = v + 1 WHERE id <= 20;
VACUUM v_place;
SELECT * FROM entries('v_place_pkey');
BEGIN;
UPDATE v_place SET v = v + 1 WHERE id <= 10;
DELETE FROM v_place WHERE id <= 5;
COMMIT;
VACUUM v_place;
SELECT * FROM entries('v_place_pkey');
SELECT count(*), sum(v) FROM v_place WHERE id > 0;
DROP TABLE v_place;

-- beside a lance_index, which Lance keeps
CREATE TABLE v_both (id int NOT NULL, k int) USING lance;
INSERT INTO v_both SELECT g, g FROM generate_series(1, 20) g;
CREATE INDEX v_both_id ON v_both (id);
CREATE INDEX v_both_k ON v_both USING lance_index (k) WITH (type = 'btree');
DELETE FROM v_both WHERE id <= 5;
VACUUM v_both;
SELECT * FROM entries('v_both_id');
SELECT count(*) FROM v_both WHERE id > 5;
DROP TABLE v_both;

-- VACUUM leaves the rows an old snapshot still reads
CREATE TABLE v_old (id int NOT NULL, v text) USING lance;
CREATE INDEX v_old_id ON v_old (id);
INSERT INTO v_old SELECT g, 'x' FROM generate_series(1, 50) g;
SELECT dblink_exec('other', 'BEGIN ISOLATION LEVEL REPEATABLE READ');
SELECT * FROM dblink('other', 'SELECT count(*) FROM v_old') AS r(n bigint);
DELETE FROM v_old WHERE id <= 25;
VACUUM v_old;
SELECT * FROM entries('v_old_id');
SELECT * FROM dblink('other', 'SELECT count(v) FROM v_old WHERE id <= 25') AS r(n bigint);
SELECT dblink_exec('other', 'COMMIT');
SELECT pg_current_xact_id() IS NOT NULL AS spent;
VACUUM v_old;
SELECT * FROM entries('v_old_id');
DROP TABLE v_old;

-- VACUUM leaves an open transaction's rows
CREATE TABLE v_open (id int NOT NULL, v text) USING lance;
CREATE INDEX v_open_id ON v_open (id);
INSERT INTO v_open VALUES (0, 'committed');
SELECT dblink_exec('other', 'BEGIN');
SELECT dblink_exec('other', 'INSERT INTO v_open SELECT g, ''open'' FROM generate_series(1, 30) g');
VACUUM v_open;
SELECT * FROM entries('v_open_id');
SELECT dblink_exec('other', 'COMMIT');
SELECT count(v) FROM v_open WHERE id BETWEEN 1 AND 30;
DROP TABLE v_open;

-- VACUUM leaves the rows of a transaction that begins and commits once it
-- has read the row ids, which the version it read has not
CREATE TABLE v_race (id int NOT NULL, v text) USING lance;
CREATE INDEX v_race_id ON v_race (id);
INSERT INTO v_race VALUES (0, 'committed');
SELECT pg_advisory_lock(424242);
SELECT dblink_exec('other', 'SET lance.debug_vacuum_wait = 424242');
SELECT dblink_send_query('other', 'VACUUM v_race');
DO $$
BEGIN
    WHILE NOT EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' AND NOT granted) LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
INSERT INTO v_race SELECT g, 'new' FROM generate_series(1, 40) g;
SELECT pg_advisory_unlock(424242);
SELECT * FROM dblink_get_result('other') AS r(status text);
SELECT * FROM dblink_get_result('other') AS r(status text);
SELECT dblink_exec('other', 'RESET lance.debug_vacuum_wait');
SELECT count(v) FROM v_race WHERE id BETWEEN 1 AND 40;
SELECT * FROM entries('v_race_id');
DROP TABLE v_race;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET lance.enable_customscan;
DROP FUNCTION entries(text);
SELECT dblink_disconnect('other');

-- In-place updates (docs/design.md §2.7): an UPDATE that changes no column
-- an index reads keeps the row's TID, so that no index needs an entry for
-- its new version, as pg_inplace_update_am's in-place updates and heap's
-- HOT updates do; one that changes one, or that an AFTER trigger would
-- refetch by TID, gives the row a new TID
CREATE TABLE inp (id int NOT NULL, k int, v text, n int) USING lance;
INSERT INTO inp SELECT g, g % 10, 'v' || g, 0 FROM generate_series(1, 100) g;
CREATE UNIQUE INDEX inp_id ON inp (id);
CREATE INDEX inp_k ON inp (k);
CREATE TEMP TABLE t0 AS SELECT id, ctid AS t FROM inp;

-- a column no index reads: the same TID; an indexed one: a new TID
UPDATE inp SET v = 'w' || id, n = n + 1 WHERE id <= 5;
UPDATE inp SET k = k + 100 WHERE id = 6;
SET lance.update_in_place = off;
UPDATE inp SET n = n + 1 WHERE id = 7;
RESET lance.update_in_place;
SELECT i.id, i.ctid = t0.t AS same_tid, i.k, i.v, i.n
FROM inp i JOIN t0 USING (id) WHERE id <= 8 ORDER BY id;

-- the indexes find the rows, a new TID's and the same TID's
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT id, v, n FROM inp WHERE id IN (3, 6, 7) ORDER BY id;
SELECT id FROM inp WHERE k = 106;
RESET enable_seqscan;
RESET lance.enable_customscan;

-- one transaction updating a row again and again, savepoints, a cursor
BEGIN;
DECLARE c CURSOR FOR SELECT id, n FROM inp WHERE id = 10;
UPDATE inp SET n = n + 1 WHERE id = 10;
UPDATE inp SET n = n + 1 WHERE id = 10;
SELECT id, n FROM inp WHERE id = 10;
SAVEPOINT s;
UPDATE inp SET n = n + 100 WHERE id = 10;
SELECT id, n FROM inp WHERE id = 10;
ROLLBACK TO SAVEPOINT s;
SELECT id, n FROM inp WHERE id = 10;
FETCH c;
CLOSE c;
UPDATE inp SET n = n + 1 WHERE id = 10 RETURNING old.n AS old_n, new.n AS new_n;
DELETE FROM inp WHERE id = 11;
UPDATE inp SET n = 9 WHERE id = 11;
SELECT count(*) FROM inp WHERE id = 11;
COMMIT;
SELECT i.id, i.ctid = t0.t AS same_tid, i.n FROM inp i JOIN t0 USING (id) WHERE id IN (10, 11);
SELECT count(*), count(DISTINCT ctid), count(DISTINCT id) FROM inp;

-- a row the transaction inserted, updated in place before the commit
BEGIN;
INSERT INTO inp VALUES (500, 5, 'new', 0);
CREATE TEMP TABLE t500 AS SELECT ctid AS t FROM inp WHERE id = 500;
UPDATE inp SET n = 1 WHERE id = 500;
UPDATE inp SET n = 2 WHERE id = 500;
SELECT n, ctid = (SELECT t FROM t500) AS same_tid FROM inp WHERE id = 500;
COMMIT;
SELECT n, ctid = (SELECT t FROM t500) AS same_tid FROM inp WHERE id = 500;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT n FROM inp WHERE id = 500;
RESET enable_seqscan;
RESET lance.enable_customscan;

-- ON CONFLICT's update of a column no index reads keeps the TID too
INSERT INTO inp VALUES (1, 1, 'conflict', 0) ON CONFLICT (id) DO UPDATE SET n = inp.n + 10;
SELECT i.ctid = t0.t AS same_tid, i.n FROM inp i JOIN t0 USING (id) WHERE id = 1;
-- ... and refuses to change a row it changed in place a second time
INSERT INTO inp VALUES (2, 2, 'a', 0), (2, 2, 'b', 0) ON CONFLICT (id) DO UPDATE SET n = 5;
SELECT n FROM inp WHERE id = 2;

-- an AFTER ROW trigger refetches OLD and NEW by TID: a new TID
CREATE TABLE inp_log (id int, old_n int, new_n int);
CREATE FUNCTION inp_log_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO inp_log VALUES (NEW.id, OLD.n, NEW.n);
    RETURN NULL;
END$$;
CREATE TRIGGER inp_after AFTER UPDATE ON inp FOR EACH ROW EXECUTE FUNCTION inp_log_fn();
CREATE TEMP TABLE t20 AS SELECT ctid AS t FROM inp WHERE id = 20;
UPDATE inp SET n = 42 WHERE id = 20;
SELECT * FROM inp_log;
SELECT ctid = (SELECT t FROM t20) AS same_tid FROM inp WHERE id = 20;
DROP TRIGGER inp_after ON inp;

-- a foreign key: the referenced row's other columns in place; its key,
-- whose check the update needs, with a new TID
CREATE TABLE inp_ref (a int REFERENCES inp (id) ON UPDATE CASCADE) USING lance;
INSERT INTO inp_ref VALUES (30);
CREATE TEMP TABLE t30 AS SELECT ctid AS t FROM inp WHERE id = 30;
UPDATE inp SET v = 'referenced' WHERE id = 30;
SELECT ctid = (SELECT t FROM t30) AS same_tid FROM inp WHERE id = 30;
UPDATE inp SET id = 3000 WHERE id = 30;
SELECT ctid = (SELECT t FROM t30) AS same_tid FROM inp WHERE id = 3000;
SELECT a FROM inp_ref;
INSERT INTO inp_ref VALUES (31);
INSERT INTO inp_ref VALUES (99999);
-- the referencing row updated in place, its check reading NEW
UPDATE inp_ref SET a = 31 WHERE a = 31;
UPDATE inp_ref SET a = 99999 WHERE a = 31;
DROP TABLE inp_ref;

-- an index's operator that updates the referenced row while the foreign
-- key's check holds the index's page: the update is in place, and the
-- check does not wait for its own page (PostgreSQL 19's foreign_key.sql)
BEGIN;
CREATE TABLE fp_pk (id int, v int DEFAULT 0) USING lance;
CREATE FUNCTION fp_eq(int, int) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
    UPDATE fp_pk SET v = v + 1 WHERE id OPERATOR(pg_catalog.=) $1;
    RETURN $1 OPERATOR(pg_catalog.=) $2;
END$$;
CREATE OPERATOR =~= (LEFTARG = int, RIGHTARG = int, FUNCTION = fp_eq);
CREATE OPERATOR CLASS fp_ops FOR TYPE int USING btree AS
    OPERATOR 1 pg_catalog.<, OPERATOR 2 pg_catalog.<=, OPERATOR 3 =~=,
    OPERATOR 4 pg_catalog.>=, OPERATOR 5 pg_catalog.>,
    FUNCTION 1 btint4cmp(int, int);
CREATE UNIQUE INDEX fp_pk_id ON fp_pk (id fp_ops);
INSERT INTO fp_pk VALUES (1);
CREATE TABLE fp_fk (a int REFERENCES fp_pk (id)) USING lance;
SAVEPOINT fp;
INSERT INTO fp_fk VALUES (1);
ROLLBACK TO SAVEPOINT fp;
ROLLBACK;

-- two sessions updating rows in place, of a table without a unique index,
-- whose writers write alone
CREATE TABLE inc (id int NOT NULL, k int, n int) USING lance;
INSERT INTO inc SELECT g, g % 10, 0 FROM generate_series(1, 30) g;
INSERT INTO inc SELECT g, g % 10, 0 FROM generate_series(31, 60) g;
CREATE INDEX inc_k ON inc (k);
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('inp_other', 'dbname=' || current_database());
\set VERBOSITY terse
-- the same row: the first committer wins, as with new TIDs
BEGIN;
UPDATE inc SET n = n + 1 WHERE id = 40;
SELECT dblink_exec('inp_other', 'UPDATE inc SET n = n + 1000 WHERE id = 40');
COMMIT;
SELECT n FROM inc WHERE id = 40;
-- an update in place against a delete
BEGIN;
DELETE FROM inc WHERE id = 41;
SELECT dblink_exec('inp_other', 'UPDATE inc SET n = 7 WHERE id = 41');
COMMIT;
SELECT id, n FROM inc WHERE id = 41;
-- different rows: both commit
BEGIN;
UPDATE inc SET n = n + 1 WHERE id = 42;
SELECT dblink_exec('inp_other', 'UPDATE inc SET n = n + 1 WHERE id = 43');
COMMIT;
SELECT id, n FROM inc WHERE id IN (42, 43) ORDER BY id;
-- a compaction moving the row between the read and the commit
SELECT (lance.table_info('inc')->>'fragments')::int > 1 AS many_fragments;
BEGIN;
UPDATE inc SET n = 77 WHERE id = 50;
SELECT dblink_exec('inp_other', 'VACUUM inc');
COMMIT;
SELECT (lance.table_info('inc')->>'fragments')::int AS fragments;
SELECT id, n FROM inc WHERE id = 50;
\set VERBOSITY default
SELECT dblink_disconnect('inp_other');
SELECT count(*), count(DISTINCT ctid), count(DISTINCT id) FROM inc;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT id, n FROM inc WHERE k = 0 ORDER BY id;
RESET enable_seqscan;
RESET lance.enable_customscan;
DROP TABLE inc;

-- a version per row, after VACUUM too
SELECT count(*), count(DISTINCT ctid), count(DISTINCT id) FROM inp;
SET enable_seqscan = off;
SET lance.enable_customscan = off;
SELECT id, n FROM inp WHERE id IN (1, 10, 20, 500) ORDER BY id;
RESET enable_seqscan;
RESET lance.enable_customscan;
DROP TABLE inp, inp_log;
DROP FUNCTION inp_log_fn();

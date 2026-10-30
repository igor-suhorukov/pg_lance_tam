-- Two sessions writing one table (docs/design.md §2.7): writes that touch
-- different rows both commit, Lance merging their deletion vectors; of two
-- that touch the same row, the first to commit wins and the second fails
-- with a serialization failure; each snapshot reads the version its
-- catalog row names.
CREATE EXTENSION dblink;
CREATE TABLE acc (id int NOT NULL, bal int) USING lance;
INSERT INTO acc SELECT g, 100 FROM generate_series(1, 10) g;
SELECT dblink_connect('other', 'dbname=' || current_database());
-- the conflicts' details name row ids, which differ from run to run
\set VERBOSITY terse

-- the same row: the first committer wins
BEGIN;
UPDATE acc SET bal = bal - 10 WHERE id = 1;
SELECT dblink_exec('other', 'UPDATE acc SET bal = bal - 20 WHERE id = 1');
COMMIT;
SELECT bal FROM acc WHERE id = 1;

-- a delete against an update of the same row
BEGIN;
DELETE FROM acc WHERE id = 4;
SELECT dblink_exec('other', 'UPDATE acc SET bal = 0 WHERE id = 4');
COMMIT;
SELECT id, bal FROM acc WHERE id = 4;

-- different rows of the same fragment: both commit
BEGIN;
UPDATE acc SET bal = bal + 1 WHERE id = 2;
SELECT dblink_exec('other', 'UPDATE acc SET bal = bal + 1 WHERE id = 3');
COMMIT;
SELECT id, bal FROM acc WHERE id IN (2, 3) ORDER BY id;
BEGIN;
DELETE FROM acc WHERE id = 5;
SELECT dblink_exec('other', 'DELETE FROM acc WHERE id = 6');
COMMIT;
SELECT count(*) FROM acc WHERE id IN (5, 6);

-- inserts never conflict
BEGIN;
INSERT INTO acc VALUES (100, 1);
SELECT dblink_exec('other', 'INSERT INTO acc VALUES (101, 1)');
COMMIT;
SELECT count(*) FROM acc WHERE id >= 100;

-- uncommitted rows are their transaction's alone
BEGIN;
INSERT INTO acc VALUES (200, 1);
SELECT * FROM dblink('other', 'SELECT count(*) FROM acc WHERE id = 200') AS t(c bigint);
COMMIT;
SELECT * FROM dblink('other', 'SELECT count(*) FROM acc WHERE id = 200') AS t(c bigint);

-- REPEATABLE READ keeps its snapshot's version; READ COMMITTED moves on
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM acc;
SELECT dblink_exec('other', 'INSERT INTO acc VALUES (102, 1)');
SELECT count(*) FROM acc;
COMMIT;
BEGIN;
SELECT count(*) FROM acc;
SELECT dblink_exec('other', 'DELETE FROM acc WHERE id = 102');
SELECT count(*) FROM acc;
COMMIT;

-- a transaction that fails after its Lance commit: the version it
-- committed is undone, and no later commit brings its rows back
SELECT max(version) AS before FROM lance.versions('acc') \gset
SET lance.debug_fail_after_commit = 1;
INSERT INTO acc VALUES (300, 1);
RESET lance.debug_fail_after_commit;
SELECT count(*) FROM acc WHERE id = 300;
SELECT max(version) > :before AS restored_as_new_version FROM lance.versions('acc');
INSERT INTO acc VALUES (301, 1);
SELECT count(*) FROM acc WHERE id IN (300, 301);
SELECT format('CREATE TABLE acc_latest () USING lance WITH (uri = %L, read_only = true, follow = true)',
              lance.table_info('acc')->>'uri') AS ddl \gset
:ddl;
SELECT count(*) FROM acc_latest WHERE id IN (300, 301);
SELECT (SELECT count(*) FROM acc) = (SELECT count(*) FROM acc_latest) AS lance_agrees;

-- Row ids (docs/design.md §2.6): a writer takes them from lance.rowid_seq a
-- chunk at a time, and keeps a chunk while no dataset it writes holds the
-- ids it has left.  A transaction that took a chunk holds the sequence to
-- its end: no writer waits for it.
CREATE FUNCTION rowid(t tid) RETURNS bigint LANGUAGE sql IMMUTABLE
AS $$ SELECT ((t::text::point)[0] * 256 + (t::text::point)[1] - 1)::bigint $$;
CREATE TABLE w1 (id int) USING lance;
CREATE TABLE w2 (id int) USING lance;
SELECT dblink_exec('other', 'SET lock_timeout = ''5s''');
SELECT dblink_exec('other', 'INSERT INTO w1 VALUES (1)');
-- rows above the other session's chunk
INSERT INTO w1 SELECT g FROM generate_series(2, 70001) g;
BEGIN;
INSERT INTO w2 SELECT g FROM generate_series(1, 70000) g;
SELECT dblink_exec('other', 'INSERT INTO w1 VALUES (0)');
COMMIT;
SELECT count(*), count(DISTINCT ctid) FROM w1;
SELECT (SELECT rowid(ctid) FROM w1 WHERE id = 0) - (SELECT rowid(ctid) FROM w1 WHERE id = 1)
    AS chunk_kept;

-- a dataset that outlived the sequence -- the extension made again over
-- its datasets -- holds ids the sequence gives again: a writer's chunks
-- start above the dataset's ids, the sequence moved forward by nextval()
SELECT last_value AS seq_was FROM lance.rowid_seq \gset
BEGIN;
INSERT INTO w2 SELECT g FROM generate_series(1, 70000) g;
SELECT setval('lance.rowid_seq', 549755813888, false) IS NOT NULL AS reset;
SELECT dblink_connect('fresh', 'dbname=' || current_database());
SELECT dblink_exec('fresh', 'SET lock_timeout = ''5s''');
SELECT dblink_exec('fresh', 'INSERT INTO w1 VALUES (-1)');
COMMIT;
SELECT count(*), count(DISTINCT ctid) FROM w1;
SELECT rowid(ctid) > (SELECT max(rowid(ctid)) FROM w1 WHERE id >= 0) AS above FROM w1 WHERE id = -1;

-- a chunk whose ids the dataset holds is left, though the dataset's
-- highest id is above it, as another writer's are: here a backend given
-- the other session's chunk again wrote some of its ids
CREATE TABLE w3 (id int) USING lance;
SELECT dblink_exec('other', 'INSERT INTO w3 VALUES (1)');
SELECT rowid(ctid) AS r, rowid(ctid) - rowid(ctid) % 65536 AS k FROM w3 \gset
CREATE TABLE w4 (id int) USING lance;
SELECT setval('lance.rowid_seq', :k, false) IS NOT NULL AS reset;
SELECT dblink_connect('again', 'dbname=' || current_database());
SELECT dblink_exec('again', 'INSERT INTO w4 SELECT g FROM generate_series(1, ' || (:r - :k + 10) || ') g')
    LIKE 'INSERT 0 %' AS inserted;
SELECT dblink_disconnect('again');
INSERT INTO w4 VALUES (100000);
SELECT dblink_exec('other', 'INSERT INTO w4 VALUES (200000)');
SELECT count(*), count(DISTINCT ctid) FROM w4;
SELECT rowid(ctid) > (SELECT max(rowid(ctid)) FROM w4 WHERE id <> 200000) AS above
FROM w4 WHERE id = 200000;
-- the sequence past every chunk given
SELECT setval('lance.rowid_seq', greatest(:seq_was, last_value)) > 0 AS restored FROM lance.rowid_seq;
SELECT dblink_disconnect('fresh');
SELECT dblink_exec('other', 'RESET lock_timeout');
DROP TABLE w1, w2, w3, w4;
DROP FUNCTION rowid(tid);

SELECT dblink_disconnect('other');
DROP TABLE acc_latest;
DROP TABLE acc;
DROP EXTENSION dblink;

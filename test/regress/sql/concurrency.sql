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

SELECT dblink_disconnect('other');
DROP TABLE acc_latest;
DROP TABLE acc;
DROP EXTENSION dblink;

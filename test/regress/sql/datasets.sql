-- Datasets: where they live, attaching one that exists, versions,
-- maintenance (docs/design.md §2.4, §2.10, §2.11)
CREATE TABLE src (id int NOT NULL, label text, emb vector(2)) USING lance;
INSERT INTO src VALUES (1, 'one', '[1,0]'), (2, 'two', '[0,1]');
INSERT INTO src VALUES (3, 'three', '[1,1]');
SELECT (lance.table_info('src')->>'managed')::bool AS managed,
       lance.table_info('src')->>'uri' LIKE '%/lance_regress/public/src_%.lance' AS default_location;

-- a second table over the same dataset, its columns taken from it
SELECT format('CREATE TABLE att () USING lance WITH (uri = %L, read_only = true)',
              lance.table_info('src')->>'uri') AS ddl \gset
:ddl;
\d att
SELECT * FROM att ORDER BY id;
INSERT INTO att VALUES (4, 'four', '[2,2]');
SELECT (lance.table_info('att')->>'managed')::bool AS managed;

-- it reads the version its catalog row names: src's later commits are not
-- att's until it is refreshed ...
INSERT INTO src VALUES (4, 'four', '[2,2]');
SELECT count(*) FROM att;
SELECT lance.refresh('att') > 0 AS refreshed;
SELECT count(*) FROM att;

-- ... unless it follows its dataset
SELECT format('CREATE TABLE fol () USING lance WITH (uri = %L, read_only = true, follow = true)',
              lance.table_info('src')->>'uri') AS ddl \gset
:ddl;
INSERT INTO src VALUES (5, 'five', '[3,3]');
SELECT count(*) FROM fol;

-- a table naming a column the dataset lacks is refused
DO $$
BEGIN
  EXECUTE format('CREATE TABLE bad (nope int) USING lance WITH (uri = %L)',
                 lance.table_info('src')->>'uri');
EXCEPTION WHEN undefined_column THEN
  RAISE NOTICE 'refused: %', regexp_replace(SQLERRM, ' at ".*"', '');
END $$;

-- dropping an attached table leaves the dataset
DROP TABLE att, fol;
SELECT count(*) FROM src;

-- versions: each commit one, tagged with the transaction that made it
SELECT count(*) >= 4 AS versions FROM lance.versions('src');
SELECT (lance.table_info('src')->>'version')::int = max(version) AS current FROM lance.versions('src');

-- maintenance
SELECT lance.optimize('src') ? 'fragments_removed' AS optimized;
SELECT (lance.table_info('src')->>'fragments')::int AS fragments;
SELECT count(*), string_agg(label, ',' ORDER BY id) FROM src;
SELECT lance.cleanup('src', '0 seconds') ? 'old_versions' AS cleaned;
SELECT count(*) FROM src;
SELECT lance.explain('src', 'id > 1') ILIKE '%filter%' AS explained;

-- options: credentials are refused, unknown storage options pass to Lance
CREATE TABLE secret (a int) USING lance WITH (aws_secret_access_key = 'x');
-- ... and PostgreSQL's own storage parameters stay PostgreSQL's
CREATE TABLE ns (a int) USING lance
    WITH (autovacuum_enabled = false, parallel_workers = 2, toast.autovacuum_enabled = false,
          max_rows_per_file = 100);
SELECT reloptions FROM pg_class WHERE relname = 'ns';
ALTER TABLE ns SET (autovacuum_enabled = true);
ALTER TABLE ns RESET (parallel_workers);
SELECT reloptions FROM pg_class WHERE relname = 'ns';
ALTER TABLE ns SET (max_rows_per_file = 5);
CREATE TABLE badparam (a int) USING lance WITH (autovacuum_enabled = 'maybe');
DROP TABLE ns;

-- ALTER TABLE that would leave the dataset behind is refused
ALTER TABLE src SET UNLOGGED;
ALTER TABLE src ALTER COLUMN id TYPE bigint;
ALTER TABLE src SET ACCESS METHOD heap;
VACUUM FULL src;
CLUSTER src USING nothing;
-- ... a rename, a comment, a default are the catalog's alone
ALTER TABLE src RENAME COLUMN label TO title;
SELECT title FROM src WHERE id = 1;
ALTER TABLE src RENAME TO src2;
SELECT count(*) FROM src2;
DROP TABLE src2;

-- Lance tables in an S3 bucket (docs/design.md §2.4): the endpoint and
-- region a storage server's options, the credentials a user mapping's
CREATE EXTENSION vector;
CREATE EXTENSION lance_tam;
CREATE SERVER s3 FOREIGN DATA WRAPPER lance_storage
    OPTIONS (aws_endpoint 'http://s3:4566', aws_region 'us-east-1', allow_http 'true');
CREATE USER MAPPING FOR PUBLIC SERVER s3
    OPTIONS (aws_access_key_id 'test', aws_secret_access_key 'test');

CREATE TABLE docs (id int NOT NULL, body text, emb vector(4))
    USING lance WITH (uri = 's3://lance-test/docs.lance', server = 's3');
SELECT lance.table_info('docs')->>'uri' AS uri,
       (lance.table_info('docs')->>'managed')::bool AS managed;
INSERT INTO docs SELECT g, 'doc ' || g, ARRAY[g % 7, g % 11, g % 13, g % 17]::real[]::vector
FROM generate_series(1, 2000) g;
SELECT count(*), min(id), max(id) FROM docs;

-- a transaction's own rows, its deletes, its rollback, on S3 as on disk
BEGIN;
INSERT INTO docs VALUES (5000, 'pending', '[1,1,1,1]');
DELETE FROM docs WHERE id <= 100;
SELECT count(*) FROM docs;
ROLLBACK;
SELECT count(*) FROM docs;
UPDATE docs SET body = 'updated' WHERE id % 500 = 0;
DELETE FROM docs WHERE id > 1990;
SELECT count(*), count(*) FILTER (WHERE body = 'updated') AS updated FROM docs;

-- search, with a filter given to Lance, and an index
SELECT id FROM docs ORDER BY emb <-> '[0,0,0,0]' LIMIT 3;
SELECT id FROM docs WHERE id > 1000 ORDER BY emb <-> '[1,2,3,4]' LIMIT 3;
CREATE INDEX docs_emb ON docs USING lance_index (emb) WITH (type = 'ivf_flat', num_partitions = 2);
SELECT count(*) FROM (SELECT id FROM docs ORDER BY emb <-> '[1,2,3,4]' LIMIT 5) s;
CREATE INDEX docs_id ON docs USING lance_index (id) WITH (type = 'btree');
SELECT body FROM docs WHERE id = 1000;
SELECT jsonb_array_length(lance.indices('docs')) AS indices;

-- maintenance
VACUUM docs;
SELECT (lance.table_info('docs')->>'fragments')::int AS fragments;
SELECT count(*) FROM docs;

-- attached a second time, read-only, its columns from the dataset
CREATE TABLE docs_ro () USING lance
    WITH (uri = 's3://lance-test/docs.lance', server = 's3', read_only = true);
SELECT count(*) FROM docs_ro;
DROP TABLE docs_ro;

-- a table of an explicit uri is not the server's: its dataset outlives it
DROP TABLE docs;
CREATE TABLE docs_again () USING lance WITH (uri = 's3://lance-test/docs.lance', server = 's3');
SELECT count(*) FROM docs_again;
DROP TABLE docs_again;

-- the default location in the bucket, through the default server
SET lance.default_location = 's3://lance-test/pg';
SET lance.default_server = 's3';
CREATE TABLE managed_s3 (a int, b text) USING lance;
SELECT lance.table_info('managed_s3')->>'uri' LIKE 's3://lance-test/pg/lance_s3/public/managed_s3_%.lance' AS in_bucket,
       (lance.table_info('managed_s3')->>'managed')::bool AS managed;
INSERT INTO managed_s3 SELECT g, 'b' || g FROM generate_series(1, 10) g;
SELECT sum(a) FROM managed_s3;
DROP TABLE managed_s3;
RESET lance.default_location;
RESET lance.default_server;

-- credentials stay out of the table's options; a missing user mapping
-- leaves the request unsigned and the store refuses it, or not: LocalStack
-- takes any request, so the check is the options' only
SELECT count(*) FROM lance.tables;
SELECT srvoptions FROM pg_foreign_server WHERE srvname = 's3';

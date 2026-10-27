-- lance_tam against heap (and pgvector's own indexes): 200,000 rows of a
-- 128-dimension vector, an int and a text; each timed statement run twice,
-- the second time reported.  The vectors lie on a surface of 4 dimensions
-- (each row's 128 values are functions of 4 random parameters), so that
-- their nearest neighbours are well defined, as embeddings' are.
\set ON_ERROR_STOP 1
CREATE EXTENSION vector;
CREATE EXTENSION lance_tam;
SET max_parallel_workers_per_gather = 0;
CREATE TABLE src AS
SELECT g::bigint AS id, g % 100 AS cat, 'row ' || g AS label,
       (SELECT array_agg((sin(p.p1 * x / 10.0 + p.p2) * p.p3 + cos(p.p4 * x / 20.0))::real ORDER BY x)
        FROM generate_series(1, 128) x)::vector(128) AS emb
FROM generate_series(1, 200000) g,
     LATERAL (SELECT random() + 0 * g AS p1, random() * 6.28 + 0 * g AS p2,
                     random() + 0 * g AS p3, random() + 0 * g AS p4) p;
CREATE TABLE q AS SELECT emb FROM src WHERE id = 4242;
SELECT emb AS qv FROM q \gset
CREATE TABLE hp (id bigint NOT NULL, cat int, label text, emb vector(128));
CREATE TABLE lt (id bigint NOT NULL, cat int, label text, emb vector(128)) USING lance;
\timing on
\echo == load
INSERT INTO hp SELECT * FROM src;
INSERT INTO lt SELECT * FROM src;
TRUNCATE hp; TRUNCATE lt;
INSERT INTO hp SELECT * FROM src;
INSERT INTO lt SELECT * FROM src;
VACUUM ANALYZE hp;
ANALYZE lt;
\echo == projection: sum of one column
SELECT sum(cat) FROM hp; SELECT sum(cat) FROM hp;
SELECT sum(cat) FROM lt; SELECT sum(cat) FROM lt;
SET lance.scan_in_order = off;
SELECT sum(cat) FROM lt; SELECT sum(cat) FROM lt;
SELECT sum(length(label)) FROM lt; SELECT sum(length(label)) FROM lt;
RESET lance.scan_in_order;
SELECT sum(length(label)) FROM lt; SELECT sum(length(label)) FROM lt;
SELECT sum(length(label)) FROM hp; SELECT sum(length(label)) FROM hp;
\echo == filter: count where cat = 7
SELECT count(*) FROM hp WHERE cat = 7; SELECT count(*) FROM hp WHERE cat = 7;
SELECT count(*) FROM lt WHERE cat = 7; SELECT count(*) FROM lt WHERE cat = 7;
\echo == exact nearest 10
SELECT id FROM hp ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM hp ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
\echo == vector index build: pgvector hnsw, ivfflat; lance ivf_pq
CREATE INDEX hp_ivf ON hp USING ivfflat (emb vector_l2_ops) WITH (lists = 256);
DROP INDEX hp_ivf;
CREATE INDEX hp_hnsw ON hp USING hnsw (emb vector_l2_ops);
CREATE INDEX lt_ivfpq ON lt USING lance_index (emb) WITH (type = 'ivf_pq', num_partitions = 256, num_sub_vectors = 16);
\echo == indexed nearest 10: pgvector hnsw (ef_search 40); lance ivf_pq (nprobes 20, refine 10)
SET lance.nprobes = 20;
SET lance.refine_factor = 10;
SELECT id FROM hp ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM hp ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
\timing off
SELECT count(*) AS hnsw_recall FROM (SELECT id FROM hp ORDER BY emb <-> :'qv' LIMIT 10) a
  JOIN (SELECT id FROM src ORDER BY emb <-> :'qv' LIMIT 10) b USING (id);
SELECT count(*) AS ivfpq_recall FROM (SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10) a
  JOIN (SELECT id FROM src ORDER BY emb <-> :'qv' LIMIT 10) b USING (id);
\timing on
\echo == lance ivf_hnsw_sq
DROP INDEX lt_ivfpq;
CREATE INDEX lt_hnsw ON lt USING lance_index (emb) WITH (type = 'ivf_hnsw_sq', num_partitions = 16);
RESET lance.refine_factor;
SET lance.nprobes = 4;
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10 \g /dev/null
\timing off
SELECT count(*) AS hnsw_sq_recall FROM (SELECT id FROM lt ORDER BY emb <-> :'qv' LIMIT 10) a
  JOIN (SELECT id FROM src ORDER BY emb <-> :'qv' LIMIT 10) b USING (id);
\timing on
\echo == update and delete 2000 rows, by a filter
UPDATE hp SET cat = cat + 1 WHERE id % 100 = 3;
UPDATE lt SET cat = cat + 1 WHERE id % 100 = 3;
DELETE FROM hp WHERE id % 100 = 5;
DELETE FROM lt WHERE id % 100 = 5;
\echo == vacuum
VACUUM hp;
VACUUM (VERBOSE) lt;
\echo == sizes
\timing off
SELECT pg_size_pretty(pg_total_relation_size('hp')) AS heap_with_indexes,
       pg_size_pretty((lance.table_info('lt')->>'data_bytes')::bigint) AS lance_data,
       lance.table_info('lt')->>'fragments' AS lance_fragments;

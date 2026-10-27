-- The basics: a table, its rows, a transaction's own view, a search
CREATE EXTENSION vector;
CREATE EXTENSION lance_tam;
SELECT lance.engine_version() LIKE 'lance_engine%' AS engine_loaded;

CREATE TABLE t (id bigint NOT NULL, name text, score float8, emb vector(3)) USING lance;
INSERT INTO t VALUES (1, 'a', 1.5, '[1,2,3]'), (2, 'b', 2.5, '[4,5,6]'), (3, NULL, NULL, NULL);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;

-- a transaction sees its own rows; ROLLBACK takes them back
BEGIN;
INSERT INTO t VALUES (4, 'd', 4.5, '[7,8,9]');
SELECT count(*) FROM t;
ROLLBACK;
SELECT count(*) FROM t;

UPDATE t SET name = 'bb' WHERE id = 2;
DELETE FROM t WHERE id = 1;
SELECT * FROM t ORDER BY id;

-- nearest neighbours
INSERT INTO t VALUES (5, 'e', 0, '[1,2,4]'), (6, 'f', 0, '[10,10,10]');
SELECT id, round((emb <-> '[1,2,3]')::numeric, 4) AS d FROM t ORDER BY emb <-> '[1,2,3]' LIMIT 2;
EXPLAIN (COSTS OFF) SELECT id FROM t ORDER BY emb <-> '[1,2,3]' LIMIT 2;
EXPLAIN (COSTS OFF) SELECT id, name FROM t WHERE id > 2;

SELECT (lance.table_info('t')->>'rows')::int AS rows, (lance.table_info('t')->>'writable')::bool AS writable;
DROP TABLE t;

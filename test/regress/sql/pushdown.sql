-- LanceScan's filters (docs/design.md §2.8): the quals Lance's scalar
-- indices answer, costed by them; quals of values the scan is given when it
-- starts -- a generic plan's parameters, an InitPlan's result, a nested
-- loop's outer values -- made into Lance's filter each time it starts
CREATE TABLE pairs (origin int NOT NULL, target int, t smallint, name text, day date, flag bool)
  USING lance;
INSERT INTO pairs
SELECT g / 10, g, (g % 3600)::smallint, 'n' || (g % 100), DATE '2026-01-01' + (g % 30), g % 2 = 0
FROM generate_series(0, 49999) g;
ANALYZE pairs;

-- no index: Lance filters, and the plan counts on no index
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE origin = 42;
CREATE INDEX pairs_origin ON pairs USING lance_index (origin) WITH (type = 'btree');
CREATE INDEX pairs_name ON pairs USING lance_index (name) WITH (type = 'bitmap');
SELECT i->>'name' AS name, i->>'type' AS type, (i->>'rows')::bigint AS rows
FROM jsonb_array_elements(lance.indices('pairs')) i ORDER BY 1;

-- the indices answer equality, IN lists, ranges, ORs of them; not <>
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE origin = 42;
SELECT count(*) FROM pairs WHERE origin = 42;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE origin IN (1, 2, 3) AND t < 15;
SELECT count(*) FROM pairs WHERE origin IN (1, 2, 3) AND t < 15;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE name = 'n7' OR origin BETWEEN 5 AND 6;
SELECT count(*) FROM pairs WHERE name = 'n7' OR origin BETWEEN 5 AND 6;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE origin <> 42;
SET lance.enable_filter_pushdown = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pairs WHERE origin = 42;
RESET lance.enable_filter_pushdown;

-- a generic plan's parameters
SET plan_cache_mode = force_generic_plan;
PREPARE by_origin(int) AS SELECT count(*), min(target), max(target) FROM pairs WHERE origin = $1;
EXPLAIN (COSTS OFF) EXECUTE by_origin(42);
EXECUTE by_origin(42);
EXECUTE by_origin(4999);
EXECUTE by_origin(-1);
-- = NULL admits no row: nothing is read
EXECUTE by_origin(NULL);
-- a value outside the column's type is no literal of it: PostgreSQL's qual
PREPARE by_t(bigint) AS SELECT count(*) FROM pairs WHERE t < $1;
EXPLAIN (COSTS OFF) EXECUTE by_t(100);
EXECUTE by_t(100);
EXECUTE by_t(100000);
EXECUTE by_t(-100000);
-- under an OR a NULL decides nothing; under NOT it is NULL still
PREPARE by_or(int) AS SELECT count(*) FROM pairs WHERE origin = $1 OR origin = 7;
EXECUTE by_or(8);
EXECUTE by_or(NULL);
PREPARE by_not(int) AS SELECT count(*) FROM pairs WHERE NOT (origin = $1) AND origin < 3;
EXECUTE by_not(1);
EXECUTE by_not(NULL);
-- NOT (a AND b) is NOT a OR NOT b: a NULL in it decides nothing either
PREPARE by_nand(int) AS SELECT count(*) FROM pairs WHERE NOT (origin = $1 AND t = 5) AND origin < 3;
EXECUTE by_nand(0);
EXECUTE by_nand(NULL);
-- NOT of an empty IN list is true, of a list of nulls NULL
PREPARE by_none(int[]) AS SELECT count(*) FROM pairs WHERE NOT (origin = ANY ($1)) AND origin < 3;
EXECUTE by_none('{}');
EXECUTE by_none('{1}');
EXECUTE by_none('{NULL}');
-- an array parameter's IN list
PREPARE by_any(int[]) AS SELECT count(*) FROM pairs WHERE origin = ANY ($1);
EXPLAIN (COSTS OFF) EXECUTE by_any('{1,2}');
EXECUTE by_any('{1,2}');
EXECUTE by_any('{1,NULL,3}');
EXECUTE by_any('{NULL}');
EXECUTE by_any('{}');
EXECUTE by_any(NULL);
PREPARE by_list(int, int) AS SELECT count(*) FROM pairs WHERE origin IN ($1, $2, 9);
EXPLAIN (COSTS OFF) EXECUTE by_list(1, 2);
EXECUTE by_list(1, NULL);
-- text, date and boolean values, a quote among them, and a stable expression
PREPARE by_name(text, date, bool) AS
  SELECT count(*) FROM pairs WHERE name = $1 AND day >= $2 AND flag = $3;
EXPLAIN (COSTS OFF) EXECUTE by_name('n7', '2026-01-15', true);
EXECUTE by_name('n7', '2026-01-15', false);
EXECUTE by_name('n7''x', '2026-01-15', false);
PREPARE by_len(text) AS SELECT count(*) FROM pairs WHERE origin = length($1);
EXPLAIN (COSTS OFF) EXECUTE by_len('abc');
EXECUTE by_len('abc');
-- a writer's: every column read, the filter of its parameter
PREPARE bump(int) AS UPDATE pairs SET t = t + 1 WHERE origin = $1;
EXPLAIN (COSTS OFF) EXECUTE bump(42);
SELECT sum(t) FROM pairs WHERE origin = 42;
EXECUTE bump(42);
SELECT sum(t) FROM pairs WHERE origin = 42;
RESET plan_cache_mode;

-- an InitPlan's value
EXPLAIN (COSTS OFF)
SELECT count(*) FROM pairs WHERE origin = (SELECT max(g) FROM generate_series(1, 42) g);
SELECT count(*) FROM pairs WHERE origin = (SELECT max(g) FROM generate_series(1, 42) g);

-- a nested loop searches the index for each outer row; a hash join agrees
CREATE TABLE wanted (origin int);
INSERT INTO wanted VALUES (3), (5), (5), (4999), (-7), (NULL);
ANALYZE wanted;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF)
SELECT w.origin, count(p.target) FROM wanted w JOIN pairs p ON p.origin = w.origin
GROUP BY w.origin ORDER BY 1;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM wanted w JOIN pairs p ON p.origin = w.origin;
SELECT w.origin, count(p.target) FROM wanted w JOIN pairs p ON p.origin = w.origin
GROUP BY w.origin ORDER BY 1;
SELECT w.origin, count(p.target) FROM wanted w LEFT JOIN pairs p ON p.origin = w.origin
GROUP BY w.origin ORDER BY 1 NULLS FIRST;
RESET enable_hashjoin;
RESET enable_mergejoin;
SET enable_nestloop = off;
SELECT w.origin, count(p.target) FROM wanted w JOIN pairs p ON p.origin = w.origin
GROUP BY w.origin ORDER BY 1;
RESET enable_nestloop;
-- a LATERAL subquery planned apart: its parameter
EXPLAIN (COSTS OFF)
SELECT w.origin, s.n FROM wanted w
CROSS JOIN LATERAL (SELECT count(*) AS n FROM pairs p WHERE p.origin = w.origin) s
ORDER BY 1 NULLS FIRST, 2;
SELECT w.origin, s.n FROM wanted w
CROSS JOIN LATERAL (SELECT count(*) AS n FROM pairs p WHERE p.origin = w.origin) s
ORDER BY 1 NULLS FIRST, 2;

-- lance partitions: each searched for the outer row
CREATE TABLE ppairs (mode smallint NOT NULL, origin int NOT NULL, target int)
  PARTITION BY LIST (mode);
CREATE TABLE ppairs_1 PARTITION OF ppairs FOR VALUES IN (1) USING lance;
CREATE TABLE ppairs_2 PARTITION OF ppairs FOR VALUES IN (2) USING lance;
INSERT INTO ppairs SELECT 1 + g % 2, g / 10, g FROM generate_series(0, 19999) g;
CREATE INDEX ppairs_1_origin ON ppairs_1 USING lance_index (origin);
CREATE INDEX ppairs_2_origin ON ppairs_2 USING lance_index (origin);
ANALYZE ppairs;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM wanted w JOIN ppairs p ON p.origin = w.origin;
SELECT count(*) FROM wanted w JOIN ppairs p ON p.origin = w.origin;
SELECT count(*) FROM wanted w JOIN ppairs p ON p.origin = w.origin AND p.mode = 2;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- a search filtered by a parameter: the filter before the search still
CREATE TABLE vitems (id int, category text, emb vector(3)) USING lance;
INSERT INTO vitems
SELECT g, 'c' || (g % 4), ARRAY[g % 7, g % 11, g % 13]::real[]::vector FROM generate_series(1, 500) g;
SET plan_cache_mode = force_generic_plan;
PREPARE nn(text, vector) AS
  SELECT id, category FROM vitems WHERE category = $1 ORDER BY emb <-> $2 LIMIT 5;
EXPLAIN (COSTS OFF) EXECUTE nn('c1', '[0,0,0]');
CREATE TEMP TABLE found AS EXECUTE nn('c1', '[0,0,0]');
SELECT count(*), bool_and(category = 'c1') FROM found;
RESET plan_cache_mode;

-- rows after the index: not among its rows until VACUUM brings it up to them
INSERT INTO pairs VALUES (99999, 1, 1, 'late', '2026-02-01', true);
SELECT (i->>'rows')::bigint AS rows FROM jsonb_array_elements(lance.indices('pairs')) i
WHERE i->>'name' = 'pairs_origin';
SELECT count(*) FROM pairs WHERE origin = 99999;
VACUUM pairs;
SELECT (i->>'rows')::bigint AS rows FROM jsonb_array_elements(lance.indices('pairs')) i
WHERE i->>'name' = 'pairs_origin';
SELECT count(*) FROM pairs WHERE origin = 99999;

DROP TABLE pairs, wanted, ppairs, vitems;

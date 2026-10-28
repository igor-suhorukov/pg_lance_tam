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

-- arrays, Lance's lists: @> and && of an array of the elements' type and
-- = ANY of an element, as array_has_all, array_has_any and array_has, which
-- a LABEL_LIST index answers.  Lance's match NULL with NULL: @> of an
-- array with a NULL, && of nulls only, and an empty array stay
-- PostgreSQL's, and so does NOT (= ANY), Lance's false where PostgreSQL's
-- is NULL.  Each qual against a heap copy: no row of it missed, none read
-- that PostgreSQL's quals remove.
CREATE TABLE tagged (id int, tags text[], nums int[], small smallint[], days date[],
  score float8[], emb vector(2)) USING lance;
INSERT INTO tagged
SELECT g,
  CASE WHEN g % 50 = 0 THEN NULL
       WHEN g % 97 = 3 THEN ARRAY['rare', 't' || g % 7]
       WHEN g % 89 = 5 THEN ARRAY['it''s']
       WHEN g % 10 = 1 THEN ARRAY['pg', NULL]
       WHEN g % 10 = 2 THEN '{}'
       ELSE ARRAY['t' || g % 7, 't' || g % 3] END,
  CASE WHEN g % 40 = 0 THEN NULL WHEN g % 40 = 1 THEN ARRAY[NULL, 3]
       ELSE ARRAY[g % 5, -(g % 4)] END,
  ARRAY[g % 6]::smallint[], ARRAY[DATE '2026-01-01' + g % 9], ARRAY[g / 10.0],
  ARRAY[g % 7, g % 11]::real[]::vector
FROM generate_series(1, 5000) g;
ANALYZE tagged;
CREATE TABLE tagged_heap AS SELECT * FROM tagged;
CREATE FUNCTION given(cond text, tab text DEFAULT 'tagged',
  OUT lance_filter text, OUT missed bigint, OUT removed bigint)
LANGUAGE plpgsql SET enable_seqscan = off AS $$
DECLARE
  plan jsonb;
  matching bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I WHERE ', tab || '_heap') || cond INTO matching;
  EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, FORMAT JSON) ' ||
    format('SELECT id FROM %I WHERE ', tab) || cond INTO plan;
  lance_filter := plan->0->'Plan'->>'Lance Filter';
  missed := matching - (plan->0->'Plan'->>'Actual Rows')::numeric;
  removed := (plan->0->'Plan'->>'Rows Removed by Filter')::numeric;
END $$;
CREATE TABLE conds (n serial, cond text);
INSERT INTO conds (cond) VALUES
  ($$tags @> ARRAY['t1']$$), ($$tags @> ARRAY['t1', 't2']$$), ($$tags @> ARRAY['pg']$$),
  ($$ARRAY['t1'] <@ tags$$), ($$tags && ARRAY['t1', NULL, 'pg']$$), ($$ARRAY['rare'] && tags$$),
  ($$NOT (tags @> ARRAY['t1'])$$), ($$NOT (tags && ARRAY['t1', 'pg'])$$),
  ($$'pg' = ANY (tags)$$), ($$'zz' = ANY (tags) OR id < 10$$), ($$tags @> ARRAY['it''s']$$),
  ($$nums @> ARRAY[1, -2]$$), ($$NOT (nums && ARRAY[3])$$), ($$3 = ANY (nums)$$),
  ($$small && ARRAY[1, 2]::smallint[]$$), ($$4 = ANY (small)$$),
  ($$days @> ARRAY[DATE '2026-01-03']$$),
  ($$(tags @> ARRAY['t1'] OR 2 = ANY (nums)) AND NOT (days && ARRAY[DATE '2026-01-02'])$$),
  ($$NOT (tags @> ARRAY['t1'] AND nums && ARRAY[0])$$),
  ($$tags @> ARRAY['pg', NULL]$$), ($$tags @> '{}'$$), ($$tags && '{NULL}'$$),
  ($$tags <@ ARRAY['t1', 't2']$$), ($$NOT ('t1' = ANY (tags))$$), ($$score @> ARRAY[0.5::float8]$$);
SELECT n, cond, g.* FROM conds, LATERAL given(cond) g ORDER BY n;
-- the same through LABEL_LIST indexes, which the plan counts on
CREATE INDEX tagged_tags ON tagged USING lance_index (tags) WITH (type = 'label_list');
CREATE INDEX tagged_nums ON tagged USING lance_index (nums) WITH (type = 'label_list');
CREATE INDEX tagged_small ON tagged USING lance_index (small) WITH (type = 'label_list');
CREATE INDEX tagged_days ON tagged USING lance_index (days) WITH (type = 'label_list');
EXPLAIN (COSTS OFF) SELECT count(*) FROM tagged WHERE tags @> ARRAY['t1'];
EXPLAIN (COSTS OFF) SELECT count(*) FROM tagged WHERE 3 = ANY (nums) AND id < 100;
SELECT n, g.lance_filter IS NOT NULL AS given, g.missed, g.removed
FROM conds, LATERAL given(cond) g ORDER BY n;
-- a generic plan's arrays: @> of one with a NULL admits no row, of an
-- empty one every list; NOT (&&) of nulls only is true
SET plan_cache_mode = force_generic_plan;
PREPARE has_all(text[]) AS SELECT count(*) FROM tagged WHERE tags @> $1;
EXPLAIN (COSTS OFF) EXECUTE has_all('{t1}');
EXECUTE has_all('{t1}');
EXECUTE has_all('{t1,NULL}');
EXECUTE has_all('{}');
EXECUTE has_all(NULL);
PREPARE has_none(text[]) AS SELECT count(*) FROM tagged WHERE NOT (tags && $1);
EXECUTE has_none('{t1,NULL}');
EXECUTE has_none('{NULL}');
EXECUTE has_none('{}');
PREPARE has_one(int) AS SELECT count(*) FROM tagged WHERE $1 = ANY (nums);
EXPLAIN (COSTS OFF) EXECUTE has_one(3);
EXECUTE has_one(3);
EXECUTE has_one(NULL);
PREPARE has_two(int, int) AS SELECT count(*) FROM tagged WHERE nums @> ARRAY[$1, $2];
EXPLAIN (COSTS OFF) EXECUTE has_two(1, -2);
EXECUTE has_two(1, -2);
EXECUTE has_two(1, NULL);
RESET plan_cache_mode;
SELECT count(*) FILTER (WHERE tags @> '{t1}') AS has_t1, count(*) FILTER (WHERE tags @> '{}') AS has_none,
  count(*) FILTER (WHERE NOT (tags && '{t1,NULL}')) AS not_t1,
  count(*) FILTER (WHERE NOT (tags && '{NULL}')) AS not_null,
  count(*) FILTER (WHERE 3 = ANY (nums)) AS any_3, count(*) FILTER (WHERE nums @> '{1,-2}') AS has_both
FROM tagged_heap;
-- a search filtered by an array: the filter before the search, the exact
-- search's k rows; fetching 10 k, the 50 nearest, would find none
EXPLAIN (COSTS OFF)
SELECT id FROM tagged WHERE tags @> ARRAY['rare'] ORDER BY emb <-> '[0,0]' LIMIT 5;
SELECT array_agg(round((emb <-> '[0,0]')::numeric, 3)) FROM
  (SELECT emb FROM tagged WHERE tags @> ARRAY['rare'] ORDER BY emb <-> '[0,0]' LIMIT 5) s;
SELECT array_agg(round((emb <-> '[0,0]')::numeric, 3)) FROM
  (SELECT emb FROM tagged_heap WHERE tags @> ARRAY['rare'] ORDER BY emb <-> '[0,0]' LIMIT 5) s;

-- timestamps: their fields are the server's timestamp[us], without a time
-- zone and with UTC, which Lance compares with a literal -- a timestamptz's
-- in UTC -- as PostgreSQL does; infinities, years past 9999 and other
-- types' values (a timestamp's for a timestamptz, which TimeZone decides,
-- a date's) stay PostgreSQL's.  Each qual against a heap copy.
CREATE TABLE stamps (id int, ts timestamp, tz timestamptz) USING lance;
INSERT INTO stamps
SELECT g,
  CASE WHEN g = 1 THEN '-infinity' WHEN g = 2 THEN 'infinity' WHEN g = 3 THEN '12000-01-01'
       WHEN g = 4 THEN '0100-06-01 BC' WHEN g % 50 = 0 THEN NULL
       ELSE TIMESTAMP '2026-01-01' + make_interval(secs => g * 3600.000123) END,
  CASE WHEN g = 1 THEN '-infinity' WHEN g = 2 THEN 'infinity' WHEN g % 40 = 0 THEN NULL
       ELSE TIMESTAMPTZ '2026-01-01 00:00:00+00' + make_interval(secs => g * 1800.000007) END
FROM generate_series(1, 2000) g;
ANALYZE stamps;
CREATE TABLE stamps_heap AS SELECT * FROM stamps;
SELECT ts::text FROM stamps WHERE id IN (1, 2, 3, 4, 10) ORDER BY id;
SELECT tz AT TIME ZONE 'UTC' FROM stamps WHERE id IN (1, 2, 10) ORDER BY id;
TRUNCATE conds RESTART IDENTITY;
INSERT INTO conds (cond) VALUES
  ($$ts = TIMESTAMP '2026-01-01 10:00:00.00123'$$),
  ($$ts <= TIMESTAMP '2026-01-01 10:00:00.00123'$$),
  ($$ts < TIMESTAMP '2026-01-01 10:00:00.00123'$$),
  ($$ts > TIMESTAMP '2026-01-01 10:00:00.001229'$$),
  ($$ts <> TIMESTAMP '2026-01-01 10:00:00.00123'$$),
  ($$TIMESTAMP '2026-01-03' >= ts$$),
  ($$ts IN (TIMESTAMP '2026-01-01 10:00:00.00123', TIMESTAMP '2026-01-01 11:00:00.001353', NULL)$$),
  ($$ts > TIMESTAMP '9000-01-01'$$), ($$ts < TIMESTAMP '0001-01-01 00:00:01'$$),
  ($$ts > TIMESTAMP '10000-01-01'$$), ($$ts > '-infinity'$$), ($$ts < 'infinity'$$),
  ($$ts BETWEEN '2026-01-02' AND '2026-01-04 12:30' OR ts IS NULL$$),
  ($$tz = TIMESTAMPTZ '2026-01-01 05:00:00.000070+00'$$),
  ($$tz >= '2026-01-03 00:00:00+03'$$),
  ($$tz BETWEEN '2026-01-02 00:00:00.5+00' AND '2026-01-04 12:00:00-05'$$),
  ($$NOT (tz < '2026-01-03 00:00:00+00') AND tz < '2026-01-05'$$),
  ($$tz IN ('2026-01-01 05:00:00.000070+00', '2026-01-01 05:30:00.000077+00')$$),
  ($$tz > '-infinity' AND tz < 'infinity'$$),
  ($$tz < TIMESTAMP '2026-01-03'$$), ($$ts >= DATE '2026-01-03'$$);
SELECT n, cond, g.* FROM conds, LATERAL given(cond, 'stamps') g ORDER BY n;
-- the same through BTREE indexes, which the plan counts on
CREATE INDEX stamps_ts ON stamps USING lance_index (ts) WITH (type = 'btree');
CREATE INDEX stamps_tz ON stamps USING lance_index (tz) WITH (type = 'btree');
EXPLAIN (COSTS OFF)
SELECT count(*) FROM stamps WHERE tz BETWEEN '2026-01-02 00:00:00+00' AND '2026-01-02 06:00:00+00';
EXPLAIN (COSTS OFF)
SELECT id FROM stamps WHERE ts IN (TIMESTAMP '2026-01-01 10:00:00.00123', TIMESTAMP '2026-01-01 11:00:00.001353');
SELECT n, g.lance_filter IS NOT NULL AS given, g.missed, g.removed
FROM conds, LATERAL given(cond, 'stamps') g ORDER BY n;
-- a generic plan's timestamptz and a stable expression of it: an infinite
-- value is no literal, and its qual PostgreSQL's
SET plan_cache_mode = force_generic_plan;
PREPARE since(timestamptz) AS SELECT count(*) FROM stamps WHERE tz > $1 - interval '1 day';
EXPLAIN (COSTS OFF) EXECUTE since('2026-01-20 00:00:00+00');
EXECUTE since('2026-01-20 00:00:00+00');
EXECUTE since('-infinity');
EXECUTE since(NULL);
RESET plan_cache_mode;
SELECT count(*) FILTER (WHERE tz > TIMESTAMPTZ '2026-01-19 00:00:00+00') AS since_19,
  count(*) FILTER (WHERE tz > '-infinity') AS finite_or_inf
FROM stamps_heap;

DROP TABLE pairs, wanted, ppairs, vitems, tagged, tagged_heap, conds, stamps, stamps_heap;
DROP FUNCTION given;

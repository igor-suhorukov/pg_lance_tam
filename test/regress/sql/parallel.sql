-- Parallel LanceScan (docs/design.md §2.8): its participants take the
-- version's fragments one at a time, each a reader of the scan's columns
-- and filter; the rows are the serial scan's
-- fragments of 1,000 rows, which no autovacuum compacts while the test runs
CREATE TABLE pt (id int, x int, t text) USING lance
  WITH (max_rows_per_file = 1000, autovacuum_enabled = false);
INSERT INTO pt SELECT g, (g * 7919) % 10007, 'r' || g FROM generate_series(1, 20000) g;
ANALYZE pt;
SELECT (lance.table_info('pt')->>'fragments')::int > 10 AS many_fragments;

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*), sum(x) FROM pt;
SELECT count(*), sum(x) FROM pt;
-- Lance's filter in each participant
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM pt WHERE x < 500;
SELECT count(*), sum(id) FROM pt WHERE x < 500;
-- a generic plan's parameter, given to the workers
PREPARE pq(int) AS SELECT count(*), sum(id) FROM pt WHERE x < $1;
SET plan_cache_mode = force_generic_plan;
EXPLAIN (COSTS OFF) EXECUTE pq(500);
EXECUTE pq(500);
EXECUTE pq(NULL);
RESET plan_cache_mode;
-- rows through Gather, in order
SELECT id, x FROM pt WHERE x < 4 ORDER BY x, id;
-- the leader alone, every fragment its own
SET parallel_leader_participation = off;
SELECT count(*), sum(x) FROM pt;
RESET parallel_leader_participation;

-- the serial scan agrees
SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*), sum(x) FROM pt;
SELECT count(*), sum(x) FROM pt;
SELECT count(*), sum(id) FROM pt WHERE x < 500;
SET max_parallel_workers_per_gather = 2;

-- a transaction that wrote the table scans it alone: its rows are its own
BEGIN;
INSERT INTO pt VALUES (0, 0, 'own');
EXPLAIN (COSTS OFF) SELECT count(*), sum(x) FROM pt;
SELECT count(*), sum(x) FROM pt;
ROLLBACK;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
DROP TABLE pt;

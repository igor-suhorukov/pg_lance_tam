-- Vector search: LanceScan's nearest-neighbour searches, with and without
-- Lance's vector indices (docs/design.md §2.8, §2.9)
CREATE TABLE items (id int NOT NULL, category text, price int, emb vector(8), hemb halfvec(8)) USING lance;
-- distinct vectors: a hash of the row's id and the dimension, in [0, 1)
CREATE FUNCTION pseudo_vec(g int) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT '[' || array_to_string(ARRAY(
           SELECT round((((g::bigint * 2654435761 + d * 40503) % 10007) / 10007.0)::numeric, 4)
           FROM generate_series(1, 8) d), ',') || ']'
$$;
INSERT INTO items
SELECT g, 'c' || (g % 5), g % 100, pseudo_vec(g)::vector, pseudo_vec(g)::halfvec
FROM generate_series(1, 3000) g;

-- the query is one of the rows: it is its own nearest
SELECT id FROM items ORDER BY emb <-> (SELECT emb FROM items WHERE id = 1234) LIMIT 1;

-- a flat search is exact: it agrees with PostgreSQL's own ordering
SET lance.enable_ann = off;
CREATE TEMP TABLE exact AS
SELECT id FROM items ORDER BY emb <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]', id LIMIT 10;
RESET lance.enable_ann;
SELECT array_agg(id) = (SELECT array_agg(id) FROM exact) AS same
FROM (SELECT id FROM items ORDER BY emb <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 10) s;

-- cosine and inner product, and halfvec
EXPLAIN (COSTS OFF) SELECT id FROM items ORDER BY emb <=> '[1,0,0,0,0,0,0,0]' LIMIT 3;
SELECT count(*) FROM (SELECT id FROM items ORDER BY emb <=> '[1,0,0,0,0,0,0,0]' LIMIT 3) s;
SELECT count(*) FROM (SELECT id FROM items ORDER BY emb <#> '[1,0,0,0,0,0,0,0]' LIMIT 3) s;
SELECT count(*) FROM (SELECT id FROM items ORDER BY hemb <-> '[1,0,0,0,0,0,0,0]' LIMIT 3) s;

-- a filter Lance decides is applied before the search: k rows come back
EXPLAIN (COSTS OFF) SELECT id FROM items WHERE category = 'c3' ORDER BY emb <-> '[0,0,0,0,0,0,0,0]' LIMIT 5;
SELECT count(*), bool_and(category = 'c3') FROM
  (SELECT id, category FROM items WHERE category = 'c3' ORDER BY emb <-> '[0,0,0,0,0,0,0,0]' LIMIT 5) s;
SELECT count(*), bool_and(price < 10) FROM
  (SELECT id, price FROM items WHERE price < 10 ORDER BY emb <-> '[0,0,0,0,0,0,0,0]' LIMIT 7) s;

-- a parameter's query vector
PREPARE nn(vector) AS SELECT id FROM items ORDER BY emb <-> $1 LIMIT 3;
SELECT pseudo_vec(77) AS q77, pseudo_vec(78) AS q78 \gset
EXECUTE nn(:'q77');
EXECUTE nn(:'q78');
DEALLOCATE nn;

-- an IVF_PQ index, Lance's: an approximate search, whose recall against
-- the exact one is measured
CREATE INDEX items_emb ON items USING lance_index (emb) WITH (type = 'ivf_pq', num_partitions = 4, num_sub_vectors = 2);
SELECT jsonb_array_length(lance.indices('items')) AS indices;
EXPLAIN (COSTS OFF) SELECT id FROM items ORDER BY emb <-> '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]' LIMIT 10;
SET lance.nprobes = 4;
-- re-ranked from 300 candidates: k-means' random start leaves 10's recall
-- at 7 one build in four
SET lance.refine_factor = 30;
SELECT count(*) >= 8 AS recall_at_10
FROM (SELECT id FROM items ORDER BY emb <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 10) a
JOIN exact USING (id);
SELECT id FROM items ORDER BY emb <-> pseudo_vec(1500)::vector LIMIT 1;
RESET lance.nprobes;
RESET lance.refine_factor;

-- rows after the index: searched without it until it is optimized
INSERT INTO items VALUES (9001, 'new', 1, '[9,9,9,9,9,9,9,9]', '[9,9,9,9,9,9,9,9]');
SELECT id FROM items ORDER BY emb <-> '[9,9,9,9,9,9,9,9]' LIMIT 1;

-- an HNSW index on the halfvec column, a scalar index, an inverted one
CREATE INDEX items_hemb ON items USING lance_index (hemb) WITH (type = 'ivf_hnsw_sq', num_partitions = 2, metric = 'l2');
CREATE INDEX items_cat ON items USING lance_index (category) WITH (type = 'bitmap');
CREATE INDEX items_price ON items USING lance_index (price);
SELECT jsonb_array_length(lance.indices('items')) AS indices;
SELECT count(*) FROM items WHERE category = 'c1';
SELECT count(*) FROM items WHERE price = 42;
DROP INDEX items_cat;
SELECT jsonb_array_length(lance.indices('items')) AS indices;

-- an index made in a transaction that rolls back is not the dataset's
BEGIN;
CREATE INDEX items_price2 ON items USING lance_index (price) WITH (type = 'btree');
ROLLBACK;
SELECT jsonb_array_length(lance.indices('items')) AS indices;

-- unknown options are refused
CREATE INDEX bad ON items USING lance_index (emb) WITH (nonsense = 1);
DROP TABLE items;
DROP FUNCTION pseudo_vec(int);

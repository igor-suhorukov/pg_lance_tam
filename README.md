# lance_tam

[Lance](https://github.com/lance-format/lance) datasets as PostgreSQL 19
tables: a table access method, `lance`, whose rows live in a Lance dataset
in a directory or in an object store (S3 and S3-compatible stores), with
Lance's nearest-neighbour search behind `ORDER BY embedding <-> $1 LIMIT k`,
Lance's own vector and scalar indexes made with `CREATE INDEX`, and
PostgreSQL's transactions over it all.  The datasets stay Lance's: Lance's
other clients read what PostgreSQL writes and write what PostgreSQL reads --
its Python package does so in the tests, beside the server.

It works on an unmodified PostgreSQL 19 core.  Lance's code runs in an
engine library built from Lance's Rust crates (`engine/`), behind a C ABI
(`include/lance_engine.h`); the extension proper is C, built by PGXS.  With
[pg_vexec](https://github.com/igor-suhorukov/pg_vexec) preloaded, the
tables have a batch source and sink, through which vexec reads Lance's
Arrow columns and writes its batches; without vexec the extension builds
and works all the same.

## Status

As of 2026-10-27, built and tested against:

| | |
|---|---|
| PostgreSQL | 19rc1: PGDG's packages (`19~rc1-1.pgdg13+1`) in Docker Hub's `postgres:19rc1-trixie`, without assertions -- the build PostGIS's 19rc1 image runs -- and the same release built from source with assertions (`lance_tam/dev:19rc1-assert`) |
| Lance | Rust crates `14.0.0-beta.19` (checkout at `dc53d5d`), linked from the checkout |
| pgvector | 0.8.7 |
| pylance | 14.0.0, for the interop leg |
| pg_vexec | the batch source and sink contracts of pg_vexec's tree (optional) |

- Every leg of `test/run.sh` passes, on both builds: `checks` (the ABI's two
  declarations agree), `engine`, `suite` (12 files: basic, types, dml,
  search, indexes, datasets, schema, concurrency, encoding, pushdown,
  parallel, inplace), `recovery`, `s3` (LocalStack), `interop` (pylance),
  `vexec`.
- PostgreSQL's own regression suite with `default_table_access_method =
  lance` ran all 239 of its files on 19beta3 with no crash, failed
  assertion or PANIC.  116 files differ from heap's expected output (160
  before the fixes below), by other plans (LanceScan in EXPLAIN, other join
  orders), by what is refused (below) and the statements after it that
  depend on it, by row order and TIDs, and by the semantics stated under
  [Transactions](#transactions-and-concurrency) and [Limits](#limits).
- On 19rc1, PGDG's build and the build with assertions alike, all 239
  files run with no crash, failed assertion or PANIC, and the same 117
  differ, the one more than on 19beta3 a new check of `sqljson_queryfuncs`
  that counts TOAST chunks, which lance tables have none of.  A check new
  to `foreign_key.sql` -- an equality function that updates the
  referenced row while the foreign key's check holds the unique index's
  page locked -- passes since UPDATEs keep TIDs in place: a lance UPDATE
  used to insert into that page, and the backend waited on its own lock.

## What it does

- **Tables over datasets**, local (`/path/to/table.lance`) or on S3
  (`s3://bucket/prefix/table.lance`): a new table makes its dataset, or
  attaches one that exists -- written by Lance's Python package, say --
  taking its columns.
- **In-place updates**: an UPDATE that changes no indexed column keeps the
  row's TID, as pg_inplace_update_am's in-place updates and heap's HOT
  updates do, so that no index needs an entry for the new version;
  `lance.update_in_place = off` gives every UPDATE a new TID.
- **All of PostgreSQL's DML**: INSERT, COPY, UPDATE, DELETE, MERGE,
  `INSERT ... ON CONFLICT`, TRUNCATE, cursors, TID scans, triggers,
  constraints, foreign keys both ways, row-level security, partitions.
- **Transactions**: a snapshot reads the dataset version its visible
  transactions committed; a transaction sees its own changes; ROLLBACK and
  savepoints undo them; a commit is one Lance version.  A server killed
  between Lance's commit and its own leaves a version no reader in the
  server sees, which the next writer undoes.
- **Vector search by Lance**: `ORDER BY v <-> q LIMIT k` (and `<=>`,
  `<#>`) over pgvector's `vector(n)` and `halfvec(n)` columns, through the
  dataset's vector index when it has one, the filter applied before the
  search when Lance can decide it.
- **Projection and filter pushdown**: LanceScan reads only the columns a
  query needs and gives Lance the quals it decides as PostgreSQL does --
  of constants, of a generic plan's parameters, of a nested loop's outer
  row, an array's `@>`, `&&` and `= ANY` among them -- and is costed by
  the dataset's BTREE, BITMAP and LABEL_LIST indexes that answer them.  A
  scan that reads many rows runs in parallel, its workers taking the
  dataset's fragments.
- **Lance's own indexes** (`lance_index`): IVF_FLAT, IVF_PQ, IVF_SQ,
  IVF_RQ, IVF_HNSW_FLAT, IVF_HNSW_PQ, IVF_HNSW_SQ; BTREE, BITMAP,
  LABEL_LIST, INVERTED (full text), NGRAM, ZONEMAP, BLOOMFILTER -- the
  tests build IVF_FLAT, IVF_PQ, IVF_HNSW_SQ, BTREE, BITMAP and LABEL_LIST;
  the others are Lance's parameters passed through.
- **PostgreSQL's own indexes** -- btree, hash, GIN, GiST, SP-GiST,
  unique and primary keys, `CREATE INDEX CONCURRENTLY`, parallel builds --
  over the rows' stable ids, valid through Lance's compaction.
- **Schema changes**: `ADD COLUMN` and `DROP COLUMN` change the dataset;
  type changes that convert no values are kept.
- **ANALYZE and VACUUM**: ANALYZE samples the dataset; VACUUM and
  autovacuum compact it, bring its indexes up to its new rows and remove
  old versions.
- **pg_vexec**, when preloaded: vexec's scans read Lance's Arrow columns
  without copies where they can, and its inserts write batches.

## Using it

```sql
-- postgresql.conf: shared_preload_libraries = 'lance_tam'
--                  lance.default_location = '/var/lib/lance'   -- or 's3://bucket/prefix'
CREATE EXTENSION vector;        -- pgvector, for vector(n) and halfvec(n) columns
CREATE EXTENSION lance_tam;

CREATE TABLE items (id bigint NOT NULL, title text, price int, embedding vector(768))
    USING lance;
INSERT INTO items SELECT ...;               -- COPY, INSERT ... SELECT, UPDATE, DELETE, MERGE
ALTER TABLE items ADD COLUMN tags text[];   -- a column of the dataset's

-- nearest neighbours, by Lance: through the vector index when there is one,
-- the filter applied before the search
SELECT id, title FROM items
WHERE price < 100
ORDER BY embedding <=> $1
LIMIT 10;

-- Lance's own indexes of the dataset
CREATE INDEX ON items USING lance_index (embedding)
    WITH (type = 'ivf_hnsw_sq', metric = 'cosine', num_partitions = 256);
CREATE INDEX ON items USING lance_index (price) WITH (type = 'btree');
-- and PostgreSQL's own, over the rows' stable ids
CREATE UNIQUE INDEX ON items (id);
```

Without `uri`, a table's dataset is made under `lance.default_location`
(default: `pg_lance` in the data directory) as
`<database>/<schema>/<table>_<oid>.lance`, and DROP TABLE removes it.

### On S3

Endpoint and region are a storage server's options, credentials a user
mapping's -- never a table's options, which every user can read:

```sql
CREATE SERVER s3 FOREIGN DATA WRAPPER lance_storage
    OPTIONS (aws_region 'us-east-1');     -- aws_endpoint, allow_http for MinIO and the like
CREATE USER MAPPING FOR PUBLIC SERVER s3
    OPTIONS (aws_access_key_id '...', aws_secret_access_key '...');

CREATE TABLE docs (id int NOT NULL, body text, emb vector(384))
    USING lance WITH (uri = 's3://my-bucket/docs.lance', server = 's3');
```

Without a server, Lance's default credential chains apply (the server
process's environment, instance profiles).  `lance.default_server` names
the server of a table that names none.

### A dataset that exists

A table of no columns over a dataset takes the dataset's columns:

```sql
CREATE TABLE events () USING lance WITH (uri = 's3://my-bucket/events.lance', server = 's3');
CREATE TABLE live () USING lance
    WITH (uri = '/data/live.lance', read_only = true, follow = true);  -- reads Lance's latest version
SELECT lance.refresh('events');    -- take other writers' commits into the table
```

A dataset without stable row ids is read-only.  A writable table's commits
land on top of other writers' commits; `lance.refresh()` takes them into
the table, and says when PostgreSQL's indexes over it need a `REINDEX`.

### Options of a table

| Option | |
|---|---|
| `uri` | the dataset: a path, or `s3://bucket/prefix/name.lance` and Lance's other object store schemes |
| `server` | a `lance_storage` server: endpoint, region, credentials |
| `managed` | DROP TABLE removes the dataset; the default without `uri` |
| `read_only` | refuse writes |
| `follow` | with `read_only`: read Lance's latest version at each statement |
| `storage_version` | Lance's file format for a new dataset (`stable`, `2.1`, ...) |
| `max_rows_per_file`, `max_rows_per_group`, `buffer_rows` | Lance's write parameters |
| `compression`, `compression_level` | Lance's general compression of every column: `lz4`, `zstd`, `fsst` or `none`, and its level |
| `column_encoding` | Lance's encodings of columns, a JSON object: `'{"ids": {"compression": "zstd", "compression-level": 3}}'` |
| any other | a storage option passed to Lance; a name with `secret`, `password`, `token` or `credential` in it is refused |

PostgreSQL's own storage parameters (`autovacuum_enabled`,
`parallel_workers`, `fillfactor`, `toast.*`, ...) stay PostgreSQL's, as for
heap, and `ALTER TABLE ... SET (...)` changes them; a lance option is set
when the table is created.

### Column encodings

Lance picks each column's encoding itself -- bit packing, run lengths,
byte stream split, dictionaries, FSST -- and reads its fields'
`lance-encoding:` metadata for what to do otherwise.  `compression` and
`compression_level` give every column of a new table that metadata,
`column_encoding` a column its own over them, with Lance's keys:
`compression`, `compression-level`, `rle-threshold`, `minichunk-size`,
`dict-divisor`, `dict-size-ratio`, `dict-values-compression`,
`dict-values-compression-level`, `packed`, `structural-encoding`, `bss`
(the `lance-encoding:` prefix optional).  A column ADD COLUMN adds takes
the table's encodings; the encodings apply to every version written after,
compaction's included.  `lance.schema(rel)` shows the fields' metadata.

```sql
CREATE TABLE pairs (origin_id int, target_deltas int[], time_s smallint[])
    USING lance WITH (compression = 'lz4',
                      column_encoding = '{"target_deltas": {"compression": "zstd"}}');
```

Lance's general compression wraps fixed-width values and strings, after
their bit packing; a `vector(n)` or `halfvec(n)` column, a fixed-size
list, Lance 2.2 and 2.3 store as it is, whatever the metadata says.

### Vector search

LanceScan answers `ORDER BY v <op> q LIMIT k` over one relation, `v` a
`vector(n)` or `halfvec(n)` column and `q` known when the scan starts (a
constant or a parameter): `<->` is L2, `<=>` cosine, `<#>` dot.  With every
qual decided by Lance the filter runs first and the search yields `k` rows;
else it fetches `10 k` and PostgreSQL filters them.  `lance.nprobes`,
`lance.ef` and `lance.refine_factor` tune the search; `lance.enable_ann =
off` makes it exact.

A `lance_index` takes `type` and Lance's parameters: `metric`
(`l2`, `cosine`, `dot`, `hamming`), `num_partitions`,
`target_partition_size`, `num_sub_vectors`, `num_bits`, `max_iterations`,
`sample_rate`, `m`, `ef_construction`, and for INVERTED and NGRAM
`base_tokenizer`, `language`, `lower_case`, `stem`, `remove_stop_words`,
`ascii_folding`, `max_token_length`, `with_position`, `min_ngram_length`,
`max_ngram_length`, `prefix_only`.  Defaults: IVF_PQ for a vector column,
BTREE otherwise.  The index is built when CREATE INDEX runs and committed
with the transaction; rows added later are searched without it until
VACUUM or `lance.optimize()` brings it up to them.

### Filters and scalar indexes

LanceScan gives Lance the quals it decides as PostgreSQL does --
comparisons of integer, boolean, date, timestamp, timestamptz and text
columns, `IN` lists, `IS [NOT] NULL`; of arrays of those types but the
timestamps, `@>` and `&&` with an array and `= ANY` of an element, Lance's
`array_has_all`, `array_has_any` and `array_has`; and `AND`, `OR`, `NOT`
of them -- with constants, or with values known when the scan starts: a
generic plan's parameters, an InitPlan's result, stable expressions of
them (`now() - interval '1 day'`), a nested loop's outer row.  Such a
filter is made again each time the scan starts.  A value Lance cannot take
as a literal of its column (`smallint_col < 100000`, an infinite or
year-10000 timestamp) leaves its qual to PostgreSQL; a comparison with NULL
reads nothing.  A timestamp's qual is given where its column is the
`timestamp[us]` the server writes, with UTC for a timestamptz: an attached
dataset's other units and time zones keep it PostgreSQL's, since Lance
would truncate the literal to milliseconds or read it in the field's zone.
Lance's array functions match a NULL element with NULL, which PostgreSQL's
never do: `@>` of an array with a NULL and either operator with an empty
array stay PostgreSQL's, `&&` is given its array without the NULLs, and `x
= ANY (col)` is not given under a `NOT`.

The planner costs a LanceScan as Lance runs it.  A BTREE or BITMAP
`lance_index` that answers a qual -- equality, ranges, `IN`, `IS NULL`, ORs
of them -- or a LABEL_LIST one an array's `@>`, `&&` or `= ANY`, costs its
searches and the reads of the rows it finds, and the rows it does not
cover yet are read with the filter; without one, the filtered read.  For a
join clause such an index answers, a LanceScan parameterized by the outer
relation is offered, so that a nested loop searches the index for each
outer row; `lance.scan_start_cost` is what each of those scans costs to
start.

```
CREATE INDEX pairs_origin ON pairs USING lance_index (origin_id) WITH (type = 'btree');
PREPARE q(int) AS SELECT count(*) FROM pairs WHERE origin_id = $1;
EXPLAIN (COSTS OFF) EXECUTE q(100000);   -- a generic plan
 Aggregate
   ->  Custom Scan (LanceScan) on pairs
         Filter: (origin_id = $1)
         Lance Columns: origin_id
         Lance Filter: (`origin_id` = $1)
         Lance Index: pairs_origin
```

On 24 million rows of `(origin_id int, target_id int, time_s smallint,
distance_m smallint)`, LZ4, 8 CPUs: one `origin_id` (about 550 rows) took
300 ms as the Parallel Seq Scan planned before LanceScan was costed by its
filter; it takes 25 ms by Lance's filter alone, 0.8-1.3 ms through the
BTREE, 0.4 ms as a generic plan's `$1`.  A join of 100 origins, a nested
loop, takes 34 ms (346 ms by a hash join); from about 1,000 origins the
planner takes the hash join, 374 ms against the nested loop's 599 ms.

### Parallel scans

A LanceScan that no index answers is planned in parallel too: a Gather's
workers and its leader take the dataset's fragments one at a time, each
read with the scan's columns and Lance's filter, all of them at the
version the first took.  Workers are counted by the table's pages, as for
a parallel SeqScan, and no more than the fragments; a table the
transaction has written is scanned by no worker.  On the 24 million rows
above (24 fragments, 84 MB), the best of three runs, in ms:

| | serial | parallel, 2 / 4 / 6 workers | the access method's Parallel Seq Scan, 2 / 4 / 6 |
|---|---:|---:|---:|
| `sum(time_s)` | 629 | 246 / 160 / 136 | 328 / 210 / 177 |
| three sums, 3 of the 4 columns | 1093 | 423 / 277 / 229 | 409 / 269 / 225 |
| `WHERE time_s <= 900` (20% of the rows) | 101 | 52 / 40 / 37 | 310 / 208 / 171 |
| `WHERE origin_id = 100000`, no index | 25 | 21 / 18 / 19 | 300 / 199 / 165 |
| `GROUP BY origin_id` | 1595 | 596 / 381 / 318 | 613 / 409 / 336 |

With `max_parallel_workers_per_gather = 2` the planner takes the parallel
LanceScan for all five.  It is 2.5 to 5 times the serial LanceScan where
PostgreSQL's work on each row dominates; reading few columns, a quarter
faster than the Parallel Seq Scan the planner took before, and as fast
reading most of them; with Lance's filter, 6 to 15 times.  A point lookup
gains little: Lance's filter already runs on its threads.

### Functions

| | |
|---|---|
| `lance.table_info(rel)` | jsonb: uri, version, latest version, managed, writable, rows, deleted rows, fragments, data bytes, stable row ids |
| `lance.schema(rel)` | jsonb: the dataset's fields, each with its Arrow format, nullability, metadata and children |
| `lance.versions(rel)` | the dataset's versions: version, timestamp, metadata |
| `lance.indices(rel)`, `lance.fragments(rel)` | jsonb; an index's `rows` are the live rows of the fragments it covers |
| `lance.optimize(rel, options)` | compaction and index optimization now (`target_rows_per_fragment`, `materialize_deletions`, `compact`, `indices`) |
| `lance.cleanup(rel, older_than)` | Lance's removal of old versions |
| `lance.refresh(rel)` | the table becomes its dataset's latest version |
| `lance.explain(rel, filter, verbose)` | Lance's plan of a scan |
| `lance.engine_version()` | the engine library's and Lance's versions |

### Settings

| Setting | Default | |
|---|---|---|
| `lance.default_location` | `pg_lance` in the data directory | a directory or `s3://bucket/prefix` |
| `lance.default_server` | | the storage server of a table without one |
| `lance.engine_library` | `$libdir/liblance_engine.so` | |
| `lance.worker_threads` | 4 | a backend's engine threads |
| `lance.index_cache_mb`, `lance.metadata_cache_mb` | 512, 128 | a backend's Lance caches |
| `lance.batch_rows`, `lance.readahead` | 8192, 4 | a scan's batches, and how many are decoded ahead |
| `lance.buffer_rows` | 131072 | rows held before a fragment is written |
| `lance.nprobes`, `lance.ef`, `lance.refine_factor` | Lance's | vector search |
| `lance.enable_customscan`, `lance.enable_ann`, `lance.enable_filter_pushdown` | on | LanceScan, its searches, its filters |
| `lance.scan_start_cost` | 100 | the planner's cost of starting a Lance scan, which each scan of a nested loop's parameterized LanceScan pays |
| `lance.update_in_place` | on | an UPDATE that changes no indexed column keeps the row's TID |
| `lance.scan_in_order` | on | rows in the order they were written, as heap's mostly are |
| `lance.vacuum_compact` | on | VACUUM compacts the dataset |
| `lance.version_retention` | 3600 s | the versions VACUUM keeps; -1 keeps all |

## Types

| PostgreSQL | Arrow, the dataset's column |
|---|---|
| boolean | bool |
| smallint, integer, bigint | int16, int32, int64 |
| real, double precision | float32, float64 |
| numeric(p, s), p <= 38 | decimal128(p, s) |
| text, varchar, char | utf8 |
| bytea | binary |
| date | date32 |
| time | time64[us] |
| timestamp, timestamptz | timestamp[us], without and with UTC |
| uuid | fixed_size_binary(16) |
| vector(n), halfvec(n) (pgvector) | fixed_size_list<float32 \| float16, n> |
| a one-dimensional array of the above | list<element> |
| anything else (json, jsonb, interval, inet, enums, unconstrained numeric, name, composites, ...) | utf8, the value's text form |

`-infinity` and `infinity` dates and timestamps are stored as their types'
extremes, ordered below and above every other value in Lance's filters as
in PostgreSQL.  Refused rather than mangled: a decimal's NaN,
multidimensional arrays, a timestamp from 294247-01-10 on, text that is not
UTF-8.  A vector column declares its dimension.  Every column of a dataset
the server makes is nullable: PostgreSQL keeps NOT NULL.  Reading, an
attached dataset's `large_utf8`, `utf8_view`, dictionary or narrower
integer columns are cast to the table's types.

## Transactions and concurrency

A snapshot reads the version of the dataset that the last transaction
visible to it committed, through a catalog row (`lance.tables`) that
PostgreSQL's MVCC versions; a transaction sees its own uncommitted rows and
deletions; ROLLBACK and savepoints take them back.  A transaction's changes
become one Lance commit when it commits, tagged with the transaction; a
commit that PostgreSQL then aborts, or that a crash leaves behind, is undone
by the next writer.

Rows are not locked: two transactions that change the same row both
proceed, and the one that commits second fails with a serialization failure
(SQLSTATE 40001) -- retry it.  Writes to different rows never conflict.  A
table with a unique index lets one writer at a time.  `PREPARE TRANSACTION`
of a transaction that changed a lance table is refused.

## Schema changes

- **`ALTER TABLE ... ADD COLUMN`** adds the column to the dataset when the
  statement ends, one Lance version, each row the table has taking the
  default -- a column of NULLs is the manifest's change alone, a constant
  default is written once per fragment.  `CHECK` and `NOT NULL` over the new
  column see the rows' values.  In a transaction the column comes after its
  earlier changes, which are committed first; a ROLLBACK, or a savepoint's,
  undoes it.  A volatile default (`random()`, a sequence) would rewrite the
  table and is refused.
- **`ALTER TABLE ... DROP COLUMN`** drops the dataset's column as well, for
  every reader of the dataset, not only for PostgreSQL -- the manifest's
  change alone, the data staying until compaction; its name is free again.
  The last column of a table cannot be dropped.
- **`ALTER COLUMN ... TYPE`** is kept when PostgreSQL makes it in its
  catalog alone and the dataset's column keeps its Arrow type
  (`varchar(10)` to `varchar(30)` or `text`, a domain over the type); a
  change that converts values (`integer` to `bigint`) is refused.
- Renames, defaults, comments, constraints, triggers, identity, owners and
  `DROP NOT NULL` are PostgreSQL's alone.  A virtual generated column has
  no column in the dataset; a stored one has.
- `CREATE INDEX CONCURRENTLY` and `REINDEX CONCURRENTLY` work for
  PostgreSQL's own indexes; a `lance_index` is built in one transaction.
- Materialized views are refused under lance -- one that names it, and one
  that lance is only `default_table_access_method` for -- since `REFRESH`
  rewrites them: create them `USING heap`.

## Performance

One run of `test/run.sh bench`, in the dev container on the build machine,
`shared_buffers = 1GB`, no parallel workers: 200,000 rows of `(id bigint,
cat int, label text, emb vector(128))`, the vectors on a surface of 4
dimensions so that nearest neighbours are well defined, as embeddings'
are; each query run twice, the second time given.  Heap's vector indexes
are pgvector's.

| | heap + pgvector | lance |
|---|---|---|
| `INSERT ... SELECT` 200,000 rows | 149 ms | 124 ms |
| `sum(cat)` | 9.2 ms | 5.7 ms |
| `sum(length(label))` | 14.2 ms | 12.5 ms |
| `count(*) WHERE cat = 7` | 8.7 ms | 0.9 ms (Lance's filter) |
| exact 10 nearest, no index | 46.6 ms | 19.3 ms (Lance's flat search) |
| vector index build | HNSW 8.3 s; IVFFlat (256 lists) 0.57 s | IVF_PQ (256 partitions, 16 sub-vectors) 3.8 s; IVF_HNSW_SQ (16 partitions) 1.4 s |
| 10 nearest through the index | HNSW 0.19 ms, recall 10/10 | IVF_PQ 2.3 ms, 10/10; IVF_HNSW_SQ 1.4 ms, 10/10 |
| `UPDATE` 2,000 rows | 381 ms (the HNSW index's inserts) | 70 ms |
| `DELETE` 2,000 rows | 15 ms | 32 ms |
| `VACUUM` after them | 11.0 s (HNSW) | 2.0 s (compaction, the index brought up to date) |
| size | 272 MB with its index | 99 MB of data files |

A search through a vector index is slower than pgvector's HNSW because each
Lance scan costs about a millisecond to set up.

## Limits

- Refused, because the dataset would be left behind the relation: what
  rewrites a table -- a volatile default, a type change that converts
  values, `VACUUM FULL`, `CLUSTER`, `REPACK`, `SET LOGGED/UNLOGGED`, `SET
  ACCESS METHOD` -- BRIN indexes (Lance's zone maps do their work), a
  `lance_index` built `CONCURRENTLY`, materialized views,
  tables of no columns, two-phase commit.
- Rows are not locked: `SELECT ... FOR UPDATE` and UPDATE do not wait for
  a concurrent writer of the row; the later commit fails with 40001, under
  every isolation level.
- `xmin` of a row the transaction inserted is the top transaction's, of
  any other row frozen; `cmin` and `cmax` are 0.  An UPDATE that changes no
  indexed column keeps the row's TID; one that changes one, or that an
  AFTER ROW trigger would refetch by TID, gives it a new one.  TIDs are
  the rows' stable ids, from block 2^31 up.
- Backward scans: a cursor over LanceScan is materialized when declared
  `SCROLL` (and is then not the scan `WHERE CURRENT OF` finds), and refuses
  `FETCH BACKWARD` otherwise.
- A composite value is stored as its text form: once `ALTER TYPE` adds an
  attribute to its type, the stored values no longer read.
- The dataset's files are not WAL-logged: a physical standby reads a
  dataset on shared storage (S3) at the versions the replicated catalog
  names, but has no copy of one on the primary's disk; `pg_basebackup` does
  not copy a dataset consistently.  `pg_dump` dumps a lance table's rows,
  which restore into a managed table at the default location.
- Each backend has its own Lance caches: a vector index is read into each
  backend that searches it.
- Unverified: object stores other than S3's API.

## What PostgreSQL's regression suite found

Running PostgreSQL's whole suite over lance tables found these, each now
fixed and kept by a test of `test/regress`:

- a parallel index build indexed every row in every worker;
- ANALYZE failed on any table of more than 30,000 rows (its sample's TIDs),
  and its block sampling of a large table returned one row;
- an `ON COMMIT DELETE ROWS` temporary table with an index crashed at
  commit;
- an array of a domain's values crashed an insert;
- text that is not UTF-8 made a column unreadable to every reader;
- a filter literal outside its column's type (`int2col < 100000`) broke
  the query;
- `WHERE CURRENT OF`, `ctid`/`xmin`/`cmin` in a LanceScan, and the integer
  table options did not work;
- NOT NULL broke writes after `DROP COLUMN` and on virtual generated
  columns;
- a table made and filled in one transaction was planned as empty: a
  self-join of 20,000 such rows took 23 s as a nested loop, and takes 5 ms.

## Decisions

Decided: `DROP COLUMN` drops the column for every reader of the dataset;
materialized views are refused, even when lance is only the default access
method.

Open:

1. **Table rewrites** could be one Lance `Overwrite` of the dataset with the
   new schema, PostgreSQL's transient table's rows written to the table's
   own dataset.  Build it, or keep refusing?
2. **Row locks**: wait for a concurrent writer of a row, as heap's READ
   COMMITTED does, or keep failing the later commit with 40001?

## Building and testing

Everything builds and runs in containers (`docker/`, `test/run.sh`); the
host needs Docker with Compose v2, and Lance's checkout beside this one
(`LT_LANCE_SRC`, default `~/dev/projects/oss_databases/github/lance`).
Each run's results go to `~/.cache/pg_vexec/lance_tam/runs/<time>-<leg>`.
The dev image is Docker Hub's `postgres:19rc1-trixie` with PGXS at the
server's own version; `LT_PG_TAG` names another tag of that image (a later
19 release), `LT_UID` the uid the server runs as (the host user's, 1000).
`LT_DEV_IMAGE=lance_tam/dev:19rc1-assert` runs a leg on PostgreSQL built
from the same release with `--enable-cassert` (`docker/Dockerfile.assert`).

```
test/run.sh images     # the engine builder and the dev image (postgres:19rc1-trixie, pgvector, pylance)
test/run.sh assert-image  # the dev image with assertions: 19rc1 built from its source release
test/run.sh fetch      # cargo fetch: the one step that reaches the network
test/run.sh engine     # liblance_engine.so, into ~/.cache/pg_vexec/lance_tam/engine/lib
test/run.sh checks     # the C ABI's two declarations agree
test/run.sh suite      # the regression suite (test/regress)
test/run.sh recovery   # a writer killed between Lance's commit and its own
test/run.sh s3         # tables in an S3 bucket (LocalStack)
test/run.sh interop    # Lance's Python package reading and writing the same datasets
test/run.sh vexec      # pg_vexec's batch source and sink (LT_pg_vexec: pg_vexec's tree)
test/run.sh pgregress  # PostgreSQL's own suite with default_table_access_method = lance
test/run.sh bench      # lance against heap and pgvector: the table above
test/run.sh repro      # a scratch script: LT_RUN=<dir> holding repro1.sql
test/run.sh all        # checks, engine, suite, recovery, s3, interop, vexec
```

To install elsewhere: build the engine library (`cargo build --release` in
`engine/`, with Lance's checkout at `/lance` or `engine/Cargo.toml`'s paths
changed), then `make && make install` with your PostgreSQL 19's
`pg_config`, and copy `liblance_engine.so` to `$(pg_config --pkglibdir)`.
Preload the module (`shared_preload_libraries = 'lance_tam'`) for its
options and DDL handling, and for pg_vexec's source and sink.

## Licence

Apache-2.0.  Lance is Apache-2.0, and is linked, not copied.

-- Encodings: compression, compression_level and column_encoding as the
-- "lance-encoding:" metadata of the dataset's fields (docs/design.md §2.5)
CREATE TABLE enc_plain (id int, ids int[]) USING lance;
SELECT f->>'name' AS name, f->>'format' AS format, f ? 'metadata' AS has_metadata,
       f#>'{children,0}' ? 'metadata' AS item_has_metadata
FROM jsonb_array_elements(lance.schema('enc_plain')) f;

-- every column takes compression and compression_level, a column
-- column_encoding names its own entries over them, the prefix optional
CREATE TABLE enc (id int, ids int[], t smallint[], label text, emb vector(3))
    USING lance WITH (compression = 'lz4', compression_level = 1,
                      column_encoding = '{"ids": {"compression": "zstd", "compression-level": 3},
                                          "t": {"lance-encoding:rle-threshold": "0.9", "bss": "off"}}');
SELECT f->>'name' AS name, f->'metadata' AS metadata, f#>'{children,0,metadata}' AS item_metadata
FROM jsonb_array_elements(lance.schema('enc')) f;

-- what is written reads back
INSERT INTO enc SELECT g, ARRAY[g, g + 1, g + 2], ARRAY[g % 7, g % 5]::smallint[], 'label ' || g,
                       format('[%s,%s,%s]', g, g * 2, g * 3)::vector(3)
FROM generate_series(1, 5000) g;
SELECT count(*), sum(ids[3]), sum(t[1]), max(label), sum(emb[1]::float8) AS emb_first FROM
    (SELECT id, ids, t, label, emb::real[] AS emb FROM enc) x;
SELECT * FROM enc WHERE id = 4321;
VACUUM enc;
SELECT count(*), sum(ids[1]) FROM enc;

-- a column ALTER TABLE adds takes the table's encodings too
ALTER TABLE enc ADD COLUMN extra int[];
SELECT f->'metadata' AS metadata FROM jsonb_array_elements(lance.schema('enc')) f
WHERE f->>'name' = 'extra';
UPDATE enc SET extra = ARRAY[id] WHERE id <= 10;
SELECT count(extra), sum(extra[1]) FROM enc;

-- refused
CREATE TABLE enc_bad (id int) USING lance WITH (compression = 'gzip');
CREATE TABLE enc_bad (id int) USING lance WITH (compression_level = 'high');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '[1]');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '{"id": "lz4"}');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '{"id": {"colour": "red"}}');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '{"id": {"compression": null}}');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '{"id": {"compression": "brotli"}}');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = '{"nope": {"compression": "lz4"}}');
CREATE TABLE enc_bad (id int) USING lance WITH (column_encoding = 'not json');

DROP TABLE enc, enc_plain;

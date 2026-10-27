-- Every kind of column, round trip, NULLs included (docs/design.md §2.5)
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE typed (
    id int NOT NULL,
    b boolean,
    i2 smallint,
    i4 integer,
    i8 bigint,
    f4 real,
    f8 double precision,
    n numeric(12, 3),
    n38 numeric(38, 10),
    nu numeric,
    t text,
    vc varchar(10),
    bc char(4),
    nm name,
    by bytea,
    d date,
    tm time,
    ts timestamp,
    tz timestamptz,
    u uuid,
    j json,
    jb jsonb,
    iv interval,
    ip inet,
    m mood,
    ai int[],
    at text[],
    af float8[],
    ad date[],
    v vector(4),
    h halfvec(2)
) USING lance;

INSERT INTO typed VALUES
 (1, true, -32768, -2147483648, -9223372036854775808, 1.5, -2.25, 123456789.123, 12345678901234567890.0123456789, 1e100,
  'héllo wörld', 'abc', 'xy', 'nm', '\x00ff10', '2024-02-29', '23:59:59.999999', '1999-12-31 23:59:59.5',
  '2024-06-01 12:00:00+02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1, 2]}', '{"b": {"c": null}}',
  '1 year 2 mons 3 days 04:05:06', '192.168.0.1/24', 'happy', '{1,NULL,3}', '{"x",NULL,"z"}', '{1.5,NaN}',
  '{2000-01-01,1970-01-01}', '[1,2,3,4]', '[0.5,-1]'),
 (2, false, 32767, 2147483647, 9223372036854775807, '-Infinity', 'NaN', -0.001, -1.5, -0.0,
  '', '', '', '', '\x', '1970-01-01', '00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
  '00000000-0000-0000-0000-000000000000', '[]', '[]', '-1 day', '::1', 'sad', '{}', '{}', '{}', '{}',
  '[0,0,0,0]', '[0,0]'),
 (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT id, b, i2, i4, i8, f4, f8, n, n38, nu FROM typed ORDER BY id;
SELECT id, t, vc, bc, nm, by, d, tm, ts FROM typed ORDER BY id;
SELECT id, tz AT TIME ZONE 'UTC' AS tz_utc, u, j, jb, iv, ip, m FROM typed ORDER BY id;
SELECT id, ai, at, af, ad, v, h FROM typed ORDER BY id;

-- what the dataset's columns are
SELECT jsonb_array_length(lance.fragments('typed')) > 0 AS has_fragments;

-- infinities round trip, and order below and above every other value, in
-- Lance's filters as in PostgreSQL's
INSERT INTO typed (id, d, ts, tz, ad) VALUES
 (4, 'infinity', '-infinity', 'infinity', '{-infinity,2000-01-01}'),
 (5, '-infinity', 'infinity', '-infinity', '{infinity}');
SELECT id, d, ts, tz, ad FROM typed WHERE id >= 4 ORDER BY id;
SELECT id FROM typed WHERE d > '2020-01-01' ORDER BY id;
SELECT id FROM typed WHERE d < '1980-01-01' ORDER BY id;
SELECT id, ts > '2020-01-01' AS later, tz < '2020-01-01' AS earlier FROM typed WHERE id >= 4 ORDER BY id;
DELETE FROM typed WHERE id >= 4;

-- over-wide values are refused, not mangled
INSERT INTO typed (id, ts) VALUES (4, '294270-01-01 00:00:00');
INSERT INTO typed (id, n) VALUES (4, 'NaN');
INSERT INTO typed (id, ai) VALUES (4, '{{1,2},{3,4}}');
INSERT INTO typed (id) VALUES (NULL);
SELECT count(*) FROM typed;

-- a vector's dimension must be declared
CREATE TABLE novec (v vector) USING lance;

-- an array of a domain's values stays one
CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE TABLE pit (f posint[]) USING lance;
INSERT INTO pit VALUES (ARRAY[42]), ('{1,2}');
INSERT INTO pit VALUES (ARRAY[-1]);
SELECT f, f = ARRAY[42]::posint[] AS is42, pg_typeof(f) FROM pit ORDER BY f;
DROP TABLE pit;
DROP DOMAIN posint;
DROP TABLE typed;
DROP TYPE mood;

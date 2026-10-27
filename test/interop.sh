#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The interop leg (docs/design.md §6): Lance's Python package (pylance, in
# the dev image) reads the datasets the server writes, writes datasets the
# server attaches, and appends to one the server writes too.
set -euo pipefail
. "$LT_SRC/test/server.sh"
out="$LT_WORK/interop.out"
py() { /opt/pylance/bin/python -I "$LT_SRC/test/interop.py" "$@" 2>&1 | grep -v -i warn; }
q() { psql -X -q -At -d lance_interop -c "$1" 2>&1 || true; }

createdb lance_interop
{
	q "CREATE EXTENSION vector"
	q "CREATE EXTENSION lance_tam"
	q "CREATE TABLE t (id bigint NOT NULL, name text, emb vector(3)) USING lance"
	q "INSERT INTO t SELECT g, 'pg ' || g, ARRAY[g % 3 = 0, g % 3 = 1, g % 3 = 2]::int[]::real[]::vector FROM generate_series(1, 100) g"
	q "DELETE FROM t WHERE id > 90"
	uri="$(q "SELECT lance.table_info('t')->>'uri'")"
	echo "== PostgreSQL wrote 90 rows"
	py read "$uri"

	echo "== Python appends: PostgreSQL sees the rows after lance.refresh()"
	py append "$uri"
	echo "pg rows before refresh: $(q 'SELECT count(*) FROM t')"
	q "SELECT lance.refresh('t') > 0"
	echo "pg rows after refresh: $(q 'SELECT count(*) FROM t')"
	echo "pg rows from python: $(q "SELECT string_agg(id::text, ',' ORDER BY id) FROM t WHERE name LIKE 'from python%'")"
	echo "pg nearest to [9,9,9]: $(q "SELECT id FROM t ORDER BY emb <-> '[9,9,9]' LIMIT 1")"

	echo "== PostgreSQL writes on top of Python's commit"
	q "UPDATE t SET name = 'updated by pg' WHERE id = 1001"
	q "INSERT INTO t VALUES (2000, 'pg again', '[1,1,1]')"
	py count "$uri"
	echo "pg rows: $(q 'SELECT count(*) FROM t')"

	echo "== a Python index, used by PostgreSQL's filter"
	py index "$uri"
	q "SELECT lance.refresh('t') > 0"
	echo "pg indices: $(q "SELECT string_agg(i->>'name', ',' ORDER BY i->>'name') FROM jsonb_array_elements(lance.indices('t')) i")"
	echo "pg by id: $(q 'SELECT name FROM t WHERE id = 1001')"

	echo "== a column PostgreSQL adds, Python reads"
	q "ALTER TABLE t ADD COLUMN tag text DEFAULT 'old'"
	q "INSERT INTO t (id, name, emb, tag) VALUES (3000, 'tagged', '[2,2,2]', 'new')"
	py columns "$uri"

	echo "== a dataset Python made, attached"
	py create "$LT_WORK/py_made.lance"
	q "CREATE TABLE pym () USING lance WITH (uri = '$LT_WORK/py_made.lance')"
	psql -X -q -d lance_interop -c '\d pym' | sed -n '3,6p'
	echo "pg reads: $(q "SELECT string_agg(k || ':' || coalesce(tag, '-') || ':' || score, ' ' ORDER BY k) FROM pym")"
	q "UPDATE pym SET score = score * 10 WHERE k = 2"
	q "INSERT INTO pym VALUES (4, 'd', 4.5)"
	echo "pg after writes: $(q "SELECT string_agg(k || ':' || score, ' ' ORDER BY k) FROM pym")"
	py count "$LT_WORK/py_made.lance"

	echo "== a dataset without stable row ids: read-only"
	py plain "$LT_WORK/py_plain.lance"
	q "CREATE TABLE pyp () USING lance WITH (uri = '$LT_WORK/py_plain.lance')"
	echo "pg reads: $(q 'SELECT sum(k) FROM pyp')"
	q "INSERT INTO pyp VALUES (9)" | head -1
	q "DROP TABLE pym, pyp"
	echo "python still has it: $(py count "$LT_WORK/py_made.lance")"
} >"$out" 2>&1
sed -e "s|$LT_WORK|WORK|g" "$out" >"$out.sed"
cat "$out.sed"
diff -u "$LT_SRC/test/interop.expected" "$out.sed"

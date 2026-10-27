#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The recovery leg (docs/design.md §2.7): a backend killed between its
# Lance commit and its own commit leaves a version in the dataset that the
# catalog never names.  After the server's restart, readers read the
# catalog's version, and the next writer undoes the killed transaction's
# version before it commits, so the dataset's latest version never holds
# the killed transaction's rows.
set -euo pipefail
. "$LT_SRC/test/server.sh"
out="$LT_WORK/recovery.out"
q() { psql -X -q -At -d lance_recovery -c "$1" 2>&1; }

createdb lance_recovery
{
	q "CREATE EXTENSION lance_tam"
	q "CREATE TABLE r (id int NOT NULL, v text) USING lance"
	q "INSERT INTO r SELECT g, 'v' || g FROM generate_series(1, 10) g"
	echo "rows before: $(q 'SELECT count(*) FROM r')"
	uri="$(q "SELECT lance.table_info('r')->>'uri'")"
	q "CREATE TABLE r_latest () USING lance WITH (uri = '$uri', read_only = true, follow = true)"

	# the killed writer
	psql -X -q -d lance_recovery -c "SET lance.debug_fail_after_commit = 2; INSERT INTO r VALUES (11, 'killed')" >/dev/null 2>&1 || true
	for _ in $(seq 1 60); do pg_isready -q && q "SELECT 1" >/dev/null && break; sleep 1; done
	echo "server back: $(q 'SELECT 1')"
	echo "rows the catalog names: $(q 'SELECT count(*) FROM r')"
	echo "rows of Lance's latest version: $(q 'SELECT count(*) FROM r_latest')"

	# the next writer undoes the killed transaction's version first
	q "INSERT INTO r VALUES (12, 'next')"
	echo "rows after the next commit: $(q 'SELECT count(*) FROM r')"
	echo "rows of Lance's latest version: $(q 'SELECT count(*) FROM r_latest')"
	echo "the killed row: $(q "SELECT count(*) FROM r_latest WHERE v = 'killed'")"
	echo "ids: $(q "SELECT string_agg(id::text, ',' ORDER BY id) FROM r")"
} >"$out" 2>&1
cat "$out"
diff -u "$LT_SRC/test/recovery.expected" "$out"

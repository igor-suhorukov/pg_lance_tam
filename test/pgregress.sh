#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# PostgreSQL's own regression suite with default_table_access_method =
# lance (docs/design.md §6), in a dev container: the suite and its
# libraries at the server's commit (/cb/pgregress), against a server that
# preloads the module, in a database with the extension.  Its differences
# are kept for review -- most are statements lance tables refuse -- and a
# crash, an assertion or a PANIC fails the leg.  A transaction the suite
# leaves prepared is rolled back by a watchdog after 30 s, and a lock wait
# times out, so that nothing the suite leaves behind hangs it.
set -u
LT_SETTINGS="max_connections = 120
${LT_SETTINGS:-}"
. "$LT_SRC/test/server.sh"
# build.sh, sourced, sets -e and pipefail; the suite's differences must not
# end the leg before its summary
set +e +o pipefail
R=/cb/pgregress/src/test/regress
PG_REGRESS="$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress"
PSQL=(psql -X -q -At -v ON_ERROR_STOP=1)

"${PSQL[@]}" -d postgres -c "CREATE DATABASE regression TEMPLATE template0" || exit 1
"${PSQL[@]}" -d regression -c "
	ALTER DATABASE regression SET lc_messages TO 'C';
	ALTER DATABASE regression SET lc_monetary TO 'C';
	ALTER DATABASE regression SET lc_numeric TO 'C';
	ALTER DATABASE regression SET lc_time TO 'C';
	ALTER DATABASE regression SET bytea_output TO 'hex';
	ALTER DATABASE regression SET timezone_abbreviations TO 'Default';
	CREATE EXTENSION lance_tam;" || exit 1

out="$LT_WORK/pgregress"
mkdir -p "$out"
cd "$out" || exit 1
(
	while sleep 5; do
		"${PSQL[@]}" -F ' ' -d postgres -c "SELECT database, quote_literal(gid) FROM pg_prepared_xacts
			WHERE prepared < now() - interval '30 seconds'" 2>/dev/null |
		while read -r db gid; do
			"${PSQL[@]}" -d "$db" -c "ROLLBACK PREPARED $gid" >/dev/null 2>&1 &&
				echo "rolled back the prepared transaction $gid in $db" >>"$LT_WORK/watchdog.log"
		done
	done
) &
watchdog=$!
PGOPTIONS="-c default_table_access_method=lance -c lock_timeout=60s -c statement_timeout=180s" "$PG_REGRESS" --use-existing \
	--inputdir="$R" --outputdir="$out" --bindir="$(pg_config --bindir)" --dlpath="$R" \
	--host=/tmp --port=5432 --dbname=regression \
	--schedule="$R/parallel_schedule" --max-connections=20 >"$out/pg_regress.log" 2>&1
rc=$?
kill "$watchdog" 2>/dev/null
wait "$watchdog" 2>/dev/null
{
	echo "pg_regress exit $rc"
	grep -cE "^ok " "$out/pg_regress.log" | sed 's/^/tests passed: /'
	grep -cE "^not ok " "$out/pg_regress.log" | sed 's/^/tests failed: /'
	echo "errors in the differences, by message:"
	grep -E "^\+ERROR:" "$out/regression.diffs" 2>/dev/null | sed -E 's/"[^"]*"/"..."/g' | sort | uniq -c | sort -rn | head -40
	[ -s "$LT_WORK/watchdog.log" ] && { echo "the watchdog:"; cat "$LT_WORK/watchdog.log"; }
} | tee "$LT_WORK/summary.txt"
if grep -E "TRAP:|PANIC|terminated by signal|was terminated" "$LT_WORK/server.log" >/dev/null; then
	echo "pgregress: the server log has failures"
	grep -E -B2 "TRAP:|PANIC|terminated by signal|was terminated" "$LT_WORK/server.log" | head -40
	exit 1
fi
echo "pgregress: no crash, assertion or PANIC; the differences are in $out/regression.diffs"

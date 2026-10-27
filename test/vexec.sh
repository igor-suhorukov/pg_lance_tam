#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The vexec leg (docs/design.md §2.13): pg_vexec beside lance_tam, vexec
# built from pg_accel's tree (mounted at /pg_accel) against the container's
# server.  A heap table of every type class is loaded into lance tables by
# COPY, by INSERT ... SELECT through the access method, and by vexec's
# VecInsert through lance_tam's batch sink; each load is read back by the
# plain scan, by LanceScan, and by vexec's VecScan through lance_tam's batch
# source in both of vexec's batch formats, and every read must give the
# heap's rows.
set -euo pipefail
[ -d /pg_accel/modules/vexec ] || { echo "vexec.sh: no pg_accel tree at /pg_accel (LT_PG_ACCEL)"; exit 1; }

build=/tmp/vexec-build
rm -rf "$build"; mkdir -p "$build"
cp -a /pg_accel/include /pg_accel/modules "$build/"
make -s -C "$build/modules/vexec" PG_CONFIG=pg_config VEXEC_INCLUDE="$build/include" -j"${MAKE_JOBS:-8}" >"$LT_WORK/vexec-build.log" 2>&1 ||
	{ tail -30 "$LT_WORK/vexec-build.log"; exit 1; }
make -s -C "$build/modules/vexec" PG_CONFIG=pg_config VEXEC_INCLUDE="$build/include" install >>"$LT_WORK/vexec-build.log" 2>&1
echo "vexec $(git -C /pg_accel rev-parse --short HEAD 2>/dev/null || echo '?') built"

LT_SETTINGS="shared_preload_libraries = 'vexec,lance_tam'
vexec.min_rows = 0"
. "$LT_SRC/test/server.sh"
q() { psql -X -q -At -d lance_vexec -c "$1" 2>&1 || true; }
createdb lance_vexec
ROWS="${LT_ROWS:-30000}"
out=$(q "SET client_min_messages = warning; CREATE EXTENSION vexec; CREATE EXTENSION vector; CREATE EXTENSION lance_tam;
CREATE TABLE src AS
SELECT g AS id,
       (g % 30000 - 15000)::int2 AS i2, (g * 7919) % 100003 - 50000 AS i4, g::int8 * 1000003 - 9000000000 AS i8,
       (g / 7.0)::float4 AS f4, g / 13.0 AS f8,
       ((g % 1000) / 7.0)::numeric(12,4) AS n, (g * 1.5)::numeric AS nfree,
       'name ' || (g % 503) AS t, ('x' || g)::varchar(20) AS v, lpad((g % 97)::text, 3)::char(6) AS bp,
       DATE '2020-01-01' + g % 1000 AS d, TIMESTAMP '2020-01-01' + g * interval '1 minute' AS ts,
       TIMESTAMPTZ '2020-01-01 00:00:00+00' + g * interval '1 second' AS tz, time '00:00' + g * interval '1 second' AS tm,
       g % 3 = 0 AS b, md5(g::text)::uuid AS u, decode(md5(g::text), 'hex') AS by,
       CASE WHEN g % 50 = 0 THEN repeat(md5(g::text), 400) ELSE repeat('ab', g % 40) END AS longt,
       ('{' || g % 5 || ',' || g % 7 || '}')::int[] AS arr,
       jsonb_build_object('k', g % 11) AS j, (g % 13) * interval '1 hour' AS iv,
       ('[' || g % 5 || ',' || g % 7 || ',' || g % 11 || ']')::vector(3) AS emb
FROM generate_series(1, $ROWS) g;
UPDATE src SET i2 = NULL, f8 = NULL, t = NULL, d = NULL, u = NULL, emb = NULL WHERE id % 64 IN (0, 63, 1);
UPDATE src SET i4 = NULL, n = NULL, b = NULL, ts = NULL, by = NULL WHERE id % 101 = 7;
COPY src TO '$LT_WORK/src.copy' (FORMAT binary);")
case "$out" in *ERROR*) echo "the data: $out"; exit 1 ;; esac

fail=0
sum="SELECT md5(string_agg(x::text, '|' ORDER BY x.id)) FROM (SELECT * FROM %s) x"
aggs="SELECT count(*), count(i4), min(i4), max(i4), min(d), max(ts), sum(n) FROM %s"
ref=$(q "$(printf "$sum" src)")
ref_aggs=$(q "$(printf "$aggs" src)")

reads() {
	local read set got
	for read in plain lancescan vexec-postgres vexec-arrow; do
		case "$read" in
			plain) set="SET vexec.mode = off; SET lance.enable_customscan = off;" ;;
			lancescan) set="SET vexec.mode = off;" ;;
			vexec-postgres) set="SET vexec.mode = force; SET vexec.batch_format = postgres;" ;;
			vexec-arrow) set="SET vexec.mode = force; SET vexec.batch_format = arrow;" ;;
		esac
		got=$(q "$set $(printf "$sum" "$1")")
		if [ "$got" = "$ref" ]; then
			echo "  $1 read by $read: the heap's rows"
		else
			echo "  $1 read by $read: DIFFERENT: $got, heap's $ref"
			fail=1
		fi
		got=$(q "$set $(printf "$aggs" "$1")")
		[ "$got" = "$ref_aggs" ] || { echo "  $1 read by $read: aggregates DIFFERENT: $got, heap's $ref_aggs"; fail=1; }
	done
	got=$(q "SET vexec.mode = force; EXPLAIN (VERBOSE, COSTS OFF) SELECT count(i4) FROM $1")
	echo "$got" | grep -q "Source: .*lance" && echo "  $1: VecScan reads through the source" ||
		{ echo "  $1: VecScan did not read through lance_tam's source: $got"; fail=1; }
}
for load in copy insert vecinsert; do
	t="l_$load"
	out=$(q "SET client_min_messages = warning; CREATE TABLE $t (LIKE src) USING lance;")
	case "$out" in *ERROR*) echo "  $t: $out"; fail=1; continue ;; esac
	case "$load" in
		copy) out=$(q "COPY $t FROM '$LT_WORK/src.copy' (FORMAT binary)") ;;
		insert) out=$(q "SET vexec.mode = off; INSERT INTO $t SELECT * FROM src") ;;
		vecinsert)
			out=$(q "SET vexec.mode = force; EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) INSERT INTO $t SELECT * FROM src")
			echo "$out" | grep -q "Write: sink lance" && echo "  $t: VecInsert writes through the sink" ||
				{ echo "  $t: VecInsert did not write through lance_tam's sink: $out"; fail=1; }
			;;
	esac
	case "$out" in *ERROR*) echo "  $t: $out"; fail=1; continue ;; esac
	reads "$t"
done
echo
echo "vexec: $([ $fail -eq 0 ] && echo "every load and read the heap's" || echo "a load or read DIFFERED")"
exit $fail

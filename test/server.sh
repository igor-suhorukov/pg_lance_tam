#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# A server for a leg (docs/design.md §6), sourced: the extension built and
# installed, a cluster made in the run's directory with the module
# preloaded, started, and stopped when the leg exits.  PGHOST and PGPORT
# are set for psql.
. "$LT_SRC/test/build.sh"
PGDATA="$LT_WORK/data"
export PGHOST=/tmp PGPORT=5432 PGDATABASE=postgres
rm -rf "$PGDATA"
initdb -D "$PGDATA" -A trust -E UTF8 --locale=C.UTF-8 >"$LT_WORK/initdb.log" 2>&1
mkdir -p "$LT_WORK/lance"
cat >>"$PGDATA/postgresql.conf" <<CONF
shared_preload_libraries = 'lance_tam'
lance.default_location = '$LT_WORK/lance'
unix_socket_directories = '/tmp'
listen_addresses = ''
max_connections = 40
log_min_messages = warning
lc_messages = 'C'
${LT_SETTINGS:-}
CONF
pg_ctl -D "$PGDATA" -l "$LT_WORK/server.log" -w start >/dev/null
trap 'pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null 2>&1 || true' EXIT

# pg_regress over a directory of sql/ and expected/, the tests named
regress() {
	local dir="$1"; shift
	mkdir -p "$LT_WORK/$(basename "$dir")"
	cp -a "$dir/." "$LT_WORK/$(basename "$dir")/"
	createdb "lance_$(basename "$dir")"
	"$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress" \
		--inputdir="$LT_WORK/$(basename "$dir")" --outputdir="$LT_WORK/$(basename "$dir")" \
		--use-existing --dbname="lance_$(basename "$dir")" "$@"
}

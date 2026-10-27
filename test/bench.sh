#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The bench leg (docs/design.md §4): test/bench.sql on a server of its own,
# its output in the run's bench.out.
set -euo pipefail
. "$LT_SRC/test/build.sh"
PGDATA=/tmp/data
export PGHOST=/tmp PGPORT=5432 PGDATABASE=postgres
initdb -D "$PGDATA" -A trust -E UTF8 --locale=C.UTF-8 >/dev/null 2>&1
mkdir -p /tmp/lance
cat >>"$PGDATA/postgresql.conf" <<CONF
shared_preload_libraries = 'lance_tam'
lance.default_location = '/tmp/lance'
unix_socket_directories = '/tmp'
listen_addresses = ''
shared_buffers = '1GB'
work_mem = '64MB'
maintenance_work_mem = '1GB'
CONF
pg_ctl -D "$PGDATA" -l "$LT_WORK/server.log" -w start >/dev/null
psql -X -f "$LT_SRC/test/bench.sql" >"$LT_WORK/bench.out" 2>&1
pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null
grep -E '^==|^Time|recall|^ +[0-9]+ *$' "$LT_WORK/bench.out"

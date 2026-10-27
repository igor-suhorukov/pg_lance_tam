#!/bin/bash
# A scratch leg: the suite's server, a script of psql's against it.
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
${LT_SETTINGS:-}
CONF
pg_ctl -D "$PGDATA" -l "$LT_WORK/server.log" -w start >/dev/null
psql -X -f "$LT_WORK/${LT_SCRIPT:-repro1.sql}" 2>&1 || true
pg_ctl -D "$PGDATA" -m immediate -w stop >/dev/null 2>&1 || true

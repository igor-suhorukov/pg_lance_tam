#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The extension built by PGXS in a copy of the read-only tree, and
# installed into the container's server with the engine library
# (docs/design.md §6).  Sourced by the legs; the copy is at $LT_BUILD.
set -euo pipefail

LT_BUILD="${LT_BUILD:-/tmp/lance_tam}"
rm -rf "$LT_BUILD"
mkdir -p "$LT_BUILD"
cp -a "$LT_SRC/." "$LT_BUILD/"
rm -rf "$LT_BUILD/engine/target"
make -C "$LT_BUILD" -s -k -j"${MAKE_JOBS:-8}" PG_CONFIG=pg_config COPT="-Werror" >"$LT_WORK/build.log" 2>&1 || {
	cat "$LT_WORK/build.log"
	exit 1
}
make -C "$LT_BUILD" -s install PG_CONFIG=pg_config >>"$LT_WORK/build.log" 2>&1
if [ ! -f "$LT_ENGINE" ]; then
	echo "build.sh: no engine library at $LT_ENGINE: test/run.sh engine" >&2
	exit 1
fi
make -C "$LT_BUILD" -s install-engine PG_CONFIG=pg_config LT_ENGINE="$LT_ENGINE" >>"$LT_WORK/build.log" 2>&1
echo "build.sh: built and installed"

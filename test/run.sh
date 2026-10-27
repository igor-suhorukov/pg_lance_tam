#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# lance_tam's legs on the host (docs/design.md §6): the images, and each
# leg in a container of its own (docker/compose.yml), its results in a run
# directory of the cache.  Nothing is built, installed or run against a
# server on the host; it needs docker with Compose v2 and BuildKit.
#
#   run.sh images        the builder and dev images
#   run.sh assert-image  the dev image with assertions: PostgreSQL built
#                        from its source release with --enable-cassert
#   run.sh fetch         cargo fetch for the engine (the only leg with the
#                        network)
#   run.sh engine        the engine library, ~/.cache/pg_accel/lance_tam/engine
#   run.sh build         the extension built and installed in a dev container
#   run.sh checks        the C ABI's two declarations agree (checks/abi.py)
#   run.sh suite         the regression suite (test/regress), the module
#                        preloaded, tables on local storage
#   run.sh s3            the S3 leg: an S3 API (LocalStack) beside the
#                        server, tables in a bucket
#   run.sh recovery      a writer killed between Lance's commit and its own
#   run.sh interop       the interop leg: the datasets read and written by
#                        Lance's Python package beside the server
#   run.sh vexec         the suite's scans and inserts through pg_vexec's
#                        source and sink (pg_accel's tree at LT_PG_ACCEL)
#   run.sh pgregress     PostgreSQL's own regression suite with
#                        default_table_access_method = lance, its
#                        differences kept for review
#   run.sh bench         test/bench.sql, lance against heap and pgvector
#   run.sh repro         a scratch script: LT_RUN's repro1.sql on a server
#                        of its own
#   run.sh shell         a dev container's shell, the server running
#   run.sh all           checks, engine, suite, recovery, s3, interop, vexec
#
# Settings, from the environment:
#   LT_CACHE      the runs: ~/.cache/pg_accel/lance_tam
#   LT_CPUS       each container's CPUs: 0, as many as there are
#   LT_TESTS      the suite's tests, all by default
#   LT_LANCE_SRC  Lance's checkout: ~/dev/projects/oss_databases/github/lance
#   LT_PG_ACCEL   pg_accel's tree, for vexec
#   LT_RUN        repro: the run directory that holds repro1.sql
#   LT_DEV_IMAGE  the image the legs run on: lance_tam/dev:19rc1-trixie;
#                 lance_tam/dev:19rc1-assert for assertions
set -u -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$here/.." && pwd)"
COMPOSE="$ROOT/docker/compose.yml"
export LT_CACHE="${LT_CACHE:-$HOME/.cache/pg_accel/lance_tam}"
export LT_CPUS="${LT_CPUS:-0}"
mkdir -p "$LT_CACHE/runs" "$LT_CACHE/engine/lib" "$LT_CACHE/cargo"

die() { echo "run.sh: $*" >&2; exit 1; }
compose() { docker compose -f "$COMPOSE" "$@"; }

# leg <name> <service> <command...>: its run's directory under the cache,
# its output kept there too.
leg() {
	local name="$1"; shift
	local run="$LT_CACHE/runs/$(date +%Y%m%dT%H%M%S)-$name"
	mkdir -p "$run"
	chmod 777 "$run"
	echo "== $name: $run"
	LT_WORK="$run" compose --profile run run --rm -T "$@" 2>&1 \
		| grep -v -E '^ (Container|Network|Volume) ' | tee "$run/output.log"
	local rc=${PIPESTATUS[0]}
	echo "== $name: $( [ "$rc" = 0 ] && echo passed || echo "FAILED ($rc)")"
	return "$rc"
}

case "${1:-}" in
	images)
		compose --profile build build lt-engine-builder lt-dev-vanilla ;;
	assert-image)
		compose --profile build build lt-dev-assert ;;
	fetch)
		leg fetch engine-fetch /src/test/engine.sh fetch ;;
	engine)
		leg engine engine /src/test/engine.sh build ;;
	build)
		leg build vanilla /src/test/suite.sh build ;;
	checks)
		leg checks vanilla /src/checks/checks.sh ;;
	suite)
		leg suite vanilla /src/test/suite.sh ;;
	s3)
		compose --profile run up -d s3 >/dev/null || die "cannot start the S3 service"
		leg s3 vanilla /src/test/s3.sh
		rc=$?
		compose --profile run stop s3 >/dev/null
		exit "$rc" ;;
	recovery)
		leg recovery vanilla /src/test/recovery.sh ;;
	interop)
		leg interop vanilla /src/test/interop.sh ;;
	vexec)
		leg vexec vanilla /src/test/vexec.sh ;;
	pgregress)
		leg pgregress vanilla /src/test/pgregress.sh ;;
	bench)
		leg bench vanilla /src/test/bench.sh ;;
	repro)
		[ -n "${LT_RUN:-}" ] && [ -f "$LT_RUN/repro1.sql" ] || die "LT_RUN: a directory with repro1.sql"
		LT_WORK="$LT_RUN" compose --profile run run --rm -T vanilla /src/test/repro.sh ;;
	shell)
		LT_WORK="$LT_CACHE/runs/shell" compose --profile run run --rm vanilla /src/test/suite.sh shell ;;
	all)
		"$0" checks && "$0" engine && "$0" suite && "$0" recovery && "$0" s3 && "$0" interop && "$0" vexec ;;
	*)
		sed -n '3,/^set /p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
		exit 2 ;;
esac

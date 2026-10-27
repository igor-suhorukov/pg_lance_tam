#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The regression suite, in a dev container (docs/design.md §6): a server
# with the module preloaded and lance tables on local storage under the
# run's directory (server.sh), and pg_regress over test/regress.
#
#   suite.sh          the suite (LT_TESTS: the tests, all by default)
#   suite.sh build    the build alone
#   suite.sh shell    a shell, the server running
set -euo pipefail

mode="${1:-suite}"
if [ "$mode" = build ]; then
	. "$LT_SRC/test/build.sh"
	exit 0
fi
. "$LT_SRC/test/server.sh"
if [ "$mode" = shell ]; then
	echo "server running; psql connects"
	bash -i
	exit 0
fi
tests="${LT_TESTS:-$(sed -n 's/^test: //p' "$LT_SRC/test/regress/schedule" | tr '\n' ' ')}"
regress "$LT_SRC/test/regress" $tests

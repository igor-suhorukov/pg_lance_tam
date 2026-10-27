#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The engine library, built in the builder container (docker/compose.yml,
# services "engine" and "engine-fetch"): engine/ is copied out of the
# read-only tree into /build/src, built against Lance's checkout at /lance,
# and the library left at /build/lib/liblance_engine.so.
#
#   engine.sh fetch   cargo fetch, the only step that reaches the network
#   engine.sh build   cargo build --offline (the default)
#
# The lockfile lives in /build/Cargo.lock between runs; the first run seeds
# it from engine/Cargo.lock, or else from Lance's own, so the dependency
# versions are the ones Lance is tested with.
set -euo pipefail

step="${1:-build}"
src=/build/src/engine
profile="${LT_ENGINE_PROFILE:-release}"

rm -rf "$src"
mkdir -p "$src" /build/lib
cp -a /src/engine/. "$src/"
if [ -f /build/Cargo.lock ]; then
	cp /build/Cargo.lock "$src/Cargo.lock"
elif [ -f /src/engine/Cargo.lock ]; then
	: # copied with the tree
else
	cp /lance/Cargo.lock "$src/Cargo.lock"
fi

cd "$src"
case "$step" in
	fetch)
		cargo fetch
		cp Cargo.lock /build/Cargo.lock
		;;
	build)
		cargo build --offline --profile "$profile" -j "${LT_ENGINE_JOBS:-24}"
		cp Cargo.lock /build/Cargo.lock
		dir="$profile"
		[ "$profile" = dev ] && dir=debug
		cp "/build/target/$dir/liblance_engine.so" /build/lib/liblance_engine.so.tmp
		mv /build/lib/liblance_engine.so.tmp /build/lib/liblance_engine.so
		ls -la /build/lib/liblance_engine.so
		;;
	*)
		echo "engine.sh: unknown step $step" >&2
		exit 2
		;;
esac

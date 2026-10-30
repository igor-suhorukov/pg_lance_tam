#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The engine library, built in the builder container (docker/compose.yml,
# services "engine" and "engine-fetch"): engine/ is copied out of the
# read-only tree into /build/src, built at the versions of its lockfile,
# and the library left at /build/lib/liblance_engine.so.
#
#   engine.sh fetch   cargo fetch: the lockfile's crates and Lance's repository
#   engine.sh lock    cargo update: the newest versions engine/Cargo.toml
#                     allows, left in /build/Cargo.lock for engine/Cargo.lock
#   engine.sh build   cargo build --offline (the default)
#
# fetch and lock are the steps that reach the network.
set -euo pipefail

step="${1:-build}"
src=/build/src/engine
profile="${LT_ENGINE_PROFILE:-release}"

rm -rf "$src"
mkdir -p "$src" /build/lib
cp -a /src/engine/. "$src/"

cd "$src"
case "$step" in
	fetch)
		cargo fetch --locked
		;;
	lock)
		cargo update
		cp Cargo.lock /build/Cargo.lock
		;;
	build)
		cargo build --offline --locked --profile "$profile" -j "${LT_ENGINE_JOBS:-24}"
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

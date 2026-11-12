#!/usr/bin/env bash
# Regression gate: runs upstream NetBird's management tests at the exact version this
# module pins, with HA off (upstream's own server construction, no wrappers). The
# module cache is read-only and some tests write next to their sources, so the module
# is copied to a scratch directory first.
#
# Usage: scripts/upstream-mgmt-test.sh [package patterns...]
# Default packages are the ones the HA wrappers sit on.
# Commands assume the repository root is the cwd.
set -euo pipefail

src=$(go list -m -f '{{.Dir}}' github.com/netbirdio/netbird)
work=$(mktemp -d)
trap 'chmod -R u+w "$work"; rm -rf "$work"' EXIT
cp -r "$src/." "$work/"
chmod -R u+w "$work"

pkgs=("$@")
if [[ ${#pkgs[@]} -eq 0 ]]; then
	pkgs=(
		./management/internals/controllers/...
		./management/internals/shared/grpc/...
		./management/internals/server/...
		./management/internals/modules/peers/...
		./management/server
	)
fi
cd "$work"
NETBIRD_STORE_ENGINE="${NETBIRD_STORE_ENGINE:-sqlite}" go test -count=1 -timeout 30m "${pkgs[@]}"

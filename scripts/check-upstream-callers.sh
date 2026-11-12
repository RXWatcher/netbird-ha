#!/usr/bin/env bash
# Lists every direct call in upstream's management code to the per-instance stream
# methods (SendUpdate, HasChannel, CloseChannel, ...) and compares the list with the
# checked-in allowlist. The HA controller wrapper only fans out updates that go
# through network_map.Controller; a new direct caller would silently stay local to
# one instance, so it must be reviewed and then added to the allowlist.
#
# Usage: scripts/check-upstream-callers.sh [--update]
# Commands assume the repository root is the cwd.
set -euo pipefail

allowlist=scripts/upstream-callers.allowlist
dir=$(go list -m -f '{{.Dir}}' github.com/netbirdio/netbird)
pattern='\.(SendUpdate|HasChannel|CreateChannel|CloseChannel|CloseChannels|CloseSessionChannel)\('

current=$(cd "$dir" && grep -rnoE --include='*.go' "$pattern" management combined \
	| grep -vE '_test\.go:|_mock\.go:|/mock_|testing_tools' \
	| awk -F: '{ sub(/\($/, "", $3); print $1 "\t" $3 }' \
	| sort | uniq -c | awk '{ print $2 "\t" $3 "\t" $1 }')

if [[ "${1:-}" == "--update" ]]; then
	{
		echo "# file<TAB>call<TAB>count; regenerate with scripts/check-upstream-callers.sh --update after review"
		echo "$current"
	} > "$allowlist"
	echo "wrote $allowlist"
	exit 0
fi

if ! diff -u <(grep -v '^#' "$allowlist") <(echo "$current"); then
	echo >&2 "upstream direct stream callers changed: review each new caller (does it need cross-instance fan-out?), then run scripts/check-upstream-callers.sh --update"
	exit 1
fi
echo "upstream direct stream callers match $allowlist"

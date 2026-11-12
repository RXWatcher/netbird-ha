#!/bin/bash
# pg_auto_failover monitor or data node. Usage: pgaf-entrypoint monitor|node
# Env: PGAF_HOSTNAME (name other containers use), PGAF_MONITOR (node only).
set -euo pipefail
role="${1:?monitor|node}"
base=/var/lib/postgresql/pgaf
export PGDATA="$base/data"
export XDG_CONFIG_HOME="$base/config"
export XDG_DATA_HOME="$base/share"
mkdir -p "$base"
chown -R postgres:postgres "$base"
# A kill -9 leaves pg_autoctl's pidfile in /tmp, which survives docker start.
rm -rf /tmp/pg_autoctl

if [ "$role" = node ]; then
	# Role check for HAProxy (httpchk on :8008): 200 when this node is a writable primary.
	socat TCP-LISTEN:8008,fork,reuseaddr EXEC:/usr/local/bin/pgaf-role-http &
	# pg_autoctl's --pg-hba-lan only admits the one --dbname. The bed also uses
	# netbird_events and dex, so keep a trust line for every database on the
	# container subnet (test bed only: the network is private to the bed).
	(
		line='host all all samenet trust'
		while sleep 2; do
			f="$PGDATA/pg_hba.conf"
			[ -f "$f" ] || continue
			if ! grep -qxF "$line" "$f"; then
				echo "$line" >>"$f"
				gosu postgres psql -h 127.0.0.1 -U postgres -d postgres -tAc 'select pg_reload_conf()' >/dev/null 2>&1 || true
			fi
		done
	) &
fi

run_as_pg() { exec gosu postgres env PGDATA="$PGDATA" XDG_CONFIG_HOME="$XDG_CONFIG_HOME" XDG_DATA_HOME="$XDG_DATA_HOME" "$@"; }

if [ -f "$PGDATA/PG_VERSION" ]; then
	run_as_pg pg_autoctl run
fi

case "$role" in
monitor)
	run_as_pg pg_autoctl create monitor \
		--hostname "${PGAF_HOSTNAME:?}" --pgport 5432 \
		--auth trust --no-ssl --run
	;;
node)
	run_as_pg pg_autoctl create postgres \
		--hostname "${PGAF_HOSTNAME:?}" --name "${PGAF_HOSTNAME}" --pgport 5432 \
		--monitor "${PGAF_MONITOR:?}" \
		--auth trust --no-ssl --pg-hba-lan \
		--dbname netbird --run
	;;
*)
	echo "unknown role $role" >&2
	exit 2
	;;
esac

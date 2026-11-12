#!/usr/bin/env bash
# netbird-ha docker compose test bed.
#
#   deploy/bed.sh up       build/start the bed, bootstrap the account, enroll clients
#   deploy/bed.sh down     remove every container, volume and the network
#   deploy/bed.sh status   containers, LB backends and peers
#   deploy/bed.sh env      print the bed's connection details as shell exports
#   deploy/bed.sh compose  run docker compose with the bed's arguments
#
# Settings (environment, read by `up`; `up` records them in deploy/.env):
#   NBHA_IMPL       auto (default) | stock | ha
#                   stock: upstream netbirdio/{management,signal}:0.82.1 images.
#                   ha: build cmd/management and cmd/signal from this repository.
#                   auto: ha when cmd/management/Dockerfile exists, else stock.
#   NBHA_INSTANCES  1 | 2 (default: 2 for ha, 1 for stock)
#   NBHA_LB_MODE    l4 (default) | l7       see deploy/compose/haproxy/haproxy.cfg
#   NBHA_MGMT_BALANCE   roundrobin (default) | leastconn | first | source
#   NBHA_SIGNAL_BALANCE first (default) | roundrobin | leastconn | source
#   NBHA_SIGNAL_PIN 1 (default): mgmt-N hands out signal frontend lb:1000N (signal-N first);
#                   0: both hand out lb:10000 (balanced with NBHA_SIGNAL_BALANCE).
#   NBHA_PG_ACCESS  multihost (default): libpq multi-host DSN, target_session_attrs=read-write
#                   lb: through HAProxy's primary-tracking :5432 frontend.
#   NBHA_CLIENTS    1 (default) start the six clients; 0 leaves them out.
#   NBHA_IMAGE_VERSION  ha: image tag and version string (default: git describe)
#   NBHA_UPGRADE_BUILD  1: also build <version>-next images for scenario 8
#
# Everything runs in compose project "nbha-bed" on docker network "nbha-bed".
# Host ports are ephemeral and bound to 127.0.0.1.
set -euo pipefail

DEPLOY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$DEPLOY_DIR/.." && pwd)
COMPOSE_FILE="$DEPLOY_DIR/compose/compose.yml"
ENV_FILE="$DEPLOY_DIR/.env"
STATE_DIR="$DEPLOY_DIR/.state"
PROJECT=nbha-bed
CLIENTS=(c1 c2 c3 c4 c5 c6)

log() { printf '%s bed: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

env_get() { # key [default]
	local v=""
	[ -f "$ENV_FILE" ] && v=$(grep -E "^$1=" "$ENV_FILE" | tail -1 | cut -d= -f2- || true)
	printf '%s' "${v:-${2:-}}"
}

env_set() { # key value
	touch "$ENV_FILE"
	chmod 600 "$ENV_FILE"
	if grep -qE "^$1=" "$ENV_FILE"; then
		local tmp
		tmp=$(mktemp)
		grep -vE "^$1=" "$ENV_FILE" >"$tmp" || true
		cat "$tmp" >"$ENV_FILE"
		rm -f "$tmp"
	fi
	printf '%s=%s\n' "$1" "$2" >>"$ENV_FILE"
}

profiles() {
	local p=()
	[ "$(env_get NBHA_INSTANCES 1)" = 2 ] && p+=(--profile second)
	[ "$(env_get NBHA_CLIENTS 1)" = 1 ] && p+=(--profile clients)
	printf '%s\n' "${p[@]}"
}

dc() { # docker compose with the bed's arguments
	local p
	mapfile -t p < <(profiles)
	docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "${p[@]}" "$@"
}

lb_port() { # container port -> host port
	dc port lb "$1" | awk -F: '{print $NF}'
}

rand_b64() { head -c "$1" /dev/urandom | base64 | tr -d '\n'; }

write_settings() {
	local impl=${NBHA_IMPL:-auto}
	if [ "$impl" = auto ]; then
		if [ -f "$ROOT/cmd/management/Dockerfile" ] && [ -f "$ROOT/cmd/signal/Dockerfile" ]; then impl=ha; else impl=stock; fi
	fi
	case "$impl" in stock | ha) ;; *) die "NBHA_IMPL must be auto, stock or ha" ;; esac
	local instances=${NBHA_INSTANCES:-}
	[ -n "$instances" ] || { [ "$impl" = ha ] && instances=2 || instances=1; }

	env_set NBHA_IMPL "$impl"
	env_set NBHA_INSTANCES "$instances"
	env_set NBHA_LB_MODE "${NBHA_LB_MODE:-l4}"
	env_set NBHA_MGMT_BALANCE "${NBHA_MGMT_BALANCE:-roundrobin}"
	env_set NBHA_SIGNAL_BALANCE "${NBHA_SIGNAL_BALANCE:-first}"
	env_set NBHA_CLIENTS "${NBHA_CLIENTS:-1}"
	env_set NBHA_CLIENT_LOG_LEVEL "${NBHA_CLIENT_LOG_LEVEL:-debug}"

	if [ "${NBHA_SIGNAL_PIN:-1}" = 1 ]; then
		env_set NBHA_SIGNAL_URI_1 lb:10001
		env_set NBHA_SIGNAL_URI_2 lb:10002
	else
		env_set NBHA_SIGNAL_URI_1 lb:10000
		env_set NBHA_SIGNAL_URI_2 lb:10000
	fi

	local pg_access=${NBHA_PG_ACCESS:-multihost}
	env_set NBHA_PG_ACCESS "$pg_access"
	if [ "$pg_access" = lb ]; then
		env_set NBHA_PG_DSN 'postgresql://postgres@lb:5432/netbird?sslmode=disable'
		env_set NBHA_EVENTS_DSN 'postgresql://postgres@lb:5432/netbird_events?sslmode=disable'
	else
		env_set NBHA_PG_DSN 'postgresql://postgres@pg-1:5432,pg-2:5432/netbird?sslmode=disable&target_session_attrs=read-write'
		env_set NBHA_EVENTS_DSN 'postgresql://postgres@pg-1:5432,pg-2:5432/netbird_events?sslmode=disable&target_session_attrs=read-write'
	fi
	# Dex always goes through HAProxy: NetBird's embedded-IdP DSN parser keeps
	# only host/port/dbname/user/password/sslmode, so a multi-host DSN with
	# target_session_attrs cannot reach Dex (idp/dex/config.go parsePostgresDSN).
	env_set NBHA_DEX_DSN 'postgresql://postgres@lb:5432/dex?sslmode=disable'

	# Secrets: generated once per bed, shared by every instance.
	[ -n "$(env_get NBHA_RELAY_SECRET)" ] || env_set NBHA_RELAY_SECRET "$(rand_b64 24)"
	[ -n "$(env_get NBHA_ENCRYPTION_KEY)" ] || env_set NBHA_ENCRYPTION_KEY "$(rand_b64 32)"
	[ -n "$(env_get NBHA_IDP_COOKIE_KEY)" ] || env_set NBHA_IDP_COOKIE_KEY "$(rand_b64 32)"
	[ -n "$(env_get NBHA_WG_PRIVATE_KEY)" ] || env_set NBHA_WG_PRIVATE_KEY "$(rand_b64 32)"
	[ -n "$(env_get NBHA_SETUP_KEY)" ] || env_set NBHA_SETUP_KEY ""

	if [ "$impl" = ha ]; then
		# Images from `make images` (cmd/*/Dockerfile), tagged with the build version.
		local v=${NBHA_IMAGE_VERSION:-$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)}
		env_set NBHA_IMAGE_VERSION "$v"
		env_set NBHA_MGMT_IMAGE "netbird-ha/management:$v"
		env_set NBHA_SIGNAL_IMAGE "netbird-ha/signal:$v"
		if [ "${NBHA_UPGRADE_BUILD:-0}" = 1 ]; then
			# Scenario 8 rolls the instances onto a second build (only the version differs).
			env_set NBHA_UPGRADE_MGMT_IMAGE "netbird-ha/management:$v-next"
			env_set NBHA_UPGRADE_SIGNAL_IMAGE "netbird-ha/signal:$v-next"
		fi
	else
		env_set NBHA_MGMT_IMAGE "${NBHA_STOCK_MGMT_IMAGE:-netbirdio/management:0.82.1}"
		env_set NBHA_SIGNAL_IMAGE "${NBHA_STOCK_SIGNAL_IMAGE:-netbirdio/signal:0.82.1}"
	fi
}

build_images() {
	log "building pg_auto_failover image"
	docker build -q -t nbha-bed/pgaf:17 "$DEPLOY_DIR/compose/pgaf" >/dev/null
	if [ "$(env_get NBHA_IMPL)" = ha ]; then
		local v
		v=$(env_get NBHA_IMAGE_VERSION)
		log "building management and signal from cmd/ (make images HA_VERSION=$v)"
		make -s -C "$ROOT" images HA_VERSION="$v" IMAGE_TAG="$v" >/dev/null
		if [ -n "$(env_get NBHA_UPGRADE_MGMT_IMAGE)" ]; then
			log "building the upgrade images (make images HA_VERSION=$v-next)"
			make -s -C "$ROOT" images HA_VERSION="$v-next" IMAGE_TAG="$v-next" >/dev/null
		fi
	fi
}

wait_http() { # url timeout_s
	local deadline=$((SECONDS + $2))
	until curl -fsS -o /dev/null --max-time 3 "$1"; do
		[ $SECONDS -lt $deadline ] || return 1
		sleep 1
	done
}

api() { # method path [json]
	local port pat
	port=$(lb_port 80)
	pat=$(env_get NBHA_PAT)
	curl -fsS --max-time 15 -X "$1" -H "Authorization: Token $pat" -H 'Content-Type: application/json' \
		${3:+--data "$3"} "http://127.0.0.1:$port/api$2"
}

setup_owner() { # host_port email -> writes response to $STATE_DIR/setup-<port>.json, prints http code
	curl -sS --max-time 30 -o "$STATE_DIR/setup-$1.json" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
		--data "{\"email\":\"$2\",\"password\":\"Nbha-bed-Passw0rd!\",\"name\":\"Bed Owner\",\"create_pat\":true,\"pat_expire_in\":30}" \
		"http://127.0.0.1:$1/api/setup" || true
}

bootstrap() {
	mkdir -p "$STATE_DIR"
	local p80 p1 p2
	p80=$(lb_port 80)
	p1=$(lb_port 8081)
	p2=$(lb_port 8082)

	if [ -n "$(env_get NBHA_PAT)" ] && api GET /users/current >/dev/null 2>&1; then
		log "account already bootstrapped"
	else
		local required
		required=$(curl -fsS "http://127.0.0.1:$p80/api/instance" | jq -r .setup_required)
		[ "$required" = true ] || die "instance setup is not required but deploy/.env has no working PAT; run bed-down first"
		if [ "$(env_get NBHA_INSTANCES)" = 2 ]; then
			# Scenario 6: the first owner set up concurrently on both instances.
			log "racing instance setup on mgmt-1 and mgmt-2"
			local c1 c2
			setup_owner "$p1" owner1@nbha.test >"$STATE_DIR/setup-code-1" &
			setup_owner "$p2" owner2@nbha.test >"$STATE_DIR/setup-code-2" &
			wait
			c1=$(cat "$STATE_DIR/setup-code-1")
			c2=$(cat "$STATE_DIR/setup-code-2")
			jq -n --arg c1 "$c1" --arg c2 "$c2" \
				--slurpfile r1 <(cat "$STATE_DIR/setup-$p1.json" 2>/dev/null || echo null) \
				--slurpfile r2 <(cat "$STATE_DIR/setup-$p2.json" 2>/dev/null || echo null) \
				'{mgmt1:{code:($c1|tonumber),body:$r1[0]},mgmt2:{code:($c2|tonumber),body:$r2[0]}}' \
				>"$STATE_DIR/setup-race.json" 2>/dev/null ||
				printf '{"mgmt1":{"code":%s},"mgmt2":{"code":%s}}\n' "${c1:-0}" "${c2:-0}" >"$STATE_DIR/setup-race.json"
			log "setup race: mgmt-1 HTTP $c1, mgmt-2 HTTP $c2"
			local pat=""
			for f in "$STATE_DIR/setup-$p1.json" "$STATE_DIR/setup-$p2.json"; do
				[ -n "$pat" ] || pat=$(jq -r '.personal_access_token // empty' "$f" 2>/dev/null || true)
			done
			[ -n "$pat" ] || die "no setup call returned a PAT: $(cat "$STATE_DIR"/setup-*.json 2>/dev/null)"
			env_set NBHA_PAT "$pat"
		else
			local code
			code=$(setup_owner "$p80" owner1@nbha.test)
			[ "$code" = 200 ] || die "setup failed with HTTP $code: $(cat "$STATE_DIR/setup-$p80.json")"
			env_set NBHA_PAT "$(jq -r .personal_access_token "$STATE_DIR/setup-$p80.json")"
		fi
	fi

	if [ -z "$(env_get NBHA_SETUP_KEY)" ]; then
		local key
		key=$(api POST /setup-keys '{"name":"bed","type":"reusable","expires_in":2592000,"auto_groups":[],"usage_limit":0,"ephemeral":false}' | jq -r .key)
		[ -n "$key" ] && [ "$key" != null ] || die "could not create a setup key"
		env_set NBHA_SETUP_KEY "$key"
	fi
}

wait_peers() { # count timeout_s
	local deadline=$((SECONDS + $2)) n=0
	while [ $SECONDS -lt $deadline ]; do
		n=$(api GET /peers 2>/dev/null | jq '[.[] | select(.connected)] | length' 2>/dev/null || echo 0)
		[ "$n" -ge "$1" ] && return 0
		sleep 2
	done
	log "only $n of $1 peers connected"
	return 1
}

cmd_up() {
	command -v jq >/dev/null || die "jq is required"
	write_settings
	build_images
	log "starting infrastructure ($(env_get NBHA_IMPL), $(env_get NBHA_INSTANCES) instance(s), LB $(env_get NBHA_LB_MODE))"
	local svcs=(pgaf-monitor pg-1 pg-2 pg-init valkey-1 valkey-2 valkey-3 sentinel-1 sentinel-2 sentinel-3 lb relay-1 relay-2 mgmt-1 signal-1)
	[ "$(env_get NBHA_INSTANCES)" = 2 ] && svcs+=(signal-2)
	dc up -d --quiet-pull "${svcs[@]}"
	local p80
	p80=$(lb_port 80)
	log "waiting for management on 127.0.0.1:$p80"
	wait_http "http://127.0.0.1:$(lb_port 8081)/api/instance" 180 || { dc logs --tail 50 mgmt-1 >&2; die "management did not come up"; }
	# Each HAProxy backend health-checks on its own; :80 can lag :8081.
	wait_http "http://127.0.0.1:$p80/api/instance" 30 || die "management is up but not behind lb:80"
	if [ "$(env_get NBHA_INSTANCES)" = 2 ]; then
		# The first instance creates the schemas alone: upstream's activity-event
		# store migrates outside the advisory lock (ha/store/doc.go).
		dc up -d mgmt-2
		local deadline=$((SECONDS + 120))
		until curl -fsS "http://127.0.0.1:$(lb_port 8404)/;csv" 2>/dev/null | grep -qE '^be_mgmt2,mgmt-2,.*,UP'; do
			[ $SECONDS -lt $deadline ] || { dc logs --tail 50 mgmt-2 >&2; die "mgmt-2 did not come up"; }
			sleep 1
		done
	fi
	bootstrap
	if [ "$(env_get NBHA_CLIENTS 1)" = 1 ]; then
		log "starting clients"
		dc up -d "${CLIENTS[@]}"
		log "waiting for ${#CLIENTS[@]} peers to connect"
		wait_peers "${#CLIENTS[@]}" 180 || die "clients did not connect"
	fi
	log "bed is up"
	cmd_env | sed 's/^/  /' >&2
}

cmd_down() {
	local ids
	ids=$(docker ps -aq --filter label=nbha.bed.dynamic=1)
	[ -z "$ids" ] || docker rm -f $ids >/dev/null
	if [ -f "$ENV_FILE" ]; then
		dc --profile second --profile clients down -v --remove-orphans --timeout 5
	else
		docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile second --profile clients down -v --remove-orphans --timeout 5 2>/dev/null || true
	fi
	rm -rf "$STATE_DIR" "$ENV_FILE"
	log "bed removed"
}

cmd_env() {
	[ -f "$ENV_FILE" ] || die "no bed (deploy/.env missing)"
	echo "export NBHA_IMPL=$(env_get NBHA_IMPL)"
	echo "export NBHA_INSTANCES=$(env_get NBHA_INSTANCES)"
	echo "export NBHA_LB_MODE=$(env_get NBHA_LB_MODE)"
	echo "export NBHA_API=http://127.0.0.1:$(lb_port 80)"
	echo "export NBHA_API_MGMT1=http://127.0.0.1:$(lb_port 8081)"
	echo "export NBHA_API_MGMT2=http://127.0.0.1:$(lb_port 8082)"
	echo "export NBHA_LB_STATS=http://127.0.0.1:$(lb_port 8404)"
	echo "export NBHA_LB_ADMIN=127.0.0.1:$(lb_port 9999)"
	echo "export NBHA_PAT=$(env_get NBHA_PAT)"
	echo "export NBHA_UPGRADE_MGMT_IMAGE=$(env_get NBHA_UPGRADE_MGMT_IMAGE)"
	echo "export NBHA_UPGRADE_SIGNAL_IMAGE=$(env_get NBHA_UPGRADE_SIGNAL_IMAGE)"
}

cmd_status() {
	dc ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'
	echo
	curl -fsS "http://127.0.0.1:$(lb_port 8404)/;csv" | awk -F, '$2!="FRONTEND" && $2!="BACKEND" && NR>1 {printf "%-11s %-10s %s\n", $1, $2, $18}'
	echo
	api GET /peers | jq -r '.[] | "\(.name)\t\(.ip)\tconnected=\(.connected)"'
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
status) cmd_status ;;
env) cmd_env ;;
compose) shift; dc "$@" ;;
*) sed -n '2,25p' "${BASH_SOURCE[0]}"; exit 2 ;;
esac

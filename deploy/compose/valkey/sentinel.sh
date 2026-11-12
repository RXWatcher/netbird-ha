#!/bin/sh
# Writes a fresh Sentinel config (Sentinel rewrites its file, so it must be writable) and starts it.
set -eu
cat > /data/sentinel.conf <<CONF
port 26379
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SENTINEL_HOSTNAME}
sentinel monitor nbha valkey-1 6379 2
sentinel down-after-milliseconds nbha 3000
sentinel failover-timeout nbha 10000
sentinel parallel-syncs nbha 1
CONF
exec valkey-sentinel /data/sentinel.conf

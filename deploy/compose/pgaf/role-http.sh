#!/bin/bash
# Answers one HTTP request on stdin/stdout (spawned by socat): 200 on a writable
# primary, 503 on a standby or when Postgres does not answer.
read -r -t 2 _request_line || true
state=$(PGCONNECT_TIMEOUT=2 psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null || echo down)
case "$state" in
f) code="200 OK" body="primary" ;;
t) code="503 Service Unavailable" body="standby" ;;
*) code="503 Service Unavailable" body="down" ;;
esac
printf 'HTTP/1.0 %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s\n' "$code" $((${#body} + 1)) "$body"

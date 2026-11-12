# PostgreSQL 17 with pg_auto_failover from the PGDG apt repository (open source,
# PostgreSQL licence). One image serves the monitor and both data nodes.
FROM postgres:17-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-17-auto-failover socat \
 && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 entrypoint.sh /usr/local/bin/pgaf-entrypoint
COPY --chmod=0755 role-http.sh /usr/local/bin/pgaf-role-http
ENTRYPOINT ["/usr/local/bin/pgaf-entrypoint"]

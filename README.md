# netbird-ha (working name)

Active-active control plane for self-hosted NetBird: two or more Management and Signal instances behind a
load balancer, on shared PostgreSQL (pg_auto_failover) and Valkey/Redis with Sentinel. Built as a separate
Go module on top of stock upstream NetBird; upstream files are never edited. Stock NetBird clients need no
change. The design and its evidence are in [docs/design-report.md](docs/design-report.md).

License: AGPL-3.0 (the combined work includes NetBird's AGPL management/signal/relay code). Reused BSD-3 code
keeps its notice in its own directory. Not affiliated with NetBird GmbH; "NetBird" is their trademark.

## Layout and owners (parallel build, 2026-11-12)

| Path | What | Workstream |
|---|---|---|
| `go.mod`, `cmd/management/`, `cmd/signal/` | module root: requires `github.com/netbirdio/netbird v0.83.0`, copies upstream's `replace` lines, adds the dispatcher `replace` | A (core) |
| `ha/wgkey/`, `ha/bus/`, `ha/controller/` | shared server WG key, Redis message bus + resync, network-map controller wrapper | A (core) |
| `ha/store/`, `ha/cache/`, `ha/ephemeral/` | advisory-lock store wrapper (global lock + migrations), Sentinel-aware cache store, cleanup of ephemeral peers of dead instances | B (state) |
| `dispatcher/` | own Go module replacing `github.com/netbirdio/signal-dispatcher` (Redis routing) | C (signal) |
| `deploy/`, `e2e/` | docker compose test bed (pg_auto_failover, Valkey+Sentinel, HAProxy, mgmt x2, signal x2, relay x2, Dex on Postgres, stock clients) and scenario tests | D (test bed) |

Rules: each workstream works in its own git worktree and branch (`ws-a`, `ws-b`, `ws-c`, `ws-d`) and merges
into local `main` when its tests pass (rebase first). Nothing is pushed to any remote yet.

## Building

`make build` builds `bin/netbird-mgmt` and `bin/netbird-signal`; `make test`, `make lint`,
`make check-upstream-callers` (fails when upstream adds a direct `SendUpdate`/`HasChannel`/`CloseChannel`
caller that the controller wrapper does not see; allowlist in `scripts/upstream-callers.allowlist`) and
`make upstream-mgmt-test` (upstream's management tests at the pinned version, HA off). Images:
`cmd/management/Dockerfile` and `cmd/signal/Dockerfile`, built from the repository root.

`go.mod` swaps upstream's signal dispatcher for `dispatcher/` with
`replace github.com/netbirdio/signal-dispatcher/dispatcher => ./dispatcher`. `dispatcher/` is its own
module: `make test` and `make lint` cover it, and `make test-dispatcher-integration` runs its Valkey and
Sentinel tests in Docker. Its configuration (`NB_SIGNAL_REDIS_URL` and friends) and delivery guarantees
are in [dispatcher/README.md](dispatcher/README.md).

## Management HA configuration (`ha/`)

With `NB_HA_REDIS_URL` unset, `cmd/management` is upstream's server unchanged.

All three Redis variables (`NB_HA_REDIS_URL`, `NB_CACHE_REDIS_ADDRESS`, `NB_SIGNAL_REDIS_URL`) take one URL
format, parsed by the same code (`ha/cache.ParseURL`, mirrored in `dispatcher/`; a test in `ha/bus` checks
that all three agree). Write it once and reuse it:

- one server: `redis[s]://[user:password@]host:6379[/db]`
- Sentinel: `redis[s]://[user:password@]sentinel1:26379[/db]?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379`
  (go-redis `ParseFailoverURL` format; the user info authenticates to the data nodes, `sentinel_username` and
  `sentinel_password` to the Sentinels; also `db`, `dial_timeout`, `read_timeout`, `write_timeout`,
  `pool_size`, `skip_verify` for `rediss`; any other option is an error)

| Variable | Meaning |
|---|---|
| `NB_HA_REDIS_URL` | Turns HA on: the management update bus. Then the store must be PostgreSQL and `NB_CACHE_REDIS_ADDRESS` and the shared key must be set, or the instance refuses to start. |
| `NB_HA_WG_PRIVATE_KEY` / `NB_HA_WG_PRIVATE_KEY_FILE` | The server WireGuard private key shared by every instance (`wg genkey` format). The file wins. |
| `NB_HA_INSTANCE_ID` | This instance's name on the bus and in the migration record (default hostname plus a random suffix, new on every start). Set it to a stable unique name. |
| `NB_HA_BUS_CHANNEL` | Pub/sub channel (default `netbird-ha:mgmt:updates`). |
| `NB_HA_RESYNC_INTERVAL` | Periodic network-map resync for accounts with local streams (default `10m`, `off` to disable). A resync also runs after every (re)subscription. |
| `NB_HA_FORCE_MIGRATIONS` | `true` runs upstream's migrations even when another instance already ran them for this release (`ha/store`). |

Metrics: `netbird_ha_bus_*` and `netbird_ha_controller_*` on the management metrics port. Other
workstreams add their overrides with `ha.Register` from an `init` function in their own file.

Upstream pieces that are already multi-instance and need nothing from `ha/`:

- Peer liveness (since v0.83.0): every instance registers in `management_instances` and heartbeats each
  minute; a peer reads as connected only while a fresh instance holds its row in `peer_connections`, and live
  instances sweep the connections of an instance that stopped heartbeating (5 min). `ha/ephemeral` only adds
  the scheduling of the dead instance's ephemeral peers for deletion, which upstream does only at boot.
- The MCP endpoint (`NB_MCP_ENABLED`, off by default) keeps its OAuth grants in Redis at
  `NB_CACHE_REDIS_ADDRESS`, which upstream parses as a single-server URL. With a Sentinel URL its OAuth
  broker turns itself off with a warning and MCP clients must use a personal access token.

## Upgrading to a new upstream release

1. Bump `github.com/netbirdio/netbird` in `go.mod` and `dispatcher/go.mod`, copy any changed `replace`
   lines, and compare upstream's `toolchain` and its `signal-dispatcher` pin with ours.
2. `go build ./...` and `go vet ./...`: interface changes (network-map controller, secrets manager, store,
   ephemeral manager, cache store) show up here. A new `network_map.Controller` method must be wrapped in
   `ha/controller`.
3. `make check-upstream-callers`; review each new direct stream caller before `--update`.
4. Read the release notes and the diff of `management/`, `signal/`, `relay/` for new background jobs, update
   paths, caches and per-process state.
5. Review the migration diff as `ha/store/doc.go` describes and decide between a rolling upgrade and a
   window.
6. `make test`, `make lint`, `make upstream-mgmt-test`, `NBHA_UPGRADE_BUILD=1 make e2e`, and
   `NBHA_UPGRADE_FROM=<previous tag> E2E_RUN=TestS8 make e2e` for the mixed-version roll.

## Two-instance deployment: environment

Every management instance gets the same values except `NB_HA_INSTANCE_ID`; likewise every signal instance
except `NB_SIGNAL_INSTANCE_ID`. With Valkey behind Sentinel (`$REDIS` below; a plain `redis://host:6379`
works too, but then Redis is a single point of failure):

```sh
REDIS='redis://:valkeypw@sentinel1:26379?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379&sentinel_password=sentinelpw'

# management (netbird-ha/management image), mgmt-1 and mgmt-2
NB_HA_INSTANCE_ID=mgmt-1                        # mgmt-2 on the other one
NB_HA_REDIS_URL=$REDIS                          # update bus
NB_CACHE_REDIS_ADDRESS=$REDIS                   # sessions, PKCE, one-time tokens, IdP cache
NB_HA_WG_PRIVATE_KEY_FILE=/run/secrets/mgmt_wg_key   # same key on both
NB_STORE_ENGINE_POSTGRES_DSN='host=pg1,pg2 port=5432 user=netbird password=... dbname=netbird target_session_attrs=read-write'
# management.json: "StoreConfig": {"Engine": "postgres"} (or NETBIRD_STORE_ENGINE=postgres),
#   the same DataStoreEncryptionKey, Relay secret and IdP settings on both instances.
# Leave NB_STORE_ENGINE_POSTGRES_REPLICA_DSN unset: replica lag serves stale network maps.

# signal (netbird-ha/signal image), signal-1 and signal-2
NB_SIGNAL_INSTANCE_ID=signal-1                  # signal-2 on the other one
NB_SIGNAL_REDIS_URL=$REDIS
```

One URL for all three is fine: the bus uses only pub/sub, and the cache and signal keys have their own
prefixes. To separate them anyway, add `&db=N` (or put `/N` before the `?`). Optional tuning for signal is in [dispatcher/README.md](dispatcher/README.md). Clients see one
management URL and one signal URL on the load balancer; list each relay by its own URL with the same secret.

## Test bed and end-to-end scenarios (`deploy/`, `e2e/`)

A local Docker Compose bed (project and network `nbha-bed`) with the topology of
[docs/design-report.md](docs/design-report.md) section 4.1, and the scenarios from section 4.6 in `e2e/`
(its own Go module). Needs Docker, `jq` and `curl`. Secrets are generated at `bed-up` into the gitignored
`deploy/.env`; host ports are ephemeral and bound to 127.0.0.1.

| Target | What |
|---|---|
| `make bed-up` / `make bed-down` / `make bed-status` | start (with `NBHA_IMPL=ha` it runs `make images` first), remove everything, show state |
| `make e2e` | fresh bed with this repository's images (2 management, 2 signal), every scenario, then `bed-down` |
| `make e2e-baseline` | the same with stock `netbirdio/*:0.83.0` images (1 instance; `NBHA_INSTANCES=2` for the two-instance stock baseline, where cross-instance scenarios report XFAIL) |
| `make e2e-run` | the scenarios against the bed that is up now |

Useful variables: `E2E_RUN=<regexp>` selects scenarios, `KEEP_BED=1` leaves the bed up, `NBHA_E2E_LONG=1`
adds the ephemeral and soak scenarios, `NBHA_SOAK=30m` sets the soak length, `NBHA_UPGRADE_BUILD=1` builds
a second image set for the rolling upgrade, `NBHA_UPGRADE_FROM=<tag>` starts the bed on an earlier release
and lets the rolling upgrade move it onto this tree (mixed versions), `NBHA_LB_MODE=l7` balances per gRPC stream. The report table
is printed at the end and written to `deploy/.state/e2e-report.tsv`. `deploy/bed.sh` documents the rest.

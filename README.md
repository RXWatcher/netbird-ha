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
| `go.mod`, `cmd/management/`, `cmd/signal/` | module root: requires `github.com/netbirdio/netbird v0.82.1`, copies upstream's `replace` lines, adds the dispatcher `replace` | A (core) |
| `ha/wgkey/`, `ha/bus/`, `ha/controller/` | shared server WG key, Redis message bus + resync, network-map controller wrapper | A (core) |
| `ha/store/`, `ha/cache/`, `ha/ephemeral/` | advisory-lock store wrapper (global lock + migrations), Sentinel-aware cache store, ephemeral cross-instance fix | B (state) |
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

TODO (A, once workstream C lands `dispatcher/`): add
`replace github.com/netbirdio/signal-dispatcher/dispatcher => ./dispatcher` to `go.mod`
(upstream's module path ends in `/dispatcher`).

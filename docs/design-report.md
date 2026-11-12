# Active-active NetBird control plane without the Enterprise license

Date 2026-11-12. Checked: upstream `netbirdio/netbird` `v0.82.1` (2026-11-10), `openzro/openzro` @ `43ee3574` (2026-10-23), `nik-dev-ops/netbird-ha` @ `f1cf7f6b`. This is not legal advice; have a lawyer confirm section 5 before a public release.

## 1. Recommendation

1. **Build our own design on v0.82.x and treat openZro as reference only.** openZro forked v0.52.2. Since then upstream has changed 1,005 files in `management/ signal/ relay/ combined/` (+166k/−44k lines, 1,310 commits). openZro's main update fan-out also cannot work on current upstream (see 3.2).
2. **Ship it as a separate Go module with zero edits to upstream files.** v0.82.1 already has the extension points NetBird seems to use for its own closed builds:
   - the dependency-injection container (`Inject[T]`, `internals/server/container.go`);
   - `SetNewServer` (`management/cmd/management.go:39`, `combined/cmd/server.go:11`);
   - `AfterInit` and `RegisterGRPCExtension` (#6894, "Generic gRPC extension seam for external modules").

   Signal's dispatcher is a separate BSD-3 module (`netbirdio/signal-dispatcher`, pinned at `6b4ac470`), so a `replace` line in our own `go.mod` swaps it.
3. **Use Valkey/Redis Sentinel for messaging and Postgres advisory locks for locking.** Multi-instance upstream already needs a shared Redis (`NB_CACHE_REDIS_ADDRESS`) for session, PKCE and one-time tokens and for the IdP cache. NATS would add a second stateful system.
4. **Deploy a warm standby first.** The spare must be stopped while idle (see section 6).
5. **Active-active effort:** about 17–22 engineer-days, then about 0.5–1 day per upstream minor release.
6. **License-safe:** build only from AGPL source, publish under AGPL with a source link, keep the BSD attributions, and do not name the product "NetBird".

## 2. openZro's HA design (based on v0.52.2)

| Concern | Implementation | Size |
|---|---|---|
| Broker | `cluster.Coordinator` (Lock, Publish, Subscribe, Close). Selected by `OPENZRO_BROKER=embedded` (in-process NATS + JetStream), `OPENZRO_NATS_URL` or `OPENZRO_REDIS_URL`. | `cluster/` 940 LOC, **0 tests** |
| Peer update fan-out | `SendUpdate` to a peer that is not local: marshal the whole `SyncResponse` and publish it on `mgmt.peer.<id>`. One subscription per local peer, plus a broadcast topic for disconnects. Channel buffer raised from 100 to 1000. | `management/server/updatechannel.go` (+260/−26) plus a disconnect file, 525 LOC |
| Shared server WireGuard key | `OPENZRO_MGMT_WG_PRIVATE_KEY`, else `WgPrivateKey` in `management.json`, else generate and persist. ADR-0011 records the failure without it: "error while decrypting Sync request message". | `grpcserver.go` `resolveManagementWgKey`, `types/config.go` +48 |
| Signal routing | `Dispatcher` interface forked from BSD-3 `signal-dispatcher@a464fd5f`. The Redis backend keeps a peer→instance key (90 s TTL, heartbeat) and one pub/sub channel per instance. | `signal/dispatcher/*` plus cmd, about 840 LOC, **0 tests** |
| Ephemeral peers | Before deleting, re-check `Status.Connected` in the database. | `ephemeral.go` +101 |
| Locks | The distributed lock is used only in `local_user.go` and for openZro's own posture-scheduler leader. Account write locks stay in-process (`sync.RWMutex`, `store/sql_store.go:179`, 76 call sites). | — |
| Relay | Kubernetes multi-pod relay: headless-Service discovery, WHO_HAS broadcast, custom TCP forwarder between pods (ADR-0014). | 2,315 LOC |
| Migrations, leader for core jobs | None. AutoMigrate runs on every replica. | — |

The HA-relevant code is about 4.5–5k LOC, about half of it the relay. Total divergence from v0.52.2 is 1,313 files, +118,757/−7,720 (dashboard excluded).

Two accuracy notes:
- ADR-0011 cites #3547 as the "management HA" issue, but #3547 is about one-way firewall rules.
- The README's "leader election and per-resource locks" claim is broader than the code: locks exist only at the two sites above.

## 3. Upstream today

### 3.1 What changed since v0.52.2

- **Dependency-injection container** (#4343, 2025-08).
- **Network-map controller and update-manager interfaces** (#4774, 2025-11). The controller only computes and sends updates for peers that have a local channel (`HasChannel` at `controller/controller.go:203, 245, 367, 380, 777, 785`).
- **Store locks removed** (#4390). Correctness now rests on database transactions and `LockingStrengthUpdate`. Only `AcquireGlobalLock` is left, at 3 account-creation sites (`user.go:1003`, `account.go:1966`, `account.go:2258`).
- **Session-aware peer status:** `MarkPeerConnectedIfNewerSession` and `MarkPeerDisconnectedIfSameSession` (`peer.go:107, 207`). This already prevents a stale disconnect from marking a reconnected peer offline.
- **Shared cache:** `NB_CACHE_REDIS_ADDRESS` with `SetNX`/`GetDel` (#7084). It uses `redis.ParseURL`, so it has **no Sentinel support**.
- **Combined `netbird-server`** (#5232, 2026-02; `combined/` is AGPL).
- **Embedded Dex** (#5008). It can store its state in Postgres (`idp/embedded.go:89`).
- **Network-map read replica:** `NB_STORE_ENGINE_POSTGRES_REPLICA_DSN` (#6919, #8302). Leave it unset for HA, because replica lag would serve stale network maps.
- **MySQL engine removed** (#7914).

### 3.2 HA scaffolding and gaps

The open code has no broker interface, cluster flag, enterprise build tag or license check. The gaps:

- **Per-process random server WG key** (`internals/shared/grpc/token_mgr.go:56`). Clients open their Sync and Job streams through separate key fetches (`shared/management/client/grpc.go:209-266`), so behind an L7 load balancer they hit instances with different keys and decryption fails.
- **No fan-out between instances.** A change made on instance A never reaches peers streaming from B.
- **Global lock is in-process only.** Two simultaneous first logins can create duplicate accounts.
- **Signal dispatcher is in-memory** (`signal/server/signal.go:76`).
- **Ephemeral cleanup does not re-check** whether a peer is still connected (`ephemeral.go:171-227`).
- **AutoMigrate runs on every boot.** Destructive migrations exist (`migration.go:450, 710` use `DropColumn`).
- **Peer jobs and reverse-proxy updates are local to one instance** (`job.Manager` is a concrete type).

openZro's fan-out does not fit because upstream never calls `SendUpdate` for peers on other instances. The right level is the controller: broadcast "account X / these peers / this reason", and let each instance recompute only for its own streams.

### 3.3 Issue #1584 and PR #5925

- In #1584, @nazarewk wrote on 2025-03-26: "There is no plan to support any additional form of HA for unlicensed self-hosting."
- @dlf-dds (2026-04-21) pointed to **PR #5925**. It is **open**, needs review, and was last updated 2026-04-21. It adds `NETBIRD_SKIP_MIGRATIONS=true` (+19/−7, `boot.go` and the activity store).
- The "locked variable" is the hard-coded `skipMigration=false`. It is a migration switch for multi-master Postgres, not a license gate.
- We do not need the PR: our module calls the exported `store.NewSqlStore(..., skipMigration)` itself.
- In #5724, @braginini wrote that open source supports "active-passive … active-active … refer to the enterprise commercial license". That is a statement of business position, not a license term.

## 4. Plan

### 4.1 Topology

- A load balancer (HAProxy L4, or a gRPC-aware L7) in front of **mgmt×2 and signal×2**.
- Shared Postgres on pg_auto_failover, primary only, which also holds Dex's storage.
- Valkey with Sentinel for messaging and the cache.
- Relays need no code: list several distinct relay URLs with the same secret. Clients already dial relays other than their home relay (`shared/relay/client/manager.go:177, 447`). Never put one URL in front of several relays.
- Use the standalone `management` and `signal` binaries rather than the combined one.

### 4.2 Shared state and how it hooks in

| What must be shared | Mechanism | Hook |
|---|---|---|
| Server WG key | One key from env or secret file, the same on every instance | `Inject[grpc.SecretsManager]`: a wrapper that overrides `GetWGKey` |
| Peer updates across instances | Redis channel carrying `{v, origin, kind, account, peerIDs, reason}`. The wrapper calls the real controller locally, then publishes. Receivers call the real controller and skip their own messages. Covers `UpdateAccountPeers`, `UpdateAffectedPeers`, `Buffer*`, `UpdateAccountPeer`, `OnPeers{Added,Updated,Deleted}`, `DisconnectPeers`. | `Inject[network_map.Controller]`, building the real `nmapcontroller.NewController` lazily |
| Lost messages | On (re)subscribe and every 10 min, re-send network maps for every account that has local streams. Clients ignore older serials. | Same wrapper |
| Cache (sessions, PKCE, tokens, IdP data) | Valkey through a Sentinel-aware store (go-redis `NewFailoverClient`), or an HAProxy that tracks the Redis master | `Inject[nbcache.Store]` |
| Account-creation lock | `pg_advisory_lock` on a dedicated connection | `Inject[store.Store]`: a wrapper that overrides `AcquireGlobalLock` |
| Migrations | Advisory lock around `NewSqlStore(..., false)` | Same wrapper |
| Ephemeral peer cleanup | Broadcast "peer connected", or skip deletion of peers the database shows as connected | `Inject[ephemeral.Manager]` |
| Signal routing | Replacement dispatcher: a Redis key mapping peer→instance (90 s TTL, 30 s heartbeat, delete only if this instance owns it), one channel per instance, a fast path for local peers, local-only fallback when Redis is down. openZro's BSD-3 Redis dispatcher can be reused with attribution. | `replace` line in our `go.mod` |
| Dex | Postgres storage | Config only |
| Login and inactivity expiry jobs | Leave as is: idempotent, and their updates now fan out. No leader election needed. | — |
| Phase 2 | Reverse-proxy update fan-out (`Inject[proxy.Controller]`). Peer jobs work only when the API call lands on the instance holding the peer's stream, unless we patch upstream. | — |

Why Valkey/Redis: it is already required for the cache, `go-redis` is already in upstream's `go.mod`, and you already operate it. NATS would add a cluster to run. Postgres LISTEN/NOTIFY would tie signal to the database and caps payloads at 8 KB.

### 4.3 Packaging and rebasing

A new module `require`s `github.com/netbirdio/netbird v0.82.x`. It copies upstream's 3 `replace` lines (dependency replaces are ignored otherwise) and adds the dispatcher `replace`. It contains:
- `cmd/management`: calls `SetNewServer(ha.Wrap)` and then `Execute()`;
- `cmd/signal`: upstream's unchanged, with the dispatcher swapped by `replace`;
- `ha/` and `dispatcher/`.

**Seam ledger:** 0 upstream lines edited. Seams used: `SetNewServer`, six `Inject` calls, and one `replace`.

Upstream changes that need attention:
- Signature changes in `NewController` or the secrets manager, and new `Controller` methods, show up as compile errors.
- New direct `SendUpdate`, `HasChannel` or `CloseChannel` callers don't break the build. A CI grep against a checked-in allowlist catches them.
- A bump of upstream's dispatcher pin means updating ours to the new API.

One caveat: `Inject` runs its constructor immediately. Build everything lazily except the store. In the combined binary an eager store would create metrics before `combined/cmd/root.go:323` injects the shared ones, which is another reason to stay on the standalone binaries.

### 4.4 Rolling upgrades

For each release, review the migration diff.
- **Additive only:** drain one instance and stop it. Its clients reconnect in about 1–20 s (backoff 0.8 s, max 10 s with jitter, `client/grpc.go:188`). Upgrade it, start it (it migrates under the lock), check health, then do the next.
- **Destructive:** upgrade all instances in one short window. Tunnels keep working throughout.
- Version the messages so releases N and N+1 can run side by side.

### 4.5 Effort

| Work | Days |
|---|---|
| Repo, image builds, CI | 1–1.5 |
| Shared WG key | 0.5 |
| Message bus with resync hook | 1.5 |
| **Controller wrapper** (riskiest) | 2.5–3 |
| Store locks | 1–1.5 |
| Sentinel-aware cache | 0.5–1 |
| Ephemeral fix | 0.5–1 |
| **Signal dispatcher** (riskiest, on the hot path) | 2–3 |
| Test setup and scenarios | 5–7 |
| Runbook | 1 |
| Manual client checks | 1 |
| **Total** | **17–22** |

Phase 2 adds 2–3 days.

Riskiest parts:
1. A missed update path leaves peers stale with no error. Updates must also be published only after the database commit.
2. Messages lost during a Sentinel failover. Resync only limits how long peers stay stale.
3. Mixed-version windows during destructive migrations.
4. Upstream churn.
5. A Valkey outage means signal works only between peers on the same instance. HAProxy `balance first` keeps everyone on one signal instance and removes most of that risk.

### 4.6 Test plan

- **Unit tests:** every controller method with a fake bus; the dispatcher against miniredis; the locks.
- **Regression:** upstream's management tests with HA turned off.
- **Docker compose setup:** pg_auto_failover (monitor + 2 nodes), Valkey×3 + Sentinel×3, HAProxy, mgmt×2, signal×2, relay×2, Dex on Postgres, and 4–6 stock client containers.

Scenarios:
1. Peers pinned to different instances connect directly and through relays.
2. A policy change, peer add/delete, user block or expiry made on mgmt-1 reaches peers on mgmt-2 in under 2 s. Check the network-map serial and connectivity.
3. `kill -9` of an instance during continuous ping and during connection setup: no decrypt errors, reconnect in under 30 s.
4. Sentinel failover during policy churn: state converges within the resync interval.
5. pg_auto_failover switchover: instances recover using a multi-host DSN with `target_session_attrs=read-write`.
6. The same new user logs in on both instances at once and exactly one account is created.
7. An ephemeral peer that moves from mgmt-1 to mgmt-2 is not deleted.
8. Rolling upgrade under load.
9. A 24 h soak comparing network maps against a single-instance run (upstream's `nmaptest` golden tooling helps).

**Client compatibility:** stock clients need no change. The protocol and endpoints are the same, and clients still see one management URL and one signal URL. The iOS and macOS apps run the same Go core. Still check them by hand: SSO login, survival when their instance is killed, and receiving a policy change made on the other instance.

## 5. Licensing and trademark

- **Licenses.**
  - The root `LICENSE` is BSD-3 except `management/ signal/ relay/ combined/`, which are AGPLv3; `proxy/` is AGPL too.
  - Each AGPL file is the verbatim text (md5 matches `LICENSES/AGPL-3.0.txt`), with no additional terms under section 7.
  - NetBird's 2025-08-04 announcement says you may modify it for internal use without sharing "as long as you do not provide the modified software as a service to other users/organizations over a network."
  - AGPL sections 10 and 12 forbid adding further restrictions.
- **The EULA does not cover the open-source build.**
  - It defines Software as "the object-code version or binaries of NetBird's on-premise software … delivered by NetBird", and §9.2 says open-source components follow their own licenses.
  - The commercial product is `ghcr.io/netbirdio/netbird-server-cloud` plus `dashboard-cloud`, which check `NETBIRD_LICENSE_KEY`. Its installer requires typing "accept" (`getting-started-enterprise.sh:523, 943, 981`).
  - §4.1 of the EULA forbids reverse engineering, and §4.3 forbids circumventing license keys.
  - **To stay clean:** build only from the GitHub source, and never pull those images, run the enterprise or migration scripts, accept a PoC, or inspect enterprise binaries.
- **Attribution.**
  - Keep upstream's LICENSE files and copyright headers.
  - The combined work is AGPL-3.0.
  - Reused BSD-3 code (signal-dispatcher, "Copyright (c) 2024, netbirdio", and any openZro files) keeps its notice, license and AUTHORS file in its directory.
  - Don't use their names to endorse the fork.
- **AGPL source offer.** People using the modified servers over the network must be offered the exact source of what's running.
  - Keep a public repo and tag every deployed build.
  - Include the build scripts and Dockerfiles.
  - Put a visible "Source" link where users will see it, for example the dashboard footer or a `/source` URL.
- **Trademark.**
  - No published NetBird trademark policy was found: `/trademark` and `/legal` on netbird.io return 404, and the repo has none. The cloud Terms say NetBird owns the trademarks relating to its Services.
  - Treat "NetBird" and the logo as NetBird GmbH's marks: give the fork its own name and publish images under your own namespace.
  - "Based on NetBird" or "works with stock NetBird clients" is fine.
  - WireGuard® belongs to Jason A. Donenfeld.

## 6. Alternatives

- **openZro has a bus factor of 1.**
  - Created 2026-04-26; 51 stars, 5 forks, 33 open issues.
  - One active maintainer (@klinux) wrote 1,256 of 1,292 fork commits; since September it is 345 commits from him against 4 from others.
  - Active but alpha, frozen on v0.52.2-era code, with no tests for its HA brokers.
- **nik-dev-ops/netbird-ha** is an AGPL fork of upstream around v0.69.
  - Worked on for three days (2026-04-23 to 25), 11 stars, now dormant.
  - Redis-based; edits about 20 upstream files (+6,154/−469) and includes 14 integration tests.
  - Its diff shows no shared WG key handling. Useful for test ideas only.
- **No other HA forks** turned up in GitHub searches or among upstream's top forks. NetBird's scaling guide (updated 2026-10-23) points HA to the enterprise license and notes that clients tolerate a management outage once connections are established.
- **A warm standby is close to free.**
  - Setup: one active management+signal, Postgres on pg_auto_failover, Dex on Postgres, 2+ relays, and automatic failover (Proxmox HA, or keepalived/HAProxy plus a start script).
  - During a failover, existing tunnels keep working; new logins, policy changes and new direct connections pause for about 30–90 s.
  - **The spare must be stopped, not running idle.** A live spare runs `EphemeralManager.LoadInitialPeers` at boot and later deletes ephemeral peers that reconnected to the active instance (`ephemeral.go:171-227`). It also runs migrations and has its own WG key.
  - Active-active adds failover with no gap, rolling upgrades where clients reconnect in about a second, and horizontal scale.
  - **Middle ground:** run the active-active build behind HAProxy `balance first`, so traffic crosses instances only during failover and upgrades.
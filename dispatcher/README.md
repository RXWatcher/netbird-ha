# Signal dispatcher with Redis routing

This module replaces `github.com/netbirdio/signal-dispatcher/dispatcher` at
`v0.0.0-20250805121659-6b4ac470ca45`, the version NetBird v0.82.1 and v0.83.0 pin. It keeps the package path
and the exported API that `signal/server` uses (`NewDispatcher`, `SendMessage`,
`ListenForMessages`), so NetBird's signal server builds unchanged. The root `go.mod` swaps it in
with:

```
replace github.com/netbirdio/signal-dispatcher/dispatcher => ./dispatcher
```

Without `NB_SIGNAL_REDIS_URL` it behaves like upstream's in-memory dispatcher. With it, several
signal instances behind one load balancer route messages to each other's peers through Redis or
Valkey.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `NB_SIGNAL_REDIS_URL` | unset (in-memory only) | Redis URL, see below |
| `NB_SIGNAL_INSTANCE_ID` | `<hostname>-<8 random hex>` | Name of this instance in Redis. Must be unique among running instances and must not contain `\|`. The default differs on every start. |
| `NB_SIGNAL_REDIS_KEY_PREFIX` | `nbha:signal:` | Prefix of every key and channel |
| `NB_SIGNAL_REDIS_PEER_TTL` | `90s` | Lifetime of a peer's key without a heartbeat |
| `NB_SIGNAL_REDIS_HEARTBEAT` | `30s` | How often an instance extends its peers' keys; at most half the TTL |
| `NB_SIGNAL_REDIS_TIMEOUT` | `1s` | Limit for each Redis call on the request path |

Invalid values stop the signal server at startup. An unreachable Redis does not: the instance
starts in local-only mode and switches to routing once Redis answers.

URL forms:

- `redis://[user:password@]host:6379[/db]`, or `rediss://` for TLS. Parsed by go-redis
  `ParseURL`, so its query options work too.
- Sentinel, when the URL has `master_name`:
  `redis[s]://[user:password@]sentinel1:26379[/db]?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379`.
  The host and each `addr` are Sentinels (port defaults to 26379); the user info authenticates
  to the data nodes. This is the format of go-redis `ParseFailoverURL` (v9.10+), with these
  options: `sentinel_username`, `sentinel_password`, `db`, `dial_timeout`, `read_timeout`,
  `write_timeout` (Go durations, or plain seconds), `pool_size`, and `skip_verify` for
  `rediss`. Any other option is an error. The client is go-redis `NewFailoverClient`.

Redis Cluster is not supported: the scripts touch keys and channels that the cluster would place
in different slots.

## How it routes

| Redis | Value |
|---|---|
| `<prefix>peer:<peer key>` | `<instance ID>\|<registration token>\|<registered at, Unix ms>`, expires after the peer TTL |
| `<prefix>inst:<instance ID>` | pub/sub channel of one instance |

- **Register.** When a peer's stream connects, the instance writes the peer's key
  unconditionally. The newest stream wins, so a peer that reconnects to another instance takes
  its key from the old one.
- **Heartbeat.** Every heartbeat one Lua call per 500 peers extends the keys this instance
  still owns. A key that disappeared (Redis restarted, or a failover lost recent writes) is
  written again. A key that names another instance means the peer moved: the local stream is
  marked stale, no longer refreshed, and messages from this instance go to the new owner.
- **Deregister.** When the stream ends, a compare-and-delete script removes the key only if it
  still holds this registration's value. A peer that moved, or reconnected to the same instance,
  keeps its newer key.
- **Send.** A peer on this instance gets the message directly, without Redis. For any other
  peer, one Lua call reads the key and publishes the message on the owner's channel. If the owner
  has no subscriber, it is gone: the message is dropped and the stale key deleted without
  waiting for the TTL.
- **Receive.** Each instance subscribes to its own channel and queues arriving messages for the
  local peer (up to 128 per peer).
- **Self-check.** Every 5 s the instance publishes a ping on its own channel. `PUBLISH` counts
  only subscribers on the node that runs it, so zero means the subscription is lost or sits on a
  demoted master; the instance subscribes again. Every confirmed subscription rewrites the keys.
- **Shutdown.** When the server's context ends, the instance deletes the keys it owns.

A message from another instance is one protobuf `EncryptedMessage` behind a version byte
(`0x01`; `0x00` is the self-check ping). Unknown versions are dropped and counted, so a later
format can be introduced while old instances still run.

## Delivery guarantees

- **At most once.** No acknowledgements and no retries inside the dispatcher. A message is lost
  when the destination is not registered anywhere, when its instance just died, during a
  pub/sub reconnect or Sentinel failover, when Redis is unreachable, or when the peer's queue
  is full. `Send` still returns success, as upstream does for a peer that is not connected. The
  NetBird client repeats its offers until the connection comes up, so a loss delays a
  connection; it does not break it. One exception to "at most once": go-redis retries a call
  that failed on the network, so if Redis ran the publish but its reply was lost, the message
  arrives twice.
- **Order.** While neither peer moves, messages from one sender to one receiver arrive in the
  order the sender's `Send` calls completed, locally and across instances: one channel per
  instance, one reader, one queue per peer. Concurrent `Send` calls have no defined order, as
  upstream. Messages in flight when the receiver moves to another instance are lost.
- **Back-pressure.** A local send waits until the receiving stream takes the message, as
  upstream. A send to another instance returns once Redis has published it; the receiving
  instance never blocks on a slow peer and drops instead.

## When Redis is unavailable

The first failed call switches the instance to local-only mode and logs a warning once:

- peers on the same instance still reach each other;
- new peers can still register; their keys are written once Redis is back;
- messages to peers elsewhere are dropped (`reason=redis_unavailable`).

Every 2 s the instance pings Redis. When it answers, the instance logs that it is back and
rewrites the keys of all its peers. A registration made during the outage yields to a newer
registration of the same peer on another instance. HAProxy `balance first` in front of the
signal instances keeps everyone on one instance, which makes an outage harmless.

## Metrics

Registered on the meter that the signal server passes in:

| Metric | Labels |
|---|---|
| `dispatcher_messages_total` | `path`: `local`, `redis_out`, `redis_in` |
| `dispatcher_dropped_messages_total` | `reason`: `not_connected`, `not_registered`, `redis_unavailable`, `no_subscriber`, `stale_owner`, `no_local_peer`, `queue_full`, `bad_envelope` |
| `dispatcher_redis_errors_total` | `op`: `ping`, `claim`, `release`, `send`, `refresh`, `self_check` |
| `dispatcher_peer_moved_total` | |
| `dispatcher_local_peers` | gauge |
| `dispatcher_redis_degraded` | gauge, 1 in local-only mode |

## Tests

Commands assume this directory is the cwd.

- `go test ./...`: unit tests against miniredis, plus NetBird's own signal server (two
  instances sharing one miniredis) driven by gRPC clients.
- `go test -tags integration -run Integration ./...`: the same scenarios against a Valkey
  container, and failovers with one master, one replica and three Sentinels. Needs Docker and
  uses the host network; the image is `valkey/valkey:8.1-alpine` unless
  `DISPATCHER_TEST_VALKEY_IMAGE` says otherwise.

To run NetBird's own signal tests against this module without editing the NetBird checkout,
copy its `go.mod` and `go.sum` elsewhere, append the replace line with an absolute path, and run
`go test -modfile=<copy>/go.mod ./signal/... ./shared/signal/client/...` from the checkout.

## License

`dispatcher.go` derives from netbirdio/signal-dispatcher (BSD 3-Clause, Copyright (c) 2024,
netbirdio); its license and authors are in [LICENSE](LICENSE) and [AUTHORS](AUTHORS). The Redis
routing (`redis.go`, `config.go`, `metrics.go`) and the tests are new and are covered by the
repository's license. openZro's Redis dispatcher was read for design only; no code was copied
from it.

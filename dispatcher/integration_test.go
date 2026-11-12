//go:build integration

package dispatcher

// Integration tests against real Valkey containers on the host network:
//
//	go test -tags integration -run Integration ./...
//
// They need a local Docker daemon and the image in DISPATCHER_TEST_VALKEY_IMAGE
// (default valkey/valkey:8.1-alpine).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func valkeyImage() string {
	if img := os.Getenv("DISPATCHER_TEST_VALKEY_IMAGE"); img != "" {
		return img
	}
	return "valkey/valkey:8.1-alpine"
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

func requireDocker(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func uniqueName(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "nbha-dispatcher-test-" + prefix + "-" + hex.EncodeToString(b)
}

// runContainer starts a Valkey process on the host network; data is never persisted, so a
// restart comes back empty.
func runContainer(t *testing.T, name string, shell string) {
	t.Helper()
	docker(t, "run", "-d", "--name", name, "--network", "host", "--entrypoint", "sh", valkeyImage(), "-c", shell)
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
}

func startValkey(t *testing.T, port int, extra string) string {
	name := uniqueName(fmt.Sprintf("valkey-%d", port))
	runContainer(t, name, fmt.Sprintf("exec valkey-server --port %d --bind 127.0.0.1 --save '' --appendonly no %s", port, extra))
	waitPing(t, fmt.Sprintf("127.0.0.1:%d", port))
	return name
}

func waitPing(t *testing.T, addr string) {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return c.Ping(ctx).Err() == nil
	}, 30*time.Second, 100*time.Millisecond, "valkey at %s never answered", addr)
}

func integrationConfig() Config {
	return Config{
		PeerTTL:           2 * time.Second,
		Heartbeat:         500 * time.Millisecond,
		OpTimeout:         500 * time.Millisecond,
		ProbeInterval:     100 * time.Millisecond,
		SelfCheckInterval: 200 * time.Millisecond,
	}
}

type valkeyBackend struct {
	name string
	addr string
}

func newValkeyBackend(t *testing.T) *valkeyBackend {
	requireDocker(t)
	port := freePort(t)
	return &valkeyBackend{name: startValkey(t, port, ""), addr: fmt.Sprintf("127.0.0.1:%d", port)}
}

func (b *valkeyBackend) client(*testing.T) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: b.addr, DialTimeout: 300 * time.Millisecond})
}

func (b *valkeyBackend) stop(t *testing.T) { docker(t, "kill", b.name) }

func (b *valkeyBackend) start(t *testing.T) {
	docker(t, "start", b.name)
	waitPing(t, b.addr)
}

func (b *valkeyBackend) expire(t *testing.T, key string) {
	c := b.client(t)
	defer c.Close()
	require.Eventually(t, func() bool {
		n, err := c.Exists(context.Background(), key).Result()
		return err == nil && n == 0
	}, 3*integrationConfig().PeerTTL, 50*time.Millisecond, "key %s never expired", key)
}

func (b *valkeyBackend) config() Config { return integrationConfig() }

func TestIntegrationValkeyScenarios(t *testing.T) {
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) { s.run(t, newValkeyBackend(t)) })
	}
}

func TestIntegrationValkeyURL(t *testing.T) {
	b := newValkeyBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Setenv(EnvRedisURL, "redis://"+b.addr+"/0")
	t.Setenv(EnvSignalInstanceID, "from-env")
	d, err := NewDispatcher(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "from-env", d.InstanceID())
	require.False(t, d.Degraded())
}

// sentinelSetup is one master, one replica and three Sentinels (quorum 2) on the host
// network.
type sentinelSetup struct {
	master, replica string // container names
	masterPort      int
	replicaPort     int
	sentinelPorts   []int
	url             string
}

func startSentinelSetup(t *testing.T) *sentinelSetup {
	requireDocker(t)
	s := &sentinelSetup{masterPort: freePort(t), replicaPort: freePort(t)}
	s.master = startValkey(t, s.masterPort, "")
	s.replica = startValkey(t, s.replicaPort, fmt.Sprintf("--replicaof 127.0.0.1 %d", s.masterPort))
	for i := 0; i < 3; i++ {
		port := freePort(t)
		s.sentinelPorts = append(s.sentinelPorts, port)
		conf := fmt.Sprintf(`port %d
bind 127.0.0.1
sentinel monitor mymaster 127.0.0.1 %d 2
sentinel down-after-milliseconds mymaster 1000
sentinel failover-timeout mymaster 4000
sentinel parallel-syncs mymaster 1
`, port, s.masterPort)
		runContainer(t, uniqueName(fmt.Sprintf("sentinel-%d", port)),
			fmt.Sprintf("printf '%%s' '%s' > /tmp/sentinel.conf && exec valkey-server /tmp/sentinel.conf --sentinel", conf))
		waitPing(t, fmt.Sprintf("127.0.0.1:%d", port))
	}
	s.url = fmt.Sprintf("redis://127.0.0.1:%d/0?master_name=mymaster&addr=127.0.0.1:%d&addr=127.0.0.1:%d&dial_timeout=300ms",
		s.sentinelPorts[0], s.sentinelPorts[1], s.sentinelPorts[2])

	// Wait until every Sentinel sees the replica and the other Sentinels, so a failover can
	// be authorized.
	for _, p := range s.sentinelPorts {
		sc := redis.NewSentinelClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", p)})
		require.Eventually(t, func() bool {
			ctx := context.Background()
			replicas, err := sc.Replicas(ctx, "mymaster").Result()
			if err != nil || len(replicas) == 0 {
				return false
			}
			sentinels, err := sc.Sentinels(ctx, "mymaster").Result()
			return err == nil && len(sentinels) == 2
		}, 30*time.Second, 200*time.Millisecond, "sentinel on %d never saw the replica and its peers", p)
		_ = sc.Close()
	}
	return s
}

func (s *sentinelSetup) masterAddr(t *testing.T) string {
	sc := redis.NewSentinelClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", s.sentinelPorts[0])})
	defer sc.Close()
	addr, err := sc.GetMasterAddrByName(context.Background(), "mymaster").Result()
	require.NoError(t, err)
	return net.JoinHostPort(addr[0], addr[1])
}

func (s *sentinelSetup) instance(t *testing.T, id string) *instance {
	t.Helper()
	cfg := integrationConfig()
	cfg.InstanceID = id
	c, err := NewRedisClient(s.url)
	require.NoError(t, err)
	cfg.Client = c
	ctx, cancel := context.WithCancel(context.Background())
	d, err := New(ctx, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		_ = c.Close()
	})
	inst := &instance{Dispatcher: d, rdb: c, cancel: cancel}
	waitSubscribed(t, inst)
	return inst
}

func TestIntegrationSentinelMasterKilled(t *testing.T) {
	s := startSentinelSetup(t)
	a := s.instance(t, "a")
	b := s.instance(t, "b")
	ra := listen(t, a.Dispatcher, "peer-a")
	rb := listen(t, b.Dispatcher, "peer-b")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-b", "b")
	send(t, b.Dispatcher, msg("peer-b", "peer-a", "before"))
	expectBody(t, ra, "before")

	oldMaster := s.masterAddr(t)
	docker(t, "kill", s.master)
	require.Eventually(t, func() bool { return s.masterAddr(t) != oldMaster }, 30*time.Second, 200*time.Millisecond, "no failover")
	require.Equal(t, fmt.Sprintf("127.0.0.1:%d", s.replicaPort), s.masterAddr(t))

	// Both directions work again through the promoted replica, and both instances
	// still own their peers' keys there.
	sendUntilReceived(t, b.Dispatcher, "peer-b", "peer-a", ra, 30*time.Second)
	sendUntilReceived(t, a.Dispatcher, "peer-a", "peer-b", rb, 30*time.Second)
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-b", "b")

	// New registrations after the failover are routed too.
	rn := listen(t, b.Dispatcher, "peer-new")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-new", "b")
	send(t, a.Dispatcher, msg("peer-a", "peer-new", "after"))
	expectBody(t, rn, "after")
}

func TestIntegrationSentinelManualFailover(t *testing.T) {
	// SENTINEL FAILOVER keeps the old master running as a replica. Subscriptions left on it
	// would still see messages replicated from the new master, but PUBLISH counts only
	// subscribers on the node that runs it, so the dispatcher must move them.
	s := startSentinelSetup(t)
	a := s.instance(t, "a")
	b := s.instance(t, "b")
	ra := listen(t, a.Dispatcher, "peer-a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")

	oldMaster := s.masterAddr(t)
	sc := redis.NewSentinelClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", s.sentinelPorts[0])})
	defer sc.Close()
	require.NoError(t, sc.Failover(context.Background(), "mymaster").Err())
	require.Eventually(t, func() bool { return s.masterAddr(t) != oldMaster }, 30*time.Second, 200*time.Millisecond, "no failover")

	sendUntilReceived(t, b.Dispatcher, "peer-b", "peer-a", ra, 30*time.Second)
	// a is subscribed on the new master: PUBLISH there reaches it.
	newMaster := redis.NewClient(&redis.Options{Addr: s.masterAddr(t)})
	defer newMaster.Close()
	require.Eventually(t, func() bool {
		n, err := newMaster.Publish(context.Background(), a.router.channel, []byte{envelopeControl}).Result()
		return err == nil && n > 0
	}, 30*time.Second, 100*time.Millisecond)
	// And its key is still there, owned by a.
	v, err := newMaster.Get(context.Background(), a.router.peerKey("peer-a")).Result()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(v, "a|"))
}

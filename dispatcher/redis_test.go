package dispatcher

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type miniBackend struct {
	mr *miniredis.Miniredis
}

func newMiniBackend(t *testing.T) *miniBackend {
	return &miniBackend{mr: miniredis.RunT(t)}
}

func (b *miniBackend) client(*testing.T) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: b.mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
}

func (b *miniBackend) stop(*testing.T) { b.mr.Close() }

func (b *miniBackend) start(t *testing.T) {
	b.mr.FlushAll()
	require.NoError(t, b.mr.Restart())
}

// expire moves miniredis' clock past the TTL; it does not expire keys in real time.
func (b *miniBackend) expire(t *testing.T, key string) {
	b.mr.FastForward(defaultPeerTTL + time.Second)
	require.False(t, b.mr.Exists(key))
}

func (b *miniBackend) config() Config {
	return Config{
		PeerTTL:           defaultPeerTTL,
		Heartbeat:         defaultHeartbeat,
		OpTimeout:         500 * time.Millisecond,
		ProbeInterval:     20 * time.Millisecond,
		SelfCheckInterval: 50 * time.Millisecond,
	}
}

func TestRedisScenarios(t *testing.T) {
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) { s.run(t, newMiniBackend(t)) })
	}
}

func TestHeartbeatExtendsTTL(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	listen(t, a.Dispatcher, "p")
	key := a.router.peerKey("p")
	waitOwner(t, a.rdb, a.Dispatcher, "p", "a")

	b.mr.FastForward(80 * time.Second)
	require.Less(t, b.mr.TTL(key), 15*time.Second)
	a.router.refreshAll()
	require.Equal(t, defaultPeerTTL, b.mr.TTL(key))
}

func TestRefreshRewritesLostKeys(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	listen(t, a.Dispatcher, "p")
	waitOwner(t, a.rdb, a.Dispatcher, "p", "a")

	// A failover to a replica that had not received the key yet.
	b.mr.FlushAll()
	a.router.refreshAll()
	waitOwner(t, a.rdb, a.Dispatcher, "p", "a")
}

func TestUnclaimedRegistrationYieldsToNewerOwner(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	listen(t, a.Dispatcher, "p")
	pc := mustLookup(t, a.Dispatcher, "p")
	a.mu.Lock()
	pc.claimed = false // as if registered while Redis was down
	a.mu.Unlock()

	// Another instance holds a newer registration for the same peer.
	newer := "b|tok|" + itoa(pc.since+1000)
	require.NoError(t, a.rdb.Set(context.Background(), a.router.peerKey("p"), newer, time.Minute).Err())
	a.router.refreshAll()
	v, _ := a.rdb.Get(context.Background(), a.router.peerKey("p")).Result()
	require.Equal(t, newer, v)
	require.True(t, a.isSuperseded(pc))

	// An older registration elsewhere loses to ours.
	b2 := newMiniBackend(t)
	a2 := newInstance(t, b2, "a")
	listen(t, a2.Dispatcher, "q")
	qc := mustLookup(t, a2.Dispatcher, "q")
	a2.mu.Lock()
	qc.claimed = false
	a2.mu.Unlock()
	require.NoError(t, a2.rdb.Set(context.Background(), a2.router.peerKey("q"), "b|tok|"+itoa(qc.since-1000), time.Minute).Err())
	a2.router.refreshAll()
	waitOwner(t, a2.rdb, a2.Dispatcher, "q", "a")
}

func TestStartsDegradedWhenRedisIsDown(t *testing.T) {
	b := newMiniBackend(t)
	addr := b.mr.Addr()
	b.mr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := b.config()
	cfg.InstanceID = "a"
	cfg.Client = redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = cfg.Client.Close() })
	d, err := New(ctx, nil, cfg)
	require.NoError(t, err)
	require.True(t, d.Degraded())

	r := listen(t, d, "p")
	r2 := listen(t, d, "p2")
	send(t, d, msg("p2", "p", "local"))
	expectBody(t, r, "local")
	send(t, d, msg("p", "elsewhere", "dropped"))
	expectNothing(t, r2, 50*time.Millisecond)

	b.start(t)
	require.Eventually(t, func() bool { return !d.Degraded() }, waitFor, 10*time.Millisecond)
	require.Eventually(t, func() bool { return b.mr.Exists(d.router.peerKey("p")) && b.mr.Exists(d.router.peerKey("p2")) }, waitFor, 10*time.Millisecond)
}

func TestShutdownReleasesKeys(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	listen(t, a.Dispatcher, "p")
	waitOwner(t, a.rdb, a.Dispatcher, "p", "a")
	a.cancel()
	require.Eventually(t, func() bool { return !b.mr.Exists(a.router.peerKey("p")) }, waitFor, 10*time.Millisecond)
}

func TestResubscribeRestoresRouting(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	r := listen(t, a.Dispatcher, "p")
	c := newInstance(t, b, "c")

	// What a failed self-check does: drop the subscription and subscribe again.
	a.router.resubscribe <- struct{}{}
	waitSubscribed(t, a)
	sendUntilReceived(t, c.Dispatcher, "c", "p", r, waitFor)
}

func TestBadEnvelopesAreDropped(t *testing.T) {
	b := newMiniBackend(t)
	a := newInstance(t, b, "a")
	r := listen(t, a.Dispatcher, "p")
	for _, payload := range []string{"", "\x07junk", "\x01\xff\xff\xff"} {
		require.NoError(t, a.rdb.Publish(context.Background(), a.router.channel, payload).Err())
	}
	c := newInstance(t, b, "c")
	send(t, c.Dispatcher, msg("c", "p", "good"))
	expectBody(t, r, "good")
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

package bus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	mu   sync.Mutex
	msgs []Envelope
	subs int
}

func (r *recorder) handle(_ context.Context, e Envelope) {
	r.mu.Lock()
	r.msgs = append(r.msgs, e)
	r.mu.Unlock()
}

func (r *recorder) subscribed(context.Context) {
	r.mu.Lock()
	r.subs++
	r.mu.Unlock()
}

func (r *recorder) snapshot() ([]Envelope, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Envelope(nil), r.msgs...), r.subs
}

func newTestBus(t *testing.T, addr, origin string) *Redis {
	t.Helper()
	b, err := NewRedis(Config{URL: "redis://" + addr, Origin: origin, PingInterval: 200 * time.Millisecond, MaxBackoff: 100 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func run(t *testing.T, b Bus, r *recorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx, r.handle, r.subscribed); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool { _, n := r.snapshot(); return n > 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestRedisRoundTripAndOriginFilter(t *testing.T) {
	mr := miniredis.RunT(t)
	a, b := newTestBus(t, mr.Addr(), "a"), newTestBus(t, mr.Addr(), "b")
	ra, rb := &recorder{}, &recorder{}
	run(t, a, ra)
	run(t, b, rb)

	ctx := context.Background()
	require.NoError(t, a.Publish(ctx, Envelope{Kind: KindUpdateAffectedPeers, Account: "acc", PeerIDs: []string{"p1", "p2"}, Reason: &Reason{Resource: "policy", Operation: "update"}}))

	require.Eventually(t, func() bool { m, _ := rb.snapshot(); return len(m) == 1 }, 5*time.Second, 10*time.Millisecond)
	m, _ := rb.snapshot()
	assert.Equal(t, Version, m[0].V)
	assert.Equal(t, "a", m[0].Origin)
	assert.Equal(t, KindUpdateAffectedPeers, m[0].Kind)
	assert.Equal(t, "acc", m[0].Account)
	assert.Equal(t, []string{"p1", "p2"}, m[0].PeerIDs)
	assert.Equal(t, &Reason{Resource: "policy", Operation: "update"}, m[0].Reason)

	// The publisher never receives its own message.
	time.Sleep(100 * time.Millisecond)
	own, _ := ra.snapshot()
	assert.Empty(t, own)
	assert.EqualValues(t, 1, a.Metrics().Published.Load())
	assert.EqualValues(t, 1, b.Metrics().Received.Load())
}

func TestRedisDropsInvalidMessages(t *testing.T) {
	mr := miniredis.RunT(t)
	b := newTestBus(t, mr.Addr(), "b")
	rb := &recorder{}
	run(t, b, rb)

	mr.Publish(DefaultChannel, "not json")
	mr.Publish(DefaultChannel, `{"v":1,"origin":"x","kind":"resync"}`) // no account
	mr.Publish(DefaultChannel, `{"v":7,"origin":"x","kind":"future_kind","account":"acc","newField":true}`)

	require.Eventually(t, func() bool { m, _ := rb.snapshot(); return len(m) == 1 }, 5*time.Second, 10*time.Millisecond)
	m, _ := rb.snapshot()
	assert.Equal(t, 7, m[0].V, "newer versions are delivered; the controller falls back to a refresh")
	assert.Equal(t, Kind("future_kind"), m[0].Kind)
	assert.EqualValues(t, 2, b.Metrics().DroppedInvalid.Load())
}

func TestRedisResubscribesAfterServerRestart(t *testing.T) {
	mr := miniredis.RunT(t)
	a, b := newTestBus(t, mr.Addr(), "a"), newTestBus(t, mr.Addr(), "b")
	rb := &recorder{}
	run(t, b, rb)

	mr.Close()
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, mr.Restart())

	require.Eventually(t, func() bool { _, n := rb.snapshot(); return n >= 2 }, 10*time.Second, 20*time.Millisecond,
		"onSubscribed must run again after reconnecting")

	require.NoError(t, a.Publish(context.Background(), Envelope{Kind: KindResync, Account: "acc"}))
	require.Eventually(t, func() bool { m, _ := rb.snapshot(); return len(m) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, b.Metrics().ReceiveErrors.Load(), int64(1))
}

func TestRedisPublishFailsWhenDown(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newTestBus(t, mr.Addr(), "a")
	mr.Close()
	err := a.Publish(context.Background(), Envelope{Kind: KindResync, Account: "acc"})
	require.Error(t, err)
	assert.EqualValues(t, 1, a.Metrics().PublishErrors.Load())
}

func TestNewClientSelectsSentinel(t *testing.T) {
	c, err := NewClient("redis://u:p@127.0.0.1:26379/2?master_name=mymaster&addr=127.0.0.2:26379&sentinel_password=spw")
	require.NoError(t, err)
	defer c.Close()
	// NewFailoverClient returns a *redis.Client whose dialer asks the sentinels.
	assert.Equal(t, "FailoverClient", c.Options().Addr)
	assert.Equal(t, 2, c.Options().DB)
	assert.Equal(t, "u", c.Options().Username)

	c2, err := NewClient("redis://user:pw@127.0.0.1:6380/3")
	require.NoError(t, err)
	defer c2.Close()
	assert.Equal(t, "127.0.0.1:6380", c2.Options().Addr)
	assert.Equal(t, 3, c2.Options().DB)

	_, err = NewClient("redis://s1?master_name=")
	require.Error(t, err, "empty master name")
	_, err = NewClient("")
	require.Error(t, err)
}

func TestConfigFromEnv(t *testing.T) {
	_, enabled, err := ConfigFromEnv()
	require.NoError(t, err)
	assert.False(t, enabled)

	t.Setenv(EnvRedisURL, "redis://s1:26379?master_name=mymaster&addr=s2:26379")
	t.Setenv(EnvInstanceID, "mgmt-1")
	c, enabled, err := ConfigFromEnv()
	require.NoError(t, err)
	assert.True(t, enabled)
	assert.Equal(t, "mgmt-1", c.Origin)

	t.Setenv(EnvRedisURL, "redis://u:secretpw@s1?master_name=m&bogus=1")
	_, enabled, err = ConfigFromEnv()
	require.Error(t, err)
	assert.True(t, enabled)
	assert.NotContains(t, err.Error(), "secretpw")
}

func TestDefaultOriginIsUnique(t *testing.T) {
	assert.NotEqual(t, defaultOrigin(), defaultOrigin())
}

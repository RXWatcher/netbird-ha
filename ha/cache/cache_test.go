package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/eko/gocache/lib/v4/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/signal-dispatcher/dispatcher"

	nbcache "github.com/netbirdio/netbird/management/server/cache"
)

func newMini(t *testing.T) (*miniredis.Miniredis, *Store) {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := New(context.Background(), "redis://"+mr.Addr()+"/0", 10)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return mr, st
}

// checkSemantics runs upstream's contract for nbcache.Store on st.
func checkSemantics(t *testing.T, st nbcache.Store) {
	t.Helper()
	ctx := context.Background()

	ok, err := st.SetNX(ctx, "nx", "first", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = st.SetNX(ctx, "nx", "second", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "SetNX must not overwrite")

	v, found, err := st.GetDel(ctx, "nx")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "first", v)
	_, found, err = st.GetDel(ctx, "nx")
	require.NoError(t, err)
	assert.False(t, found, "GetDel consumes the key")

	// The gocache interface the rest of upstream uses.
	require.NoError(t, st.Set(ctx, "k", "v", store.WithExpiration(time.Minute)))
	got, err := st.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)
	require.NoError(t, st.Delete(ctx, "k"))
	_, err = st.Get(ctx, "k")
	assert.Error(t, err)

	// One-time token: many concurrent consumers, exactly one wins.
	ok, err = st.SetNX(ctx, "token", "secret", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, found, err := st.GetDel(ctx, "token"); err == nil && found {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), winners.Load())
}

func TestStoreSemanticsOnPlainURL(t *testing.T) {
	_, st := newMini(t)
	assert.False(t, st.sentinel)
	checkSemantics(t, st)
}

func TestSetNXHonoursTTL(t *testing.T) {
	mr, st := newMini(t)
	ctx := context.Background()
	ok, err := st.SetNX(ctx, "ttl", "v", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	mr.FastForward(2 * time.Second)
	ok, err = st.SetNX(ctx, "ttl", "w", time.Second)
	require.NoError(t, err)
	assert.True(t, ok, "an expired key can be set again")
}

func TestNewFailsWhenServerIsDown(t *testing.T) {
	_, err := New(context.Background(), "redis://127.0.0.1:1/0", 10)
	require.Error(t, err)
}

func TestParseURL(t *testing.T) {
	opts, failover, err := ParseURL("redis://user:pw@10.0.0.1:6380/2")
	require.NoError(t, err)
	require.Nil(t, failover)
	assert.Equal(t, "10.0.0.1:6380", opts.Addr)
	assert.Equal(t, 2, opts.DB)

	_, failover, err = ParseURL("rediss://user:pw@s1:26380/3?master_name=mymaster&addr=s2:26379&addr=[fd00::1]:26379" +
		"&sentinel_password=spw&sentinel_username=su&dial_timeout=3&read_timeout=500ms&skip_verify=true&pool_size=7")
	require.NoError(t, err)
	assert.Equal(t, "mymaster", failover.MasterName)
	assert.Equal(t, []string{"s1:26380", "s2:26379", "[fd00::1]:26379"}, failover.SentinelAddrs)
	assert.Equal(t, "user", failover.Username)
	assert.Equal(t, "pw", failover.Password)
	assert.Equal(t, "su", failover.SentinelUsername)
	assert.Equal(t, "spw", failover.SentinelPassword)
	assert.Equal(t, 3, failover.DB)
	assert.Equal(t, 3*time.Second, failover.DialTimeout)
	assert.Equal(t, 500*time.Millisecond, failover.ReadTimeout)
	assert.Equal(t, 7, failover.PoolSize)
	require.NotNil(t, failover.TLSConfig)
	assert.True(t, failover.TLSConfig.InsecureSkipVerify)

	_, failover, err = ParseURL("redis://sentinel?master_name=m")
	require.NoError(t, err)
	assert.Equal(t, []string{"sentinel:26379"}, failover.SentinelAddrs)
	assert.Equal(t, 5*time.Second, failover.DialTimeout, "default dial timeout for Sentinel")
}

// TestURLFormatMatchesDispatcher keeps NB_CACHE_REDIS_ADDRESS and
// NB_SIGNAL_REDIS_URL in one format: every URL is accepted or refused by both
// parsers alike, and the accepted ones reach the same data nodes with the
// same credentials and database.
func TestURLFormatMatchesDispatcher(t *testing.T) {
	for _, raw := range []string{
		"redis://localhost:6379",
		"redis://user:pw@10.0.0.1:6380/2",
		"rediss://:pw@cache:6380/1",
		"redis://localhost:6379/0?dial_timeout=3s&pool_size=5",
		"unix:///run/redis.sock",
		"redis://s1:26379?master_name=m",
		"redis://sentinel?master_name=m",
		"rediss://user:pw@s1:26380/3?master_name=mymaster&addr=s2:26379&addr=[fd00::1]:26379&sentinel_password=spw&sentinel_username=su&dial_timeout=3&read_timeout=500ms&skip_verify=true&pool_size=7",
		"redis://u:p@s1:26379/4?master_name=m&db=5",
		"redis://s1?master_name=",
		"redis://s1?master_name=m&bogus=1",
		"redis://s1?master_name=m&route_by_latency=true",
		"redis://s1?master_name=m&addr=nohost",
		"redis://s1?master_name=m&db=x",
		"http://s1?master_name=m",
		"http://localhost:6379",
		"redis:///?master_name=m",
		"redis://localhost:6379/notadb",
	} {
		t.Run(raw, func(t *testing.T) {
			theirs, theirErr := dispatcher.NewRedisClient(raw)
			ours, _, ourErr := NewClient(raw, 10)
			require.Equal(t, theirErr == nil, ourErr == nil, "dispatcher err=%v, cache err=%v", theirErr, ourErr)
			if theirErr != nil {
				return
			}
			defer theirs.Close()
			defer ours.Close()
			a, b := theirs.Options(), ours.Options()
			assert.Equal(t, a.Addr, b.Addr)
			assert.Equal(t, a.Username, b.Username)
			assert.Equal(t, a.Password, b.Password)
			assert.Equal(t, a.DB, b.DB)
			assert.Equal(t, a.TLSConfig == nil, b.TLSConfig == nil)
		})
	}
}

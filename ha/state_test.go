package ha

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nbcache "github.com/netbirdio/netbird/management/server/cache"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"

	"github.com/Bloem-Studios/netbird-ha/ha/bus"
	"github.com/Bloem-Studios/netbird-ha/ha/cache"
	"github.com/Bloem-Studios/netbird-ha/ha/ephemeral"
	"github.com/Bloem-Studios/netbird-ha/ha/store"
	"github.com/Bloem-Studios/netbird-ha/ha/store/storetest"
)

func TestStateOffInjectsNothing(t *testing.T) {
	s := newBase()
	require.NoError(t, installState(s))
	for _, key := range []string{"store.Store", "cache.Store", "ephemeral.Manager", "db.Conn"} {
		_, ok := s.GetContainer(key)
		assert.False(t, ok, key)
	}
}

func TestStateOnInstallsStoreCacheAndEphemeral(t *testing.T) {
	storetest.StartForTest(t)
	dsn := storetest.NewDatabase(t)
	t.Setenv(bus.EnvRedisURL, "redis://127.0.0.1:1") // the bus is not dialled here
	t.Setenv(nbstore.PostgresDsnEnv, dsn)
	t.Setenv(nbcache.RedisStoreEnvVar, "redis://"+miniredis.RunT(t).Addr())

	s := newBase()
	s.Config.StoreConfig.Engine = types.PostgresStoreEngine
	require.NoError(t, installState(s))
	t.Cleanup(s.RunStopHandlers)

	assert.IsType(t, &store.Store{}, s.Store())
	assert.IsType(t, &cache.Store{}, s.CacheStore())
	assert.IsType(t, &ephemeral.Manager{}, s.EphemeralManager())

	// Upstream's ephemeral manager and its dependencies are still lazy.
	_, built := s.GetContainer("peers.Manager")
	assert.False(t, built)
}

func TestStateOnNeedsTheSharedCache(t *testing.T) {
	storetest.StartForTest(t)
	dsn := storetest.NewDatabase(t)
	t.Setenv(bus.EnvRedisURL, "redis://127.0.0.1:1")
	t.Setenv(nbstore.PostgresDsnEnv, dsn)
	t.Setenv(nbcache.RedisStoreEnvVar, "")
	t.Setenv("NB_IDP_CACHE_REDIS_ADDRESS", "")

	s := newBase()
	s.Config.StoreConfig.Engine = types.PostgresStoreEngine
	t.Cleanup(s.RunStopHandlers)
	require.ErrorContains(t, installState(s), nbcache.RedisStoreEnvVar)
}

package ha

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"github.com/netbirdio/netbird/management/internals/server"
	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"

	"github.com/RXWatcher/netbird-ha/ha/bus"
	"github.com/RXWatcher/netbird-ha/ha/controller"
	"github.com/RXWatcher/netbird-ha/ha/wgkey"
)

func newBase() *server.BaseServer {
	return server.NewServer(&server.Config{NbConfig: &nbconfig.Config{}})
}

func TestHAOffInjectsNothing(t *testing.T) {
	s := newBase()
	require.NoError(t, installWGKey(s))
	require.NoError(t, installController(s))
	_, ok := s.GetContainer("network_map.Controller")
	assert.False(t, ok)
	_, ok = s.GetContainer("grpc.SecretsManager")
	assert.False(t, ok)
}

func TestHAOnInjectsLazyWrappers(t *testing.T) {
	k, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	t.Setenv(wgkey.EnvKey, k.String())
	t.Setenv(bus.EnvRedisURL, "redis://127.0.0.1:1") // never dialled: nothing starts before Start

	s := newBase()
	require.NoError(t, installWGKey(s))
	require.NoError(t, installController(s))

	c, ok := s.GetContainer("network_map.Controller")
	require.True(t, ok)
	assert.IsType(t, &controller.Controller{}, c)
	sm, ok := s.GetContainer("grpc.SecretsManager")
	require.True(t, ok)
	got, _ := sm.(*wgkey.SecretsManager).GetWGKey()
	assert.Equal(t, k, got)

	// Upstream's accessors hand out the injected values.
	assert.Same(t, c, s.NetworkMapController())
	assert.Same(t, sm, s.SecretsManager())

	// Injecting must not have built any real dependency.
	for _, key := range []string{"store.Store", "telemetry.AppMetrics", "network_map.PeersUpdateManager"} {
		_, built := s.GetContainer(key)
		assert.False(t, built, key)
	}
}

func TestBusWithoutSharedKeyFails(t *testing.T) {
	t.Setenv(bus.EnvRedisURL, "redis://127.0.0.1:1")
	require.Error(t, installWGKey(newBase()))
}

func TestResyncIntervalEnv(t *testing.T) {
	t.Setenv(EnvResyncInterval, "off")
	o, err := controllerOptionsFromEnv()
	require.NoError(t, err)
	assert.Negative(t, int64(o.ResyncInterval))
	t.Setenv(EnvResyncInterval, "90s")
	o, err = controllerOptionsFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "1m30s", o.ResyncInterval.String())
	t.Setenv(EnvResyncInterval, "0")
	_, err = controllerOptionsFromEnv()
	require.Error(t, err)
}

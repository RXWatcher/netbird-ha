package ha

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	"github.com/netbirdio/netbird/management/internals/server"
	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	nbgrpc "github.com/netbirdio/netbird/management/internals/shared/grpc"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"

	"github.com/RXWatcher/netbird-ha/ha/bus"
	"github.com/RXWatcher/netbird-ha/ha/controller"
	"github.com/RXWatcher/netbird-ha/ha/wgkey"
)

func newBase() *server.BaseServer {
	return server.NewServer(&server.Config{NbConfig: &nbconfig.Config{}})
}

// key is upstream's container key for the named type T: its full package path
// and name (typeKey in management/internals/server/container.go since v0.83.0).
func key[T any]() string {
	t := reflect.TypeFor[T]()
	return t.PkgPath() + "." + t.Name()
}

type keyProbe interface{ probe() }

// TestContainerKey keeps key in step with upstream, so that the "not built"
// checks below cannot pass by looking up a key upstream never uses.
func TestContainerKey(t *testing.T) {
	s := newBase()
	server.Create(s, func() keyProbe { return nil })
	_, ok := s.GetContainer(key[keyProbe]())
	assert.True(t, ok, key[keyProbe]())
}

func TestHAOffInjectsNothing(t *testing.T) {
	s := newBase()
	require.NoError(t, installWGKey(s))
	require.NoError(t, installController(s))
	_, ok := s.GetContainer(key[network_map.Controller]())
	assert.False(t, ok)
	_, ok = s.GetContainer(key[nbgrpc.SecretsManager]())
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

	c, ok := s.GetContainer(key[network_map.Controller]())
	require.True(t, ok)
	assert.IsType(t, &controller.Controller{}, c)
	sm, ok := s.GetContainer(key[nbgrpc.SecretsManager]())
	require.True(t, ok)
	got, _ := sm.(*wgkey.SecretsManager).GetWGKey()
	assert.Equal(t, k, got)

	// Upstream's accessors hand out the injected values.
	assert.Same(t, c, s.NetworkMapController())
	assert.Same(t, sm, s.SecretsManager())

	// Injecting must not have built any real dependency.
	for _, key := range []string{key[nbstore.Store](), key[telemetry.AppMetrics](), key[network_map.PeersUpdateManager]()} {
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

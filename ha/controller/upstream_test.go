package controller

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	nmapcontroller "github.com/netbirdio/netbird/management/internals/controllers/network_map/controller"
	"github.com/netbirdio/netbird/management/internals/controllers/network_map/update_channel"
	"github.com/netbirdio/netbird/management/internals/modules/peers"
	ephemeralmanager "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral/manager"
	networkmapdb "github.com/netbirdio/netbird/management/internals/network_map_db"
	networkmapdbfactory "github.com/netbirdio/netbird/management/internals/network_map_db/factory"
	"github.com/netbirdio/netbird/management/internals/server/config"
	nbserver "github.com/netbirdio/netbird/management/server"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/settings"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"
	"github.com/netbirdio/netbird/management/server/types"

	"github.com/RXWatcher/netbird-ha/ha/bus"
)

const (
	testAccount = "bf1c8084-ba50-4ce7-9439-34653001fc3b"
	testPeer    = "ct286bi7qv930dsrrug0"
)

// upstreamTestdata finds upstream's testdata in the module cache.
func upstreamTestdata(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/netbirdio/netbird").Output()
	require.NoError(t, err)
	return filepath.Join(strings.TrimSpace(string(out)), "management", "server", "testdata", name)
}

// realInstance is one management process: its own update channels, request buffer
// and real upstream controller, on the database shared with the other instances.
type realInstance struct {
	updates *update_channel.PeersUpdateManager
	inner   *nmapcontroller.Controller
}

func newRealInstance(t *testing.T, ctx context.Context, s store.Store, nmdata *networkmapdb.NetworkMapDBStoreImpl) *realInstance {
	t.Helper()
	metrics, err := telemetry.NewDefaultAppMetrics(ctx)
	require.NoError(t, err)
	settingsManager := settings.NewMockManager(gomock.NewController(t))
	settingsManager.EXPECT().GetExtraSettings(gomock.Any(), gomock.Any()).Return(&types.ExtraSettings{}, nil).AnyTimes()
	peersManager := peers.NewManager(s, permissions.NewManager(s))
	updates := update_channel.NewPeersUpdateManager(metrics)
	inner := nmapcontroller.NewController(ctx, s, metrics, updates, nbserver.NewAccountRequestBuffer(ctx, s),
		nbserver.MockIntegratedValidator{}, settingsManager, "netbird.selfhosted",
		ephemeralmanager.NewEphemeralManager(s, peersManager), &config.Config{}, nmdata)
	return &realInstance{updates: updates, inner: inner}
}

func (r *realInstance) wrap(t *testing.T, ctx context.Context, b bus.Bus) *Controller {
	t.Helper()
	c := New(func() network_map.Controller { return r.inner }, func() network_map.PeersUpdateManager { return r.updates }, b, Options{ResyncInterval: -1})
	c.Start(ctx)
	waitResyncIdle(t, c)
	return c
}

func receive(t *testing.T, ch chan *network_map.UpdateMessage, within time.Duration) *network_map.UpdateMessage {
	t.Helper()
	select {
	case m, ok := <-ch:
		require.True(t, ok, "channel closed")
		return m
	case <-time.After(within):
		t.Fatalf("no update within %s", within)
		return nil
	}
}

// Two instances with the real upstream controller on one database: a change made
// through instance A reaches the peer whose stream is on instance B, both with the
// legacy account path and with the network-map data store used in production.
func TestRealControllerFanOut(t *testing.T) {
	for _, withNmdata := range []bool{false, true} {
		name := "account path"
		if withNmdata {
			name = "network map data store"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NB_STORE_ENGINE", "sqlite")
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			dataDir := t.TempDir()
			s, cleanup, err := store.NewTestStoreFromSQL(ctx, upstreamTestdata(t, "store.sql"), dataDir)
			require.NoError(t, err)
			t.Cleanup(cleanup)

			var nmdata *networkmapdb.NetworkMapDBStoreImpl
			if withNmdata {
				nmdata, err = networkmapdbfactory.NewNetworkMapDBStore(ctx, types.SqliteStoreEngine, dataDir, nbserver.MockIntegratedValidator{}, nil, nil, nil)
				require.NoError(t, err)
			}

			hub := bus.NewMemoryHub()
			ra, rb := newRealInstance(t, ctx, s, nmdata), newRealInstance(t, ctx, s, nmdata)
			a, b := ra.wrap(t, ctx, hub.Bus("a")), rb.wrap(t, ctx, hub.Bus("b"))

			ch, err := b.OnPeerConnected(ctx, testAccount, testPeer)
			require.NoError(t, err)
			require.True(t, rb.updates.HasChannel(testPeer))
			require.False(t, ra.updates.HasChannel(testPeer))

			// Without the wrapper, upstream's controller on A does nothing for B's stream.
			require.NoError(t, ra.inner.UpdateAccountPeers(ctx, testAccount, policyUpdate))
			select {
			case <-ch:
				t.Fatal("upstream alone must not reach another instance's stream")
			case <-time.After(300 * time.Millisecond):
			}

			require.NoError(t, a.UpdateAccountPeers(ctx, testAccount, policyUpdate))
			m := receive(t, ch, 2*time.Second)
			assert.Equal(t, network_map.MessageTypeNetworkMap, m.MessageType)
			require.NotNil(t, m.Update.GetNetworkMap())

			require.NoError(t, a.UpdateAffectedPeers(ctx, testAccount, []string{testPeer}))
			receive(t, ch, 2*time.Second)

			// Upstream reports "doesn't have a channel" for a peer streaming elsewhere;
			// the wrapper keeps that local result and still fans out.
			_ = a.UpdateAccountPeer(ctx, testAccount, testPeer)
			receive(t, ch, 2*time.Second)

			// Deleting the peer through A sends the empty map on B and closes the stream.
			require.NoError(t, a.OnPeersDeleted(ctx, testAccount, []string{testPeer}, nil))
			m = receive(t, ch, 2*time.Second)
			assert.True(t, m.Update.GetNetworkMap().GetRemotePeersIsEmpty())
			require.Eventually(t, func() bool { return !rb.updates.HasChannel(testPeer) }, 2*time.Second, 5*time.Millisecond)
		})
	}
}

// DisconnectPeers (user blocked, login expired) made on A closes B's stream.
func TestRealControllerDisconnectFanOut(t *testing.T) {
	t.Setenv("NB_STORE_ENGINE", "sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s, cleanup, err := store.NewTestStoreFromSQL(ctx, upstreamTestdata(t, "store.sql"), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	hub := bus.NewMemoryHub()
	ra, rb := newRealInstance(t, ctx, s, nil), newRealInstance(t, ctx, s, nil)
	a, b := ra.wrap(t, ctx, hub.Bus("a")), rb.wrap(t, ctx, hub.Bus("b"))

	ch, err := b.OnPeerConnected(ctx, testAccount, testPeer)
	require.NoError(t, err)
	a.DisconnectPeers(ctx, testAccount, []string{testPeer})
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream channel must be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("stream not closed")
	}
}

package ephemeral

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	peerlivenesscontroller "github.com/netbirdio/netbird/management/internals/controllers/peerliveness/controller"
	"github.com/netbirdio/netbird/management/internals/modules/peers"
	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"
	"github.com/netbirdio/netbird/management/internals/shared/db"
	"github.com/netbirdio/netbird/management/server/activity"
	"github.com/netbirdio/netbird/management/server/mock_server"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/permissions"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"

	hastore "github.com/RXWatcher/netbird-ha/ha/store"
	"github.com/RXWatcher/netbird-ha/ha/store/storetest"
)

func TestMain(m *testing.M) { storetest.Main(m) }

const accountID = "acc-1"

// fakeInner records what reaches upstream's manager.
type fakeInner struct {
	mu           sync.Mutex
	connected    []string
	disconnected []string
	loaded       int
	stopped      int
}

func (f *fakeInner) LoadInitialPeers(context.Context) { f.mu.Lock(); f.loaded++; f.mu.Unlock() }
func (f *fakeInner) Stop()                            { f.mu.Lock(); f.stopped++; f.mu.Unlock() }
func (f *fakeInner) OnPeerConnected(_ context.Context, p *nbpeer.Peer) {
	f.mu.Lock()
	f.connected = append(f.connected, p.ID)
	f.mu.Unlock()
}
func (f *fakeInner) OnPeerDisconnected(_ context.Context, p *nbpeer.Peer) {
	f.mu.Lock()
	f.disconnected = append(f.disconnected, p.ID)
	f.mu.Unlock()
}
func (f *fakeInner) disconnects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.disconnected...)
}

type env struct {
	dsn    string
	store  nbstore.Store
	locker *hastore.Locker
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := storetest.NewDatabase(t)
	ctx := context.Background()
	conn, err := db.OpenPostgres(ctx, dsn, db.DefaultPoolConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	pool, err := hastore.OpenLockPool(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	locker := hastore.NewLocker(pool)
	st, _, err := hastore.OpenSQLStore(ctx, conn, nil, locker, hastore.MigrationOptions{Version: "test"})
	require.NoError(t, err)

	account := &types.Account{
		Id: accountID, CreatedBy: "owner", Domain: "example.com", DomainCategory: types.PrivateCategory,
		Network: types.NewNetwork(), Peers: map[string]*nbpeer.Peer{},
		Users:    map[string]*types.User{"owner": {Id: "owner", AccountID: accountID, Role: types.UserRoleOwner}},
		Settings: &types.Settings{},
	}
	//nolint:staticcheck // the simplest way to seed an account, as upstream's own tests do
	require.NoError(t, st.SaveAccount(ctx, account))
	return &env{dsn: dsn, store: hastore.Wrap(st, locker, nil), locker: locker}
}

var ipCounter byte

// instance registers a management instance in upstream's liveness table.
func (e *env) instance(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, e.store.RegisterInstance(context.Background(), id, "host-"+id))
}

// kill makes the instance look dead: no heartbeat for an hour.
func (e *env) kill(t *testing.T, id string) {
	t.Helper()
	_, err := e.locker.Pool().Exec(context.Background(),
		`UPDATE management_instances SET last_seen = now() - interval '1 hour' WHERE id = $1`, id)
	require.NoError(t, err)
}

// addPeer creates an ephemeral peer. With an instance it is connected there
// with a new session; without one it has the session-less connection record
// that a new peer, or the v0.83.0 backfill, starts with.
func (e *env) addPeer(t *testing.T, id, instance string) *nbpeer.Peer {
	t.Helper()
	ipCounter++
	p := &nbpeer.Peer{
		ID: id, AccountID: accountID, Key: "key-" + id, Name: id, DNSLabel: id,
		IP: netip.AddrFrom4([4]byte{100, 64, 0, ipCounter}), Ephemeral: true,
		Status: &nbpeer.PeerStatus{},
	}
	require.NoError(t, e.store.AddPeerToAccount(context.Background(), p))
	if instance != "" {
		e.connect(t, id, instance)
	}
	return e.peer(t, id)
}

func (e *env) connect(t *testing.T, id, instance string) {
	t.Helper()
	ok, err := e.store.ConnectPeer(context.Background(), accountID, id, instance, time.Now().UnixNano())
	require.NoError(t, err)
	require.True(t, ok)
}

func (e *env) peer(t *testing.T, id string) *nbpeer.Peer {
	t.Helper()
	p, err := e.store.GetPeerByID(context.Background(), nbstore.LockingStrengthNone, accountID, id)
	require.NoError(t, err)
	return p
}

func (e *env) manager(t *testing.T) (*Manager, *fakeInner) {
	t.Helper()
	inner := &fakeInner{}
	m := New(func() upstream.Manager { return inner }, func() nbstore.Store { return e.store }, Config{ReconcileInterval: time.Hour})
	t.Cleanup(m.Stop)
	return m, inner
}

func TestReconcileLeavesConnectedPeersAlone(t *testing.T) {
	e := newEnv(t)
	m, inner := e.manager(t)
	e.instance(t, "live")
	e.addPeer(t, "p", "live")
	require.True(t, e.peer(t, "p").Status.Connected)

	n, err := m.Reconcile(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, inner.disconnects())
}

func TestReconcileSchedulesPeersOfADeadInstance(t *testing.T) {
	e := newEnv(t)
	m, inner := e.manager(t)
	e.instance(t, "dead")
	e.instance(t, "live")
	e.addPeer(t, "orphan", "dead")
	e.addPeer(t, "kept", "live")
	// Never held by a v0.83.0+ instance (or held by an older release during
	// a rolling upgrade): left to upstream's own boot-time load.
	e.addPeer(t, "sessionless", "")
	e.kill(t, "dead")
	require.False(t, e.peer(t, "orphan").Status.Connected, "upstream reads a dead instance's peer as disconnected")

	n, err := m.Reconcile(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"orphan"}, inner.disconnects(), "handed to upstream's manager for deletion")
}

func TestReconcileSkipsAPeerThatMoved(t *testing.T) {
	e := newEnv(t)
	m, inner := e.manager(t)
	e.instance(t, "dead")
	e.instance(t, "live")
	e.addPeer(t, "mover", "dead")
	e.kill(t, "dead")
	e.connect(t, "mover", "live")

	n, err := m.Reconcile(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, inner.disconnects())
}

func TestReconcileSchedulesEveryOrphan(t *testing.T) {
	e := newEnv(t)
	m, inner := e.manager(t)
	e.instance(t, "dead")
	for i := range 10 {
		e.addPeer(t, fmt.Sprintf("orphan-%d", i), "dead")
	}
	e.kill(t, "dead")

	n, err := m.Reconcile(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Len(t, inner.disconnects(), 10)
}

func TestLoadInitialPeersDelegatesAndStopStops(t *testing.T) {
	e := newEnv(t)
	inner := &fakeInner{}
	m := New(func() upstream.Manager { return inner }, func() nbstore.Store { return e.store }, Config{})
	m.LoadInitialPeers(context.Background())
	m.Stop()
	assert.Equal(t, 1, inner.loaded)
	assert.Equal(t, 1, inner.stopped)
}

// TestUpstreamDeleteSkipsConnectedPeers pins the upstream behaviour this
// package builds on: DeletePeers with checkConnected, which upstream's
// ephemeral manager uses, keeps a peer that reads connected (for example
// after it moved to another instance) and deletes one that has been gone for
// the ephemeral lifetime.
func TestUpstreamDeleteSkipsConnectedPeers(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	nmc := network_map.NewMockController(ctrl)
	nmc.EXPECT().GetDNSDomain(gomock.Any()).Return("netbird.cloud").AnyTimes()

	pm := peers.NewManager(e.store, permissions.NewManager(e.store), peerlivenesscontroller.NewController(e.store))
	pm.SetNetworkMapController(nmc)
	pm.SetAccountManager(&mock_server.MockAccountManager{})

	e.instance(t, "live")
	e.instance(t, "dead")
	e.addPeer(t, "moved", "live")
	e.addPeer(t, "gone", "dead")
	e.kill(t, "dead")
	_, err := e.locker.Pool().Exec(ctx, `UPDATE peer_connections SET last_seen = now() - $1::interval WHERE peer_id IN ('moved', 'gone')`,
		fmt.Sprintf("%d seconds", int(upstream.EphemeralLifeTime.Seconds())+60))
	require.NoError(t, err)

	require.NoError(t, pm.DeletePeers(ctx, accountID, []string{"moved", "gone"}, activity.SystemInitiator, true))
	_, err = e.store.GetPeerByID(ctx, nbstore.LockingStrengthNone, accountID, "moved")
	assert.NoError(t, err, "a connected peer is kept")
	_, err = e.store.GetPeerByID(ctx, nbstore.LockingStrengthNone, accountID, "gone")
	assert.Error(t, err, "a peer gone for the lifetime is deleted")
}

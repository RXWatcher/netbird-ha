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
	"github.com/netbirdio/netbird/management/internals/modules/peers"
	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"
	"github.com/netbirdio/netbird/management/internals/shared/db"
	"github.com/netbirdio/netbird/management/server/activity"
	"github.com/netbirdio/netbird/management/server/mock_server"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/permissions"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"

	hastore "github.com/Bloem-Studios/netbird-ha/ha/store"
	"github.com/Bloem-Studios/netbird-ha/ha/store/storetest"
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
	st, _, err := hastore.OpenSQLStore(ctx, conn, nil, locker, hastore.MigrationOptions{Version: "v0.82.1"})
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

// addPeer creates an ephemeral peer, connected with session when session > 0.
func (e *env) addPeer(t *testing.T, id string, session int64) *nbpeer.Peer {
	t.Helper()
	ipCounter++
	p := &nbpeer.Peer{
		ID: id, AccountID: accountID, Key: "key-" + id, Name: id, DNSLabel: id,
		IP: netip.AddrFrom4([4]byte{100, 64, 0, ipCounter}), Ephemeral: true,
		Status: &nbpeer.PeerStatus{LastSeen: time.Now()},
	}
	require.NoError(t, e.store.AddPeerToAccount(context.Background(), p))
	if session > 0 {
		ok, err := e.store.MarkPeerConnectedIfNewerSession(context.Background(), accountID, id, session)
		require.NoError(t, err)
		require.True(t, ok)
	}
	got, err := e.store.GetPeerByID(context.Background(), nbstore.LockingStrengthNone, accountID, id)
	require.NoError(t, err)
	return got
}

// age makes the peer look last seen long ago.
func (e *env) age(t *testing.T, id string) {
	t.Helper()
	_, err := e.locker.Pool().Exec(context.Background(),
		`UPDATE peers SET peer_status_last_seen = now() - interval '1 hour' WHERE id = $1`, id)
	require.NoError(t, err)
}

func (e *env) peer(t *testing.T, id string) *nbpeer.Peer {
	t.Helper()
	p, err := e.store.GetPeerByID(context.Background(), nbstore.LockingStrengthNone, accountID, id)
	require.NoError(t, err)
	return p
}

func (e *env) manager(t *testing.T, instance string) (*Manager, *fakeInner) {
	t.Helper()
	inner := &fakeInner{}
	m := New(func() upstream.Manager { return inner }, func() nbstore.Store { return e.store }, e.locker, instance,
		Config{HeartbeatInterval: time.Hour, StaleAfter: 2 * time.Second, ReconcileInterval: time.Hour})
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(m.Stop)
	return m, inner
}

func newSession() int64 { return time.Now().UnixNano() }

func TestReconcileLeavesLiveSessionsAlone(t *testing.T) {
	e := newEnv(t)
	a, innerA := e.manager(t, "mgmt-a")
	b, innerB := e.manager(t, "mgmt-b")
	ctx := context.Background()

	live := e.addPeer(t, "live", newSession())
	a.OnPeerConnected(ctx, live)
	assert.Equal(t, []string{"live"}, innerA.connected, "upstream still sees the connect")
	a.Heartbeat(ctx)
	e.age(t, "live")

	taken, err := b.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, taken)
	assert.True(t, e.peer(t, "live").Status.Connected)
	assert.Empty(t, innerB.disconnects())
}

func TestReconcileTakesOverPeersOfADeadInstance(t *testing.T) {
	e := newEnv(t)
	b, innerB := e.manager(t, "mgmt-b")
	ctx := context.Background()

	// "dead" is connected to an instance that was killed: the database says
	// connected and nobody refreshes its session.
	dead := e.addPeer(t, "dead", newSession())
	e.age(t, "dead")
	// Recently connected peers get a grace period before anyone judges
	// them, even without a heartbeat yet.
	e.addPeer(t, "fresh", newSession())

	taken, err := b.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, taken)
	assert.False(t, e.peer(t, "dead").Status.Connected)
	assert.Equal(t, int64(0), e.peer(t, "dead").Status.SessionStartedAt)
	assert.Equal(t, []string{dead.ID}, innerB.disconnects(), "handed to upstream's manager for deletion")
	assert.True(t, e.peer(t, "fresh").Status.Connected)

	// Nothing left to take.
	taken, err = b.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, taken)
}

func TestReconcileIgnoresSessionsFromBeforeTheRegistry(t *testing.T) {
	e := newEnv(t)
	// A session that started before any HA instance ran: it may belong to
	// a stock instance, which never heartbeats.
	e.addPeer(t, "old", time.Now().Add(-time.Hour).UnixNano())
	e.age(t, "old")
	b, innerB := e.manager(t, "mgmt-b")

	taken, err := b.Reconcile(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, taken)
	assert.True(t, e.peer(t, "old").Status.Connected)
	assert.Empty(t, innerB.disconnects())
}

func TestConcurrentReconcilesTakeAPeerOnce(t *testing.T) {
	e := newEnv(t)
	const instances = 4
	managers := make([]*Manager, instances)
	inners := make([]*fakeInner, instances)
	for i := range instances {
		managers[i], inners[i] = e.manager(t, fmt.Sprintf("mgmt-%d", i))
	}
	for i := range 10 {
		e.addPeer(t, fmt.Sprintf("dead-%d", i), newSession())
		e.age(t, fmt.Sprintf("dead-%d", i))
	}

	var wg sync.WaitGroup
	for _, m := range managers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Reconcile(context.Background())
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	total := 0
	seen := map[string]bool{}
	for _, inner := range inners {
		for _, id := range inner.disconnects() {
			assert.False(t, seen[id], "%s scheduled twice", id)
			seen[id] = true
			total++
		}
	}
	assert.Equal(t, 10, total)
}

func TestPeerThatReconnectedIsNotTakenOver(t *testing.T) {
	e := newEnv(t)
	b, innerB := e.manager(t, "mgmt-b")
	ctx := context.Background()

	p := e.addPeer(t, "mover", newSession())
	e.age(t, "mover")
	stale := p.Status.SessionStartedAt
	// It reconnects elsewhere between the candidate query and the fenced
	// write: the stale session no longer matches.
	ok, err := e.store.MarkPeerConnectedIfNewerSession(ctx, accountID, "mover", newSession())
	require.NoError(t, err)
	require.True(t, ok)
	updated, err := e.store.MarkPeerDisconnectedIfSameSession(ctx, accountID, "mover", stale)
	require.NoError(t, err)
	assert.False(t, updated, "upstream's fencing rejects the stale session")

	// The new session is recent, so the reconcile leaves it alone too.
	taken, err := b.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, taken)
	assert.True(t, e.peer(t, "mover").Status.Connected)
	assert.Empty(t, innerB.disconnects())
}

func TestPartitionedInstanceReclaimsItsPeers(t *testing.T) {
	e := newEnv(t)
	a, _ := e.manager(t, "mgmt-a")
	b, _ := e.manager(t, "mgmt-b")
	ctx := context.Background()

	p := e.addPeer(t, "kept", newSession())
	a.OnPeerConnected(ctx, p)
	a.Heartbeat(ctx)

	// A loses the database for longer than StaleAfter; B takes the peer.
	_, err := e.locker.Pool().Exec(ctx, `UPDATE `+sessionsTable+` SET heartbeat_at = now() - interval '1 hour'`)
	require.NoError(t, err)
	e.age(t, "kept")
	a.mu.Lock()
	a.heartbeatFailed = true
	a.mu.Unlock()
	taken, err := b.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, taken)
	require.False(t, e.peer(t, "kept").Status.Connected)

	// A's database comes back: its next heartbeat re-marks its live
	// stream's peer connected with the same session, so upstream's
	// connected check stops the deletion.
	a.Heartbeat(ctx)
	got := e.peer(t, "kept")
	assert.True(t, got.Status.Connected)
	assert.Equal(t, p.Status.SessionStartedAt, got.Status.SessionStartedAt)
	a.mu.Lock()
	assert.False(t, a.heartbeatFailed)
	a.mu.Unlock()
}

func TestDisconnectAndStopRemoveRegistryRows(t *testing.T) {
	e := newEnv(t)
	a, innerA := e.manager(t, "mgmt-a")
	ctx := context.Background()
	rows := func() int {
		var n int
		require.NoError(t, e.locker.Pool().QueryRow(ctx, `SELECT count(*) FROM `+sessionsTable).Scan(&n))
		return n
	}

	p1 := e.addPeer(t, "p1", newSession())
	p2 := e.addPeer(t, "p2", newSession())
	a.OnPeerConnected(ctx, p1)
	a.OnPeerConnected(ctx, p2)
	// A non-ephemeral peer is not tracked.
	notEphemeral := *p1
	notEphemeral.ID, notEphemeral.Ephemeral = "static", false
	a.OnPeerConnected(ctx, &notEphemeral)
	a.Heartbeat(ctx)
	assert.Equal(t, 2, rows())

	a.OnPeerDisconnected(ctx, p1)
	assert.Equal(t, 1, rows())
	assert.Equal(t, []string{"p1"}, innerA.disconnects())

	a.Stop()
	assert.Equal(t, 0, rows())
	assert.Equal(t, 1, innerA.stopped)
}

func TestLoadInitialPeersStartsRegistryAndDelegates(t *testing.T) {
	e := newEnv(t)
	inner := &fakeInner{}
	m := New(func() upstream.Manager { return inner }, func() nbstore.Store { return e.store }, e.locker, "mgmt-a", Config{})
	m.LoadInitialPeers(context.Background())
	t.Cleanup(m.Stop)
	assert.Equal(t, 1, inner.loaded)
	assert.NotZero(t, m.epoch)
}

// TestUpstreamDeleteSkipsConnectedPeers pins the upstream behaviour this
// package builds on: DeletePeers with checkConnected, which upstream's
// ephemeral manager uses, keeps a peer the database shows connected (for
// example after it moved to another instance) and deletes one that has been
// gone for the ephemeral lifetime.
func TestUpstreamDeleteSkipsConnectedPeers(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	nmc := network_map.NewMockController(ctrl)
	nmc.EXPECT().GetDNSDomain(gomock.Any()).Return("netbird.cloud").AnyTimes()

	pm := peers.NewManager(e.store, permissions.NewManager(e.store))
	pm.SetNetworkMapController(nmc)
	pm.SetAccountManager(&mock_server.MockAccountManager{})

	e.addPeer(t, "moved", newSession())
	e.age(t, "moved")
	e.addPeer(t, "gone", 0)
	_, err := e.locker.Pool().Exec(ctx, `UPDATE peers SET peer_status_last_seen = now() - $1::interval WHERE id = 'gone'`,
		fmt.Sprintf("%d seconds", int(upstream.EphemeralLifeTime.Seconds())+60))
	require.NoError(t, err)

	require.NoError(t, pm.DeletePeers(ctx, accountID, []string{"moved", "gone"}, activity.SystemInitiator, true))
	_, err = e.store.GetPeerByID(ctx, nbstore.LockingStrengthNone, accountID, "moved")
	assert.NoError(t, err, "a connected peer is kept")
	_, err = e.store.GetPeerByID(ctx, nbstore.LockingStrengthNone, accountID, "gone")
	assert.Error(t, err, "a peer gone for the lifetime is deleted")
}

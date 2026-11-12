package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	"github.com/netbirdio/netbird/management/server/types"

	"github.com/Bloem-Studios/netbird-ha/ha/bus"
)

// fakeUpdates is the per-instance stream registry (upstream's PeersUpdateManager).
type fakeUpdates struct {
	network_map.PeersUpdateManager
	mu    sync.Mutex
	chans map[string]chan *network_map.UpdateMessage
}

func (f *fakeUpdates) HasChannel(peerID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.chans[peerID]
	return ok
}

func (f *fakeUpdates) open(peerID string) chan *network_map.UpdateMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *network_map.UpdateMessage, 1)
	f.chans[peerID] = ch
	return ch
}

func (f *fakeUpdates) close(peerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.chans, peerID)
}

// fakeInner records every call made on the real controller.
type fakeInner struct {
	network_map.Controller
	updates *fakeUpdates
	mu      sync.Mutex
	calls   []string
}

func (f *fakeInner) rec(format string, args ...any) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf(format, args...))
	f.mu.Unlock()
}

func (f *fakeInner) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.calls...)
	sort.Strings(out)
	return out
}

func (f *fakeInner) reset() {
	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
}

func r(reason types.UpdateReason) string {
	return string(reason.Resource) + "/" + string(reason.Operation)
}
func j(ids []string) string { return strings.Join(ids, ",") }

func (f *fakeInner) UpdateAccountPeers(_ context.Context, a string, reason types.UpdateReason) error {
	f.rec("UpdateAccountPeers %s %s", a, r(reason))
	return nil
}
func (f *fakeInner) BufferUpdateAccountPeers(_ context.Context, a string, reason types.UpdateReason) error {
	f.rec("BufferUpdateAccountPeers %s %s", a, r(reason))
	return nil
}
func (f *fakeInner) UpdateAffectedPeers(_ context.Context, a string, ids []string) error {
	f.rec("UpdateAffectedPeers %s %s", a, j(ids))
	return nil
}
func (f *fakeInner) BufferUpdateAffectedPeers(_ context.Context, a string, ids []string, reason types.UpdateReason) error {
	f.rec("BufferUpdateAffectedPeers %s %s %s", a, j(ids), r(reason))
	return nil
}
func (f *fakeInner) UpdateAccountPeer(_ context.Context, a, p string) error {
	f.rec("UpdateAccountPeer %s %s", a, p)
	return nil
}
func (f *fakeInner) OnPeersAdded(_ context.Context, a string, ids, aff []string) error {
	f.rec("OnPeersAdded %s %s %s", a, j(ids), j(aff))
	return nil
}
func (f *fakeInner) OnPeersUpdated(_ context.Context, a string, ids, aff []string) error {
	f.rec("OnPeersUpdated %s %s %s", a, j(ids), j(aff))
	return nil
}
func (f *fakeInner) OnPeersDeleted(_ context.Context, a string, ids, aff []string) error {
	f.rec("OnPeersDeleted %s %s %s", a, j(ids), j(aff))
	return nil
}
func (f *fakeInner) DisconnectPeers(_ context.Context, a string, ids []string) {
	f.rec("DisconnectPeers %s %s", a, j(ids))
	for _, id := range ids {
		f.updates.close(id)
	}
}
func (f *fakeInner) OnPeerConnected(_ context.Context, _ string, p string) (chan *network_map.UpdateMessage, error) {
	return f.updates.open(p), nil
}
func (f *fakeInner) OnPeerDisconnected(_ context.Context, _ string, p string, _ chan *network_map.UpdateMessage) bool {
	f.updates.close(p)
	return true
}
func (f *fakeInner) GetDNSDomain(*types.Settings) string { return "netbird.test" }
func (f *fakeInner) CountStreams() int {
	f.updates.mu.Lock()
	defer f.updates.mu.Unlock()
	return len(f.updates.chans)
}

type instance struct {
	c     *Controller
	inner *fakeInner
	upd   *fakeUpdates
	bus   *bus.Memory
}

func newInstance(t *testing.T, hub *bus.MemoryHub, origin string, opts Options) *instance {
	t.Helper()
	upd := &fakeUpdates{chans: map[string]chan *network_map.UpdateMessage{}}
	inner := &fakeInner{updates: upd}
	b := hub.Bus(origin)
	if opts.ResyncInterval == 0 {
		opts.ResyncInterval = -1
	}
	c := New(func() network_map.Controller { return inner }, func() network_map.PeersUpdateManager { return upd }, b, opts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.Start(ctx)
	waitResyncIdle(t, c)
	return &instance{c: c, inner: inner, upd: upd, bus: b}
}

// waitResyncIdle waits for the initial (empty) resync after Start to finish, so it
// cannot race the test.
func waitResyncIdle(t *testing.T, c *Controller) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.resyncMu.Lock()
		defer c.resyncMu.Unlock()
		return c.metrics.Resyncs.Load() > 0 && !c.resyncRunning
	}, time.Second, time.Millisecond)
}

// connect opens a stream for peer on this instance, as upstream's Sync handler does.
func (i *instance) connect(t *testing.T, account, peer string) {
	t.Helper()
	_, err := i.c.OnPeerConnected(context.Background(), account, peer)
	require.NoError(t, err)
}

func eventuallyCalls(t *testing.T, i *instance, want ...string) {
	t.Helper()
	sort.Strings(want)
	ok := assert.Eventually(t, func() bool { return assert.ObjectsAreEqual(want, i.inner.Calls()) }, 2*time.Second, 2*time.Millisecond)
	require.True(t, ok, "want %v, got %v", want, i.inner.Calls())
}

func settle() { time.Sleep(50 * time.Millisecond) }

var policyUpdate = types.UpdateReason{Resource: types.UpdateResourcePolicy, Operation: types.UpdateOperationUpdate}

// Every update method of network_map.Controller runs on the local real controller
// once, is replayed on an instance holding a stream of the account, and is skipped by
// an instance without one. The publisher never replays its own message.
func TestEveryUpdateMethodFansOut(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		call func(c *Controller)
		want string
	}{
		{"UpdateAccountPeers", func(c *Controller) { _ = c.UpdateAccountPeers(ctx, "acc", policyUpdate) },
			"UpdateAccountPeers acc policy/update"},
		{"BufferUpdateAccountPeers", func(c *Controller) { _ = c.BufferUpdateAccountPeers(ctx, "acc", policyUpdate) },
			"BufferUpdateAccountPeers acc policy/update"},
		{"UpdateAffectedPeers", func(c *Controller) { _ = c.UpdateAffectedPeers(ctx, "acc", []string{"p1", "p2"}) },
			"UpdateAffectedPeers acc p1,p2"},
		{"BufferUpdateAffectedPeers", func(c *Controller) {
			_ = c.BufferUpdateAffectedPeers(ctx, "acc", []string{"p1"}, policyUpdate)
		}, "BufferUpdateAffectedPeers acc p1 policy/update"},
		{"UpdateAccountPeer", func(c *Controller) { _ = c.UpdateAccountPeer(ctx, "acc", "p1") },
			"UpdateAccountPeer acc p1"},
		{"OnPeersAdded", func(c *Controller) { _ = c.OnPeersAdded(ctx, "acc", []string{"new"}, []string{"p1"}) },
			"OnPeersAdded acc new p1"},
		{"OnPeersUpdated", func(c *Controller) { _ = c.OnPeersUpdated(ctx, "acc", []string{"x"}, []string{"p1"}) },
			"OnPeersUpdated acc x p1"},
		{"OnPeersDeleted", func(c *Controller) { _ = c.OnPeersDeleted(ctx, "acc", []string{"p1"}, []string{"p2"}) },
			"OnPeersDeleted acc p1 p2"},
		{"DisconnectPeers", func(c *Controller) { c.DisconnectPeers(ctx, "acc", []string{"p1"}) },
			"DisconnectPeers acc p1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub := bus.NewMemoryHub()
			a := newInstance(t, hub, "a", Options{})
			b := newInstance(t, hub, "b", Options{})
			c := newInstance(t, hub, "c", Options{})
			b.connect(t, "acc", "p1")
			b.inner.reset()

			tc.call(a.c)

			eventuallyCalls(t, b, tc.want)
			settle()
			assert.Equal(t, []string{tc.want}, a.inner.Calls(), "local call exactly once, no echo")
			assert.Empty(t, c.inner.Calls(), "instance without streams of the account skips it")
			assert.EqualValues(t, 1, c.c.Metrics().Skipped.Load())
		})
	}
}

func TestNothingPublishedForEmptyPeerLists(t *testing.T) {
	ctx := context.Background()
	hub := bus.NewMemoryHub()
	a := newInstance(t, hub, "a", Options{})
	b := newInstance(t, hub, "b", Options{})
	b.connect(t, "acc", "p1")

	_ = a.c.UpdateAffectedPeers(ctx, "acc", nil)
	_ = a.c.BufferUpdateAffectedPeers(ctx, "acc", nil, policyUpdate)
	_ = a.c.OnPeersAdded(ctx, "acc", []string{"new"}, nil)
	_ = a.c.OnPeersUpdated(ctx, "acc", []string{"x"}, nil)
	a.c.DisconnectPeers(ctx, "acc", nil)
	settle()
	assert.Zero(t, a.bus.Metrics().Published.Load()+b.bus.Metrics().Received.Load())
	assert.Empty(t, b.inner.Calls())
}

func TestUpdateAccountPeerStaysLocalWhenStreamIsHere(t *testing.T) {
	hub := bus.NewMemoryHub()
	a := newInstance(t, hub, "a", Options{})
	b := newInstance(t, hub, "b", Options{})
	a.connect(t, "acc", "p1")
	b.connect(t, "acc", "p2")
	b.inner.reset()

	_ = a.c.UpdateAccountPeer(context.Background(), "acc", "p1")
	settle()
	assert.Empty(t, b.inner.Calls())
}

// An instance that holds the stream of a named peer replays the message even when it
// never saw OnPeerConnected for that account (e.g. tracking was pruned in a race).
func TestRelevantByPeerChannel(t *testing.T) {
	hub := bus.NewMemoryHub()
	a := newInstance(t, hub, "a", Options{})
	b := newInstance(t, hub, "b", Options{})
	b.upd.open("p9")

	a.c.DisconnectPeers(context.Background(), "acc", []string{"p9"})
	eventuallyCalls(t, b, "DisconnectPeers acc p9")
	assert.False(t, b.upd.HasChannel("p9"))
}

func TestUnknownKindFallsBackToAccountRefresh(t *testing.T) {
	hub := bus.NewMemoryHub()
	b := newInstance(t, hub, "b", Options{})
	b.connect(t, "acc", "p1")
	b.inner.reset()

	require.NoError(t, hub.PublishRaw(bus.Envelope{V: 9, Origin: "future", Kind: "proxy_updated", Account: "acc"}))
	require.NoError(t, hub.PublishRaw(bus.Envelope{V: 9, Origin: "future", Kind: "proxy_updated", Account: "other"}))
	eventuallyCalls(t, b, "BufferUpdateAccountPeers acc ha_resync/update")
	assert.EqualValues(t, 1, b.c.Metrics().Fallbacks.Load())
	assert.EqualValues(t, 1, b.c.Metrics().Skipped.Load())
}

func TestResyncOnResubscribeCoversLocalAccountsOnly(t *testing.T) {
	hub := bus.NewMemoryHub()
	b := newInstance(t, hub, "b", Options{})
	b.connect(t, "acc1", "p1")
	b.connect(t, "acc2", "p2")
	b.connect(t, "acc3", "p3")
	b.upd.close("p3") // channel closed without OnPeerDisconnected: pruned after the grace period
	b.c.local.mu.Lock()
	b.c.local.byAccount["acc3"]["p3"] = time.Now().Add(-2 * pruneGrace)
	b.c.local.mu.Unlock()
	b.inner.reset()

	hub.Resubscribe()
	eventuallyCalls(t, b, "UpdateAccountPeers acc1 ha_resync/update", "UpdateAccountPeers acc2 ha_resync/update")
	assert.False(t, b.c.local.has("acc3"))
}

// A peer tracked by OnPeerConnected whose channel does not exist yet survives a
// concurrent resync.
func TestPruneKeepsConnectingPeers(t *testing.T) {
	l := localStreams{byAccount: map[string]map[string]time.Time{}}
	l.add("acc", "p1")
	none := func(string) bool { return false }
	assert.Equal(t, []string{"acc"}, l.prune(none, time.Now()))
	assert.Empty(t, l.prune(none, time.Now().Add(pruneGrace+time.Second)))
}

func TestDisconnectStopsTracking(t *testing.T) {
	hub := bus.NewMemoryHub()
	b := newInstance(t, hub, "b", Options{})
	ch, err := b.c.OnPeerConnected(context.Background(), "acc", "p1")
	require.NoError(t, err)
	assert.True(t, b.c.local.has("acc"))
	assert.True(t, b.c.OnPeerDisconnected(context.Background(), "acc", "p1", ch))
	assert.False(t, b.c.local.has("acc"))
}

func TestFailedPublishIsResyncedWhenBusReturns(t *testing.T) {
	hub := bus.NewMemoryHub()
	a := newInstance(t, hub, "a", Options{})
	b := newInstance(t, hub, "b", Options{})
	b.connect(t, "acc", "p1")
	b.inner.reset()

	hub.SetFailing(true)
	_ = a.c.UpdateAccountPeers(context.Background(), "acc", policyUpdate)
	require.Eventually(t, func() bool { return a.c.Metrics().Pending.Load() == 1 }, time.Second, time.Millisecond)
	assert.Empty(t, b.inner.Calls())

	hub.SetFailing(false)
	hub.Resubscribe()
	// b resyncs its own accounts, and a tells everyone to refresh the account it missed.
	eventuallyCalls(t, b, "UpdateAccountPeers acc ha_resync/update", "BufferUpdateAccountPeers acc ha_resync/update")
	assert.Zero(t, a.c.Metrics().Pending.Load())
}

func TestQueueOverflowMarksAccountPending(t *testing.T) {
	upd := &fakeUpdates{chans: map[string]chan *network_map.UpdateMessage{}}
	inner := &fakeInner{updates: upd}
	// Not started: nothing drains the queue.
	c := New(func() network_map.Controller { return inner }, func() network_map.PeersUpdateManager { return upd }, bus.NewMemoryHub().Bus("a"), Options{QueueSize: 1})
	ctx := context.Background()
	_ = c.UpdateAccountPeers(ctx, "acc1", policyUpdate)
	_ = c.UpdateAccountPeers(ctx, "acc2", policyUpdate)
	assert.EqualValues(t, 1, c.Metrics().QueueOverflows.Load())
	assert.EqualValues(t, 1, c.Metrics().Pending.Load())
}

func TestPeriodicResync(t *testing.T) {
	hub := bus.NewMemoryHub()
	b := newInstance(t, hub, "b", Options{ResyncInterval: 30 * time.Millisecond})
	b.connect(t, "acc", "p1")
	require.Eventually(t, func() bool { return b.c.Metrics().ResyncAccounts.Load() >= 2 }, 2*time.Second, 5*time.Millisecond)
}

func TestLazyConstruction(t *testing.T) {
	built := 0
	upd := &fakeUpdates{chans: map[string]chan *network_map.UpdateMessage{}}
	c := New(func() network_map.Controller { built++; return &fakeInner{updates: upd} },
		func() network_map.PeersUpdateManager { return upd }, bus.NewMemoryHub().Bus("a"), Options{})
	assert.Zero(t, built, "New must not build the real controller")
	assert.Equal(t, "netbird.test", c.GetDNSDomain(nil))
	assert.Zero(t, c.CountStreams())
	assert.Equal(t, 1, built)
}

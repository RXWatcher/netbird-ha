// Package controller fans network-map updates out to every management instance.
//
// Upstream's network_map.Controller only computes and sends updates for peers that
// have an update channel on this process (HasChannel). With several instances, a
// change made through instance A never reaches peers streaming from B. Controller
// wraps the real controller: each update call runs locally as before and is then
// published on the bus as "account X / these peers / this reason". Every other
// instance replays the same call on its own real controller, which recomputes from
// the shared database and sends only to its own streams.
//
// Upstream calls these methods after the database transaction has committed, and
// the publish happens after the local call returns, so receivers read committed
// state. Pub/sub can lose messages (reconnects, Sentinel failover), so the wrapper
// also resends the network maps of every account with local streams after each
// (re)subscription and periodically; clients ignore network maps older than the
// serial they have.
package controller

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/networkmap/nmdata"

	"github.com/RXWatcher/netbird-ha/ha/bus"
)

// ResyncReason labels resync updates in upstream's update metrics.
var ResyncReason = types.UpdateReason{Resource: "ha_resync", Operation: types.UpdateOperationUpdate}

// Options tune the wrapper. Zero values take the defaults.
type Options struct {
	// ResyncInterval is the periodic resync period (default 10m, negative disables).
	// Each instance adds up to 10% jitter so instances do not resync in lockstep.
	ResyncInterval time.Duration
	// ResyncConcurrency bounds concurrent account resyncs (default 4).
	ResyncConcurrency int
	// ApplyConcurrency bounds concurrent unbuffered remote applies (default 32).
	ApplyConcurrency int
	// QueueSize is the outgoing publish queue length (default 1024). On overflow the
	// account is marked for a resync message instead.
	QueueSize int
}

func (o *Options) defaults() {
	if o.ResyncInterval == 0 {
		o.ResyncInterval = 10 * time.Minute
	}
	if o.ResyncConcurrency <= 0 {
		o.ResyncConcurrency = 4
	}
	if o.ApplyConcurrency <= 0 {
		o.ApplyConcurrency = 32
	}
	if o.QueueSize <= 0 {
		o.QueueSize = 1024
	}
}

// Metrics are plain counters; Register exports them over OTel.
type Metrics struct {
	Applied        atomic.Int64 // remote messages replayed on the real controller
	Skipped        atomic.Int64 // remote messages for accounts with no local stream
	Fallbacks      atomic.Int64 // unknown kinds handled as a full account refresh
	QueueOverflows atomic.Int64
	Resyncs        atomic.Int64 // resync rounds
	ResyncAccounts atomic.Int64 // accounts resent across all rounds
	Pending        atomic.Int64 // accounts waiting for a resync message to be published
}

// Controller implements network_map.Controller around the real controller.
type Controller struct {
	newInner   func() network_map.Controller
	newUpdates func() network_map.PeersUpdateManager
	once       sync.Once
	inner      network_map.Controller
	updates    network_map.PeersUpdateManager

	bus     bus.Bus
	opts    Options
	queue   chan bus.Envelope
	local   localStreams
	applies chan struct{}
	metrics Metrics

	pendingMu sync.Mutex
	pending   map[string]struct{}

	resyncMu      sync.Mutex
	resyncRunning bool
	resyncAgain   bool
}

var _ network_map.Controller = (*Controller)(nil)

// New returns the wrapper. newInner builds the real controller and newUpdates
// returns the PeersUpdateManager that controller uses; both run on first use, not
// here, because server.Inject stores this value before their dependencies exist.
func New(newInner func() network_map.Controller, newUpdates func() network_map.PeersUpdateManager, b bus.Bus, opts Options) *Controller {
	opts.defaults()
	return &Controller{
		newInner:   newInner,
		newUpdates: newUpdates,
		bus:        b,
		opts:       opts,
		queue:      make(chan bus.Envelope, opts.QueueSize),
		local:      localStreams{byAccount: map[string]map[string]time.Time{}},
		applies:    make(chan struct{}, opts.ApplyConcurrency),
		pending:    map[string]struct{}{},
	}
}

func (c *Controller) real() network_map.Controller {
	c.once.Do(func() {
		c.inner = c.newInner()
		c.updates = c.newUpdates()
	})
	return c.inner
}

func (c *Controller) hasChannel(peerID string) bool {
	c.real()
	return c.updates.HasChannel(peerID)
}

// Metrics returns the wrapper's counters.
func (c *Controller) Metrics() *Metrics { return &c.metrics }

// Start runs the publisher, the subscriber and the periodic resync until ctx is done.
func (c *Controller) Start(ctx context.Context) {
	go c.publishLoop(ctx)
	go c.bus.Run(ctx, c.handle, func(ctx context.Context) { c.triggerResync(ctx, "subscribed") })
	if c.opts.ResyncInterval > 0 {
		go c.resyncLoop(ctx)
	}
}

// --- update calls: run locally, then publish -------------------------------------

func (c *Controller) UpdateAccountPeers(ctx context.Context, accountID string, reason types.UpdateReason) error {
	err := c.real().UpdateAccountPeers(ctx, accountID, reason)
	c.publish(bus.Envelope{Kind: bus.KindUpdateAccountPeers, Account: accountID, Reason: wireReason(reason)})
	return err
}

func (c *Controller) BufferUpdateAccountPeers(ctx context.Context, accountID string, reason types.UpdateReason) error {
	err := c.real().BufferUpdateAccountPeers(ctx, accountID, reason)
	c.publish(bus.Envelope{Kind: bus.KindBufferUpdateAccountPeers, Account: accountID, Reason: wireReason(reason)})
	return err
}

func (c *Controller) UpdateAffectedPeers(ctx context.Context, accountID string, peerIDs []string) error {
	err := c.real().UpdateAffectedPeers(ctx, accountID, peerIDs)
	if len(peerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindUpdateAffectedPeers, Account: accountID, PeerIDs: peerIDs})
	}
	return err
}

func (c *Controller) BufferUpdateAffectedPeers(ctx context.Context, accountID string, peerIDs []string, reason types.UpdateReason) error {
	err := c.real().BufferUpdateAffectedPeers(ctx, accountID, peerIDs, reason)
	if len(peerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindBufferUpdateAffectedPeers, Account: accountID, PeerIDs: peerIDs, Reason: wireReason(reason)})
	}
	return err
}

func (c *Controller) UpdateAccountPeer(ctx context.Context, accountID string, peerID string) error {
	err := c.real().UpdateAccountPeer(ctx, accountID, peerID)
	// Upstream sends only when this process holds the peer's stream; if it does, no
	// other instance needs to hear about it. Otherwise upstream returns a "doesn't
	// have a channel" error, which is kept (callers ignore it) while the call fans out.
	if !c.hasChannel(peerID) {
		c.publish(bus.Envelope{Kind: bus.KindUpdateAccountPeer, Account: accountID, PeerIDs: []string{peerID}})
	}
	return err
}

func (c *Controller) OnPeersAdded(ctx context.Context, accountID string, peerIDs []string, affectedPeerIDs []string) error {
	err := c.real().OnPeersAdded(ctx, accountID, peerIDs, affectedPeerIDs)
	if len(affectedPeerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindPeersAdded, Account: accountID, PeerIDs: peerIDs, Affected: affectedPeerIDs})
	}
	return err
}

func (c *Controller) OnPeersUpdated(ctx context.Context, accountID string, peerIDs []string, affectedPeerIDs []string) error {
	err := c.real().OnPeersUpdated(ctx, accountID, peerIDs, affectedPeerIDs)
	if len(affectedPeerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindPeersUpdated, Account: accountID, PeerIDs: peerIDs, Affected: affectedPeerIDs})
	}
	return err
}

// OnPeersDeleted always publishes: the deleted peers' streams may be on another
// instance, which must send them the empty map and close them.
func (c *Controller) OnPeersDeleted(ctx context.Context, accountID string, peerIDs []string, affectedPeerIDs []string) error {
	err := c.real().OnPeersDeleted(ctx, accountID, peerIDs, affectedPeerIDs)
	if len(peerIDs) > 0 || len(affectedPeerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindPeersDeleted, Account: accountID, PeerIDs: peerIDs, Affected: affectedPeerIDs})
	}
	return err
}

func (c *Controller) DisconnectPeers(ctx context.Context, accountID string, peerIDs []string) {
	c.real().DisconnectPeers(ctx, accountID, peerIDs)
	if len(peerIDs) > 0 {
		c.publish(bus.Envelope{Kind: bus.KindDisconnectPeers, Account: accountID, PeerIDs: peerIDs})
	}
}

// --- stream lifecycle: local, but tracked for relevance and resync ----------------

func (c *Controller) OnPeerConnected(ctx context.Context, accountID string, peerID string) (chan *network_map.UpdateMessage, error) {
	// Track before the channel exists so no message for this account is skipped.
	c.local.add(accountID, peerID)
	return c.real().OnPeerConnected(ctx, accountID, peerID)
}

func (c *Controller) OnPeerDisconnected(ctx context.Context, accountID string, peerID string, session chan *network_map.UpdateMessage) bool {
	closed := c.real().OnPeerDisconnected(ctx, accountID, peerID, session)
	if closed && !c.hasChannel(peerID) {
		c.local.remove(accountID, peerID)
	}
	return closed
}

// --- read-only and local calls: pass through -------------------------------------

func (c *Controller) GetValidatedPeerWithMap(ctx context.Context, isRequiresApproval bool, accountID string, peerID string) (*types.NetworkMap, []*nmdata.PostureChecks, int64, error) {
	return c.real().GetValidatedPeerWithMap(ctx, isRequiresApproval, accountID, peerID)
}

func (c *Controller) GetValidatedPeerWithComponents(ctx context.Context, isRequiresApproval bool, accountID string, p *nbpeer.Peer) (*nbpeer.Peer, *types.NetworkMapComponents, []*nmdata.PostureChecks, int64, error) {
	return c.real().GetValidatedPeerWithComponents(ctx, isRequiresApproval, accountID, p)
}

func (c *Controller) GetDNSDomain(settings *types.Settings) string {
	return c.real().GetDNSDomain(settings)
}

func (c *Controller) StartWarmup(ctx context.Context) { c.real().StartWarmup(ctx) }

func (c *Controller) GetNetworkMap(ctx context.Context, peerID string) (*types.NetworkMap, error) {
	return c.real().GetNetworkMap(ctx, peerID)
}

func (c *Controller) CountStreams() int { return c.real().CountStreams() }

// TrackEphemeralPeer stays local; cross-instance ephemeral handling is ha/ephemeral's.
func (c *Controller) TrackEphemeralPeer(ctx context.Context, peer *nbpeer.Peer) {
	c.real().TrackEphemeralPeer(ctx, peer)
}

// --- publishing ---------------------------------------------------------------

// publish queues e without blocking the caller (an API request or a gRPC stream).
func (c *Controller) publish(e bus.Envelope) {
	select {
	case c.queue <- e:
	default:
		c.metrics.QueueOverflows.Add(1)
		c.markPending(e.Account)
	}
}

func (c *Controller) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-c.queue:
			if err := c.bus.Publish(ctx, e); err != nil {
				log.Warnf("ha controller: %v; account %s will be resynced", err, e.Account)
				c.markPending(e.Account)
			}
		}
	}
}

func (c *Controller) markPending(accountID string) {
	c.pendingMu.Lock()
	c.pending[accountID] = struct{}{}
	c.metrics.Pending.Store(int64(len(c.pending)))
	c.pendingMu.Unlock()
}

// flushPending publishes a resync message for every account whose update could not
// be published, so the other instances catch up as soon as the bus is back.
func (c *Controller) flushPending(ctx context.Context) {
	c.pendingMu.Lock()
	accounts := make([]string, 0, len(c.pending))
	for a := range c.pending {
		accounts = append(accounts, a)
	}
	c.pending = map[string]struct{}{}
	c.pendingMu.Unlock()

	for _, a := range accounts {
		if err := c.bus.Publish(ctx, bus.Envelope{Kind: bus.KindResync, Account: a}); err != nil {
			c.markPending(a)
		}
	}
	c.pendingMu.Lock()
	c.metrics.Pending.Store(int64(len(c.pending)))
	c.pendingMu.Unlock()
}

// --- receiving ----------------------------------------------------------------

// handle replays a message from another instance on the real controller.
func (c *Controller) handle(ctx context.Context, e bus.Envelope) {
	if !c.relevant(e) {
		c.metrics.Skipped.Add(1)
		return
	}
	c.metrics.Applied.Add(1)
	inner := c.real()
	reason := upstreamReason(e.Reason)
	logErr := func(err error) {
		if err != nil {
			log.WithContext(ctx).Debugf("ha controller: replaying %s from %s for account %s: %v", e.Kind, e.Origin, e.Account, err)
		}
	}

	switch e.Kind {
	// Buffered calls return at once; upstream sends in its own goroutines.
	case bus.KindBufferUpdateAccountPeers:
		logErr(inner.BufferUpdateAccountPeers(ctx, e.Account, reason))
	case bus.KindBufferUpdateAffectedPeers:
		logErr(inner.BufferUpdateAffectedPeers(ctx, e.Account, e.PeerIDs, reason))
	case bus.KindPeersAdded:
		logErr(inner.OnPeersAdded(ctx, e.Account, e.PeerIDs, e.Affected))
	case bus.KindPeersUpdated:
		logErr(inner.OnPeersUpdated(ctx, e.Account, e.PeerIDs, e.Affected))
	case bus.KindDisconnectPeers:
		inner.DisconnectPeers(ctx, e.Account, e.PeerIDs)
	case bus.KindResync:
		logErr(inner.BufferUpdateAccountPeers(ctx, e.Account, ResyncReason))

	// Unbuffered calls compute network maps synchronously: run them off the
	// subscriber goroutine, bounded.
	case bus.KindUpdateAccountPeers:
		c.async(func() { logErr(inner.UpdateAccountPeers(ctx, e.Account, reason)) })
	case bus.KindUpdateAffectedPeers:
		c.async(func() { logErr(inner.UpdateAffectedPeers(ctx, e.Account, e.PeerIDs)) })
	case bus.KindUpdateAccountPeer:
		c.async(func() {
			for _, p := range e.PeerIDs {
				logErr(inner.UpdateAccountPeer(ctx, e.Account, p))
			}
		})
	case bus.KindPeersDeleted:
		c.async(func() { logErr(inner.OnPeersDeleted(ctx, e.Account, e.PeerIDs, e.Affected)) })

	default:
		// A newer release added a kind this build does not know. A full refresh of
		// the account is always correct, only less targeted.
		c.metrics.Fallbacks.Add(1)
		log.WithContext(ctx).Debugf("ha controller: unknown message kind %q (v%d) from %s, refreshing account %s", e.Kind, e.V, e.Origin, e.Account)
		logErr(inner.BufferUpdateAccountPeers(ctx, e.Account, ResyncReason))
	}
}

func (c *Controller) async(fn func()) {
	c.applies <- struct{}{}
	go func() {
		defer func() { <-c.applies }()
		fn()
	}()
}

// relevant reports whether this instance holds a stream the message could affect.
// Skipping the rest saves an account load per instance per change.
func (c *Controller) relevant(e bus.Envelope) bool {
	if c.local.has(e.Account) {
		return true
	}
	for _, ids := range [][]string{e.PeerIDs, e.Affected} {
		for _, id := range ids {
			if c.hasChannel(id) {
				return true
			}
		}
	}
	return false
}

// --- resync -------------------------------------------------------------------

func (c *Controller) resyncLoop(ctx context.Context) {
	for {
		jitter := time.Duration(rand.Int64N(int64(c.opts.ResyncInterval)/10 + 1)) //nolint:gosec // jitter only
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.opts.ResyncInterval + jitter):
			c.triggerResync(ctx, "periodic")
		}
	}
}

// triggerResync runs a resync in the background. A trigger that arrives while one
// is running schedules exactly one more round, so resyncs never overlap and never
// block the subscriber.
func (c *Controller) triggerResync(ctx context.Context, why string) {
	c.resyncMu.Lock()
	if c.resyncRunning {
		c.resyncAgain = true
		c.resyncMu.Unlock()
		return
	}
	c.resyncRunning = true
	c.resyncMu.Unlock()

	go func() {
		for {
			c.resync(ctx, why)
			c.resyncMu.Lock()
			if !c.resyncAgain || ctx.Err() != nil {
				c.resyncRunning = false
				c.resyncMu.Unlock()
				return
			}
			c.resyncAgain = false
			c.resyncMu.Unlock()
		}
	}()
}

// resync publishes pending resync messages and resends the network maps of every
// account that has a stream on this instance.
func (c *Controller) resync(ctx context.Context, why string) {
	c.flushPending(ctx)
	c.real()
	accounts := c.local.prune(c.updates.HasChannel, time.Now())
	c.metrics.Resyncs.Add(1)
	if len(accounts) == 0 {
		return
	}
	log.WithContext(ctx).Debugf("ha controller: %s resync of %d accounts", why, len(accounts))

	sem := make(chan struct{}, c.opts.ResyncConcurrency)
	var wg sync.WaitGroup
	for _, a := range accounts {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(accountID string) {
			defer func() { <-sem; wg.Done() }()
			if err := c.inner.UpdateAccountPeers(ctx, accountID, ResyncReason); err != nil {
				log.WithContext(ctx).Warnf("ha controller: resync of account %s: %v", accountID, err)
			}
			c.metrics.ResyncAccounts.Add(1)
		}(a)
	}
	wg.Wait()
}

// --- helpers ------------------------------------------------------------------

func wireReason(r types.UpdateReason) *bus.Reason {
	if r.Resource == "" && r.Operation == "" {
		return nil
	}
	return &bus.Reason{Resource: string(r.Resource), Operation: string(r.Operation)}
}

func upstreamReason(r *bus.Reason) types.UpdateReason {
	if r == nil {
		return ResyncReason
	}
	return types.UpdateReason{Resource: types.UpdateResource(r.Resource), Operation: types.UpdateOperation(r.Operation)}
}

// pruneGrace keeps a freshly tracked peer through a resync: OnPeerConnected tracks
// the peer before upstream creates its channel, and a resync running in between
// must not drop it.
const pruneGrace = 2 * time.Minute

// localStreams maps account ID to the peers that opened a Sync stream here, with the
// time each was tracked.
type localStreams struct {
	mu        sync.Mutex
	byAccount map[string]map[string]time.Time
}

func (l *localStreams) add(accountID, peerID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	peers := l.byAccount[accountID]
	if peers == nil {
		peers = map[string]time.Time{}
		l.byAccount[accountID] = peers
	}
	peers[peerID] = time.Now()
}

func (l *localStreams) remove(accountID, peerID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if peers := l.byAccount[accountID]; peers != nil {
		delete(peers, peerID)
		if len(peers) == 0 {
			delete(l.byAccount, accountID)
		}
	}
}

func (l *localStreams) has(accountID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.byAccount[accountID]) > 0
}

// prune drops peers whose channel is gone (closed by DisconnectPeers or
// OnPeersDeleted without a matching disconnect) and returns the remaining accounts.
// It is a safety net; OnPeerDisconnected normally removes peers.
func (l *localStreams) prune(hasChannel func(string) bool, now time.Time) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	accounts := make([]string, 0, len(l.byAccount))
	for a, peers := range l.byAccount {
		for p, since := range peers {
			if now.Sub(since) > pruneGrace && !hasChannel(p) {
				delete(peers, p)
			}
		}
		if len(peers) == 0 {
			delete(l.byAccount, a)
			continue
		}
		accounts = append(accounts, a)
	}
	return accounts
}

// Register exports the counters on meter.
func (m *Metrics) Register(meter metric.Meter) error {
	for _, c := range []struct {
		name, desc string
		v          *atomic.Int64
	}{
		{"netbird_ha_controller_applied_total", "remote updates replayed locally", &m.Applied},
		{"netbird_ha_controller_skipped_total", "remote updates for accounts without local streams", &m.Skipped},
		{"netbird_ha_controller_fallbacks_total", "unknown message kinds handled as a full refresh", &m.Fallbacks},
		{"netbird_ha_controller_queue_overflows_total", "publishes dropped from a full queue (resynced instead)", &m.QueueOverflows},
		{"netbird_ha_controller_resyncs_total", "resync rounds", &m.Resyncs},
		{"netbird_ha_controller_resync_accounts_total", "accounts resent by resyncs", &m.ResyncAccounts},
	} {
		v := c.v
		if _, err := meter.Int64ObservableCounter(c.name, metric.WithDescription(c.desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(v.Load())
				return nil
			})); err != nil {
			return err
		}
	}
	_, err := meter.Int64ObservableGauge("netbird_ha_controller_pending_accounts", metric.WithDescription("accounts waiting for a resync message to be published"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.Pending.Load())
			return nil
		}))
	return err
}

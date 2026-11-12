// Package ephemeral makes upstream NetBird's ephemeral-peer cleanup correct
// when several management instances share one database.
//
// # What upstream already does
//
// Since v0.83.0 upstream records which management instance holds each
// peer's sync stream (tables peer_connections and management_instances,
// management/internals/controllers/peerliveness). Instances heartbeat their
// row every minute; a peer held by an instance that has not heartbeated for
// nbpeer.InstanceStaleAfter (5 min) reads as disconnected, and a live
// instance sweeps the dead instance's connections. Session fencing
// (ConnectPeer, DisconnectPeer) keeps a peer that moved to another instance
// connected.
//
// Upstream's EphemeralManager deletes an ephemeral peer ten minutes after it
// is scheduled, through peers.Manager.DeletePeers with checkConnected=true,
// which skips a peer that reads connected or was seen in the last 9m50s. It
// schedules a peer only when the stream closes on this instance, or at boot.
//
// # The gap this package closes
//
// When an instance dies (kill -9, host loss, network partition) nobody sees
// its ephemeral peers' streams close. Upstream now marks them disconnected,
// but no live instance schedules them, so they stay until some instance
// restarts and loads every ephemeral peer at boot.
//
// Manager wraps upstream's manager. Every ReconcileInterval it lists the
// ephemeral peers that read disconnected and hands them to upstream's
// OnPeerDisconnected, which schedules each once and deletes it after the
// usual lifetime unless it comes back. Scheduling a peer that another
// instance already scheduled is harmless: DeletePeers skips a peer that is
// gone or connected.
//
// Only peers whose connection record carries a session (SessionStartedAt >
// 0, written by a v0.83.0+ instance) are reconciled. Rows backfilled by the
// v0.83.0 migration have no session, so the peers of instances that still
// run an older release during a rolling upgrade are left alone.
//
// Up to v0.1.x this package kept its own session registry
// (netbird_ha_ephemeral_sessions, netbird_ha_meta). Upstream's liveness
// tracking replaced it; those tables are no longer used and can be dropped
// once no v0.1.x instance runs.
package ephemeral

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	nbstore "github.com/netbirdio/netbird/management/server/store"
)

// Config tunes the reconciler. Zero values take the defaults.
type Config struct {
	// ReconcileInterval is how often an instance looks for ephemeral peers
	// nobody scheduled. Default 1m, with up to 20% jitter.
	ReconcileInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = time.Minute
	}
	return c
}

// Manager is upstream's ephemeral.Manager plus a periodic reconcile of the
// ephemeral peers of dead instances. Upstream's manager and store are built
// lazily, on first use, so Manager can be injected before the server builds
// them.
type Manager struct {
	buildInner func() upstream.Manager
	buildStore func() nbstore.Store
	innerOnce  sync.Once
	inner      upstream.Manager
	storeOnce  sync.Once
	store      nbstore.Store

	cfg Config

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

var _ upstream.Manager = (*Manager)(nil)

// New returns a Manager. buildInner builds upstream's manager and buildStore
// returns the server's store; both run once, on first use.
func New(buildInner func() upstream.Manager, buildStore func() nbstore.Store, cfg Config) *Manager {
	return &Manager{buildInner: buildInner, buildStore: buildStore, cfg: cfg.withDefaults()}
}

func (m *Manager) innerManager() upstream.Manager {
	m.innerOnce.Do(func() { m.inner = m.buildInner() })
	return m.inner
}

func (m *Manager) st() nbstore.Store {
	m.storeOnce.Do(func() { m.store = m.buildStore() })
	return m.store
}

// LoadInitialPeers runs upstream's LoadInitialPeers and starts the reconcile
// loop. Upstream calls it once, when the server starts.
func (m *Manager) LoadInitialPeers(ctx context.Context) {
	m.innerManager().LoadInitialPeers(ctx)
	m.Start(ctx)
}

// Start starts the reconcile loop. It runs once; later calls do nothing.
func (m *Manager) Start(ctx context.Context) {
	m.startOnce.Do(func() {
		loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		m.cancel = cancel
		m.wg.Add(1)
		go m.reconcileLoop(loopCtx)
	})
}

// Stop stops the reconcile loop and upstream's manager.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			m.wg.Wait()
		}
	})
	m.innerManager().Stop()
}

// OnPeerConnected runs upstream's OnPeerConnected.
func (m *Manager) OnPeerConnected(ctx context.Context, peer *nbpeer.Peer) {
	m.innerManager().OnPeerConnected(ctx, peer)
}

// OnPeerDisconnected runs upstream's OnPeerDisconnected.
func (m *Manager) OnPeerDisconnected(ctx context.Context, peer *nbpeer.Peer) {
	m.innerManager().OnPeerDisconnected(ctx, peer)
}

func (m *Manager) reconcileLoop(ctx context.Context) {
	defer m.wg.Done()
	for {
		jitter := time.Duration(rand.Int64N(int64(m.cfg.ReconcileInterval)/5 + 1)) //nolint:gosec // spreads load, not a secret
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.cfg.ReconcileInterval + jitter):
		}
		if _, err := m.Reconcile(ctx); err != nil && ctx.Err() == nil {
			log.WithContext(ctx).Warnf("ephemeral peer reconcile failed: %v", err)
		}
	}
}

// Reconcile hands every ephemeral peer that reads disconnected, and that a
// v0.83.0+ instance held, to upstream's manager for deletion. It returns how
// many peers it handed over; upstream ignores the ones already scheduled.
func (m *Manager) Reconcile(ctx context.Context) (int, error) {
	peers, err := m.st().GetAllEphemeralPeers(ctx, nbstore.LockingStrengthNone)
	if err != nil {
		return 0, err
	}
	handed := 0
	for _, p := range peers {
		if p.Status == nil || p.Status.Connected || p.Status.SessionStartedAt == 0 || p.ProxyMeta.Embedded {
			continue
		}
		log.WithContext(ctx).Debugf("ephemeral peer %s reads disconnected; scheduling cleanup", p.ID)
		m.innerManager().OnPeerDisconnected(ctx, p)
		handed++
	}
	return handed, nil
}

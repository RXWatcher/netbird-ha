// Package ephemeral makes upstream's ephemeral-peer cleanup correct when
// several management instances share one database.
//
// # What upstream already does
//
// Upstream's EphemeralManager (management/internals/modules/peers/ephemeral/
// manager) keeps an in-memory list of ephemeral peers that disconnected from
// this instance, plus every ephemeral peer in the database at boot, and
// deletes them ten minutes later through peers.Manager.DeletePeers with
// checkConnected=true. Since upstream #4789 (2025-12) DeletePeers re-reads
// each peer in a transaction and skips it when the database shows it
// connected or seen in the last 9m50s. So a peer that moved to another instance, which marks it
// connected with a newer session (MarkPeerConnectedIfNewerSession), is not
// deleted. That part needs no fix in v0.82.
//
// # The gap this package closes
//
// The database's connected flag is only cleared by the instance that holds
// the stream. When an instance dies (kill -9, host loss, network partition)
// its peers stay connected=true in the database for ever. Upstream's check
// then refuses to delete those ephemeral peers, and since no live instance
// ever sees them disconnect, nobody schedules them again: they leak.
//
// Manager wraps upstream's manager and adds a liveness registry:
//
//   - Each instance records its ephemeral sessions (peer, session token) in
//     netbird_ha_ephemeral_sessions and refreshes them every
//     HeartbeatInterval.
//   - Every ReconcileInterval each instance looks for ephemeral peers that
//     the database shows connected, whose session no instance has refreshed
//     within StaleAfter, and that were last seen more than StaleAfter ago.
//     It marks each one disconnected with upstream's session-fenced
//     MarkPeerDisconnectedIfSameSession (a no-op if the peer reconnected
//     meanwhile, and only one instance wins) and hands it to upstream's
//     manager, which deletes it after the usual lifetime unless it comes
//     back.
//   - An instance whose heartbeat failed re-marks its own live sessions
//     connected once the database is reachable again, so a partitioned
//     instance's peers are not deleted from under it.
//
// Only sessions that started after the registry first came up (its epoch)
// are reconciled, so sessions held by instances that predate this build are
// never touched. Every instance sharing the database must run this build;
// a stock instance's sessions would look dead to the others.
package ephemeral

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"

	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	nbstore "github.com/netbirdio/netbird/management/server/store"

	hastore "github.com/Bloem-Studios/netbird-ha/ha/store"
)

const (
	sessionsTable = "netbird_ha_ephemeral_sessions"
	metaTable     = "netbird_ha_meta"
	epochKey      = "ephemeral_registry_epoch"

	reconcileBatch = 500
)

// Config tunes the registry. Zero values take the defaults.
type Config struct {
	// HeartbeatInterval is how often an instance refreshes its sessions.
	// Default 30s.
	HeartbeatInterval time.Duration
	// StaleAfter is how long a session may go without a refresh before
	// other instances treat it as dead. Default 3m. Keep it several
	// heartbeats long.
	StaleAfter time.Duration
	// ReconcileInterval is how often an instance looks for dead sessions.
	// Default 1m, with up to 20% jitter.
	ReconcileInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
	if c.StaleAfter <= 0 {
		c.StaleAfter = 3 * time.Minute
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = time.Minute
	}
	return c
}

type session struct {
	accountID string
	startedAt int64
}

// Manager is upstream's ephemeral.Manager with a cross-instance liveness
// registry. Upstream's manager and store are built lazily, on first use, so
// Manager can be injected before the server builds them.
type Manager struct {
	buildInner func() upstream.Manager
	buildStore func() nbstore.Store
	innerOnce  sync.Once
	inner      upstream.Manager
	storeOnce  sync.Once
	store      nbstore.Store

	pool     *pgxpool.Pool
	locker   *hastore.Locker
	instance string
	cfg      Config

	mu    sync.Mutex
	local map[string]session

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	epoch     int64
	// heartbeatFailed is set when a heartbeat fails, and cleared once the
	// instance's sessions have been re-marked connected.
	heartbeatFailed bool
}

var _ upstream.Manager = (*Manager)(nil)

// New returns a Manager. buildInner builds upstream's manager and buildStore
// returns the server's store; both run once, on first use. pool and locker
// are the HA lock pool (see ha/store); instance must be unique per running
// instance.
func New(buildInner func() upstream.Manager, buildStore func() nbstore.Store, locker *hastore.Locker, instance string, cfg Config) *Manager {
	return &Manager{
		buildInner: buildInner,
		buildStore: buildStore,
		pool:       locker.Pool(),
		locker:     locker,
		instance:   instance,
		cfg:        cfg.withDefaults(),
		local:      make(map[string]session),
	}
}

func (m *Manager) innerManager() upstream.Manager {
	m.innerOnce.Do(func() { m.inner = m.buildInner() })
	return m.inner
}

func (m *Manager) st() nbstore.Store {
	m.storeOnce.Do(func() { m.store = m.buildStore() })
	return m.store
}

// LoadInitialPeers starts the registry and then runs upstream's
// LoadInitialPeers. Upstream calls it once, when the server starts.
func (m *Manager) LoadInitialPeers(ctx context.Context) {
	if err := m.Start(ctx); err != nil {
		// Without the registry the instance behaves like upstream:
		// correct for peers that move between live instances, leaking the
		// ephemeral peers of a dead one.
		log.WithContext(ctx).Errorf("ephemeral session registry not started: %v", err)
	}
	m.innerManager().LoadInitialPeers(ctx)
}

// Start creates the registry tables, reads the epoch and starts the
// heartbeat and reconcile loops. It runs once; later calls return nil.
func (m *Manager) Start(ctx context.Context) error {
	var err error
	m.startOnce.Do(func() {
		if err = m.ensureSchema(ctx); err != nil {
			return
		}
		loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		m.cancel = cancel
		m.wg.Add(2)
		go m.heartbeatLoop(loopCtx)
		go m.reconcileLoop(loopCtx)
	})
	return err
}

func (m *Manager) ensureSchema(ctx context.Context) error {
	return m.locker.WithLock(ctx, hastore.MigrationLockKey, func(ctx context.Context) error {
		if _, err := m.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+sessionsTable+` (
			peer_id            text PRIMARY KEY,
			account_id         text NOT NULL,
			session_started_at bigint NOT NULL,
			instance_id        text NOT NULL,
			heartbeat_at       timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
			return fmt.Errorf("create %s: %w", sessionsTable, err)
		}
		if _, err := m.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+metaTable+` (
			key   text PRIMARY KEY,
			value text NOT NULL
		)`); err != nil {
			return fmt.Errorf("create %s: %w", metaTable, err)
		}
		if _, err := m.pool.Exec(ctx, `INSERT INTO `+metaTable+` (key, value)
			VALUES ($1, (floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint * 1000)::text)
			ON CONFLICT (key) DO NOTHING`, epochKey); err != nil {
			return fmt.Errorf("record registry epoch: %w", err)
		}
		var raw string
		if err := m.pool.QueryRow(ctx, `SELECT value FROM `+metaTable+` WHERE key = $1`, epochKey).Scan(&raw); err != nil {
			return fmt.Errorf("read registry epoch: %w", err)
		}
		epoch, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("parse registry epoch %q: %w", raw, err)
		}
		m.epoch = epoch
		return nil
	})
}

// Stop stops the loops, removes this instance's sessions from the registry
// and stops upstream's manager.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			m.wg.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := m.pool.Exec(ctx, `DELETE FROM `+sessionsTable+` WHERE instance_id = $1`, m.instance); err != nil {
				log.Warnf("remove this instance's ephemeral sessions: %v", err)
			}
		}
	})
	m.innerManager().Stop()
}

// OnPeerConnected records a local ephemeral session and runs upstream's
// OnPeerConnected. The peer is the one upstream's controller read after
// marking it connected, so its status carries this stream's session.
func (m *Manager) OnPeerConnected(ctx context.Context, peer *nbpeer.Peer) {
	if peer.Ephemeral && peer.Status != nil && peer.Status.SessionStartedAt > 0 {
		m.mu.Lock()
		m.local[peer.ID] = session{accountID: peer.AccountID, startedAt: peer.Status.SessionStartedAt}
		m.mu.Unlock()
	}
	m.innerManager().OnPeerConnected(ctx, peer)
}

// OnPeerDisconnected forgets the local session and runs upstream's
// OnPeerDisconnected.
func (m *Manager) OnPeerDisconnected(ctx context.Context, peer *nbpeer.Peer) {
	if peer.Ephemeral {
		m.mu.Lock()
		_, had := m.local[peer.ID]
		delete(m.local, peer.ID)
		m.mu.Unlock()
		if had {
			m.forget(ctx, peer.ID)
		}
	}
	m.innerManager().OnPeerDisconnected(ctx, peer)
}

// forget removes this instance's registry row for peerID. A row another
// instance owns (the peer moved there) is left alone.
func (m *Manager) forget(ctx context.Context, peerID string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := m.pool.Exec(ctx, `DELETE FROM `+sessionsTable+` WHERE peer_id = $1 AND instance_id = $2`, peerID, m.instance); err != nil {
		// The row goes stale and the reconciler finds the peer
		// disconnected in the database, so this is harmless.
		log.WithContext(ctx).Debugf("remove ephemeral session of %s: %v", peerID, err)
	}
}

func (m *Manager) snapshot() map[string]session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]session, len(m.local))
	for id, s := range m.local {
		out[id] = s
	}
	return out
}

func (m *Manager) heartbeatLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		m.Heartbeat(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Heartbeat refreshes this instance's sessions in the registry. After a
// failed heartbeat it also re-marks them connected, undoing a reconcile that
// another instance ran while this one could not reach the database.
func (m *Manager) Heartbeat(ctx context.Context) {
	sessions := m.snapshot()
	if err := m.refresh(ctx, sessions); err != nil {
		if ctx.Err() == nil {
			log.WithContext(ctx).Warnf("ephemeral session heartbeat failed: %v", err)
		}
		m.mu.Lock()
		m.heartbeatFailed = true
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	recovering := m.heartbeatFailed
	m.mu.Unlock()
	if !recovering {
		return
	}
	for peerID, s := range sessions {
		if _, err := m.st().MarkPeerConnectedIfNewerSession(ctx, s.accountID, peerID, s.startedAt); err != nil {
			log.WithContext(ctx).Warnf("re-mark ephemeral peer %s connected: %v", peerID, err)
			return
		}
	}
	m.mu.Lock()
	m.heartbeatFailed = false
	m.mu.Unlock()
}

func (m *Manager) refresh(ctx context.Context, sessions map[string]session) error {
	if len(sessions) == 0 {
		return m.pool.Ping(ctx)
	}
	ids := make([]string, 0, len(sessions))
	accounts := make([]string, 0, len(sessions))
	starts := make([]int64, 0, len(sessions))
	for id, s := range sessions {
		ids = append(ids, id)
		accounts = append(accounts, s.accountID)
		starts = append(starts, s.startedAt)
	}
	// The newest session wins a row two instances both claim.
	_, err := m.pool.Exec(ctx, `INSERT INTO `+sessionsTable+` AS s (peer_id, account_id, session_started_at, instance_id, heartbeat_at)
		SELECT u.peer_id, u.account_id, u.started, $4, now()
		FROM unnest($1::text[], $2::text[], $3::bigint[]) AS u(peer_id, account_id, started)
		ON CONFLICT (peer_id) DO UPDATE
		SET account_id = EXCLUDED.account_id, session_started_at = EXCLUDED.session_started_at,
		    instance_id = EXCLUDED.instance_id, heartbeat_at = now()
		WHERE s.session_started_at <= EXCLUDED.session_started_at`,
		ids, accounts, starts, m.instance)
	return err
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
		m.mu.Lock()
		unhealthy := m.heartbeatFailed
		m.mu.Unlock()
		if unhealthy {
			// This instance cannot vouch for its own sessions, so it
			// must not judge anyone else's.
			continue
		}
		if _, err := m.Reconcile(ctx); err != nil && ctx.Err() == nil {
			log.WithContext(ctx).Warnf("ephemeral session reconcile failed: %v", err)
		}
	}
}

// Reconcile marks disconnected the ephemeral peers whose session no live
// instance holds and schedules them with upstream's manager. It returns how
// many peers it took over.
func (m *Manager) Reconcile(ctx context.Context) (int, error) {
	stale := fmt.Sprintf("%d microseconds", m.cfg.StaleAfter.Microseconds())
	rows, err := m.pool.Query(ctx, `SELECT p.account_id, p.id, p.peer_status_session_started_at
		FROM peers p
		WHERE p.ephemeral
		  AND p.peer_status_connected
		  AND NOT COALESCE(p.proxy_meta_embedded, false)
		  AND p.peer_status_session_started_at >= $1
		  AND p.peer_status_last_seen < now() - $2::interval
		  AND NOT EXISTS (
		    SELECT 1 FROM `+sessionsTable+` s
		    WHERE s.peer_id = p.id
		      AND s.session_started_at >= p.peer_status_session_started_at
		      AND s.heartbeat_at >= now() - $2::interval)
		LIMIT $3`, m.epoch, stale, reconcileBatch)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		accountID, peerID string
		session           int64
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.accountID, &c.peerID, &c.session); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	taken := 0
	for _, c := range candidates {
		updated, err := m.st().MarkPeerDisconnectedIfSameSession(ctx, c.accountID, c.peerID, c.session)
		if err != nil {
			return taken, fmt.Errorf("mark peer %s disconnected: %w", c.peerID, err)
		}
		if !updated {
			// It reconnected, or another instance took it first.
			continue
		}
		peer, err := m.st().GetPeerByID(ctx, nbstore.LockingStrengthNone, c.accountID, c.peerID)
		if err != nil {
			log.WithContext(ctx).Warnf("read ephemeral peer %s after marking it disconnected: %v", c.peerID, err)
			continue
		}
		log.WithContext(ctx).Infof("ephemeral peer %s had no live session (its instance is gone); scheduling cleanup", c.peerID)
		m.innerManager().OnPeerDisconnected(ctx, peer)
		taken++
	}
	return taken, nil
}

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

// Advisory lock keys. They are fixed so that every instance of every release
// agrees on them; never change a value, only add new ones.
const (
	// GlobalLockKey replaces upstream's in-process global account lock.
	GlobalLockKey int64 = 0x6e62686101 // "nbha" 01
	// MigrationLockKey serialises schema changes: upstream's migrations and
	// the tables this module owns.
	MigrationLockKey int64 = 0x6e62686102 // "nbha" 02
)

const (
	pollMinDelay   = 10 * time.Millisecond
	pollMaxDelay   = 250 * time.Millisecond
	retryMinDelay  = 100 * time.Millisecond
	retryMaxDelay  = 2 * time.Second
	unlockTimeout  = 5 * time.Second
	discardTimeout = 2 * time.Second
)

// ErrLockLost reports that an advisory lock was not held any more when it was
// released, so the critical section may have overlapped another holder's.
var ErrLockLost = errors.New("advisory lock was lost before it was released")

// Locker takes PostgreSQL session-level advisory locks. Each held lock pins
// one connection of its own pool for as long as it is held: a session lock
// belongs to the connection that took it, and a connection that goes back to
// a pool with a lock still held would leak the lock to whatever borrows it
// next. Any doubt about a connection's state (an error while locking or
// unlocking, a cancelled context) closes the connection instead, which makes
// the server drop every lock the session held.
//
// The pool must reach PostgreSQL directly or through a session-mode pooler.
// A transaction-mode pooler (PgBouncer pool_mode=transaction) breaks session
// locks.
type Locker struct {
	pool *pgxpool.Pool
}

// NewLocker returns a Locker on pool. The pool should be dedicated to locks;
// see OpenLockPool.
func NewLocker(pool *pgxpool.Pool) *Locker {
	return &Locker{pool: pool}
}

// OpenLockPool opens a small pgx pool for advisory locks. It is separate from
// upstream's pool so that held locks never starve, or leak into, upstream's
// queries.
func OpenLockPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open lock pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping lock pool: %w", err)
	}
	return pool, nil
}

// Pool returns the pool the locks are taken on.
func (l *Locker) Pool() *pgxpool.Pool {
	return l.pool
}

// Held is an advisory lock held on a pinned connection.
type Held struct {
	conn *pgxpool.Conn
	key  int64
}

// Lock blocks until it holds the advisory lock key, ctx is done, or (when
// retry is false) the database cannot be reached. With retry set, connection
// errors are retried with backoff until ctx is done, so a database outage
// makes the caller wait rather than run unlocked.
func (l *Locker) Lock(ctx context.Context, key int64, retry bool) (*Held, error) {
	delay := retryMinDelay
	for {
		held, err := l.tryLock(ctx, key)
		if err == nil {
			return held, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("acquire advisory lock %#x: %w", key, ctx.Err())
		}
		if !retry {
			return nil, fmt.Errorf("acquire advisory lock %#x: %w", key, err)
		}
		log.WithContext(ctx).Warnf("acquire advisory lock %#x failed, retrying in %v: %v", key, delay, err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("acquire advisory lock %#x: %w", key, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, retryMaxDelay)
	}
}

func (l *Locker) tryLock(ctx context.Context, key int64) (*Held, error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	// Poll with pg_try_advisory_lock rather than wait in pg_advisory_lock.
	// A session waiting inside a statement holds a snapshot, and upstream's
	// migrations run CREATE INDEX CONCURRENTLY, which waits for every such
	// snapshot to end: instances waiting for the migration lock in
	// pg_advisory_lock would deadlock with the instance migrating, unseen
	// by PostgreSQL's deadlock detector because the lock and the index
	// build are on different sessions.
	delay := pollMinDelay
	for {
		var got bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil {
			// The lock may have been granted just before a cancellation,
			// so the session is closed rather than returned to the pool.
			discard(conn)
			return nil, err
		}
		if got {
			return &Held{conn: conn, key: key}, nil
		}
		select {
		case <-ctx.Done():
			// The last try returned false, so the session holds nothing.
			conn.Release()
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, pollMaxDelay)
	}
}

// Unlock releases the lock. It returns ErrLockLost when the session no longer
// held the lock (the connection had dropped, or the server restarted or
// failed over), in which case another instance may have held it too.
func (h *Held) Unlock() error {
	ctx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
	defer cancel()

	var released bool
	err := h.conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", h.key).Scan(&released)
	if err != nil {
		// The server releases the lock when the session ends.
		discard(h.conn)
		return fmt.Errorf("%w: unlock %#x: %v", ErrLockLost, h.key, err)
	}
	if !released {
		discard(h.conn)
		return fmt.Errorf("%w: lock %#x was not held by this session", ErrLockLost, h.key)
	}
	h.conn.Release()
	return nil
}

// discard closes conn's session instead of returning it to the pool.
func discard(conn *pgxpool.Conn) {
	pc := conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), discardTimeout)
	defer cancel()
	_ = pc.Close(ctx)
}

// WithLock runs fn while holding key. It does not retry connection errors.
func (l *Locker) WithLock(ctx context.Context, key int64, fn func(ctx context.Context) error) error {
	held, err := l.Lock(ctx, key, false)
	if err != nil {
		return err
	}
	fnErr := fn(ctx)
	if err := held.Unlock(); err != nil {
		return errors.Join(fnErr, err)
	}
	return fnErr
}

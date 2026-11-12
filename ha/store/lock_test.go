package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RXWatcher/netbird-ha/ha/store/storetest"
)

func TestMain(m *testing.M) { storetest.Main(m) }

func newLocker(t *testing.T, dsn string) *Locker {
	t.Helper()
	pool, err := OpenLockPool(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return NewLocker(pool)
}

// advisoryLocks counts the advisory locks granted in the database.
func advisoryLocks(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n))
	return n
}

func TestLockExcludesOtherInstances(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	a, b := newLocker(t, dsn), newLocker(t, dsn)
	ctx := context.Background()

	held, err := a.Lock(ctx, GlobalLockKey, false)
	require.NoError(t, err)

	got := make(chan *Held, 1)
	go func() {
		h, err := b.Lock(ctx, GlobalLockKey, false)
		assert.NoError(t, err)
		got <- h
	}()

	select {
	case <-got:
		t.Fatal("second instance took the lock while the first held it")
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, held.Unlock())
	select {
	case h := <-got:
		require.NoError(t, h.Unlock())
	case <-time.After(5 * time.Second):
		t.Fatal("second instance did not get the lock after the first released it")
	}
	assert.Equal(t, 0, advisoryLocks(t, a.Pool()))
}

func TestLockCancelledWhileWaitingLeavesNoLock(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	a, b := newLocker(t, dsn), newLocker(t, dsn)

	held, err := a.Lock(context.Background(), GlobalLockKey, false)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = b.Lock(ctx, GlobalLockKey, true)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, held.Unlock())
	// The cancelled waiter must not have been granted the lock later.
	require.Eventually(t, func() bool { return advisoryLocks(t, a.Pool()) == 0 }, 5*time.Second, 50*time.Millisecond)

	h, err := a.Lock(context.Background(), GlobalLockKey, false)
	require.NoError(t, err)
	require.NoError(t, h.Unlock())
}

func TestUnlockReportsLostConnection(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	a, b := newLocker(t, dsn), newLocker(t, dsn)
	ctx := context.Background()

	held, err := a.Lock(ctx, GlobalLockKey, false)
	require.NoError(t, err)
	var pid int
	require.NoError(t, held.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

	// The server kills the session, as on a failover or a network loss.
	_, err = b.Pool().Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
	require.NoError(t, err)

	// Another instance can take the lock at once: the server released it.
	other, err := b.Lock(ctx, GlobalLockKey, false)
	require.NoError(t, err)
	require.NoError(t, other.Unlock())

	err = held.Unlock()
	require.ErrorIs(t, err, ErrLockLost)

	// The broken connection was not returned to the pool.
	h, err := a.Lock(ctx, GlobalLockKey, false)
	require.NoError(t, err)
	require.NoError(t, h.Unlock())
}

func TestUnlockReturnsConnectionWithoutLock(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	a := newLocker(t, dsn)
	ctx := context.Background()
	for range 20 {
		h, err := a.Lock(ctx, GlobalLockKey, false)
		require.NoError(t, err)
		require.NoError(t, h.Unlock())
	}
	assert.Equal(t, 0, advisoryLocks(t, a.Pool()))
}

func TestGlobalLockFailsClosedWhileDatabaseIsDown(t *testing.T) {
	// Nothing listens on port 1. The pool is lazy, so it opens fine.
	cfg, err := pgxpool.ParseConfig("postgres://test:test@127.0.0.1:1/test?sslmode=disable&connect_timeout=1")
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	s := Wrap(nil, NewLocker(pool), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	start := time.Now()
	unlock := s.AcquireGlobalLock(ctx)
	unlock()

	// It kept retrying until the caller gave up rather than returning at
	// the first error.
	assert.GreaterOrEqual(t, time.Since(start), 650*time.Millisecond)
	assert.True(t, errors.Is(ctx.Err(), context.DeadlineExceeded))

	// The local slot was released, so a later caller is not stuck.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	s.AcquireGlobalLock(ctx2)()
	assert.ErrorIs(t, ctx2.Err(), context.DeadlineExceeded)
}

func TestGlobalLockSerialisesCallersInOneInstance(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	s := Wrap(nil, newLocker(t, dsn), nil)
	ctx := context.Background()

	unlock := s.AcquireGlobalLock(ctx)
	got := make(chan struct{})
	go func() {
		s.AcquireGlobalLock(ctx)()
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("second caller entered while the first held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	unlock()
	unlock() // a second call is harmless
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("second caller did not enter after release")
	}
}

func TestIsReleaseVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"v0.82.1":                               true,
		"v0.83.0-rc.1":                          true,
		"":                                      false,
		"(devel)":                               false,
		"v0.0.0-20261110120000-a48050004abc":    false,
		"v0.82.2-0.20261110120000-a48050004abc": false,
		"(replaced) ../netbird v0.0.0":          false,
	} {
		assert.Equal(t, want, isReleaseVersion(v), v)
	}
}

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/shared/db"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"
)

const (
	// upstreamModule is the module whose version names a schema.
	upstreamModule = "github.com/netbirdio/netbird"

	// ForceMigrationsEnv makes an instance run upstream's migrations even
	// when another instance already ran them for this release.
	ForceMigrationsEnv = "NB_HA_FORCE_MIGRATIONS"

	migrationsTable = "netbird_ha_schema_migrations"
)

// Store is upstream's store with AcquireGlobalLock replaced by a PostgreSQL
// advisory lock, so that account creation is serialised across instances
// instead of within one process. Every other method is upstream's.
type Store struct {
	nbstore.Store

	locker  *Locker
	metrics telemetry.AppMetrics
	// local serialises the instance's own callers before they take a lock
	// connection, so one instance pins at most one connection for the lock.
	local chan struct{}
}

// Wrap returns inner with the cross-instance global lock.
func Wrap(inner nbstore.Store, locker *Locker, metrics telemetry.AppMetrics) *Store {
	return &Store{Store: inner, locker: locker, metrics: metrics, local: make(chan struct{}, 1)}
}

// AcquireGlobalLock blocks until this instance holds the cluster-wide account
// lock and returns the function that releases it.
//
// The signature cannot report an error. When the lock cannot be taken the
// call keeps retrying until ctx is done, so a database outage makes callers
// wait instead of running unlocked. Only when ctx is done does it return
// without the lock (a no-op unlock); upstream's account writes in the
// critical section (SaveAccount and friends) run in transactions on that same
// ctx and so fail instead of creating an account unlocked.
//
// If the lock's connection dies while the caller is inside the critical
// section, PostgreSQL releases the lock and another instance may enter. That
// cannot be signalled to the caller; it is logged as an error on unlock.
func (s *Store) AcquireGlobalLock(ctx context.Context) (unlock func()) {
	start := time.Now()

	select {
	case s.local <- struct{}{}:
	case <-ctx.Done():
		log.WithContext(ctx).Errorf("global lock not acquired: %v", ctx.Err())
		return func() {}
	}

	held, err := s.locker.Lock(ctx, GlobalLockKey, true)
	if err != nil {
		<-s.local
		log.WithContext(ctx).Errorf("global lock not acquired: %v", err)
		return func() {}
	}

	took := time.Since(start)
	log.WithContext(ctx).Tracef("took %v to acquire global advisory lock", took)
	if s.metrics != nil {
		s.metrics.StoreMetrics().CountGlobalLockAcquisitionDuration(took)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			if err := held.Unlock(); err != nil {
				log.WithContext(ctx).Errorf("global lock released after %v: %v", time.Since(start), err)
			} else {
				log.WithContext(ctx).Tracef("released global advisory lock after %v", time.Since(start))
			}
			<-s.local
		})
	}
}

// MigrationOptions controls OpenSQLStore.
type MigrationOptions struct {
	// Version names the schema upstream's migrations produce. Empty means
	// UpstreamVersion(). A version that does not identify a release
	// ("(devel)", a pseudo-version of a local build) still migrates under
	// the lock, but on every start.
	Version string
	// Force runs the migrations even when they are recorded as done.
	Force bool
	// Instance is recorded with the migration, for operators.
	Instance string
}

// OpenSQLStore builds upstream's SqlStore on conn and runs upstream's
// migrations at most once per release across all instances: under
// MigrationLockKey, the first instance of a release runs them and records the
// release; every other instance waits for the lock, finds the record and
// opens with skipMigration=true. It reports whether this call ran them.
func OpenSQLStore(ctx context.Context, conn *db.Conn, metrics telemetry.AppMetrics, locker *Locker, opts MigrationOptions) (st *nbstore.SqlStore, migrated bool, err error) {
	version := opts.Version
	if version == "" {
		version = UpstreamVersion()
	}
	repeatable := isReleaseVersion(version)

	err = locker.WithLock(ctx, MigrationLockKey, func(ctx context.Context) error {
		pool := locker.Pool()
		if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+migrationsTable+` (
			version     text PRIMARY KEY,
			migrated_at timestamptz NOT NULL DEFAULT now(),
			migrated_by text NOT NULL DEFAULT ''
		)`); err != nil {
			return fmt.Errorf("create %s: %w", migrationsTable, err)
		}

		done := false
		if repeatable && !opts.Force {
			var one int
			err := pool.QueryRow(ctx, `SELECT 1 FROM `+migrationsTable+` WHERE version = $1`, version).Scan(&one)
			switch {
			case err == nil:
				done = true
			case errors.Is(err, pgx.ErrNoRows):
			default:
				return fmt.Errorf("read %s: %w", migrationsTable, err)
			}
		}

		if done {
			log.WithContext(ctx).Infof("upstream migrations for %s already ran, opening the store without them", version)
		} else {
			log.WithContext(ctx).Infof("running upstream migrations for %s under the migration lock", version)
		}
		start := time.Now()
		opened, err := nbstore.NewSqlStore(ctx, conn, metrics, done)
		if err != nil {
			return err
		}
		if !done {
			log.WithContext(ctx).Infof("upstream migrations for %s took %v", version, time.Since(start))
			if repeatable {
				if _, err := pool.Exec(ctx, `INSERT INTO `+migrationsTable+` (version, migrated_by) VALUES ($1, $2)
					ON CONFLICT (version) DO UPDATE SET migrated_at = now(), migrated_by = EXCLUDED.migrated_by`,
					version, opts.Instance); err != nil {
					return fmt.Errorf("record migration %s: %w", version, err)
				}
			}
		}
		st, migrated = opened, !done
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return st, migrated, nil
}

// UpstreamVersion returns the version of the NetBird module this binary was
// built with, or "" when it cannot tell.
func UpstreamVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, dep := range info.Deps {
		if dep.Path != upstreamModule {
			continue
		}
		if dep.Replace != nil {
			// A replaced module is a local or forked build: its schema is
			// not the release's.
			return "(replaced) " + dep.Replace.Path + " " + dep.Replace.Version
		}
		return dep.Version
	}
	return ""
}

// isReleaseVersion reports whether v names a tagged release, whose schema is
// fixed, so its migrations need to run only once.
func isReleaseVersion(v string) bool {
	if v == "" || v == "(devel)" || len(v) < 2 || v[0] != 'v' {
		return false
	}
	// A pseudo-version (v0.0.0-20260101000000-abcdef123456, or
	// v0.82.2-0.20260101000000-abcdef123456) is an untagged commit.
	for i := 0; i+15 < len(v); i++ {
		if (v[i] == '-' || v[i] == '.') && isDigits(v[i+1:i+15]) && v[i+15] == '-' {
			return false
		}
	}
	return true
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// forceMigrationsFromEnv reads ForceMigrationsEnv.
func forceMigrationsFromEnv() bool {
	v, _ := strconv.ParseBool(os.Getenv(ForceMigrationsEnv))
	return v
}

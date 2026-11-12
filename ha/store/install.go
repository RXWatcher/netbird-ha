package store

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/server"
	"github.com/netbirdio/netbird/management/internals/shared/db"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/util/crypt"
)

// Install builds the store eagerly and injects the wrapped store into s, in
// place of upstream's BaseServer.Store. It must run before anything asks s
// for its store, that is in the function passed to upstream's SetNewServer.
//
// It opens upstream's shared database connection (s.DBConn), a small pool of
// its own for advisory locks, and runs upstream's migrations once per release
// across instances. instance names this instance in the migration record.
//
// On an engine other than PostgreSQL it injects nothing and returns a nil
// Locker: SQLite cannot be shared between instances.
func Install(ctx context.Context, s *server.BaseServer, instance string) (*Locker, error) {
	conn := s.DBConn()
	if conn.Engine() != db.PostgresStoreEngine {
		log.WithContext(ctx).Warnf("store engine is %s: the HA store needs PostgreSQL, keeping upstream's single-instance store", conn.Engine())
		return nil, nil
	}

	dsn, ok := nbstore.LookupPostgresDSN()
	if !ok {
		return nil, fmt.Errorf("%s is not set", nbstore.PostgresDsnEnv)
	}
	pool, err := OpenLockPool(ctx, dsn)
	if err != nil {
		return nil, err
	}
	locker := NewLocker(pool)

	metrics := s.Metrics()
	sqlStore, _, err := OpenSQLStore(ctx, conn, metrics, locker, MigrationOptions{
		Force:    forceMigrationsFromEnv(),
		Instance: instance,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open store: %w", err)
	}

	// Mirror upstream's BaseServer.Store.
	if key := s.Config.DataStoreEncryptionKey; key != "" {
		fieldEncrypt, err := crypt.NewFieldEncrypt(key)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("create field encryptor: %w", err)
		}
		sqlStore.SetFieldEncrypt(fieldEncrypt)
	}

	wrapped := Wrap(sqlStore, locker, metrics)
	server.Inject[nbstore.Store](s, wrapped)
	if got, ok := s.Store().(*Store); !ok || got != wrapped {
		pool.Close()
		return nil, errors.New("upstream's store was built before the HA store was installed")
	}
	s.OnStop(pool.Close)

	log.WithContext(ctx).Info("HA store installed: global lock and migrations use PostgreSQL advisory locks")
	return locker, nil
}

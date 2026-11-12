package ha

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/server"
	nbcache "github.com/netbirdio/netbird/management/server/cache"

	"github.com/RXWatcher/netbird-ha/ha/bus"
	"github.com/RXWatcher/netbird-ha/ha/cache"
	"github.com/RXWatcher/netbird-ha/ha/ephemeral"
	"github.com/RXWatcher/netbird-ha/ha/store"
)

// EnvInstanceID names this instance in the migration record. It must be
// unique among running instances;
// the default (<hostname>-<8 random hex>) differs on every start.
const EnvInstanceID = "NB_HA_INSTANCE_ID"

func init() { Register(installState) }

// installState shares the store, cache and ephemeral cleanup between
// instances. It is on whenever HA messaging is (the bus variables are set),
// and then needs PostgreSQL and NB_CACHE_REDIS_ADDRESS.
//
// Unlike the other installers it builds the store eagerly: the store must be
// a concrete value to be injected, and the first thing upstream's boot does
// is build it anyway (open the database, run the migrations). The cache
// client is built eagerly too, without I/O beyond one ping; upstream's
// ephemeral manager is still built lazily.
func installState(s *server.BaseServer) error {
	if _, on, _ := bus.ConfigFromEnv(); !on {
		return nil
	}
	ctx := context.Background()
	instance := instanceID()

	locker, err := store.Install(ctx, s, instance)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	if locker == nil {
		return fmt.Errorf("HA messaging is configured but the store engine is not PostgreSQL; instances cannot share a SQLite store")
	}

	on, err := cache.Install(ctx, s)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	if !on {
		return fmt.Errorf("HA messaging is configured but %s is not set; sessions, PKCE state and one-time tokens must be shared", nbcache.RedisStoreEnvVar)
	}

	ephemeral.Install(s, ephemeral.Config{})
	log.Infof("ha: shared state on as %s (advisory locks, shared cache, ephemeral reconcile)", instance)
	return nil
}

func instanceID() string {
	if id := strings.TrimSpace(os.Getenv(EnvInstanceID)); id != "" {
		return id
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "mgmt"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

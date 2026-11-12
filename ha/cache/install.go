package cache

import (
	"context"
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/server"
	nbcache "github.com/netbirdio/netbird/management/server/cache"
)

// Install injects a Sentinel-aware store for NB_CACHE_REDIS_ADDRESS (or its
// legacy name) into s, in place of upstream's BaseServer.CacheStore. It must
// run before anything asks s for its cache store.
//
// Without the variable it injects nothing and returns false: upstream's
// in-memory cache then holds sessions, PKCE state and one-time tokens per
// instance, which breaks logins behind a load balancer.
func Install(ctx context.Context, s *server.BaseServer) (bool, error) {
	addr := nbcache.GetAddrFromEnv()
	if addr == "" {
		log.WithContext(ctx).Warnf("%s is not set: the cache is in memory and not shared between instances", nbcache.RedisStoreEnvVar)
		return false, nil
	}
	st, err := New(ctx, addr, nbcache.DefaultStoreMaxConn)
	if err != nil {
		return false, err
	}
	server.Inject[nbcache.Store](s, st)
	if got, ok := s.CacheStore().(*Store); !ok || got != st {
		_ = st.Close()
		return false, errors.New("upstream's cache store was built before the HA cache was installed")
	}
	s.OnStop(func() { _ = st.Close() })
	return true, nil
}

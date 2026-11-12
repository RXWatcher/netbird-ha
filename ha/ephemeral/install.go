package ephemeral

import (
	"github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral/manager"
	"github.com/netbirdio/netbird/management/internals/server"

	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"

	hastore "github.com/RXWatcher/netbird-ha/ha/store"
)

// Install injects Manager into s in place of upstream's
// BaseServer.EphemeralManager. Upstream's manager is built on first use
// exactly as BaseServer.EphemeralManager builds it. locker is the one
// ha/store's Install returned; instance must be unique per running instance.
func Install(s *server.BaseServer, locker *hastore.Locker, instance string, cfg Config) *Manager {
	m := New(func() upstream.Manager {
		em := manager.NewEphemeralManager(s.Store(), s.PeersManager())
		if metrics := s.Metrics(); metrics != nil {
			em.SetMetrics(metrics.EphemeralPeersMetrics())
		}
		return em
	}, s.Store, locker, instance, cfg)
	server.Inject[upstream.Manager](s, m)
	return m
}

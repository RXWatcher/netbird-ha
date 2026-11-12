package ephemeral

import (
	"github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral/manager"
	"github.com/netbirdio/netbird/management/internals/server"

	upstream "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral"
)

// Install injects Manager into s in place of upstream's
// BaseServer.EphemeralManager. Upstream's manager is built on first use
// exactly as BaseServer.EphemeralManager builds it.
func Install(s *server.BaseServer, cfg Config) *Manager {
	m := New(func() upstream.Manager {
		em := manager.NewEphemeralManager(s.Store(), s.PeersManager())
		if metrics := s.Metrics(); metrics != nil {
			em.SetMetrics(metrics.EphemeralPeersMetrics())
		}
		return em
	}, s.Store, cfg)
	server.Inject[upstream.Manager](s, m)
	return m
}

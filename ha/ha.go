// Package ha wires the active-active pieces into upstream's management server.
//
// NewServer is passed to upstream's cmd.SetNewServer. It builds the stock
// BaseServer and overrides individual dependencies with server.Inject before
// upstream's boot code asks for them. Inject stores the value immediately, so
// every injected value must be cheap to construct and build its real
// dependencies lazily (on first use).
package ha

import (
	"github.com/netbirdio/netbird/management/internals/server"
)

// NewServer builds upstream's management server with the HA overrides injected.
func NewServer(cfg *server.Config) server.Server {
	s := server.NewServer(cfg)
	return s
}

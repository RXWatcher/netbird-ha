// Package ha wires the active-active pieces into upstream's management server.
//
// NewServer is passed to upstream's cmd.SetNewServer. It builds the stock
// BaseServer and overrides individual dependencies with server.Inject before
// upstream's boot code asks for them. Inject stores the value immediately, so
// every injected value must be cheap to construct and build its real
// dependencies lazily (on first use).
//
// Each override is an installer. A package adds its own from an init function in
// its own file (Register), so workstreams do not edit each other's code.
//
// With no NB_HA_* variables set nothing is injected and the server is upstream's.
package ha

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	nmapcontroller "github.com/netbirdio/netbird/management/internals/controllers/network_map/controller"
	"github.com/netbirdio/netbird/management/internals/server"
	nbgrpc "github.com/netbirdio/netbird/management/internals/shared/grpc"

	"github.com/Bloem-Studios/netbird-ha/ha/bus"
	"github.com/Bloem-Studios/netbird-ha/ha/controller"
	"github.com/Bloem-Studios/netbird-ha/ha/wgkey"
)

// EnvResyncInterval overrides the periodic network-map resync (Go duration, default
// 10m; "off" disables it, leaving only the resync after each resubscription).
const EnvResyncInterval = "NB_HA_RESYNC_INTERVAL"

// Installer overrides dependencies on a freshly built server. It must not call any
// BaseServer accessor directly, only inside the lazy constructors it injects.
type Installer func(s *server.BaseServer) error

var (
	installersMu sync.Mutex
	installers   = []Installer{installWGKey, installController}
)

// Register adds an installer; call it from an init function.
func Register(i Installer) {
	installersMu.Lock()
	installers = append(installers, i)
	installersMu.Unlock()
}

// NewServer builds upstream's management server with the HA overrides injected.
func NewServer(cfg *server.Config) server.Server {
	s := server.NewServer(cfg)
	installersMu.Lock()
	list := append([]Installer(nil), installers...)
	installersMu.Unlock()
	for _, install := range list {
		if err := install(s); err != nil {
			log.Fatalf("ha: %v", err)
		}
	}
	return s
}

// installWGKey shares the server WireGuard key. It is required whenever the bus is
// on: without it clients fail to decrypt once their streams land on two instances.
func installWGKey(s *server.BaseServer) error {
	key, ok, err := wgkey.FromEnv()
	if err != nil {
		return err
	}
	if _, busOn, _ := bus.ConfigFromEnv(); busOn && !ok {
		return fmt.Errorf("HA messaging is configured but %s or %s is not set; every instance needs the same key", wgkey.EnvKey, wgkey.EnvKeyFile)
	}
	if !ok {
		return nil
	}
	log.Infof("ha: using the shared management WireGuard key (public %s)", key.PublicKey())
	server.Inject[nbgrpc.SecretsManager](s, wgkey.New(key, func() nbgrpc.SecretsManager {
		// Same construction as upstream's BaseServer.SecretsManager.
		m, err := nbgrpc.NewTimeBasedAuthSecretsManager(s.PeersUpdateManager(), s.Config.TURNConfig, s.Config.Relay, s.SettingsManager(), s.GroupsManager())
		if err != nil {
			log.Fatalf("failed to create secrets manager: %v", err)
		}
		return m
	}))
	return nil
}

// installController wraps the network-map controller and starts the bus once the
// server has initialised.
func installController(s *server.BaseServer) error {
	cfg, on, err := bus.ConfigFromEnv()
	if err != nil {
		return err
	}
	if !on {
		log.Infof("ha: %s not set, cross-instance updates are off", bus.EnvRedisURL)
		return nil
	}
	opts, err := controllerOptionsFromEnv()
	if err != nil {
		return err
	}
	b, err := bus.NewRedis(cfg)
	if err != nil {
		return err
	}

	ctrl := controller.New(func() network_map.Controller {
		// Same construction as upstream's BaseServer.NetworkMapController.
		return nmapcontroller.NewController(context.Background(), s.Store(), s.Metrics(), s.PeersUpdateManager(), s.AccountRequestBuffer(), s.IntegratedValidator(), s.SettingsManager(), s.DNSDomain(), s.EphemeralManager(), s.Config, s.NetworkMapStore())
	}, func() network_map.PeersUpdateManager { return s.PeersUpdateManager() }, b, opts)
	server.Inject[network_map.Controller](s, ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	s.AfterInit(func(s *server.BaseServer) {
		if m := s.Metrics(); m != nil {
			if err := b.Metrics().Register(m.GetMeter()); err != nil {
				log.Warnf("ha: bus metrics: %v", err)
			}
			if err := ctrl.Metrics().Register(m.GetMeter()); err != nil {
				log.Warnf("ha: controller metrics: %v", err)
			}
		}
		log.Infof("ha: cross-instance updates on as %s, channel %s", b.Origin(), b.Channel())
		ctrl.Start(ctx)
	})
	s.OnStop(func() {
		cancel()
		_ = b.Close()
	})
	return nil
}

func controllerOptionsFromEnv() (controller.Options, error) {
	var o controller.Options
	switch v := os.Getenv(EnvResyncInterval); v {
	case "":
	case "off":
		o.ResyncInterval = -1
	default:
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return o, fmt.Errorf("%s: want a positive duration or \"off\", got %q", EnvResyncInterval, v)
		}
		o.ResyncInterval = d
	}
	return o, nil
}

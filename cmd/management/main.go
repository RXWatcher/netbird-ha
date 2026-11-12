// Command netbird-mgmt is upstream NetBird's management server with the HA wrapper
// installed through upstream's SetNewServer hook. Everything else (flags, config,
// subcommands) is upstream's.
package main

import (
	"net/http"
	_ "net/http/pprof" //nolint:gosec // same opt-in pprof listener as upstream's main
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/cmd"

	"github.com/RXWatcher/netbird-ha/ha"
)

func main() {
	if pprofAddr := os.Getenv("NB_PPROF_ADDR"); pprofAddr != "" {
		log.Infof("pprof enabled, listening on: %s", pprofAddr)
		go func() {
			log.Println(http.ListenAndServe(pprofAddr, nil)) //nolint:gosec
		}()
	}

	cmd.SetNewServer(ha.NewServer)

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// Command netbird-signal is upstream NetBird's signal server, unchanged. The HA part
// is the dispatcher module, swapped in by the replace line in go.mod.
package main

import (
	"os"

	"github.com/netbirdio/netbird/signal/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

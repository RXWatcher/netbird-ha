// Package bed drives the docker compose test bed in deploy/ from Go tests:
// the management REST API, the client containers, HAProxy's runtime API,
// pg_auto_failover and Valkey Sentinel. It uses only the standard library and
// the docker CLI.
package bed

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Project is the compose project name; containers are named <Project>-<service>-1.
const Project = "nbha-bed"

// Network is the docker network the bed runs on.
const Network = "nbha-bed"

// Bed holds the connection details deploy/bed.sh reports.
type Bed struct {
	Root      string // repository root
	Impl      string // stock | ha
	Instances int    // management/signal instances (1 or 2)
	LBMode    string // l4 | l7

	API      *API // load-balanced management API (lb:80)
	APIMgmt1 *API // management API pinned to mgmt-1 (lb:8081)
	APIMgmt2 *API // management API pinned to mgmt-2 (lb:8082)

	LBStats string // HAProxy stats URL
	LBAdmin string // HAProxy runtime API host:port

	PAT string

	// Images scenario 8 rolls the instances onto (empty: recreate with the same image).
	UpgradeMgmtImage, UpgradeSignalImage string
}

// Load reads the bed's details from `deploy/bed.sh env`.
func Load(ctx context.Context) (*Bed, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	out, err := exec.CommandContext(ctx, filepath.Join(root, "deploy", "bed.sh"), "env").Output()
	if err != nil {
		return nil, fmt.Errorf("deploy/bed.sh env: %w (is the bed up? run make bed-up)", err)
	}
	vars := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := strings.TrimPrefix(strings.TrimSpace(sc.Text()), "export ")
		if k, v, ok := strings.Cut(line, "="); ok {
			vars[k] = v
		}
	}
	b := &Bed{
		Root:    root,
		Impl:    vars["NBHA_IMPL"],
		LBMode:  vars["NBHA_LB_MODE"],
		LBStats: vars["NBHA_LB_STATS"],
		LBAdmin: vars["NBHA_LB_ADMIN"],
		PAT:     vars["NBHA_PAT"],

		UpgradeMgmtImage:   vars["NBHA_UPGRADE_MGMT_IMAGE"],
		UpgradeSignalImage: vars["NBHA_UPGRADE_SIGNAL_IMAGE"],
	}
	b.Instances = 1
	if vars["NBHA_INSTANCES"] == "2" {
		b.Instances = 2
	}
	if b.PAT == "" {
		return nil, fmt.Errorf("bed has no PAT; bootstrap did not finish")
	}
	b.API = NewAPI(vars["NBHA_API"], b.PAT)
	b.APIMgmt1 = NewAPI(vars["NBHA_API_MGMT1"], b.PAT)
	b.APIMgmt2 = NewAPI(vars["NBHA_API_MGMT2"], b.PAT)
	return b, nil
}

func repoRoot() (string, error) {
	if r := os.Getenv("NBHA_ROOT"); r != "" {
		return r, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for d := dir; d != "/"; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "deploy", "bed.sh")); err == nil {
			return d, nil
		}
	}
	return "", fmt.Errorf("deploy/bed.sh not found above %s; set NBHA_ROOT", dir)
}

// HA reports whether management and signal were built from this repository.
func (b *Bed) HA() bool { return b.Impl == "ha" }

// Container returns the container name of a compose service.
func Container(service string) string { return Project + "-" + service + "-1" }

// Docker runs a docker CLI command and returns its combined output.
func Docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// Exec runs a command in a running container.
func Exec(ctx context.Context, container string, args ...string) (string, error) {
	return Docker(ctx, append([]string{"exec", container}, args...)...)
}

// Compose runs docker compose with the bed's project, files and env file.
func (b *Bed) Compose(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(b.Root, "deploy", "bed.sh"), append([]string{"compose"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("compose %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// Kill sends SIGKILL to a service's container (kill -9).
func Kill(ctx context.Context, service string) error {
	_, err := Docker(ctx, "kill", "--signal", "KILL", Container(service))
	return err
}

// Stop stops a service's container gracefully.
func Stop(ctx context.Context, service string) error {
	_, err := Docker(ctx, "stop", "--time", "10", Container(service))
	return err
}

// Start starts a stopped service container.
func Start(ctx context.Context, service string) error {
	_, err := Docker(ctx, "start", Container(service))
	return err
}

// Running reports whether a service's container is running.
func Running(ctx context.Context, service string) bool {
	out, err := Docker(ctx, "inspect", "--format", "{{.State.Running}}", Container(service))
	return err == nil && strings.TrimSpace(out) == "true"
}

// StartedAt returns the container's last start time (changes on restart).
func StartedAt(ctx context.Context, service string) string {
	out, _ := Docker(ctx, "inspect", "--format", "{{.State.StartedAt}}", Container(service))
	return strings.TrimSpace(out)
}

// Logs returns a container's log output since t.
func Logs(ctx context.Context, container string, since time.Time) (string, error) {
	return Docker(ctx, "logs", "--since", since.UTC().Format(time.RFC3339Nano), container)
}

// Eventually polls fn every interval until it returns nil or the timeout
// passes, and returns how long it took or the last error.
func Eventually(ctx context.Context, timeout, interval time.Duration, fn func() error) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		err := fn()
		if err == nil {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return time.Since(start), fmt.Errorf("after %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-time.After(interval):
		}
	}
}

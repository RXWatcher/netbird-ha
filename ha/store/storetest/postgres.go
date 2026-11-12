// Package storetest starts a throwaway PostgreSQL for tests.
//
// Set NB_HA_TEST_POSTGRES_DSN to use a server you started yourself (it must
// be a disposable one: every test creates and drops its own database there).
// Otherwise, when docker is available, Main starts a postgres container on a
// random loopback port and removes it when the package's tests finish.
// Without either, the tests that need PostgreSQL skip.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// DSNEnv names an existing disposable server to use instead of docker.
const DSNEnv = "NB_HA_TEST_POSTGRES_DSN"

const image = "postgres:17-alpine"

var adminDSN string

// Main runs the package's tests with a PostgreSQL server available.
func Main(m *testing.M) {
	code := func() int {
		if dsn := os.Getenv(DSNEnv); dsn != "" {
			adminDSN = dsn
			return m.Run()
		}
		stop, dsn, err := startContainer()
		if err != nil {
			fmt.Fprintf(os.Stderr, "storetest: no PostgreSQL (%v); tests that need it will skip\n", err)
			return m.Run()
		}
		defer stop()
		adminDSN = dsn
		return m.Run()
	}()
	os.Exit(code)
}

func startContainer() (stop func(), dsn string, err error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, "", err
	}
	removeStale()
	name := "nbha-test-pg-" + randHex(4)
	out, err := dockerCmd("run", "-d", "--rm", "--name", name,
		"--label", staleLabel+"="+strconv.FormatInt(time.Now().Unix(), 10),
		"-e", "POSTGRES_PASSWORD=test", "-e", "POSTGRES_USER=test", "-e", "POSTGRES_DB=test",
		"-p", "127.0.0.1::5432", image,
		"-c", "max_connections=300", "-c", "fsync=off").CombinedOutput()
	if err != nil {
		return nil, "", fmt.Errorf("docker run: %v: %s", err, out)
	}
	stop = func() { _ = dockerCmd("rm", "-f", name).Run() }

	port, err := dockerCmd("port", name, "5432/tcp").Output()
	if err != nil {
		stop()
		return nil, "", fmt.Errorf("docker port: %w", err)
	}
	hostPort := strings.TrimSpace(strings.Split(string(port), "\n")[0])
	dsn = "postgres://test:test@" + hostPort + "/test?sslmode=disable"

	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			err = conn.Ping(ctx)
			_ = conn.Close(ctx)
		}
		cancel()
		if err == nil {
			return stop, dsn, nil
		}
		if time.Now().After(deadline) {
			stop()
			return nil, "", fmt.Errorf("postgres did not become ready: %w", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// staleLabel marks the containers Main starts with their start time. A test
// binary that panics or is killed leaves its container behind; the next run
// removes those older than staleAge.
const (
	staleLabel = "nbha-test-started"
	staleAge   = 30 * time.Minute
)

func removeStale() {
	out, err := dockerCmd("ps", "-a", "--filter", "label="+staleLabel,
		"--format", `{{.ID}} {{.Label "`+staleLabel+`"}}`).Output()
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		id, started, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		sec, err := strconv.ParseInt(started, 10, 64)
		if err == nil && time.Since(time.Unix(sec, 0)) > staleAge {
			_ = dockerCmd("rm", "-f", id).Run()
		}
	}
}

// StartForTest makes a server available to t when the package has no Main:
// it uses DSNEnv, or starts a container that is removed when t ends. Call it
// before NewDatabase.
func StartForTest(t testing.TB) {
	t.Helper()
	if adminDSN != "" {
		return
	}
	if dsn := os.Getenv(DSNEnv); dsn != "" {
		adminDSN = dsn
		return
	}
	stop, dsn, err := startContainer()
	if err != nil {
		t.Skipf("no PostgreSQL available (%v): set %s or install docker", err, DSNEnv)
	}
	adminDSN = dsn
	t.Cleanup(func() {
		stop()
		adminDSN = ""
	})
}

// NewDatabase creates an empty database for t, drops it when t ends, and
// returns its DSN. It skips t when no server is available.
func NewDatabase(t testing.TB) string {
	t.Helper()
	if adminDSN == "" {
		t.Skip("no PostgreSQL available: set " + DSNEnv + " or install docker")
	}
	name := "t_" + randHex(6)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect to test server: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			return
		}
		defer func() { _ = admin.Close(ctx) }()
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse test dsn: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// dockerCmd runs the docker CLI with this helper's own arguments.
func dockerCmd(args ...string) *exec.Cmd {
	return exec.Command("docker", args...) //nolint:gosec // fixed binary, arguments built here
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// SentinelImageEnv overrides the Valkey image of the Sentinel test.
const SentinelImageEnv = "NB_HA_TEST_VALKEY_IMAGE"

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

// sentinelDeployment starts a throwaway master, replica and Sentinel on a
// private docker network and returns the Sentinel's loopback address. The
// Sentinel hands out the master's bridge address, which the host can reach
// on Linux.
func sentinelDeployment(t *testing.T) (sentinelAddr string) {
	t.Helper()
	if testing.Short() {
		t.Skip("needs docker")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("needs docker")
	}
	image := os.Getenv(SentinelImageEnv)
	if image == "" {
		image = "valkey/valkey:8.1-alpine"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	id := "nbha-test-cache-" + hex.EncodeToString(b)
	network := id
	docker(t, "network", "create", network)
	var containers []string
	t.Cleanup(func() {
		for _, c := range containers {
			_ = exec.Command("docker", "rm", "-f", c).Run()
		}
		_ = exec.Command("docker", "network", "rm", network).Run()
	})
	run := func(name string, args ...string) string {
		containers = append(containers, name)
		docker(t, append([]string{"run", "-d", "--rm", "--name", name, "--network", network}, args...)...)
		return docker(t, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name)
	}

	masterIP := run(id+"-m", image, "valkey-server", "--port", "6379", "--save", "", "--appendonly", "no")
	run(id+"-r", image, "valkey-server", "--port", "6379", "--save", "", "--appendonly", "no", "--replicaof", masterIP, "6379")
	conf := fmt.Sprintf("port 26379\nsentinel monitor mymaster %s 6379 1\nsentinel down-after-milliseconds mymaster 2000\nsentinel failover-timeout mymaster 10000\n", masterIP)
	run(id+"-s", "-p", "127.0.0.1::26379", "--entrypoint", "sh", image, "-c",
		"printf '"+conf+"' > /tmp/sentinel.conf && exec valkey-server /tmp/sentinel.conf --sentinel")
	port := docker(t, "port", id+"-s", "26379/tcp")
	sentinelAddr = strings.Split(port, "\n")[0]

	// Wait until the Sentinel knows the master and its replica.
	sc := redis.NewSentinelClient(&redis.Options{Addr: sentinelAddr})
	defer sc.Close()
	require.Eventually(t, func() bool {
		replicas, err := sc.Replicas(context.Background(), "mymaster").Result()
		return err == nil && len(replicas) == 1 && !strings.Contains(fmt.Sprint(replicas), "s_down")
	}, 30*time.Second, 200*time.Millisecond, "sentinel did not discover the replica")

	// Skip rather than fail where the host cannot reach the bridge.
	direct := redis.NewClient(&redis.Options{Addr: masterIP + ":6379", DialTimeout: 2 * time.Second})
	defer direct.Close()
	if err := direct.Ping(context.Background()).Err(); err != nil {
		t.Skipf("host cannot reach the docker bridge (%v)", err)
	}
	return sentinelAddr
}

func TestSentinelFailover(t *testing.T) {
	sentinelAddr := sentinelDeployment(t)
	ctx := context.Background()

	st, err := New(ctx, "redis://"+sentinelAddr+"/0?master_name=mymaster", 10)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.True(t, st.sentinel)
	checkSemantics(t, st)

	ok, err := st.SetNX(ctx, "before-failover", "kept", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	// Let the replica acknowledge the write so it survives the switch.
	require.NoError(t, st.Client().Do(ctx, "WAIT", 1, 5000).Err())

	sc := redis.NewSentinelClient(&redis.Options{Addr: sentinelAddr})
	defer sc.Close()
	oldMaster, err := sc.GetMasterAddrByName(ctx, "mymaster").Result()
	require.NoError(t, err)
	require.NoError(t, sc.Failover(ctx, "mymaster").Err())
	require.Eventually(t, func() bool {
		addr, err := sc.GetMasterAddrByName(ctx, "mymaster").Result()
		return err == nil && addr[0] != oldMaster[0]
	}, 30*time.Second, 200*time.Millisecond, "sentinel did not promote the replica")

	// The same store follows the new master: writes succeed (a client still
	// on the demoted master would get READONLY) and the replicated key is
	// there to consume exactly once.
	require.Eventually(t, func() bool {
		ok, err := st.SetNX(ctx, "after-failover", "x", time.Minute)
		return err == nil && ok
	}, 30*time.Second, 200*time.Millisecond, "store did not follow the failover")
	v, found, err := st.GetDel(ctx, "before-failover")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "kept", v)
	checkSemantics(t, st)
}

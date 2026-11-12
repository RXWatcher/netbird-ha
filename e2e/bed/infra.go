package bed

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// HAProxy sends a runtime API command and returns the reply.
func (b *Bed) HAProxy(ctx context.Context, command string) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", b.LBAdmin)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, command+"\n"); err != nil {
		return "", err
	}
	out, err := io.ReadAll(conn)
	return string(out), err
}

// ServerState returns HAProxy's status (UP, DOWN, MAINT, ...) of backend/server.
func (b *Bed) ServerState(ctx context.Context, backend, server string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.LBStats+"/;csv", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(bs), "\n") {
		f := strings.Split(line, ",")
		if len(f) > 17 && f[0] == backend && f[1] == server {
			return f[17], nil
		}
	}
	return "", fmt.Errorf("%s/%s not in HAProxy stats", backend, server)
}

// WaitServerUp waits until HAProxy reports backend/server UP.
func (b *Bed) WaitServerUp(ctx context.Context, backend, server string, timeout time.Duration) error {
	_, err := Eventually(ctx, timeout, 500*time.Millisecond, func() error {
		st, err := b.ServerState(ctx, backend, server)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(st, "UP") {
			return fmt.Errorf("%s/%s is %s", backend, server, st)
		}
		return nil
	})
	return err
}

var sessRe = regexp.MustCompile(`src=([0-9.]+):\d+ fe=(\S+) be=(\S+) srv=(\S+)`)

// Placement maps each container (by bed IP) to the servers its live HAProxy
// sessions use, e.g. placement["c1"]["mgmt"] == "mgmt-1".
func (b *Bed) Placement(ctx context.Context) (map[string]map[string]string, error) {
	out, err := b.HAProxy(ctx, "show sess")
	if err != nil {
		return nil, err
	}
	names, err := containerIPs(ctx)
	if err != nil {
		return nil, err
	}
	res := map[string]map[string]string{}
	for _, m := range sessRe.FindAllStringSubmatch(out, -1) {
		name, ok := names[m[1]]
		if !ok {
			continue
		}
		kind := ""
		switch {
		case strings.HasPrefix(m[3], "be_mgmt"):
			kind = "mgmt"
		case strings.HasPrefix(m[3], "be_signal"):
			kind = "signal"
		default:
			continue
		}
		if res[name] == nil {
			res[name] = map[string]string{}
		}
		res[name][kind] = m[4]
	}
	return res, nil
}

// containerIPs maps bed IP addresses to short container names (service or dyn name).
func containerIPs(ctx context.Context) (map[string]string, error) {
	out, err := Docker(ctx, "network", "inspect", Network, "--format",
		`{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}`)
	if err != nil {
		return nil, err
	}
	res := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		ip, _, _ := strings.Cut(f[1], "/")
		name := strings.TrimPrefix(f[0], Project+"-")
		name = strings.TrimPrefix(name, "dyn-")
		name = strings.TrimSuffix(name, "-1")
		res[ip] = name
	}
	return res, nil
}

// PrimaryDSN reaches the writable pg_auto_failover node.
const PrimaryDSN = "postgresql://postgres@pg-1:5432,pg-2:5432/%s?sslmode=disable&target_session_attrs=read-write&connect_timeout=3"

// SQL runs a query on the writable primary (from the monitor container) and
// returns psql's unaligned, tuples-only output.
func SQL(ctx context.Context, db, query string) (string, error) {
	out, err := Exec(ctx, Container("pgaf-monitor"), "psql", fmt.Sprintf(PrimaryDSN, db), "-tAc", query)
	return strings.TrimSpace(out), err
}

// SQLInt runs a query returning one integer.
func SQLInt(ctx context.Context, db, query string) (int64, error) {
	out, err := SQL(ctx, db, query)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// AccountSerial returns the network serial of the (single) account.
func AccountSerial(ctx context.Context) (uint64, error) {
	v, err := SQLInt(ctx, "netbird", "select max(network_serial) from accounts")
	return uint64(v), err
}

// PGAutoctl runs pg_autoctl as postgres in the monitor container.
func PGAutoctl(ctx context.Context, args ...string) (string, error) {
	base := []string{"gosu", "postgres", "env",
		"XDG_CONFIG_HOME=/var/lib/postgresql/pgaf/config", "XDG_DATA_HOME=/var/lib/postgresql/pgaf/share",
		"pg_autoctl"}
	args = append(args, "--pgdata", "/var/lib/postgresql/pgaf/data")
	return Exec(ctx, Container("pgaf-monitor"), append(base, args...)...)
}

// PGPrimary returns the name of the node pg_auto_failover reports as primary.
func PGPrimary(ctx context.Context) (string, error) {
	state, err := Exec(ctx, Container("pgaf-monitor"), "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "pg_auto_failover", "-tAc",
		"select nodename from pgautofailover.node where reportedstate in ('primary','wait_primary','single') and goalstate = reportedstate")
	if err != nil {
		return "", err
	}
	p := strings.TrimSpace(state)
	if p == "" || strings.Contains(p, "\n") {
		return "", fmt.Errorf("no single primary: %q", p)
	}
	return p, nil
}

// Sentinel runs a valkey-cli command against sentinel-1.
func Sentinel(ctx context.Context, args ...string) (string, error) {
	out, err := Exec(ctx, Container("sentinel-1"), append([]string{"valkey-cli", "-p", "26379"}, args...)...)
	return strings.TrimSpace(out), err
}

// ValkeyMaster returns the host Sentinel reports as master.
func ValkeyMaster(ctx context.Context) (string, error) {
	out, err := Sentinel(ctx, "sentinel", "get-master-addr-by-name", "nbha")
	if err != nil {
		return "", err
	}
	host, _, _ := strings.Cut(out, "\n")
	return strings.TrimSpace(host), nil
}

// PGStates returns the nodes' "reported/goal" states sorted, e.g.
// "primary/primary secondary/secondary".
func PGStates(ctx context.Context) (string, error) {
	out, err := Exec(ctx, Container("pgaf-monitor"), "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "pg_auto_failover", "-tAc",
		"select string_agg(reportedstate || '/' || goalstate, ' ' order by reportedstate) from pgautofailover.node")
	return strings.TrimSpace(out), err
}

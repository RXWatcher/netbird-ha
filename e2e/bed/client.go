package bed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Clients are the bed's compose client services.
var Clients = []string{"c1", "c2", "c3", "c4", "c5", "c6"}

// HomeInstance is the management/signal instance each compose client is
// pinned to while that instance is up (deploy/compose/compose.yml).
var HomeInstance = map[string]int{"c1": 1, "c2": 1, "c3": 2, "c4": 2, "c5": 1, "c6": 2}

// ForceRelay lists the clients that run with NB_FORCE_RELAY=true.
var ForceRelay = map[string]bool{"c5": true, "c6": true}

// Status is the subset of `netbird status --json` the scenarios use.
type Status struct {
	DaemonStatus string `json:"daemonStatus"`
	Management   struct {
		URL       string `json:"url"`
		Connected bool   `json:"connected"`
		Error     string `json:"error"`
	} `json:"management"`
	Signal struct {
		URL       string `json:"url"`
		Connected bool   `json:"connected"`
	} `json:"signal"`
	NetbirdIP string `json:"netbirdIp"`
	FQDN      string `json:"fqdn"`
	PublicKey string `json:"publicKey"`
	Peers     struct {
		Total     int          `json:"total"`
		Connected int          `json:"connected"`
		Details   []PeerStatus `json:"details"`
	} `json:"peers"`
}

// PeerStatus is one remote peer as a client sees it.
type PeerStatus struct {
	FQDN           string `json:"fqdn"`
	NetbirdIP      string `json:"netbirdIp"`
	PublicKey      string `json:"publicKey"`
	Status         string `json:"status"`
	ConnectionType string `json:"connectionType"`
	RelayAddress   string `json:"relayAddress"`
}

// Name returns the peer's short host name (fqdn up to the first dot).
func (p PeerStatus) Name() string { n, _, _ := strings.Cut(p.FQDN, "."); return n }

// PeerNames returns the names of the peers in the client's network map.
func (s Status) PeerNames() map[string]PeerStatus {
	out := map[string]PeerStatus{}
	for _, p := range s.Peers.Details {
		out[p.Name()] = p
	}
	return out
}

// IP returns the client's overlay IPv4 address without the prefix length.
func (s Status) IP() string { ip, _, _ := strings.Cut(s.NetbirdIP, "/"); return ip }

// ClientStatus runs `netbird status --json` in a client container.
func ClientStatus(ctx context.Context, container string) (Status, error) {
	var s Status
	out, err := Exec(ctx, container, "netbird", "status", "--json")
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		return s, fmt.Errorf("parse status of %s: %w: %.200s", container, err, out)
	}
	return s, nil
}

// EnableSerialLogging turns on sync-response persistence so the client logs
// "sync response persisted with serial N" for every network map it accepts
// (with NB_LOG_LEVEL=debug, which the bed sets).
func EnableSerialLogging(ctx context.Context, container string) error {
	_, err := Exec(ctx, container, "netbird", "debug", "persistence", "on")
	return err
}

var serialRe = regexp.MustCompile(`sync response persisted with serial (\d+)`)

// LastSerial returns the highest network-map serial the client logged since t,
// or 0 when it logged none.
func LastSerial(ctx context.Context, container string, since time.Time) (uint64, error) {
	out, err := Logs(ctx, container, since)
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, m := range serialRe.FindAllStringSubmatch(out, -1) {
		if v, err := strconv.ParseUint(m[1], 10, 64); err == nil && v > max {
			max = v
		}
	}
	return max, nil
}

// PingResult summarises a ping run.
type PingResult struct {
	Sent, Received int
	// MaxGap is the longest run of consecutive lost replies, as a duration.
	MaxGap time.Duration
	Output string
}

// Loss returns the lost fraction (0..1).
func (r PingResult) Loss() float64 {
	if r.Sent == 0 {
		return 1
	}
	return float64(r.Sent-r.Received) / float64(r.Sent)
}

var (
	pingStatsRe = regexp.MustCompile(`(\d+) packets transmitted, (\d+) (?:packets )?received`)
	pingSeqRe   = regexp.MustCompile(`seq=(\d+)`)
)

// Ping pings ip from a client container for the given count at interval.
func Ping(ctx context.Context, container, ip string, count int, interval time.Duration) PingResult {
	out, _ := Exec(ctx, container, "ping", "-c", strconv.Itoa(count), "-i", fmt.Sprintf("%.2f", interval.Seconds()), "-W", "1", ip)
	return parsePing(out, interval)
}

// PingFor pings ip continuously for d (busybox ping -w).
func PingFor(ctx context.Context, container, ip string, d, interval time.Duration) PingResult {
	out, _ := Exec(ctx, container, "ping", "-w", strconv.Itoa(int(d.Seconds())), "-i", fmt.Sprintf("%.2f", interval.Seconds()), "-W", "1", ip)
	return parsePing(out, interval)
}

func parsePing(out string, interval time.Duration) PingResult {
	r := PingResult{Output: out}
	if m := pingStatsRe.FindStringSubmatch(out); m != nil {
		r.Sent, _ = strconv.Atoi(m[1])
		r.Received, _ = strconv.Atoi(m[2])
	}
	prev := -1
	maxGap := 0
	for _, m := range pingSeqRe.FindAllStringSubmatch(out, -1) {
		seq, _ := strconv.Atoi(m[1])
		if gap := seq - prev - 1; gap > maxGap {
			maxGap = gap
		}
		prev = seq
	}
	if tail := r.Sent - 1 - prev; tail > maxGap {
		maxGap = tail
	}
	r.MaxGap = time.Duration(maxGap) * interval
	return r
}

// Reachable pings ip a few times and reports whether any reply came back.
func Reachable(ctx context.Context, container, ip string) bool {
	return Ping(ctx, container, ip, 3, 300*time.Millisecond).Received > 0
}

// DynClient is a client container a scenario starts and removes itself.
type DynClient struct {
	Name      string
	Container string
}

// DynClientOptions configures StartDynClient.
type DynClientOptions struct {
	SetupKey      string // empty: the client waits for an SSO login
	ManagementURL string // e.g. http://lb:8081
	ForceRelay    bool
}

// StartDynClient starts a stock client container on the bed network. It is
// labelled so that `make bed-down` removes it even if the test does not.
func StartDynClient(ctx context.Context, name string, o DynClientOptions) (*DynClient, error) {
	image := os.Getenv("NBHA_CLIENT_IMAGE")
	if image == "" {
		image = "netbirdio/netbird:0.82.1"
	}
	c := &DynClient{Name: name, Container: Project + "-dyn-" + name}
	_, _ = Docker(ctx, "rm", "-f", c.Container)
	args := []string{"run", "-d", "--name", c.Container, "--hostname", name,
		"--label", "nbha.bed.dynamic=1", "--network", Network,
		"--cap-add", "NET_ADMIN", "--cap-add", "SYS_ADMIN", "--cap-add", "SYS_RESOURCE",
		"--device", "/dev/net/tun:/dev/net/tun",
		"--log-opt", "max-size=10m",
		"-e", "NB_HOSTNAME=" + name, "-e", "NB_LOG_LEVEL=debug",
		"-e", "NB_MANAGEMENT_URL=" + o.ManagementURL,
	}
	if o.SetupKey != "" {
		args = append(args, "-e", "NB_SETUP_KEY="+o.SetupKey)
	}
	if o.ForceRelay {
		args = append(args, "-e", "NB_FORCE_RELAY=true")
	}
	args = append(args, image)
	if _, err := Docker(ctx, args...); err != nil {
		return nil, err
	}
	return c, nil
}

// Remove deletes the container.
func (c *DynClient) Remove(ctx context.Context) {
	_, _ = Docker(ctx, "rm", "-f", c.Container)
}

// MoveManagement points a client at another management URL by re-running
// `netbird up` with it; the daemon reconnects to the new address.
func MoveManagement(ctx context.Context, container, url string) error {
	if _, err := Exec(ctx, container, "netbird", "down"); err != nil {
		return err
	}
	_, err := Exec(ctx, container, "netbird", "up", "--management-url", url)
	return err
}

// LogScanner follows a container's log from now on, for watching for a line.
type LogScanner struct {
	mu    sync.Mutex
	lines []string
	cmd   *exec.Cmd
}

// FollowLogs starts `docker logs -f` on a container.
func FollowLogs(ctx context.Context, container string) (*LogScanner, error) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", "--since", "0s", container)
	pr, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	ls := &LogScanner{cmd: cmd}
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			ls.mu.Lock()
			ls.lines = append(ls.lines, sc.Text())
			ls.mu.Unlock()
		}
	}()
	return ls, nil
}

// Find returns the first submatch of re in the lines seen so far.
func (l *LogScanner) Find(re *regexp.Regexp) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if m := re.FindStringSubmatch(line); m != nil {
			return m
		}
	}
	return nil
}

// Stop ends the log follower.
func (l *LogScanner) Stop() {
	if l.cmd.Process != nil {
		_ = l.cmd.Process.Kill()
	}
	_ = l.cmd.Wait()
}

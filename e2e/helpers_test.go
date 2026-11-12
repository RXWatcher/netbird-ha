package e2e

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/RXWatcher/netbird-ha/e2e/bed"
)

// propagationBudget is the cross-instance propagation target from the design report.
const propagationBudget = 2 * time.Second

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// instanceServices lists the management and signal services the bed runs.
func instanceServices(b *bed.Bed) (mgmt, signal []string) {
	mgmt, signal = []string{"mgmt-1"}, []string{"signal-1"}
	if b.Instances == 2 {
		mgmt, signal = append(mgmt, "mgmt-2"), append(signal, "signal-2")
	}
	return mgmt, signal
}

// healthy brings the bed back to its resting state before a scenario: every
// instance running and UP in HAProxy, every compose client connected to its
// home instance.
func healthy(s *S) {
	s.Helper()
	ctx := s.Ctx
	mgmt, signal := instanceServices(s.B)
	for _, svc := range append(append([]string{}, mgmt...), signal...) {
		if !bed.Running(ctx, svc) {
			s.Logf("restarting %s left down by an earlier scenario", svc)
			if err := bed.Start(ctx, svc); err != nil {
				s.T.Fatalf("start %s: %v", svc, err)
			}
		}
	}
	for _, svc := range mgmt {
		n := strings.TrimPrefix(svc, "mgmt-")
		if err := s.B.WaitServerUp(ctx, "be_mgmt"+n, svc, 90*time.Second); err != nil {
			s.T.Fatalf("%v", err)
		}
	}
	for _, svc := range signal {
		n := strings.TrimPrefix(svc, "signal-")
		if err := s.B.WaitServerUp(ctx, "be_signal"+n, svc, 60*time.Second); err != nil {
			s.T.Fatalf("%v", err)
		}
	}
	pgHealthy(s)
	rebalance(s)
	if _, err := bed.Eventually(ctx, 60*time.Second, time.Second, func() error {
		for _, c := range bed.Clients {
			st, err := bed.ClientStatus(ctx, bed.Container(c))
			if err != nil {
				return err
			}
			if !st.Management.Connected || !st.Signal.Connected {
				return fmt.Errorf("%s: management=%v signal=%v", c, st.Management.Connected, st.Signal.Connected)
			}
		}
		return nil
	}); err != nil {
		s.T.Fatalf("clients not connected: %v", err)
	}
	if s.B.Instances == 2 {
		if _, err := bed.Eventually(ctx, 30*time.Second, time.Second, func() error {
			return checkHomePlacement(ctx, s.B)
		}); err != nil {
			s.T.Fatalf("clients not on their home instances: %v", err)
		}
	}
}

// rebalance cuts the sessions that pinned frontends sent to their backup
// server, so those clients reconnect to their home instance.
func rebalance(s *S) {
	if s.B.Instances < 2 {
		return
	}
	for _, cmd := range []string{
		"shutdown sessions server be_mgmt1/mgmt-2", "shutdown sessions server be_mgmt2/mgmt-1",
		"shutdown sessions server be_signal1/signal-2", "shutdown sessions server be_signal2/signal-1",
	} {
		if _, err := s.B.HAProxy(s.Ctx, cmd); err != nil {
			s.Logf("haproxy %q: %v", cmd, err)
		}
	}
}

func checkHomePlacement(ctx context.Context, b *bed.Bed) error {
	pl, err := b.Placement(ctx)
	if err != nil {
		return err
	}
	for _, c := range bed.Clients {
		want := fmt.Sprintf("mgmt-%d", bed.HomeInstance[c])
		if got := pl[c]["mgmt"]; got != want {
			return fmt.Errorf("%s is on %q, want %s", c, got, want)
		}
	}
	return nil
}

// peerIPs maps peer names to overlay IPs from the API.
func peerIPs(s *S) map[string]string {
	s.Helper()
	ps, err := s.B.API.Peers(s.Ctx)
	if err != nil {
		s.Fatalf("list peers: %v", err)
	}
	out := map[string]string{}
	for _, p := range ps {
		out[p.Name] = p.IP
	}
	return out
}

func peerIDs(s *S, names ...string) []string {
	s.Helper()
	ps, err := s.B.API.Peers(s.Ctx)
	if err != nil {
		s.Fatalf("list peers: %v", err)
	}
	byName := map[string]string{}
	for _, p := range ps {
		byName[p.Name] = p.ID
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := byName[n]
		if !ok {
			s.Fatalf("peer %s not registered", n)
		}
		out = append(out, id)
	}
	return out
}

// sees reports whether observer's network map currently contains peer.
func sees(ctx context.Context, observer, peer string) (bool, error) {
	st, err := bed.ClientStatus(ctx, bed.Container(observer))
	if err != nil {
		return false, err
	}
	_, ok := st.PeerNames()[peer]
	return ok, nil
}

// waitSees polls observer's status every 100 ms until its view of peer equals
// want, and returns how long that took.
func waitSees(ctx context.Context, observer, peer string, want bool, timeout time.Duration) (time.Duration, error) {
	return bed.Eventually(ctx, timeout, 100*time.Millisecond, func() error {
		ok, err := sees(ctx, observer, peer)
		if err != nil {
			return err
		}
		if ok != want {
			return fmt.Errorf("%s sees %s: %v, want %v", observer, peer, ok, want)
		}
		return nil
	})
}

// waitSerial waits until a client has logged a network map with a serial of
// at least min since t (time is measured from the call).
func waitSerial(ctx context.Context, client string, min uint64, since time.Time, timeout time.Duration) (time.Duration, error) {
	return bed.Eventually(ctx, timeout, 100*time.Millisecond, func() error {
		got, err := bed.LastSerial(ctx, bed.Container(client), since)
		if err != nil {
			return err
		}
		if got < min {
			return fmt.Errorf("%s at serial %d, want >= %d", client, got, min)
		}
		return nil
	})
}

// pairPolicy is the fixture for propagation scenarios: Default policy off,
// groups A={c1,c2} and B={c3,c4}, and policy "A-B" between them.
type pairPolicy struct {
	defaultPolicy  bed.Policy
	groupA, groupB bed.Group
	policy         bed.Policy
}

func setupPairPolicy(s *S, enabled bool) *pairPolicy {
	s.Helper()
	ctx, api := s.Ctx, s.B.API
	pf := &pairPolicy{}
	pols, err := api.Policies(ctx)
	if err != nil {
		s.Fatalf("policies: %v", err)
	}
	for _, p := range pols {
		if p.Name == "Default" {
			pf.defaultPolicy = p
		}
		if strings.HasPrefix(p.Name, "e2e-") {
			_ = api.DeletePolicy(ctx, p.ID)
		}
	}
	if pf.defaultPolicy.ID == "" {
		s.Fatalf("Default policy not found")
	}
	if gs, err := api.Groups(ctx); err == nil {
		for _, g := range gs {
			if strings.HasPrefix(g.Name, "e2e-") {
				_ = api.DeleteGroup(ctx, g.ID)
			}
		}
	}
	if pf.groupA, err = api.CreateGroup(ctx, "e2e-A", peerIDs(s, "c1", "c2")); err != nil {
		s.Fatalf("create group A: %v", err)
	}
	if pf.groupB, err = api.CreateGroup(ctx, "e2e-B", peerIDs(s, "c3", "c4")); err != nil {
		s.Fatalf("create group B: %v", err)
	}
	if pf.policy, err = api.CreatePolicy(ctx, "e2e-A-B", enabled, []string{pf.groupA.ID}, []string{pf.groupB.ID}); err != nil {
		s.Fatalf("create policy: %v", err)
	}
	if err := api.SetPolicyEnabled(ctx, pf.defaultPolicy, false); err != nil {
		s.Fatalf("disable Default: %v", err)
	}
	s.Cleanup(func() { pf.teardown(s) })
	// Settle: c3 sees c1 only when the pair policy is on.
	if _, err := waitSees(ctx, "c3", "c1", enabled, 60*time.Second); err != nil {
		s.Fatalf("fixture did not settle: %v", err)
	}
	if _, err := waitSees(ctx, "c3", "c5", false, 60*time.Second); err != nil {
		s.Fatalf("fixture did not settle: %v", err)
	}
	return pf
}

func (pf *pairPolicy) teardown(s *S) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	api := s.B.API
	_ = api.SetPolicyEnabled(ctx, pf.defaultPolicy, true)
	_ = api.DeletePolicy(ctx, pf.policy.ID)
	_ = api.DeleteGroup(ctx, pf.groupA.ID)
	_ = api.DeleteGroup(ctx, pf.groupB.ID)
	// Leave the bed fully meshed for the next scenario.
	_, _ = waitSees(ctx, "c3", "c5", true, 60*time.Second)
	_, _ = waitSees(ctx, "c5", "c3", true, 60*time.Second)
}

// expectedView returns, for each compose client, the set of peer names the API
// says it may reach.
func expectedView(ctx context.Context, b *bed.Bed) (map[string][]string, error) {
	ps, err := b.API.Peers(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, p := range ps {
		if !contains(bed.Clients, p.Name) {
			continue
		}
		acc, err := b.API.AccessiblePeers(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		names := []string{}
		for _, a := range acc {
			label, _, _ := strings.Cut(a.DNSLabel, ".")
			names = append(names, label)
		}
		sort.Strings(names)
		out[p.Name] = names
	}
	return out, nil
}

// converged checks that every compose client's network map holds exactly the
// peers the API says it may reach.
func converged(ctx context.Context, b *bed.Bed) error {
	want, err := expectedView(ctx, b)
	if err != nil {
		return err
	}
	var bad []string
	for _, c := range bed.Clients {
		st, err := bed.ClientStatus(ctx, bed.Container(c))
		if err != nil {
			return err
		}
		got := []string{}
		for n := range st.PeerNames() {
			got = append(got, n)
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want[c], ",") {
			bad = append(bad, fmt.Sprintf("%s has [%s] want [%s]", c, strings.Join(got, ","), strings.Join(want[c], ",")))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%s", strings.Join(bad, "; "))
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// decryptErrors counts management decrypt failures in a client's log since t.
func decryptErrors(ctx context.Context, container string, since time.Time) int {
	out, _ := bed.Logs(ctx, container, since)
	n := 0
	for _, line := range strings.Split(out, "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, "decrypt") && (strings.Contains(l, "erro") || strings.Contains(l, "fail") || strings.Contains(l, "warn")) {
			n++
		}
	}
	return n
}

// waitManagementConnected waits until a client reports its management stream connected.
func waitManagementConnected(ctx context.Context, client string, timeout time.Duration) (time.Duration, error) {
	return bed.Eventually(ctx, timeout, 250*time.Millisecond, func() error {
		st, err := bed.ClientStatus(ctx, bed.Container(client))
		if err != nil {
			return err
		}
		if !st.Management.Connected {
			return fmt.Errorf("%s management not connected: %s", client, st.Management.Error)
		}
		return nil
	})
}

// waitReachable pings until a reply comes back.
func waitReachable(ctx context.Context, from, ip string, timeout time.Duration) (time.Duration, error) {
	return bed.Eventually(ctx, timeout, 200*time.Millisecond, func() error {
		if bed.Ping(ctx, bed.Container(from), ip, 1, 200*time.Millisecond).Received == 0 {
			return fmt.Errorf("%s cannot reach %s", from, ip)
		}
		return nil
	})
}

// dynName returns a peer name that no earlier run used, so API lookups by
// name cannot pick up a leftover peer.
func dynName(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano()/1e6%1000000)
}

// pgHealthy restarts stopped Postgres nodes and waits for one primary and one
// secondary.
func pgHealthy(s *S) {
	s.Helper()
	ctx := s.Ctx
	for _, n := range []string{"pg-1", "pg-2"} {
		if !bed.Running(ctx, n) {
			s.Logf("restarting %s left down by an earlier scenario", n)
			if err := bed.Start(ctx, n); err != nil {
				s.T.Fatalf("start %s: %v", n, err)
			}
		}
	}
	if _, err := bed.Eventually(ctx, 180*time.Second, 2*time.Second, func() error {
		st, err := bed.PGStates(ctx)
		if err != nil {
			return err
		}
		if st != "primary/primary secondary/secondary" {
			return fmt.Errorf("pg_auto_failover states: %s", st)
		}
		return nil
	}); err != nil {
		s.T.Fatalf("Postgres not healthy: %v", err)
	}
}

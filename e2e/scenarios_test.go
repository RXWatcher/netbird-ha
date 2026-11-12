package e2e

// Scenarios from docs/design-report.md section 4.6. Numbering follows the report.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RXWatcher/netbird-ha/e2e/bed"
)

// connectivity pings from a to b until it works and checks the connection type.
func connectivity(s *S, a, b, wantType string) {
	s.Helper()
	ips := peerIPs(s)
	took, err := waitReachable(s.Ctx, a, ips[b], 45*time.Second)
	if err != nil {
		s.Errorf("%s -> %s: %v", a, b, err)
		return
	}
	var got string
	if _, err := bed.Eventually(s.Ctx, 20*time.Second, 500*time.Millisecond, func() error {
		st, err := bed.ClientStatus(s.Ctx, bed.Container(a))
		if err != nil {
			return err
		}
		got = st.PeerNames()[b].ConnectionType
		if got != wantType {
			return fmt.Errorf("connection type %q", got)
		}
		return nil
	}); err != nil {
		s.Errorf("%s -> %s reachable but connection type is %q, want %s", a, b, got, wantType)
		return
	}
	s.Logf("%s -> %s: reachable after %s, %s", a, b, took.Round(time.Millisecond), got)
}

// 1. Peers on the same instance connect directly and through relays.
func TestS1aSameInstanceConnectivity(t *testing.T) {
	run(t, spec{id: "1a", title: "same-instance peers connect (P2P and relayed)"}, func(s *S) {
		connectivity(s, "c1", "c2", "P2P")
		connectivity(s, "c3", "c4", "P2P")
		connectivity(s, "c5", "c2", "Relayed")
	})
}

// 1. Peers pinned to different instances connect directly and through relays.
func TestS1bCrossInstanceConnectivity(t *testing.T) {
	run(t, spec{id: "1b", title: "cross-instance peers connect (P2P and relayed)", needsTwo: true, haIfTwo: true}, func(s *S) {
		connectivity(s, "c1", "c3", "P2P")
		connectivity(s, "c4", "c2", "P2P")
		connectivity(s, "c5", "c6", "Relayed")
		connectivity(s, "c1", "c6", "Relayed")
	})
}

// 2a. A policy change made on mgmt-1 reaches a peer on mgmt-2 in under 2 s.
func TestS2aPolicyPropagation(t *testing.T) {
	run(t, spec{id: "2a", title: "policy change on mgmt-1 reaches mgmt-2 peers < 2s", haIfTwo: true}, func(s *S) {
		pf := setupPairPolicy(s, false)
		ctx, api := s.Ctx, s.B.APIMgmt1
		for _, enable := range []bool{true, false} {
			t0 := time.Now()
			if err := api.SetPolicyEnabled(ctx, pf.policy, enable); err != nil {
				s.Fatalf("toggle policy on mgmt-1: %v", err)
			}
			serial, err := bed.AccountSerial(ctx)
			if err != nil {
				s.Fatalf("account serial: %v", err)
			}
			took, err := waitSees(ctx, "c3", "c1", enable, 15*time.Second)
			if err != nil {
				s.Fatalf("policy enabled=%v: %v", enable, err)
			}
			if took > propagationBudget {
				s.Errorf("policy enabled=%v took %s to reach c3 (budget %s)", enable, took, propagationBudget)
			}
			got, _ := bed.LastSerial(ctx, bed.Container("c3"), t0.Add(-time.Second))
			if got < serial {
				s.Errorf("c3 applied serial %d, account serial is %d", got, serial)
			}
			s.Logf("policy enabled=%v: c3 updated after %s (serial %d)", enable, took.Round(time.Millisecond), got)
			if enable {
				if _, err := waitReachable(ctx, "c3", peerIPs(s)["c1"], 20*time.Second); err != nil {
					s.Errorf("c3 -> c1 after enabling: %v", err)
				}
			} else if bed.Reachable(ctx, bed.Container("c3"), peerIPs(s)["c1"]) {
				s.Errorf("c3 still reaches c1 after the policy was disabled")
			}
		}
	})
}

// 2b/2c. A peer added on mgmt-1 appears on mgmt-2 peers in under 2 s; deleting it removes it.
func TestS2bPeerAddDelete(t *testing.T) {
	run(t, spec{id: "2b", title: "peer add/delete on mgmt-1 reaches mgmt-2 peers < 2s", haIfTwo: true}, func(s *S) {
		ctx := s.Ctx
		key, err := s.B.API.CreateSetupKey(ctx, "e2e-add", false, nil)
		if err != nil {
			s.Fatalf("setup key: %v", err)
		}
		name := dynName("cadd")
		dc, err := bed.StartDynClient(ctx, name, bed.DynClientOptions{SetupKey: key.Key, ManagementURL: "http://lb:8081"})
		if err != nil {
			s.Fatalf("start client: %v", err)
		}
		s.Cleanup(func() { dc.Remove(ctx) })

		// Watch the API (registration) and c3's map (propagation) together.
		var tAPI, tSeen time.Time
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) && (tAPI.IsZero() || tSeen.IsZero()) {
			now := time.Now()
			if tAPI.IsZero() {
				if _, err := s.B.APIMgmt1.PeerByName(ctx, name); err == nil {
					tAPI = now
				}
			}
			if tSeen.IsZero() {
				if ok, _ := sees(ctx, "c3", name); ok {
					tSeen = now
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if tAPI.IsZero() {
			s.Fatalf("cadd never registered")
		}
		if tSeen.IsZero() {
			s.Fatalf("c3 never saw cadd (registered %s ago)", time.Since(tAPI).Round(time.Second))
		}
		add := tSeen.Sub(tAPI)
		if add < 0 {
			add = 0
		}
		s.Logf("peer add: c3 saw cadd %s after registration", add.Round(time.Millisecond))
		if add > propagationBudget+200*time.Millisecond {
			s.Errorf("peer add took %s to reach c3", add)
		}
		if _, err := waitReachable(ctx, "c3", peerIPs(s)[name], 30*time.Second); err != nil {
			s.Errorf("c3 -> cadd: %v", err)
		}

		p, err := s.B.APIMgmt1.PeerByName(ctx, name)
		if err != nil {
			s.Fatalf("%v", err)
		}
		dc.Remove(ctx)
		if err := s.B.APIMgmt1.DeletePeer(ctx, p.ID); err != nil {
			s.Fatalf("delete peer on mgmt-1: %v", err)
		}
		took, err := waitSees(ctx, "c3", name, false, 15*time.Second)
		if err != nil {
			s.Fatalf("peer delete: %v", err)
		}
		s.Logf("peer delete: c3 dropped cadd after %s", took.Round(time.Millisecond))
		if took > propagationBudget {
			s.Errorf("peer delete took %s to reach c3", took)
		}
	})
}

// 2d. Blocking a user on mgmt-1 disconnects that user's peer on mgmt-2 and
// removes it from other peers' maps in under 2 s.
func TestS2dUserBlock(t *testing.T) {
	run(t, spec{id: "2d", title: "user block on mgmt-1 reaches mgmt-2 peers < 2s", haIfTwo: true}, func(s *S) {
		ctx := s.Ctx
		email := fmt.Sprintf("e2e-%d@nbha.test", time.Now().Unix())
		u, err := s.B.API.CreateUser(ctx, email, "E2E User")
		if err != nil {
			s.Fatalf("create user: %v", err)
		}
		s.Cleanup(func() { _ = s.B.API.DeleteUser(ctx, u.ID) })
		if u.Password == "" {
			s.Fatalf("user created without a password (embedded IdP expected)")
		}
		// The user's peer lives on mgmt-2; the block is made on mgmt-1.
		name := dynName("cuser")
		dc, err := ssoClient(s, name, "http://lb:8082", email, u.Password)
		if err != nil {
			s.Fatalf("SSO peer: %v", err)
		}
		s.Cleanup(func() {
			dc.Remove(ctx)
			if p, err := s.B.API.PeerByName(ctx, name); err == nil {
				_ = s.B.API.DeletePeer(ctx, p.ID)
			}
		})
		if _, err := waitSees(ctx, "c1", name, true, 60*time.Second); err != nil {
			s.Fatalf("c1 never saw the user's peer: %v", err)
		}
		t0 := time.Now()
		if err := s.B.APIMgmt1.SetUserBlocked(ctx, u, true); err != nil {
			s.Fatalf("block user on mgmt-1: %v", err)
		}
		took, err := bed.Eventually(ctx, 15*time.Second, 100*time.Millisecond, func() error {
			st, err := bed.ClientStatus(ctx, dc.Container)
			if err != nil {
				return err
			}
			if st.Management.Connected && st.DaemonStatus == "Connected" {
				return fmt.Errorf("user's peer still connected to management")
			}
			return nil
		})
		if err != nil {
			s.Fatalf("blocked user's peer (on mgmt-2) was not disconnected: %v", err)
		}
		s.Logf("user block: user's peer (home mgmt-2) disconnected %s after the call (%s total)", took.Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
		if took > propagationBudget {
			s.Errorf("user block took %s", took)
		}
		// Upstream expires the blocked user's peers: they move to the other
		// peers' offline list, which a new network-map serial carries.
		serial, err := bed.AccountSerial(ctx)
		if err != nil {
			s.Fatalf("account serial: %v", err)
		}
		for _, obs := range []string{"c1", "c3"} {
			took, err := waitSerial(ctx, obs, serial, t0.Add(-time.Second), 15*time.Second)
			if err != nil {
				s.Errorf("%s did not get the post-block network map: %v", obs, err)
			} else if took > propagationBudget {
				s.Errorf("%s got the post-block network map after %s", obs, took)
			} else {
				s.Logf("%s applied serial >= %d %s after the block", obs, serial, took.Round(time.Millisecond))
			}
		}
	})
}

// 3a. kill -9 of an instance during continuous ping: tunnels keep working,
// clients reconnect in under 30 s, no decrypt errors.
func TestS3aKillDuringPing(t *testing.T) {
	run(t, spec{id: "3a", title: "kill -9 mgmt/signal during continuous ping", haIfTwo: true}, func(s *S) {
		for _, victim := range []string{"mgmt-1", "signal-1"} {
			killDuringPing(s, victim)
			healthy(s)
		}
	})
}

func killDuringPing(s *S, victim string) {
	ctx := s.Ctx
	ips := peerIPs(s)
	pairs := [][2]string{{"c1", "c2"}, {"c1", "c3"}, {"c5", "c6"}}
	for _, p := range pairs { // establish lazy connections before measuring
		if _, err := waitReachable(ctx, p[0], ips[p[1]], 45*time.Second); err != nil {
			s.Fatalf("before kill: %s -> %s: %v", p[0], p[1], err)
		}
	}
	const dur = 40 * time.Second
	results := make([]bed.PingResult, len(pairs))
	var wg sync.WaitGroup
	for i, p := range pairs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = bed.PingFor(ctx, bed.Container(p[0]), ips[p[1]], dur, 200*time.Millisecond)
		}()
	}
	time.Sleep(5 * time.Second)
	tKill := time.Now()
	if err := bed.Kill(ctx, victim); err != nil {
		s.Fatalf("kill %s: %v", victim, err)
	}
	s.Logf("killed %s", victim)
	if !s.Two() {
		time.Sleep(3 * time.Second)
		if err := bed.Start(ctx, victim); err != nil {
			s.Fatalf("restart %s: %v", victim, err)
		}
	}
	// Clients homed on the victim reconnect (to the other instance, or to the restarted one).
	for _, c := range []string{"c1", "c2", "c5"} {
		took, err := bed.Eventually(ctx, 45*time.Second, 250*time.Millisecond, func() error {
			st, err := bed.ClientStatus(ctx, bed.Container(c))
			if err != nil {
				return err
			}
			if strings.HasPrefix(victim, "mgmt") && !st.Management.Connected {
				return fmt.Errorf("management not connected")
			}
			if strings.HasPrefix(victim, "signal") && !st.Signal.Connected {
				return fmt.Errorf("signal not connected")
			}
			return nil
		})
		if err != nil {
			s.Errorf("%s did not reconnect after kill of %s: %v", c, victim, err)
			continue
		}
		// Status can still read connected before the client notices the drop;
		// the decisive check is the next one, after the pings.
		s.Logf("%s connected %s after the kill", c, took.Round(time.Millisecond))
	}
	wg.Wait()
	for i, p := range pairs {
		r := results[i]
		s.Logf("%s -> %s during kill of %s: %d/%d replies, longest gap %s", p[0], p[1], victim, r.Received, r.Sent, r.MaxGap)
		if r.Sent == 0 || r.Loss() > 0.05 || r.MaxGap > 3*time.Second {
			s.Errorf("%s -> %s lost traffic during kill of %s: %d/%d, gap %s", p[0], p[1], victim, r.Received, r.Sent, r.MaxGap)
		}
	}
	reconnectBudget := 30 * time.Second
	for _, c := range []string{"c1", "c2", "c5"} {
		took, err := waitManagementConnected(ctx, c, reconnectBudget)
		if err != nil {
			s.Errorf("%s: %v", c, err)
		} else {
			s.Logf("%s management connected (checked %s after the kill)", c, (time.Since(tKill) - took).Round(time.Second))
		}
		if n := decryptErrors(ctx, bed.Container(c), tKill); n > 0 {
			s.Errorf("%s logged %d decrypt errors after the kill of %s", c, n, victim)
		}
	}
	// A change after the kill still reaches everyone.
	if _, err := bed.Eventually(ctx, 30*time.Second, time.Second, func() error { return converged(ctx, s.B) }); err != nil {
		s.Errorf("not converged after kill of %s: %v", victim, err)
	}
	if s.Two() {
		if err := bed.Start(ctx, victim); err != nil {
			s.Fatalf("restart %s: %v", victim, err)
		}
	}
}

// 3b. kill -9 of an instance during connection setup: the new connection is
// established anyway within 30 s.
func TestS3bKillDuringSetup(t *testing.T) {
	run(t, spec{id: "3b", title: "kill -9 mgmt/signal during connection setup", haIfTwo: true}, func(s *S) {
		ctx := s.Ctx
		for _, victim := range []string{"signal-1", "mgmt-1"} {
			ips := peerIPs(s)
			// c2 re-logs in: every peer connection is set up again.
			if _, err := bed.Exec(ctx, bed.Container("c2"), "netbird", "down"); err != nil {
				s.Fatalf("netbird down: %v", err)
			}
			if _, err := bed.Exec(ctx, bed.Container("c2"), "netbird", "up"); err != nil {
				s.Fatalf("netbird up: %v", err)
			}
			go bed.Ping(ctx, bed.Container("c2"), ips["c4"], 1, 100*time.Millisecond) // trigger the lazy connection
			time.Sleep(150 * time.Millisecond)
			tKill := time.Now()
			if err := bed.Kill(ctx, victim); err != nil {
				s.Fatalf("kill %s: %v", victim, err)
			}
			if !s.Two() {
				time.Sleep(3 * time.Second)
				if err := bed.Start(ctx, victim); err != nil {
					s.Fatalf("restart %s: %v", victim, err)
				}
			}
			took, err := waitReachable(ctx, "c2", ips["c4"], 45*time.Second)
			if err != nil {
				s.Errorf("c2 -> c4 after kill of %s during setup: %v", victim, err)
			} else {
				s.Logf("c2 -> c4 established %s after kill of %s", (time.Since(tKill)).Round(time.Millisecond), victim)
				if took > 30*time.Second {
					s.Errorf("connection took %s after kill of %s", took, victim)
				}
			}
			if n := decryptErrors(ctx, bed.Container("c2"), tKill); n > 0 {
				s.Errorf("c2 logged %d decrypt errors", n)
			}
			healthy(s)
		}
	})
}

// 4. Sentinel failover during policy churn: state converges within the resync interval.
func TestS4SentinelFailoverDuringChurn(t *testing.T) {
	run(t, spec{id: "4", title: "Sentinel failover during policy churn converges", haIfTwo: true}, func(s *S) {
		ctx := s.Ctx
		pf := setupPairPolicy(s, false)
		oldMaster, err := bed.ValkeyMaster(ctx)
		if err != nil {
			s.Fatalf("sentinel: %v", err)
		}
		apis := []*bed.API{s.B.APIMgmt1}
		if s.Two() {
			apis = append(apis, s.B.APIMgmt2)
		}
		var errs []string
		stop := time.Now().Add(20 * time.Second)
		failoverAt := time.Now().Add(5 * time.Second)
		failedOver := false
		enabled := false
		for i := 0; time.Now().Before(stop); i++ {
			if !failedOver && time.Now().After(failoverAt) {
				out, err := bed.Sentinel(ctx, "sentinel", "failover", "nbha")
				if err != nil || !strings.Contains(out, "OK") {
					s.Fatalf("sentinel failover: %v %s", err, out)
				}
				failedOver = true
				s.Logf("sentinel failover started (master was %s)", oldMaster)
			}
			enabled = !enabled
			if err := apis[i%len(apis)].SetPolicyEnabled(ctx, pf.policy, enabled); err != nil {
				errs = append(errs, err.Error())
				enabled = !enabled
			}
			time.Sleep(700 * time.Millisecond)
		}
		if _, err := bed.Eventually(ctx, 30*time.Second, time.Second, func() error {
			m, err := bed.ValkeyMaster(ctx)
			if err != nil || m == oldMaster {
				return fmt.Errorf("master still %s (%v)", m, err)
			}
			return nil
		}); err != nil {
			s.Errorf("no new Valkey master: %v", err)
		}
		if newMaster, _ := bed.ValkeyMaster(ctx); newMaster != "" {
			if err := s.B.WaitServerUp(ctx, "be_valkey", newMaster, 30*time.Second); err != nil {
				s.Errorf("HAProxy does not track the new master: %v", err)
			}
		}
		s.Logf("%d API errors during churn: %v", len(errs), errs)
		if err := s.B.API.Do(ctx, "GET", "/users/current", nil, nil); err != nil {
			s.Errorf("API after failover: %v", err)
		}
		resync := envDuration("NBHA_RESYNC_TIMEOUT", 60*time.Second)
		took, err := bed.Eventually(ctx, resync, time.Second, func() error { return converged(ctx, s.B) })
		if err != nil {
			s.Errorf("not converged within %s after churn (final enabled=%v): %v", resync, enabled, err)
		} else {
			s.Logf("converged %s after churn (final enabled=%v)", took.Round(time.Millisecond), enabled)
		}
	})
}

// 5a. pg_auto_failover switchover: instances keep serving through the
// multi-host DSN with target_session_attrs=read-write, without a restart.
func TestS5aPGSwitchover(t *testing.T) {
	run(t, spec{id: "5a", title: "pg_auto_failover switchover, instances recover", haIfTwo: true}, func(s *S) {
		pgRoleChange(s, func() error {
			_, err := bed.PGAutoctl(s.Ctx, "perform", "switchover")
			return err
		})
	})
}

// 5b. Hard failover: kill -9 of the primary's container.
func TestS5bPGFailover(t *testing.T) {
	run(t, spec{id: "5b", title: "pg_auto_failover failover (kill -9 primary)", haIfTwo: true}, func(s *S) {
		// pg_auto_failover notices the dead primary after its health checks
		// fail (about 20-30 s) and promotes the secondary.
		pgRoleChange(s, func() error {
			p, err := bed.PGPrimary(s.Ctx)
			if err != nil {
				return err
			}
			s.Cleanup(func() { _ = bed.Start(context.Background(), p) })
			return bed.Kill(s.Ctx, p)
		})
		// The old primary rejoins as a secondary (checked by the next scenario's
		// health check too).
		pgHealthy(s)
	})
}

func pgRoleChange(s *S, change func() error) {
	ctx := s.Ctx
	mgmt, _ := instanceServices(s.B)
	started := map[string]string{}
	for _, m := range mgmt {
		started[m] = bed.StartedAt(ctx, m)
	}
	before, err := bed.PGPrimary(ctx)
	if err != nil {
		s.Fatalf("primary: %v", err)
	}
	t0 := time.Now()
	if err := change(); err != nil {
		s.Fatalf("role change: %v", err)
	}
	after := ""
	if _, err := bed.Eventually(ctx, 120*time.Second, time.Second, func() error {
		p, err := bed.PGPrimary(ctx)
		if err != nil {
			return err
		}
		if p == before {
			return fmt.Errorf("primary still %s", p)
		}
		after = p
		return nil
	}); err != nil {
		s.Fatalf("no new primary: %v", err)
	}
	s.Logf("primary %s -> %s after %s", before, after, time.Since(t0).Round(time.Second))
	apis := map[string]*bed.API{"mgmt-1": s.B.APIMgmt1}
	if s.Two() {
		apis["mgmt-2"] = s.B.APIMgmt2
	}
	for name, api := range apis {
		took, err := bed.Eventually(ctx, 60*time.Second, time.Second, func() error {
			g, err := api.CreateGroup(ctx, fmt.Sprintf("e2e-pg-%s-%d", name, time.Now().UnixNano()), nil)
			if err != nil {
				return err
			}
			return api.DeleteGroup(ctx, g.ID)
		})
		if err != nil {
			s.Errorf("%s cannot write after the role change: %v", name, err)
		} else {
			s.Logf("%s writes again %s after the new primary", name, took.Round(time.Millisecond))
		}
	}
	// Dex reads its signing keys from Postgres (through HAProxy's primary frontend).
	if _, err := bed.Eventually(ctx, 60*time.Second, time.Second, func() error {
		return s.B.API.GetRaw(ctx, "/oauth2/keys")
	}); err != nil {
		s.Errorf("embedded Dex after the role change: %v", err)
	}
	for _, m := range mgmt {
		if got := bed.StartedAt(ctx, m); got != started[m] {
			s.Errorf("%s restarted during the role change", m)
		}
	}
	if _, err := bed.Eventually(ctx, 30*time.Second, time.Second, func() error { return converged(ctx, s.B) }); err != nil {
		s.Errorf("not converged after the role change: %v", err)
	}
}

// 6. The first owner set up on both instances at once yields exactly one account.
// deploy/bed.sh races /api/setup on mgmt-1 and mgmt-2 when it bootstraps a two-instance bed.
func TestS6ConcurrentFirstLogin(t *testing.T) {
	run(t, spec{id: "6", title: "concurrent first setup on both instances -> one account", needsTwo: true, haIfTwo: true}, func(s *S) {
		race, err := os.ReadFile(s.B.Root + "/deploy/.state/setup-race.json")
		if err != nil {
			s.Fatalf("setup race record missing (bed bootstrapped before the race existed?): %v", err)
		}
		s.Logf("setup race: %s", strings.TrimSpace(string(race)))
		ok := strings.Count(string(race), `"code":200`) + strings.Count(string(race), `"code": 200`)
		accounts, err := bed.SQLInt(s.Ctx, "netbird", "select count(*) from accounts")
		if err != nil {
			s.Fatalf("count accounts: %v", err)
		}
		owners, err := bed.SQLInt(s.Ctx, "netbird", "select count(*) from users where role = 'owner'")
		if err != nil {
			s.Fatalf("count owners: %v", err)
		}
		s.Logf("successful setups: %d, accounts: %d, owners: %d", ok, accounts, owners)
		if accounts != 1 {
			s.Errorf("%d accounts after the concurrent setup, want 1", accounts)
		}
		if ok != 1 {
			s.Errorf("%d setup calls succeeded, want exactly 1", ok)
		}
		if owners != 1 {
			s.Errorf("%d owner users, want 1", owners)
		}
	})
}

// 7. An ephemeral peer that moves from mgmt-1 to mgmt-2 is not deleted.
func TestS7EphemeralMove(t *testing.T) {
	run(t, spec{id: "7", title: "ephemeral peer moving mgmt-1 -> mgmt-2 is kept", needsTwo: true, haIfTwo: true, long: true}, func(s *S) {
		ctx := s.Ctx
		key, err := s.B.API.CreateSetupKey(ctx, "e2e-ephemeral", true, nil)
		if err != nil {
			s.Fatalf("setup key: %v", err)
		}
		name := dynName("ceph")
		dc, err := bed.StartDynClient(ctx, name, bed.DynClientOptions{SetupKey: key.Key, ManagementURL: "http://lb:8081"})
		if err != nil {
			s.Fatalf("start: %v", err)
		}
		s.Cleanup(func() {
			dc.Remove(ctx)
			if p, err := s.B.API.PeerByName(ctx, name); err == nil {
				_ = s.B.API.DeletePeer(ctx, p.ID)
			}
		})
		var label string
		if _, err := bed.Eventually(ctx, 90*time.Second, time.Second, func() error {
			p, err := s.B.API.PeerByName(ctx, name)
			if err != nil || !p.Connected || !p.Ephemeral {
				return fmt.Errorf("ceph not connected/ephemeral: %+v %v", p, err)
			}
			// Peers added with a setup key can get a DNS label with a suffix;
			// clients list peers by label.
			label, _, _ = strings.Cut(p.DNSLabel, ".")
			return nil
		}); err != nil {
			s.Fatalf("%v", err)
		}
		if _, err := waitSees(ctx, "c1", label, true, 30*time.Second); err != nil {
			s.Fatalf("c1 does not list %s before the move: %v", label, err)
		}
		if err := bed.MoveManagement(ctx, dc.Container, "http://lb:8082"); err != nil {
			s.Fatalf("move to mgmt-2: %v", err)
		}
		if _, err := bed.Eventually(ctx, 60*time.Second, time.Second, func() error {
			pl, err := s.B.Placement(ctx)
			if err != nil {
				return err
			}
			if pl[name]["mgmt"] != "mgmt-2" {
				return fmt.Errorf("ceph on %q", pl[name]["mgmt"])
			}
			return nil
		}); err != nil {
			s.Fatalf("ceph did not move: %v", err)
		}
		// Upstream deletes an ephemeral peer 10 min after its last disconnect
		// from that instance, checked every minute.
		wait := envDuration("NBHA_EPHEMERAL_WAIT", 12*time.Minute)
		s.Logf("ceph moved to mgmt-2; waiting %s for mgmt-1's ephemeral cleanup", wait)
		deadline := time.Now().Add(wait)
		for time.Now().Before(deadline) {
			if _, err := s.B.API.PeerByName(ctx, name); err != nil {
				s.Fatalf("ephemeral peer was deleted while connected to mgmt-2 (%s after the move)", wait-time.Until(deadline).Round(time.Second))
			}
			time.Sleep(15 * time.Second)
		}
		if ok, _ := sees(ctx, "c1", label); !ok {
			s.Errorf("c1 no longer lists %s", label)
		}
		if p, err := s.B.API.PeerByName(ctx, name); err != nil || !p.Connected {
			s.Errorf("ceph after the wait: connected=%v err=%v", p.Connected, err)
		}
	})
}

// 7b. An ephemeral peer whose instance dies with it (host loss) is still
// cleaned up. Upstream (v0.83.0+) reads the peer as disconnected once the dead
// instance's heartbeat is stale, but only a restart would schedule its
// deletion; ha/ephemeral's reconcile hands it to upstream's ephemeral manager.
func TestS7bEphemeralOfKilledInstance(t *testing.T) {
	run(t, spec{id: "7b", title: "ephemeral peer of a killed instance is cleaned up", haOnly: true, long: true}, func(s *S) {
		ctx := s.Ctx
		key, err := s.B.API.CreateSetupKey(ctx, "e2e-ephemeral-kill", true, nil)
		if err != nil {
			s.Fatalf("setup key: %v", err)
		}
		name := dynName("cekill")
		dc, err := bed.StartDynClient(ctx, name, bed.DynClientOptions{SetupKey: key.Key, ManagementURL: "http://lb:8081"})
		if err != nil {
			s.Fatalf("start: %v", err)
		}
		s.Cleanup(func() {
			dc.Remove(context.Background())
			if p, err := s.B.API.PeerByName(context.Background(), name); err == nil {
				_ = s.B.API.DeletePeer(context.Background(), p.ID)
			}
		})
		var peerID string
		if _, err := bed.Eventually(ctx, 90*time.Second, time.Second, func() error {
			p, err := s.B.API.PeerByName(ctx, name)
			if err != nil || !p.Connected || !p.Ephemeral {
				return fmt.Errorf("%s not connected/ephemeral: %+v %v", name, p, err)
			}
			peerID = p.ID
			if s.Two() {
				pl, err := s.B.Placement(ctx)
				if err != nil {
					return err
				}
				if pl[name]["mgmt"] != "mgmt-1" {
					return fmt.Errorf("%s on %q, want mgmt-1", name, pl[name]["mgmt"])
				}
			}
			return nil
		}); err != nil {
			s.Fatalf("%v", err)
		}
		// Freeze the client so it cannot reconnect elsewhere, kill its instance,
		// then remove the client: nobody ever sees the stream close.
		if _, err := bed.Docker(ctx, "pause", dc.Container); err != nil {
			s.Fatalf("pause client: %v", err)
		}
		tKill := time.Now()
		if err := bed.Kill(ctx, "mgmt-1"); err != nil {
			s.Fatalf("kill mgmt-1: %v", err)
		}
		dc.Remove(ctx)
		if s.Two() {
			// Host loss: mgmt-1 stays down, only mgmt-2 can clean up.
			s.Cleanup(func() { _ = bed.Start(context.Background(), "mgmt-1") })
		} else {
			time.Sleep(3 * time.Second)
			if err := bed.Start(ctx, "mgmt-1"); err != nil {
				s.Fatalf("restart mgmt-1: %v", err)
			}
		}
		s.Logf("killed mgmt-1 holding %s's stream and removed the client", name)
		// Upstream: the instance is stale 5 min after its last heartbeat (up to
		// 1 min before the kill); ha/ephemeral's reconcile (1 min + 20% jitter)
		// schedules the peer, then upstream's 10 min lifetime and 1 min cleanup
		// window.
		wait := envDuration("NBHA_EPHEMERAL_CLEANUP_WAIT", 20*time.Minute)
		var tDisc time.Duration
		took, err := bed.Eventually(ctx, wait, 10*time.Second, func() error {
			api := s.B.API
			p, err := api.PeerByName(ctx, name)
			var he *bed.HTTPError
			if err != nil && (errors.As(err, &he) && he.Status == 404 || strings.Contains(err.Error(), "not found")) {
				return nil
			}
			if err != nil {
				return err
			}
			if !p.Connected && tDisc == 0 {
				tDisc = time.Since(tKill)
				s.Logf("%s marked disconnected %s after the kill", name, tDisc.Round(time.Second))
			}
			return fmt.Errorf("%s (%s) still registered, connected=%v", name, peerID, p.Connected)
		})
		if err != nil {
			s.Fatalf("ephemeral peer of the killed instance not deleted: %v", err)
		}
		s.Logf("%s deleted %s after the kill (marked disconnected after %s)", name, took.Round(time.Second), tDisc.Round(time.Second))
	})
}

// 8. Rolling upgrade under load: tunnels keep working, clients reconnect, state converges.
func TestS8RollingUpgrade(t *testing.T) {
	run(t, spec{id: "8", title: "rolling upgrade under load", haIfTwo: true}, func(s *S) {
		ctx := s.Ctx
		ips := peerIPs(s)
		pairs := [][2]string{{"c1", "c3"}, {"c2", "c4"}, {"c5", "c6"}}
		for _, p := range pairs {
			if _, err := waitReachable(ctx, p[0], ips[p[1]], 45*time.Second); err != nil {
				s.Fatalf("before upgrade: %s -> %s: %v", p[0], p[1], err)
			}
		}
		mgmt, signal := instanceServices(s.B)
		steps := append(append([]string{}, mgmt...), signal...)
		dur := time.Duration(len(steps))*35*time.Second + 20*time.Second

		results := make([]bed.PingResult, len(pairs))
		var wg sync.WaitGroup
		for i, p := range pairs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = bed.PingFor(ctx, bed.Container(p[0]), ips[p[1]], dur, 250*time.Millisecond)
			}()
		}
		// Load: group churn changes the account serial (and every network map) once a second.
		churnStop := make(chan struct{})
		var churnErrs int
		var churnWG sync.WaitGroup
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			g, err := s.B.API.CreateGroup(ctx, "e2e-churn", nil)
			if err != nil {
				churnErrs++
				return
			}
			defer func() { _ = s.B.API.DeleteGroup(ctx, g.ID) }()
			ids := peerIDs(s, "c1")
			for i := 0; ; i++ {
				select {
				case <-churnStop:
					return
				case <-time.After(time.Second):
				}
				members := []string{}
				if i%2 == 0 {
					members = ids
				}
				if err := s.B.API.UpdateGroup(ctx, g.ID, "e2e-churn", members); err != nil {
					churnErrs++
				}
			}
		}()

		image := map[string]string{"mgmt": s.B.UpgradeMgmtImage, "signal": s.B.UpgradeSignalImage}
		if v := os.Getenv("NBHA_UPGRADE_MGMT_IMAGE"); v != "" {
			image["mgmt"] = v
		}
		if v := os.Getenv("NBHA_UPGRADE_SIGNAL_IMAGE"); v != "" {
			image["signal"] = v
		}
		s.Logf("upgrade images: mgmt=%q signal=%q (empty: recreate with the current image)", image["mgmt"], image["signal"])
		for _, svc := range steps {
			kind, n, _ := strings.Cut(svc, "-")
			be := "be_" + kind + n
			if _, err := s.B.HAProxy(ctx, fmt.Sprintf("set server %s/%s state drain", be, svc)); err != nil {
				s.Logf("drain %s: %v", svc, err)
			}
			if err := bed.Stop(ctx, svc); err != nil {
				s.Fatalf("stop %s: %v", svc, err)
			}
			args := []string{"up", "-d", "--no-deps", "--force-recreate", svc}
			if img := image[kind]; img != "" {
				s.Setenv("NBHA_"+strings.ToUpper(kind)+"_IMAGE", img)
			}
			if out, err := s.B.Compose(ctx, args...); err != nil {
				s.Fatalf("recreate %s: %v %s", svc, err, out)
			}
			if _, err := s.B.HAProxy(ctx, fmt.Sprintf("set server %s/%s state ready", be, svc)); err != nil {
				s.Logf("ready %s: %v", svc, err)
			}
			if err := s.B.WaitServerUp(ctx, be, svc, 90*time.Second); err != nil {
				s.Fatalf("%s did not come back: %v", svc, err)
			}
			s.Logf("upgraded %s", svc)
			time.Sleep(10 * time.Second)
		}
		close(churnStop)
		churnWG.Wait()
		wg.Wait()
		for i, p := range pairs {
			r := results[i]
			s.Logf("%s -> %s during the upgrade: %d/%d replies, longest gap %s", p[0], p[1], r.Received, r.Sent, r.MaxGap)
			if r.Sent == 0 || r.Loss() > 0.02 || r.MaxGap > 5*time.Second {
				s.Errorf("%s -> %s lost traffic: %d/%d, gap %s", p[0], p[1], r.Received, r.Sent, r.MaxGap)
			}
		}
		s.Logf("group churn errors during the upgrade: %d", churnErrs)
		for _, c := range bed.Clients {
			if _, err := waitManagementConnected(ctx, c, 30*time.Second); err != nil {
				s.Errorf("%v", err)
			}
		}
		if _, err := bed.Eventually(ctx, 30*time.Second, time.Second, func() error { return converged(ctx, s.B) }); err != nil {
			s.Errorf("not converged after the upgrade: %v", err)
		}
	})
}

// 9. Soak: churn policies and groups for NBHA_SOAK (e.g. 30m, 24h) and check
// that every client's network map matches the API.
func TestS9Soak(t *testing.T) {
	d := envDuration("NBHA_SOAK", 0)
	run(t, spec{id: "9", title: "soak: churn and compare network maps", haIfTwo: true, long: true}, func(s *S) {
		if d == 0 {
			s.Skip("set NBHA_SOAK to a duration (e.g. 30m or 24h)")
		}
		ctx := s.Ctx
		pf := setupPairPolicy(s, false)
		apis := []*bed.API{s.B.APIMgmt1}
		if s.Two() {
			apis = append(apis, s.B.APIMgmt2)
		}
		converge := envDuration("NBHA_SOAK_CONVERGE", 10*time.Second)
		end := time.Now().Add(d)
		enabled := false
		var checks, mismatches, apiErrs int
		for i := 0; time.Now().Before(end); i++ {
			enabled = !enabled
			if err := apis[i%len(apis)].SetPolicyEnabled(ctx, pf.policy, enabled); err != nil {
				apiErrs++
				enabled = !enabled
			}
			time.Sleep(2 * time.Second)
			if i%5 == 0 {
				checks++
				if _, err := bed.Eventually(ctx, converge, 500*time.Millisecond, func() error { return converged(ctx, s.B) }); err != nil {
					mismatches++
					s.Logf("mismatch %d at %s: %v", mismatches, time.Now().Format(time.TimeOnly), err)
				}
			}
			if i%30 == 0 && enabled {
				// The pair policy is on: group A reaches group B across instances.
				ips := peerIPs(s)
				for _, p := range [][2]string{{"c1", "c3"}, {"c4", "c2"}} {
					if _, err := waitReachable(ctx, p[0], ips[p[1]], converge); err != nil {
						mismatches++
						s.Logf("%s cannot reach %s: %v", p[0], p[1], err)
					}
				}
			}
		}
		s.Logf("soak %s: %d convergence checks, %d mismatches, %d API errors", d, checks, mismatches, apiErrs)
		if mismatches > 0 {
			s.Errorf("%d of %d convergence checks failed (budget %s)", mismatches, checks, converge)
		}
	})
}

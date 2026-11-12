package e2e

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RXWatcher/netbird-ha/e2e/bed"
)

// Scenario outcomes, printed as a table at the end of the run.
const (
	outPass  = "PASS"
	outFail  = "FAIL"
	outSkip  = "SKIP"
	outXFail = "XFAIL" // expected to fail on this build, and did
	outXPass = "XPASS" // expected to fail on this build, but passed
)

var (
	theBed *bed.Bed

	resultsMu sync.Mutex
	results   []result
)

type result struct {
	id, title, outcome, detail string
	took                       time.Duration
}

func TestMain(m *testing.M) {
	if os.Getenv("NBHA_E2E") != "1" {
		fmt.Println("e2e: NBHA_E2E is not 1; run through make e2e / make e2e-baseline (needs a running bed)")
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	b, err := bed.Load(ctx)
	if err == nil {
		err = prepare(ctx, b)
	}
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: bed not usable:", err)
		os.Exit(1)
	}
	theBed = b
	fmt.Printf("e2e: bed impl=%s instances=%d lb=%s\n", b.Impl, b.Instances, b.LBMode)
	code := m.Run()
	printReport()
	os.Exit(code)
}

// prepare turns on serial logging on every compose client and checks they are enrolled.
func prepare(ctx context.Context, b *bed.Bed) error {
	for _, c := range bed.Clients {
		if err := bed.EnableSerialLogging(ctx, bed.Container(c)); err != nil {
			return fmt.Errorf("client %s: %w", c, err)
		}
	}
	return nil
}

// spec describes a scenario and when it is expected to work.
type spec struct {
	id    string // report id, e.g. "2a"
	title string
	// needsTwo: skip unless the bed runs two management/signal instances.
	needsTwo bool
	// haIfTwo: with two instances the scenario depends on the HA build; on the
	// stock build it is expected to fail (XFAIL).
	haIfTwo bool
	// haOnly: needs the HA build even with one instance (an upstream gap the
	// HA build closes); expected to fail (XFAIL) on the stock build.
	haOnly bool
	// long: runs only with NBHA_E2E_LONG=1.
	long bool
}

// S is the handle a scenario gets. Fatalf/Errorf go through it so that a
// scenario expected to fail can be recorded as XFAIL instead of failing the run.
type S struct {
	*testing.T
	B     *bed.Bed
	Ctx   context.Context
	xfail string
	fails []string
}

type abort struct{}

// Fatalf records a failure and stops the scenario.
func (s *S) Fatalf(format string, args ...any) {
	s.Helper()
	msg := fmt.Sprintf(format, args...)
	s.fails = append(s.fails, msg)
	if s.xfail != "" {
		s.Logf("expected failure: %s", msg)
		panic(abort{})
	}
	s.T.Fatal(msg)
}

// Errorf records a failure and continues.
func (s *S) Errorf(format string, args ...any) {
	s.Helper()
	msg := fmt.Sprintf(format, args...)
	s.fails = append(s.fails, msg)
	if s.xfail != "" {
		s.Logf("expected failure: %s", msg)
		return
	}
	s.T.Error(msg)
}

// Two reports whether the bed runs two instances.
func (s *S) Two() bool { return s.B.Instances == 2 }

// run executes a scenario according to its spec.
func run(t *testing.T, sp spec, fn func(s *S)) {
	start := time.Now()
	res := &result{id: sp.id, title: sp.title}
	t.Cleanup(func() {
		res.took = time.Since(start).Round(100 * time.Millisecond)
		if res.outcome == "" {
			switch {
			case t.Failed():
				res.outcome = outFail
			case t.Skipped():
				res.outcome = outSkip
			default:
				res.outcome = outPass
			}
		}
		resultsMu.Lock()
		results = append(results, *res)
		resultsMu.Unlock()
	})

	if sp.long && os.Getenv("NBHA_E2E_LONG") != "1" {
		res.detail = "long scenario; set NBHA_E2E_LONG=1"
		t.Skip(res.detail)
	}
	if sp.needsTwo && theBed.Instances < 2 {
		res.detail = "needs two instances (HA build)"
		t.Skip(res.detail)
	}
	ctx, cancel := context.WithTimeout(context.Background(), scenarioTimeout(sp))
	// Cancel after the scenario's own cleanups (t.Cleanup runs last-in first-out),
	// which use ctx to remove containers, users and peers.
	t.Cleanup(cancel)
	s := &S{T: t, B: theBed, Ctx: ctx}
	if sp.haIfTwo && theBed.Instances == 2 && !theBed.HA() {
		s.xfail = "stock build has no cross-instance state"
	}
	if sp.haOnly && !theBed.HA() {
		s.xfail = "upstream gap closed only by the HA build"
	}
	if theBed.Instances == 1 && !sp.needsTwo {
		t.Logf("single instance: this checks the scenario's mechanics, not cross-instance behaviour")
	}

	healthy(s)
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(abort); !ok {
					panic(r)
				}
			}
		}()
		fn(s)
	}()

	if s.xfail != "" {
		if len(s.fails) > 0 {
			res.outcome, res.detail = outXFail, s.fails[0]
			t.Skipf("XFAIL (%s): %s", s.xfail, strings.Join(s.fails, "; "))
		}
		res.outcome = outXPass
		t.Logf("XPASS: expected to fail on the stock build (%s) but passed", s.xfail)
	} else if len(s.fails) > 0 {
		res.detail = s.fails[0]
	}
}

func scenarioTimeout(sp spec) time.Duration {
	if sp.long {
		return 26 * time.Hour
	}
	return 10 * time.Minute
}

func printReport() {
	resultsMu.Lock()
	defer resultsMu.Unlock()
	sort.SliceStable(results, func(i, j int) bool { return results[i].id < results[j].id })
	fmt.Printf("\n==== netbird-ha e2e report (impl=%s, instances=%d, lb=%s) ====\n", theBed.Impl, theBed.Instances, theBed.LBMode)
	counts := map[string]int{}
	for _, r := range results {
		counts[r.outcome]++
		detail := r.detail
		if len(detail) > 110 {
			detail = detail[:110] + "..."
		}
		fmt.Printf("%-3s %-6s %-58s %8s  %s\n", r.id, r.outcome, r.title, r.took, detail)
	}
	fmt.Printf("---- %d PASS, %d FAIL, %d XFAIL, %d XPASS, %d SKIP\n\n",
		counts[outPass], counts[outFail], counts[outXFail], counts[outXPass], counts[outSkip])
	if f := os.Getenv("NBHA_E2E_REPORT"); f != "" {
		var sb strings.Builder
		for _, r := range results {
			fmt.Fprintf(&sb, "%s\t%s\t%s\t%s\t%s\n", r.id, r.outcome, r.title, r.took, r.detail)
		}
		_ = os.WriteFile(f, []byte(sb.String()), 0o644)
	}
}

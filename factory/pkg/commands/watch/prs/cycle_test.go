package prs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

// recordingServer serves empty listings and records the path and query of every
// request, which is how these tests tell a sweep apart from a fast pass.
func recordingServer(t *testing.T) (*githubv39.Client, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var calls []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		call := r.URL.Path
		if q := r.URL.RawQuery; q != "" {
			call += "?" + q
		}
		calls = append(calls, call)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]interface{}{})
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	return gh, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), calls...)
	}
}

func countCalls(calls []string, substr string) int {
	n := 0
	for _, c := range calls {
		if strings.Contains(c, substr) {
			n++
		}
	}
	return n
}

// TestScanOnce_SweepsThenFastPasses pins the two-cadence design: the first cycle
// after startup is a full sweep, and the cycles until the sweep interval comes
// round again are the cheap pass over assigned pull requests.
//
// The distinguishing evidence is the open pull request listing, which only the
// sweep performs: it is the expensive query whose cost is the reason the
// cadences were split in the first place.
func TestScanOnce_SweepsThenFastPasses(t *testing.T) {
	gh, calls := recordingServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1"},
		TriggerLabel: "factory",
	})
	// Long enough that the second cycle cannot be a sweep.
	s.cfg.SweepInterval = time.Hour

	s.ScanOnce(context.Background())

	if got := countCalls(calls(), "/repos/test-owner/test-repo/pulls"); got != 1 {
		t.Errorf("open PR listings during the first cycle = %d, want 1 (a sweep)", got)
	}
	if got := countCalls(calls(), "labels=factory"); got == 0 {
		t.Error("the first cycle did not run the labelled listing; want a sweep")
	}

	before := len(calls())
	s.ScanOnce(context.Background())
	second := calls()[before:]

	if got := countCalls(second, "/repos/test-owner/test-repo/pulls"); got != 0 {
		t.Errorf("open PR listings during the second cycle = %d, want 0 (a fast pass)", got)
	}
	if got := countCalls(second, "labels=factory"); got != 0 {
		t.Errorf("labelled listings during the second cycle = %d, want 0 (a fast pass)", got)
	}
	if got := countCalls(second, "assignee=bot1"); got == 0 {
		t.Error("the second cycle did not list assigned pull requests; want a fast pass")
	}
}

// TestScanOnce_PausedWhileDraining checks that a draining watcher stops the
// scanner before it spends any GitHub requests, not just before it queues.
func TestScanOnce_PausedWhileDraining(t *testing.T) {
	gh, calls := recordingServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{GitHub: gh, BotUsers: []string{"bot1"}})
	s.paused = func() bool { return true }

	s.ScanOnce(context.Background())

	if got := len(calls()); got != 0 {
		t.Errorf("made %d GitHub requests while draining, want 0: %v", got, calls())
	}
}

// TestRun_StopsOnContextCancellation checks that cancellation stops the scanner
// and is not reported as a failure: it is how the subcontroller is asked to stop.
func TestRun_StopsOnContextCancellation(t *testing.T) {
	s, _ := newTestScanner(t, t.TempDir(), testOpts{})
	s.cfg.Interval = time.Hour
	s.paused = func() bool { return true }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s of cancellation")
	}
}

// TestRun_WaitsForIntervalAfterCycleCompletes pins the back-off the rate limit
// forced on us: the wait starts when a cycle finishes, not when it starts.
//
// A fixed-rate ticker would put the cycles one interval apart no matter how
// long each took - and for a cycle that overran the interval, back to back -
// which is the opposite of what a scanner being throttled by GitHub should do.
// The evidence is the gap between consecutive cycles: it must cover the work
// *and* the interval.
func TestRun_WaitsForIntervalAfterCycleCompletes(t *testing.T) {
	const (
		cycleDuration = 200 * time.Millisecond
		interval      = 150 * time.Millisecond
	)

	s, _ := newTestScanner(t, t.TempDir(), testOpts{})
	s.cfg.Interval = interval

	var mu sync.Mutex
	var starts []time.Time

	// Paused is read at the top of a cycle, which makes it both the clock and
	// the stand-in for a slow cycle: the scan returns as soon as it reports
	// true, so the sleep here is the whole of the cycle's duration.
	s.paused = func() bool {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		time.Sleep(cycleDuration)
		return true
	}

	// Long enough for three cycles at the end-to-start cadence, and for at
	// least four at the fixed-rate one this is here to rule out.
	ctx, cancel := context.WithTimeout(context.Background(), 3*(cycleDuration+interval))
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(starts) < 2 {
		t.Fatalf("ran %d cycles, want at least 2 to measure a gap", len(starts))
	}
	for i := 1; i < len(starts); i++ {
		gap := starts[i].Sub(starts[i-1])
		if gap < cycleDuration+interval {
			t.Errorf("cycle %d started %v after cycle %d, want at least %v (the cycle plus the interval)",
				i, gap, i-1, cycleDuration+interval)
		}
	}
}

// TestScanCandidates_Dedupes covers the guarantee a cycle depends on: a pull
// request that is both assigned and labelled is handed out once, so the dozen
// GitHub requests evaluating it costs are not spent twice in the same cycle.
func TestScanCandidates_Dedupes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The same pull request comes back from the assignee query and the
		// label query, alongside an issue that must be dropped entirely.
		_ = json.NewEncoder(w).Encode([]*githubv39.Issue{
			{Number: githubv39.Int(7), PullRequestLinks: &githubv39.PullRequestLinks{}},
			{Number: githubv39.Int(8)},
		})
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1", "bot2"},
		TriggerLabel: "factory",
	})

	candidates, err := s.scanCandidates(context.Background())
	if err != nil {
		t.Fatalf("scanCandidates() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("scanCandidates() returned %d candidates, want 1", len(candidates))
	}
	if candidates[0].GetNumber() != 7 {
		t.Errorf("candidate = #%d, want #7 (the pull request, not the issue)", candidates[0].GetNumber())
	}
}

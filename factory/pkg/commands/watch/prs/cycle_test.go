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

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/ratelimit"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
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

// rateLimitedServer refuses every request the way GitHub refuses one that has
// tripped a secondary limit, and records what was asked for.
//
// No quota headers are sent, deliberately. go-github throttles itself once it
// has seen them, which would make a cycle that spent nothing look like a
// working backoff no matter what this package did.
func rateLimitedServer(t *testing.T) (*githubv39.Client, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var calls []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`))
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

// TestScanCycle_RateLimitStopsTheCycleAndDelaysTheNext covers both halves of
// the backoff, and the seam between them.
//
// Within the cycle, the refusal must stop it: the quota is one number, so
// repeating the listing for the second bot account and then for the label only
// collects the same refusal. ScanOnce reports that by returning the error
// rather than by consulting the backoff, and it is Run - through cycle - that
// feeds it in. Between cycles, the resulting wait must displace the scan
// interval, which is what stops the daemon waking a minute later to spend the
// same requests on the same refusal.
func TestScanCycle_RateLimitStopsTheCycleAndDelaysTheNext(t *testing.T) {
	gh, calls := rateLimitedServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1", "bot2"},
		TriggerLabel: "factory",
	})

	if got := s.nextDelay(); got != s.cfg.Interval {
		t.Fatalf("delay before any refusal = %v, want the scan interval %v", got, s.cfg.Interval)
	}

	err := s.ScanOnce(context.Background())
	if err == nil {
		t.Fatal("ScanOnce() error = nil, want the refusal that stopped the cycle")
	}
	if !github.IsRateLimited(err) {
		t.Errorf("ScanOnce() error = %v, want one the backoff recognises as a rate limit", err)
	}

	refused := len(calls())
	if refused == 0 {
		t.Fatal("the cycle made no requests; the test cannot tell a backoff from a no-op")
	}
	if refused > 1 {
		t.Errorf("the refused cycle made %d requests, want 1: %v", refused, calls())
	}

	// ScanOnce reports the refusal; it does not act on it. Nothing has told the
	// backoff yet, so the cadence is still the plain interval.
	if got := s.nextDelay(); got != s.cfg.Interval {
		t.Errorf("delay after an unreported refusal = %v, want the scan interval %v: only Run feeds the backoff", got, s.cfg.Interval)
	}

	s.cycle(context.Background())

	delay := s.nextDelay()
	if delay <= s.cfg.Interval {
		t.Errorf("delay after the refusal = %v, want longer than the scan interval %v", delay, s.cfg.Interval)
	}
	if delay > DefaultMaxRateLimitBackoff {
		t.Errorf("delay after the refusal = %v, want no more than the cap %v", delay, DefaultMaxRateLimitBackoff)
	}
}

// TestRun_WaitsOutTheRateLimitBeforeScanningAgain is the same guarantee seen
// from outside: the loop that schedules cycles honours the wait, rather than
// each cycle waking up and turning itself away.
func TestRun_WaitsOutTheRateLimitBeforeScanningAgain(t *testing.T) {
	gh, calls := rateLimitedServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1"},
		TriggerLabel: "factory",
	})
	// An interval short enough that a second cycle would have run many times
	// over by the time the test looks, if the refusal were not holding it.
	s.cfg.Interval = time.Millisecond
	s.rateLimit = ratelimit.New("test", time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s of cancellation")
	}

	// Only the immediate cycle Run starts with should have happened.
	if got := len(calls()); got != 1 {
		t.Errorf("made %d requests over 100 one-millisecond intervals, want 1: the hour-long wait was not honoured", got)
	}
}

// TestRun_ResumesAfterTheBackoff checks the other half: the scanner comes back
// on its own once the wait has elapsed, without needing a restart.
func TestRun_ResumesAfterTheBackoff(t *testing.T) {
	gh, calls := rateLimitedServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1"},
		TriggerLabel: "factory",
	})
	s.cfg.Interval = time.Millisecond
	// A wait short enough to serve out inside a test, rather than the minute a
	// deployment uses.
	s.rateLimit = ratelimit.New("test", 5*time.Millisecond, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s of cancellation")
	}

	if got := len(calls()); got < 2 {
		t.Errorf("made %d requests, want more than one: the scanner did not resume after its wait elapsed", got)
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

// TestScanCandidates_Dedupes covers the guarantee the worker pool depends on: a
// pull request that is both assigned and labelled is handed out once, so no two
// workers can touch the same pull request's state in a cycle.
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

// twoCandidateServer serves a healthy pull request #2 and lets the caller decide
// how pull request #1 is answered, which is how the two tests below vary only in
// the kind of failure the first candidate runs into.
//
// It records the method and path of every request so a test can say whether the
// second candidate was ever reached.
func twoCandidateServer(t *testing.T, refuseFirst func(w http.ResponseWriter)) (*githubv39.Client, func() []string) {
	t.Helper()

	const headSHA = "sha-healthy"
	now := time.Now()

	var mu sync.Mutex
	var calls []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/repos/test-owner/test-repo/pulls/1":
			refuseFirst(w)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/pulls/2":
			_ = json.NewEncoder(w).Encode(&githubv39.PullRequest{
				Number:    githubv39.Int(2),
				Mergeable: githubv39.Bool(true),
				State:     githubv39.String("open"),
				User:      &githubv39.User{Login: githubv39.String("bot1")},
				Head:      &githubv39.PullRequestBranch{SHA: githubv39.String(headSHA)},
				CreatedAt: &now,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/pulls/2/commits":
			_ = json.NewEncoder(w).Encode([]*githubv39.RepositoryCommit{{
				SHA:    githubv39.String(headSHA),
				Commit: &githubv39.Commit{Committer: &githubv39.CommitAuthor{Date: &now}},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/pulls/2/reviews":
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestReview{})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/issues/2/comments":
			_ = json.NewEncoder(w).Encode([]*githubv39.IssueComment{})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/commits/"+headSHA+"/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"check_runs": []interface{}{}})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/test-owner/test-repo/commits/"+headSHA+"/statuses":
			_ = json.NewEncoder(w).Encode([]interface{}{})
		case strings.HasSuffix(r.URL.Path, "/labels"):
			_ = json.NewEncoder(w).Encode([]*githubv39.Label{})
		case isReadReactionsRequest(r):
			_ = json.NewEncoder(w).Encode([]*githubv39.Reaction{})
		case isReviewCommentsRequest(r):
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestComment{})
		default:
			_, _ = w.Write([]byte("{}"))
		}
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

func testCandidates(numbers ...int) []*githubv39.Issue {
	issues := make([]*githubv39.Issue, 0, len(numbers))
	for _, n := range numbers {
		issues = append(issues, &githubv39.Issue{
			Number:    githubv39.Int(n),
			Assignees: []*githubv39.User{{Login: githubv39.String("bot1")}},
			Labels:    []*githubv39.Label{{Name: githubv39.String("factory")}},
		})
	}
	return issues
}

// TestEvaluateAll_SkipsTheCandidateThatFailsForItsOwnReasons pins the isolation
// between candidates: one pull request the scanner cannot read must not cost the
// rest of the queue their turn.
//
// The failure is deliberately one that the next cycle will run into again. If a
// single such pull request could end the pass, every candidate behind it would
// be starved every cycle, and the only symptom would be work quietly not
// happening.
func TestEvaluateAll_SkipsTheCandidateThatFailsForItsOwnReasons(t *testing.T) {
	gh, calls := twoCandidateServer(t, func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"Server Error"}`))
	})
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	if err := s.evaluateAll(context.Background(), testCandidates(1, 2)); err != nil {
		t.Errorf("evaluateAll() error = %v, want nil: only a refusal ends the pass", err)
	}
	if got := countCalls(calls(), "GET /repos/test-owner/test-repo/pulls/2"); got == 0 {
		t.Errorf("the healthy pull request was never read: %v", calls())
	}
}

// TestEvaluateAll_StopsThePassWhenGitHubRefusesUs is the other half of the same
// contract. A refusal says nothing about the pull request that met it, so the
// candidates behind it would spend a dozen requests each collecting the same
// answer; the pass ends and reports it, and the cycle after the wait picks them
// up.
func TestEvaluateAll_StopsThePassWhenGitHubRefusesUs(t *testing.T) {
	gh, calls := twoCandidateServer(t, func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`))
	})
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       gh,
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	err := s.evaluateAll(context.Background(), testCandidates(1, 2))
	if err == nil {
		t.Fatal("evaluateAll() error = nil, want the refusal that ended the pass")
	}
	if !github.IsRateLimited(err) {
		t.Errorf("evaluateAll() error = %v, want one the backoff recognises as a rate limit", err)
	}
	if got := countCalls(calls(), "GET /repos/test-owner/test-repo/pulls/2"); got != 0 {
		t.Errorf("the pass carried on to the next candidate after a refusal: %v", calls())
	}
}

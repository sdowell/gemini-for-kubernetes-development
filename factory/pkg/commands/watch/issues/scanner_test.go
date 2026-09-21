package issues

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

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/ratelimit"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// fakeQueue records what the scanner queued and withdrew, and stands in for the
// finished work the real queue would hand back.
type fakeQueue struct {
	mu        sync.Mutex
	existing  map[string]bool
	enqueued  map[string]*api.QueueTask
	processed map[string]*api.QueueTask
	removed   []int
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{
		existing:  map[string]bool{},
		enqueued:  map[string]*api.QueueTask{},
		processed: map[string]*api.QueueTask{},
	}
}

func (q *fakeQueue) TaskExists(filename string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.existing[filename]
}

func (q *fakeQueue) Enqueue(filename string, task *api.QueueTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueued[filename] = task
	q.existing[filename] = true
	return nil
}

func (q *fakeQueue) RemovePendingTasksForNumber(number int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.removed = append(q.removed, number)
	return nil
}

func (q *fakeQueue) GetProcessedTask(filename string) *api.QueueTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.processed[filename]
}

func (q *fakeQueue) ListProcessedTasks() map[string]*api.QueueTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	snapshot := make(map[string]*api.QueueTask, len(q.processed))
	for fn, t := range q.processed {
		snapshot[fn] = t
	}
	return snapshot
}

// finish records a task as completed, as the real queue does when a task ends.
func (q *fakeQueue) finish(filename string, task *api.QueueTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.processed[filename] = task
}

// fakeEntities stands in for the shared entity cache.
type fakeEntities struct {
	hasPRs     bool
	referenced map[int]bool
	openIssues []int
	prsSet     int
}

func (e *fakeEntities) HasOpenPRs() bool { return e.hasPRs }

func (e *fakeEntities) UpdateOpenPRs(prs []*githubv39.PullRequest) {
	e.prsSet++
	e.hasPRs = true
	if e.referenced == nil {
		e.referenced = map[int]bool{}
	}
}

func (e *fakeEntities) GetReferencedIssuesMap() map[int]bool { return e.referenced }

func (e *fakeEntities) SetOpenIssueNumbers(nums []int) { e.openIssues = nums }

// fakeSandboxes reports the sandboxes that are mid-run.
type fakeSandboxes struct {
	running map[string]bool
}

func (s fakeSandboxes) IsTaskRunning(_ context.Context, name string) (bool, error) {
	return s.running[name], nil
}

// fakeUsers pins every task to one account.
type fakeUsers struct{ user string }

func (u fakeUsers) SelectUser(context.Context, api.TaskType, int) (string, error) {
	return u.user, nil
}

// newScanner builds a scanner over fakes, with the cache already primed so that
// tests exercising the queueing path do not have to serve a PR listing.
func newScanner(t *testing.T, cfg Config, deps Deps) (*Scanner, *fakeQueue, *fakeEntities) {
	t.Helper()
	queue := newFakeQueue()
	entities := &fakeEntities{hasPRs: true, referenced: map[int]bool{}}

	if cfg.TriggerLabel == "" {
		cfg.TriggerLabel = "factory"
	}
	if deps.GitHub == nil {
		// A client with no transport still carries the owner and repo, which
		// the scanner reads for sandbox names and task URLs.
		deps.GitHub = github.ForRepo(nil, "test-owner", "test-repo")
	}
	if deps.Queue == nil {
		deps.Queue = queue
	}
	if deps.Entities == nil {
		deps.Entities = entities
	}
	if deps.Sandboxes == nil {
		deps.Sandboxes = fakeSandboxes{}
	}
	if deps.Users == nil {
		deps.Users = fakeUsers{user: "bot1"}
	}
	return New(cfg, deps), queue, entities
}

func TestQueueTasks_Filters(t *testing.T) {
	s, queue, _ := newScanner(t, Config{MinNumber: 6}, Deps{})

	issues := []*githubv39.Issue{
		{Number: githubv39.Int(5)},
		{
			Number: githubv39.Int(10),
			Labels: []*githubv39.Label{{Name: githubv39.String("overseer/stop")}},
		},
		{Number: githubv39.Int(15)},
	}

	s.queueTasks(context.Background(), issues, map[int]bool{15: true})

	if len(queue.enqueued) != 0 {
		t.Errorf("queued %d tasks, want 0: %v", len(queue.enqueued), queue.enqueued)
	}
	// A stopped issue must also have its pending work withdrawn, not merely be
	// skipped: the operator applied the label to stop work already queued.
	if len(queue.removed) != 1 || queue.removed[0] != 10 {
		t.Errorf("withdrew pending tasks for %v, want [10]", queue.removed)
	}
}

// TestQueueTasks_ReadsFinishedWorkFromQueue covers the two points at which the
// scanner asks the queue what has already been done. Both used to be reads of
// the processed/ directory.
//
// The control case comes first and is what makes the rest mean anything: an
// issue with nothing recorded against it must actually be queued. Without it
// the "not queued" assertions would pass for any reason at all, including a
// scan that never got as far as the queue.
//
// The two gating cases are then separated by timing so that each can only be
// explained by one of the two lookups. The default workflow cooldown is ten
// minutes, so a task that finished two hours ago is well out of cooldown and
// can only be caught by the last-worked-on record; an issue updated just now
// is newer than its last task and can only be caught by the cooldown.
func TestQueueTasks_ReadsFinishedWorkFromQueue(t *testing.T) {
	// newIssueScanner returns a scanner whose GitHub calls are served well
	// enough for an issue to make it all the way to the queue.
	newIssueScanner := func(t *testing.T) (*Scanner, *fakeQueue) {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/timeline") {
				// An empty but complete timeline settles the linked-PR check
				// without a fall back to the Search API.
				_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
				return
			}
			if strings.HasSuffix(r.URL.Path, "/labels") {
				// Adopting an unlabelled issue posts here, and the response is
				// the issue's labels. Answering with the default object body
				// fails to decode, which now ends the scan rather than being
				// logged and stepped over.
				_ = json.NewEncoder(w).Encode([]*githubv39.Label{})
				return
			}
			_, _ = w.Write([]byte("{}"))
		}))
		t.Cleanup(server.Close)

		gh := githubv39.NewClient(nil)
		gh.BaseURL, _ = url.Parse(server.URL + "/")

		s, queue, _ := newScanner(t, Config{TargetAssignee: "bot1"}, Deps{
			GitHub: github.ForRepo(gh, "test-owner", "test-repo"),
		})
		return s, queue
	}

	issue := func(num int, updated time.Time) *githubv39.Issue {
		return &githubv39.Issue{Number: githubv39.Int(num), UpdatedAt: &updated}
	}

	t.Run("queues an issue with no finished work against it", func(t *testing.T) {
		s, queue := newIssueScanner(t)

		s.queueTasks(context.Background(), []*githubv39.Issue{
			issue(7, time.Now().Add(-3*time.Hour)),
		}, map[int]bool{})

		if _, ok := queue.enqueued["task-issue-7.yaml"]; !ok {
			t.Fatalf("issue 7 was not queued; queued: %v", queue.enqueued)
		}
	})

	t.Run("skips an issue not updated since its last task finished", func(t *testing.T) {
		s, queue := newIssueScanner(t)
		queue.finish("task-issue-7.yaml", &api.QueueTask{
			Type:        api.TypeIssueFix,
			Number:      7,
			Status:      api.StatusCompleted,
			CompletedAt: time.Now().Add(-2 * time.Hour),
		})

		s.queueTasks(context.Background(), []*githubv39.Issue{
			issue(7, time.Now().Add(-3*time.Hour)),
		}, map[int]bool{})

		if len(queue.enqueued) != 0 {
			t.Errorf("queued %v, want nothing: the issue has not been updated since its last task finished", queue.enqueued)
		}
	})

	t.Run("skips an issue whose last task is still in cooldown", func(t *testing.T) {
		s, queue := newIssueScanner(t)
		queue.finish("task-issue-7.yaml", &api.QueueTask{
			Type:        api.TypeIssueFix,
			Number:      7,
			Status:      api.StatusCompleted,
			CompletedAt: time.Now().Add(-time.Minute),
		})

		s.queueTasks(context.Background(), []*githubv39.Issue{
			issue(7, time.Now()),
		}, map[int]bool{})

		if len(queue.enqueued) != 0 {
			t.Errorf("queued %v, want nothing: the last task finished a minute ago, inside the cooldown", queue.enqueued)
		}
	})

	// A failed task did not do the work, so it must gate nothing: the issue
	// still needs an agent, and recording the failure would park it until
	// someone touched the issue again.
	t.Run("a failed task gates nothing", func(t *testing.T) {
		s, queue := newIssueScanner(t)
		queue.finish("task-issue-7.yaml", &api.QueueTask{
			Type:        api.TypeIssueFix,
			Number:      7,
			Status:      api.StatusFailed,
			CompletedAt: time.Now().Add(-2 * time.Hour),
		})

		s.queueTasks(context.Background(), []*githubv39.Issue{
			issue(7, time.Now().Add(-3*time.Hour)),
		}, map[int]bool{})

		if _, ok := queue.enqueued["task-issue-7.yaml"]; !ok {
			t.Errorf("issue 7 was gated by a task that failed; queued: %v", queue.enqueued)
		}
	})
}

// TestScanOnce_ColdPRCacheFailsClosed is the regression test for
// k8s-config-connector#9259: after a restart into a rate limit window the open
// PR cache is empty, which makes every issue look like it has no linked PR.
func TestScanOnce_ColdPRCacheFailsClosed(t *testing.T) {
	listedIssues := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/test-owner/test-repo/pulls":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
		default:
			listedIssues = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]*githubv39.Issue{{Number: githubv39.Int(1)}})
		}
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	entities := &fakeEntities{referenced: map[int]bool{}}
	s, queue, _ := newScanner(t, Config{BotUsers: []string{"bot1"}, PrimeOpenPRs: true}, Deps{GitHub: github.ForRepo(gh, "test-owner", "test-repo"), Entities: entities})

	s.ScanOnce(context.Background())

	if listedIssues {
		t.Error("scanned issues with an unpopulated open PR cache; want the cycle to stop")
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("queued %d tasks with an unpopulated open PR cache, want 0", len(queue.enqueued))
	}
}

// TestScanOnce_PrimesPRCache covers the mode where no pull request scanner
// runs: the issue scanner pays for the one listing itself rather than stalling
// forever on a cache nobody is going to fill.
func TestScanOnce_PrimesPRCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/test-owner/test-repo/pulls":
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequest{})
		default:
			_ = json.NewEncoder(w).Encode([]*githubv39.Issue{})
		}
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	entities := &fakeEntities{referenced: map[int]bool{}}
	s, _, _ := newScanner(t, Config{PrimeOpenPRs: true}, Deps{GitHub: github.ForRepo(gh, "test-owner", "test-repo"), Entities: entities})

	s.ScanOnce(context.Background())

	if entities.prsSet != 1 {
		t.Errorf("published the open PR listing %d times, want 1", entities.prsSet)
	}

	// Once a scan has published, there is nothing left to prime.
	s.ScanOnce(context.Background())
	if entities.prsSet != 1 {
		t.Errorf("published the open PR listing %d times after priming, want 1", entities.prsSet)
	}
}

// TestScanOnce_DoesNotPrimePRCacheWhenPRScannerRuns is the other half of
// TestScanOnce_PrimesPRCache. When a pull request scanner is running, that
// scanner owns the open PR half of the cache; priming it here as well would
// duplicate the same paginated listing on every start and give the cache two
// writers. The cold cycle is skipped instead, and the next one proceeds on what
// the pull request scanner published.
func TestScanOnce_DoesNotPrimePRCacheWhenPRScannerRuns(t *testing.T) {
	var listedPRs bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/test-owner/test-repo/pulls" {
			listedPRs = true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*githubv39.Issue{})
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	entities := &fakeEntities{referenced: map[int]bool{}}
	s, _, _ := newScanner(t, Config{}, Deps{GitHub: github.ForRepo(gh, "test-owner", "test-repo"), Entities: entities})

	s.ScanOnce(context.Background())

	if listedPRs {
		t.Error("listed open PRs while a pull request scanner owns that half of the cache; want the listing left to it")
	}
	if entities.prsSet != 0 {
		t.Errorf("published the open PR listing %d times, want 0", entities.prsSet)
	}
}

func TestScanOnce_QueuesAssignedIssue(t *testing.T) {
	updated := time.Now().Add(-time.Hour)
	created := updated.Add(-time.Hour)

	var addedLabels []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/repos/test-owner/test-repo/issues" && r.Method == http.MethodGet:
			// Only the assignee query returns anything; the labelled sweep and
			// the creator query are served empty.
			if r.URL.Query().Get("assignee") != "bot1" {
				_ = json.NewEncoder(w).Encode([]*githubv39.Issue{})
				return
			}
			_ = json.NewEncoder(w).Encode([]*githubv39.Issue{{
				Number:    githubv39.Int(7),
				CreatedAt: &created,
				UpdatedAt: &updated,
				Body:      githubv39.String("please fix"),
			}})
		case r.URL.Path == "/repos/test-owner/test-repo/issues/7/timeline":
			_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
		case r.URL.Path == "/repos/test-owner/test-repo/issues/7/labels" && r.Method == http.MethodPost:
			var labels []string
			_ = json.NewDecoder(r.Body).Decode(&labels)
			addedLabels = append(addedLabels, labels...)
			_ = json.NewEncoder(w).Encode([]interface{}{})
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	s, queue, entities := newScanner(t, Config{BotUsers: []string{"bot1"}, TargetAssignee: "bot1"}, Deps{GitHub: github.ForRepo(gh, "test-owner", "test-repo")})

	s.ScanOnce(context.Background())

	task, ok := queue.enqueued["task-issue-7.yaml"]
	if !ok {
		t.Fatalf("issue 7 was not queued; queued: %v", queue.enqueued)
	}
	if task.Type != api.TypeIssueFix {
		t.Errorf("task type = %q, want %q", task.Type, api.TypeIssueFix)
	}
	if task.Assignee != "bot1" {
		t.Errorf("task assignee = %q, want 'bot1'", task.Assignee)
	}
	// An issue picked up by assignment is adopted with the trigger label, so
	// that the label always records what the watcher is acting on.
	if len(addedLabels) != 1 || addedLabels[0] != "factory" {
		t.Errorf("added labels %v, want ['factory']", addedLabels)
	}
	// The labelled sweep runs on the first cycle, so the open issue set is
	// published for the sandbox reconciler to collect against.
	if entities.openIssues == nil {
		t.Error("the open issue set was not published after the first sweep")
	}

	// A second cycle must not re-queue: the issue has not been updated since.
	delete(queue.existing, "task-issue-7.yaml")
	delete(queue.enqueued, "task-issue-7.yaml")
	s.ScanOnce(context.Background())
	if _, ok := queue.enqueued["task-issue-7.yaml"]; ok {
		t.Error("issue 7 was queued twice without having been updated in between")
	}
}

func TestScanOnce_PausedWhileDraining(t *testing.T) {
	s, queue, _ := newScanner(t, Config{}, Deps{Paused: func() bool { return true }})

	s.ScanOnce(context.Background())

	if len(queue.enqueued) != 0 {
		t.Errorf("queued %d tasks while draining, want 0", len(queue.enqueued))
	}
}

func TestScanOnce_SkipsIssueWithRunningSandbox(t *testing.T) {
	// The in-flight sandbox check is the last gate before queueing, so the
	// timeline lookup ahead of it has to be served for the test to reach it.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
	}))
	defer server.Close()

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	updated := time.Now()
	s, queue, _ := newScanner(t, Config{}, Deps{
		GitHub:    github.ForRepo(gh, "test-owner", "test-repo"),
		Sandboxes: fakeSandboxes{running: map[string]bool{"fix-test-repo-9": true}},
	})

	s.queueTasks(context.Background(), []*githubv39.Issue{{
		Number:    githubv39.Int(9),
		UpdatedAt: &updated,
		Labels:    []*githubv39.Label{{Name: githubv39.String("factory")}},
	}}, nil)

	if len(queue.enqueued) != 0 {
		t.Errorf("queued %d tasks for an issue with an in-flight sandbox, want 0", len(queue.enqueued))
	}
}

func TestRun_StopsOnContextCancellation(t *testing.T) {
	s, _, _ := newScanner(t, Config{Interval: time.Hour}, Deps{Paused: func() bool { return true }})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		// Cancellation is how the subcontroller is asked to stop, so it is not
		// reported as a failure.
		if err != nil {
			t.Errorf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s of cancellation")
	}
}

// rateLimitedServer refuses every request the way GitHub refuses one that has
// tripped a secondary limit, and counts what was asked for.
//
// No quota headers are sent, deliberately. go-github throttles itself once it
// has seen them, which would make a cycle that spent nothing look like a
// working backoff no matter what this package did.
func rateLimitedServer(t *testing.T) (*github.Client, func() int) {
	t.Helper()

	var mu sync.Mutex
	var calls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`))
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")

	return github.ForRepo(gh, "test-owner", "test-repo"), func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
}

// TestScanCycle_RateLimitStopsTheCycleAndDelaysTheNext covers both halves of
// the backoff, and the seam between them.
//
// Within the cycle, the refusal must stop it: the quota is one number, so
// repeating the listing for every bot account and then for the operator only
// collects the same refusal. ScanOnce reports that by returning the error
// rather than by consulting the backoff, and it is Run - through cycle - that
// feeds it in. Between cycles, the resulting wait must displace the scan
// interval, which is what stops the daemon waking half a minute later to spend
// the same requests on the same refusal.
func TestScanCycle_RateLimitStopsTheCycleAndDelaysTheNext(t *testing.T) {
	gh, requests := rateLimitedServer(t)
	s, _, _ := newScanner(t, Config{GitHubLogin: "operator", BotUsers: []string{"bot1", "bot2"}}, Deps{GitHub: gh})

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

	refused := requests()
	if refused == 0 {
		t.Fatal("the cycle made no requests; the test cannot tell a backoff from a no-op")
	}
	if refused > 1 {
		t.Errorf("the refused cycle made %d requests, want 1", refused)
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
	gh, requests := rateLimitedServer(t)
	// An interval short enough that a second cycle would have run many times
	// over by the time the test looks, if the refusal were not holding it.
	s, _, _ := newScanner(t, Config{Interval: time.Millisecond, BotUsers: []string{"bot1"}}, Deps{GitHub: gh})
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
	if got := requests(); got != 1 {
		t.Errorf("made %d requests over 100 one-millisecond intervals, want 1: the hour-long wait was not honoured", got)
	}
}

// TestRun_ResumesAfterTheBackoff checks the other half: the scanner comes back
// on its own once the wait has elapsed, without needing a restart.
func TestRun_ResumesAfterTheBackoff(t *testing.T) {
	gh, requests := rateLimitedServer(t)
	s, _, _ := newScanner(t, Config{Interval: time.Millisecond, BotUsers: []string{"bot1"}}, Deps{GitHub: gh})
	// A wait short enough to serve out inside a test, rather than the half
	// minute a deployment uses.
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

	if got := requests(); got < 2 {
		t.Errorf("made %d requests, want more than one: the scanner did not resume after its wait elapsed", got)
	}
}

// twoIssueServer serves issue #8 well enough to be queued, and lets the caller
// decide how issue #7's timeline is answered. That read is the first GitHub call
// queueTask makes for an issue, so failing it stands in for anything that can go
// wrong while evaluating one.
func twoIssueServer(t *testing.T, refuseFirst func(w http.ResponseWriter)) *github.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/issues/7/"):
			refuseFirst(w)
		case strings.HasSuffix(r.URL.Path, "/timeline"):
			_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
		case strings.HasSuffix(r.URL.Path, "/labels"):
			_ = json.NewEncoder(w).Encode([]*githubv39.Label{})
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")
	return github.ForRepo(gh, "test-owner", "test-repo")
}

// TestQueueTasks_SkipsTheIssueThatFailsForItsOwnReasons pins the isolation
// between issues: one issue the scanner cannot read must not cost the issues
// behind it their turn, because nothing about the next cycle makes that issue
// readable and the queue would simply stop at it every time.
func TestQueueTasks_SkipsTheIssueThatFailsForItsOwnReasons(t *testing.T) {
	gh := twoIssueServer(t, func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"Server Error"}`))
	})
	s, queue, _ := newScanner(t, Config{}, Deps{GitHub: gh})

	issues := []*githubv39.Issue{
		{Number: githubv39.Int(7)},
		{Number: githubv39.Int(8)},
	}
	if err := s.queueTasks(context.Background(), issues, map[int]bool{}); err != nil {
		t.Errorf("queueTasks() error = %v, want nil: only a refusal ends the pass", err)
	}
	if _, ok := queue.enqueued["task-issue-8.yaml"]; !ok {
		t.Errorf("issue 8 was not queued after issue 7 failed; queued: %v", queue.enqueued)
	}
}

// TestQueueTasks_StopsThePassWhenGitHubRefusesUs is the other half: a refusal is
// about the quota rather than about the issue, so the issues behind it would
// only collect the same answer.
func TestQueueTasks_StopsThePassWhenGitHubRefusesUs(t *testing.T) {
	gh := twoIssueServer(t, func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`))
	})
	s, queue, _ := newScanner(t, Config{}, Deps{GitHub: gh})

	issues := []*githubv39.Issue{
		{Number: githubv39.Int(7)},
		{Number: githubv39.Int(8)},
	}
	err := s.queueTasks(context.Background(), issues, map[int]bool{})
	if err == nil {
		t.Fatal("queueTasks() error = nil, want the refusal that ended the pass")
	}
	if !github.IsRateLimited(err) {
		t.Errorf("queueTasks() error = %v, want one the backoff recognises as a rate limit", err)
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("queued %v after a refusal, want nothing", queue.enqueued)
	}
}

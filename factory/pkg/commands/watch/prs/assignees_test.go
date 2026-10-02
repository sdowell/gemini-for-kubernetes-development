package prs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

// assigneeServer is a fake GitHub API for the assignee inheritance tests. It
// serves the pull requests and issues it was given, accepts the label,
// comment and assignee writes, and records every request.
type assigneeServer struct {
	pulls  map[int]*githubv39.PullRequest
	issues map[int]*githubv39.Issue

	mu sync.Mutex
	// failAssign makes POST .../assignees fail with a 500.
	failAssign bool
	calls      []string
	// assigned is the body of every successful assignee request.
	assigned [][]string
}

func newAssigneeServer(t *testing.T) (*assigneeServer, *githubv39.Client) {
	t.Helper()
	fs := &assigneeServer{pulls: map[int]*githubv39.PullRequest{}, issues: map[int]*githubv39.Issue{}}
	server := httptest.NewServer(http.HandlerFunc(fs.serve))
	t.Cleanup(server.Close)
	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")
	return fs, gh
}

func (fs *assigneeServer) serve(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.calls = append(fs.calls, r.Method+" "+r.URL.Path)

	const prefix = "/repos/test-owner/test-repo/"
	path := strings.TrimPrefix(r.URL.Path, prefix)
	parts := strings.Split(path, "/")
	writeJSON := func(v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	num := func() int {
		var n int
		if len(parts) > 1 {
			_ = json.Unmarshal([]byte(parts[1]), &n)
		}
		return n
	}

	switch {
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "pulls":
		if pr, ok := fs.pulls[num()]; ok {
			writeJSON(pr)
			return
		}
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "issues":
		if issue, ok := fs.issues[num()]; ok {
			writeJSON(issue)
			return
		}
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "issues" && parts[2] == "assignees":
		if fs.failAssign {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Assignees []string `json:"assignees"`
		}
		_ = json.Unmarshal(body, &req)
		fs.assigned = append(fs.assigned, req.Assignees)
		writeJSON(&githubv39.Issue{})
		return
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "issues" && parts[2] == "labels":
		writeJSON([]*githubv39.Label{})
		return
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "issues" && parts[2] == "comments":
		writeJSON(&githubv39.IssueComment{})
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// takeCalls returns the requests recorded so far and forgets them.
func (fs *assigneeServer) takeCalls() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	calls := fs.calls
	fs.calls = nil
	return calls
}

// takeAssigned returns the assignee requests recorded so far and forgets them.
func (fs *assigneeServer) takeAssigned() [][]string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	assigned := fs.assigned
	fs.assigned = nil
	return assigned
}

func testUser(login string) *githubv39.User { return &githubv39.User{Login: stringPtr(login)} }

func testIssue(num int, creator string, assignees ...string) *githubv39.Issue {
	issue := &githubv39.Issue{Number: &num, User: testUser(creator)}
	for _, a := range assignees {
		issue.Assignees = append(issue.Assignees, testUser(a))
	}
	return issue
}

func assignedEqual(got [][]string, want ...[]string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if strings.Join(got[i], ",") != strings.Join(want[i], ",") {
			return false
		}
	}
	return true
}

// TestReconcileReadiness_AlreadyReadySyncsAssignees checks that a pull request
// already labelled ready-for-human still picks up humans from its parent
// issues, and that the creator fallback applies only to an issue it closes.
func TestReconcileReadiness_AlreadyReadySyncsAssignees(t *testing.T) {
	fs, gh := newAssigneeServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{GitHub: gh, BotUsers: []string{"ada-coder"}})

	prNum := 100
	mergeable := true
	now := time.Now()
	pr := &githubv39.PullRequest{
		Number:    &prNum,
		Mergeable: &mergeable,
		State:     stringPtr("open"),
		// #7 is closed by the PR; #9 is only mentioned, on the same line,
		// which a looser parser would read as closing it too.
		Body: stringPtr("Fixes #7, related to #9"),
	}
	pc := &prContext{
		pr: pr,
		prIssue: &githubv39.Issue{
			Number: &prNum,
			Labels: []*githubv39.Label{{Name: stringPtr("overseer/ready-for-human")}},
		},
		headSHA: "sha123",
		refIssues: &refIssues{
			pr:     pr,
			loaded: true,
			resolved: []*githubv39.Issue{
				testIssue(7, "dave"),
				testIssue(9, "erin"),
			},
		},
	}
	history := &prHistory{
		reviews: []*githubv39.PullRequestReview{{
			User:        testUser("reviewbot"),
			CommitID:    stringPtr("sha123"),
			State:       stringPtr("APPROVED"),
			SubmittedAt: &now,
		}},
	}

	s.reconcileReadiness(context.Background(), pc, prCheckAnalysis{}, false, history, false, "")

	if got := fs.takeAssigned(); !assignedEqual(got, []string{"dave"}) {
		t.Errorf("assigned %v; want [[dave]]", got)
	}
}

// TestInheritAssigneesForStoppedPR covers when the early stop path re-fetches a
// pull request to sync its assignees.
func TestInheritAssigneesForStoppedPR(t *testing.T) {
	ctx := context.Background()
	prNum := 100

	setup := func(t *testing.T) (*assigneeServer, *Scanner, *githubv39.Issue) {
		fs, gh := newAssigneeServer(t)
		s, _ := newTestScanner(t, t.TempDir(), testOpts{GitHub: gh, BotUsers: []string{"ada-coder"}})
		fs.pulls[prNum] = &githubv39.PullRequest{Number: &prNum, Body: stringPtr("Fixes #7")}
		fs.issues[7] = testIssue(7, "dave")
		updated := time.Now().Add(-time.Hour)
		prIssue := &githubv39.Issue{
			Number:    &prNum,
			User:      testUser("ada-coder"),
			UpdatedAt: &updated,
			Labels:    []*githubv39.Label{{Name: stringPtr("overseer/stop")}},
		}
		return fs, s, prIssue
	}

	t.Run("syncs once, then skips until the PR changes", func(t *testing.T) {
		fs, s, prIssue := setup(t)

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if got := fs.takeAssigned(); !assignedEqual(got, []string{"dave"}) {
			t.Fatalf("first sync assigned %v; want [[dave]]", got)
		}
		fs.takeCalls()

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if calls := fs.takeCalls(); len(calls) != 0 {
			t.Fatalf("unchanged PR made %d requests (%v); want none", len(calls), calls)
		}

		later := time.Now().Add(time.Minute)
		prIssue.UpdatedAt = &later
		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if calls := fs.takeCalls(); len(calls) == 0 {
			t.Fatalf("PR updated after the sync made no requests; want a re-sync")
		}
	})

	t.Run("re-syncs once a sweep interval has passed", func(t *testing.T) {
		fs, s, prIssue := setup(t)

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		fs.takeCalls()

		state := s.state.get(prNum)
		state.stoppedAssigneeSyncTime = time.Now().Add(-s.cfg.SweepInterval - time.Second)
		s.state.set(prNum, state)
		// The PR has not changed since, so only the interval can trigger this.
		before := state.stoppedAssigneeSyncTime.Add(-time.Minute)
		prIssue.UpdatedAt = &before

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if calls := fs.takeCalls(); len(calls) == 0 {
			t.Fatalf("made no requests after a sweep interval; want a re-sync")
		}
	})

	t.Run("a failed assignment is retried on the next pass", func(t *testing.T) {
		fs, s, prIssue := setup(t)
		fs.failAssign = true

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		fs.takeCalls()

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if calls := fs.takeCalls(); len(calls) == 0 {
			t.Fatalf("made no requests after a failed assignment; want a retry")
		}
	})

	t.Run("a PR not authored by the bot pool is left alone", func(t *testing.T) {
		fs, s, prIssue := setup(t)
		prIssue.User = testUser("alice")

		s.inheritAssigneesForStoppedPR(ctx, prIssue)
		if calls := fs.takeCalls(); len(calls) != 0 {
			t.Fatalf("made %d requests (%v) for a human-authored PR; want none", len(calls), calls)
		}
	})
}

// TestPauseIfInactive_InheritsAssignees checks that pausing an inactive pull
// request assigns its parent issues' humans and records the sync, so the next
// pass's early stop path does not repeat it.
func TestPauseIfInactive_InheritsAssignees(t *testing.T) {
	fs, gh := newAssigneeServer(t)
	s, _ := newTestScanner(t, t.TempDir(), testOpts{GitHub: gh, BotUsers: []string{"ada-coder"}})
	s.cfg.InactivityTimeout = time.Hour

	prNum := 100
	created := time.Now().Add(-2 * time.Hour)
	pr := &githubv39.PullRequest{Number: &prNum, CreatedAt: &created, Body: stringPtr("Fixes #7")}
	prIssue := &githubv39.Issue{Number: &prNum, User: testUser("ada-coder"), UpdatedAt: &created}
	refs := &refIssues{pr: pr, loaded: true, resolved: []*githubv39.Issue{testIssue(7, "dave", "bob")}}

	// The pause's own label, comment and assignment all land on GitHub
	// during the call - after this instant, before the sync is recorded - so
	// any updated_at in that window stands in for them.
	pausedAt := time.Now()
	if !s.pauseIfInactive(context.Background(), pr, prIssue, refs, &prHistory{}, "sha123") {
		t.Fatalf("pauseIfInactive() = false; want true for a PR inactive past the timeout")
	}
	if got := fs.takeAssigned(); !assignedEqual(got, []string{"bob"}) {
		t.Errorf("assigned %v; want [[bob]] (the issue's assignee, not its creator)", got)
	}
	// Those writes move updated_at; that must not count as a change that
	// needs another sync.
	prIssue.UpdatedAt = &pausedAt
	if !s.stoppedAssigneesInSync(prIssue) {
		t.Errorf("stoppedAssigneesInSync() = false after pausing; want the sync recorded")
	}
}

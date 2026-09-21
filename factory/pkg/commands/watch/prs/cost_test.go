package prs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

// The tests in this file are about what an evaluation *costs*, rather than what
// it decides. They exist because the cost is not visible in the behaviour: a
// scanner that fetches the same parent issue three times and one that fetches it
// once queue exactly the same work, and the difference only shows up as a rate
// limit in production. Each test therefore asserts on the requests the scanner
// made, not on its conclusions.

// prFixture is a GitHub stand-in for one pull request that records every path
// it is asked for.
//
// The knobs are the inputs that decide whether a cycle may skip the pull
// request - its updated_at, whether CI is still running - so that a test can
// move one of them between cycles and watch what the scanner does about it.
type prFixture struct {
	mu sync.Mutex

	num     int
	headSHA string
	// body is the pull request body, which is where a "Fixes #7" reference
	// that pulls a parent issue into the evaluation comes from.
	body string
	// updated is the updated_at the listing reports for the pull request.
	updated time.Time
	// pending makes the head commit's CI report an unfinished check run.
	pending bool
	// reviews are the submitted reviews, whose inline comments used to cost a
	// request each.
	reviews []*githubv39.PullRequestReview
	// inline are the review comments the pull request-wide listing returns.
	inline []*githubv39.PullRequestComment

	calls []string
}

// start brings up the server and returns a client pointed at it.
func (f *prFixture) start(t *testing.T) *githubv39.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		call := r.URL.Path
		if q := r.URL.RawQuery; q != "" {
			call += "?" + q
		}
		f.calls = append(f.calls, call)
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		f.route(w, r)
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")
	return gh
}

func (f *prFixture) route(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	base := "/repos/test-owner/test-repo"
	mergeable := true
	now := time.Now()

	switch r.URL.Path {
	case base + "/issues":
		// Both the fast pass's assignee query and the sweep's label query.
		_ = json.NewEncoder(w).Encode([]*githubv39.Issue{f.listingIssueLocked()})

	case base + "/pulls":
		_ = json.NewEncoder(w).Encode([]*githubv39.PullRequest{})

	case fmt.Sprintf("%s/pulls/%d", base, f.num):
		_ = json.NewEncoder(w).Encode(&githubv39.PullRequest{
			Number:    githubv39.Int(f.num),
			Mergeable: &mergeable,
			State:     githubv39.String("open"),
			Body:      githubv39.String(f.body),
			User:      &githubv39.User{Login: githubv39.String("bot1")},
			Head:      &githubv39.PullRequestBranch{SHA: githubv39.String(f.headSHA)},
			CreatedAt: &now,
		})

	case fmt.Sprintf("%s/pulls/%d/commits", base, f.num):
		_ = json.NewEncoder(w).Encode([]*githubv39.RepositoryCommit{{
			SHA:    githubv39.String(f.headSHA),
			Commit: &githubv39.Commit{Committer: &githubv39.CommitAuthor{Date: &now}},
		}})

	case fmt.Sprintf("%s/issues/%d/comments", base, f.num):
		_ = json.NewEncoder(w).Encode([]*githubv39.IssueComment{})

	case fmt.Sprintf("%s/pulls/%d/reviews", base, f.num):
		_ = json.NewEncoder(w).Encode(f.reviews)

	case fmt.Sprintf("%s/pulls/%d/comments", base, f.num):
		_ = json.NewEncoder(w).Encode(f.inline)

	case base + "/commits/" + f.headSHA + "/check-runs":
		runs := []*githubv39.CheckRun{}
		if f.pending {
			runs = append(runs, &githubv39.CheckRun{
				Name:   githubv39.String("build"),
				Status: githubv39.String("in_progress"),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"check_runs": runs})

	case base + "/commits/" + f.headSHA + "/statuses":
		_ = json.NewEncoder(w).Encode([]*githubv39.RepoStatus{})

	case fmt.Sprintf("%s/issues/%d/labels", base, f.num):
		// Label reads and writes both land here, and both answer with a list.
		_ = json.NewEncoder(w).Encode([]*githubv39.Label{})

	default:
		// Referenced issue reads, assignee writes and anything else the
		// evaluation happens to touch.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"number": 7,
			"labels": []interface{}{},
		})
	}
}

// listingIssueLocked is the pull request as the issue listings report it. The
// caller must hold f.mu.
func (f *prFixture) listingIssueLocked() *githubv39.Issue {
	updated := f.updated
	return &githubv39.Issue{
		Number:           githubv39.Int(f.num),
		UpdatedAt:        &updated,
		PullRequestLinks: &githubv39.PullRequestLinks{},
		Assignees:        []*githubv39.User{{Login: githubv39.String("bot1")}},
		Labels:           []*githubv39.Label{{Name: githubv39.String("factory")}},
	}
}

// count returns how many recorded paths contain substr.
func (f *prFixture) count(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			n++
		}
	}
	return n
}

// reset forgets the recorded calls, so the next assertion covers only what
// happened after it.
func (f *prFixture) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *prFixture) set(mutate func(*prFixture)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mutate(f)
}

// newFixtureScanner wires a scanner to the fixture with the settings every test
// in this file shares.
func newFixtureScanner(t *testing.T, f *prFixture) *Scanner {
	t.Helper()
	s, _ := newTestScanner(t, t.TempDir(), testOpts{
		GitHub:       f.start(t),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})
	return s
}

// evaluated reports whether anything was fetched for the pull request itself,
// which is the signal that a full evaluation ran rather than being skipped.
func evaluated(f *prFixture) bool {
	return f.count(fmt.Sprintf("/pulls/%d", f.num)) > 0
}

// TestFastPass_SkipsUnchangedPullRequest is the point of the skip gate: a pull
// request nobody has touched is listed, found unchanged, and left alone.
//
// Before the gate, this second cycle re-fetched the pull request, its commits,
// its conversation, its reviews and its CI to arrive at the verdict it had
// already reached a minute earlier.
func TestFastPass_SkipsUnchangedPullRequest(t *testing.T) {
	f := &prFixture{num: 10, headSHA: "sha-1", updated: time.Now().Add(-time.Hour)}
	s := newFixtureScanner(t, f)
	ctx := context.Background()

	s.fastPass(ctx)
	if !evaluated(f) {
		t.Fatal("the first pass did not evaluate the pull request; the fixture is not wired up")
	}

	f.reset()
	s.fastPass(ctx)

	if evaluated(f) {
		t.Errorf("the second pass re-evaluated an unchanged pull request, want it skipped")
	}
	if f.count("/issues?") == 0 {
		t.Error("the second pass did not list assigned pull requests; the gate must skip the evaluation, not the cycle")
	}
}

// TestFastPass_ReevaluatesWhenUpdatedAtMoves covers the ordinary way a pull
// request becomes interesting again: somebody pushed, commented or labelled it.
func TestFastPass_ReevaluatesWhenUpdatedAtMoves(t *testing.T) {
	f := &prFixture{num: 10, headSHA: "sha-1", updated: time.Now().Add(-time.Hour)}
	s := newFixtureScanner(t, f)
	ctx := context.Background()

	s.fastPass(ctx)
	f.reset()

	f.set(func(f *prFixture) { f.updated = time.Now() })
	s.fastPass(ctx)

	if !evaluated(f) {
		t.Error("a pull request whose updated_at moved was skipped, want it re-evaluated")
	}
}

// TestFastPass_ReevaluatesWhileCIInFlight is the blind spot the gate has to
// cover explicitly: a check run completing does not move the pull request's
// updated_at, so a pull request waiting on CI would otherwise be skipped until
// the next sweep - exactly the pull request the fast pass exists for.
func TestFastPass_ReevaluatesWhileCIInFlight(t *testing.T) {
	f := &prFixture{num: 10, headSHA: "sha-1", updated: time.Now().Add(-time.Hour), pending: true}
	s := newFixtureScanner(t, f)
	ctx := context.Background()

	s.fastPass(ctx)
	f.reset()

	// Nothing about the pull request changed, and nothing about it can: CI
	// finishing is invisible in updated_at.
	s.fastPass(ctx)

	if !evaluated(f) {
		t.Error("a pull request with CI in flight was skipped, want it re-evaluated every cycle")
	}
}

// TestFastPass_ReevaluatesWhenTaskFinishes covers the other invisible
// transition: a task completing is what makes a pull request ready for a human,
// and a task that pushed nothing leaves updated_at untouched.
func TestFastPass_ReevaluatesWhenTaskFinishes(t *testing.T) {
	f := &prFixture{num: 10, headSHA: "sha-1", updated: time.Now().Add(-time.Hour)}

	tempDir := t.TempDir()
	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       f.start(t),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})
	ctx := context.Background()

	// A task is in flight, so the first pass records the pull request as
	// having work against it.
	taskPath := filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml")
	if err := os.WriteFile(taskPath, []byte("type: pr-comments\n"), 0644); err != nil {
		t.Fatalf("writing task file: %v", err)
	}
	s.fastPass(ctx)
	if !evaluated(f) {
		t.Fatal("the first pass did not evaluate the pull request")
	}

	// The task finishes without touching the pull request on GitHub, so
	// updated_at is exactly where the first pass left it.
	if err := os.Remove(taskPath); err != nil {
		t.Fatalf("removing task file: %v", err)
	}

	f.reset()
	s.fastPass(ctx)

	if !evaluated(f) {
		t.Error("a pull request whose task just finished was skipped, want it re-evaluated so the ready-for-human label is reconciled")
	}
}

// TestSweep_EvaluatesRegardlessOfSkipGate pins the safety valve the gate relies
// on. Every signal the gate cannot see - a re-run check, a change upstream - is
// bounded by the sweep, so the sweep must never consult it.
func TestSweep_EvaluatesRegardlessOfSkipGate(t *testing.T) {
	f := &prFixture{num: 10, headSHA: "sha-1", updated: time.Now().Add(-time.Hour)}
	s := newFixtureScanner(t, f)
	ctx := context.Background()

	// A fast pass first, so the pull request is on record as evaluated and a
	// gated cycle would skip it.
	s.fastPass(ctx)
	f.reset()

	s.sweep(ctx)

	if !evaluated(f) {
		t.Error("the sweep skipped an unchanged pull request; it must evaluate everything unconditionally")
	}
}

// TestFetchHistory_ReadsInlineCommentsInOneRequest covers the change from the
// per-review endpoint to the pull request-wide one: the cost of reading a
// conversation must not grow with the number of reviews it contains.
func TestFetchHistory_ReadsInlineCommentsInOneRequest(t *testing.T) {
	f := &prFixture{
		num:     10,
		headSHA: "sha-1",
		reviews: []*githubv39.PullRequestReview{
			{ID: githubv39.Int64(1)},
			{ID: githubv39.Int64(2)},
			{ID: githubv39.Int64(3)},
		},
		inline: []*githubv39.PullRequestComment{
			{ID: githubv39.Int64(11), PullRequestReviewID: githubv39.Int64(1), Body: githubv39.String("a")},
			{ID: githubv39.Int64(12), PullRequestReviewID: githubv39.Int64(1), Body: githubv39.String("b")},
			{ID: githubv39.Int64(13), PullRequestReviewID: githubv39.Int64(3), Body: githubv39.String("c")},
			// An inline comment with no review behind it, which must not end
			// up filed under review zero.
			{ID: githubv39.Int64(14), Body: githubv39.String("orphan")},
		},
	}
	s := newFixtureScanner(t, f)

	history, err := s.fetchHistory(context.Background(), 10)
	if err != nil {
		t.Fatalf("fetchHistory() error = %v", err)
	}

	if got := f.count("/pulls/10/comments"); got != 1 {
		t.Errorf("inline comment requests = %d, want 1 for the whole pull request", got)
	}
	if got := f.count("/reviews/"); got != 0 {
		t.Errorf("made %d per-review requests, want 0", got)
	}

	// The grouping the per-review fetch used to provide has to survive the
	// change, or every consumer that looks a review up by ID silently sees
	// nothing.
	if got := len(history.revCommentsMap[1]); got != 2 {
		t.Errorf("review 1 has %d inline comments, want 2", got)
	}
	if got := len(history.revCommentsMap[3]); got != 1 {
		t.Errorf("review 3 has %d inline comments, want 1", got)
	}
	if _, ok := history.revCommentsMap[2]; ok {
		t.Error("review 2 has no inline comments and must not appear in the map")
	}
	if _, ok := history.revCommentsMap[0]; ok {
		t.Error("the review-less comment was filed under review 0, want it dropped")
	}
}

// TestEvaluate_FetchesReferencedIssueOnce covers the memoisation: the label
// sync, the review opt-in check and the readiness check all want the issues the
// pull request closes, and they used to fetch them one after another.
func TestEvaluate_FetchesReferencedIssueOnce(t *testing.T) {
	f := &prFixture{
		num:     10,
		headSHA: "sha-1",
		body:    "Fixes #7",
		updated: time.Now().Add(-time.Hour),
	}
	s := newFixtureScanner(t, f)

	updated := time.Now()
	s.evaluate(context.Background(), &githubv39.Issue{
		Number:           githubv39.Int(10),
		UpdatedAt:        &updated,
		PullRequestLinks: &githubv39.PullRequestLinks{},
		Labels:           []*githubv39.Label{{Name: githubv39.String("factory")}},
	})

	if got := f.count("/issues/7"); got != 1 {
		t.Errorf("fetched referenced issue #7 %d times in one evaluation, want 1", got)
	}
}

package pipelines

import (
	"context"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/pipeline"
)

// listGitHub answers the controller's listings; every other call fails, so
// a pass over a parent errors out harmlessly after listing.
type listGitHub struct {
	pipeline.GitHub
	open, closed []github.IssueRef
	calls        []github.IssueRefListOptions
}

func (g *listGitHub) ListIssueRefs(_ context.Context, opts github.IssueRefListOptions) ([]github.IssueRef, error) {
	g.calls = append(g.calls, opts)
	switch {
	case opts.Creator != "":
		return nil, nil
	case opts.State == "open" && opts.Labels[0] == "overseer/pipeline":
		return g.open, nil
	case opts.State == "closed" && opts.Labels[0] == "overseer/pipeline":
		return g.closed, nil
	}
	return nil, nil
}

func (g *listGitHub) RepoPermissions(context.Context) (map[string]bool, error) {
	return map[string]bool{"triage": true}, nil
}

func TestListParents(t *testing.T) {
	gh := &listGitHub{
		open:   []github.IssueRef{{Number: 1}, {Number: 2, IsPR: true}},
		closed: []github.IssueRef{{Number: 3}, {Number: 1}},
	}
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	c := New(Config{TriggerLabel: "factory"}, Deps{GitHub: gh, Now: func() time.Time { return now }})
	got, err := c.listParents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Errorf("parents = %v, want [1 3]", got)
	}
	var sawSince bool
	for _, call := range gh.calls {
		if call.State == "closed" && call.Since == "2026-10-19T12:00:00Z" {
			sawSince = true
		}
	}
	if !sawSince {
		t.Errorf("closed parents not limited to the lookback: %+v", gh.calls)
	}
}

func TestNudgeWakesParent(t *testing.T) {
	c := New(Config{}, Deps{GitHub: &listGitHub{}})
	m := pipeline.Marker{Parent: 42, Pipeline: "demo", Step: "one", Params: "0123456789abcdef"}
	c.NudgeLinkedWorkflows(context.Background(), &githubv39.Issue{Number: githubv39.Int(7), Body: githubv39.String("x\n" + m.String())})
	c.NudgeLinkedWorkflows(context.Background(), &githubv39.Issue{Number: githubv39.Int(8), Body: githubv39.String("no marker")})
	select {
	case n := <-c.wake:
		if n != 42 {
			t.Errorf("woke #%d, want #42", n)
		}
	default:
		t.Fatal("no wake")
	}
	select {
	case n := <-c.wake:
		t.Errorf("unexpected wake for #%d", n)
	default:
	}

	var nilController *Controller
	nilController.NudgeLinkedWorkflows(context.Background(), &githubv39.Issue{})
}

func TestPausedSkipsPass(t *testing.T) {
	gh := &listGitHub{open: []github.IssueRef{{Number: 1}}}
	c := New(Config{}, Deps{GitHub: gh, Paused: func() bool { return true }})
	c.SyncOnce(context.Background())
	if len(gh.calls) != 0 {
		t.Errorf("a draining watcher listed parents: %+v", gh.calls)
	}
}

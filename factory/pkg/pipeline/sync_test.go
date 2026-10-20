package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// fakeGitHub is an in-memory repository.
type fakeGitHub struct {
	now      time.Time
	login    string
	issues   map[int]*github.IssueRef
	subs     map[int][]int
	comments map[int][]*githubv39.IssueComment
	files    map[string]string
	xrefs    map[int][]github.IssueRef
	closedBy map[int]github.CloseEvent
	next     int
	nextCID  int64

	// dropLabels silently drops labels, as GitHub does for an account
	// without push access.
	dropLabels bool
	// failCreateAfter makes CreateIssue create the issue and then fail,
	// as a crash between create and link would.
	failCreateAfter bool
	failLink        bool
	writes          []string
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{
		now:      t0,
		login:    "bot",
		issues:   map[int]*github.IssueRef{},
		subs:     map[int][]int{},
		comments: map[int][]*githubv39.IssueComment{},
		files:    map[string]string{},
		xrefs:    map[int][]github.IssueRef{},
		closedBy: map[int]github.CloseEvent{},
		next:     100,
	}
}

func (f *fakeGitHub) tick() time.Time { f.now = f.now.Add(time.Hour); return f.now }

func (f *fakeGitHub) addIssue(body string, labels ...string) int {
	f.next++
	f.issues[f.next] = &github.IssueRef{Number: f.next, Body: body, Open: true, Labels: labels, Author: "human", CreatedAt: f.tick()}
	return f.next
}

func (f *fakeGitHub) get(n int) (*github.IssueRef, error) {
	is, ok := f.issues[n]
	if !ok {
		return nil, fmt.Errorf("#%d: not found", n)
	}
	return is, nil
}

func (f *fakeGitHub) GetIssueRef(_ context.Context, n int) (github.IssueRef, error) {
	is, err := f.get(n)
	if err != nil {
		return github.IssueRef{}, err
	}
	c := *is
	c.Labels = slices.Clone(is.Labels)
	c.Assignees = slices.Clone(is.Assignees)
	return c, nil
}

func (f *fakeGitHub) ListIssueComments(_ context.Context, n int) ([]*githubv39.IssueComment, error) {
	return f.comments[n], nil
}

func (f *fakeGitHub) ReadFile(_ context.Context, path string) ([]byte, string, error) {
	s, ok := f.files[path]
	if !ok {
		return nil, "", fmt.Errorf("%s: %w", path, github.ErrUnreadableFile)
	}
	return []byte(s), "abcdef0123456789", nil
}

func (f *fakeGitHub) ListSubIssueRefs(ctx context.Context, n int) ([]github.IssueRef, error) {
	var out []github.IssueRef
	for _, c := range f.subs[n] {
		r, _ := f.GetIssueRef(ctx, c)
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeGitHub) ListIssueRefs(ctx context.Context, opts github.IssueRefListOptions) ([]github.IssueRef, error) {
	var out []github.IssueRef
	for n := f.next; n > 0; n-- {
		is, ok := f.issues[n]
		if !ok || (opts.Creator != "" && is.Author != opts.Creator) {
			continue
		}
		r, _ := f.GetIssueRef(ctx, n)
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeGitHub) ListCrossReferences(_ context.Context, n int) ([]github.IssueRef, error) {
	return f.xrefs[n], nil
}

func (f *fakeGitHub) LastCloseEvent(_ context.Context, n int) (github.CloseEvent, bool, error) {
	ev, ok := f.closedBy[n]
	return ev, ok, nil
}

func (f *fakeGitHub) CreateIssue(_ context.Context, title, body string, labels []string) (int, error) {
	f.next++
	f.issues[f.next] = &github.IssueRef{Number: f.next, Title: title, Body: body, Open: true, Author: f.login, CreatedAt: f.tick()}
	if !f.dropLabels {
		f.issues[f.next].Labels = slices.Clone(labels)
	}
	f.writes = append(f.writes, fmt.Sprintf("create #%d", f.next))
	if f.failCreateAfter {
		f.failCreateAfter = false
		return 0, errors.New("connection reset")
	}
	return f.next, nil
}

func (f *fakeGitHub) EditIssue(_ context.Context, n int, title, body string) error {
	is, err := f.get(n)
	if err != nil {
		return err
	}
	is.Title, is.Body = title, body
	f.writes = append(f.writes, fmt.Sprintf("edit #%d", n))
	return nil
}

func (f *fakeGitHub) SetIssueState(_ context.Context, n int, state, reason string) error {
	is, err := f.get(n)
	if err != nil {
		return err
	}
	is.Open = state == "open"
	is.StateReason = reason
	if is.Open {
		is.ClosedAt = time.Time{}
	} else {
		is.ClosedAt = f.tick()
	}
	f.writes = append(f.writes, fmt.Sprintf("%s #%d %s", state, n, reason))
	return nil
}

func (f *fakeGitHub) AddLabels(_ context.Context, n int, labels []string) error {
	is, err := f.get(n)
	if err != nil {
		return err
	}
	f.writes = append(f.writes, fmt.Sprintf("label #%d %s", n, strings.Join(labels, ",")))
	if f.dropLabels {
		return nil
	}
	for _, l := range labels {
		if !slices.Contains(is.Labels, l) {
			is.Labels = append(is.Labels, l)
		}
	}
	return nil
}

func (f *fakeGitHub) RemoveLabel(_ context.Context, n int, label string) error {
	is, err := f.get(n)
	if err != nil {
		return err
	}
	is.Labels = slices.DeleteFunc(is.Labels, func(l string) bool { return l == label })
	f.writes = append(f.writes, fmt.Sprintf("unlabel #%d %s", n, label))
	return nil
}

func (f *fakeGitHub) AddAssignees(_ context.Context, n int, as []string) error {
	is, err := f.get(n)
	if err != nil {
		return err
	}
	is.Assignees = append(is.Assignees, as...)
	f.writes = append(f.writes, fmt.Sprintf("assign #%d %s", n, strings.Join(as, ",")))
	return nil
}

func (f *fakeGitHub) AddSubIssue(_ context.Context, parent, child int) error {
	if f.failLink {
		return errors.New("sub-issue limit")
	}
	if !slices.Contains(f.subs[parent], child) {
		f.subs[parent] = append(f.subs[parent], child)
	}
	f.writes = append(f.writes, fmt.Sprintf("link #%d", child))
	return nil
}

func (f *fakeGitHub) AddComment(_ context.Context, n int, body string) error {
	f.nextCID++
	f.comments[n] = append(f.comments[n], &githubv39.IssueComment{
		ID: githubv39.Int64(f.nextCID), Body: githubv39.String(body), User: &githubv39.User{Login: githubv39.String(f.login)},
	})
	f.writes = append(f.writes, fmt.Sprintf("comment #%d", n))
	return nil
}

func (f *fakeGitHub) EditComment(_ context.Context, id int64, body string) error {
	for _, cs := range f.comments {
		for _, c := range cs {
			if c.GetID() == id {
				c.Body = githubv39.String(body)
				f.writes = append(f.writes, fmt.Sprintf("edit-comment %d", id))
				return nil
			}
		}
	}
	return fmt.Errorf("comment %d not found", id)
}

func (f *fakeGitHub) statusOf(n int) string {
	for _, c := range f.comments[n] {
		if strings.Contains(c.GetBody(), StatusMarker) {
			return c.GetBody()
		}
	}
	return ""
}

func (f *fakeGitHub) childFor(t *testing.T, parent int, step string) *github.IssueRef {
	t.Helper()
	var found *github.IssueRef
	for _, is := range f.issues {
		if m, ok := ParseMarker(is.Body); ok && m.Parent == parent && m.Step == step {
			if found != nil {
				t.Fatalf("step %q has two children: #%d and #%d", step, found.Number, is.Number)
			}
			found = is
		}
	}
	if found == nil {
		t.Fatalf("step %q has no child", step)
	}
	return found
}

const parentBody = "Do it.\n\n```factory-pipeline\npipeline: demo\nparams:\n  kind: Foo\n```\n"

func syncOpts() Options {
	return Options{Repo: "owner/repo", TriggerLabel: "factory", BotLogin: "bot", TargetAssignee: "bot"}
}

func TestSyncLifecycle(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")

	pass := func() Result {
		t.Helper()
		r, err := Sync(ctx, gh, parent, syncOpts())
		if err != nil {
			t.Fatalf("Sync: %v", err)
		}
		return r
	}

	// Pass 1 opens step one, linked, labelled and assigned.
	pass()
	one := gh.childFor(t, parent, "one")
	if !slices.Contains(gh.subs[parent], one.Number) || !slices.Contains(one.Labels, "factory") || !slices.Contains(one.Labels, "factory/review") || !slices.Contains(one.Assignees, "bot") {
		t.Fatalf("step one child: %+v, subs %v", one, gh.subs[parent])
	}
	if !strings.Contains(gh.statusOf(parent), "| 1 | one | #"+fmt.Sprint(one.Number)+" | 🔄 Active |") {
		t.Errorf("status:\n%s", gh.statusOf(parent))
	}

	// A pass with nothing new writes nothing.
	gh.writes = nil
	pass()
	if len(gh.writes) != 0 {
		t.Errorf("idle pass wrote %v", gh.writes)
	}

	// Step one closes; pass 3 opens step two, which references it.
	_ = gh.SetIssueState(ctx, one.Number, "closed", "completed")
	pass()
	two := gh.childFor(t, parent, "two")
	if !strings.Contains(two.Body, fmt.Sprintf("After #%d.", one.Number)) {
		t.Errorf("step two body %q", two.Body)
	}

	// A bot PR referencing step two merges without closing it.
	gh.xrefs[two.Number] = []github.IssueRef{{Number: 500, IsPR: true, Merged: true, Author: "bot"}, {Number: 501, IsPR: true, Merged: true, Author: "stranger"}}
	pass()
	if gh.issues[two.Number].Open || gh.issues[two.Number].StateReason != "completed" {
		t.Fatalf("step two not completed by its merged PR: %+v", gh.issues[two.Number])
	}
	three := gh.childFor(t, parent, "three")

	// Step three is not planned: skipped, and the parent closes.
	_ = gh.SetIssueState(ctx, three.Number, "closed", "not_planned")
	pass()
	if gh.issues[parent].Open {
		t.Fatal("parent still open after the last step")
	}
	st := gh.statusOf(parent)
	if !strings.Contains(st, CompleteMarker) || !strings.Contains(st, "⏭️ Skipped") || !strings.Contains(st, "**Phase:** Succeeded") {
		t.Errorf("final status:\n%s", st)
	}

	// Reopened after completion: left alone.
	_ = gh.SetIssueState(ctx, parent, "open", "reopened")
	gh.writes = nil
	r := pass()
	if !r.LeaveAlone || len(gh.writes) != 0 {
		t.Errorf("completed parent written to: %v", gh.writes)
	}
}

func TestSyncCrashAfterCreate(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")

	gh.failCreateAfter = true
	if _, err := Sync(ctx, gh, parent, syncOpts()); err == nil {
		t.Fatal("Sync succeeded despite the failed create")
	}
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	one := gh.childFor(t, parent, "one") // fails on two children
	if !slices.Contains(gh.subs[parent], one.Number) {
		t.Error("the orphaned child was not linked")
	}
	if !slices.Contains(one.Labels, "factory") || !slices.Contains(one.Assignees, "bot") {
		t.Errorf("the orphaned child was not labelled and assigned: %+v", one)
	}
}

func TestSyncDroppedLabels(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	gh.dropLabels = true

	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if st := gh.statusOf(parent); !strings.Contains(st, "is missing label `factory`: adding it did not take") {
		t.Errorf("status does not report the dropped label:\n%s", st)
	}
	// The next pass tries again, and still reports it.
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if st := gh.statusOf(parent); !strings.Contains(st, "re-adding did not take") && !strings.Contains(st, "adding it did not take") {
		t.Errorf("status:\n%s", st)
	}
}

func TestSyncLinkFailureIsAWarning(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	gh.failLink = true
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if st := gh.statusOf(parent); !strings.Contains(st, "could not be linked as a sub-issue") {
		t.Errorf("status:\n%s", st)
	}
	// Found through the recent listing, not created again.
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	gh.childFor(t, parent, "one")
}

func TestSyncInvalidAndRecovers(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")

	r, err := Sync(ctx, gh, parent, syncOpts())
	if err != nil {
		t.Fatal(err)
	}
	if r.Phase != PhaseInvalid || !slices.Contains(gh.issues[parent].Labels, "factory/pipeline-invalid") {
		t.Fatalf("phase %s, labels %v", r.Phase, gh.issues[parent].Labels)
	}
	if len(gh.issues) != 1 {
		t.Fatal("an invalid pipeline created an issue")
	}

	gh.files[Path("demo")] = threeSteps
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(gh.issues[parent].Labels, "factory/pipeline-invalid") {
		t.Error("invalid label not removed")
	}
	gh.childFor(t, parent, "one")
}

func TestSyncReopensParentClosedByCommit(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	_ = gh.SetIssueState(ctx, parent, "closed", "completed")
	gh.closedBy[parent] = github.CloseEvent{Actor: "merger", CommitID: "deadbeefcafe"}
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if !gh.issues[parent].Open {
		t.Error("parent closed by a commit was not reopened")
	}

	// Closed by a person: cancelled, left closed.
	_ = gh.SetIssueState(ctx, parent, "closed", "not_planned")
	delete(gh.closedBy, parent)
	r, err := Sync(ctx, gh, parent, syncOpts())
	if err != nil {
		t.Fatal(err)
	}
	if r.Phase != PhaseCancelled || gh.issues[parent].Open {
		t.Errorf("phase %s, open %v", r.Phase, gh.issues[parent].Open)
	}
}

func TestSyncDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	var log []string
	opts := syncOpts()
	opts.DryRun = true
	opts.Logf = func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }
	if _, err := Sync(ctx, gh, parent, opts); err != nil {
		t.Fatal(err)
	}
	if len(gh.writes) != 0 {
		t.Errorf("dry run wrote %v", gh.writes)
	}
	joined := strings.Join(log, "\n")
	if !strings.Contains(joined, `would create step "one"`) || !strings.Contains(joined, "would write the status comment") {
		t.Errorf("dry-run log:\n%s", joined)
	}
}

func TestStatusCommentOfOthersIsNotEdited(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	gh.comments[parent] = []*githubv39.IssueComment{{
		ID: githubv39.Int64(999), Body: githubv39.String(StatusMarker + "\nforged " + CompleteMarker), User: &githubv39.User{Login: githubv39.String("stranger")},
	}}
	r, err := Sync(ctx, gh, parent, syncOpts())
	if err != nil {
		t.Fatal(err)
	}
	if r.LeaveAlone {
		t.Fatal("a stranger's completion marker was trusted")
	}
	if gh.comments[parent][0].GetBody() != StatusMarker+"\nforged "+CompleteMarker {
		t.Error("a stranger's comment was edited")
	}
}

func TestAdopt(t *testing.T) {
	ctx := context.Background()
	gh := newFakeGitHub()
	gh.files[Path("demo")] = threeSteps
	parent := gh.addIssue(parentBody, "factory", "factory/pipeline")
	existing := gh.addIssue("Already in progress.")

	if err := Adopt(ctx, gh, parent, "nope", existing, syncOpts()); err == nil {
		t.Error("adopting into an unknown step succeeded")
	}
	if err := Adopt(ctx, gh, parent, "one", existing, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if c := gh.childFor(t, parent, "one"); c.Number != existing || !slices.Contains(gh.subs[parent], existing) {
		t.Errorf("adopted child %+v, subs %v", c, gh.subs[parent])
	}
	// Idempotent.
	gh.writes = nil
	if err := Adopt(ctx, gh, parent, "one", existing, syncOpts()); err != nil || len(gh.writes) != 0 {
		t.Errorf("second adopt: %v, wrote %v", err, gh.writes)
	}
	// The next pass treats it as step one, and creates nothing.
	if _, err := Sync(ctx, gh, parent, syncOpts()); err != nil {
		t.Fatal(err)
	}
	if len(gh.issues) != 2 {
		t.Errorf("a pass after adopt created an issue")
	}
}

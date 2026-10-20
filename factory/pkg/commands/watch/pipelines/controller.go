// Package pipelines is the watch daemon's issue pipeline subcontroller
// (design/issue-pipelines.md §7). It keeps every pipeline parent - an issue
// whose body has a factory-pipeline block, which the issue scanner labels
// '<trigger>/pipeline' - moving: each pass observes the parent and its
// children on GitHub, reconciles, and applies the result, opening the next
// step's child once the previous one closes and closing the parent after the
// last.
//
// It owns no workers and never touches the task queue or sandboxes: children
// are ordinary trigger-labelled issues, which the issue scanner picks up.
// Parents are reconciled one after another from a single goroutine, at
// startup, on a periodic resync, and whenever a child is reported closed.
// Nothing it knows has to survive a restart.
package pipelines

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/pipeline"

	githubv39 "github.com/google/go-github/v39/github"
)

// DefaultInterval is the delay between resyncs of every parent. A child
// closing wakes its parent sooner; the resync catches what no wake reports,
// such as a bot PR merged without closing its child, or a stop label removed.
const DefaultInterval = 5 * time.Minute

// DefaultClosedLookback is how far back recently closed parents are
// reconciled too, so a parent closed by a stray "Fixes #N" is reopened.
const DefaultClosedLookback = 24 * time.Hour

// closedPageSize bounds the recently closed parents read per pass.
const closedPageSize = 50

// GitHub is what the controller needs of the repository.
type GitHub interface {
	pipeline.GitHub
	RepoPermissions(ctx context.Context) (map[string]bool, error)
}

// Config holds the tuning knobs of a Controller.
type Config struct {
	// Interval is the delay between resyncs.
	Interval time.Duration
	// ClosedLookback is how long after closing a parent is still
	// reconciled.
	ClosedLookback time.Duration
	// Repo is "owner/repo".
	Repo string
	// TriggerLabel is the label namespace: parents carry
	// '<trigger>/pipeline', and children get the trigger label itself.
	TriggerLabel string
	// GitHubLogin is the account the watcher writes as.
	GitHubLogin string
	// BotLogins are the accounts whose merged PRs complete a child.
	BotLogins []string
	// TargetAssignee is who children are assigned to by default.
	TargetAssignee string
	// MinNumber ignores parents below it, as the scanners do. Zero
	// reconciles all.
	MinNumber int
	// DryRun logs each pass's actions and writes nothing.
	DryRun bool
}

// Deps holds the collaborators of a Controller.
type Deps struct {
	GitHub GitHub
	// Paused reports whether the watcher is draining, in which case no
	// pass runs: opening a child is starting work.
	Paused func() bool
	// Now is time.Now, replaced in tests.
	Now func() time.Time
}

// Controller reconciles the pipeline parents.
type Controller struct {
	cfg    Config
	gh     GitHub
	paused func() bool
	now    func() time.Time
	// wake carries the parents whose child just closed. It is buffered so
	// that whoever reports one never waits on a pass.
	wake chan int
}

// New constructs a Controller.
func New(cfg Config, deps Deps) *Controller {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.ClosedLookback <= 0 {
		cfg.ClosedLookback = DefaultClosedLookback
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &Controller{cfg: cfg, gh: deps.GitHub, paused: deps.Paused, now: now, wake: make(chan int, 64)}
}

// Run reconciles every parent at once, then each interval, and a single
// parent as soon as one of its children is reported closed, until ctx is
// cancelled.
func (c *Controller) Run(ctx context.Context) error {
	c.CheckPermissions(ctx)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	c.SyncOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.SyncOnce(ctx)
		case n := <-c.wake:
			c.syncParent(ctx, n)
		}
	}
}

// CheckPermissions logs the account the reconciler writes as, and warns
// when its writes may be limited (design §6.2).
func (c *Controller) CheckPermissions(ctx context.Context) {
	mode := "writing"
	if c.cfg.DryRun {
		mode = "dry-run"
	}
	klog.Infof("Issue pipelines: %s as %q in %s", mode, c.cfg.GitHubLogin, c.cfg.Repo)
	perms, err := c.gh.RepoPermissions(ctx)
	if err != nil {
		klog.Warningf("Issue pipelines: checking %q's permissions on %s: %v", c.cfg.GitHubLogin, c.cfg.Repo, err)
		return
	}
	switch {
	case perms["push"] || perms["maintain"] || perms["admin"]:
	case perms["triage"]:
		klog.Warningf("Issue pipelines: %q has triage access to %s, not push: labels and assignees set when a child is created are dropped and applied afterwards", c.cfg.GitHubLogin, c.cfg.Repo)
	default:
		klog.Warningf("Issue pipelines: %q has neither push nor triage access to %s: labelling, assigning and closing issues may fail", c.cfg.GitHubLogin, c.cfg.Repo)
	}
}

// SyncOnce reconciles every open pipeline parent, and those closed within
// ClosedLookback.
func (c *Controller) SyncOnce(ctx context.Context) {
	if c.isPaused() {
		klog.V(2).Infof("Skipping issue pipelines because the watcher is draining.")
		return
	}
	parents, err := c.listParents(ctx)
	if err != nil {
		klog.Errorf("Listing pipeline parents: %v", err)
		if len(parents) == 0 {
			return
		}
	}
	// The reconciler's recent issues are read once for every parent.
	recent, err := pipeline.ListRecent(ctx, c.gh, c.cfg.GitHubLogin)
	if err != nil {
		klog.Errorf("Listing %s's recent issues: %v", c.cfg.GitHubLogin, err)
		return
	}
	if recent == nil {
		recent = []github.IssueRef{}
	}
	for _, n := range parents {
		if ctx.Err() != nil {
			return
		}
		c.sync(ctx, n, recent)
	}
}

// NudgeLinkedWorkflows wakes the parent of a pipeline child that just
// closed, so the next step is opened without waiting for the resync (design
// §7.2). Anything else is not a child and is ignored. It never blocks: when
// the wake buffer is full, the resync picks the parent up.
func (c *Controller) NudgeLinkedWorkflows(_ context.Context, closed *githubv39.Issue) {
	if c == nil {
		return
	}
	m, ok := pipeline.ParseMarker(closed.GetBody())
	if !ok {
		return
	}
	select {
	case c.wake <- m.Parent:
		klog.Infof("Pipeline child #%d closed; waking parent #%d", closed.GetNumber(), m.Parent)
	default:
	}
}

// Wake asks for parent n to be reconciled soon.
func (c *Controller) Wake(n int) {
	if c == nil {
		return
	}
	select {
	case c.wake <- n:
	default:
	}
}

func (c *Controller) syncParent(ctx context.Context, n int) {
	if c.isPaused() {
		return
	}
	c.sync(ctx, n, nil)
}

func (c *Controller) sync(ctx context.Context, n int, recent []github.IssueRef) {
	if c.cfg.MinNumber > 0 && n < c.cfg.MinNumber {
		return
	}
	res, err := pipeline.Sync(ctx, c.gh, n, pipeline.Options{
		Repo:           c.cfg.Repo,
		TriggerLabel:   c.cfg.TriggerLabel,
		BotLogin:       c.cfg.GitHubLogin,
		BotLogins:      c.cfg.BotLogins,
		TargetAssignee: c.cfg.TargetAssignee,
		Recent:         recent,
		DryRun:         c.cfg.DryRun,
		Logf: func(format string, args ...any) {
			klog.Infof("pipeline #%d: "+format, append([]any{n}, args...)...)
		},
	})
	if err != nil {
		// Transient: the parent is retried on the next pass.
		klog.Errorf("Pipeline #%d: %v", n, err)
		return
	}
	if res.Phase == pipeline.PhaseSucceeded && res.Complete && !c.cfg.DryRun {
		klog.Infof("Pipeline #%d is done", n)
	}
}

// listParents lists every open issue labelled as a pipeline parent, in each
// of the label's spellings, and those closed within ClosedLookback.
func (c *Controller) listParents(ctx context.Context) ([]int, error) {
	seen := map[int]bool{}
	var parents []int
	add := func(refs []github.IssueRef) {
		for _, r := range refs {
			if !r.IsPR && !seen[r.Number] {
				seen[r.Number] = true
				parents = append(parents, r.Number)
			}
		}
	}
	since := c.now().Add(-c.cfg.ClosedLookback).UTC().Format(time.RFC3339)
	for _, label := range conventions.PipelineLabels(c.cfg.TriggerLabel) {
		for page := 1; page <= maxOpenPages; page++ {
			refs, err := c.gh.ListIssueRefs(ctx, github.IssueRefListOptions{
				Labels: []string{label}, State: "open", Sort: "created", Direction: "asc", PerPage: 100, Page: page,
			})
			if err != nil {
				return parents, err
			}
			add(refs)
			if len(refs) < 100 {
				break
			}
		}
		closed, err := c.gh.ListIssueRefs(ctx, github.IssueRefListOptions{
			Labels: []string{label}, State: "closed", Sort: "updated", Direction: "desc", Since: since, PerPage: closedPageSize,
		})
		if err != nil {
			return parents, err
		}
		add(closed)
	}
	return parents, nil
}

// maxOpenPages bounds the pages of open parents read per label.
const maxOpenPages = 10

func (c *Controller) isPaused() bool {
	return c.paused != nil && c.paused()
}

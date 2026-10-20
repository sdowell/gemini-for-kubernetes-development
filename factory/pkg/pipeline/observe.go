package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// GitHub is what a pass reads and writes. *github.Client has it.
type GitHub interface {
	GetIssueRef(ctx context.Context, number int) (github.IssueRef, error)
	ListIssueComments(ctx context.Context, number int) ([]*githubv39.IssueComment, error)
	// ReadFile returns a file on the default branch and the commit it was
	// read at; github.ErrUnreadableFile when the file is missing or unusable.
	ReadFile(ctx context.Context, path string) ([]byte, string, error)
	ListSubIssueRefs(ctx context.Context, number int) ([]github.IssueRef, error)
	ListIssueRefs(ctx context.Context, opts github.IssueRefListOptions) ([]github.IssueRef, error)
	ListCrossReferences(ctx context.Context, number int) ([]github.IssueRef, error)
	LastCloseEvent(ctx context.Context, number int) (github.CloseEvent, bool, error)

	CreateIssue(ctx context.Context, title, body string, labels []string) (int, error)
	EditIssue(ctx context.Context, number int, title, body string) error
	SetIssueState(ctx context.Context, number int, state, reason string) error
	AddLabels(ctx context.Context, number int, labels []string) error
	RemoveLabel(ctx context.Context, number int, label string) error
	AddAssignees(ctx context.Context, number int, assignees []string) error
	AddSubIssue(ctx context.Context, parent, child int) error
	AddComment(ctx context.Context, number int, body string) error
	EditComment(ctx context.Context, commentID int64, body string) error
}

// RecentPageSize is how many of the reconciler's most recently created
// issues are read to find children created but not yet linked (design §5.2).
const RecentPageSize = 100

// Options are a pass's.
type Options struct {
	// Repo is "owner/repo".
	Repo string
	// TriggerLabel is the deployment's: every child gets it.
	TriggerLabel string
	// BotLogin is the account the reconciler writes as: its status comment
	// is the only one edited, and its recent issues are searched for
	// unlinked children.
	BotLogin string
	// BotLogins are the accounts whose merged PRs complete a child, and
	// whose status comments may record completion. BotLogin always counts.
	BotLogins []string
	// TargetAssignee is who a child is assigned to when its step names
	// nobody.
	TargetAssignee string
	// Recent, when set, are the reconciler's recently created issues,
	// already listed for every parent in a pass; nil lists them.
	Recent []github.IssueRef
	// DryRun reads and decides, and writes nothing.
	DryRun bool
	// Logf, when set, is told what the pass does (or would do).
	Logf func(format string, args ...any)
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

func (o Options) isBot(login string) bool {
	if login == "" {
		return false
	}
	if strings.EqualFold(login, o.BotLogin) {
		return true
	}
	return slices.ContainsFunc(o.BotLogins, func(b string) bool { return strings.EqualFold(b, login) })
}

// Observation is what Observe read: Reconcile's input, and the status
// comment to edit.
type Observation struct {
	Input Input
	// StatusComment is the reconciler's own status comment, nil when it
	// has not posted one.
	StatusComment *githubv39.IssueComment
}

// ListRecent lists the bot's most recently created issues, where children
// created but not yet linked to their parent are found. It reads the issue
// listing, not the eventually consistent search index.
func ListRecent(ctx context.Context, gh GitHub, botLogin string) ([]github.IssueRef, error) {
	if botLogin == "" {
		return nil, nil
	}
	return gh.ListIssueRefs(ctx, github.IssueRefListOptions{
		Creator: botLogin, State: "all", Sort: "created", Direction: "desc", PerPage: RecentPageSize,
	})
}

// Observe reads everything Reconcile needs about parent from GitHub. An
// error is transient (GitHub's, not the pipeline's): the parent is retried
// on the next pass. A missing or invalid definition is not an error; it is
// in the input, for Reconcile to report.
func Observe(ctx context.Context, gh GitHub, parent int, opts Options) (Observation, error) {
	ref, err := gh.GetIssueRef(ctx, parent)
	if err != nil {
		return Observation{}, err
	}
	if ref.IsPR {
		return Observation{}, fmt.Errorf("#%d is a pull request, not an issue", parent)
	}
	in := Input{
		Repo:           opts.Repo,
		Parent:         issueOf(ref),
		TriggerLabel:   opts.TriggerLabel,
		TargetAssignee: opts.TargetAssignee,
	}
	in.Stopped = slices.ContainsFunc(conventions.StopLabels(opts.TriggerLabel), in.Parent.hasLabel)

	comments, err := gh.ListIssueComments(ctx, parent)
	if err != nil {
		return Observation{}, fmt.Errorf("listing the comments of #%d: %w", parent, err)
	}
	var obs Observation
	for _, c := range comments {
		body := c.GetBody()
		if !strings.Contains(body, StatusMarker) || !opts.isBot(c.GetUser().GetLogin()) {
			continue
		}
		if strings.Contains(body, CompleteMarker) {
			in.Completed = true
		}
		if obs.StatusComment == nil && strings.EqualFold(c.GetUser().GetLogin(), opts.BotLogin) {
			obs.StatusComment = c
		}
	}
	if in.Completed {
		obs.Input = in
		return obs, nil
	}

	if !in.Parent.Open {
		ev, ok, err := gh.LastCloseEvent(ctx, parent)
		if err != nil {
			return Observation{}, err
		}
		if ok {
			in.ParentClosedByCommit = ev.CommitID
		}
	}

	in.Request, in.RequestErr = ParseRequest(in.Parent.Body)
	if in.Request != nil {
		path := Path(in.Request.Pipeline)
		data, commit, err := gh.ReadFile(ctx, path)
		switch {
		case errors.Is(err, github.ErrUnreadableFile):
			in.DefinitionErr = err
		case err != nil:
			return Observation{}, fmt.Errorf("reading %s: %w", path, err)
		default:
			in.DefinitionRef = fmt.Sprintf("%s@%s", path, shortSHA(commit))
			in.Definition, in.DefinitionErr = Parse(data)
		}
	}

	subs, err := gh.ListSubIssueRefs(ctx, parent)
	if err != nil {
		return Observation{}, err
	}
	seen := map[int]bool{}
	for _, s := range subs {
		if s.IsPR {
			continue
		}
		if m, ok := ParseMarker(s.Body); ok && m.Parent == parent {
			seen[s.Number] = true
			in.Children = append(in.Children, Child{Issue: issueOf(s), Marker: m, Linked: true})
			continue
		}
		in.Unmanaged = append(in.Unmanaged, s.Number)
	}

	recent := opts.Recent
	if recent == nil {
		if recent, err = ListRecent(ctx, gh, opts.BotLogin); err != nil {
			return Observation{}, err
		}
	}
	for _, s := range recent {
		if s.IsPR || seen[s.Number] || !strings.EqualFold(s.Author, opts.BotLogin) {
			continue
		}
		if m, ok := ParseMarker(s.Body); ok && m.Parent == parent {
			seen[s.Number] = true
			in.Children = append(in.Children, Child{Issue: issueOf(s), Marker: m})
		}
	}

	// A merged bot PR completes an open child (design §6.3). Only worth
	// reading while the parent is running.
	if in.Parent.Open && !in.Stopped {
		for i := range in.Children {
			c := &in.Children[i]
			if !c.Open {
				continue
			}
			refs, err := gh.ListCrossReferences(ctx, c.Number)
			if err != nil {
				return Observation{}, err
			}
			for _, r := range refs {
				if r.IsPR && r.Merged && opts.isBot(r.Author) {
					c.MergedPRs = append(c.MergedPRs, r.Number)
				}
			}
		}
	}

	obs.Input = in
	return obs, nil
}

func issueOf(r github.IssueRef) Issue {
	return Issue{
		Number:      r.Number,
		Title:       r.Title,
		Body:        r.Body,
		Open:        r.Open,
		StateReason: r.StateReason,
		Labels:      slices.Clone(r.Labels),
		Assignees:   slices.Clone(r.Assignees),
		Author:      r.Author,
		CreatedAt:   r.CreatedAt,
		ClosedAt:    r.ClosedAt,
	}
}

package pipeline

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// Sync runs one pass over a parent: observe, reconcile, and apply (or, in
// dry-run, log what would be applied and the status that would be written).
func Sync(ctx context.Context, gh GitHub, parent int, opts Options) (Result, error) {
	obs, err := Observe(ctx, gh, parent, opts)
	if err != nil {
		return Result{}, err
	}
	r := Reconcile(obs.Input)
	if r.LeaveAlone {
		opts.logf("phase %s: %s; nothing to do", r.Phase, r.Reason)
		return r, nil
	}
	opts.logf("phase %s%s", r.Phase, reasonNote(r.Reason))
	if opts.DryRun {
		if len(r.Actions) == 0 {
			opts.logf("would change no issue")
		}
		for _, a := range r.Actions {
			opts.logf("would %s", a)
		}
		if obs.StatusComment != nil && SameText(obs.StatusComment.GetBody(), r.Status) {
			opts.logf("would leave the status comment as it is:\n%s", r.Status)
		} else {
			opts.logf("would write the status comment:\n%s", r.Status)
		}
		return r, nil
	}
	return r, Apply(ctx, gh, obs, &r, opts)
}

func reasonNote(reason string) string {
	if reason == "" {
		return ""
	}
	return ": " + oneLine(reason)
}

// Apply carries out a result's actions in order (design §6.2), then writes
// the status comment if its content changed, then closes the parent if the
// result completes it. Every action is safe to repeat: a failure ends the
// pass, and the next pass observes again and carries on. Writes GitHub may
// silently drop - labels and assignees - are checked, and those that did not
// take are reported in the status comment rather than failing the pass.
func Apply(ctx context.Context, gh GitHub, obs Observation, r *Result, opts Options) error {
	if r.LeaveAlone {
		return nil
	}
	in := obs.Input
	parent := in.Parent.Number
	var warnings []string
	warn := func(format string, args ...any) {
		w := fmt.Sprintf(format, args...)
		opts.logf("warning: %s", w)
		warnings = append(warnings, w)
	}
	closeParent := false

	for _, a := range r.Actions {
		if a.Kind == ActionCloseParent {
			closeParent = true
			continue
		}
		opts.logf("%s", a)
		switch a.Kind {
		case ActionReopenParent:
			if err := gh.SetIssueState(ctx, parent, "open", "reopened"); err != nil {
				return err
			}
			if err := gh.AddComment(ctx, parent, a.Comment); err != nil {
				return err
			}

		case ActionLabelParent:
			if err := gh.AddLabels(ctx, parent, a.Labels); err != nil {
				return err
			}

		case ActionUnlabelParent:
			for _, l := range a.Labels {
				if err := gh.RemoveLabel(ctx, parent, l); err != nil && !github.IsNotFound(err) {
					return err
				}
			}

		case ActionLinkSubIssue:
			if err := gh.AddSubIssue(ctx, parent, a.Issue); err != nil {
				warn("#%d could not be linked as a sub-issue: %v", a.Issue, err)
			}

		case ActionCloseDuplicate:
			if err := gh.AddComment(ctx, a.Issue, a.Comment); err != nil {
				return err
			}
			if err := gh.SetIssueState(ctx, a.Issue, "closed", "duplicate"); err != nil {
				// Older GitHub versions know no "duplicate" reason.
				if err2 := gh.SetIssueState(ctx, a.Issue, "closed", "not_planned"); err2 != nil {
					return err
				}
			}

		case ActionAddLabels:
			if err := gh.AddLabels(ctx, a.Issue, a.Labels); err != nil {
				for _, l := range a.Labels {
					warn("#%d is missing label `%s`: re-adding failed (%v)", a.Issue, l, err)
				}
				continue
			}
			_, w := verify(ctx, gh, a.Issue, a.Labels, nil, opts)
			warnings = append(warnings, w...)

		case ActionAddAssignees:
			if err := gh.AddAssignees(ctx, a.Issue, a.Assignees); err != nil {
				warn("#%d is assigned to nobody: assigning %s failed (%v)", a.Issue, strings.Join(a.Assignees, ", "), err)
				continue
			}
			_, w := verify(ctx, gh, a.Issue, nil, a.Assignees, opts)
			warnings = append(warnings, w...)

		case ActionCompleteChild:
			if err := gh.AddComment(ctx, a.Issue, a.Comment); err != nil {
				return err
			}
			if err := gh.SetIssueState(ctx, a.Issue, "closed", "completed"); err != nil {
				return err
			}

		case ActionCreateChild:
			n, err := gh.CreateIssue(ctx, a.Title, a.Body, a.Labels)
			if err != nil {
				return err
			}
			opts.logf("created #%d for step %q", n, a.Step)
			if err := gh.AddSubIssue(ctx, parent, n); err != nil {
				warn("#%d could not be linked as a sub-issue: %v", n, err)
			}
			// Labels and assignees set at creation are silently dropped
			// for an account without push access, so they are applied
			// again, and checked.
			if len(a.Labels) > 0 {
				if err := gh.AddLabels(ctx, n, a.Labels); err != nil {
					warn("#%d: adding labels %s failed (%v)", n, strings.Join(a.Labels, ", "), err)
				}
			}
			if len(a.Assignees) > 0 {
				if err := gh.AddAssignees(ctx, n, a.Assignees); err != nil {
					warn("#%d: assigning %s failed (%v)", n, strings.Join(a.Assignees, ", "), err)
				}
			}
			ref, w := verify(ctx, gh, n, a.Labels, a.Assignees, opts)
			warnings = append(warnings, w...)
			// The status shows the new child as the next pass will.
			for i := range r.Steps {
				if r.Steps[i].ID == a.Step {
					r.Steps[i].Issue = n
					r.Steps[i].Started = ref.CreatedAt
				}
			}

		default:
			return fmt.Errorf("unknown action %q", a.Kind)
		}
	}

	status := RenderStatus(in, *r, warnings)
	r.Status = status
	switch {
	case obs.StatusComment == nil:
		opts.logf("post the status comment")
		if err := gh.AddComment(ctx, parent, status); err != nil {
			return err
		}
	case !SameText(obs.StatusComment.GetBody(), status):
		opts.logf("update the status comment")
		if err := gh.EditComment(ctx, obs.StatusComment.GetID(), status); err != nil {
			return err
		}
	}

	if closeParent {
		opts.logf("close #%d as completed", parent)
		if err := gh.SetIssueState(ctx, parent, "closed", "completed"); err != nil {
			return err
		}
	}
	return nil
}

// verify re-reads an issue and reports the labels and assignees that did not
// take.
func verify(ctx context.Context, gh GitHub, n int, labels, assignees []string, opts Options) (github.IssueRef, []string) {
	ref, err := gh.GetIssueRef(ctx, n)
	if err != nil {
		w := fmt.Sprintf("#%d: could not check its labels and assignees (%v)", n, err)
		opts.logf("warning: %s", w)
		return github.IssueRef{}, []string{w}
	}
	got := issueOf(ref)
	var out []string
	for _, l := range missingLabels(got, labels) {
		out = append(out, fmt.Sprintf("#%d is missing label `%s`: adding it did not take (does the account have triage access?)", n, l))
	}
	if len(assignees) > 0 && !slices.ContainsFunc(got.Assignees, func(a string) bool {
		return slices.ContainsFunc(assignees, func(w string) bool { return strings.EqualFold(a, w) })
	}) {
		out = append(out, fmt.Sprintf("#%d is not assigned to %s: assigning did not take (is it a collaborator?)", n, strings.Join(assignees, ", ")))
	}
	for _, w := range out {
		opts.logf("warning: %s", w)
	}
	return ref, out
}

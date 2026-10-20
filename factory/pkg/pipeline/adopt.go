package pipeline

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// Adopt binds an existing issue to a step of a parent's pipeline (design
// §9): it adds the step's marker to the issue's body and links it as a
// sub-issue of the parent. Existing issues are bound only this way, by an
// operator, never from an issue body.
func Adopt(ctx context.Context, gh GitHub, parent int, stepID string, issue int, opts Options) error {
	obs, err := Observe(ctx, gh, parent, opts)
	if err != nil {
		return err
	}
	in := obs.Input
	if _, err := resolve(in); err != nil {
		return fmt.Errorf("#%d's pipeline is invalid: %w", parent, err)
	}
	if _, _, ok := in.Definition.Step(stepID); !ok {
		var ids []string
		for _, s := range in.Definition.Spec.Steps {
			ids = append(ids, s.ID)
		}
		return fmt.Errorf("pipeline %q has no step %q (steps: %s)", in.Request.Pipeline, stepID, strings.Join(ids, ", "))
	}
	if issue == parent {
		return fmt.Errorf("#%d is the parent", issue)
	}

	ref, err := gh.GetIssueRef(ctx, issue)
	if err != nil {
		return err
	}
	if ref.IsPR {
		return fmt.Errorf("#%d is a pull request, not an issue", issue)
	}
	if HasRequest(ref.Body) {
		return fmt.Errorf("#%d has a factory-pipeline block of its own", issue)
	}
	want := Marker{Parent: parent, Pipeline: in.Request.Pipeline, Step: stepID, Params: ParamsHash(in.Request.Params)}
	marked := false
	if m, ok := ParseMarker(ref.Body); ok {
		if m != want {
			return fmt.Errorf("#%d is already marked for step %q of #%d's pipeline %q", issue, m.Step, m.Parent, m.Pipeline)
		}
		marked = true
	}
	for _, c := range in.Children {
		if c.Marker.Step == stepID && c.Number != issue && c.Open {
			return fmt.Errorf("step %q already has #%d open; close it first", stepID, c.Number)
		}
	}
	linked := slices.ContainsFunc(in.Children, func(c Child) bool { return c.Number == issue && c.Linked }) ||
		slices.Contains(in.Unmanaged, issue)

	verb := ""
	if opts.DryRun {
		verb = "would "
	}
	if !marked {
		opts.logf("%sadd the marker for step %q to #%d", verb, stepID, issue)
		if !opts.DryRun {
			body := strings.TrimRight(strings.ReplaceAll(ref.Body, "\r\n", "\n"), " \t\n")
			if err := gh.EditIssue(ctx, issue, ref.Title, childBody(body, want)); err != nil {
				return err
			}
		}
	}
	if !linked {
		opts.logf("%slink #%d as a sub-issue of #%d", verb, issue, parent)
		if !opts.DryRun {
			if err := gh.AddSubIssue(ctx, parent, issue); err != nil {
				return err
			}
		}
	}
	if marked && linked {
		opts.logf("#%d is already step %q of #%d", issue, stepID, parent)
	}
	return nil
}

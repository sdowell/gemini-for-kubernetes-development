package issues

import (
	"context"
	"fmt"
	"strings"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// scanLabelled lists every open issue carrying the trigger label, following
// pagination. It is the authoritative view of the work the repository has asked
// for, and the expensive half of scanning, which is why it runs on the sweep
// interval rather than on every cycle.
//
// Pull requests are dropped: GitHub's issue endpoints return them alongside
// issues, and they belong to the pull request scanner.
//
// Nothing is returned alongside an error. What was paged in before the failure
// is a prefix of the labelled set, and the caller publishes this list as the
// complete picture of the open issues - a truncated one would have the sandbox
// reconciler garbage collect the issues that never got listed.
func (s *Scanner) scanLabelled(ctx context.Context) ([]*githubv39.Issue, error) {
	var labelled []*githubv39.Issue
	opts := &githubv39.IssueListByRepoOptions{
		Labels:      []string{s.cfg.TriggerLabel},
		State:       "open",
		ListOptions: githubv39.ListOptions{PerPage: 100},
	}
	for {
		pageIssues, resp, err := s.gh.ListIssues(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing issues for label %s: %w", s.cfg.TriggerLabel, err)
		}
		for _, item := range pageIssues {
			if item.PullRequestLinks == nil {
				labelled = append(labelled, item)
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return labelled, nil
}

// scanAssigned lists the issues that are the watcher's business by ownership
// rather than by label: those assigned to a bot in the pool, and those the
// operator filed themselves.
//
// Each query is a single page sorted by update time, which is what makes this
// cheap enough to run every interval. An issue that falls off the first page
// has not been touched recently and is picked up by the labelled sweep.
//
// One account failing ends the pass. The accounts share a quota, so whatever
// refused the first will refuse the rest.
func (s *Scanner) scanAssigned(ctx context.Context) ([]*githubv39.Issue, error) {
	var allItems []*githubv39.Issue

	for _, botUser := range s.cfg.BotUsers {
		assigned, _, err := s.gh.ListIssues(ctx, &githubv39.IssueListByRepoOptions{
			Assignee:    botUser,
			State:       "open",
			Sort:        "updated",
			Direction:   "desc",
			ListOptions: githubv39.ListOptions{PerPage: s.cfg.ScanLimit},
		})
		if err != nil {
			return nil, fmt.Errorf("listing issues for assignee %s: %w", botUser, err)
		}
		klog.Infof("Fetched %d issues assigned to %s from GitHub API", len(assigned), botUser)
		allItems = append(allItems, assigned...)
	}

	if s.cfg.GitHubLogin != "" {
		created, _, err := s.gh.ListIssues(ctx, &githubv39.IssueListByRepoOptions{
			Creator:     s.cfg.GitHubLogin,
			State:       "open",
			Sort:        "updated",
			Direction:   "desc",
			ListOptions: githubv39.ListOptions{PerPage: s.cfg.ScanLimit},
		})
		if err != nil {
			return nil, fmt.Errorf("listing issues created by %s: %w", s.cfg.GitHubLogin, err)
		}
		klog.Infof("Fetched %d issues created by %s from GitHub API", len(created), s.cfg.GitHubLogin)
		adopted, err := s.adoptCreatedIssues(ctx, created)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, adopted...)
	}

	unique := make(map[int]*githubv39.Issue)
	for _, item := range allItems {
		unique[item.GetNumber()] = item
	}

	issues := make([]*githubv39.Issue, 0, len(unique))
	for _, item := range unique {
		// Pull requests belong to the pull request scanner, which lists them
		// itself. Handing them over would couple the two subcontrollers for the
		// sake of an interval's latency.
		if item.PullRequestLinks == nil {
			issues = append(issues, item)
		}
	}
	return issues, nil
}

// adoptCreatedIssues labels and assigns the issues the operator filed, so that
// an issue they created is worked on without them having to label it, and
// returns the ones eligible for queueing.
//
// An issue whose adoption write fails is logged and left out of the returned
// set. Returning it anyway would leave the local copy and GitHub disagreeing
// about whether the issue was adopted, and the caller trusts the local copy;
// leaving it out costs nothing but a cycle, since the next pass lists it again.
//
// A rate limit refusal ends the pass, because every issue still to come would
// spend its writes on refusals and arrive at the same answer.
func (s *Scanner) adoptCreatedIssues(ctx context.Context, created []*githubv39.Issue) ([]*githubv39.Issue, error) {
	var adopted []*githubv39.Issue
	for _, issue := range created {
		if issue.PullRequestLinks != nil {
			continue
		}
		num := issue.GetNumber()
		if conventions.HasStopLabel(issue.Labels, s.cfg.TriggerLabel) {
			klog.Infof("Skipping auto labeling/assigning issue #%d because it has the stop label ('overseer/stop' or '%s/stop')", num, s.cfg.TriggerLabel)
			continue
		}

		if err := s.adopt(ctx, issue); err != nil {
			if github.IsRateLimited(err) {
				return nil, err
			}
			klog.Errorf("Failed to adopt issue #%d: %v", num, err)
			continue
		}
		adopted = append(adopted, issue)
	}
	return adopted, nil
}

// adopt applies the trigger label and the pool assignee to one issue the
// operator filed, and mirrors both onto the local copy so the caller's
// subsequent label and assignee checks see the adoption that just happened
// rather than the state GitHub returned before it.
//
// The local copy is only updated once the corresponding write has landed, so a
// partial adoption - the label written, the assignee refused - leaves the copy
// describing exactly what GitHub now holds.
func (s *Scanner) adopt(ctx context.Context, issue *githubv39.Issue) error {
	num := issue.GetNumber()
	hasTriggerLabel := conventions.HasTriggerLabel(issue.Labels, s.cfg.TriggerLabel)
	hasAssignee := s.assignedToPool(issue)

	if hasTriggerLabel && hasAssignee {
		return nil
	}
	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would label issue #%d created by %s with '%s' and assign to %s\n", num, s.cfg.GitHubLogin, s.cfg.TriggerLabel, s.cfg.TargetAssignee)
		return nil
	}

	fmt.Printf("Labelling issue #%d created by %s with '%s' and assigning to %s...\n", num, s.cfg.GitHubLogin, s.cfg.TriggerLabel, s.cfg.TargetAssignee)
	if !hasTriggerLabel {
		if err := s.gh.AddLabels(ctx, num, []string{s.cfg.TriggerLabel}); err != nil {
			return fmt.Errorf("adding label %q to issue #%d: %w", s.cfg.TriggerLabel, num, err)
		}
		issue.Labels = append(issue.Labels, &githubv39.Label{Name: githubv39.String(s.cfg.TriggerLabel)})
	}
	if !hasAssignee && s.cfg.TargetAssignee != "" {
		if err := s.gh.AddAssignees(ctx, num, []string{s.cfg.TargetAssignee}); err != nil {
			return fmt.Errorf("assigning %s to issue #%d: %w", s.cfg.TargetAssignee, num, err)
		}
		issue.Assignees = append(issue.Assignees, &githubv39.User{Login: githubv39.String(s.cfg.TargetAssignee)})
	}
	return nil
}

// assignedToPool reports whether any bot account is already assigned to the issue.
func (s *Scanner) assignedToPool(issue *githubv39.Issue) bool {
	for _, u := range issue.Assignees {
		for _, bot := range s.cfg.BotUsers {
			if strings.EqualFold(u.GetLogin(), bot) {
				return true
			}
		}
	}
	return false
}

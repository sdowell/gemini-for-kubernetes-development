package prs

import (
	"context"
	"fmt"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// syncReferencedIssueLabels copies the labels of the issues a pull request
// closes onto the pull request itself.
//
// Labels are how the repository directs the watcher - stop, review, priority -
// and they are naturally put on the issue, not on the fix. Inheriting them is
// what makes 'overseer/stop' on an issue actually stop work on its pull
// request, which is why the caller re-checks the stop label immediately after
// calling this.
func (s *Scanner) syncReferencedIssueLabels(ctx context.Context, pr *githubv39.PullRequest, prIssue *githubv39.Issue, refs *refIssues) {
	allMissingLabels := getMissingLabelsForPR(prIssue.Labels, refs.all(ctx), s.cfg.TriggerLabel)

	if len(allMissingLabels) > 0 {
		klog.Infof("Adding inherited labels %v to PR #%d", allMissingLabels, pr.GetNumber())
		if err := s.gh.AddLabels(ctx, pr.GetNumber(), allMissingLabels); err != nil {
			klog.Errorf("Failed to add labels %v to PR #%d: %v", allMissingLabels, pr.GetNumber(), err)
		} else {
			for _, labelName := range allMissingLabels {
				prIssue.Labels = append(prIssue.Labels, &githubv39.Label{Name: githubv39.String(labelName)})
			}
		}
	}
}

// getMissingLabelsForPR returns the labels present on the referenced issues but
// not yet on the pull request. Pull requests and the ready-for-human label are
// never inherited, and once the pull request carries ready-for-human, review
// labels on the parent issues are not re-copied onto the pull request.
func getMissingLabelsForPR(prLabels []*githubv39.Label, refIssues []*githubv39.Issue, triggerLabel string) []string {
	prLabelsSet := make(map[string]bool)
	for _, label := range prLabels {
		if label.GetName() != "" {
			prLabelsSet[label.GetName()] = true
		}
	}

	skipReview := hasReadyForHumanLabel(prLabels, triggerLabel)

	var allMissingLabels []string
	missingLabelsSet := make(map[string]bool)

	for _, refIssue := range refIssues {
		if refIssue == nil || refIssue.IsPullRequest() {
			continue
		}

		for _, label := range refIssue.Labels {
			labelName := label.GetName()
			if labelName == "" {
				continue
			}
			// Never inherit the ready-for-human label
			if isReadyForHumanLabel(labelName, triggerLabel) {
				continue
			}
			if skipReview && isReviewLabel(labelName, triggerLabel) {
				continue
			}
			if !prLabelsSet[labelName] && !missingLabelsSet[labelName] {
				missingLabelsSet[labelName] = true
				allMissingLabels = append(allMissingLabels, labelName)
			}
		}
	}

	return allMissingLabels
}

// isPRApprovedOrLGTM reports whether the pull request has already been signed
// off, in which case there is nothing for an automated review to add.
//
// A requested change anywhere outrules an approval: the review that asked for
// something is the one still outstanding, regardless of who approved alongside it.
func isPRApprovedOrLGTM(pr *githubv39.PullRequest, prIssue *githubv39.Issue, reviews []*githubv39.PullRequestReview) bool {
	// 1. Check labels
	for _, label := range prIssue.Labels {
		if strings.EqualFold(label.GetName(), "lgtm") || strings.EqualFold(label.GetName(), "approved") {
			return true
		}
	}

	// 2. Check reviews
	hasApproved := false
	hasChangesRequested := false
	latestReviews := make(map[string]string)
	for _, r := range reviews {
		if r.GetUser() != nil && r.GetState() != "" {
			latestReviews[r.GetUser().GetLogin()] = r.GetState()
		}
	}
	for _, state := range latestReviews {
		if state == "APPROVED" {
			hasApproved = true
		} else if state == "CHANGES_REQUESTED" {
			hasChangesRequested = true
		}
	}

	return hasApproved && !hasChangesRequested
}

// isReviewLabel reports whether labelName is a review opt-in label.
func isReviewLabel(labelName, triggerLabel string) bool {
	if strings.EqualFold(labelName, "overseer/review") {
		return true
	}
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		if strings.EqualFold(labelName, triggerLabel+"/review") {
			return true
		}
	}
	return false
}

// getReviewLabels returns the review opt-in labels currently present in labels.
func getReviewLabels(labels []*githubv39.Label, triggerLabel string) []string {
	var out []string
	for _, label := range labels {
		name := label.GetName()
		if isReviewLabel(name, triggerLabel) {
			out = append(out, name)
		}
	}
	return out
}

// hasReviewLabel reports whether a set of labels opts the change into automated
// review.
func hasReviewLabel(labels []*githubv39.Label, triggerLabel string) bool {
	return len(getReviewLabels(labels, triggerLabel)) > 0
}

// shouldAutoReviewPR reports whether the pull request has been opted into
// automated review, either directly or (before ready-for-human is set) through
// an issue it closes.
//
// Review is opt-in rather than universal because it costs an agent run per
// commit; the label is how a repository says a change is worth that. Once a
// pull request has settled and been marked ready-for-human, its review label is
// removed and only a review label explicitly re-added to the pull request
// itself triggers another automated review.
func (s *Scanner) shouldAutoReviewPR(ctx context.Context, prIssue *githubv39.Issue, refs *refIssues) bool {
	if hasReviewLabel(prIssue.Labels, s.cfg.TriggerLabel) {
		return true
	}
	if hasReadyForHumanLabel(prIssue.Labels, s.cfg.TriggerLabel) {
		return false
	}
	for _, refIssue := range refs.all(ctx) {
		if hasReviewLabel(refIssue.Labels, s.cfg.TriggerLabel) {
			return true
		}
	}
	return false
}

// readyForHumanLabel returns the label marking a pull request as done with
// automation and waiting on a person.
func readyForHumanLabel(triggerLabel string) string {
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		return triggerLabel + "/ready-for-human"
	}
	return "overseer/ready-for-human"
}

// isReadyForHumanLabel reports whether labelName is a ready-for-human label.
func isReadyForHumanLabel(labelName, triggerLabel string) bool {
	if strings.EqualFold(labelName, "overseer/ready-for-human") {
		return true
	}
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		if strings.EqualFold(labelName, triggerLabel+"/ready-for-human") {
			return true
		}
	}
	return false
}

// hasReadyForHumanLabel reports whether the pull request already carries the
// ready-for-human label under either spelling.
func hasReadyForHumanLabel(labels []*githubv39.Label, triggerLabel string) bool {
	for _, label := range labels {
		if isReadyForHumanLabel(label.GetName(), triggerLabel) {
			return true
		}
	}
	return false
}

// hasCompletedBotReviewOnHead reports whether an automated review of the
// current head has finished without asking for changes.
//
// Only the most recent qualifying review counts: a reviewer that asked for
// changes and then approved after a fixup has been satisfied, and treating the
// earlier verdict as still standing would hold the pull request back forever.
func (s *Scanner) hasCompletedBotReviewOnHead(reviews []*githubv39.PullRequestReview, headSHA string, lastCommitTime time.Time) bool {
	var latestReview *githubv39.PullRequestReview
	for _, r := range reviews {
		if conventions.IsReviewerBot(r.GetUser(), s.cfg.ReviewerLogins) && (r.GetSubmittedAt().After(lastCommitTime) || r.GetCommitID() == headSHA) {
			if latestReview == nil || r.GetSubmittedAt().After(latestReview.GetSubmittedAt()) {
				latestReview = r
			}
		}
	}
	if latestReview == nil {
		return false
	}
	return latestReview.GetState() != "CHANGES_REQUESTED"
}

// isHumanUser reports whether u is a human account rather than one of the
// configured or recognised bot accounts.
func (s *Scanner) isHumanUser(u *githubv39.User) bool {
	if u == nil || u.GetLogin() == "" {
		return false
	}
	login := u.GetLogin()
	for _, bot := range s.cfg.BotUsers {
		if strings.EqualFold(login, bot) {
			return false
		}
	}
	if conventions.IsReviewerBot(u, s.cfg.ReviewerLogins) {
		return false
	}
	if conventions.ShouldIgnoreUser(u, s.cfg.GitHubLogin, nil) {
		return false
	}
	return true
}

// getMissingHumanAssigneesForPR returns the humans on the referenced parent
// issues that are not yet assigned to the pull request: each issue's human
// assignees or, for an issue the pull request closes that has no human
// assignee, its creator when that creator is a human. closing holds the
// numbers of the issues the pull request closes (see refIssues.closing).
//
// The creator is the fallback because they are the person who asked for the
// change, and so the natural owner of reviewing it when nobody has been
// assigned the issue. That only holds for an issue the pull request closes: a
// pull request that merely mentions an issue ("related to #45") is not the
// change its creator asked for. An issue that does have a human assignee has
// an owner already, and the creator is left off. An issue opened by a bot - an
// automated report, a filing from the watcher itself - has no such owner,
// which is why the creator only counts when isHumanUser says so.
func (s *Scanner) getMissingHumanAssigneesForPR(prAssignees []*githubv39.User, refIssues []*githubv39.Issue, closing map[int]bool) []string {
	existing := make(map[string]bool, len(prAssignees))
	for _, u := range prAssignees {
		if login := u.GetLogin(); login != "" {
			existing[strings.ToLower(login)] = true
		}
	}

	var missing []string
	seen := make(map[string]bool)
	add := func(u *githubv39.User) {
		login := u.GetLogin()
		key := strings.ToLower(login)
		if !existing[key] && !seen[key] {
			seen[key] = true
			missing = append(missing, login)
		}
	}
	for _, refIssue := range refIssues {
		if refIssue == nil || refIssue.PullRequestLinks != nil {
			continue
		}
		hasHumanAssignee := false
		for _, u := range refIssue.Assignees {
			if s.isHumanUser(u) {
				hasHumanAssignee = true
				add(u)
			}
		}
		// Whether the issue has a human assignee is judged on the issue,
		// not on what is still missing from the pull request: an assignee
		// already carried over still means the issue has an owner.
		if !hasHumanAssignee && closing[refIssue.GetNumber()] && s.isHumanUser(refIssue.User) {
			add(refIssue.User)
		}
	}
	return missing
}

// inheritHumanAssignees assigns to the pull request the humans from its parent
// issues (see getMissingHumanAssigneesForPR) that it does not already carry.
// reason says why in the log lines. It reports whether the pull request is now
// in step with its parent issues: false means nothing could be checked or the
// assignment failed, and is worth retrying.
//
// It is idempotent: when nothing is missing it makes no API calls, which is
// what lets it run on every evaluation of a pull request that is ready for a
// human or stopped, rather than only on the transition into that state. A
// human assigned to the parent issue later therefore still reaches the pull
// request.
func (s *Scanner) inheritHumanAssignees(ctx context.Context, prIssue *githubv39.Issue, refs *refIssues, reason string) bool {
	if refs == nil || prIssue == nil || !s.gh.Ready() {
		return false
	}
	num := prIssue.GetNumber()
	humanAssignees := s.getMissingHumanAssigneesForPR(prIssue.Assignees, refs.all(ctx), refs.closing())
	if len(humanAssignees) == 0 {
		return true
	}
	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would assign inherited human assignees %v to PR #%d (%s)\n", humanAssignees, num, reason)
		return true
	}
	klog.Infof("Assigning inherited human assignees %v to PR #%d (%s)", humanAssignees, num, reason)
	if err := s.gh.AddAssignees(ctx, num, humanAssignees); err != nil {
		klog.Errorf("Failed to assign inherited human assignees %v to PR #%d: %v", humanAssignees, num, err)
		return false
	}
	return true
}

// isBotPoolUser reports whether login is one of the bot accounts the watcher
// works through. Only pull requests they authored are the watcher's to act
// on: it cannot push to a fork it does not own.
func (s *Scanner) isBotPoolUser(login string) bool {
	for _, bot := range s.cfg.BotUsers {
		if strings.EqualFold(login, bot) {
			return true
		}
	}
	return false
}

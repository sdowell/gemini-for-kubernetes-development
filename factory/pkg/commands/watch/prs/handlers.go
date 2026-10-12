package prs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/common"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// maxInvestigations is how many times CI failures on the same revision are
// investigated before the watcher gives up and hands the pull request back.
//
// Three attempts is the point past which the agent is demonstrably not making
// progress on its own, and continuing would burn agent runs in a loop. The
// counter resets on a new commit or a human comment, so this is a limit on
// futile repetition rather than on the pull request as a whole.
const maxInvestigations = 3

// maxCommentRetries is how many times a failed address-comments task for the
// same commit SHA is retried before the watcher stops requeueing it.
const maxCommentRetries = 3

// investigationRetryAfter is how long after an investigation the same revision
// becomes worth investigating again.
//
// Re-running against an unchanged commit is normally pointless, but flaky
// infrastructure and transient failures do resolve themselves, and after a
// couple of hours a retry is cheaper than a stuck pull request.
const investigationRetryAfter = 2 * time.Hour

// prContext is the per-pull-request facts each handler needs, assembled once so
// that the handlers do not each re-derive them.
type prContext struct {
	pr                   *githubv39.PullRequest
	prIssue              *githubv39.Issue
	headSHA              string
	shortSHA             string
	lastCommitTime       time.Time
	taskAssignee         string
	isExplicitlyAssigned bool
	prURL                string
	// refIssues resolves the issues this pull request closes. It is shared
	// with the evaluation that built this context, so a handler reading it
	// spends nothing: by the time a handler runs, the issues have already been
	// fetched for the label sync.
	refIssues *refIssues
}

// handlePRIterate queues a rebase for a pull request that conflicts with its
// base branch.
//
// It is gated on the head SHA rather than on time: if the last rebase produced
// no new commit then it did not resolve anything, and running it again against
// the identical tree would produce the identical result.
func (s *Scanner) handlePRIterate(ctx context.Context, pc *prContext) {
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)

	if state.lastIteratedSHA != "" && state.lastIteratedSHA == pc.headSHA {
		klog.Infof("Skipping PR #%d rebase/conflict resolution because an iterate task was already processed for head SHA %s.", num, pc.headSHA)
		return
	}

	filename := fmt.Sprintf("task-pr-%d-iterate.yaml", num)
	if s.queue.TaskExists(filename) {
		return
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRIterate, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return
	} else if running {
		klog.Infof("Skipping PR #%d rebase because there is an in-flight sandbox %s.", num, sandboxName)
		return
	}

	baseRef := ""
	if pc.pr.GetBase() != nil {
		baseRef = pc.pr.GetBase().GetRef()
	}
	notes := fmt.Sprintf("PR #%d has merge conflicts with base branch '%s'; head commit %s committer date %s, PR updated at %s", num, baseRef, pc.shortSHA, pc.lastCommitTime.Format(time.RFC3339), pc.pr.GetUpdatedAt().Format(time.RFC3339))

	task := s.newTask(taskOptions{
		Type:             api.TypePRIterate,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseRebase,
		Assignee:         pc.taskAssignee,
		CommitSHA:        pc.headSHA,
		TriggerEventTime: pc.lastCommitTime,
		TriggerReason:    api.TriggerReasonPRMergeConflict,
		TriggerNotes:     notes,
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue rebase task for PR #%d: %s\n", num, pc.prURL)
		return
	}
	fmt.Printf("Queueing rebase task for PR #%d...\n", num)
	// The state is recorded before the enqueue rather than after, so that a
	// failure to write the task file does not leave the next cycle thinking the
	// work is still outstanding while the file is in fact being retried.
	state.lastIteratedSHA = pc.headSHA
	state.lastIteratedTime = time.Now()
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue rebase task for PR #%d: %v", num, err)
	}
}

// handlePRMergeQueueEviction queues a rebase-and-fix for a pull request that
// was removed from the merge queue because it failed there, while its own head
// neither conflicts nor fails CI.
//
// That combination almost always means a semantic conflict: something that
// landed on the base branch after the pull request was branched - a renamed or
// removed function, type or field, say - breaks the change without touching a
// line of it. A plain rebase does not fix that, so the task runs the rebase
// recipe with instructions to build and test the whole result, and to fix what
// the base branch broke.
//
// It is gated on the eviction rather than on the head SHA: the task is the
// answer to one eviction, and a run that finds nothing to push must not be
// repeated until the queue evicts the pull request again. It reports whether
// the phase completed cleanly; a false return means a sandbox probe or enqueue
// failed and the pull request must be looked at again next cycle.
func (s *Scanner) handlePRMergeQueueEviction(ctx context.Context, pc *prContext, eviction *github.MergeQueueEviction) bool {
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)

	if !state.lastEvictionHandledAt.IsZero() && !eviction.At.After(state.lastEvictionHandledAt) {
		klog.Infof("Skipping PR #%d merge queue eviction at %s because a rebase-and-fix task was already queued for it.", num, eviction.At.Format(time.RFC3339))
		return true
	}

	// The same file name as a conflict rebase: both run the rebase recipe in
	// the PR's sandbox, and two of them at once would fight over the branch.
	filename := fmt.Sprintf("task-pr-%d-iterate.yaml", num)
	if s.queue.TaskExists(filename) {
		return true
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRIterate, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return false
	} else if running {
		klog.Infof("Skipping PR #%d merge queue fix because there is an in-flight sandbox %s.", num, sandboxName)
		return false
	}

	baseRef := ""
	if pc.pr.GetBase() != nil {
		baseRef = pc.pr.GetBase().GetRef()
	}

	// The checks that evicted the pull request ran on the merge-group commit,
	// so that is where the failure is read from. A failed read only costs the
	// agent a hint: it can still find the runs from the commit SHA.
	failure := ""
	if eviction.MergeGroupSHA != "" {
		analysis, err := s.evaluateChecks(ctx, eviction.MergeGroupSHA)
		if err != nil {
			klog.Warningf("Failed to read merge queue checks for PR #%d on %s: %v", num, eviction.MergeGroupSHA, err)
		} else if analysis.hasFailure {
			failure = fmt.Sprintf("earliest failure in '%s' (%s); %d failed check(s)", analysis.earliestFailureName, analysis.earliestFailureConclusion, analysis.failedCount)
		}
	}

	notes := fmt.Sprintf("PR #%d was removed from the merge queue at %s (reason: %s); merge-group commit %s; head commit %s committer date %s",
		num, eviction.At.Format(time.RFC3339), eviction.Reason, abbrevSHA(eviction.MergeGroupSHA), pc.shortSHA, pc.lastCommitTime.Format(time.RFC3339))
	if failure != "" {
		notes += "; " + failure
	}

	task := s.newTask(taskOptions{
		Type:             api.TypePRIterate,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseRebase,
		Assignee:         pc.taskAssignee,
		CommitSHA:        pc.headSHA,
		TriggerEventTime: eviction.At,
		TriggerReason:    api.TriggerReasonPRMergeQueueEvicted,
		TriggerNotes:     notes,
		Instructions:     []string{mergeQueueEvictionInstructions(baseRef, eviction, failure)},
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue merge queue fix task for PR #%d: %s\n", num, pc.prURL)
		return true
	}
	fmt.Printf("Queueing merge queue fix task for PR #%d...\n", num)
	// Recorded before the enqueue, as for a conflict rebase: a failed write is
	// retried by the queue rather than re-queued by the next cycle.
	state.lastEvictionHandledAt = eviction.At
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue merge queue fix task for PR #%d: %v", num, err)
		return false
	}
	return true
}

// mergeQueueEvictionInstructions tells the rebase agent why it is rebasing a
// pull request that has no conflicts: the recipe on its own would rebase, find
// nothing to resolve, and push a branch that fails the merge queue again.
func mergeQueueEvictionInstructions(baseRef string, eviction *github.MergeQueueEviction, failure string) string {
	if baseRef == "" {
		baseRef = "the default branch"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "This pull request was removed from the merge queue at %s (reason: %s). ", eviction.At.Format(time.RFC3339), eviction.Reason)
	fmt.Fprintf(&b, "Its own head has no merge conflicts and passes CI, but the merge queue tests it merged into the current %s, and there it failed. ", baseRef)
	b.WriteString("The usual cause is a change that landed on the base branch after this pull request was branched - a renamed or removed function, type, field or file it depends on - which breaks it without conflicting textually.\n\n")
	if eviction.MergeGroupSHA != "" {
		fmt.Fprintf(&b, "The failing merge queue checks ran on merge-group commit %s. If the gh CLI is available, find them with `gh run list --commit %s` and read the errors with `gh run view <run-id> --log-failed`.", eviction.MergeGroupSHA, eviction.MergeGroupSHA)
		if failure != "" {
			fmt.Fprintf(&b, " Summary: %s.", failure)
		}
		b.WriteString("\n\n")
	}
	b.WriteString("After rebasing, do not stop at resolving conflicts: there may be none. Build the whole project and run its linters and the tests covering what this pull request touches, as its CI would. ")
	b.WriteString("Fix every failure so the change works on top of the current base branch while still doing what it did, following how the base branch now does things (for example, migrate to the API that replaced a removed one). ")
	b.WriteString("In your report, say what the base branch changed, what it broke, and how you fixed it.")
	return b.String()
}

// abbrevSHA abbreviates a commit SHA for messages.
func abbrevSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// canInvestigatePR reports whether CI failures on this pull request are worth
// another look.
//
// Hitting the retry limit counts as "yes" so that handlePRInvestigate is
// reached and can apply the stop label: the decision to give up has to be
// announced on the pull request, not made silently here.
func (s *Scanner) canInvestigatePR(
	num int,
	headSHA string,
	isExplicitlyAssigned bool,
	state prState,
	comments []*githubv39.IssueComment,
	lastCommitTime time.Time,
) bool {
	filename := fmt.Sprintf("task-pr-%d-investigate.yaml", num)
	if s.queue.TaskExists(filename) {
		return false
	}
	if getInvestigationCount(comments, lastCommitTime, s.cfg.BotUsers, s.cfg.GitHubLogin, s.cfg.AllowlistedBots, s.cfg.TrustedLogins, s.cfg.TriggerLabel) >= maxInvestigations {
		return true
	}
	return state.lastInvestigatedSHA != headSHA ||
		s.lastInvestigationFailed(filename) ||
		isExplicitlyAssigned ||
		time.Since(state.lastInvestigatedTime) > investigationRetryAfter
}

// lastInvestigationFailed reports whether the previous investigation task ended
// in failure, which makes the same revision worth retrying: the agent never got
// to finish, so its verdict says nothing about the CI failure.
func (s *Scanner) lastInvestigationFailed(filename string) bool {
	last := s.queue.GetProcessedTask(filename)
	return last != nil && last.Status == api.StatusFailed
}

// handlePRInvestigate queues an investigation of the pull request's CI
// failures, or gives up and pauses the pull request once the retry limit has
// been reached. It reports whether the phase completed cleanly; a false return
// means a sandbox probe or enqueue failed and the pull request must be looked
// at again next cycle.
func (s *Scanner) handlePRInvestigate(
	ctx context.Context,
	pc *prContext,
	checkAnalysis prCheckAnalysis,
	comments []*githubv39.IssueComment,
) bool {
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-investigate.yaml", num)

	if s.queue.TaskExists(filename) {
		return true
	}

	investigationCount := getInvestigationCount(comments, pc.lastCommitTime, s.cfg.BotUsers, s.cfg.GitHubLogin, s.cfg.AllowlistedBots, s.cfg.TrustedLogins, s.cfg.TriggerLabel)
	if investigationCount >= maxInvestigations {
		stopLabel := conventions.StopLabel(s.cfg.TriggerLabel)
		if !s.cfg.DryRun {
			s.comment(ctx, num, fmt.Sprintf("🤖 AI Factory has attempted to investigate/fix CI check failures for this pull request %d times since the last commit or update without success. To prevent infinite loops, I am pausing automated investigation and attaching the `%s` label.\n\nTo request another attempt or resume automated processing, please remove the `%s` label from this pull request (and/or push a new commit or leave a comment).", maxInvestigations, stopLabel, stopLabel))
			if err := s.gh.AddLabels(ctx, num, []string{stopLabel}); err != nil {
				klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
			}
		}
		klog.Infof("Skipping PR #%d investigate because it has reached the maximum retry limit (%d attempts since last update) and applying stop label '%s'.", num, maxInvestigations, stopLabel)
		return true
	}

	if state.lastInvestigatedSHA == pc.headSHA &&
		!s.lastInvestigationFailed(filename) &&
		!pc.isExplicitlyAssigned &&
		time.Since(state.lastInvestigatedTime) <= investigationRetryAfter {
		return true
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRInvestigate, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return false
	} else if running {
		klog.Infof("Skipping PR #%d investigate because there is an in-flight sandbox %s.", num, sandboxName)
		return false
	}

	eventTime := checkAnalysis.earliestFailureTime
	if eventTime.IsZero() {
		eventTime = pc.lastCommitTime
	}
	failName := checkAnalysis.earliestFailureName
	if failName == "" {
		failName = "unknown check"
	}
	failConclusion := checkAnalysis.earliestFailureConclusion
	if failConclusion == "" {
		failConclusion = "failed"
	}
	notes := fmt.Sprintf("Earliest CI failure in '%s' (%s) at %s; total %d failed check(s) on commit %s", failName, failConclusion, eventTime.Format(time.RFC3339), checkAnalysis.failedCount, pc.shortSHA)

	task := s.newTask(taskOptions{
		Type:             api.TypePRInvestigate,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseInvestigate,
		Assignee:         pc.taskAssignee,
		CommitSHA:        pc.headSHA,
		TriggerEventTime: eventTime,
		TriggerReason:    api.TriggerReasonPRCheckFailed,
		TriggerNotes:     notes,
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue investigate task for PR #%d: %s\n", num, pc.prURL)
		return true
	}
	fmt.Printf("Queueing investigate task for PR #%d...\n", num)
	state.lastInvestigatedSHA = pc.headSHA
	state.lastInvestigatedTime = time.Now()
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue investigate task for PR #%d: %v", num, err)
		return false
	}
	return true
}

// shouldRetryComments reports whether the previous address-comments task
// failed against the current head commit and needs retry handling (either
// queueing a retry, or applying the stop label once maxCommentRetries is
// reached).
func (s *Scanner) shouldRetryComments(num int, headSHA string) bool {
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)
	if s.queue.TaskExists(filename) {
		return false
	}
	return s.hasFailedCommentsOnHead(num, headSHA)
}

// hasFailedCommentsOnHead reports whether the last processed address-comments
// task for this pull request failed against the current head commit.
func (s *Scanner) hasFailedCommentsOnHead(num int, headSHA string) bool {
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)
	last := s.queue.GetProcessedTask(filename)
	return last != nil && last.Status == api.StatusFailed && last.CommitSHA == headSHA
}

// handlePRComments queues a task to address the outstanding review feedback,
// or retries a previous address-comments task that failed on the same commit.
// It reports whether the phase completed cleanly; a false return means a
// sandbox probe or enqueue failed and the pull request must be looked at again
// next cycle.
func (s *Scanner) handlePRComments(ctx context.Context, pc *prContext, commentAnalysis prCommentAnalysis, comments []*githubv39.IssueComment) bool {
	if os.Getenv("DRY_RUN") == "true" {
		return true
	}
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)

	if s.queue.TaskExists(filename) {
		return true
	}

	var eventTime time.Time
	var notes string
	retryCount := 0

	if commentAnalysis.hasNewComments {
		commitInfo := ""
		if !pc.lastCommitTime.IsZero() {
			commitInfo = fmt.Sprintf(" since last commit %s (committer date %s)", pc.shortSHA, pc.lastCommitTime.Format(time.RFC3339))
		}
		authorStr := ""
		if commentAnalysis.oldestCommentAuthor != "" {
			authorStr = fmt.Sprintf(" by %s", commentAnalysis.oldestCommentAuthor)
		}
		cType := commentAnalysis.oldestCommentType
		if cType == "" {
			cType = "comment"
		}
		eventTime = commentAnalysis.oldestCommentTime
		notes = fmt.Sprintf("Oldest unaddressed %s%s added at %s (ID %d)%s", cType, authorStr, commentAnalysis.oldestCommentTime.Format(time.RFC3339), commentAnalysis.oldestCommentID, commitInfo)
	} else {
		last := s.queue.GetProcessedTask(filename)
		if last != nil && last.RetryCount >= maxCommentRetries {
			stopLabel := conventions.StopLabel(s.cfg.TriggerLabel)
			if !hasCommentPauseAfter(comments, last.CompletedAt) {
				if !s.cfg.DryRun {
					if err := s.gh.AddLabels(ctx, num, []string{stopLabel}); err != nil {
						klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
						return false
					}
					s.comment(ctx, num, fmt.Sprintf("🤖 AI Factory has attempted to address review feedback for this pull request %d times since the last commit or update without success. To prevent infinite loops, I am pausing automated feedback addressing and attaching the `%s` label.\n\nTo request another attempt or resume automated processing, please remove the `%s` label from this pull request (and/or push a new commit or leave a comment).", maxCommentRetries, stopLabel, stopLabel))
				}
				klog.Infof("Skipping PR #%d address-comments retry because it has reached the maximum retry limit (%d retries) for SHA %s and applying stop label '%s'.", num, maxCommentRetries, pc.headSHA, stopLabel)
				return true
			}
			// A pause comment was already posted after the last failure, and the
			// stop label is no longer present on the PR: a human removed the label
			// to request another attempt. Reset retryCount to 0.
			retryCount = 0
			eventTime = last.TriggerEventTime
			if eventTime.IsZero() {
				eventTime = pc.lastCommitTime
			}
			notes = fmt.Sprintf("Retrying failed address-comments task after '%s' label removal on commit %s", stopLabel, pc.shortSHA)
		} else {
			if last != nil {
				retryCount = last.RetryCount + 1
				eventTime = last.TriggerEventTime
			} else {
				retryCount = 1
			}
			if eventTime.IsZero() {
				eventTime = pc.lastCommitTime
			}
			notes = fmt.Sprintf("Retrying failed address-comments task (retry %d/%d) on commit %s", retryCount, maxCommentRetries, pc.shortSHA)
		}
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRComments, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return false
	} else if running {
		klog.Infof("Skipping PR #%d address-comments because there is an in-flight sandbox %s.", num, sandboxName)
		return false
	}

	task := s.newTask(taskOptions{
		Type:             api.TypePRComments,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseComments,
		Assignee:         pc.taskAssignee,
		CommitSHA:        pc.headSHA,
		RetryCount:       retryCount,
		TriggerEventTime: eventTime,
		TriggerReason:    api.TriggerReasonPRCommentsAdded,
		TriggerNotes:     notes,
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue address-comments task for PR #%d: %s\n", num, pc.prURL)
		return true
	}
	fmt.Printf("Queueing address-comments task for PR #%d...\n", num)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue address-comments task for PR #%d: %v", num, err)
		return false
	}
	// The acknowledgement reactions are what tell the next cycle these comments
	// are already spoken for, and what tell the commenter they were seen.
	for _, cid := range commentAnalysis.unackCommentIDs {
		s.react(ctx, cid, conventions.ReactionAcknowledged)
	}
	for _, cid := range commentAnalysis.unackPRCommentIDs {
		s.reactToReviewComment(ctx, cid, conventions.ReactionAcknowledged)
	}
	for _, nodeID := range commentAnalysis.unackReviewNodeIDs {
		s.reactToReview(ctx, nodeID, conventions.ReactionAcknowledged)
	}
	state.lastCommentAddressedTime = task.EnqueuedAt
	s.state.set(num, state)
	return true
}

// handlePRReview queues an automated review of a green, unreviewed pull request.
//
// Review instructions are collected from the pull request body and from the
// issues it closes, so that a repository can steer the review from wherever the
// requirement was written down. It reports whether the phase completed cleanly;
// a false return means a sandbox probe or enqueue failed and the pull request
// must be looked at again next cycle.
func (s *Scanner) handlePRReview(ctx context.Context, pc *prContext, checkRuns []*githubv39.CheckRun) bool {
	if os.Getenv("DRY_RUN") == "true" {
		return true
	}
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-review.yaml", num)

	if s.queue.TaskExists(filename) {
		return true
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRReview, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return false
	} else if running {
		klog.Infof("Skipping PR #%d review because there is an in-flight sandbox %s.", num, sandboxName)
		return false
	}

	var bodies []string
	if pc.pr.GetBody() != "" {
		bodies = append(bodies, pc.pr.GetBody())
	}
	for _, refIssue := range pc.refIssues.all(ctx) {
		if refIssue.GetBody() != "" {
			bodies = append(bodies, refIssue.GetBody())
		}
	}
	instructions := common.ExtractReviewInstructions(bodies...)

	// The review is triggered by CI going green, so that is the event it is
	// timed from whenever the checks finished after the commit landed.
	var latestCheckCompletedTime time.Time
	for _, run := range checkRuns {
		t := run.GetCompletedAt().Time
		if t.After(latestCheckCompletedTime) {
			latestCheckCompletedTime = t
		}
	}
	eventTime := pc.lastCommitTime
	var notes string
	if !latestCheckCompletedTime.IsZero() && latestCheckCompletedTime.After(pc.lastCommitTime) {
		eventTime = latestCheckCompletedTime
		notes = fmt.Sprintf("Automated review triggered; all CI checks passed at %s for commit %s (committer date %s)", latestCheckCompletedTime.Format(time.RFC3339), pc.shortSHA, pc.lastCommitTime.Format(time.RFC3339))
	} else {
		notes = fmt.Sprintf("Automated review triggered for commit %s at %s", pc.shortSHA, eventTime.Format(time.RFC3339))
	}

	task := s.newTask(taskOptions{
		Type:             api.TypePRReview,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseComments,
		CommitSHA:        pc.headSHA,
		TriggerEventTime: eventTime,
		TriggerReason:    api.TriggerReasonPRReadyForReview,
		TriggerNotes:     notes,
		Instructions:     instructions,
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue review task for PR #%d: %s\n", num, pc.prURL)
		return true
	}
	fmt.Printf("Queueing review task for PR #%d (Instructions: %d)...\n", num, len(instructions))
	state.lastReviewedSHA = pc.headSHA
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue review task for PR #%d: %v", num, err)
		return false
	}
	return true
}

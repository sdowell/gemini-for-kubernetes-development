package prs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/common"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// maxInvestigations is how many times CI failures on the same revision are
// investigated before the watcher gives up and hands the pull request back.
//
// Three attempts is the point past which the agent is demonstrably not making
// progress on its own, and continuing would burn agent runs in a loop. The
// counter resets on a new commit or a human comment, so this is a limit on
// futile repetition rather than on the pull request as a whole.
const maxInvestigations = 3

// investigationRetryAfter is how long after an investigation the same revision
// becomes worth investigating again.
//
// Re-running against an unchanged commit is normally pointless, but flaky
// infrastructure and transient failures do resolve themselves, and after a
// couple of hours a retry is cheaper than a stuck pull request.
const investigationRetryAfter = 2 * time.Hour

// maxCommentRetries is how many times an address-comments task that ended in
// failure is re-queued before the watcher gives up and hands the pull request
// back to a human.
//
// Unlike an investigation, a failed address-comments task cannot be left to the
// next scan to notice: the watcher reacts to every comment it picks up, and
// those reactions are what make the feedback invisible to the scan that
// follows. So the retry has to be deliberate, and therefore bounded - the
// common causes of failure (an exhausted Gemini quota, a sandbox that died)
// clear on their own within a few attempts, and anything that does not is a
// loop rather than a transient. The count resets whenever new feedback arrives,
// so this bounds futile repetition of the same work, not the pull request.
const maxCommentRetries = 3

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

	// A conflicted pull request is never ready for a human, whatever it looked
	// like before.
	s.reconcileReadyForHumanLabel(ctx, num, pc.prIssue, false, pc.headSHA)
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
	if getInvestigationCount(comments, lastCommitTime, s.cfg.BotUsers, s.cfg.GitHubLogin, s.cfg.AllowlistedBots, s.cfg.TriggerLabel) >= maxInvestigations {
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
	return taskFailed(s.loadProcessedTask(filename))
}

// loadProcessedTask reads the last completed run of a task file, or returns nil
// when there is none to read.
//
// The processed directory is the watcher's memory of what it has already done:
// unlike the in-memory state it survives a restart, and unlike GitHub it
// records why a run ended. That makes it the only place a retry decision can
// honestly be made from.
func (s *Scanner) loadProcessedTask(filename string) *api.QueueTask {
	data, err := os.ReadFile(filepath.Join(s.cfg.ProcessedDir, filename))
	if err != nil {
		return nil
	}
	var t api.QueueTask
	if err := yaml.Unmarshal(data, &t); err != nil {
		return nil
	}
	return &t
}

// taskFailed reports whether a completed task ended in failure. The comparison
// is case-insensitive because the status is also written by hand into task
// files during operational fixups.
func taskFailed(t *api.QueueTask) bool {
	return t != nil && strings.EqualFold(string(t.Status), string(api.StatusFailed))
}

// failedCommentsTask returns the address-comments task whose failure is still
// outstanding for a pull request, or nil when there is nothing to retry.
//
// This is what makes the retry idempotent. The verdict comes from the single
// task file the pull request's address-comments work is written to, so a task
// already queued or running is not retried, and a retry that finishes replaces
// the failure it was answering - leaving nothing for the next scan to act on.
// The attempt count travels in the file too, which is what keeps the budget
// intact across watcher restarts.
//
// A task whose count has passed the limit has already been given up on and
// announced, and is likewise nothing to act on: without that the pull request
// would be re-labelled the moment a human removed the stop label the giving up
// applied.
func (s *Scanner) failedCommentsTask(num int) *api.QueueTask {
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)
	if s.queue.TaskExists(filename) {
		return nil
	}
	t := s.loadProcessedTask(filename)
	if !taskFailed(t) || t.Retries > maxCommentRetries {
		return nil
	}
	return t
}

// markCommentRetriesExhausted records in the task file that the watcher has
// given up on a pull request's feedback and said so.
//
// The count is pushed one past the limit rather than a separate flag being
// added: "more attempts than the budget allows" is precisely the state being
// recorded, and it is the same field every other decision here reads.
func (s *Scanner) markCommentRetriesExhausted(num int, failed *api.QueueTask) {
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)
	t := *failed
	t.Retries = maxCommentRetries + 1
	data, err := yaml.Marshal(&t)
	if err != nil {
		klog.Errorf("Failed to marshal exhausted address-comments task for PR #%d: %v", num, err)
		return
	}
	if err := os.WriteFile(filepath.Join(s.cfg.ProcessedDir, filename), data, 0644); err != nil {
		klog.Errorf("Failed to record exhausted address-comments retries for PR #%d: %v", num, err)
	}
}

// handlePRInvestigate queues an investigation of the pull request's CI
// failures, or gives up and pauses the pull request once the retry limit has
// been reached.
func (s *Scanner) handlePRInvestigate(
	ctx context.Context,
	pc *prContext,
	checkAnalysis prCheckAnalysis,
	comments []*githubv39.IssueComment,
) {
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-investigate.yaml", num)

	if s.queue.TaskExists(filename) {
		return
	}

	investigationCount := getInvestigationCount(comments, pc.lastCommitTime, s.cfg.BotUsers, s.cfg.GitHubLogin, s.cfg.AllowlistedBots, s.cfg.TriggerLabel)
	if investigationCount >= maxInvestigations {
		stopLabel := conventions.StopLabel(s.cfg.TriggerLabel)
		if !s.cfg.DryRun {
			s.comment(ctx, num, fmt.Sprintf("🤖 AI Factory has attempted to investigate/fix CI check failures for this pull request %d times since the last commit or update without success. To prevent infinite loops, I am pausing automated investigation and attaching the `%s` label.\n\nTo request another attempt or resume automated processing, please remove the `%s` label from this pull request (and/or push a new commit or leave a comment).", maxInvestigations, stopLabel, stopLabel))
			if err := s.gh.AddLabels(ctx, num, []string{stopLabel}); err != nil {
				klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
			}
		}
		klog.Infof("Skipping PR #%d investigate because it has reached the maximum retry limit (%d attempts since last update) and applying stop label '%s'.", num, maxInvestigations, stopLabel)
		return
	}

	if state.lastInvestigatedSHA == pc.headSHA &&
		!s.lastInvestigationFailed(filename) &&
		!pc.isExplicitlyAssigned &&
		time.Since(state.lastInvestigatedTime) <= investigationRetryAfter {
		return
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRInvestigate, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return
	} else if running {
		klog.Infof("Skipping PR #%d investigate because there is an in-flight sandbox %s.", num, sandboxName)
		return
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
		return
	}
	fmt.Printf("Queueing investigate task for PR #%d...\n", num)
	state.lastInvestigatedSHA = pc.headSHA
	state.lastInvestigatedTime = time.Now()
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue investigate task for PR #%d: %v", num, err)
	}
}

// handlePRComments queues a task to address the outstanding review feedback.
//
// failed is the pull request's last address-comments task when it ended in
// failure, and nil otherwise. A failed attempt is re-queued rather than
// forgotten: the watcher reacted to every comment it handed over, so without a
// retry that feedback is never looked at again.
//
// Which comments the retry covers is not recorded here. The reactions on the
// comments themselves say that - a failed attempt leaves them marked confused,
// which is what puts them back in front of the next scan - so the only thing
// the task file has to remember is how many attempts have been spent.
func (s *Scanner) handlePRComments(ctx context.Context, pc *prContext, commentAnalysis prCommentAnalysis, failed *api.QueueTask) {
	if os.Getenv("DRY_RUN") == "true" {
		return
	}
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)

	if s.queue.TaskExists(filename) {
		return
	}

	// New feedback starts the budget over. The budget is patience with
	// repeated failure, and a reviewer who has since said something new is
	// owed a fresh allowance of it.
	//
	// A new revision deliberately does not count, tempting though it is to
	// read one as progress. A failed attempt often pushes a commit before it
	// dies - that is the very failure this retry exists for - so counting a
	// new head would let every attempt reset its own budget and retry for
	// ever. Nor does a commit mean the feedback was answered: the agent can
	// address a comment without committing, and a commit can have nothing to
	// do with the review. Only the '+1' says the work was done.
	retries := 0
	if failed != nil && !newFeedbackSince(failed, commentAnalysis) {
		retries = failed.Retries + 1
		if retries > maxCommentRetries {
			s.giveUpOnComments(ctx, pc, failed)
			return
		}
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRComments, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return
	} else if running {
		klog.Infof("Skipping PR #%d address-comments because there is an in-flight sandbox %s.", num, sandboxName)
		return
	}

	// The task is ordered by the oldest piece of feedback still waiting, which
	// for a retry is whatever its predecessor was queued for: a second attempt
	// should not go to the back of the queue behind work raised after it.
	eventTime := commentAnalysis.oldestCommentTime
	if failed != nil && !failed.TriggerEventTime.IsZero() &&
		(eventTime.IsZero() || failed.TriggerEventTime.Before(eventTime)) {
		eventTime = failed.TriggerEventTime
	}

	notes := commentTriggerNotes(pc, commentAnalysis)
	switch {
	case retries > 0:
		reason := strings.TrimSpace(failed.Error)
		if reason == "" {
			reason = "no error recorded"
		}
		notes = fmt.Sprintf("Retry %d of %d after a failed attempt to address %d comment(s) and %d inline review comment(s): %s",
			retries, maxCommentRetries, len(commentAnalysis.unackCommentIDs), len(commentAnalysis.unackPRCommentIDs), truncate(reason, 300))
	case failed != nil:
		notes += "; also re-attempting the feedback a previous failed task left unaddressed"
	}

	task := s.newTask(taskOptions{
		Type:             api.TypePRComments,
		PR:               pc.pr,
		PRIssue:          pc.prIssue,
		Phase:            api.PhaseComments,
		Assignee:         pc.taskAssignee,
		CommitSHA:        pc.headSHA,
		TriggerEventTime: eventTime,
		TriggerReason:    api.TriggerReasonPRCommentsAdded,
		TriggerNotes:     notes,
		Retries:          retries,
	})

	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would queue address-comments task for PR #%d: %s\n", num, pc.prURL)
		return
	}
	if retries > 0 {
		fmt.Printf("Queueing address-comments retry %d/%d for PR #%d...\n", retries, maxCommentRetries, num)
	} else {
		fmt.Printf("Queueing address-comments task for PR #%d...\n", num)
	}
	// The 'eyes' reactions are what tell the next cycle these comments are
	// already spoken for, and what tell the commenter they were seen.
	for _, cid := range commentAnalysis.unackCommentIDs {
		s.react(ctx, cid, "eyes")
	}
	for _, cid := range commentAnalysis.unackPRCommentIDs {
		s.reactToReviewComment(ctx, cid, "eyes")
	}
	state.lastCommentAddressedTime = time.Now()
	state.lastCommentAddressedSHA = pc.headSHA
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue address-comments task for PR #%d: %v", num, err)
	}
}

// commentTriggerNotes describes the feedback a task was queued for, singling out
// the comment that has been waiting longest.
func commentTriggerNotes(pc *prContext, commentAnalysis prCommentAnalysis) string {
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
	return fmt.Sprintf("Oldest unaddressed %s%s added at %s (ID %d)%s", cType, authorStr, commentAnalysis.oldestCommentTime.Format(time.RFC3339), commentAnalysis.oldestCommentID, commitInfo)
}

// giveUpOnComments stops retrying a pull request's review feedback and hands it
// back to a human.
//
// The decision is announced on the pull request rather than taken silently,
// because the feedback itself is now invisible to the watcher: the comments
// carry its acknowledgement reactions, so nothing short of new feedback
// arriving will make it look at them again.
//
// It is also written back to the task file, so that it happens once. Otherwise
// removing the stop label - the very thing the announcement asks for - would
// hand the pull request straight back to this function and have it re-applied.
func (s *Scanner) giveUpOnComments(ctx context.Context, pc *prContext, failed *api.QueueTask) {
	num := pc.prIssue.GetNumber()
	stopLabel := conventions.StopLabel(s.cfg.TriggerLabel)

	klog.Infof("Skipping PR #%d address-comments because it has failed %d times in a row; applying stop label '%s'.", num, maxCommentRetries+1, stopLabel)
	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would pause address-comments on PR #%d and apply label '%s'\n", num, stopLabel)
		return
	}

	s.markCommentRetriesExhausted(num, failed)

	reason := strings.TrimSpace(failed.Error)
	if reason == "" {
		reason = "no error was recorded"
	}
	s.comment(ctx, num, fmt.Sprintf("🤖 AI Factory has attempted to address the review feedback on this pull request %d times without success, most recently failing with:\n\n```\n%s\n```\n\nTo prevent infinite loops, I am pausing automated processing and attaching the `%s` label. The outstanding feedback has **not** been addressed.\n\nTo request another attempt, please remove the `%s` label and leave a new comment describing what you would like changed.", maxCommentRetries+1, truncate(reason, 1000), stopLabel, stopLabel))
	if err := s.gh.AddLabels(ctx, num, []string{stopLabel}); err != nil {
		klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
	}
}

// newFeedbackSince reports whether a reviewer has said something new since a
// failed address-comments attempt was queued.
//
// The comparison is against when the attempt started rather than when it
// finished, because a comment written while it was running is one it may never
// have read: the feedback it was given was fixed when its prompt was built.
func newFeedbackSince(failed *api.QueueTask, commentAnalysis prCommentAnalysis) bool {
	return !failed.StartedAt.IsZero() && commentAnalysis.newestCommentTime.After(failed.StartedAt)
}

// truncate shortens a string to at most n characters, marking where it was cut.
// Task errors carry whole agent transcripts, and those belong in the log rather
// than in a trigger note or a pull request comment.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "... (truncated)"
}

// handlePRReview queues an automated review of a green, unreviewed pull request.
//
// Review instructions are collected from the pull request body and from the
// issues it closes, so that a repository can steer the review from wherever the
// requirement was written down.
func (s *Scanner) handlePRReview(ctx context.Context, pc *prContext, checkRuns []*githubv39.CheckRun) {
	if os.Getenv("DRY_RUN") == "true" {
		return
	}
	num := pc.prIssue.GetNumber()
	state := s.state.get(num)
	filename := fmt.Sprintf("task-pr-%d-review.yaml", num)

	if s.queue.TaskExists(filename) {
		return
	}

	sandboxName := s.sandboxes.ResolveName(ctx, api.TypePRReview, num)
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return
	} else if running {
		klog.Infof("Skipping PR #%d review because there is an in-flight sandbox %s.", num, sandboxName)
		return
	}

	var bodies []string
	if pc.pr.GetBody() != "" {
		bodies = append(bodies, pc.pr.GetBody())
	}
	for refIssueNum := range common.GetReferencedIssues(pc.pr) {
		refIssue, err := s.gh.GetIssue(ctx, refIssueNum)
		if err == nil && refIssue.GetBody() != "" {
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
		return
	}
	fmt.Printf("Queueing review task for PR #%d (Instructions: %d)...\n", num, len(instructions))
	state.lastReviewedSHA = pc.headSHA
	s.state.set(num, state)
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue review task for PR #%d: %v", num, err)
	}
}

package prs

import (
	"context"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// prCommentAnalysis is the verdict on a pull request's outstanding feedback.
//
// The oldest unaddressed comment is singled out, not the newest: it is the one
// that has been waiting longest, and quoting it in the task gives the agent the
// start of the conversation rather than its tail.
type prCommentAnalysis struct {
	hasNewComments      bool
	unackCommentIDs     []int64
	unackPRCommentIDs   []int64
	oldestCommentTime   time.Time
	oldestCommentAuthor string
	oldestCommentType   string
	oldestCommentID     int64
}

// evaluateComments decides whether a pull request has feedback still waiting on
// the agent.
//
// A comment counts as outstanding only if it post-dates both the last commit (a
// push is taken as the answer to everything said before it) and the last
// address-comments task (whose work is not on GitHub yet). Either being newer
// means the feedback has been dealt with.
//
// Reactions are the second gate, and the more precise one: they record what
// became of one particular comment rather than when the watcher was last busy.
// What the emoji mean, and which of them outrank the others, is
// conventions.CommentState's business; this function only asks whether the
// comment still needs attention.
//
// Human feedback always wins. Bot review feedback is held back when an
// address-comments task already ran against this exact commit, because the
// agent looking at the same review and producing no commit means it judged
// there was nothing to change - running it again would loop forever.
//
// A retry sets the reactions aside, which is what makes each attempt work from
// the same set of comments. The marks record that the watcher took the feedback
// on, not that it answered it, and the attempt they were written for failed.
// The timestamps need no such treatment: a failed attempt writes neither of
// them, so an addressed-at stamp can only have come from an attempt that
// succeeded.
func (s *Scanner) evaluateComments(
	ctx context.Context,
	num int,
	pr *githubv39.PullRequest,
	history *prHistory,
	lastCommitTime, lastCommentAddressedTime time.Time,
	lastCommentAddressedSHA, headSHA string,
	retry commentRetry,
) prCommentAnalysis {
	var analysis prCommentAnalysis

	comments := history.comments
	reviews := history.reviews
	revCommentsMap := history.revCommentsMap
	bots := s.cfg.AllowlistedBots

	hasNewHumanComments := false
	hasNewBotReviews := false

	updateOldestComment := func(t time.Time, author string, cType string, id int64) {
		if !t.IsZero() && (analysis.oldestCommentTime.IsZero() || t.Before(analysis.oldestCommentTime)) {
			analysis.oldestCommentTime = t
			analysis.oldestCommentAuthor = author
			analysis.oldestCommentType = cType
			analysis.oldestCommentID = id
		}
	}

	for _, c := range comments {
		isReviewer := conventions.IsReviewerBot(c.GetUser(), s.cfg.ReviewerLogins)
		if !isReviewer && conventions.ShouldIgnoreUser(c.GetUser(), s.cfg.GitHubLogin, bots) {
			continue
		}
		// The pull request's own author talking to itself is not feedback.
		if strings.EqualFold(c.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
			continue
		}
		if conventions.HasIgnorePrefix(c.GetBody(), s.cfg.TriggerLabel) {
			continue
		}
		if c.GetCreatedAt().After(lastCommitTime) && c.GetCreatedAt().After(lastCommentAddressedTime) {
			if !s.commentNeedsAttention(ctx, c.GetID(), retry) {
				continue
			}
			if isReviewer {
				hasNewBotReviews = true
			} else {
				hasNewHumanComments = true
			}
			analysis.unackCommentIDs = append(analysis.unackCommentIDs, c.GetID())
			author := ""
			if c.GetUser() != nil {
				author = c.GetUser().GetLogin()
			}
			updateOldestComment(c.GetCreatedAt(), author, "comment", c.GetID())
		}
	}

	// Also check inline PR review comments directly
	for _, r := range reviews {
		isReviewer := conventions.IsReviewerBot(r.GetUser(), s.cfg.ReviewerLogins)
		if !isReviewer && conventions.ShouldIgnoreUser(r.GetUser(), s.cfg.GitHubLogin, bots) {
			continue
		}
		if strings.EqualFold(r.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
			continue
		}
		if r.GetSubmittedAt().After(lastCommitTime) && r.GetSubmittedAt().After(lastCommentAddressedTime) {
			if conventions.HasIgnorePrefix(r.GetBody(), s.cfg.TriggerLabel) {
				continue
			}
			// A review with an empty body carries no instruction of its own -
			// its inline comments below are the feedback, and each of them is
			// judged on its own marks further down.
			//
			// This has to hold for the flags too, not just for the bookkeeping.
			// A review cannot carry a reaction - GitHub has nowhere to put one -
			// so an empty review that raised the flag by itself could never be
			// answered, and would keep the feedback outstanding however the
			// comments beneath it were marked.
			if strings.TrimSpace(r.GetBody()) != "" {
				if isReviewer {
					hasNewBotReviews = true
				} else {
					hasNewHumanComments = true
				}
				author := ""
				if r.GetUser() != nil {
					author = r.GetUser().GetLogin()
				}
				updateOldestComment(r.GetSubmittedAt(), author, "review", r.GetID())
			}
		}

		revComments := revCommentsMap[r.GetID()]
		for _, rc := range revComments {
			isInlineReviewer := conventions.IsReviewerBot(rc.GetUser(), s.cfg.ReviewerLogins)
			if !isInlineReviewer && conventions.ShouldIgnoreUser(rc.GetUser(), s.cfg.GitHubLogin, bots) {
				continue
			}
			if strings.EqualFold(rc.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
				continue
			}
			if rc.GetCreatedAt().After(lastCommitTime) && rc.GetCreatedAt().After(lastCommentAddressedTime) {
				if conventions.HasIgnorePrefix(rc.GetBody(), s.cfg.TriggerLabel) {
					continue
				}
				// Reading the marks costs a request, so it is asked last, of
				// the few comments that everything cheaper has already let
				// through.
				if !s.reviewCommentNeedsAttention(ctx, rc.GetID(), retry) {
					continue
				}
				if isInlineReviewer {
					hasNewBotReviews = true
				} else {
					hasNewHumanComments = true
				}
				analysis.unackPRCommentIDs = append(analysis.unackPRCommentIDs, rc.GetID())
				author := ""
				if rc.GetUser() != nil {
					author = rc.GetUser().GetLogin()
				}
				updateOldestComment(rc.GetCreatedAt(), author, "inline review comment", rc.GetID())
			}
		}
	}

	if hasNewHumanComments {
		analysis.hasNewComments = true
	} else if hasNewBotReviews {
		if lastCommentAddressedSHA != "" && lastCommentAddressedSHA == headSHA {
			klog.Infof("Skipping bot review feedback on PR #%d because an address-comments task already ran against SHA %s without resulting in a commit.", num, headSHA)
		} else {
			analysis.hasNewComments = true
		}
	}

	return analysis
}

// commentNeedsAttention reports whether a conversation comment is still waiting
// on the watcher, reading its reactions in the light of whether this is a retry.
//
// Outside a retry the reactions are taken at face value. Inside one, only the
// resolved mark still counts: the acknowledgement was written by the attempt
// being retried and means only that the watcher picked the comment up, which it
// then failed to act on. Resolved is left standing because it can only have
// come from an earlier attempt that succeeded on this same head, and that fix
// is already in the branch.
//
// GitHub has no remove-reaction call here, so this is the only place the marks
// of a failed attempt can be set aside.
func (s *Scanner) commentNeedsAttention(ctx context.Context, commentID int64, retry commentRetry) bool {
	return needsAttention(s.reactions.CommentState(ctx, commentID), retry)
}

// reviewCommentNeedsAttention is commentNeedsAttention for an inline review
// comment, whose reactions live on their own endpoint.
func (s *Scanner) reviewCommentNeedsAttention(ctx context.Context, commentID int64, retry commentRetry) bool {
	return needsAttention(s.reactions.ReviewCommentState(ctx, commentID), retry)
}

func needsAttention(state conventions.CommentState, retry commentRetry) bool {
	if retry.active {
		return !state.Resolved
	}
	return state.NeedsAttention()
}

// hasBotReviewAfterLastCommit reports whether the current head has already been
// reviewed by a bot, which is what stops a second review being queued for the
// same revision.
func hasBotReviewAfterLastCommit(reviews []*githubv39.PullRequestReview, lastCommitTime time.Time, headSHA, githubLogin string, bots []string) bool {
	for _, r := range reviews {
		if conventions.IsBotReply(r.GetUser(), githubLogin, bots) && (r.GetSubmittedAt().After(lastCommitTime) || r.GetCommitID() == headSHA) {
			return true
		}
	}
	return false
}

// getInvestigationCount counts how many times the watcher has investigated CI
// failures since the last thing that ought to reset its patience.
//
// The counter resets on a new commit or on any human comment, because either
// one changes the situation the previous attempts failed against. Without the
// reset a pull request a human has just given a hint on would stay stuck at the
// retry limit.
func getInvestigationCount(comments []*githubv39.IssueComment, lastCommitTime time.Time, allBotUsers []string, githubLogin string, bots []string, triggerLabel string) int {
	lastResetTime := lastCommitTime
	for _, c := range comments {
		isPoolBot := false
		for _, bot := range allBotUsers {
			if strings.EqualFold(c.GetUser().GetLogin(), bot) {
				isPoolBot = true
				break
			}
		}
		isHuman := !isPoolBot && !conventions.ShouldIgnoreUser(c.GetUser(), githubLogin, bots)
		if isHuman && conventions.HasIgnorePrefix(c.GetBody(), triggerLabel) {
			isHuman = false
		}
		if (isHuman || strings.Contains(c.GetBody(), "pausing automated investigation")) && c.GetCreatedAt().After(lastResetTime) {
			lastResetTime = c.GetCreatedAt()
		}
	}

	investigationCount := 0
	for _, c := range comments {
		isPoolBot := false
		for _, bot := range allBotUsers {
			if strings.EqualFold(c.GetUser().GetLogin(), bot) {
				isPoolBot = true
				break
			}
		}
		if isPoolBot &&
			strings.Contains(c.GetBody(), "started investigating CI check failures") &&
			c.GetCreatedAt().After(lastResetTime) {
			investigationCount++
		}
	}
	return investigationCount
}

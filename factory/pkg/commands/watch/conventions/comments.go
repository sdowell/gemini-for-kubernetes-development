package conventions

import (
	"context"
	"strings"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// HasIgnorePrefix reports whether a comment opts itself out of being acted on.
//
// The prefix is "/" + triggerLabel + "-ignore" on any line of the body. The
// '/overseer-ignore' spelling is always accepted, so the directive keeps
// working across deployments that renamed their trigger label.
func HasIgnorePrefix(body string, triggerLabel string) bool {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.ToLower(strings.TrimSpace(line))
		if strings.HasPrefix(trimmed, "/"+defaultPrefix+"-ignore") {
			return true
		}
		if triggerLabel != "" && !strings.EqualFold(triggerLabel, defaultPrefix) {
			prefix := "/" + strings.ToLower(triggerLabel) + "-ignore"
			if strings.HasPrefix(trimmed, prefix) {
				return true
			}
		}
	}
	return false
}

// CommentMarks is what the watcher has recorded about one comment, read back
// from its reactions.
//
// The reactions are the protocol, and deliberately so: they live where the
// reviewer can see them, they survive a watcher restart or a lost queue
// directory, and they are the only record that is per comment rather than per
// task. Reading all four marks from one listing, rather than asking four
// separate yes/no questions, is also what keeps a comment to a single request.
type CommentMarks struct {
	// Addressed is the watcher's '+1': the feedback was acted on.
	Addressed bool
	// Failed is the watcher's 'confused': it picked the comment up and the
	// attempt did not finish, so the feedback is still owed an answer.
	Failed bool
	// InFlight is the watcher's 'eyes': picked up, with no outcome recorded yet.
	InFlight bool
	// Rerun is a human's 'rocket': an explicit request to look again, which
	// overrides everything the watcher recorded.
	Rerun bool
	// failedID identifies the 'confused' reaction so that it can be withdrawn
	// once an attempt succeeds.
	failedID int64
}

// Outstanding reports whether the comment is still owed an answer.
//
// A failed attempt counts as outstanding, which is the entire point of the
// 'confused' mark: the task carrying that comment never got to it, and without
// the mark nothing on GitHub would ever show it as unanswered again. It is safe
// to trust because the mark is withdrawn as soon as an attempt succeeds, so it
// cannot outlive the failure it records.
//
// 'eyes' alone is not outstanding: the work has been picked up and its outcome
// is simply not known yet.
//
// Trusting that is only safe because the reactions are not the only record of
// an outcome. The task file is written first and these marks second, so a
// watcher that dies in between leaves a failure on disk that the next scan acts
// on - without which a lost 'confused' would leave the comment looking like
// work in progress for ever, which is the very way feedback used to go missing.
func (m CommentMarks) Outstanding() bool {
	switch {
	case m.Rerun:
		return true
	case m.Failed:
		return true
	case m.Addressed, m.InFlight:
		return false
	default:
		return true
	}
}

// Unresolved reports whether the comment lacks a successful outcome - it never
// got the '+1', or it carries a failure.
//
// It is deliberately broader than Outstanding in that it does not wait on
// 'eyes', and it is what a retry uses to decide which feedback to put back in
// front of the agent: a run that has already failed once should not also be
// trusted to have recorded what it did.
func (m CommentMarks) Unresolved() bool {
	return !m.Addressed || m.Failed
}

// readMarks classifies a comment's reactions. Who reacted is the whole
// question - the watcher's own marks say what it has done, a human's 'rocket'
// asks it to do the work again - so each reaction is attributed before it is
// counted.
func readMarks(reactions []*githubv39.Reaction, bots []string, selfLogin string) CommentMarks {
	var m CommentMarks
	for _, r := range reactions {
		isBot := ShouldIgnoreUser(r.GetUser(), selfLogin, bots)
		switch r.GetContent() {
		case "+1":
			m.Addressed = m.Addressed || isBot
		case "confused":
			if isBot {
				m.Failed = true
				m.failedID = r.GetID()
			}
		case "eyes":
			m.InFlight = m.InFlight || isBot
		case "rocket":
			m.Rerun = m.Rerun || !isBot
		}
	}
	return m
}

// IssueCommentMarks reads the watcher's record of a conversation comment.
//
// A comment whose reactions cannot be read comes back unmarked, which reads as
// outstanding. That errs towards doing the work twice rather than towards
// dropping feedback on the floor, which is the direction this whole mechanism
// exists to err in.
func IssueCommentMarks(ctx context.Context, client *github.Client, commentID int64, bots []string, selfLogin string) CommentMarks {
	reactions, err := client.IssueCommentReactions(ctx, commentID)
	if err != nil {
		klog.V(2).Infof("Failed to read reactions on comment %d: %v", commentID, err)
		return CommentMarks{}
	}
	return readMarks(reactions, bots, selfLogin)
}

// ReviewCommentMarks reads the watcher's record of an inline review comment.
func ReviewCommentMarks(ctx context.Context, client *github.Client, commentID int64, bots []string, selfLogin string) CommentMarks {
	reactions, err := client.PullRequestCommentReactions(ctx, commentID)
	if err != nil {
		klog.V(2).Infof("Failed to read reactions on review comment %d: %v", commentID, err)
		return CommentMarks{}
	}
	return readMarks(reactions, bots, selfLogin)
}

// ResolveCommentReactions closes out the acknowledgements on a pull request:
// every comment the watcher marked with 'eyes' when it picked the work up has
// the outcome of that work recorded on it.
//
// Conversation comments and inline review comments are both closed out. Leaving
// the inline ones unmarked would make them permanently indistinguishable from
// feedback nothing has ever been done about.
//
// Success also withdraws any 'confused' left by an earlier attempt. Reactions
// are a set and not a log - GitHub keeps one of each content per account - so a
// stale failure mark cannot be superseded by a newer one and would sit beside
// the '+1' for ever, keeping an answered comment looking unanswered.
//
// It is called from the task lifecycle rather than from a scan, which is why it
// lives here rather than with the scanner that applied the 'eyes'.
func ResolveCommentReactions(ctx context.Context, client *github.Client, prNum int, succeeded bool, bots []string, selfLogin string) {
	outcome := "confused"
	if succeeded {
		outcome = "+1"
	}

	if comments, err := client.ListIssueComments(ctx, prNum); err == nil {
		for _, c := range comments {
			if ShouldIgnoreUser(c.GetUser(), selfLogin, bots) {
				continue
			}
			marks := IssueCommentMarks(ctx, client, c.GetID(), bots, selfLogin)
			if !marks.InFlight {
				continue
			}
			if err := client.AddIssueCommentReaction(ctx, c.GetID(), outcome); err != nil {
				klog.Warningf("Failed to resolve reaction on comment %d: %v", c.GetID(), err)
			}
			if succeeded && marks.Failed {
				if err := client.RemoveIssueCommentReaction(ctx, c.GetID(), marks.failedID); err != nil {
					klog.Warningf("Failed to withdraw the failure mark on comment %d: %v", c.GetID(), err)
				}
			}
		}
	}

	revComments, err := client.ListAllReviewComments(ctx, prNum)
	if err != nil {
		return
	}
	for _, rc := range revComments {
		if ShouldIgnoreUser(rc.GetUser(), selfLogin, bots) {
			continue
		}
		marks := ReviewCommentMarks(ctx, client, rc.GetID(), bots, selfLogin)
		if !marks.InFlight {
			continue
		}
		if err := client.AddPullRequestCommentReaction(ctx, rc.GetID(), outcome); err != nil {
			klog.Warningf("Failed to resolve reaction on review comment %d: %v", rc.GetID(), err)
		}
		if succeeded && marks.Failed {
			if err := client.RemovePullRequestCommentReaction(ctx, rc.GetID(), marks.failedID); err != nil {
				klog.Warningf("Failed to withdraw the failure mark on review comment %d: %v", rc.GetID(), err)
			}
		}
	}
}

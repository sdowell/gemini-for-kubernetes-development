package conventions

import (
	"context"
	"strings"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"
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

// CommentResolverClient is the GitHub access ResolveCommentReactions needs: the
// comments on a pull request, of both kinds, the reactions already on them, and
// the ability to add one more.
type CommentResolverClient interface {
	ReactionLister
	// ListIssueComments returns every comment on an issue or pull request.
	ListIssueComments(ctx context.Context, number int) ([]*githubv39.IssueComment, error)
	// ListPullRequestComments returns every inline review comment on a pull request.
	ListPullRequestComments(ctx context.Context, number int) ([]*githubv39.PullRequestComment, error)
	// AddIssueCommentReaction records a reaction on a conversation comment.
	AddIssueCommentReaction(ctx context.Context, commentID int64, content string) error
	// AddPullRequestCommentReaction records a reaction on an inline review comment.
	AddPullRequestCommentReaction(ctx context.Context, commentID int64, content string) error
}

// ResolveCommentReactions closes out the acknowledgements on a pull request:
// every comment the watcher marked as picked up gets the outcome reaction once
// the task has finished.
//
// It is called from the task lifecycle rather than from a scan, which is why it
// lives here rather than with the scanner that applied the acknowledgement.
//
// Only acknowledged comments are touched. A comment that already carries an
// outcome belongs to an earlier task, and one the watcher never marked was
// never this task's to answer.
//
// Both kinds of comment are walked, because the scanner acknowledges both. An
// inline comment left with only its pickup mark would have no record of what
// became of it, and the reviewer who wrote it would be left watching a 👀 that
// never resolves.
func ResolveCommentReactions(ctx context.Context, client CommentResolverClient, prNum int, resolution Reaction, bots []string, selfLogin string) {
	interpreter := NewReactionInterpreter(client, selfLogin, bots)

	if comments, err := client.ListIssueComments(ctx, prNum); err == nil {
		for _, c := range comments {
			if ShouldIgnoreUser(c.GetUser(), selfLogin, bots) {
				continue
			}
			if !interpreter.CommentState(ctx, c.GetID()).Acknowledged {
				continue
			}
			if err := client.AddIssueCommentReaction(ctx, c.GetID(), string(resolution)); err != nil {
				klog.Warningf("Failed to resolve reaction on comment %d: %v", c.GetID(), err)
			}
		}
	}

	// A failure listing one kind does not stop the other: half the marks
	// resolved is better than none, and the unresolved half keeps its
	// acknowledgement, which still reads as handled.
	reviewComments, err := client.ListPullRequestComments(ctx, prNum)
	if err != nil {
		return
	}
	for _, rc := range reviewComments {
		if ShouldIgnoreUser(rc.GetUser(), selfLogin, bots) {
			continue
		}
		if !interpreter.ReviewCommentState(ctx, rc.GetID()).Acknowledged {
			continue
		}
		if err := client.AddPullRequestCommentReaction(ctx, rc.GetID(), string(resolution)); err != nil {
			klog.Warningf("Failed to resolve reaction on review comment %d: %v", rc.GetID(), err)
		}
	}
}

package github

import (
	"context"
	"fmt"

	githubv39 "github.com/google/go-github/v39/github"
)

// AddComment posts a comment on an issue or pull request. GitHub models pull
// request conversations as issue comments, so this covers both.
func (c *Client) AddComment(ctx context.Context, number int, body string) error {
	if !c.Ready() {
		return errNoClient
	}

	comment := &githubv39.IssueComment{Body: githubv39.String(body)}
	if _, _, err := c.gh.Issues.CreateComment(ctx, c.owner, c.repo, number, comment); err != nil {
		return fmt.Errorf("commenting on #%d: %w", number, err)
	}
	return nil
}

// ListIssueComments returns every comment on an issue or pull request,
// following pagination.
func (c *Client) ListIssueComments(ctx context.Context, number int) ([]*githubv39.IssueComment, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	var all []*githubv39.IssueComment
	opt := &githubv39.IssueListCommentsOptions{
		ListOptions: githubv39.ListOptions{PerPage: 100},
	}
	for {
		comments, resp, err := c.gh.Issues.ListComments(ctx, c.owner, c.repo, number, opt)
		if err != nil {
			return nil, err
		}
		all = append(all, comments...)
		if resp == nil || resp.NextPage == 0 {
			return all, nil
		}
		opt.Page = resp.NextPage
	}
}

// IssueCommentReactions returns the reactions recorded on a single comment.
//
// Who reacted matters as much as what they reacted with, so the reactions are
// returned whole rather than reduced to a boolean here: deciding whether a mark
// came from a bot or a human is the caller's policy, not this package's.
func (c *Client) IssueCommentReactions(ctx context.Context, commentID int64) ([]*githubv39.Reaction, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	reactions, _, err := c.gh.Reactions.ListIssueCommentReactions(ctx, c.owner, c.repo, commentID, nil)
	if err != nil {
		return nil, fmt.Errorf("listing reactions on comment %d: %w", commentID, err)
	}
	return reactions, nil
}

// AddIssueCommentReaction records a reaction on an issue or pull request
// conversation comment.
func (c *Client) AddIssueCommentReaction(ctx context.Context, commentID int64, content string) error {
	if !c.Ready() {
		return errNoClient
	}

	if _, _, err := c.gh.Reactions.CreateIssueCommentReaction(ctx, c.owner, c.repo, commentID, content); err != nil {
		return fmt.Errorf("adding reaction %q to comment %d: %w", content, commentID, err)
	}
	return nil
}

// AddPullRequestCommentReaction records a reaction on an inline review comment.
//
// Inline review comments are a distinct resource from conversation comments and
// their IDs come from a different namespace, which is why this cannot be folded
// into AddIssueCommentReaction.
func (c *Client) AddPullRequestCommentReaction(ctx context.Context, commentID int64, content string) error {
	if !c.Ready() {
		return errNoClient
	}

	if _, _, err := c.gh.Reactions.CreatePullRequestCommentReaction(ctx, c.owner, c.repo, commentID, content); err != nil {
		return fmt.Errorf("adding reaction %q to review comment %d: %w", content, commentID, err)
	}
	return nil
}

// PullRequestCommentReactions returns the reactions recorded on a single inline
// review comment.
func (c *Client) PullRequestCommentReactions(ctx context.Context, commentID int64) ([]*githubv39.Reaction, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	reactions, _, err := c.gh.Reactions.ListPullRequestCommentReactions(ctx, c.owner, c.repo, commentID, nil)
	if err != nil {
		return nil, fmt.Errorf("listing reactions on review comment %d: %w", commentID, err)
	}
	return reactions, nil
}

// RemoveIssueCommentReaction withdraws a reaction the caller previously
// recorded on a conversation comment.
//
// Withdrawal exists because reactions are a set and not a log: GitHub keeps at
// most one of each content per account, so a mark that is no longer true cannot
// be superseded by writing a newer one - it has to be taken back.
func (c *Client) RemoveIssueCommentReaction(ctx context.Context, commentID, reactionID int64) error {
	if !c.Ready() {
		return errNoClient
	}

	if _, err := c.gh.Reactions.DeleteIssueCommentReaction(ctx, c.owner, c.repo, commentID, reactionID); err != nil {
		return fmt.Errorf("removing reaction %d from comment %d: %w", reactionID, commentID, err)
	}
	return nil
}

// RemovePullRequestCommentReaction withdraws a reaction the caller previously
// recorded on an inline review comment.
func (c *Client) RemovePullRequestCommentReaction(ctx context.Context, commentID, reactionID int64) error {
	if !c.Ready() {
		return errNoClient
	}

	if _, err := c.gh.Reactions.DeletePullRequestCommentReaction(ctx, c.owner, c.repo, commentID, reactionID); err != nil {
		return fmt.Errorf("removing reaction %d from review comment %d: %w", reactionID, commentID, err)
	}
	return nil
}

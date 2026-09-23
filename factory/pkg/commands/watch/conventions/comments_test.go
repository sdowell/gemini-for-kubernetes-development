package conventions

import (
	"context"
	"errors"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"
)

// fakeResolverClient is a GitHub stand-in that records the outcome reactions it
// is asked to write, keeping the two kinds of comment apart.
type fakeResolverClient struct {
	fakeReactionLister

	comments       []*githubv39.IssueComment
	reviewComments []*githubv39.PullRequestComment

	// acknowledged are the comment IDs whose reactions should report the
	// watcher's pickup mark. Anything not listed reads as unmarked.
	acknowledged       map[int64]bool
	reviewAcknowledged map[int64]bool

	listErr       error
	reviewListErr error
	addErr        error

	resolved       []int64
	reviewResolved []int64
}

func (f *fakeResolverClient) ListIssueComments(_ context.Context, _ int) ([]*githubv39.IssueComment, error) {
	return f.comments, f.listErr
}

func (f *fakeResolverClient) ListPullRequestComments(_ context.Context, _ int) ([]*githubv39.PullRequestComment, error) {
	return f.reviewComments, f.reviewListErr
}

func (f *fakeResolverClient) IssueCommentReactions(_ context.Context, commentID int64) ([]*githubv39.Reaction, error) {
	if f.acknowledged[commentID] {
		return []*githubv39.Reaction{reaction(ReactionAcknowledged, testSelfLogin)}, nil
	}
	return nil, nil
}

func (f *fakeResolverClient) PullRequestCommentReactions(_ context.Context, commentID int64) ([]*githubv39.Reaction, error) {
	if f.reviewAcknowledged[commentID] {
		return []*githubv39.Reaction{reaction(ReactionAcknowledged, testSelfLogin)}, nil
	}
	return nil, nil
}

func (f *fakeResolverClient) AddIssueCommentReaction(_ context.Context, commentID int64, _ string) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.resolved = append(f.resolved, commentID)
	return nil
}

func (f *fakeResolverClient) AddPullRequestCommentReaction(_ context.Context, commentID int64, _ string) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.reviewResolved = append(f.reviewResolved, commentID)
	return nil
}

func humanComment(id int64) *githubv39.IssueComment {
	return &githubv39.IssueComment{ID: githubv39.Int64(id), User: &githubv39.User{Login: githubv39.String(testHuman)}}
}

func humanReviewComment(id int64) *githubv39.PullRequestComment {
	return &githubv39.PullRequestComment{ID: githubv39.Int64(id), User: &githubv39.User{Login: githubv39.String(testHuman)}}
}

// TestResolveCommentReactionsBothKinds is the point of the whole exercise:
// an inline review comment that was picked up gets its outcome recorded, just
// as a conversation comment does. Before this it received the pickup mark and
// nothing else, so nothing on GitHub ever said what became of it.
func TestResolveCommentReactionsBothKinds(t *testing.T) {
	client := &fakeResolverClient{
		comments:           []*githubv39.IssueComment{humanComment(1), humanComment(2)},
		reviewComments:     []*githubv39.PullRequestComment{humanReviewComment(10), humanReviewComment(20)},
		acknowledged:       map[int64]bool{1: true},
		reviewAcknowledged: map[int64]bool{20: true},
	}

	ResolveCommentReactions(context.Background(), client, 7, ReactionResolved, testBots(), testSelfLogin)

	// Only the acknowledged ones are touched: an unmarked comment was never
	// this task's to answer.
	if len(client.resolved) != 1 || client.resolved[0] != 1 {
		t.Errorf("resolved conversation comments = %v, want [1]", client.resolved)
	}
	if len(client.reviewResolved) != 1 || client.reviewResolved[0] != 20 {
		t.Errorf("resolved review comments = %v, want [20]", client.reviewResolved)
	}
}

// TestResolveCommentReactionsSkipsOwnAccount checks that the watcher does not
// mark its own chatter. Its comments are never feedback, so they are never
// picked up, and resolving them would be meaningless noise in the thread.
func TestResolveCommentReactionsSkipsOwnAccount(t *testing.T) {
	client := &fakeResolverClient{
		comments: []*githubv39.IssueComment{
			{ID: githubv39.Int64(1), User: &githubv39.User{Login: githubv39.String(testSelfLogin)}},
		},
		reviewComments: []*githubv39.PullRequestComment{
			{ID: githubv39.Int64(10), User: &githubv39.User{Login: githubv39.String(testSelfLogin)}},
		},
		acknowledged:       map[int64]bool{1: true},
		reviewAcknowledged: map[int64]bool{10: true},
	}

	ResolveCommentReactions(context.Background(), client, 7, ReactionResolved, testBots(), testSelfLogin)

	if len(client.resolved) != 0 || len(client.reviewResolved) != 0 {
		t.Errorf("resolved %v and %v, want nothing: both comments are the watcher's own", client.resolved, client.reviewResolved)
	}
}

// TestResolveCommentReactionsListFailureIsPartial covers one kind of comment
// being unreachable. The other kind is still resolved: a comment left with only
// its pickup mark still reads as handled, so finishing half the job is better
// than abandoning it.
func TestResolveCommentReactionsListFailureIsPartial(t *testing.T) {
	t.Run("conversation comments unreachable", func(t *testing.T) {
		client := &fakeResolverClient{
			listErr:            errors.New("github is down"),
			reviewComments:     []*githubv39.PullRequestComment{humanReviewComment(10)},
			reviewAcknowledged: map[int64]bool{10: true},
		}

		ResolveCommentReactions(context.Background(), client, 7, ReactionResolved, testBots(), testSelfLogin)

		if len(client.reviewResolved) != 1 {
			t.Errorf("resolved review comments = %v, want [10] despite the other listing failing", client.reviewResolved)
		}
	})

	t.Run("review comments unreachable", func(t *testing.T) {
		client := &fakeResolverClient{
			comments:      []*githubv39.IssueComment{humanComment(1)},
			acknowledged:  map[int64]bool{1: true},
			reviewListErr: errors.New("github is down"),
		}

		ResolveCommentReactions(context.Background(), client, 7, ReactionResolved, testBots(), testSelfLogin)

		if len(client.resolved) != 1 {
			t.Errorf("resolved conversation comments = %v, want [1] despite the other listing failing", client.resolved)
		}
	})
}

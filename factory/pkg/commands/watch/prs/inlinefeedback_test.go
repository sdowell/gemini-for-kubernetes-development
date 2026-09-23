package prs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// TestEvaluate_InlineCommentReactions covers the marks on an inline review
// comment deciding whether its feedback is still outstanding.
//
// Inline comments were acknowledged when picked up but never read back, so the
// 👀 the watcher had just written meant nothing to the next scan and the same
// feedback could be sent again. These cases are the conversation-comment rules
// applied to the other kind of comment.
func TestEvaluate_InlineCommentReactions(t *testing.T) {
	tests := []struct {
		name string
		// reactions are the marks already on the inline comment.
		reactions  []*githubv39.Reaction
		wantQueued bool
	}{
		{
			name:       "an unmarked comment is outstanding",
			wantQueued: true,
		},
		{
			name:       "a comment the watcher picked up is not",
			reactions:  []*githubv39.Reaction{watcherReaction(conventions.ReactionAcknowledged)},
			wantQueued: false,
		},
		{
			name:       "a comment the watcher answered is not",
			reactions:  []*githubv39.Reaction{watcherReaction(conventions.ReactionResolved)},
			wantQueued: false,
		},
		{
			name:       "a comment the watcher gave up on is not",
			reactions:  []*githubv39.Reaction{watcherReaction(conventions.ReactionFailed)},
			wantQueued: false,
		},
		{
			// The human override: 🚀 reopens work the watcher has marked.
			name: "a comment a human asked to revisit is outstanding again",
			reactions: []*githubv39.Reaction{
				watcherReaction(conventions.ReactionFailed),
				humanReaction(conventions.ReactionRedo),
			},
			wantQueued: true,
		},
		{
			// Resolved is the one mark 🚀 does not lift: that fix is already in
			// the branch, so there is nothing to do again.
			name: "a resolved comment stays resolved",
			reactions: []*githubv39.Reaction{
				watcherReaction(conventions.ReactionResolved),
				humanReaction(conventions.ReactionRedo),
			},
			wantQueued: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &inlineFeedbackServer{reactions: tc.reactions}
			tempDir := t.TempDir()

			s, _ := newTestScanner(t, tempDir, testOpts{
				GitHub:         srv.start(t),
				BotUsers:       []string{"bot1"},
				GitHubLogin:    "bot1",
				TriggerLabel:   "factory",
				ReviewerLogins: []string{"reviewbot"},
			})

			s.evaluateAll(context.Background(), []*githubv39.Issue{{
				Number:    githubv39.Int(inlinePRNumber),
				Assignees: []*githubv39.User{{Login: stringPtr("bot1")}},
				Labels:    []*githubv39.Label{{Name: stringPtr("factory")}},
			}})

			queued := taskWasQueued(t, tempDir, "task-pr-10-comments.yaml")
			if queued != tc.wantQueued {
				t.Errorf("address-comments task queued = %v, want %v", queued, tc.wantQueued)
			}
			// Whatever is queued must also be marked, or the next scan would
			// queue it a second time.
			if tc.wantQueued && !srv.acknowledged() {
				t.Error("the inline comment was not acknowledged")
			}
		})
	}
}

const (
	inlinePRNumber  = 10
	inlineCommentID = int64(555)
	inlineReviewID  = int64(77)
	inlineHeadSHA   = "sha-1234"
)

func watcherReaction(content conventions.Reaction) *githubv39.Reaction {
	return &githubv39.Reaction{
		Content: stringPtr(string(content)),
		User:    &githubv39.User{Login: stringPtr("bot1")},
	}
}

func humanReaction(content conventions.Reaction) *githubv39.Reaction {
	return &githubv39.Reaction{
		Content: stringPtr(string(content)),
		User:    &githubv39.User{Login: stringPtr("reviewer-human")},
	}
}

// inlineFeedbackServer is a GitHub stand-in for a pull request whose only
// feedback is one inline review comment left by a human after the last commit.
type inlineFeedbackServer struct {
	mu sync.Mutex

	// reactions are the marks already on that comment.
	reactions []*githubv39.Reaction
	// acked records the reaction the scanner wrote to the comment.
	acked bool
}

func (s *inlineFeedbackServer) acknowledged() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked
}

func (s *inlineFeedbackServer) start(t *testing.T) *githubv39.Client {
	t.Helper()

	base := "/repos/test-owner/test-repo"
	commitTime := time.Now().Add(-3 * time.Hour)
	commentTime := time.Now().Add(-2 * time.Hour)
	inlineReactionsPath := fmt.Sprintf("%s/pulls/comments/%d/reactions", base, inlineCommentID)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == inlineReactionsPath:
			s.mu.Lock()
			s.acked = true
			s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(&githubv39.Reaction{})

		case r.Method == http.MethodGet && r.URL.Path == inlineReactionsPath:
			s.mu.Lock()
			defer s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(s.reactions)

		case r.URL.Path == fmt.Sprintf("%s/pulls/%d", base, inlinePRNumber):
			_ = json.NewEncoder(w).Encode(&githubv39.PullRequest{
				Number:    githubv39.Int(inlinePRNumber),
				Mergeable: githubv39.Bool(true),
				State:     stringPtr("open"),
				User:      &githubv39.User{Login: stringPtr("bot1")},
				Head:      &githubv39.PullRequestBranch{SHA: stringPtr(inlineHeadSHA)},
				CreatedAt: timePtr(time.Now().Add(-24 * time.Hour)),
			})

		case r.URL.Path == fmt.Sprintf("%s/pulls/%d/commits", base, inlinePRNumber):
			_ = json.NewEncoder(w).Encode([]*githubv39.RepositoryCommit{{
				SHA:    stringPtr(inlineHeadSHA),
				Commit: &githubv39.Commit{Committer: &githubv39.CommitAuthor{Date: timePtr(commitTime)}},
			}})

		case r.URL.Path == fmt.Sprintf("%s/pulls/%d/reviews", base, inlinePRNumber):
			// An empty review body carries no instruction of its own; the
			// inline comment below it is the feedback under test.
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestReview{{
				ID:          githubv39.Int64(inlineReviewID),
				User:        &githubv39.User{Login: stringPtr("reviewer-human")},
				Body:        stringPtr(""),
				SubmittedAt: timePtr(commentTime),
			}})

		case r.URL.Path == fmt.Sprintf("%s/pulls/%d/reviews/%d/comments", base, inlinePRNumber, inlineReviewID):
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestComment{{
				ID:        githubv39.Int64(inlineCommentID),
				User:      &githubv39.User{Login: stringPtr("reviewer-human")},
				Body:      stringPtr("please rename this"),
				Path:      stringPtr("main.go"),
				CreatedAt: timePtr(commentTime),
			}})

		case r.URL.Path == fmt.Sprintf("%s/issues/%d/comments", base, inlinePRNumber):
			_ = json.NewEncoder(w).Encode([]*githubv39.IssueComment{})

		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []*githubv39.CheckRun{}})

		default:
			// Statuses, labels, assignee writes and anything else the
			// evaluation touches on its way past.
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	gh.BaseURL, _ = url.Parse(server.URL + "/")
	return gh
}

// taskWasQueued reports whether the named task reached the incoming queue.
func taskWasQueued(t *testing.T, tempDir, filename string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(tempDir, "incoming", filename))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("checking for queued task: %v", err)
	}
	return err == nil
}

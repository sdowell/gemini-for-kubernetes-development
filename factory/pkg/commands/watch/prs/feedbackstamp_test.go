package prs

import (
	"context"
	"errors"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
)

// TestNoteFeedbackOutcome covers what an address-comments task leaves behind.
//
// The stamp used to be written the moment the task was queued, so an attempt
// that died partway through parked its feedback exactly as a successful one
// did. The reviewer was left waiting on a reply nobody was going to write.
func TestNoteFeedbackOutcome(t *testing.T) {
	queued := time.Now().Add(-time.Hour)

	tests := []struct {
		name string
		// before is the state already recorded for the pull request.
		before  prState
		task    *api.QueueTask
		taskErr error

		wantTime time.Time
		wantSHA  string
	}{
		{
			name: "a success is dated by when the attempt was queued",
			task: &api.QueueTask{
				Number:      inlinePRNumber,
				CommitSHA:   inlineHeadSHA,
				EnqueuedAt:  queued,
				StartedAt:   queued.Add(5 * time.Minute),
				CompletedAt: queued.Add(40 * time.Minute),
			},
			wantTime: queued,
			wantSHA:  inlineHeadSHA,
		},
		{
			// Nothing was addressed, so nothing is parked and the next scan
			// finds the same comments still outstanding.
			name: "a failure records nothing",
			task: &api.QueueTask{
				Number:      inlinePRNumber,
				CommitSHA:   inlineHeadSHA,
				EnqueuedAt:  queued,
				CompletedAt: queued.Add(40 * time.Minute),
			},
			taskErr: errors.New("the sandbox died"),
		},
		{
			// Two attempts can finish out of order. The recorded time is a
			// high-water mark, so the later one stands.
			name:   "an older success does not move the mark backwards",
			before: prState{lastCommentAddressedTime: queued, lastCommentAddressedSHA: inlineHeadSHA},
			task: &api.QueueTask{
				Number:     inlinePRNumber,
				EnqueuedAt: queued.Add(-time.Hour),
			},
			wantTime: queued,
			wantSHA:  inlineHeadSHA,
		},
		{
			// A task recovered from disk may carry no enqueue time.
			name: "a task with no enqueue time falls back to when it started",
			task: &api.QueueTask{
				Number:      inlinePRNumber,
				StartedAt:   queued.Add(5 * time.Minute),
				CompletedAt: queued.Add(40 * time.Minute),
			},
			wantTime: queued.Add(5 * time.Minute),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestScanner(t, t.TempDir(), testOpts{})
			s.state.set(inlinePRNumber, tc.before)

			s.NoteFeedbackOutcome(tc.task, tc.taskErr)

			got := s.state.get(inlinePRNumber)
			if !got.lastCommentAddressedTime.Equal(tc.wantTime) {
				t.Errorf("lastCommentAddressedTime = %v, want %v", got.lastCommentAddressedTime, tc.wantTime)
			}
			if got.lastCommentAddressedSHA != tc.wantSHA {
				t.Errorf("lastCommentAddressedSHA = %q, want %q", got.lastCommentAddressedSHA, tc.wantSHA)
			}
		})
	}
}

// TestNoteFeedbackOutcome_IgnoresTasksWithNoPullRequest guards the two shapes
// that would otherwise write state under pull request zero.
func TestNoteFeedbackOutcome_IgnoresTasksWithNoPullRequest(t *testing.T) {
	s, _ := newTestScanner(t, t.TempDir(), testOpts{})

	s.NoteFeedbackOutcome(nil, nil)
	s.NoteFeedbackOutcome(&api.QueueTask{EnqueuedAt: time.Now()}, nil)

	if got := s.state.get(0); !got.lastCommentAddressedTime.IsZero() {
		t.Errorf("recorded state for pull request 0: %v", got.lastCommentAddressedTime)
	}
}

// TestEvaluate_QueueingFeedbackLeavesNoStamp pins the other half: the scanner
// records nothing when it hands the work out, so the only thing that can park
// the feedback is the task finishing.
func TestEvaluate_QueueingFeedbackLeavesNoStamp(t *testing.T) {
	tempDir := t.TempDir()
	s := newFeedbackScanner(t, tempDir)

	s.evaluateAll(context.Background(), []*githubv39.Issue{feedbackIssue()})

	if !taskWasQueued(t, tempDir, "task-pr-10-comments.yaml") {
		t.Fatal("expected the address-comments task to be queued")
	}
	if stamped := s.state.get(inlinePRNumber).lastCommentAddressedTime; !stamped.IsZero() {
		t.Errorf("queueing the task recorded the feedback as addressed at %v", stamped)
	}
}

// TestEvaluate_AddressedFeedbackIsNotQueuedAgain is the loop the stamp exists
// to close: once an attempt has succeeded, the comments it covered are done
// with, whatever the agent decided to do about them.
func TestEvaluate_AddressedFeedbackIsNotQueuedAgain(t *testing.T) {
	tempDir := t.TempDir()
	s := newFeedbackScanner(t, tempDir)

	s.NoteFeedbackOutcome(&api.QueueTask{
		Number:     inlinePRNumber,
		EnqueuedAt: time.Now(),
	}, nil)

	s.evaluateAll(context.Background(), []*githubv39.Issue{feedbackIssue()})

	if taskWasQueued(t, tempDir, "task-pr-10-comments.yaml") {
		t.Error("feedback that a successful attempt covered was queued again")
	}
}

// newFeedbackScanner builds a scanner over a pull request carrying one
// unaddressed human comment.
func newFeedbackScanner(t *testing.T, tempDir string) *Scanner {
	t.Helper()
	srv := &botReplyServer{}
	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:         srv.start(t),
		BotUsers:       []string{"bot1"},
		GitHubLogin:    "bot1",
		TriggerLabel:   "factory",
		ReviewerLogins: []string{"reviewbot"},
	})
	return s
}

func feedbackIssue() *githubv39.Issue {
	return &githubv39.Issue{
		Number:    githubv39.Int(inlinePRNumber),
		Assignees: []*githubv39.User{{Login: stringPtr("bot1")}},
		Labels:    []*githubv39.Label{{Name: stringPtr("factory")}},
	}
}

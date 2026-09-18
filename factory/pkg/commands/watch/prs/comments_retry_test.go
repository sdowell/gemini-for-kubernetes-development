package prs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
)

// commentsFixture is a pull request with review feedback on it, served over an
// httptest server so that a whole evaluation cycle can be run against it.
//
// The reactions are the part that matters to these tests. They are the record
// of what the watcher has done with each comment, and the only record of it
// that a failed task leaves behind: 'eyes' when the work was picked up, '+1'
// when it was finished, 'confused' when the attempt died.
type commentsFixture struct {
	prNum      int
	headSHA    string
	commitTime time.Time

	mu sync.Mutex
	// comments are the pull request's top-level comments.
	comments []*githubv39.IssueComment
	// marks are the watcher's reactions on each comment, by comment ID.
	marks map[int64][]string
	// labelsAdded and commentsPosted record the writes the scanner made.
	labelsAdded    []string
	commentsPosted []string
}

func newCommentsFixture(t *testing.T) (*commentsFixture, *githubv39.Client) {
	t.Helper()

	f := &commentsFixture{
		prNum:      10,
		headSHA:    "sha-1234",
		commitTime: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		marks:      map[int64][]string{},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")

		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/repos/test-owner/test-repo/pulls/10":
			mergeable := true
			_ = json.NewEncoder(w).Encode(&githubv39.PullRequest{
				Number:    githubv39.Int(f.prNum),
				Mergeable: &mergeable,
				State:     stringPtr("open"),
				User:      &githubv39.User{Login: stringPtr("bot1")},
				Head:      &githubv39.PullRequestBranch{SHA: stringPtr(f.headSHA)},
				CreatedAt: &f.commitTime,
			})
		case r.Method == http.MethodGet && path == "/repos/test-owner/test-repo/pulls/10/commits":
			_ = json.NewEncoder(w).Encode([]*githubv39.RepositoryCommit{{
				SHA:    stringPtr(f.headSHA),
				Commit: &githubv39.Commit{Committer: &githubv39.CommitAuthor{Date: &f.commitTime}},
			}})
		case r.Method == http.MethodGet && path == "/repos/test-owner/test-repo/issues/10/comments":
			_ = json.NewEncoder(w).Encode(f.comments)
		case r.Method == http.MethodGet && path == "/repos/test-owner/test-repo/pulls/10/reviews":
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestReview{})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/reactions"):
			var reactions []*githubv39.Reaction
			for id, contents := range f.marks {
				if !strings.Contains(path, "/comments/"+strconv.FormatInt(id, 10)+"/") {
					continue
				}
				for _, content := range contents {
					reactions = append(reactions, &githubv39.Reaction{
						Content: stringPtr(content),
						User:    &githubv39.User{Login: stringPtr("bot1")},
					})
				}
			}
			_ = json.NewEncoder(w).Encode(reactions)
		case r.Method == http.MethodPost && path == "/repos/test-owner/test-repo/issues/10/labels":
			var labels []string
			_ = json.NewDecoder(r.Body).Decode(&labels)
			f.labelsAdded = append(f.labelsAdded, labels...)
			_ = json.NewEncoder(w).Encode([]*githubv39.Label{})
		case r.Method == http.MethodPost && path == "/repos/test-owner/test-repo/issues/10/comments":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.commentsPosted = append(f.commentsPosted, body["body"])
			_ = json.NewEncoder(w).Encode(&githubv39.IssueComment{})
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	parsed, _ := url.Parse(server.URL + "/")
	gh.BaseURL = parsed
	gh.UploadURL = parsed
	return f, gh
}

// addComment records a human comment carrying the watcher's marks, if any:
// "eyes" for work picked up, "+1" for work finished, "confused" for an attempt
// that failed.
func (f *commentsFixture) addComment(id int64, at time.Time, marks ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append(f.comments, &githubv39.IssueComment{
		ID:        githubv39.Int64(id),
		User:      &githubv39.User{Login: stringPtr("human-alice")},
		CreatedAt: &at,
		Body:      stringPtr("Please rename this field"),
	})
	f.marks[id] = marks
}

func (f *commentsFixture) writes() (labels []string, comments []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.labelsAdded...), append([]string(nil), f.commentsPosted...)
}

// prIssueFixture is the issue view of the fixture's pull request, which is what
// a scan cycle is handed.
func prIssueFixture() *githubv39.Issue {
	return &githubv39.Issue{
		Number:    githubv39.Int(10),
		Assignees: []*githubv39.User{{Login: stringPtr("bot1")}},
		Labels:    []*githubv39.Label{{Name: stringPtr("factory")}},
	}
}

// writeProcessedCommentsTask lays down the record of a finished
// address-comments run, which is where the retry decision is read from.
func writeProcessedCommentsTask(t *testing.T, processedDir string, task *api.QueueTask) {
	t.Helper()
	data, err := yaml.Marshal(task)
	if err != nil {
		t.Fatalf("marshaling task: %v", err)
	}
	if err := os.WriteFile(filepath.Join(processedDir, "task-pr-10-comments.yaml"), data, 0644); err != nil {
		t.Fatalf("writing processed task: %v", err)
	}
}

func readQueuedTask(t *testing.T, path string) *api.QueueTask {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var task api.QueueTask
	if err := yaml.Unmarshal(data, &task); err != nil {
		t.Fatalf("parsing queued task: %v", err)
	}
	return &task
}

// TestEvaluate_RetriesFailedCommentsTask covers the case the retry exists for: a
// task that failed before it could address the feedback it was handed.
//
// The comment carries the marks such a task leaves behind - 'eyes' from when it
// was picked up, 'confused' from when it died - and the failed task file is the
// record of how many attempts have been spent.
func TestEvaluate_RetriesFailedCommentsTask(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "confused")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	processedDir := filepath.Join(tempDir, "processed")
	writeProcessedCommentsTask(t, processedDir, &api.QueueTask{
		Type:             api.TypePRComments,
		Number:           10,
		Status:           api.StatusFailed,
		Error:            "gemini api quota exhausted",
		TriggerEventTime: f.commitTime.Add(time.Hour),
		StartedAt:        f.commitTime.Add(90 * time.Minute),
		CommitSHA:        f.headSHA,
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	queued := filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml")
	task := readQueuedTask(t, queued)
	if task == nil {
		t.Fatal("no address-comments task was queued after the previous one failed")
	}
	if task.Retries != 1 {
		t.Errorf("retries = %d, want 1", task.Retries)
	}

	// The retry is idempotent: while it is queued there is nothing further to
	// do, however many times the pull request is evaluated.
	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})
	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	if again := readQueuedTask(t, queued); again == nil || again.Retries != 1 {
		t.Errorf("queued task after re-evaluating = %+v, want the same single retry", again)
	}
	if entries, err := os.ReadDir(filepath.Join(tempDir, "incoming")); err != nil {
		t.Fatalf("reading incoming: %v", err)
	} else if len(entries) != 1 {
		t.Errorf("incoming holds %d tasks, want 1", len(entries))
	}
}

// TestEvaluate_CommentsRetryResetsBudgetOnNewFeedback checks that feedback
// written after the failed attempt started restarts the budget: the reviewer
// has said something new, so this is no longer a repetition of work that got
// nowhere.
func TestEvaluate_CommentsRetryResetsBudgetOnNewFeedback(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "confused")
	f.addComment(101, f.commitTime.Add(2*time.Hour))

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	writeProcessedCommentsTask(t, filepath.Join(tempDir, "processed"), &api.QueueTask{
		Type:      api.TypePRComments,
		Number:    10,
		Status:    api.StatusFailed,
		Retries:   2,
		StartedAt: f.commitTime.Add(90 * time.Minute),
		CommitSHA: f.headSHA,
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml"))
	if task == nil {
		t.Fatal("no address-comments task was queued")
	}
	if task.Retries != 0 {
		t.Errorf("retries = %d, want 0 (new feedback restarts the budget)", task.Retries)
	}
}

// TestEvaluate_CommentsRetryBudgetSurvivesANewCommit is the counterpart: moving
// to a new revision must not refund the budget.
//
// An attempt that dies partway through has usually committed something first,
// so a new head is the normal state of affairs on a retry rather than evidence
// of progress. Refunding on it would let every attempt reset its own budget and
// retry for ever - and a commit is not an answer to review feedback in any
// case, only the '+1' is.
func TestEvaluate_CommentsRetryBudgetSurvivesANewCommit(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "confused")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	writeProcessedCommentsTask(t, filepath.Join(tempDir, "processed"), &api.QueueTask{
		Type:      api.TypePRComments,
		Number:    10,
		Status:    api.StatusFailed,
		Retries:   2,
		StartedAt: f.commitTime.Add(90 * time.Minute),
		// The attempt ran against an older revision and pushed a commit of its
		// own before it died.
		CommitSHA: "sha-before-the-failed-attempt",
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml"))
	if task == nil {
		t.Fatal("no address-comments task was queued")
	}
	if task.Retries != 3 {
		t.Errorf("retries = %d, want 3 (a new commit does not refund the budget)", task.Retries)
	}
}

// TestEvaluate_FailedTaskWithoutAFailureMarkIsRetried covers the crash window
// between the two records of an outcome.
//
// The task file is written first and the reactions second, so a watcher that
// dies in between leaves a failure recorded on disk and comments still marked
// only as in flight - which on their own would read as work in progress and be
// left alone for ever. The task file is what rescues them.
func TestEvaluate_FailedTaskWithoutAFailureMarkIsRetried(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	writeProcessedCommentsTask(t, filepath.Join(tempDir, "processed"), &api.QueueTask{
		Type:      api.TypePRComments,
		Number:    10,
		Status:    api.StatusFailed,
		Error:     "gemini api quota exhausted",
		StartedAt: f.commitTime.Add(90 * time.Minute),
		CommitSHA: f.headSHA,
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml"))
	if task == nil {
		t.Fatal("no address-comments task was queued for a failure only the task file records")
	}
	if task.Retries != 1 {
		t.Errorf("retries = %d, want 1", task.Retries)
	}
}

// TestEvaluate_CommentsRetryBudgetExhausted checks that the retry is bounded,
// and that running out is announced rather than leaving the pull request
// looking like it is still being worked on.
func TestEvaluate_CommentsRetryBudgetExhausted(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "confused")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	writeProcessedCommentsTask(t, filepath.Join(tempDir, "processed"), &api.QueueTask{
		Type:      api.TypePRComments,
		Number:    10,
		Status:    api.StatusFailed,
		Error:     "gemini api quota exhausted",
		Retries:   maxCommentRetries,
		StartedAt: f.commitTime.Add(90 * time.Minute),
		CommitSHA: f.headSHA,
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	if task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml")); task != nil {
		t.Errorf("queued a %d-th retry, want the budget to stop at %d", task.Retries, maxCommentRetries)
	}

	// Giving up is announced once. A second cycle - as happens the moment a
	// human removes the stop label - must not re-apply it.
	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})
	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	labels, comments := f.writes()
	stopLabels := 0
	for _, l := range labels {
		if l == "factory/stop" {
			stopLabels++
		}
	}
	if stopLabels != 1 {
		t.Errorf("stop label applied %d times, want exactly 1", stopLabels)
	}
	if len(comments) != 1 || !strings.Contains(comments[0], "has **not** been addressed") {
		t.Errorf("comments posted = %v, want exactly one saying the feedback was not addressed", comments)
	}
}

// TestEvaluate_CompletedCommentsTaskIsNotRetried guards the other half of the
// rule: only a failure is worth repeating. A task that ran to completion has
// had its say, and its comments carry the '+1' that says so, so re-queueing it
// would loop on feedback the agent judged it had already answered.
func TestEvaluate_CompletedCommentsTaskIsNotRetried(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "+1")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	writeProcessedCommentsTask(t, filepath.Join(tempDir, "processed"), &api.QueueTask{
		Type:      api.TypePRComments,
		Number:    10,
		Status:    api.StatusCompleted,
		CommitSHA: f.headSHA,
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	if task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml")); task != nil {
		t.Errorf("queued %+v, want nothing after a successful address-comments task", task)
	}
}

// TestEvaluate_ConfusedCommentIsPickedUpWithoutATaskFile checks that GitHub
// alone is enough to reopen feedback.
//
// The task file records the budget, but it is not what says the work is owed:
// a watcher that lost its queue directory, or one that never wrote the file,
// must still see a comment its own 'confused' mark says it failed on.
func TestEvaluate_ConfusedCommentIsPickedUpWithoutATaskFile(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes", "confused")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml"))
	if task == nil {
		t.Fatal("no address-comments task was queued for a comment marked as failed")
	}
	if task.Retries != 0 {
		t.Errorf("retries = %d, want 0 (no previous attempt is recorded)", task.Retries)
	}
}

// TestEvaluate_InFlightCommentIsNotRequeued guards the other direction: 'eyes'
// on its own means the work has been picked up and its outcome is not known
// yet, which must not be read as a failure. Treating it as one would queue a
// second task for every comment already being worked on.
func TestEvaluate_InFlightCommentIsNotRequeued(t *testing.T) {
	tempDir := t.TempDir()
	f, gh := newCommentsFixture(t)
	f.addComment(100, f.commitTime.Add(time.Hour), "eyes")

	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:       gh,
		Kube:         newTestKubeClient(),
		BotUsers:     []string{"bot1"},
		GitHubLogin:  "bot1",
		TriggerLabel: "factory",
	})

	s.evaluateAll(context.Background(), []*githubv39.Issue{prIssueFixture()})

	if task := readQueuedTask(t, filepath.Join(tempDir, "incoming", "task-pr-10-comments.yaml")); task != nil {
		t.Errorf("queued %+v, want nothing while the comment is still being worked on", task)
	}
}

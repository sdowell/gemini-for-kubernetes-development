package prs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"gopkg.in/yaml.v3"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

// evictionFixture describes the GitHub state served to a merge queue eviction
// test: a bot pull request that merges cleanly, is green on its own head and
// carries an approving bot review, so that the eviction is the only thing that
// can stand between it and the ready-for-human label.
type evictionFixture struct {
	// commitTime is the committer date of the pull request's head commit.
	commitTime time.Time
	// evictionTime is when the pull request was removed from the merge
	// queue. A zero value means it never was.
	evictionTime time.Time
	// reason is the removal reason GitHub reports.
	reason string
}

const (
	evictionTestHeadSHA       = "sha-head"
	evictionTestMergeGroupSHA = "sha-merge-group"
	evictionTestFailedCheck   = "golangci-lint"
)

// evictionServer serves the fixture and records the labels the scanner adds.
func evictionServer(t *testing.T, f evictionFixture) (*githubv39.Client, func() []string) {
	t.Helper()

	prNum := 10
	mergeable := true
	var mu sync.Mutex
	var addedLabels []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/pulls/10":
			_ = json.NewEncoder(w).Encode(&githubv39.PullRequest{
				Number:    &prNum,
				Mergeable: &mergeable,
				State:     stringPtr("open"),
				User:      &githubv39.User{Login: stringPtr("bot1")},
				Head:      &githubv39.PullRequestBranch{SHA: stringPtr(evictionTestHeadSHA)},
				Base:      &githubv39.PullRequestBranch{Ref: stringPtr("master")},
				CreatedAt: &f.commitTime,
			})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/pulls/10/commits":
			_ = json.NewEncoder(w).Encode([]*githubv39.RepositoryCommit{{
				SHA:    stringPtr(evictionTestHeadSHA),
				Commit: &githubv39.Commit{Committer: &githubv39.CommitAuthor{Date: &f.commitTime}},
			}})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/issues/10/comments":
			_ = json.NewEncoder(w).Encode([]*githubv39.IssueComment{})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/pulls/10/comments":
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestComment{})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/pulls/10/reviews":
			_ = json.NewEncoder(w).Encode([]*githubv39.PullRequestReview{{
				User:        &githubv39.User{Login: stringPtr("reviewbot")},
				CommitID:    stringPtr(evictionTestHeadSHA),
				State:       stringPtr("APPROVED"),
				SubmittedAt: &f.commitTime,
			}})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/commits/"+evictionTestHeadSHA+"/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"check_runs": []interface{}{}})
		case r.Method == "GET" && r.URL.Path == "/repos/test-owner/test-repo/commits/"+evictionTestMergeGroupSHA+"/check-runs":
			completed := f.evictionTime.Add(-time.Minute)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"check_runs": []*githubv39.CheckRun{{
				Name:        stringPtr(evictionTestFailedCheck),
				Status:      stringPtr("completed"),
				Conclusion:  stringPtr("failure"),
				CompletedAt: &githubv39.Timestamp{Time: completed},
			}}})
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/statuses"):
			_ = json.NewEncoder(w).Encode([]*githubv39.RepoStatus{})
		case r.Method == "POST" && r.URL.Path == "/repos/test-owner/test-repo/issues/10/labels":
			var labels []string
			_ = json.NewDecoder(r.Body).Decode(&labels)
			mu.Lock()
			addedLabels = append(addedLabels, labels...)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]*githubv39.Label{})
		case r.URL.Path == "/graphql":
			body, _ := io.ReadAll(r.Body)
			switch {
			case strings.Contains(string(body), "mergeQueueEntry"):
				_, _ = w.Write([]byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":null}}}}`))
			case strings.Contains(string(body), "REMOVED_FROM_MERGE_QUEUE_EVENT"):
				nodes := "[]"
				if !f.evictionTime.IsZero() {
					nodes = fmt.Sprintf(`[{"createdAt":%q,"reason":%q,"beforeCommit":{"oid":%q}}]`,
						f.evictionTime.UTC().Format(time.RFC3339), f.reason, evictionTestMergeGroupSHA)
				}
				_, _ = fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"timelineItems":{"nodes":%s}}}}}`, nodes)
			default:
				t.Errorf("unexpected GraphQL query: %s", body)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	t.Cleanup(server.Close)

	origEndpoint := github.GraphQLEndpoint
	github.GraphQLEndpoint = server.URL + "/graphql"
	t.Cleanup(func() { github.GraphQLEndpoint = origEndpoint })
	t.Setenv("MANUAL_PAT", "dummy-token")

	ghClient := githubv39.NewClient(nil)
	ghClient.BaseURL, _ = url.Parse(server.URL + "/")
	return ghClient, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), addedLabels...)
	}
}

// evictionTestPRIssue is the issue side of the pull request evictionServer serves.
func evictionTestPRIssue() *githubv39.Issue {
	prNum := 10
	return &githubv39.Issue{
		Number:    &prNum,
		Assignees: []*githubv39.User{{Login: stringPtr("bot1")}},
		Labels:    []*githubv39.Label{{Name: stringPtr("factory")}},
	}
}

func newEvictionTestScanner(t *testing.T, ghClient *githubv39.Client) (*Scanner, string) {
	t.Helper()
	tempDir := t.TempDir()
	s, _ := newTestScanner(t, tempDir, testOpts{
		GitHub:         ghClient,
		BotUsers:       []string{"bot1"},
		GitHubLogin:    "bot1",
		TriggerLabel:   "factory",
		ReviewerLogins: []string{"reviewbot"},
	})
	return s, filepath.Join(tempDir, "incoming")
}

// TestEvaluate_MergeQueueEviction_QueuesRebaseAndFix is the case that motivated
// the eviction check: the pull request merges cleanly and is green on its own
// head, but the merge queue threw it out because it no longer builds on top of
// the base branch. Without the check the watcher saw nothing wrong and handed
// it to a human as ready.
func TestEvaluate_MergeQueueEviction_QueuesRebaseAndFix(t *testing.T) {
	commitTime := time.Now().Add(-2 * time.Hour)
	evictionTime := time.Now().Add(-time.Hour).Truncate(time.Second)
	ghClient, addedLabels := evictionServer(t, evictionFixture{
		commitTime:   commitTime,
		evictionTime: evictionTime,
		reason:       "failed_checks",
	})
	s, incomingDir := newEvictionTestScanner(t, ghClient)

	s.evaluateAll(context.Background(), []*githubv39.Issue{evictionTestPRIssue()})

	taskPath := filepath.Join(incomingDir, "task-pr-10-iterate.yaml")
	data, err := os.ReadFile(taskPath)
	if err != nil {
		t.Fatalf("expected a rebase-and-fix task to be queued: %v", err)
	}
	var task api.QueueTask
	if err := yaml.Unmarshal(data, &task); err != nil {
		t.Fatalf("unmarshaling task: %v", err)
	}

	if task.Type != api.TypePRIterate {
		t.Errorf("task type = %q, want %q", task.Type, api.TypePRIterate)
	}
	if task.TriggerReason != api.TriggerReasonPRMergeQueueEvicted {
		t.Errorf("trigger reason = %q, want %q", task.TriggerReason, api.TriggerReasonPRMergeQueueEvicted)
	}
	if !task.TriggerEventTime.Equal(evictionTime) {
		t.Errorf("trigger event time = %v, want the eviction time %v", task.TriggerEventTime, evictionTime)
	}
	if task.CommitSHA != evictionTestHeadSHA {
		t.Errorf("commit SHA = %q, want %q", task.CommitSHA, evictionTestHeadSHA)
	}
	if !strings.Contains(task.TriggerNotes, evictionTestFailedCheck) {
		t.Errorf("trigger notes %q do not name the failed merge queue check %q", task.TriggerNotes, evictionTestFailedCheck)
	}
	if len(task.Instructions) != 1 {
		t.Fatalf("expected 1 instruction, got %d: %v", len(task.Instructions), task.Instructions)
	}
	for _, want := range []string{"merge queue", evictionTestMergeGroupSHA, evictionTestFailedCheck, "master"} {
		if !strings.Contains(task.Instructions[0], want) {
			t.Errorf("instructions do not mention %q:\n%s", want, task.Instructions[0])
		}
	}

	if labels := addedLabels(); len(labels) != 0 {
		t.Errorf("expected no labels while the eviction is being fixed, got %v", labels)
	}
}

// TestEvaluate_MergeQueueEviction_QueuedOncePerEviction checks that the task is
// the answer to one eviction: once it has run, a pull request it pushed
// nothing to is not sent back to the agent until the queue evicts it again.
func TestEvaluate_MergeQueueEviction_QueuedOncePerEviction(t *testing.T) {
	ghClient, addedLabels := evictionServer(t, evictionFixture{
		commitTime:   time.Now().Add(-2 * time.Hour),
		evictionTime: time.Now().Add(-time.Hour),
		reason:       "failed_checks",
	})
	s, incomingDir := newEvictionTestScanner(t, ghClient)

	s.evaluateAll(context.Background(), []*githubv39.Issue{evictionTestPRIssue()})

	taskPath := filepath.Join(incomingDir, "task-pr-10-iterate.yaml")
	if _, err := os.Stat(taskPath); err != nil {
		t.Fatalf("expected a rebase-and-fix task to be queued: %v", err)
	}

	// The task runs and finishes without pushing anything.
	if err := s.queue.RemovePendingTasksForNumber(10); err != nil {
		t.Fatalf("removing pending task: %v", err)
	}
	if _, err := os.Stat(taskPath); !os.IsNotExist(err) {
		t.Fatalf("expected the pending task to be gone, stat err = %v", err)
	}

	s.evaluateAll(context.Background(), []*githubv39.Issue{evictionTestPRIssue()})

	if _, err := os.Stat(taskPath); !os.IsNotExist(err) {
		t.Errorf("expected no second task for the same eviction, stat err = %v", err)
	}
	// With the eviction answered and nothing else outstanding, the pull
	// request goes back to a human, who can read the agent's report.
	if labels := addedLabels(); len(labels) != 1 || labels[0] != "factory/ready-for-human" {
		t.Errorf("expected ready-for-human once the eviction was answered, got %v", labels)
	}
}

// TestEvaluate_MergeQueueEviction_Ignored covers the evictions that are not
// the watcher's to act on.
func TestEvaluate_MergeQueueEviction_Ignored(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		fixture evictionFixture
	}{
		{
			name: "never evicted",
			fixture: evictionFixture{
				commitTime: now.Add(-time.Hour),
			},
		},
		{
			// Something was pushed after the eviction: a person or an
			// earlier task already acted on it.
			name: "commit newer than eviction",
			fixture: evictionFixture{
				commitTime:   now.Add(-time.Hour),
				evictionTime: now.Add(-2 * time.Hour),
				reason:       "failed_checks",
			},
		},
		{
			// Someone took it out of the queue on purpose.
			name: "manual removal",
			fixture: evictionFixture{
				commitTime:   now.Add(-2 * time.Hour),
				evictionTime: now.Add(-time.Hour),
				reason:       "dequeued",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ghClient, addedLabels := evictionServer(t, tc.fixture)
			s, incomingDir := newEvictionTestScanner(t, ghClient)

			s.evaluateAll(context.Background(), []*githubv39.Issue{evictionTestPRIssue()})

			if _, err := os.Stat(filepath.Join(incomingDir, "task-pr-10-iterate.yaml")); !os.IsNotExist(err) {
				t.Errorf("expected no rebase-and-fix task, stat err = %v", err)
			}
			if labels := addedLabels(); len(labels) != 1 || labels[0] != "factory/ready-for-human" {
				t.Errorf("expected ready-for-human, got %v", labels)
			}
		})
	}
}

func TestMergeQueueEviction_IsFailure(t *testing.T) {
	tests := []struct {
		reason string
		want   bool
	}{
		{"failed_checks", true},
		{"FAILED_CHECKS", true},
		{"merge_conflict", true},
		{"dequeued", false},
		{"", false},
	}
	for _, tc := range tests {
		e := &github.MergeQueueEviction{Reason: tc.reason}
		if got := e.IsFailure(); got != tc.want {
			t.Errorf("IsFailure(%q) = %v, want %v", tc.reason, got, tc.want)
		}
	}
	var nilEviction *github.MergeQueueEviction
	if nilEviction.IsFailure() {
		t.Errorf("IsFailure() on nil eviction = true, want false")
	}
}

// TestProcessedPRStates_MergeQueueEviction checks that a restart remembers
// which eviction was answered, and does not mistake the answer for a conflict
// rebase of the head it ran on.
func TestProcessedPRStates_MergeQueueEviction(t *testing.T) {
	evictionTime := time.Date(2026, 10, 12, 17, 41, 7, 0, time.UTC)
	states := processedPRStates(map[string]*api.QueueTask{
		"task-pr-10-iterate.yaml": {
			Type:             api.TypePRIterate,
			Number:           10,
			Status:           api.StatusCompleted,
			CommitSHA:        evictionTestHeadSHA,
			CompletedAt:      evictionTime.Add(time.Hour),
			TriggerEventTime: evictionTime,
			TriggerReason:    api.TriggerReasonPRMergeQueueEvicted,
		},
	})
	got := states[10]
	if !got.lastEvictionHandledAt.Equal(evictionTime) {
		t.Errorf("lastEvictionHandledAt = %v, want %v", got.lastEvictionHandledAt, evictionTime)
	}
	if got.lastIteratedSHA != "" {
		t.Errorf("lastIteratedSHA = %q, want it left unset by an eviction task", got.lastIteratedSHA)
	}
}

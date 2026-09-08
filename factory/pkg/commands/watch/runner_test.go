package watch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/common"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
)

func TestBuildTaskCommandArgs(t *testing.T) {
	w := &Watcher{
		RootFlags: common.RootFlags{
			Namespace:        "test-namespace",
			Image:            "custom-image:v1",
			DiskSize:         "50Gi",
			EphemeralStorage: "20Gi",
			CPURequest:       "2",
			CPULimit:         "4",
			MemoryRequest:    "4Gi",
			MemoryLimit:      "8Gi",
		},
		Flags: Flags{
			TaskTimeout: 30 * time.Minute,
		},
	}

	t.Run("issue-fix task", func(t *testing.T) {
		task := &api.QueueTask{
			Type:   api.TypeIssueFix,
			URL:    "https://github.com/test-owner/test-repo/issues/123",
			Number: 123,
		}
		args := w.buildTaskCommandArgs(task, "coder-bot")
		if len(args) == 0 {
			t.Fatalf("expected args, got empty slice")
		}
		if args[0] != "fix" {
			t.Errorf("expected command 'fix', got %q", args[0])
		}

		expectedFlags := map[string]string{
			"--url":                   task.URL,
			"--instruction":           "Fix this issue",
			"--namespace":             "test-namespace",
			"--user":                  "coder-bot",
			"--image":                 "custom-image:v1",
			"--workspace-disk-size":   "50Gi",
			"--ephemeral-storage":     "20Gi",
			"--cpu-request":           "2",
			"--cpu-limit":             "4",
			"--memory-request":        "4Gi",
			"--memory-limit":          "8Gi",
			"--timeout":               "30m0s",
			"--abort-on-cancel=false": "",
		}

		for flag, expectedVal := range expectedFlags {
			found := false
			for i, a := range args {
				if a == flag {
					found = true
					if expectedVal != "" && i+1 < len(args) && args[i+1] != expectedVal {
						t.Errorf("flag %s has value %q, want %q", flag, args[i+1], expectedVal)
					}
					break
				}
			}
			if !found {
				t.Errorf("missing expected flag %s in args: %v", flag, args)
			}
		}
	})

	t.Run("pr-review task with instructions", func(t *testing.T) {
		task := &api.QueueTask{
			Type:         api.TypePRReview,
			URL:          "https://github.com/test-owner/test-repo/pull/456",
			Number:       456,
			Instructions: []string{"check security", "check unit tests"},
		}
		args := w.buildTaskCommandArgs(task, "reviewer-bot")
		if len(args) == 0 {
			t.Fatalf("expected args, got empty slice")
		}
		if args[0] != "pr" || args[1] != "review" {
			t.Errorf("expected command 'pr review', got %v", args[:2])
		}

		// Verify instructions
		var instructionsFound []string
		for i, a := range args {
			if a == "--instruction" && i+1 < len(args) {
				instructionsFound = append(instructionsFound, args[i+1])
			}
		}
		if len(instructionsFound) != 2 || instructionsFound[0] != "check security" || instructionsFound[1] != "check unit tests" {
			t.Errorf("instructions = %v, want ['check security', 'check unit tests']", instructionsFound)
		}
	})

	t.Run("agent-chore task with session-id", func(t *testing.T) {
		task := &api.QueueTask{
			Type:      api.TypeAgentChore,
			URL:       "https://github.com/test-owner/test-repo/issues/789",
			Number:    789,
			AgentFile: ".agents/chore.md",
			SessionID: "issue-789",
		}
		args := w.buildTaskCommandArgs(task, "chore-bot")
		if len(args) == 0 {
			t.Fatalf("expected args, got empty slice")
		}
		if args[0] != "agent" || args[1] != "create" {
			t.Errorf("expected command 'agent create', got %v", args[:2])
		}

		sessionFound := false
		for i, a := range args {
			if a == "--session-id" && i+1 < len(args) && args[i+1] == "issue-789" {
				sessionFound = true
				break
			}
		}
		if !sessionFound {
			t.Errorf("missing --session-id issue-789 in args: %v", args)
		}
	})

	t.Run("unknown task type returns nil", func(t *testing.T) {
		task := &api.QueueTask{
			Type: "unknown-type",
		}
		args := w.buildTaskCommandArgs(task, "bot")
		if args != nil {
			t.Errorf("expected nil args for unknown type, got %v", args)
		}
	})
}

func TestShouldPostStartComment(t *testing.T) {
	expectedBody := "🤖 AI Factory started resolving merge conflicts / rebasing this pull request in a sandbox."

	t.Run("Posts comment on fresh task", func(t *testing.T) {
		tempDir := t.TempDir()
		processedDir := filepath.Join(tempDir, "processed")
		_ = os.MkdirAll(processedDir, 0755)

		w := &Watcher{
			processedDir: processedDir,
			Flags: Flags{
				Repo: RepoFlag{
					Owner: "test-owner",
					Repo:  "test-repo",
				},
			},
			githubLogin: "bot",
		}

		task := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "abc1234",
		}

		shouldComment, body := w.shouldPostStartComment(context.Background(), task, "task-pr-42-iterate.yaml")
		if !shouldComment {
			t.Errorf("expected shouldComment=true for fresh task, got false")
		}
		if body != expectedBody {
			t.Errorf("expected body %q, got %q", expectedBody, body)
		}
	})

	t.Run("Suppresses comment when matching commit SHA exists in processedDir", func(t *testing.T) {
		tempDir := t.TempDir()
		processedDir := filepath.Join(tempDir, "processed")
		_ = os.MkdirAll(processedDir, 0755)

		prevTask := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "abc1234",
			Status:    api.StatusFailed,
		}
		prevData, _ := yaml.Marshal(prevTask)
		_ = os.WriteFile(filepath.Join(processedDir, "task-pr-42-iterate.yaml"), prevData, 0644)

		w := &Watcher{
			processedDir: processedDir,
			Flags: Flags{
				Repo: RepoFlag{
					Owner: "test-owner",
					Repo:  "test-repo",
				},
			},
			githubLogin: "bot",
		}

		task := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "abc1234",
		}

		shouldComment, _ := w.shouldPostStartComment(context.Background(), task, "task-pr-42-iterate.yaml")
		if shouldComment {
			t.Errorf("expected shouldComment=false when commit SHA already in processedDir, got true")
		}
	})

	t.Run("Allows comment when processedDir has different commit SHA", func(t *testing.T) {
		tempDir := t.TempDir()
		processedDir := filepath.Join(tempDir, "processed")
		_ = os.MkdirAll(processedDir, 0755)

		prevTask := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "old-sha-0000",
			Status:    api.StatusCompleted,
		}
		prevData, _ := yaml.Marshal(prevTask)
		_ = os.WriteFile(filepath.Join(processedDir, "task-pr-42-iterate.yaml"), prevData, 0644)

		w := &Watcher{
			processedDir: processedDir,
			Flags: Flags{
				Repo: RepoFlag{
					Owner: "test-owner",
					Repo:  "test-repo",
				},
			},
			githubLogin: "bot",
		}

		task := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "new-sha-1111",
		}

		shouldComment, body := w.shouldPostStartComment(context.Background(), task, "task-pr-42-iterate.yaml")
		if !shouldComment {
			t.Errorf("expected shouldComment=true for new commit SHA, got false")
		}
		if body != expectedBody {
			t.Errorf("expected body %q, got %q", expectedBody, body)
		}
	})

	t.Run("Suppresses comment when GitHub already has duplicate recent comment", func(t *testing.T) {
		tempDir := t.TempDir()
		processedDir := filepath.Join(tempDir, "processed")
		_ = os.MkdirAll(processedDir, 0755)

		comments := []*githubv39.IssueComment{
			{
				User: &githubv39.User{Login: stringPtr("bot")},
				Body: stringPtr(expectedBody),
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/comments") {
				rw.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(rw).Encode(comments)
				return
			}
			http.NotFound(rw, r)
		}))
		defer server.Close()

		ghClient := githubv39.NewClient(nil)
		baseURL, _ := url.Parse(server.URL + "/")
		ghClient.BaseURL = baseURL

		w := &Watcher{
			processedDir: processedDir,
			Flags: Flags{
				Repo: RepoFlag{
					Owner: "test-owner",
					Repo:  "test-repo",
				},
			},
			ghClient:    ghClient,
			githubLogin: "bot",
		}

		task := &api.QueueTask{
			Type:      api.TypePRIterate,
			Number:    42,
			CommitSHA: "sha-different",
		}

		shouldComment, _ := w.shouldPostStartComment(context.Background(), task, "task-pr-42-iterate.yaml")
		if shouldComment {
			t.Errorf("expected shouldComment=false when duplicate comment exists on GitHub, got true")
		}
	})
}


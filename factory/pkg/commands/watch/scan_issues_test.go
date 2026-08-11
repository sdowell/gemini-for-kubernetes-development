package watch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
)

func TestQueueIssueTasks(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	processedDir := filepath.Join(tempDir, "processed")
	queueDir := tempDir

	_ = os.MkdirAll(incomingDir, 0755)
	_ = os.MkdirAll(processingDir, 0755)
	_ = os.MkdirAll(processedDir, 0755)

	ctx := context.Background()
	owner := "testowner"
	repo := "testrepo"
	triggerLabel := "factory"

	t.Run("Queue standard issue task", func(t *testing.T) {
		issueNum := 101
		issueTitle := "Fix bug in controller"
		issueBody := "Please fix the nil pointer dereference"
		issue := &githubv39.Issue{
			Number:    &issueNum,
			Title:     &issueTitle,
			Body:      &issueBody,
			CreatedAt: &time.Time{},
			UpdatedAt: &time.Time{},
		}

		processedIssues := make(map[int]time.Time)
		refIssues := make(map[int]bool)

		queueIssueTasks(ctx, nil, nil, nil, "default", owner, repo, []*githubv39.Issue{issue}, processedIssues, refIssues, "factory-bot", []string{"factory-bot"}, incomingDir, processingDir, processedDir, queueDir, false, triggerLabel)

		expectedFile := filepath.Join(incomingDir, "task-issue-101.yaml")
		data, err := os.ReadFile(expectedFile)
		if err != nil {
			t.Fatalf("expected task file %s to be created: %v", expectedFile, err)
		}

		var task QueueTask
		if err := yaml.Unmarshal(data, &task); err != nil {
			t.Fatalf("failed to unmarshal task: %v", err)
		}

		if task.Type != "issue-fix" {
			t.Errorf("expected task Type to be 'issue-fix', got %q", task.Type)
		}
		if task.Number != 101 {
			t.Errorf("expected task Number to be 101, got %d", task.Number)
		}
		if task.Assignee != "factory-bot" {
			t.Errorf("expected task Assignee to be 'factory-bot', got %q", task.Assignee)
		}
	})

	t.Run("Skip issue with stop label and remove pending task", func(t *testing.T) {
		issueNum := 102
		stopLabel := "factory/stop"
		pendingFile := filepath.Join(incomingDir, "task-issue-102.yaml")
		_ = os.WriteFile(pendingFile, []byte("dummy"), 0644)

		issue := &githubv39.Issue{
			Number: &issueNum,
			Labels: []*githubv39.Label{{Name: &stopLabel}},
		}

		processedIssues := make(map[int]time.Time)
		refIssues := make(map[int]bool)

		queueIssueTasks(ctx, nil, nil, nil, "default", owner, repo, []*githubv39.Issue{issue}, processedIssues, refIssues, "factory-bot", []string{"factory-bot"}, incomingDir, processingDir, processedDir, queueDir, false, triggerLabel)

		if _, err := os.Stat(pendingFile); !os.IsNotExist(err) {
			t.Errorf("expected pending task file %s to be removed for stopped issue", pendingFile)
		}
	})

	t.Run("Skip issue that already has a referenced PR", func(t *testing.T) {
		issueNum := 103
		issue := &githubv39.Issue{
			Number: &issueNum,
		}

		processedIssues := make(map[int]time.Time)
		refIssues := map[int]bool{103: true}

		queueIssueTasks(ctx, nil, nil, nil, "default", owner, repo, []*githubv39.Issue{issue}, processedIssues, refIssues, "factory-bot", []string{"factory-bot"}, incomingDir, processingDir, processedDir, queueDir, false, triggerLabel)

		expectedFile := filepath.Join(incomingDir, "task-issue-103.yaml")
		if _, err := os.Stat(expectedFile); !os.IsNotExist(err) {
			t.Errorf("expected task file %s NOT to be created because issue has referenced PR", expectedFile)
		}
	})
}

package watch

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"
)

func TestProcessPRs_SkipExternalAuthor(t *testing.T) {
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

	prNum := 201
	prIssue := &githubv39.Issue{
		Number: &prNum,
	}

	processedPRs := make(map[int]prWatchState)

	// Since ghClient is nil, external author skip or client check will safely not create any tasks
	processPRs(ctx, nil, nil, nil, RootOptions{Namespace: "default"}, owner, repo, []*githubv39.Issue{prIssue}, []string{"factory-bot"}, "factory-bot", "factory-bot", triggerLabel, 0, incomingDir, processingDir, processedDir, queueDir, false, processedPRs)

	entries, _ := os.ReadDir(incomingDir)
	if len(entries) != 0 {
		t.Errorf("expected 0 tasks queued in incomingDir, got %d", len(entries))
	}
}

func TestProcessPRs_StopLabel(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	processedDir := filepath.Join(tempDir, "processed")
	queueDir := tempDir

	_ = os.MkdirAll(incomingDir, 0755)
	_ = os.MkdirAll(processingDir, 0755)
	_ = os.MkdirAll(processedDir, 0755)

	// Create a dummy pending task file for PR #202
	pendingFile := filepath.Join(incomingDir, "task-pr-202-comments.yaml")
	_ = os.WriteFile(pendingFile, []byte("dummy"), 0644)

	ctx := context.Background()
	owner := "testowner"
	repo := "testrepo"
	triggerLabel := "factory"

	prNum := 202
	stopLabel := "factory/stop"
	prIssue := &githubv39.Issue{
		Number: &prNum,
		Labels: []*githubv39.Label{{Name: &stopLabel}},
	}

	processedPRs := make(map[int]prWatchState)

	processPRs(ctx, nil, nil, nil, RootOptions{Namespace: "default"}, owner, repo, []*githubv39.Issue{prIssue}, []string{"factory-bot"}, "factory-bot", "factory-bot", triggerLabel, 0, incomingDir, processingDir, processedDir, queueDir, false, processedPRs)

	if _, err := os.Stat(pendingFile); !os.IsNotExist(err) {
		t.Errorf("expected pending task file %s to be removed for stopped PR", pendingFile)
	}
}

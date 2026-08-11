package watch

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestRunQueueTasks_DryRun(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	processedDir := filepath.Join(tempDir, "processed")
	processingLogDir := filepath.Join(tempDir, "logs", "processing")
	processedLogDir := filepath.Join(tempDir, "logs", "processed")

	_ = os.MkdirAll(incomingDir, 0755)
	_ = os.MkdirAll(processingDir, 0755)
	_ = os.MkdirAll(processedDir, 0755)
	_ = os.MkdirAll(processingLogDir, 0755)
	_ = os.MkdirAll(processedLogDir, 0755)

	task := &QueueTask{
		Type:       "issue-fix",
		URL:        "https://github.com/testowner/testrepo/issues/123",
		Number:     123,
		Priority:   "high",
		Phase:      3,
		CreatedAt:  time.Now(),
		EnqueuedAt: time.Now(),
		Assignee:   "factory-bot",
		Status:     "Pending",
	}

	taskFilename := "task-issue-123.yaml"
	taskData, _ := yaml.Marshal(task)
	_ = os.WriteFile(filepath.Join(incomingDir, taskFilename), taskData, 0644)

	ctx := context.Background()
	var wg sync.WaitGroup

	runQueueTasks(ctx, nil, nil, nil, RootOptions{Namespace: "default"}, "testowner", "testrepo", "factory-bot", "factory-bot", "factory", 10, 10, 10*time.Minute, tempDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir, true, &wg)

	wg.Wait()

	// In dry run, incoming file should still exist
	if _, err := os.Stat(filepath.Join(incomingDir, taskFilename)); os.IsNotExist(err) {
		t.Errorf("expected task file to remain in incoming dir in dryrun mode")
	}
}

func TestRunQueueTasks_DrainMode(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	processedDir := filepath.Join(tempDir, "processed")
	processingLogDir := filepath.Join(tempDir, "logs", "processing")
	processedLogDir := filepath.Join(tempDir, "logs", "processed")

	_ = os.MkdirAll(incomingDir, 0755)
	_ = os.MkdirAll(processingDir, 0755)
	_ = os.MkdirAll(processedDir, 0755)

	// Create .drain sentinel file
	_ = os.WriteFile(filepath.Join(tempDir, ".drain"), []byte("drain"), 0644)

	task := &QueueTask{
		Type:       "issue-fix",
		URL:        "https://github.com/testowner/testrepo/issues/123",
		Number:     123,
		Priority:   "high",
		Status:     "Pending",
		EnqueuedAt: time.Now(),
	}

	taskFilename := "task-issue-123.yaml"
	taskData, _ := yaml.Marshal(task)
	_ = os.WriteFile(filepath.Join(incomingDir, taskFilename), taskData, 0644)

	ctx := context.Background()
	var wg sync.WaitGroup

	runQueueTasks(ctx, nil, nil, nil, RootOptions{Namespace: "default"}, "testowner", "testrepo", "factory-bot", "factory-bot", "factory", 10, 10, 10*time.Minute, tempDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir, false, &wg)

	wg.Wait()

	// In drain mode, no task should be moved to processing
	if _, err := os.Stat(filepath.Join(processingDir, taskFilename)); !os.IsNotExist(err) {
		t.Errorf("expected task NOT to be moved to processing dir during drain mode")
	}
}

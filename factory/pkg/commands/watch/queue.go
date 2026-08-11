package watch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

// taskItem represents a queue task bundled with its filename.
type taskItem struct {
	filename string
	task     *QueueTask
}

// getEnqueueTime returns the timestamp when a task was enqueued, falling back to
// file modification time or task creation time if EnqueuedAt is unset.
func getEnqueueTime(t *QueueTask, modTime time.Time) time.Time {
	if !t.EnqueuedAt.IsZero() {
		return t.EnqueuedAt
	}
	if !modTime.IsZero() {
		return modTime
	}
	return t.CreatedAt
}

// getEntityKey returns the unique key representing the target entity (PR, issue, or chore)
// for a given queue task.
func getEntityKey(t *QueueTask) string {
	if t.Number > 0 {
		return fmt.Sprintf("%d", t.Number)
	}
	if t.AgentFile != "" {
		return fmt.Sprintf("chore:%s", t.AgentFile)
	}
	if t.URL != "" {
		return fmt.Sprintf("url:%s", t.URL)
	}
	if t.Type != "" {
		return fmt.Sprintf("type:%s", t.Type)
	}
	return "default"
}

// priorityRankValue converts a priority string into an integer rank where lower numbers indicate higher priority.
func priorityRankValue(p string) int {
	priorityRank := map[string]int{
		"critical":  1,
		"urgent":    2,
		"important": 3,
		"high":      4,
		"medium":    5,
		"low":       6,
	}
	if r, ok := priorityRank[strings.ToLower(p)]; ok {
		return r
	}
	return 5
}

// isLessTask reports whether task a should be ordered before task b based on priority rank,
// phase rank, enqueue timestamp (FIFO), creation timestamp, and filename tiebreaking.
func isLessTask(a, b taskItem) bool {
	rankA := priorityRankValue(a.task.Priority)
	rankB := priorityRankValue(b.task.Priority)
	if rankA != rankB {
		return rankA < rankB
	}

	phaseA := a.task.Phase
	if phaseA == 0 {
		phaseA = 3
	}
	phaseB := b.task.Phase
	if phaseB == 0 {
		phaseB = 3
	}
	if phaseA != phaseB {
		return phaseA < phaseB
	}

	if !a.task.EnqueuedAt.Equal(b.task.EnqueuedAt) {
		return a.task.EnqueuedAt.Before(b.task.EnqueuedAt)
	}

	if !a.task.CreatedAt.Equal(b.task.CreatedAt) {
		return a.task.CreatedAt.Before(b.task.CreatedAt)
	}

	return a.filename < b.filename
}

// sortTasksFairly sorts queue tasks using a hybrid round-robin algorithm across entity buckets
// while maintaining FIFO arrival order, priority ranks, and phase dependencies within each entity.
func sortTasksFairly(items []taskItem) []taskItem {
	if len(items) <= 1 {
		return items
	}

	bucketsMap := make(map[string][]taskItem)

	for _, item := range items {
		key := getEntityKey(item.task)
		bucketsMap[key] = append(bucketsMap[key], item)
	}

	for key, bucket := range bucketsMap {
		sort.SliceStable(bucket, func(i, j int) bool {
			return isLessTask(bucket[i], bucket[j])
		})
		bucketsMap[key] = bucket
	}

	var result []taskItem

	for len(bucketsMap) > 0 {
		var activeKeys []string
		for key := range bucketsMap {
			activeKeys = append(activeKeys, key)
		}

		sort.SliceStable(activeKeys, func(i, j int) bool {
			headI := bucketsMap[activeKeys[i]][0]
			headJ := bucketsMap[activeKeys[j]][0]
			if isLessTask(headI, headJ) {
				return true
			}
			if isLessTask(headJ, headI) {
				return false
			}
			return activeKeys[i] < activeKeys[j]
		})

		for _, key := range activeKeys {
			bucket := bucketsMap[key]
			result = append(result, bucket[0])
			if len(bucket) == 1 {
				delete(bucketsMap, key)
			} else {
				bucketsMap[key] = bucket[1:]
			}
		}
	}

	return result
}

// writeTaskAtomically marshals task to YAML and writes it atomically via temporary file rename.
func writeTaskAtomically(dir string, filename string, task *QueueTask) error {
	data, err := yaml.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshaling task to YAML: %w", err)
	}

	tempFile := filepath.Join(dir, fmt.Sprintf(".temp-%s", filename))
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("writing temp task file: %w", err)
	}

	targetFile := filepath.Join(dir, filename)
	if err := os.Rename(tempFile, targetFile); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("renaming temp file to target %s: %w", targetFile, err)
	}

	return nil
}

// taskExists returns whether a task file exists in incoming or processing directories.
func taskExists(incomingDir, processingDir, filename string) bool {
	if _, err := os.Stat(filepath.Join(incomingDir, filename)); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(processingDir, filename)); err == nil {
		return true
	}
	return false
}

// isDoNotProcess returns true if drain / do-not-process flag or file exists.
func isDoNotProcess(queueDir string) bool {
	if os.Getenv("DO_NOT_PROCESS") == "true" || os.Getenv("FACTORY_DO_NOT_PROCESS") == "true" || os.Getenv("DRAIN") == "true" || os.Getenv("FACTORY_DRAIN") == "true" {
		return true
	}
	checkPaths := []string{
		filepath.Join(queueDir, ".do_not_process"),
		filepath.Join(queueDir, "do_not_process"),
		filepath.Join(queueDir, ".drain"),
		filepath.Join(queueDir, "drain"),
		"/workspaces/.do_not_process",
		"/workspaces/do_not_process",
		"/workspaces/.drain",
		"/workspaces/drain",
	}
	for _, p := range checkPaths {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// removePendingTasksForNumber deletes incoming pending tasks matching a PR or issue number.
func removePendingTasksForNumber(incomingDir string, number int) {
	entries, err := os.ReadDir(incomingDir)
	if err != nil {
		return
	}
	numStr := strconv.Itoa(number)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		// Match task-issue-123.yaml, task-pr-123-*.yaml, task-workflow-*-issue-123.yaml
		name := e.Name()
		if strings.HasPrefix(name, fmt.Sprintf("task-issue-%d.", number)) ||
			strings.HasPrefix(name, fmt.Sprintf("task-pr-%d-", number)) ||
			strings.HasSuffix(name, fmt.Sprintf("-issue-%d.yaml", number)) ||
			strings.Contains(name, fmt.Sprintf("-%s-", numStr)) ||
			strings.Contains(name, fmt.Sprintf("-%s.", numStr)) {
			_ = os.Remove(filepath.Join(incomingDir, name))
		}
	}
}

// getIssuePriority parses priority/<level> labels from an issue.
func getIssuePriority(issue *githubv39.Issue) string {
	for _, l := range issue.Labels {
		name := l.GetName()
		if strings.HasPrefix(name, "priority/") {
			return strings.TrimPrefix(name, "priority/")
		}
	}
	return "medium"
}

// getPRPriority parses priority/<level> labels from a PR issue.
func getPRPriority(prIssue *githubv39.Issue) string {
	return getIssuePriority(prIssue)
}

// writeTaskJournalEvent logs an event to journal.jsonl.
func writeTaskJournalEvent(queueDir string, taskFilename string, task *QueueTask, event string, duration time.Duration) {
	journalPath := filepath.Join(queueDir, "journal.jsonl")
	f, err := os.OpenFile(journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		klog.Errorf("Failed to open journal file: %v", err)
		return
	}
	defer f.Close()

	je := JournalEvent{
		Timestamp: time.Now(),
		TaskID:    strings.TrimSuffix(taskFilename, ".yaml"),
		Event:     event,
		Type:      task.Type,
		URL:       task.URL,
		Priority:  task.Priority,
		Error:     task.Error,
	}
	if duration > 0 {
		je.DurationSecond = duration.Seconds()
	}

	data, err := json.Marshal(je)
	if err != nil {
		klog.Errorf("Failed to marshal journal event: %v", err)
		return
	}

	if _, err := f.Write(append(data, '\n')); err != nil {
		klog.Errorf("Failed to write journal event: %v", err)
	}
}

package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	githubv39 "github.com/google/go-github/v39/github"
)

func TestSortTasksFairly(t *testing.T) {
	baseTime := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)

	t.Run("FIFO within single entity prevents LIFO starvation", func(t *testing.T) {
		task1 := taskItem{
			filename: "task1.yaml",
			task: &QueueTask{
				Type:       "pr-comments",
				Number:     10,
				Priority:   "medium",
				Phase:      2,
				CreatedAt:  baseTime,
				EnqueuedAt: baseTime.Add(1 * time.Minute),
			},
		}
		task2 := taskItem{
			filename: "task2.yaml",
			task: &QueueTask{
				Type:       "pr-comments",
				Number:     10,
				Priority:   "medium",
				Phase:      2,
				CreatedAt:  baseTime.Add(1 * time.Hour),
				EnqueuedAt: baseTime.Add(2 * time.Minute),
			},
		}
		task3 := taskItem{
			filename: "task3.yaml",
			task: &QueueTask{
				Type:       "pr-comments",
				Number:     10,
				Priority:   "medium",
				Phase:      2,
				CreatedAt:  baseTime.Add(2 * time.Hour),
				EnqueuedAt: baseTime.Add(3 * time.Minute),
			},
		}

		items := []taskItem{task3, task2, task1}
		got := sortTasksFairly(items)

		expectedOrder := []string{"task1.yaml", "task2.yaml", "task3.yaml"}
		for i, expected := range expectedOrder {
			if got[i].filename != expected {
				t.Errorf("at index %d: expected %s, got %s", i, expected, got[i].filename)
			}
		}
	})

	t.Run("Round-Robin across entities prevents entity starvation", func(t *testing.T) {
		pr10Task1 := taskItem{
			filename: "pr10_1.yaml",
			task:     &QueueTask{Number: 10, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(1 * time.Minute)},
		}
		pr10Task2 := taskItem{
			filename: "pr10_2.yaml",
			task:     &QueueTask{Number: 10, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(3 * time.Minute)},
		}
		pr10Task3 := taskItem{
			filename: "pr10_3.yaml",
			task:     &QueueTask{Number: 10, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(4 * time.Minute)},
		}

		pr20Task1 := taskItem{
			filename: "pr20_1.yaml",
			task:     &QueueTask{Number: 20, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(5 * time.Minute)},
		}

		pr20Task2 := taskItem{
			filename: "pr20_2.yaml",
			task:     &QueueTask{Number: 20, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(6 * time.Minute)},
		}

		items := []taskItem{pr10Task1, pr10Task2, pr10Task3, pr20Task1, pr20Task2}
		got := sortTasksFairly(items)

		expectedOrder := []string{"pr10_1.yaml", "pr20_1.yaml", "pr10_2.yaml", "pr20_2.yaml", "pr10_3.yaml"}
		for i, expected := range expectedOrder {
			if got[i].filename != expected {
				t.Errorf("at index %d: expected %s, got %s", i, expected, got[i].filename)
			}
		}
	})

	t.Run("Priority and Phase are respected across entities", func(t *testing.T) {
		criticalTask := taskItem{
			filename: "critical.yaml",
			task:     &QueueTask{Number: 10, Priority: "critical", Phase: 3, EnqueuedAt: baseTime.Add(5 * time.Minute)},
		}
		mediumTask := taskItem{
			filename: "medium.yaml",
			task:     &QueueTask{Number: 20, Priority: "medium", Phase: 3, EnqueuedAt: baseTime.Add(1 * time.Minute)},
		}
		phase1Task := taskItem{
			filename: "phase1.yaml",
			task:     &QueueTask{Number: 20, Priority: "medium", Phase: 1, EnqueuedAt: baseTime.Add(2 * time.Minute)},
		}

		items := []taskItem{mediumTask, criticalTask, phase1Task}
		got := sortTasksFairly(items)

		expectedOrder := []string{"critical.yaml", "phase1.yaml", "medium.yaml"}
		for i, expected := range expectedOrder {
			if got[i].filename != expected {
				t.Errorf("at index %d: expected %s, got %s", i, expected, got[i].filename)
			}
		}
	})

	t.Run("Fallback to modTime or CreatedAt when EnqueuedAt is zero", func(t *testing.T) {
		taskOldCreated := &QueueTask{
			CreatedAt: baseTime,
		}
		taskNewCreated := &QueueTask{
			CreatedAt: baseTime.Add(1 * time.Hour),
		}
		taskWithEnqueued := &QueueTask{
			CreatedAt:  baseTime.Add(2 * time.Hour),
			EnqueuedAt: baseTime.Add(10 * time.Minute),
		}

		t1 := getEnqueueTime(taskOldCreated, time.Time{})
		if !t1.Equal(baseTime) {
			t.Errorf("expected fallback to CreatedAt %v, got %v", baseTime, t1)
		}

		t2 := getEnqueueTime(taskNewCreated, baseTime.Add(5*time.Minute))
		if !t2.Equal(baseTime.Add(5 * time.Minute)) {
			t.Errorf("expected fallback to modTime %v, got %v", baseTime.Add(5*time.Minute), t2)
		}

		t3 := getEnqueueTime(taskWithEnqueued, baseTime.Add(5*time.Minute))
		if !t3.Equal(baseTime.Add(10 * time.Minute)) {
			t.Errorf("expected EnqueuedAt %v, got %v", baseTime.Add(10*time.Minute), t3)
		}
	})
}

func TestWriteTaskAtomicallyAndTaskExists(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	_ = os.MkdirAll(incomingDir, 0755)
	_ = os.MkdirAll(processingDir, 0755)

	task := &QueueTask{
		Type:       "issue-fix",
		Number:     42,
		URL:        "https://github.com/org/repo/issues/42",
		Priority:   "high",
		Phase:      3,
		Status:     "Pending",
		CreatedAt:  time.Now(),
		EnqueuedAt: time.Now(),
	}

	filename := "task-issue-42.yaml"
	if taskExists(incomingDir, processingDir, filename) {
		t.Errorf("expected taskExists to be false before write")
	}

	if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
		t.Fatalf("failed to write task atomically: %v", err)
	}

	if !taskExists(incomingDir, processingDir, filename) {
		t.Errorf("expected taskExists to be true after write in incoming")
	}

	// Move to processing
	err := os.Rename(filepath.Join(incomingDir, filename), filepath.Join(processingDir, filename))
	if err != nil {
		t.Fatalf("failed to rename to processing: %v", err)
	}

	if !taskExists(incomingDir, processingDir, filename) {
		t.Errorf("expected taskExists to be true after moved to processing")
	}
}

func TestIsDoNotProcess(t *testing.T) {
	tempDir := t.TempDir()

	if isDoNotProcess(tempDir) {
		t.Errorf("expected isDoNotProcess to be false in clean dir")
	}

	drainFile := filepath.Join(tempDir, ".drain")
	_ = os.WriteFile(drainFile, []byte(""), 0644)
	if !isDoNotProcess(tempDir) {
		t.Errorf("expected isDoNotProcess to be true when .drain exists")
	}
	_ = os.Remove(drainFile)

	t.Setenv("FACTORY_DRAIN", "true")
	if !isDoNotProcess(tempDir) {
		t.Errorf("expected isDoNotProcess to be true when FACTORY_DRAIN env is true")
	}
}

func TestRemovePendingTasksForNumber(t *testing.T) {
	incomingDir := t.TempDir()

	_ = os.WriteFile(filepath.Join(incomingDir, "task-issue-10.yaml"), []byte("..."), 0644)
	_ = os.WriteFile(filepath.Join(incomingDir, "task-pr-10-review.yaml"), []byte("..."), 0644)
	_ = os.WriteFile(filepath.Join(incomingDir, "task-issue-20.yaml"), []byte("..."), 0644)

	removePendingTasksForNumber(incomingDir, 10)

	if _, err := os.Stat(filepath.Join(incomingDir, "task-issue-10.yaml")); !os.IsNotExist(err) {
		t.Errorf("expected task-issue-10.yaml to be removed")
	}
	if _, err := os.Stat(filepath.Join(incomingDir, "task-pr-10-review.yaml")); !os.IsNotExist(err) {
		t.Errorf("expected task-pr-10-review.yaml to be removed")
	}
	if _, err := os.Stat(filepath.Join(incomingDir, "task-issue-20.yaml")); err != nil {
		t.Errorf("expected task-issue-20.yaml to remain")
	}
}

func TestPriorityAndEntityHelpers(t *testing.T) {
	if r := priorityRankValue("critical"); r != 1 {
		t.Errorf("expected rank 1 for critical, got %d", r)
	}
	if r := priorityRankValue("unknown"); r != 5 {
		t.Errorf("expected default rank 5 for unknown, got %d", r)
	}

	issueWithLabel := &githubv39.Issue{
		Labels: []*githubv39.Label{
			{Name: stringPtr("priority/urgent")},
		},
	}
	if p := getIssuePriority(issueWithLabel); p != "urgent" {
		t.Errorf("expected urgent, got %s", p)
	}
	if p := getPRPriority(issueWithLabel); p != "urgent" {
		t.Errorf("expected urgent, got %s", p)
	}

	taskNum := &QueueTask{Number: 10}
	if k := getEntityKey(taskNum); k != "10" {
		t.Errorf("expected 10, got %s", k)
	}

	taskChore := &QueueTask{AgentFile: "test.md"}
	if k := getEntityKey(taskChore); k != "chore:test.md" {
		t.Errorf("expected chore:test.md, got %s", k)
	}
}

func TestWriteTaskJournalEvent(t *testing.T) {
	tempDir := t.TempDir()
	task := &QueueTask{
		Type:     "issue-fix",
		URL:      "https://github.com/org/repo/issues/1",
		Priority: "high",
	}

	writeTaskJournalEvent(tempDir, "task-1.yaml", task, "Started", 12*time.Second)

	journalContent, err := os.ReadFile(filepath.Join(tempDir, "journal.jsonl"))
	if err != nil {
		t.Fatalf("failed to read journal: %v", err)
	}

	var ev JournalEvent
	if err := json.Unmarshal(bytes.TrimSpace(journalContent), &ev); err != nil {
		t.Fatalf("failed to unmarshal journal event: %v", err)
	}

	if ev.TaskID != "task-1" || ev.Event != "Started" || ev.DurationSecond != 12.0 {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestBuildQueueResponse(t *testing.T) {
	t.Run("Empty and non-existent directories", func(t *testing.T) {
		tempDir := t.TempDir()
		resp := buildQueueResponse(tempDir)

		if resp.Summary.TotalPending != 0 || resp.Summary.TotalProcessing != 0 || resp.Summary.TotalCompleted != 0 {
			t.Errorf("expected 0 totals in summary, got %+v", resp.Summary)
		}

		if len(resp.Incoming) != 0 {
			t.Errorf("expected 0 incoming tasks, got %d", len(resp.Incoming))
		}
	})

	t.Run("Field extraction and defaults", func(t *testing.T) {
		tempDir := t.TempDir()
		incomingDir := filepath.Join(tempDir, "incoming")
		if err := os.MkdirAll(incomingDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Task 1: Explicit fields
		task1Content := `type: pr-review
url: https://github.com/org/repo/pull/42
number: 42
priority: critical
phase: 2
createdAt: "2026-08-10T10:00:00Z"
enqueuedAt: "2026-08-10T10:05:00Z"
assignee: alice
status: Pending
commitSHA: abc123def
`
		if err := os.WriteFile(filepath.Join(incomingDir, "task1.yaml"), []byte(task1Content), 0644); err != nil {
			t.Fatal(err)
		}

		// Task 2: Defaults (no priority, no enqueuedAt)
		task2Content := `type: issue-fix
url: https://github.com/org/repo/issues/99
number: 99
createdAt: "2026-08-10T11:00:00Z"
status: Pending
`
		if err := os.WriteFile(filepath.Join(incomingDir, "task2.yaml"), []byte(task2Content), 0644); err != nil {
			t.Fatal(err)
		}

		// Non-yaml file and directory should be ignored
		if err := os.WriteFile(filepath.Join(incomingDir, "ignore.txt"), []byte("not yaml"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(incomingDir, "subfolder"), 0755); err != nil {
			t.Fatal(err)
		}

		resp := buildQueueResponse(tempDir)
		if len(resp.Incoming) != 2 {
			t.Fatalf("expected 2 incoming tasks, got %d", len(resp.Incoming))
		}

		if resp.Incoming[1].EnqueuedAt == "" {
			t.Errorf("expected non-empty fallback enqueuedAt for task 2")
		}

		expectedIncoming := []QueueTaskItem{
			{
				FileName:   "task1.yaml",
				QueueState: "incoming",
				Type:       "pr-review",
				URL:        "https://github.com/org/repo/pull/42",
				Number:     42,
				Priority:   "critical",
				Phase:      2,
				CreatedAt:  "2026-08-10T10:00:00Z",
				EnqueuedAt: "2026-08-10T10:05:00Z",
				Assignee:   "alice",
				Status:     "Pending",
				CommitSHA:  "abc123def",
				Rank:       1,
			},
			{
				FileName:   "task2.yaml",
				QueueState: "incoming",
				Type:       "issue-fix",
				URL:        "https://github.com/org/repo/issues/99",
				Number:     99,
				Priority:   "medium",
				Phase:      0,
				CreatedAt:  "2026-08-10T11:00:00Z",
				EnqueuedAt: resp.Incoming[1].EnqueuedAt,
				Assignee:   "",
				Status:     "Pending",
				CommitSHA:  "",
				Rank:       2,
			},
		}

		if diff := cmp.Diff(expectedIncoming, resp.Incoming); diff != "" {
			t.Errorf("incoming tasks mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Sorting by priority, phase, and createdAt", func(t *testing.T) {
		tempDir := t.TempDir()
		incomingDir := filepath.Join(tempDir, "incoming")
		if err := os.MkdirAll(incomingDir, 0755); err != nil {
			t.Fatal(err)
		}

		tasks := []struct {
			filename string
			priority string
			phase    int
			created  string
		}{
			{"low.yaml", "low", 1, "2026-08-10T01:00:00Z"},
			{"med_ph2.yaml", "medium", 2, "2026-08-10T01:00:00Z"},
			{"crit_later.yaml", "critical", 1, "2026-08-10T02:00:00Z"},
			{"urgent.yaml", "urgent", 1, "2026-08-10T01:00:00Z"},
			{"med_ph1_earlier.yaml", "medium", 1, "2026-08-10T01:00:00Z"},
			{"crit_earlier.yaml", "critical", 1, "2026-08-10T01:00:00Z"},
			{"important.yaml", "important", 1, "2026-08-10T01:00:00Z"},
			{"high.yaml", "high", 1, "2026-08-10T01:00:00Z"},
			{"med_ph1_later.yaml", "medium", 1, "2026-08-10T02:00:00Z"},
		}

		for _, task := range tasks {
			content := fmt.Sprintf("type: pr-review\npriority: %s\nphase: %d\ncreatedAt: %s\n", task.priority, task.phase, task.created)
			if err := os.WriteFile(filepath.Join(incomingDir, task.filename), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}

		resp := buildQueueResponse(tempDir)
		if len(resp.Incoming) != len(tasks) {
			t.Fatalf("expected %d tasks, got %d", len(tasks), len(resp.Incoming))
		}

		expectedOrder := []string{
			"crit_earlier.yaml",
			"crit_later.yaml",
			"urgent.yaml",
			"important.yaml",
			"high.yaml",
			"med_ph1_earlier.yaml",
			"med_ph1_later.yaml",
			"med_ph2.yaml",
			"low.yaml",
		}

		for i, exp := range expectedOrder {
			gotFile := resp.Incoming[i].FileName
			gotRank := resp.Incoming[i].Rank
			if gotFile != exp {
				t.Errorf("at index %d: expected %s, got %v", i, exp, gotFile)
			}
			if gotRank != i+1 {
				t.Errorf("at index %d: expected rank %d, got %v", i, i+1, gotRank)
			}
		}
	})

	t.Run("Fair round-robin across multiple entities matching watch loop", func(t *testing.T) {
		tempDir := t.TempDir()
		incomingDir := filepath.Join(tempDir, "incoming")
		if err := os.MkdirAll(incomingDir, 0755); err != nil {
			t.Fatal(err)
		}

		baseTime := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

		// 3 tasks for PR 10
		_ = os.WriteFile(filepath.Join(incomingDir, "pr10_1.yaml"), []byte(fmt.Sprintf("number: 10\ntype: pr-comments\npriority: medium\nphase: 3\nenqueuedAt: %s\n", baseTime.Add(1*time.Minute).Format(time.RFC3339))), 0644)
		_ = os.WriteFile(filepath.Join(incomingDir, "pr10_2.yaml"), []byte(fmt.Sprintf("number: 10\ntype: pr-comments\npriority: medium\nphase: 3\nenqueuedAt: %s\n", baseTime.Add(3*time.Minute).Format(time.RFC3339))), 0644)
		_ = os.WriteFile(filepath.Join(incomingDir, "pr10_3.yaml"), []byte(fmt.Sprintf("number: 10\ntype: pr-comments\npriority: medium\nphase: 3\nenqueuedAt: %s\n", baseTime.Add(4*time.Minute).Format(time.RFC3339))), 0644)

		// 2 tasks for PR 20
		_ = os.WriteFile(filepath.Join(incomingDir, "pr20_1.yaml"), []byte(fmt.Sprintf("number: 20\ntype: pr-comments\npriority: medium\nphase: 3\nenqueuedAt: %s\n", baseTime.Add(5*time.Minute).Format(time.RFC3339))), 0644)
		_ = os.WriteFile(filepath.Join(incomingDir, "pr20_2.yaml"), []byte(fmt.Sprintf("number: 20\ntype: pr-comments\npriority: medium\nphase: 3\nenqueuedAt: %s\n", baseTime.Add(6*time.Minute).Format(time.RFC3339))), 0644)

		resp := buildQueueResponse(tempDir)
		if len(resp.Incoming) != 5 {
			t.Fatalf("expected 5 incoming tasks, got %d", len(resp.Incoming))
		}

		// Expected round-robin order between PR 10 and PR 20
		expectedOrder := []string{"pr10_1.yaml", "pr20_1.yaml", "pr10_2.yaml", "pr20_2.yaml", "pr10_3.yaml"}
		for i, exp := range expectedOrder {
			if resp.Incoming[i].FileName != exp {
				t.Errorf("at index %d: expected %s, got %v", i, exp, resp.Incoming[i].FileName)
			}
			if resp.Incoming[i].Rank != i+1 {
				t.Errorf("at index %d: expected rank %d, got %v", i, i+1, resp.Incoming[i].Rank)
			}
		}
	})

	t.Run("Summary counts and breakdowns", func(t *testing.T) {
		tempDir := t.TempDir()
		incomingDir := filepath.Join(tempDir, "incoming")
		processingDir := filepath.Join(tempDir, "processing")
		processedDir := filepath.Join(tempDir, "processed")

		for _, d := range []string{incomingDir, processingDir, processedDir} {
			if err := os.MkdirAll(d, 0755); err != nil {
				t.Fatal(err)
			}
		}

		// 3 incoming tasks (2 critical pr-review, 1 high issue-fix)
		_ = os.WriteFile(filepath.Join(incomingDir, "task1.yaml"), []byte("type: pr-review\npriority: critical\n"), 0644)
		_ = os.WriteFile(filepath.Join(incomingDir, "task2.yaml"), []byte("type: pr-review\npriority: critical\n"), 0644)
		_ = os.WriteFile(filepath.Join(incomingDir, "task3.yaml"), []byte("type: issue-fix\npriority: high\n"), 0644)

		// 2 processing tasks
		_ = os.WriteFile(filepath.Join(processingDir, "proc1.yaml"), []byte("type: pr-review\npriority: critical\n"), 0644)
		_ = os.WriteFile(filepath.Join(processingDir, "proc2.yaml"), []byte("type: agent-chore\npriority: low\n"), 0644)

		// 1 processed task
		_ = os.WriteFile(filepath.Join(processedDir, "done1.yaml"), []byte("type: pr-review\nstatus: Completed\n"), 0644)

		resp := buildQueueResponse(tempDir)

		if resp.Summary.TotalPending != 3 {
			t.Errorf("expected totalPending 3, got %v", resp.Summary.TotalPending)
		}
		if resp.Summary.TotalProcessing != 2 {
			t.Errorf("expected totalProcessing 2, got %v", resp.Summary.TotalProcessing)
		}
		if resp.Summary.TotalCompleted != 1 {
			t.Errorf("expected totalCompleted 1, got %v", resp.Summary.TotalCompleted)
		}

		if resp.Summary.ByPriority["critical"] != 2 || resp.Summary.ByPriority["high"] != 1 {
			t.Errorf("expected byPriority map [critical:2, high:1], got %+v", resp.Summary.ByPriority)
		}

		if resp.Summary.ByType["pr-review"] != 2 || resp.Summary.ByType["issue-fix"] != 1 {
			t.Errorf("expected byType map [pr-review:2, issue-fix:1], got %+v", resp.Summary.ByType)
		}
	})

	t.Run("Processed queue capping at 20", func(t *testing.T) {
		tempDir := t.TempDir()
		processedDir := filepath.Join(tempDir, "processed")
		if err := os.MkdirAll(processedDir, 0755); err != nil {
			t.Fatal(err)
		}

		for i := 1; i <= 30; i++ {
			fn := fmt.Sprintf("task-%02d.yaml", i)
			_ = os.WriteFile(filepath.Join(processedDir, fn), []byte("type: pr-review\nstatus: Completed\n"), 0644)
		}

		resp := buildQueueResponse(tempDir)

		if len(resp.Processed) != 20 {
			t.Errorf("expected processed capped at 20 items, got %d", len(resp.Processed))
		}

		if resp.Summary.TotalCompleted != 20 {
			t.Errorf("expected totalCompleted 20 in summary, got %v", resp.Summary.TotalCompleted)
		}
	})
}

func TestQueueHTTPHandlers(t *testing.T) {
	tempDir := t.TempDir()
	incomingDir := filepath.Join(tempDir, "incoming")
	_ = os.MkdirAll(incomingDir, 0755)

	taskContent := "type: pr-review\npriority: medium\n"
	taskPath := filepath.Join(incomingDir, "task-1.yaml")
	_ = os.WriteFile(taskPath, []byte(taskContent), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Launch server on random port or use Handler directly
	// Let's test endpoint via http request with test server
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/queue", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := buildQueueResponse(tempDir)
		_ = json.NewEncoder(w).Encode(resp)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queue", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}

	var resp QueueResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Summary.TotalPending != 1 {
		t.Errorf("expected 1 pending task, got %d", resp.Summary.TotalPending)
	}
	_ = ctx
}

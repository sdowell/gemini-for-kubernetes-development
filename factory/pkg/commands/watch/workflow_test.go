package watch

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func TestFindWorkflowPath(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{
			name:     "clean URL",
			body:     "Workflow: https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt",
			expected: "https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt",
		},
		{
			name:     "URL with literal escaped newline \\n",
			body:     "This issue is to track Greenfield.\n\nWorkflow: https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt\\n",
			expected: "https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt",
		},
		{
			name:     "quoted double-quote URL should be ignored",
			body:     "Follow workflow at \"https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt\", please.",
			expected: "",
		},
		{
			name:     "quoted single-quote URL should be ignored",
			body:     "Check 'https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt'",
			expected: "",
		},
		{
			name:     "backticked URL should be ignored",
			body:     "See `https://raw.githubusercontent.com/gke-labs/gemini-for-kubernetes-development/main/.agents/workflows/kcc-greenfield.txt`",
			expected: "",
		},
		{
			name:     "local relative workflow file path",
			body:     "Please use .agents/workflows/kcc-greenfield.txt for this issue",
			expected: ".agents/workflows/kcc-greenfield.txt",
		},
		{
			name:     "local workflow path with escaped newline",
			body:     "Workflow: .agents/workflows/kcc-greenfield.txt\\n",
			expected: ".agents/workflows/kcc-greenfield.txt",
		},
		{
			name:     "backticked local workflow path should be ignored",
			body:     "Reference `.agents/workflows/kcc-greenfield.txt` in docs",
			expected: "",
		},
		{
			name:     "no workflow referenced",
			body:     "Regular bug report with some code snippets",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := findWorkflowPath(tc.body)
			if got != tc.expected {
				t.Errorf("findWorkflowPath(%q) = %q; want %q", tc.body, got, tc.expected)
			}
		})
	}
}

func TestSanitizeWorkflowPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://example.com/workflow.yaml\\n", "https://example.com/workflow.yaml"},
		{"https://example.com/workflow.yaml\\r\\n", "https://example.com/workflow.yaml"},
		{"  .agents/workflow.md  ", ".agents/workflow.md"},
	}

	for _, tc := range tests {
		got := SanitizeWorkflowPath(tc.input)
		if got != tc.expected {
			t.Errorf("SanitizeWorkflowPath(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestWorkflowCooldownCompletedAt(t *testing.T) {
	tempDir := t.TempDir()
	processedPath := filepath.Join(tempDir, "task-workflow-test-issue-1.yaml")

	// Task completed 5 hours ago
	completedAt := time.Now().Add(-5 * time.Hour)
	taskYAML := fmt.Sprintf("completedAt: %s\n", completedAt.Format(time.RFC3339Nano))
	if err := os.WriteFile(processedPath, []byte(taskYAML), 0644); err != nil {
		t.Fatalf("Failed to write test task yaml: %v", err)
	}

	info, err := os.Stat(processedPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	lastRunTime := info.ModTime()
	if data, err := os.ReadFile(processedPath); err == nil {
		var q QueueTask
		if err := yaml.Unmarshal(data, &q); err == nil && !q.CompletedAt.IsZero() {
			lastRunTime = q.CompletedAt
		}
	}

	if !lastRunTime.Equal(completedAt) {
		t.Fatalf("lastRunTime = %v, want %v", lastRunTime, completedAt)
	}
}

func TestParseGitHubURL(t *testing.T) {
	tests := []struct {
		urlStr string
		owner  string
		repo   string
		branch string
		path   string
		ok     bool
	}{
		{
			urlStr: "https://github.com/owner/repo/blob/main/.agents/workflow.yaml",
			owner:  "owner",
			repo:   "repo",
			branch: "main",
			path:   ".agents/workflow.yaml",
			ok:     true,
		},
		{
			urlStr: "https://github.com/owner/repo/raw/feat/test.txt",
			owner:  "owner",
			repo:   "repo",
			branch: "feat",
			path:   "test.txt",
			ok:     true,
		},
		{
			urlStr: "https://notgithub.com/owner/repo",
			ok:     false,
		},
	}

	for _, tc := range tests {
		owner, repo, branch, path, ok := parseGitHubURL(tc.urlStr)
		if ok != tc.ok || (ok && (owner != tc.owner || repo != tc.repo || branch != tc.branch || path != tc.path)) {
			t.Errorf("parseGitHubURL(%q) = (%s, %s, %s, %s, %v), want (%s, %s, %s, %s, %v)",
				tc.urlStr, owner, repo, branch, path, ok, tc.owner, tc.repo, tc.branch, tc.path, tc.ok)
		}
	}
}

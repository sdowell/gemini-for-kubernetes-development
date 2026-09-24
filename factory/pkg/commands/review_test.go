package commands

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"
)

func strPtr(s string) *string { return &s }

// The exact production failure: the agent emitted side "RIGHT\n" and GitHub
// 422-rejected the whole review.
func TestSanitizedSide(t *testing.T) {
	cases := []struct {
		in   *string
		want *string
	}{
		{strPtr("RIGHT\n"), strPtr("RIGHT")},
		{strPtr(" left "), strPtr("LEFT")},
		{strPtr("RIGHT"), strPtr("RIGHT")},
		{strPtr("banana"), nil},
		{strPtr("  "), nil},
		{nil, nil},
	}
	for _, c := range cases {
		got := sanitizedSide(c.in)
		switch {
		case got == nil && c.want != nil, got != nil && c.want == nil:
			t.Errorf("sanitizedSide(%v): got %v want %v", c.in, got, c.want)
		case got != nil && *got != *c.want:
			t.Errorf("sanitizedSide(%q): got %q want %q", *c.in, *got, *c.want)
		}
	}
}

func TestTrimmedString(t *testing.T) {
	if got := trimmedString(strPtr("  path/to/file.go\n")); got == nil || *got != "path/to/file.go" {
		t.Errorf("trimmedString: got %v", got)
	}
	if got := trimmedString(strPtr("   ")); got != nil {
		t.Errorf("trimmedString(blank): got %v", got)
	}
	if got := trimmedString(nil); got != nil {
		t.Errorf("trimmedString(nil): got %v", got)
	}
}

func TestParseReviewRequest(t *testing.T) {
	raw := "Thinking about the PR...\n```yaml\nreview:\n  body: \"Looks good overall.\"\n  comments:\n    - path: \" pkg/foo.go \"\n      line: 42\n      side: \" right\\n\"\n      body: \"Consider checking err here.\"\n```\n"
	req, _, err := parseReviewRequest(raw, false)
	if err != nil {
		t.Fatalf("parseReviewRequest failed: %v", err)
	}
	if req.GetBody() != "Looks good overall." {
		t.Errorf("expected body 'Looks good overall.', got %q", req.GetBody())
	}
	if req.GetEvent() != "COMMENT" {
		t.Errorf("expected event 'COMMENT', got %q", req.GetEvent())
	}
	if len(req.Comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(req.Comments))
	}
	if req.Comments[0].GetPath() != "pkg/foo.go" {
		t.Errorf("expected sanitized path 'pkg/foo.go', got %q", req.Comments[0].GetPath())
	}
	if req.Comments[0].GetSide() != "RIGHT" {
		t.Errorf("expected sanitized side 'RIGHT', got %q", req.Comments[0].GetSide())
	}

	draftReq, _, err := parseReviewRequest(raw, true)
	if err != nil {
		t.Fatalf("parseReviewRequest(draft) failed: %v", err)
	}
	if draftReq.Event != nil {
		t.Errorf("expected nil Event for draft review, got %q", draftReq.GetEvent())
	}
}

func TestPublishReviewCommand(t *testing.T) {
	var receivedReq githubv39.PullRequestReviewRequest
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/repos/test-owner/test-repo/pulls/13065/reviews" {
			requestCount++
			bodyBytes, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(bodyBytes, &receivedReq)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id": 1, "state": "COMMENTED"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	t.Setenv("GITHUB_API_URL", server.URL)
	t.Setenv("GITHUB_TOKEN", "test-token")

	tmpDir := t.TempDir()
	reviewFile := filepath.Join(tmpDir, "review-output.txt")
	content := "```yaml\nreview:\n  body: \"In-sandbox review body\"\n  comments:\n    - path: \"pkg/bar.go\"\n      line: 10\n      side: \"RIGHT\"\n      body: \"Nit\"\n```\n"
	if err := os.WriteFile(reviewFile, []byte(content), 0644); err != nil {
		t.Fatalf("writing temp review file: %v", err)
	}

	cmd := NewPublishReviewCommand(context.Background())
	cmd.SetArgs([]string{
		"--pr-url", "https://github.com/test-owner/test-repo/pull/13065",
		"--input", reviewFile,
		"--publish", "yes",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("NewPublishReviewCommand Execute failed: %v", err)
	}

	if requestCount != 1 {
		t.Fatalf("expected 1 CreateReview API call, got %d", requestCount)
	}
	if receivedReq.GetBody() != "In-sandbox review body" {
		t.Errorf("expected review body 'In-sandbox review body', got %q", receivedReq.GetBody())
	}
	if receivedReq.GetEvent() != "COMMENT" {
		t.Errorf("expected event 'COMMENT', got %q", receivedReq.GetEvent())
	}
}

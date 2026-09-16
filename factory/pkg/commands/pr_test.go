package commands

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	githubv39 "github.com/google/go-github/v39/github"
)

func strPtr(s string) *string {
	return &s
}

func int64Ptr(i int64) *int64 {
	return &i
}

func TestClassifyPRFeedback_RetryAfterFailedTask(t *testing.T) {
	t0 := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC) // Initial commit
	t1 := t0.Add(5 * time.Minute)                       // Already addressed comment (+1 reaction)
	t2 := t0.Add(10 * time.Minute)                      // Bot reply addressing t1
	t3 := t0.Add(15 * time.Minute)                      // Unaddressed human comment & review comment (failed task trigger time)
	t4 := t0.Add(16 * time.Minute)                      // Watcher "started addressing review feedback" system comment
	t5 := t0.Add(20 * time.Minute)                      // Rebase/iterate commit pushed AFTER t3 (lastCommitTime = t5)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/owner/repo/issues/comments/101/reactions":
			// Comment 101 was already addressed (+1 reaction from bot)
			_, _ = w.Write([]byte(`[{"content": "+1", "user": {"login": "overseer-bot"}}]`))
		case "/repos/owner/repo/issues/comments/103/reactions":
			// Comment 103 has "eyes" reaction from failed attempt
			_, _ = w.Write([]byte(`[{"content": "eyes", "user": {"login": "overseer-bot"}}]`))
		case "/repos/owner/repo/pulls/comments/301/reactions":
			// Inline comment 301 was already addressed (+1)
			_, _ = w.Write([]byte(`[{"content": "+1", "user": {"login": "overseer-bot"}}]`))
		case "/repos/owner/repo/pulls/comments/302/reactions":
			// Inline comment 302 by reviewer bot has "eyes" reaction from failed attempt
			_, _ = w.Write([]byte(`[{"content": "eyes", "user": {"login": "overseer-bot"}}]`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()

	ghClient := githubv39.NewClient(nil)
	ghClient.BaseURL, _ = url.Parse(server.URL + "/")

	pr := &githubv39.PullRequest{
		User: &githubv39.User{Login: strPtr("overseer-bot")},
	}

	comments := []*githubv39.IssueComment{
		{
			ID:        int64Ptr(101),
			User:      &githubv39.User{Login: strPtr("human-user")},
			Body:      strPtr("Old comment already addressed"),
			CreatedAt: &t1,
		},
		{
			ID:        int64Ptr(102),
			User:      &githubv39.User{Login: strPtr("overseer-bot")},
			Body:      strPtr("I addressed the old comment in commit 111."),
			CreatedAt: &t2,
		},
		{
			ID:        int64Ptr(103),
			User:      &githubv39.User{Login: strPtr("human-user")},
			Body:      strPtr("Unaddressed comment from failed run"),
			CreatedAt: &t3,
		},
		{
			ID:        int64Ptr(104),
			User:      &githubv39.User{Login: strPtr("overseer-bot")},
			Body:      strPtr("🤖 AI Factory started addressing review feedback for this pull request."),
			CreatedAt: &t4,
		},
	}

	reviews := []*githubv39.PullRequestReview{
		{
			ID:          int64Ptr(201),
			User:        &githubv39.User{Login: strPtr("gemini-code-assist[bot]"), Type: strPtr("Bot")},
			Body:        strPtr("Code review feedback"),
			SubmittedAt: &t3,
		},
	}

	revCommentsMap := map[int64][]*githubv39.PullRequestComment{
		201: {
			{
				ID:       int64Ptr(301),
				User:     &githubv39.User{Login: strPtr("gemini-code-assist[bot]"), Type: strPtr("Bot")},
				Path:     strPtr("pkg/controller.go"),
				DiffHunk: strPtr("@@ -1,5 +1,5 @@"),
				Body:     strPtr("Already addressed inline comment"),
			},
			{
				ID:       int64Ptr(302),
				User:     &githubv39.User{Login: strPtr("gemini-code-assist[bot]"), Type: strPtr("Bot")},
				Path:     strPtr("pkg/controller.go"),
				DiffHunk: strPtr("@@ -20,5 +20,5 @@"),
				Body:     strPtr("Unaddressed inline comment from failed run"),
			},
		},
	}

	cfg := &config.FactoryConfig{
		Roles: map[string]config.RoleConfig{
			"reviewer": {Users: []string{"gemini-code-assist[bot]"}},
		},
	}

	// Note: lastCommitTime is t5 (AFTER t3), simulating a rebase commit pushed after the review feedback.
	// since is set to t3 (the failed task's TriggerEventTime).
	oldComments, newComments, oldReviews, newReviews := classifyPRFeedback(
		context.Background(),
		ghClient,
		"owner",
		"repo",
		pr,
		comments,
		reviews,
		revCommentsMap,
		t5,
		t3.Format(time.RFC3339),
		cfg,
	)

	// Verify newComments contains only comment 103
	if len(newComments) != 1 || newComments[0].ID != 103 {
		t.Fatalf("newComments = %+v; want only comment ID 103", newComments)
	}

	// Verify oldComments contains comment 101 and bot reply 102 (and NOT system comment 104)
	if len(oldComments) != 2 || oldComments[0].ID != 101 || oldComments[1].ID != 102 {
		t.Fatalf("oldComments = %+v; want comment IDs 101 and 102", oldComments)
	}

	// Verify newReviews contains review 201 with only unaddressed inline comment 302
	if len(newReviews) != 1 || len(newReviews[0].PullRequestComments) != 1 {
		t.Fatalf("newReviews = %+v; want 1 review with 1 inline comment", newReviews)
	}
	if newReviews[0].PullRequestComments[0].Body != "Unaddressed inline comment from failed run" {
		t.Errorf("newReviews inline comment body = %q; want %q",
			newReviews[0].PullRequestComments[0].Body, "Unaddressed inline comment from failed run")
	}

	// Verify oldReviews contains review 201 with addressed inline comment 301
	if len(oldReviews) != 1 || len(oldReviews[0].PullRequestComments) != 1 {
		t.Fatalf("oldReviews = %+v; want 1 review with 1 inline comment", oldReviews)
	}
	if oldReviews[0].PullRequestComments[0].Body != "Already addressed inline comment" {
		t.Errorf("oldReviews inline comment body = %q; want %q",
			oldReviews[0].PullRequestComments[0].Body, "Already addressed inline comment")
	}
}

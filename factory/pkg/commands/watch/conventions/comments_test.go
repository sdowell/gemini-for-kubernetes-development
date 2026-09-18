package conventions

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

func reaction(content, login string) *githubv39.Reaction {
	return &githubv39.Reaction{
		Content: stringPtr(content),
		User:    &githubv39.User{Login: stringPtr(login)},
	}
}

// TestCommentMarks covers the reaction protocol, which is the only record of
// what has been done about a given comment.
func TestCommentMarks(t *testing.T) {
	tests := []struct {
		name            string
		reactions       []*githubv39.Reaction
		wantOutstanding bool
		wantUnresolved  bool
	}{
		{
			name:            "no marks at all is feedback nothing has been done about",
			wantOutstanding: true,
			wantUnresolved:  true,
		},
		{
			name:            "eyes means picked up, outcome not yet known",
			reactions:       []*githubv39.Reaction{reaction("eyes", "watcher-bot")},
			wantOutstanding: false,
			wantUnresolved:  true,
		},
		{
			name:            "the watcher's +1 closes the comment out",
			reactions:       []*githubv39.Reaction{reaction("eyes", "watcher-bot"), reaction("+1", "watcher-bot")},
			wantOutstanding: false,
			wantUnresolved:  false,
		},
		{
			name:            "confused reopens it, which is what a failed attempt leaves",
			reactions:       []*githubv39.Reaction{reaction("eyes", "watcher-bot"), reaction("confused", "watcher-bot")},
			wantOutstanding: true,
			wantUnresolved:  true,
		},
		{
			name: "a stale confused beside a +1 still counts as owed",
			// GitHub keeps one reaction of each content per account, so a
			// failure mark that is never withdrawn outlives the failure.
			reactions:       []*githubv39.Reaction{reaction("+1", "watcher-bot"), reaction("confused", "watcher-bot")},
			wantOutstanding: true,
			wantUnresolved:  true,
		},
		{
			name:            "a human's rocket overrides an answered comment",
			reactions:       []*githubv39.Reaction{reaction("+1", "watcher-bot"), reaction("rocket", "human-alice")},
			wantOutstanding: true,
			wantUnresolved:  false,
		},
		{
			name: "a human's +1 is applause, not an outcome",
			// Only the watcher's own marks record what it has done; anyone
			// else agreeing with the comment says nothing about the work.
			reactions:       []*githubv39.Reaction{reaction("+1", "human-alice")},
			wantOutstanding: true,
			wantUnresolved:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			marks := readMarks(tc.reactions, nil, "")
			if got := marks.Outstanding(); got != tc.wantOutstanding {
				t.Errorf("Outstanding() = %v, want %v (marks %+v)", got, tc.wantOutstanding, marks)
			}
			if got := marks.Unresolved(); got != tc.wantUnresolved {
				t.Errorf("Unresolved() = %v, want %v (marks %+v)", got, tc.wantUnresolved, marks)
			}
		})
	}
}

// reactionsFixture serves one commented-on pull request and records the
// reaction writes made against it.
type reactionsFixture struct {
	mu sync.Mutex
	// existing are the reactions already on comment 100.
	existing []*githubv39.Reaction
	// added are the contents written, and deleted the reaction IDs withdrawn.
	added   []string
	deleted []string
}

func newReactionsFixture(t *testing.T, existing []*githubv39.Reaction) (*reactionsFixture, *github.Client) {
	t.Helper()
	f := &reactionsFixture{existing: existing}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")

		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/repos/o/r/issues/7/comments":
			_ = json.NewEncoder(w).Encode([]*githubv39.IssueComment{{
				ID:   githubv39.Int64(100),
				User: &githubv39.User{Login: stringPtr("human-alice")},
			}})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/reactions"):
			_ = json.NewEncoder(w).Encode(f.existing)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/reactions"):
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.added = append(f.added, body["content"])
			_ = json.NewEncoder(w).Encode(&githubv39.Reaction{})
		case r.Method == http.MethodDelete && strings.Contains(path, "/reactions/"):
			parts := strings.Split(path, "/reactions/")
			f.deleted = append(f.deleted, parts[len(parts)-1])
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	t.Cleanup(server.Close)

	gh := githubv39.NewClient(nil)
	parsed, _ := url.Parse(server.URL + "/")
	gh.BaseURL = parsed
	gh.UploadURL = parsed
	return f, github.ForRepo(gh, "o", "r")
}

// TestResolveCommentReactions_SuccessWithdrawsAStaleFailure is the guard on the
// one state that would otherwise be unrecoverable: a comment that failed once
// and succeeded later.
//
// Reactions are a set rather than a log, so the '+1' cannot supersede the
// 'confused' - both would sit there, and every later scan would read the
// comment as still owed an answer and queue the work again.
func TestResolveCommentReactions_SuccessWithdrawsAStaleFailure(t *testing.T) {
	failure := reaction("confused", "watcher-bot")
	failure.ID = githubv39.Int64(555)
	f, client := newReactionsFixture(t, []*githubv39.Reaction{
		reaction("eyes", "watcher-bot"),
		failure,
	})

	ResolveCommentReactions(context.Background(), client, 7, true, nil, "")

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.added) != 1 || f.added[0] != "+1" {
		t.Errorf("reactions added = %v, want [+1]", f.added)
	}
	if len(f.deleted) != 1 || f.deleted[0] != "555" {
		t.Errorf("reactions deleted = %v, want [555] (the stale failure mark)", f.deleted)
	}
}

// TestResolveCommentReactions_FailureMarksTheComment checks the other outcome:
// a failed task leaves the mark that puts the comment back in front of the next
// scan, and withdraws nothing.
func TestResolveCommentReactions_FailureMarksTheComment(t *testing.T) {
	f, client := newReactionsFixture(t, []*githubv39.Reaction{reaction("eyes", "watcher-bot")})

	ResolveCommentReactions(context.Background(), client, 7, false, nil, "")

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.added) != 1 || f.added[0] != "confused" {
		t.Errorf("reactions added = %v, want [confused]", f.added)
	}
	if len(f.deleted) != 0 {
		t.Errorf("reactions deleted = %v, want none", f.deleted)
	}
}

// TestResolveCommentReactions_UntouchedCommentIsLeftAlone checks that only the
// comments this task picked up are closed out. A comment with no 'eyes' was
// never handed over, so recording an outcome on it would be a lie - and a '+1'
// would hide feedback that has never been looked at.
func TestResolveCommentReactions_UntouchedCommentIsLeftAlone(t *testing.T) {
	f, client := newReactionsFixture(t, nil)

	ResolveCommentReactions(context.Background(), client, 7, true, nil, "")

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.added) != 0 {
		t.Errorf("reactions added = %v, want none", f.added)
	}
}

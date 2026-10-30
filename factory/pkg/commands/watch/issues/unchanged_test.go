package issues

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/taskoutput"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/watch/rules"
)

// A fix that changed nothing said why on the issue (open-pr's comment,
// with the unchanged marker) and opened no PR: fix is not queued again,
// whatever else touched the issue since, until a trusted human comments
// after it. An issue with no comments costs no read of them.
func TestQueueTasks_UnchangedFixHoldsUntilAnswered(t *testing.T) {
	set, err := rules.Builtin()
	if err != nil {
		t.Fatal(err)
	}
	control, err := rules.NewControl(nil, set)
	if err != nil {
		t.Fatal(err)
	}
	at := func(min int) *time.Time {
		t := time.Now().Add(-time.Hour).Add(time.Duration(min) * time.Minute)
		return &t
	}
	comment := func(min int, login, assoc, body string) *githubv39.IssueComment {
		return &githubv39.IssueComment{ID: githubv39.Int64(int64(min)), Body: githubv39.String(body), CreatedAt: at(min),
			User: &githubv39.User{Login: githubv39.String(login)}, AuthorAssociation: githubv39.String(assoc)}
	}
	unchanged := "Already fixed by #6.\n\n" + taskoutput.UnchangedMarker + "\n\n<!-- factory:task-output kind=Change task=fix-1 -->"
	for _, tc := range []struct {
		name     string
		comments []*githubv39.IssueComment
		fixed    bool
	}{
		{"no comments", nil, true},
		{"a request", []*githubv39.IssueComment{comment(1, "alice", "MEMBER", "please fix")}, true},
		{"the fix said nothing should change", []*githubv39.IssueComment{comment(1, "alice", "MEMBER", "please fix"), comment(2, "bot1", "NONE", unchanged)}, false},
		{"an outsider's reply", []*githubv39.IssueComment{comment(2, "bot1", "NONE", unchanged), comment(3, "mallory", "NONE", "do it anyway")}, false},
		{"a maintainer's answer", []*githubv39.IssueComment{comment(2, "bot1", "NONE", unchanged), comment(3, "alice", "MEMBER", "It is not fixed: see foo.go.")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			read := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/timeline"):
					_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/issues/7/comments"):
					read = true
					comments := tc.comments
					if comments == nil {
						comments = []*githubv39.IssueComment{}
					}
					_ = json.NewEncoder(w).Encode(comments)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/labels"):
					_, _ = w.Write([]byte("[]"))
				default:
					_, _ = w.Write([]byte("{}"))
				}
			}))
			t.Cleanup(server.Close)
			gh := githubv39.NewClient(nil)
			gh.BaseURL, _ = url.Parse(server.URL + "/")
			s, queue, _ := newScanner(t, Config{TargetAssignee: "bot1", BotUsers: []string{"bot1"}, TriggerLabel: "factory"}, Deps{
				GitHub:   github.ForRepo(gh, "test-owner", "test-repo"),
				Recorder: rules.NewRecorder(filepath.Join(t.TempDir(), "d.jsonl"), 0, nil),
				Rules:    control,
			})
			s.queueTasks(context.Background(), []*githubv39.Issue{{
				Number: githubv39.Int(7), UpdatedAt: at(10), Body: githubv39.String("The reader panics."),
				Labels: []*githubv39.Label{{Name: githubv39.String("factory")}}, Comments: githubv39.Int(len(tc.comments)),
				User: &githubv39.User{Login: githubv39.String("alice")}, AuthorAssociation: githubv39.String("MEMBER"),
			}}, map[int]bool{})
			mu.Lock()
			defer mu.Unlock()
			if _, fixed := queue.enqueued["task-issue-7-rule-fix.yaml"]; fixed != tc.fixed {
				t.Errorf("fix queued %v, want %v: %v", fixed, tc.fixed, queue.enqueued)
			}
			if len(tc.comments) == 0 && read {
				t.Error("read the comments of an issue with none")
			}
		})
	}
}

// A fix's word that nothing should change stands until a trusted human
// comments after it: not a pool bot's or the watcher's comment, an
// outsider's, an app's, or one with the ignore prefix. Only the pool's or
// the watcher's unchanged comment counts. Who is a trusted human is
// conventions.HumanSpokeAt's, as for a give-up's "since human".
func TestUnchangedStands(t *testing.T) {
	s := &Scanner{cfg: Config{TriggerLabel: "overseer", GitHubLogin: "watcher", BotUsers: []string{"coder-bot"}, TrustedLogins: []string{"trusted"}}}
	c := func(min int, login, assoc, body string) *githubv39.IssueComment {
		at := time.Date(2026, 10, 25, 10, min, 0, 0, time.UTC)
		return &githubv39.IssueComment{ID: githubv39.Int64(int64(min)), Body: githubv39.String(body), CreatedAt: &at,
			User: &githubv39.User{Login: githubv39.String(login)}, AuthorAssociation: githubv39.String(assoc)}
	}
	said := "Already fixed by #6.\n\n" + taskoutput.UnchangedMarker
	for _, tc := range []struct {
		name     string
		comments []*githubv39.IssueComment
		want     bool
	}{
		{"none", []*githubv39.IssueComment{c(1, "alice", "MEMBER", "please fix")}, false},
		{"the pool's", []*githubv39.IssueComment{c(1, "alice", "MEMBER", "please fix"), c(2, "coder-bot", "NONE", said)}, true},
		{"the watcher's", []*githubv39.IssueComment{c(2, "watcher", "NONE", said)}, true},
		{"an outsider's marker", []*githubv39.IssueComment{c(2, "mallory", "NONE", said)}, false},
		{"a member answers", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "alice", "MEMBER", "look at foo.go")}, false},
		{"a trusted login answers", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "trusted", "CONTRIBUTOR", "no")}, false},
		{"an outsider answers", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "mallory", "NONE", "fix it!")}, true},
		{"an app answers", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "renovate[bot]", "NONE", "bump")}, true},
		{"the bots carry on", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "watcher", "NONE", "graph status")}, true},
		{"an ignored comment", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "alice", "MEMBER", "/overseer-ignore note to self")}, true},
		{"said again after an answer", []*githubv39.IssueComment{c(2, "coder-bot", "NONE", said), c(3, "alice", "MEMBER", "look again"), c(4, "coder-bot", "NONE", said)}, true},
	} {
		if got := s.unchangedStands(tc.comments); got != tc.want {
			t.Errorf("%s: stands %v, want %v", tc.name, got, tc.want)
		}
	}
}

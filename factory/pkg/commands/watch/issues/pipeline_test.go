package issues

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

const pipelineParentBody = "Onboard Foo.\n\n```factory-pipeline\npipeline: demo\nparams:\n  kind: Foo\n```\n"

// TestQueueTasks_RoutesPipelineParents pins that an issue with a
// factory-pipeline block is never queued as a task: it is labelled
// '<trigger>/pipeline' for the reconciler (when pipelines are enabled), and
// left alone otherwise. A pipeline's children are ordinary issues.
func TestQueueTasks_RoutesPipelineParents(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		var mu sync.Mutex
		var labelled []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/labels"):
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				labelled = append(labelled, r.URL.Path+" "+string(body))
				mu.Unlock()
				_, _ = w.Write([]byte("[]"))
			case strings.HasSuffix(r.URL.Path, "/timeline"):
				_ = json.NewEncoder(w).Encode([]*githubv39.Timeline{})
			case r.URL.Path == "/search/issues":
				_ = json.NewEncoder(w).Encode(githubv39.IssuesSearchResult{Total: githubv39.Int(0)})
			default:
				_, _ = w.Write([]byte("{}"))
			}
		}))
		gh := githubv39.NewClient(nil)
		gh.BaseURL, _ = url.Parse(server.URL + "/")
		s, queue, _ := newScanner(t, Config{TriggerLabel: "factory", LabelPipelines: enabled}, Deps{GitHub: github.ForRepo(gh, "test-owner", "test-repo")})

		s.queueTasks(context.Background(), []*githubv39.Issue{
			fanoutIssue(100, pipelineParentBody, "factory"),
			fanoutIssue(101, "Do step one.\n\n<!-- factory:pipeline parent=100 pipeline=demo step=one params=0123456789abcdef -->\n", "factory"),
		}, map[int]bool{})
		server.Close()

		for name := range queue.enqueued {
			if strings.Contains(name, "100") {
				t.Errorf("enabled=%v: the parent was queued: %s", enabled, name)
			}
		}
		found101 := false
		for name := range queue.enqueued {
			if strings.Contains(name, "101") {
				found101 = true
			}
		}
		if !found101 {
			t.Errorf("enabled=%v: the child was not queued: %v", enabled, queue.enqueued)
		}
		wantLabel := enabled
		gotLabel := len(labelled) == 1 && strings.Contains(labelled[0], "/issues/100/labels") && strings.Contains(labelled[0], "factory/pipeline")
		if gotLabel != wantLabel {
			t.Errorf("enabled=%v: labelled %v", enabled, labelled)
		}
	}
}

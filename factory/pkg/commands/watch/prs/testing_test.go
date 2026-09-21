package prs

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/concurrency"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/sandbox"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/k8s"
)

func stringPtr(s string) *string { return &s }

// isReviewCommentsRequest reports whether the request is for the inline
// comments of one review.
//
// Any test server whose fixture returns a review needs a case for this, because
// fetchHistory reads the inline comments of every review it lists. Falling
// through to a default handler that answers with a JSON object is not harmless:
// the client is decoding into an array, and that failure now aborts the whole
// evaluation rather than leaving the review looking like it had no inline
// comments.
func isReviewCommentsRequest(r *http.Request) bool {
	return r.Method == http.MethodGet &&
		strings.Contains(r.URL.Path, "/reviews/") &&
		strings.HasSuffix(r.URL.Path, "/comments")
}

// isReadReactionsRequest reports whether the request reads the reactions on a
// comment, as opposed to adding one.
//
// Any test server whose fixture returns a comment needs a case for this, for
// the same reason as isReviewCommentsRequest: evaluateComments asks for each
// candidate comment's reactions to decide whether it was already acknowledged,
// and an unstubbed read is now a failure rather than a comment that reads as
// unmarked.
func isReadReactionsRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/reactions")
}

// newTestKubeClient returns a client backed by an empty fake cluster, which
// makes every sandbox lookup report "not running" - the state in which the
// scanner is free to queue work.
func newTestKubeClient() *clients.KubernetesClient {
	scheme := runtime.NewScheme()
	fakeDynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		k8s.SandboxGVR: "SandboxList",
	})
	return &clients.KubernetesClient{
		DynamicClient: fakeDynamic,
	}
}

// testOpts are the parts of a Scanner's configuration a test cares about. The
// rest are fixed by newTestScanner so that individual tests do not restate them.
type testOpts struct {
	// GitHub is the client, normally pointed at an httptest server.
	GitHub *githubv39.Client
	// Kube backs the sandbox service. A nil client makes every sandbox lookup
	// report "not running", which is what most tests want.
	Kube *clients.KubernetesClient
	// BotUsers is the pool of accounts whose pull requests are evaluated.
	BotUsers []string
	// GitHubLogin is the watcher's own account.
	GitHubLogin string
	// TriggerLabel is the label prefix under test.
	TriggerLabel string
	// ReviewerLogins are the accounts whose reviews count as review feedback.
	ReviewerLogins []string
	// AllowlistedBots are the automated accounts whose comments are acted on.
	AllowlistedBots []string
	// MinNumber skips pull requests numbered below it.
	MinNumber int
}

// newTestScanner builds a Scanner over a real queue manager rooted at tempDir,
// and returns both so that a test can assert on the queue as well as on GitHub.
//
// The worker pool is pinned to one so that a cycle evaluates its candidates in
// the order they were given: several tests assert on the sequence of calls the
// httptest server saw, which a concurrent pool would interleave.
func newTestScanner(t *testing.T, tempDir string, opts testOpts) (*Scanner, *concurrency.TaskQueueManager) {
	t.Helper()

	incomingDir := filepath.Join(tempDir, "incoming")
	processingDir := filepath.Join(tempDir, "processing")
	processedDir := filepath.Join(tempDir, "processed")
	for _, dir := range []string{incomingDir, processingDir, processedDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
	}

	queue := concurrency.NewTaskQueueManager(concurrency.TaskQueueManagerConfig{
		QueueDir:      tempDir,
		IncomingDir:   incomingDir,
		ProcessingDir: processingDir,
		ProcessedDir:  processedDir,
	})

	sandboxes := sandbox.NewService(sandbox.ServiceConfig{
		Namespace: "test-ns",
		Owner:     "test-owner",
		Repo:      "test-repo",
	}, sandbox.ServiceDeps{
		Kube:   opts.Kube,
		GitHub: opts.GitHub,
	})

	scanner := New(Config{
		Workers:         1,
		TriggerLabel:    opts.TriggerLabel,
		GitHubLogin:     opts.GitHubLogin,
		BotUsers:        opts.BotUsers,
		ReviewerLogins:  opts.ReviewerLogins,
		AllowlistedBots: opts.AllowlistedBots,
		MinNumber:       opts.MinNumber,
	}, Deps{
		GitHub:    github.ForRepo(opts.GitHub, "test-owner", "test-repo"),
		Queue:     queue,
		Entities:  concurrency.NewEntityStateCache(),
		Sandboxes: sandboxes,
	})

	return scanner, queue
}

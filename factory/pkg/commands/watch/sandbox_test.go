package watch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestIsSandboxTaskCompleted(t *testing.T) {
	scheme := runtime.NewScheme()
	ctx := context.Background()
	ns := "test-ns"

	tests := []struct {
		taskType          string
		annotatedTaskType string
		state             string
		expectedCompleted bool
	}{
		{"pr-comments", "address-comments", "Completed", true},
		{"pr-comments", "address-comments", "Running", false},
		{"pr-investigate", "investigate", "Completed", true},
		{"pr-iterate", "iterate", "Completed", true},
		{"pr-review", "review", "Completed", true},
		{"issue-fix", "fix-issue", "Completed", true},
		{"agent-chore", "agent", "Completed", true},
		{"pr-comments", "wrong-type", "Completed", false},
	}

	for _, tc := range tests {
		sbName := fmt.Sprintf("sb-%s-%s", tc.taskType, tc.state)
		fakeDynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
			k8s.SandboxGVR: "SandboxList",
		})
		kubeClient := &clients.KubernetesClient{
			DynamicClient: fakeDynamic,
		}

		sb := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "agents.x-k8s.io/v1alpha1",
				"kind":       "Sandbox",
				"metadata": map[string]interface{}{
					"name":      sbName,
					"namespace": ns,
					"annotations": map[string]interface{}{
						"sandbox.gemini.google.com/last-task-state": tc.state,
						"sandbox.gemini.google.com/last-task-type":  tc.annotatedTaskType,
					},
				},
			},
		}

		_, err := fakeDynamic.Resource(k8s.SandboxGVR).Namespace(ns).Create(ctx, sb, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create mock sandbox: %v", err)
		}

		completed, err := isSandboxTaskCompleted(ctx, kubeClient, ns, sbName, tc.taskType)
		if err != nil {
			t.Errorf("Unexpected error for %s: %v", tc.taskType, err)
		}
		if completed != tc.expectedCompleted {
			t.Errorf("For taskType=%s, annotatedTaskType=%s, state=%s: expected completed=%v, got %v",
				tc.taskType, tc.annotatedTaskType, tc.state, tc.expectedCompleted, completed)
		}
	}
}

func TestResolveSandboxName(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	fakeDynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		k8s.SandboxGVR: "SandboxList",
	})
	kubeClient := &clients.KubernetesClient{DynamicClient: fakeDynamic}

	name := resolveSandboxName(ctx, kubeClient, nil, "default", "issue-fix", 123, "owner", "myrepo")
	if name != "fix-myrepo-123" {
		t.Errorf("expected fix-myrepo-123, got %s", name)
	}

	namePR := resolveSandboxName(ctx, kubeClient, nil, "default", "pr-comments", 456, "owner", "myrepo")
	if namePR != "factory-pr-456" {
		t.Errorf("expected factory-pr-456, got %s", namePR)
	}
}

func TestParseEvictionAge(t *testing.T) {
	d, err := parseEvictionAge("")
	if err != nil || d != 7*24*time.Hour {
		t.Errorf("expected default 7d, got %v (err: %v)", d, err)
	}

	d, err = parseEvictionAge("14d")
	if err != nil || d != 14*24*time.Hour {
		t.Errorf("expected 14d, got %v (err: %v)", d, err)
	}

	d, err = parseEvictionAge("12h")
	if err != nil || d != 12*time.Hour {
		t.Errorf("expected 12h, got %v (err: %v)", d, err)
	}
}

func TestSandboxUsageMeta(t *testing.T) {
	u := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "wf-issue-123",
				"labels": map[string]interface{}{
					"factory.gemini.google.com/pr":       "456",
					"factory.gemini.google.com/workflow": "kcc-greenfield",
				},
			},
		},
	}

	meta := sandboxUsageMeta(u, "owner/repo")
	if meta.Repo != "owner/repo" || meta.PR != 456 || meta.WorkflowName != "kcc-greenfield" || meta.Issue != 123 {
		t.Errorf("unexpected metadata: %+v", meta)
	}
}

func TestIsPRTask(t *testing.T) {
	if !isPRTask("pr-investigate") || !isPRTask("pr-comments") || !isPRTask("pr-iterate") {
		t.Errorf("expected PR tasks to return true")
	}
	if isPRTask("issue-fix") || isPRTask("agent-chore") {
		t.Errorf("expected non-PR tasks to return false")
	}
}

func TestSelectUserForTask(t *testing.T) {
	ctx := context.Background()
	cfg := &config.FactoryConfig{
		Roles: map[string]config.RoleConfig{
			"coder": {
				Users: []string{"bot-coder"},
				Tasks: []string{"issue-fix"},
			},
			"reviewer": {
				Users: []string{"bot-reviewer"},
				Tasks: []string{"pr-review"},
			},
		},
	}

	user, err := selectUserForTask(ctx, nil, nil, "default", cfg, "issue-fix", 0, "owner", "repo")
	if err != nil || user != "bot-coder" {
		t.Errorf("expected bot-coder, got %s (err: %v)", user, err)
	}
}

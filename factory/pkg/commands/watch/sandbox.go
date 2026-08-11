package watch

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/envd"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/k8s"
	factorysandbox "github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/sandbox"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/usagereport"
	githubv39 "github.com/google/go-github/v39/github"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
)

func sandboxUsageMeta(item *unstructured.Unstructured, repo string) usagereport.Meta {
	meta := usagereport.Meta{Repo: repo}
	labels := item.GetLabels()
	if prStr := labels["factory.gemini.google.com/pr"]; prStr != "" {
		if n, err := strconv.Atoi(prStr); err == nil {
			meta.PR = n
		}
	}
	if wf := labels["factory.gemini.google.com/workflow"]; wf != "" {
		meta.WorkflowName = wf
	}
	if numStr, ok := strings.CutPrefix(item.GetName(), "wf-issue-"); ok {
		if n, err := strconv.Atoi(numStr); err == nil {
			meta.Issue = n
			meta.Workflow = "issue-" + numStr
		}
	}
	return meta
}

func resolveSandboxName(ctx context.Context, kubeClient *clients.KubernetesClient, ghClient *githubv39.Client, namespace, taskType string, num int, owner, repo string) string {
	if taskType == "issue-fix" || taskType == "agent-chore" {
		wfName := fmt.Sprintf("wf-issue-%d", num)
		if kubeClient != nil {
			if _, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).Get(ctx, wfName, metav1.GetOptions{}); err == nil {
				return wfName
			}
		}
		return fmt.Sprintf("fix-%s-%d", repo, num)
	}

	// For PR tasks, check if there's an existing sandbox with the PR label
	if kubeClient != nil {
		listOpts := metav1.ListOptions{
			LabelSelector: fmt.Sprintf("factory.gemini.google.com/pr=%d", num),
		}
		sbs, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, listOpts)
		if err == nil && len(sbs.Items) > 0 {
			return sbs.Items[0].GetName()
		}
	}

	// If no sandbox is labeled with this PR, try to find a matching issue sandbox by checking referenced issues
	if kubeClient != nil && ghClient != nil && owner != "" {
		pr, _, err := ghClient.PullRequests.Get(ctx, owner, repo, num)
		if err == nil {
			// Find referenced issue numbers
			referencedIssues := getReferencedIssues(pr)
			for issueNum := range referencedIssues {
				// Check if there is an active/existing sandbox for this issue
				issueSandboxName := fmt.Sprintf("fix-%s-%d", repo, issueNum)
				if _, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).Get(ctx, issueSandboxName, metav1.GetOptions{}); err == nil {
					// We found a matching issue sandbox! Alias it to the PR now for future lookups.
					klog.Infof("Self-healing: Found matching issue sandbox '%s' for PR #%d. Aliasing sandbox to PR...", issueSandboxName, num)
					if aliasErr := factorysandbox.AliasSandboxToPR(ctx, kubeClient, namespace, issueSandboxName, num, pr.GetHTMLURL()); aliasErr != nil {
						klog.Warningf("Failed to dynamically alias sandbox '%s' to PR #%d: %v", issueSandboxName, num, aliasErr)
					}
					return issueSandboxName
				}
			}
		}
	}

	return fmt.Sprintf("factory-pr-%d", num)
}

func isSandboxTaskRunning(ctx context.Context, kubeClient *clients.KubernetesClient, namespace, name string) (bool, error) {
	unstructObj, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}

	annotations := unstructObj.GetAnnotations()
	if annotations == nil {
		return true, nil
	}

	state := annotations["sandbox.gemini.google.com/last-task-state"]
	if state == "" || strings.EqualFold(state, "Running") {
		// First check if the sandbox pod has failed or been evicted before calling envd.Connect
		podList, err := kubeClient.Clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("sandbox=%s", name)})
		if err == nil && len(podList.Items) > 0 {
			hasLiveOrPending := false
			var lastFailedPod *corev1.Pod
			for i := range podList.Items {
				pod := &podList.Items[i]
				if pod.DeletionTimestamp == nil {
					if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
						hasLiveOrPending = true
					} else if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || strings.EqualFold(pod.Status.Reason, "Evicted") {
						lastFailedPod = pod
					}
				}
			}
			if !hasLiveOrPending && lastFailedPod != nil {
				reason := lastFailedPod.Status.Reason
				if reason == "" {
					reason = string(lastFailedPod.Status.Phase)
				}
				taskState := "Failed"
				if lastFailedPod.Status.Phase == corev1.PodSucceeded {
					taskState = "Completed"
				}
				taskType := annotations["sandbox.gemini.google.com/last-task-type"]
				if taskType == "" {
					taskType = "task"
				}
				klog.Warningf("Sandbox %s pod is in %s state (reason: %s). Updating sandbox annotation from Running to %s.", name, lastFailedPod.Status.Phase, reason, taskState)
				_ = factorysandbox.UpdateSandboxTaskAnnotation(ctx, kubeClient, namespace, name, taskType, taskState)
				if strings.EqualFold(reason, "Evicted") || (lastFailedPod.Status.Phase == corev1.PodFailed && strings.EqualFold(lastFailedPod.Status.Reason, "Evicted")) {
					klog.Infof("Deleting evicted pod %s so controller can recreate it.", lastFailedPod.Name)
					_ = factorysandbox.IncrementSandboxEvictionCount(ctx, kubeClient, namespace, name)
					_ = kubeClient.Clientset.CoreV1().Pods(namespace).Delete(ctx, lastFailedPod.Name, metav1.DeleteOptions{})
				}
				return false, nil
			}
		}

		// Verify if the task has actually finished by connecting to the sandbox via envd
		client, err := envd.Connect(ctx, namespace, name)
		if err == nil {
			defer client.Close()
			var buf bytes.Buffer
			// Check exit_code of the latest task, and fallback to checking process viability via PID
			checkCmd := `task_dir=$(ls -td /workspaces/tasks/* 2>/dev/null | head -1)
			if [ -z "$task_dir" ]; then
				echo "NOTASKS"
			elif [ -s "$task_dir/exit_code" ]; then
				cat "$task_dir/exit_code"
			else
				pid=$(cat "$task_dir/pid" 2>/dev/null)
				if [ -n "$pid" ]; then
					stat=$(ps -o stat= -p "$pid" 2>/dev/null | cut -c 1)
					if ! kill -0 "$pid" 2>/dev/null || [ "$stat" = "Z" ]; then
						echo "137" # Report SIGKILL/Crashed/Zombie fallback exit code
					else
						echo "RUNNING"
					fi
				else
					echo "NOTASKS"
				fi
			fi`
			if err := client.Exec(ctx, checkCmd, "/workspaces", nil, nil, &buf, nil); err == nil {
				exitStr := strings.TrimSpace(buf.String())
				if exitStr == "NOTASKS" {
					return false, nil
				} else if exitStr == "RUNNING" {
					return true, nil
				} else if exitStr != "" {
					// Task has finished!
					taskState := "Completed"
					if exitStr != "0" {
						taskState = "Failed"
					}
					taskType := annotations["sandbox.gemini.google.com/last-task-type"]
					if taskType == "" {
						taskType = "task"
					}
					klog.Infof("Detected completed task %s inside sandbox %s with exit code %s. Updating sandbox annotation to %s.", taskType, name, exitStr, taskState)
					_ = factorysandbox.UpdateSandboxTaskAnnotation(ctx, kubeClient, namespace, name, taskType, taskState)
					return false, nil
				}
			}
		} else if strings.Contains(err.Error(), "cannot connect to terminated pod") || strings.Contains(err.Error(), "is in Failed state") {
			taskType := annotations["sandbox.gemini.google.com/last-task-type"]
			if taskType == "" {
				taskType = "task"
			}
			klog.Warningf("Sandbox %s pod cannot be connected (%v). Updating sandbox annotation to Failed.", name, err)
			_ = factorysandbox.UpdateSandboxTaskAnnotation(ctx, kubeClient, namespace, name, taskType, "Failed")
			return false, nil
		}
		return true, nil
	}

	return false, nil
}

func isSandboxTaskCompleted(ctx context.Context, kubeClient *clients.KubernetesClient, namespace, name, taskType string) (bool, error) {
	unstructObj, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}

	annotations := unstructObj.GetAnnotations()
	if annotations == nil {
		return false, nil
	}

	state := annotations["sandbox.gemini.google.com/last-task-state"]
	tType := annotations["sandbox.gemini.google.com/last-task-type"]

	sbTaskType := taskType
	switch taskType {
	case "issue-fix":
		sbTaskType = "fix-issue"
	case "agent-chore":
		sbTaskType = "agent"
	case "pr-comments":
		sbTaskType = "address-comments"
	case "pr-investigate":
		sbTaskType = "investigate"
	case "pr-iterate":
		sbTaskType = "iterate"
	case "pr-review":
		sbTaskType = "review"
	}

	if strings.EqualFold(state, "Completed") && strings.EqualFold(tType, sbTaskType) {
		return true, nil
	}
	return false, nil
}

func countRunningSandboxTasks(ctx context.Context, kubeClient *clients.KubernetesClient, namespace string) (int, error) {
	list, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range list.Items {
		if factorysandbox.IsCurrentSandbox(ctx, kubeClient, &item, namespace) {
			continue
		}

		if spec, ok := item.Object["spec"].(map[string]interface{}); ok {
			if r, ok := spec["replicas"].(int64); ok && r == 0 {
				continue
			}
			if r, ok := spec["replicas"].(float64); ok && int64(r) == 0 {
				continue
			}
			if r, ok := spec["replicas"].(int); ok && r == 0 {
				continue
			}
		}

		annotations := item.GetAnnotations()
		if annotations == nil {
			count++
			continue
		}

		state := annotations["sandbox.gemini.google.com/last-task-state"]
		if state == "" || strings.EqualFold(state, "Running") {
			count++
		}
	}
	return count, nil
}

func reconcileRunningSandboxes(ctx context.Context, kubeClient *clients.KubernetesClient, namespace string) {
	if kubeClient == nil {
		return
	}
	list, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list sandboxes during reconcileRunningSandboxes: %v", err)
		return
	}
	for _, item := range list.Items {
		if factorysandbox.IsCurrentSandbox(ctx, kubeClient, &item, namespace) {
			continue
		}
		annotations := item.GetAnnotations()
		state := ""
		if annotations != nil {
			state = annotations["sandbox.gemini.google.com/last-task-state"]
		}
		if state == "" || strings.EqualFold(state, "Running") {
			sbName := item.GetName()
			running, err := isSandboxTaskRunning(ctx, kubeClient, namespace, sbName)
			if err != nil {
				klog.Warningf("Failed to check status of running sandbox %s during reconcile: %v", sbName, err)
			} else if !running {
				klog.Infof("Sandbox '%s' task completed or stopped running; annotation updated.", sbName)
			}
		}
	}
}

func cleanupClosedPRSandboxes(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, owner, repo, namespace string, openPRs map[int]bool, dryRun bool) error {
	list, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing sandboxes for cleanup: %w", err)
	}

	manager := k8s.NewManager(kubeClient)
	for _, item := range list.Items {
		name := item.GetName()
		if !strings.HasPrefix(name, "factory-pr-") {
			continue
		}
		numStr := strings.TrimPrefix(name, "factory-pr-")
		prNum, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}

		if !openPRs[prNum] {
			klog.Infof("PR #%d is closed or deleted. Checking if sandbox '%s' can be cleaned up...", prNum, name)
			running, err := isSandboxTaskRunning(ctx, kubeClient, namespace, name)
			if err != nil {
				klog.Errorf("Failed to check if sandbox %s is running: %v", name, err)
				continue
			}

			if !running {
				klog.Infof("Sandbox '%s' is not running any task. Deleting sandbox...", name)
				if dryRun {
					fmt.Printf("[DRYRUN] Would cleanup sandbox '%s' for closed PR #%d\n", name, prNum)
					continue
				}

				usagereport.HarvestSandbox(ctx, namespace, name, sandboxUsageMeta(&item, owner+"/"+repo))
				if err := manager.DeleteSandbox(ctx, namespace, name); err != nil {
					klog.Errorf("Failed to delete sandbox '%s': %v", name, err)
				}
			}
		}
	}

	return nil
}

func cleanupClosedIssueSandboxes(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, owner, repo, namespace string, openIssues map[int]bool, dryRun bool) error {
	list, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing sandboxes for issue cleanup: %w", err)
	}

	manager := k8s.NewManager(kubeClient)
	for _, item := range list.Items {
		name := item.GetName()

		var issueNum int
		var isIssueSandbox bool
		if strings.HasPrefix(name, "wf-issue-") {
			numStr := strings.TrimPrefix(name, "wf-issue-")
			n, err := strconv.Atoi(numStr)
			if err == nil {
				issueNum = n
				isIssueSandbox = true
			}
		} else if strings.HasPrefix(name, fmt.Sprintf("fix-%s-", repo)) {
			numStr := strings.TrimPrefix(name, fmt.Sprintf("fix-%s-", repo))
			n, err := strconv.Atoi(numStr)
			if err == nil {
				issueNum = n
				isIssueSandbox = true
			}
		}

		if !isIssueSandbox {
			continue
		}

		if !openIssues[issueNum] {
			klog.Infof("Issue #%d is closed or deleted. Checking if sandbox '%s' can be cleaned up...", issueNum, name)
			running, err := isSandboxTaskRunning(ctx, kubeClient, namespace, name)
			if err != nil {
				klog.Errorf("Failed to check if sandbox %s is running: %v", name, err)
				continue
			}

			if !running {
				klog.Infof("Sandbox '%s' is not running any task. Deleting sandbox...", name)
				if dryRun {
					fmt.Printf("[DRYRUN] Would cleanup sandbox '%s' for closed Issue #%d\n", name, issueNum)
					continue
				}

				usagereport.HarvestSandbox(ctx, namespace, name, sandboxUsageMeta(&item, owner+"/"+repo))
				if err := manager.DeleteSandbox(ctx, namespace, name); err != nil {
					klog.Errorf("Failed to delete sandbox '%s': %v", name, err)
				}
			}
		}
	}

	return nil
}

func selectUserForTask(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, namespace string, cfg *config.FactoryConfig, taskType string, prNum int, owner, repo string) (string, error) {
	if cfg == nil || len(cfg.Roles) == 0 {
		return "", nil // default fallback to factory-user
	}

	// 1. Determine role for task type
	role := ""
	for roleName, rCfg := range cfg.Roles {
		for _, t := range rCfg.Tasks {
			if strings.EqualFold(t, taskType) {
				role = roleName
				break
			}
		}
		if role != "" {
			break
		}
	}

	if role == "" {
		switch {
		case taskType == "agent-chore":
			if rCfg, ok := cfg.Roles["agent"]; ok && len(rCfg.Users) > 0 {
				role = "agent"
			} else {
				role = "coder"
			}
		case isPRTask(taskType):
			if prNum > 0 {
				pr, _, err := ghClient.PullRequests.Get(ctx, owner, repo, prNum)
				if err == nil {
					author := pr.GetUser().GetLogin()
					if author != "" {
						inAgentPool := false
						if agentCfg, ok := cfg.Roles["agent"]; ok {
							for _, u := range agentCfg.Users {
								if strings.EqualFold(u, author) {
									inAgentPool = true
									break
								}
							}
						}
						if inAgentPool {
							role = "agent"
						} else {
							role = "coder"
						}
					}
				}
			}
			if role == "" {
				role = "coder"
			}
		case taskType == "issue-fix":
			role = "coder"
		case taskType == "pr-review":
			role = "reviewer"
		default:
			return "", nil // default fallback
		}
	}

	roleCfg, exists := cfg.Roles[role]
	if !exists || len(roleCfg.Users) == 0 {
		if role == "agent" {
			role = "coder"
			roleCfg, exists = cfg.Roles[role]
		}
		if !exists || len(roleCfg.Users) == 0 {
			return "", nil // default fallback
		}
	}

	// 2. Select bot based on new vs existing PR/Issue
	isIssueTask := taskType == "issue-fix" || taskType == "agent-chore"
	if isIssueTask {
		if prNum > 0 {
			// A. First check if a Sandbox already exists for this task on the cluster
			// and has been pinned to a specific user.
			var sandboxName string
			if taskType == "issue-fix" {
				sandboxName = fmt.Sprintf("fix-%s-%d", repo, prNum)
			} else if taskType == "agent-chore" {
				sandboxName = fmt.Sprintf("wf-issue-%d", prNum)
			}

			if sandboxName != "" && kubeClient != nil {
				sb, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).Get(ctx, sandboxName, metav1.GetOptions{})
				if err == nil {
					labels := sb.GetLabels()
					if user, ok := labels["factory.gemini.google.com/user"]; ok && user != "" {
						inPool := false
						for _, u := range roleCfg.Users {
							if strings.EqualFold(u, user) {
								inPool = true
								break
							}
						}
						if inPool {
							klog.Infof("Pinned user '%s' for issue #%d from existing sandbox '%s'", user, prNum, sandboxName)
							return user, nil
						}
					}
				}
			}

			// B. Fallback to GitHub issue assignee check
			issue, _, err := ghClient.Issues.Get(ctx, owner, repo, prNum)
			if err == nil {
				for _, a := range issue.Assignees {
					assignee := a.GetLogin()
					if assignee != "" {
						inPool := false
						for _, u := range roleCfg.Users {
							if strings.EqualFold(u, assignee) {
								inPool = true
								break
							}
						}
						if inPool {
							return assignee, nil
						}
					}
				}
			} else {
				klog.Warningf("Failed to fetch issue details for task %s #%d: %v", taskType, prNum, err)
			}
		}

		idx := time.Now().UnixNano() % int64(len(roleCfg.Users))
		return roleCfg.Users[idx], nil
	}

	if prNum > 0 {
		pr, _, err := ghClient.PullRequests.Get(ctx, owner, repo, prNum)
		if err != nil {
			return "", fmt.Errorf("fetching PR details: %w", err)
		}
		author := pr.GetUser().GetLogin()
		if author == "" {
			return "", fmt.Errorf("empty author login for PR %d", prNum)
		}

		if taskType == "pr-review" {
			reviewerRoleCfg, ok := cfg.Roles["reviewer"]
			if ok && len(reviewerRoleCfg.Users) > 0 {
				idx := time.Now().UnixNano() % int64(len(reviewerRoleCfg.Users))
				return reviewerRoleCfg.Users[idx], nil
			}
			idx := time.Now().UnixNano() % int64(len(roleCfg.Users))
			return roleCfg.Users[idx], nil
		}

		inPool := false
		for _, u := range roleCfg.Users {
			if strings.EqualFold(u, author) {
				inPool = true
				break
			}
		}
		if !inPool {
			return "", fmt.Errorf("PR author '%s' is not in the configured bot pool for role '%s'", author, role)
		}
		return author, nil
	}

	return "", nil
}

func isPRTask(taskType string) bool {
	return taskType == "pr-investigate" || taskType == "pr-comments" || taskType == "pr-iterate"
}

func cleanupStaleIdleSandboxes(ctx context.Context, kubeClient *clients.KubernetesClient, repoFullName string, namespace string, ageStr string, dryRun bool) error {
	if kubeClient == nil {
		return nil
	}

	evictionAge, err := parseEvictionAge(ageStr)
	if err != nil {
		return fmt.Errorf("parsing sandbox eviction age: %w", err)
	}

	list, err := kubeClient.DynamicClient.Resource(k8s.SandboxGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing sandboxes for idle cleanup: %w", err)
	}

	manager := k8s.NewManager(kubeClient)
	now := time.Now()
	for _, item := range list.Items {
		name := item.GetName()
		if factorysandbox.IsCurrentSandbox(ctx, kubeClient, &item, namespace) {
			continue
		}
		creationTime := item.GetCreationTimestamp().Time
		if creationTime.IsZero() {
			continue
		}

		if now.Sub(creationTime) > evictionAge {
			running, err := isSandboxTaskRunning(ctx, kubeClient, namespace, name)
			if err != nil {
				klog.Errorf("Failed to check if sandbox %s is running: %v", name, err)
				continue
			}

			if !running {
				klog.Infof("Sandbox '%s' has been idle and is older than configured eviction age %s (created: %v). Evicting...", name, ageStr, creationTime)
				if dryRun {
					fmt.Printf("[DRYRUN] Would evict stale/idle sandbox '%s'\n", name)
					continue
				}

				usagereport.HarvestSandbox(ctx, namespace, name, sandboxUsageMeta(&item, repoFullName))
				if err := manager.DeleteSandbox(ctx, namespace, name); err != nil {
					klog.Errorf("Failed to delete stale/idle sandbox '%s': %v", name, err)
				}
			}
		}
	}

	return nil
}

func parseEvictionAge(ageStr string) (time.Duration, error) {
	ageStr = strings.TrimSpace(ageStr)
	if ageStr == "" {
		return 7 * 24 * time.Hour, nil
	}

	if strings.HasSuffix(ageStr, "d") {
		daysStr := strings.TrimSuffix(ageStr, "d")
		days, err := strconv.Atoi(daysStr)
		if err != nil {
			return 0, fmt.Errorf("invalid days format %q: %w", ageStr, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return time.ParseDuration(ageStr)
}

package watch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/k8s"
	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

func runQueueTasks(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, rootOpts RootOptions, owner, repo string, targetAssignee, githubLogin, triggerLabel string, maxActions, maxPending int, taskTimeout time.Duration, queueDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir string, dryRun bool, wg *sync.WaitGroup) {
	incomingFiles, err := os.ReadDir(incomingDir)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("Failed to read incoming queue directory: %v", err)
		}
		return
	}

	var taskItems []taskItem

	for _, f := range incomingFiles {
		if f.IsDir() || !strings.HasPrefix(f.Name(), "task-") || !strings.HasSuffix(f.Name(), ".yaml") {
			continue
		}

		filename := f.Name()
		filePath := filepath.Join(incomingDir, filename)
		data, err := os.ReadFile(filePath)
		if err != nil {
			klog.Errorf("Failed to read task file %s: %v", filename, err)
			continue
		}

		var t QueueTask
		if err := yaml.Unmarshal(data, &t); err != nil {
			klog.Errorf("Failed to unmarshal task file %s: %v", filename, err)
			continue
		}

		if t.EnqueuedAt.IsZero() {
			info, err := f.Info()
			var modTime time.Time
			if err == nil {
				modTime = info.ModTime()
			}
			t.EnqueuedAt = getEnqueueTime(&t, modTime)
		}

		taskItems = append(taskItems, taskItem{
			filename: filename,
			task:     &t,
		})
	}

	tasksToRun := sortTasksFairly(taskItems)

	processingFiles, _ := os.ReadDir(processingDir)
	filesInProcessing := 0
	for _, f := range processingFiles {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "task-") && strings.HasSuffix(f.Name(), ".yaml") {
			filesInProcessing++
		}
	}

	activeSandboxesInCycle := make(map[string]bool)
	actionsTaken := 0

	for _, item := range tasksToRun {
		if isDoNotProcess(queueDir) {
			klog.Infof("[DO NOT PROCESS] Drain mode detected during cycle execution. Stopping scheduling of remaining queued tasks.")
			break
		}
		if actionsTaken >= maxActions {
			fmt.Printf("Reached maximum actions limit (%d) for this cycle. Stopping execution.\n", maxActions)
			break
		}

		runningCount, err := countRunningSandboxTasks(ctx, kubeClient, rootOpts.Namespace)
		if err != nil {
			klog.Errorf("Failed to count running sandbox tasks: %v", err)
		}
		activeCount := max(runningCount, filesInProcessing)

		if activeCount >= maxPending {
			fmt.Printf("Reached maximum pending sandboxes limit (%d). Skipping remaining queue items.\n", maxPending)
			break
		}

		filename := item.filename
		task := item.task

		sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, task.Type, task.Number, owner, repo)
		if activeSandboxesInCycle[sandboxName] {
			klog.Infof("Skipping task %s because sandbox %s is already scheduled to run a task in this cycle.", filename, sandboxName)
			continue
		}

		running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
		if err != nil {
			klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
			continue
		}
		if running {
			klog.Infof("Skipping task %s because sandbox %s is currently busy running another task.", filename, sandboxName)
			continue
		}

		if task.Number > 0 && !dryRun && ghClient != nil {
			if issueOrPR, _, err := ghClient.Issues.Get(ctx, owner, repo, task.Number); err == nil && issueOrPR != nil {
				if hasStopLabel(issueOrPR.Labels, triggerLabel) {
					klog.Infof("Skipping task %s and removing from incoming because target #%d has the stop label ('overseer/stop' or '%s/stop')", filename, task.Number, triggerLabel)
					_ = os.Remove(filepath.Join(incomingDir, filename))
					continue
				}
			}
		}

		if task.Type != "agent-chore" && task.Recovered {
			completed, err := isSandboxTaskCompleted(ctx, kubeClient, rootOpts.Namespace, sandboxName, task.Type)
			if err != nil {
				klog.Errorf("Failed to check if sandbox %s completed task: %v", sandboxName, err)
				continue
			}
			if completed {
				klog.Infof("Recovered task %s is already completed in sandbox %s. Marking as completed.", filename, sandboxName)
				if dryRun {
					continue
				}
				incomingPath := filepath.Join(incomingDir, filename)
				processedPath := filepath.Join(processedDir, filename)
				task.Status = "Completed"
				task.CompletedAt = time.Now()
				_ = writeTaskAtomically(incomingDir, filename, task)
				writeTaskJournalEvent(queueDir, filename, task, "Completed", 0)
				if err := os.Rename(incomingPath, processedPath); err != nil {
					klog.Errorf("Failed to move completed task %s to processed: %v", filename, err)
				}
				continue
			}
		}

		incomingPath := filepath.Join(incomingDir, filename)
		processingPath := filepath.Join(processingDir, filename)

		if dryRun {
			fmt.Printf("[DRYRUN] Would process task %s (Type: %s, URL: %s)\n", filename, task.Type, task.URL)
			activeSandboxesInCycle[sandboxName] = true
			actionsTaken++
			filesInProcessing++
			continue
		}

		if err := os.Rename(incomingPath, processingPath); err != nil {
			klog.Warningf("Failed to move task %s to processing (might be processed by another run): %v", filename, err)
			continue
		}

		activeSandboxesInCycle[sandboxName] = true
		task.Status = "Running"
		_ = writeTaskAtomically(processingDir, filename, task)
		writeTaskJournalEvent(queueDir, filename, task, "Started", 0)

		actionsTaken++
		filesInProcessing++

		wg.Add(1)
		go func(taskFilename string, t *QueueTask) {
			defer wg.Done()
			executeSingleTask(ctx, ghClient, kubeClient, cfg, rootOpts, owner, repo, targetAssignee, githubLogin, triggerLabel, taskTimeout, taskFilename, t, queueDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir)
		}(filename, task)
	}
}

func executeSingleTask(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, rootOpts RootOptions, owner, repo, targetAssignee, githubLogin, triggerLabel string, taskTimeout time.Duration, taskFilename string, t *QueueTask, queueDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir string) {
	fmt.Printf("Starting task %s (Type: %s, URL: %s)...\n", taskFilename, t.Type, t.URL)
	startTime := time.Now()

	taskCtx, taskCancel := context.WithTimeout(ctx, taskTimeout)
	defer taskCancel()

	if t.Number > 0 && ghClient != nil {
		if (t.Type == "issue-fix" || t.Type == "agent-chore") && t.Assignee != "" {
			klog.Infof("Assigning issue #%d to %s as claimed", t.Number, t.Assignee)
			if _, _, err := ghClient.Issues.AddAssignees(ctx, owner, repo, t.Number, []string{t.Assignee}); err != nil {
				klog.Errorf("Failed to assign issue #%d to %s: %v", t.Number, t.Assignee, err)
			}
			if t.Assignee != targetAssignee {
				if _, _, err := ghClient.Issues.RemoveAssignees(ctx, owner, repo, t.Number, []string{targetAssignee}); err != nil {
					klog.Errorf("Failed to remove watcher bot %s from issue #%d: %v", targetAssignee, t.Number, err)
				}
			}
		}

		if t.Type != "agent-chore" {
			var commentBody string
			switch t.Type {
			case "issue-fix":
				commentBody = "🤖 AI Factory started fixing this issue in a sandbox."
			case "pr-investigate":
				commentBody = "🤖 AI Factory started investigating CI check failures for this pull request."
			case "pr-comments":
				commentBody = "🤖 AI Factory started addressing review feedback for this pull request."
			case "pr-iterate":
				commentBody = "🤖 AI Factory started resolving merge conflicts / rebasing this pull request in a sandbox."
			case "pr-review":
				commentBody = "🤖 AI Factory started reviewing this pull request in a sandbox."
			}
			if commentBody != "" {
				addGitHubComment(ctx, ghClient, owner, repo, t.Number, commentBody)
			}
		}
	}

	selectedUser := t.Assignee
	var sUserErr error
	if selectedUser == "" || (isPRTask(t.Type) && strings.EqualFold(selectedUser, targetAssignee)) {
		selectedUser, sUserErr = selectUserForTask(ctx, ghClient, kubeClient, rootOpts.Namespace, cfg, t.Type, t.Number, owner, repo)
	}
	if sUserErr != nil {
		klog.Errorf("Failed to select user for task %s: %v", taskFilename, sUserErr)
		t.Status = "Failed"
		t.Error = sUserErr.Error()
		_ = writeTaskAtomically(processingDir, taskFilename, t)
		writeTaskJournalEvent(queueDir, taskFilename, t, "Failed", 0)
		processingPath := filepath.Join(processingDir, taskFilename)
		processedPath := filepath.Join(processedDir, taskFilename)
		_ = os.Rename(processingPath, processedPath)
		return
	}

	executable, err := os.Executable()
	if err != nil {
		klog.Errorf("Failed to get executable path: %v", err)
		return
	}

	var args []string
	switch t.Type {
	case "issue-fix":
		args = []string{"fix", "--url", t.URL, "--instruction", "Fix this issue"}
	case "pr-investigate":
		args = []string{"pr", "investigate", "--pr-url", t.URL}
	case "pr-comments":
		args = []string{"pr", "address-comments", "--pr-url", t.URL}
	case "pr-iterate":
		args = []string{"pr", "iterate", "--pr-url", t.URL, "--prompt", "Please resolve merge conflicts in this PR by rebasing onto the latest master/main branch and resolving any conflicts that arise."}
	case "pr-review":
		args = []string{"pr", "review", "--pr-url", t.URL, "--publish", "yes"}
		for _, inst := range t.Instructions {
			args = append(args, "--instruction", inst)
		}
	case "agent-chore":
		args = []string{"agent", "create", "--url", t.URL, "--agent", t.AgentFile}
		if t.SessionID != "" {
			args = append(args, "--session-id", t.SessionID)
		}
	default:
		klog.Errorf("Unknown task type: %s", t.Type)
		return
	}

	if rootOpts.Namespace != "" {
		args = append(args, "--namespace", rootOpts.Namespace)
	}
	if selectedUser != "" {
		args = append(args, "--user", selectedUser)
	}
	if rootOpts.Image != "" {
		args = append(args, "--image", rootOpts.Image)
	}
	if rootOpts.DiskSize != "" {
		args = append(args, "--workspace-disk-size", rootOpts.DiskSize)
	}
	if rootOpts.EphemeralStorage != "" {
		args = append(args, "--ephemeral-storage", rootOpts.EphemeralStorage)
	}
	if rootOpts.CPURequest != "" {
		args = append(args, "--cpu-request", rootOpts.CPURequest)
	}
	if rootOpts.CPULimit != "" {
		args = append(args, "--cpu-limit", rootOpts.CPULimit)
	}
	if rootOpts.MemoryRequest != "" {
		args = append(args, "--memory-request", rootOpts.MemoryRequest)
	}
	if rootOpts.MemoryLimit != "" {
		args = append(args, "--memory-limit", rootOpts.MemoryLimit)
	}
	if rootOpts.SecretName != "" {
		args = append(args, "--secret-name", rootOpts.SecretName)
	}
	for _, secretMount := range rootOpts.ResolvedSecrets {
		if secretMount.Name != "" && secretMount.MountPath != "" {
			args = append(args, "--secret-mount", fmt.Sprintf("%s=%s", secretMount.Name, secretMount.MountPath))
		}
	}
	if taskTimeout > 0 {
		args = append(args, "--timeout", taskTimeout.String())
	}
	args = append(args, "--abort-on-cancel=false")

	cmd := exec.CommandContext(taskCtx, executable, args...)

	logFilename := strings.TrimSuffix(taskFilename, ".yaml") + ".log"
	processingLogPath := filepath.Join(processingLogDir, logFilename)
	processedLogPath := filepath.Join(processedLogDir, logFilename)

	logFile, err := os.OpenFile(processingLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		klog.Errorf("Failed to create log file: %v", err)
	} else {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		defer logFile.Close()
	}

	taskErr := cmd.Run()

	processingPathLocal := filepath.Join(processingDir, taskFilename)
	processedPathLocal := filepath.Join(processedDir, taskFilename)
	duration := time.Since(startTime)

	if taskErr != nil {
		klog.Errorf("Task %s failed: %v", taskFilename, taskErr)
		t.Status = "Failed"
		t.Error = taskErr.Error()
		t.CompletedAt = time.Now()
		writeTaskJournalEvent(queueDir, taskFilename, t, "Failed", duration)
		if t.Type == "pr-comments" {
			var bots []string
			if cfg != nil {
				bots = cfg.AllowlistedBots
			}
			resolvePRCommentReactions(ctx, ghClient, owner, repo, t.Number, "confused", bots, githubLogin)
		}

		// Force clean up sandbox if the task timed out
		if taskCtx.Err() == context.DeadlineExceeded {
			var sandboxName string
			switch t.Type {
			case "issue-fix":
				if t.SessionID != "" {
					sandboxName = fmt.Sprintf("wf-issue-%d", t.Number)
				} else {
					sandboxName = fmt.Sprintf("fix-%s-%d", repo, t.Number)
				}
			case "agent-chore":
				if t.SessionID != "" {
					sandboxName = fmt.Sprintf("wf-issue-%d", t.Number)
				} else {
					sandboxName = fmt.Sprintf("agent-%s-%d", repo, t.Number)
				}
			case "pr-investigate", "pr-comments", "pr-iterate", "pr-review":
				sandboxName = resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, t.Type, t.Number, owner, repo)
			}

			if sandboxName != "" {
				klog.Warningf("Task %s timed out after %s! Force cleaning up sandbox '%s'...", taskFilename, taskTimeout, sandboxName)
				manager := k8s.NewManager(kubeClient)
				if err := manager.DeleteSandbox(ctx, rootOpts.Namespace, sandboxName); err != nil {
					klog.Errorf("Failed to delete sandbox '%s' on timeout: %v", sandboxName, err)
				}
			}
		}
	} else {
		fmt.Printf("Task %s completed successfully.\n", taskFilename)
		t.Status = "Completed"
		t.CompletedAt = time.Now()
		writeTaskJournalEvent(queueDir, taskFilename, t, "Completed", duration)
		if t.Type == "pr-comments" {
			var bots []string
			if cfg != nil {
				bots = cfg.AllowlistedBots
			}
			resolvePRCommentReactions(ctx, ghClient, owner, repo, t.Number, "+1", bots, githubLogin)
		}
	}

	_ = writeTaskAtomically(processingDir, taskFilename, t)
	if err := os.Rename(processingPathLocal, processedPathLocal); err != nil {
		klog.Errorf("Failed to move task %s to processed directory: %v", taskFilename, err)
	}
	if _, err := os.Stat(processingLogPath); err == nil {
		if err := os.Rename(processingLogPath, processedLogPath); err != nil {
			klog.Errorf("Failed to move log file %s to processed directory: %v", taskFilename, err)
		}
	}
}

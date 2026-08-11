package watch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	factorysandbox "github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/sandbox"
	githubv39 "github.com/google/go-github/v39/github"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// NewWatchCommand creates the cobra command for watching repositories and processing tasks.
func NewWatchCommand(ctx context.Context, resolveRoot ResolveRootOptionsFunc) *cobra.Command {
	var flags WatchFlags

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Watch a GitHub repo for test failures and assigned issues to automatically fix and review",
		Example: `  # Watch for unassigned issues with specific labels
  factory watch --repo owner/repo --assignee "" --labels "bug,help wanted"

  # Watch for assigned issues with labels
  factory watch --repo owner/repo --assignee "factory-bot" --labels "p0,urgent"`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var rootOpts *RootOptions
			if resolveRoot != nil {
				var err error
				rootOpts, err = resolveRoot(cmd)
				if err != nil {
					return err
				}
			}
			if rootOpts == nil {
				rootOpts = &RootOptions{
					Namespace: "default",
				}
			}

			if flags.Repo == "" {
				return fmt.Errorf("--repo is required (e.g. owner/repo)")
			}
			parts := strings.Split(flags.Repo, "/")
			if len(parts) != 2 {
				return fmt.Errorf("invalid repo format, expected owner/repo, got %s", flags.Repo)
			}

			issueMode := os.Getenv("ISSUE_MODE")
			if flags.IssueMode != "" {
				issueMode = flags.IssueMode
			}
			if issueMode == "" {
				issueMode = "enabled"
			}

			prMode := os.Getenv("PR_MODE")
			if flags.PRMode != "" {
				prMode = flags.PRMode
			}
			if prMode == "" {
				prMode = "enabled"
			}

			choresMode := os.Getenv("CHORES_MODE")
			if flags.ChoresMode != "" {
				choresMode = flags.ChoresMode
			}
			cfg, _ := config.LoadConfig()
			if cfg != nil && cfg.Chores.Mode == "disabled" {
				choresMode = "disabled"
			}
			if choresMode == "" {
				choresMode = "enabled"
			}

			return runWatch(ctx, parts[0], parts[1], flags.PollInterval, flags.Assignee, cmd.Flags().Changed("assignee"), flags.Labels, flags.DryRun, flags.WatchTimeout, flags.MaxActions, flags.MaxPending, flags.Mode, flags.QueueDir, flags.Once, issueMode, prMode, choresMode, *rootOpts, flags.ScanLimit, flags.TaskTimeout, flags.SandboxEvictionAge, flags.SandboxIdleTimeout, flags.PRInactivityTimeout)
		},
	}

	cmd.Flags().StringVar(&flags.Repo, "repo", "", "GitHub repository (e.g. owner/repo)")
	cmd.Flags().DurationVar(&flags.PollInterval, "poll-interval", 2*time.Minute, "Polling interval")
	cmd.Flags().StringVar(&flags.Assignee, "assignee", "factory-bot", "GitHub username to watch for assigned issues (use empty string for unassigned issues)")
	cmd.Flags().StringSliceVar(&flags.Labels, "labels", nil, "Comma-separated list of labels to filter issues by")
	cmd.Flags().BoolVar(&flags.DryRun, "dryrun", false, "Print actions without creating sandboxes or executing tasks")
	cmd.Flags().DurationVar(&flags.WatchTimeout, "watch-timeout", 0, "Timeout for watching (default forever)")
	cmd.Flags().IntVar(&flags.MaxActions, "max-actions", 40, "Maximum number of actions to take in a single watch loop")
	cmd.Flags().IntVar(&flags.MaxPending, "max-pending", 40, "Maximum number of pending/running sandboxes allowed before skipping actions")
	cmd.Flags().StringVar(&flags.Mode, "mode", "all", "Watch mode: all (scan & run), scan (only scan & queue), run (only process queue)")
	cmd.Flags().StringVar(&flags.QueueDir, "queue-dir", "/workspaces/queues", "Directory path for the task queues")
	cmd.Flags().BoolVar(&flags.Once, "once", false, "Run watch once and exit (waits for active tasks to complete)")
	cmd.Flags().StringVar(&flags.IssueMode, "issue-mode", "", "Issue mode: enabled or disabled (defaults to ISSUE_MODE env or enabled)")
	cmd.Flags().StringVar(&flags.PRMode, "pr-mode", "", "PR mode: enabled or disabled (defaults to PR_MODE env or enabled)")
	cmd.Flags().StringVar(&flags.ChoresMode, "chores-mode", "", "Chores mode: enabled or disabled (defaults to CHORES_MODE env or enabled)")
	cmd.Flags().IntVar(&flags.ScanLimit, "scan-limit", 100, "Maximum number of issues/PRs to fetch from GitHub API in a scan cycle")
	cmd.Flags().DurationVar(&flags.TaskTimeout, "task-timeout", 3*time.Hour, "Timeout for each task execution (default 3h)")
	cmd.Flags().StringVar(&flags.SandboxEvictionAge, "sandbox-eviction-age", "7d", "Age threshold for idle sandbox eviction (e.g. '7d', '24h')")
	cmd.Flags().DurationVar(&flags.SandboxIdleTimeout, "sandbox-idle-timeout", getEnvDuration("SANDBOX_IDLE_TIMEOUT", 0), "Idle timeout after which a sandbox that has not run any task is suspended by setting replicas to 0 (e.g. '30m', '1h')")
	cmd.Flags().DurationVar(&flags.PRInactivityTimeout, "pr-inactivity-timeout", getEnvDuration("PR_INACTIVITY_TIMEOUT", 0), "Time of inactivity with no human comments before pausing automated processing on a PR (e.g. '24h', '168h')")

	return cmd
}

type watchState struct {
	mu               sync.Mutex
	openPRs          []*githubv39.PullRequest
	referencedIssues map[int]bool
	lastPRScan       time.Time
	lastIssueScan    time.Time
	lastRunnerRun    time.Time
	shuttingDown     bool
}

func runWatch(ctx context.Context, owner, repo string, interval time.Duration, assignee string, assigneeChanged bool, labels []string, dryRun bool, watchTimeout time.Duration, maxActions int, maxPending int, mode string, queueDir string, once bool, issueMode string, prMode string, choresMode string, rootOpts RootOptions, scanLimit int, taskTimeout time.Duration, sandboxEvictionAge string, sandboxIdleTimeout time.Duration, prInactivityTimeout time.Duration) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		klog.Warningf("Failed to load factory config: %v", err)
	}
	triggerLabel := "factory"
	if cfg != nil && cfg.TriggerLabel != "" {
		triggerLabel = cfg.TriggerLabel
	}

	ghClient, err := github.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("creating github client: %w", err)
	}

	kubeClient, err := clients.NewKubernetesClient()
	if err != nil {
		return fmt.Errorf("creating k8s client: %w", err)
	}

	secret, err := kubeClient.Clientset.CoreV1().Secrets(rootOpts.Namespace).Get(ctx, rootOpts.SecretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("fetching %s secret in namespace %s: %w (make sure to run 'factory user onboard' first)", rootOpts.SecretName, rootOpts.Namespace, err)
	}
	githubLogin := string(secret.Data["GITHUB_LOGIN"])

	targetAssignee := assignee
	if !assigneeChanged {
		targetAssignee = githubLogin
	}

	var allBotUsers []string
	if cfg != nil {
		for _, rCfg := range cfg.Roles {
			for _, u := range rCfg.Users {
				if u != "" {
					allBotUsers = append(allBotUsers, u)
				}
			}
		}
	}
	if targetAssignee != "" {
		found := false
		for _, u := range allBotUsers {
			if strings.EqualFold(u, targetAssignee) {
				found = true
				break
			}
		}
		if !found {
			allBotUsers = append(allBotUsers, targetAssignee)
		}
	}

	incomingDir := filepath.Join(queueDir, "incoming")
	processingDir := filepath.Join(queueDir, "processing")
	processedDir := filepath.Join(queueDir, "processed")

	logDir := os.Getenv("FACTORY_LOGS")
	if logDir == "" {
		logDir = filepath.Join(queueDir, "logs")
	}
	processingLogDir := filepath.Join(logDir, "processing")
	processedLogDir := filepath.Join(logDir, "processed")

	if !dryRun {
		if err := os.MkdirAll(incomingDir, 0755); err != nil {
			return fmt.Errorf("failed to create incoming queue dir: %w", err)
		}
		if err := os.MkdirAll(processingDir, 0755); err != nil {
			return fmt.Errorf("failed to create processing queue dir: %w", err)
		}
		if err := os.MkdirAll(processedDir, 0755); err != nil {
			return fmt.Errorf("failed to create processed queue dir: %w", err)
		}
		if err := os.MkdirAll(processingLogDir, 0755); err != nil {
			return fmt.Errorf("failed to create processing log dir: %w", err)
		}
		if err := os.MkdirAll(processedLogDir, 0755); err != nil {
			return fmt.Errorf("failed to create processed log dir: %w", err)
		}
		go startQueueHTTPServer(ctx, queueDir, ":13338")
	}

	fmt.Printf("Starting watch for repository %s/%s (mode: %s, queueDir: %s, poll interval: %s, assignee: '%s', labels: %v, dryRun: %v, watchTimeout: %s)...\n", owner, repo, mode, queueDir, interval, targetAssignee, labels, dryRun, watchTimeout)

	var timeoutChan <-chan time.Time
	if watchTimeout > 0 {
		timeoutChan = time.After(watchTimeout)
	}

	processedIssues := make(map[int]time.Time)
	processedPRs := make(map[int]prWatchState)
	if files, err := os.ReadDir(processedDir); err == nil {
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".yaml") {
				continue
			}
			filePath := filepath.Join(processedDir, f.Name())
			if strings.HasPrefix(f.Name(), "task-issue-") {
				trimmed := strings.TrimPrefix(f.Name(), "task-issue-")
				trimmed = strings.TrimSuffix(trimmed, ".yaml")
				if num, err := strconv.Atoi(trimmed); err == nil {
					var t QueueTask
					hasTask := false
					if data, err := os.ReadFile(filePath); err == nil {
						if err := yaml.Unmarshal(data, &t); err == nil {
							hasTask = true
						}
					}
					if info, err := f.Info(); err == nil {
						tTime := info.ModTime()
						if hasTask && !t.CompletedAt.IsZero() {
							tTime = t.CompletedAt
						}
						processedIssues[num] = tTime
					}
				}
			} else if strings.HasPrefix(f.Name(), "task-pr-") {
				name := strings.TrimPrefix(f.Name(), "task-pr-")
				name = strings.TrimSuffix(name, ".yaml")

				isComments := strings.HasSuffix(name, "-comments")
				isInvestigate := strings.HasSuffix(name, "-investigate")
				isReview := strings.HasSuffix(name, "-review")
				isIterate := strings.HasSuffix(name, "-iterate")

				var numStr string
				if isComments {
					numStr = strings.TrimSuffix(name, "-comments")
				} else if isInvestigate {
					numStr = strings.TrimSuffix(name, "-investigate")
				} else if isReview {
					numStr = strings.TrimSuffix(name, "-review")
				} else if isIterate {
					numStr = strings.TrimSuffix(name, "-iterate")
				}

				if numStr != "" {
					if num, err := strconv.Atoi(numStr); err == nil {
						state := processedPRs[num]
						info, _ := f.Info()
						processedPRs[num] = parseProcessedPRTask(filePath, name, info, state)
					}
				}
			}
		}
	}

	// Recovery: Handle any leftover tasks in processingDir on startup
	if files, err := os.ReadDir(processingDir); err == nil {
		for _, f := range files {
			if !f.IsDir() && strings.HasPrefix(f.Name(), "task-") && strings.HasSuffix(f.Name(), ".yaml") {
				processingPath := filepath.Join(processingDir, f.Name())

				if data, err := os.ReadFile(processingPath); err == nil {
					var t QueueTask
					if err := yaml.Unmarshal(data, &t); err == nil {
						sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, t.Type, t.Number, owner, repo)
						if kubeClient != nil && sandboxName != "" {
							running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
							if err == nil && running {
								klog.Infof("Task %s is still actively running in sandbox %s. Leaving in processing.", f.Name(), sandboxName)
								continue
							}
							completed, err := isSandboxTaskCompleted(ctx, kubeClient, rootOpts.Namespace, sandboxName, t.Type)
							if err == nil && completed {
								klog.Infof("Task %s already completed in sandbox %s. Moving from processing to processed.", f.Name(), sandboxName)
								t.Status = "Completed"
								if t.CompletedAt.IsZero() {
									t.CompletedAt = time.Now()
								}
								if err := writeTaskAtomically(processedDir, f.Name(), &t); err == nil {
									_ = os.Remove(processingPath)
									writeTaskJournalEvent(queueDir, f.Name(), &t, "Completed", 0)
									continue
								}
							}
						}

						t.Status = "Pending"
						t.Recovered = true
						if err := writeTaskAtomically(incomingDir, f.Name(), &t); err == nil {
							_ = os.Remove(processingPath)
							klog.Infof("Recovered stuck task %s from processing to incoming", f.Name())
							continue
						}
					}
				}

				incomingPath := filepath.Join(incomingDir, f.Name())
				if err := os.Rename(processingPath, incomingPath); err == nil {
					klog.Infof("Recovered stuck task %s (fallback rename) to incoming", f.Name())
				} else {
					klog.Errorf("Failed to recover stuck task %s: %v", f.Name(), err)
				}
			}
		}
	}

	state := &watchState{
		referencedIssues: make(map[int]bool),
	}

	var wg sync.WaitGroup

	checkRepo := func() {
		state.mu.Lock()
		if state.shuttingDown {
			state.mu.Unlock()
			return
		}
		state.mu.Unlock()

		// Proactively delete any evicted sandbox pods in the namespace so the sandbox controller can recreate them or free resources.
		func() {
			podList, err := kubeClient.Clientset.CoreV1().Pods(rootOpts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "sandbox"})
			if err != nil {
				return
			}
			for i := range podList.Items {
				pod := &podList.Items[i]
				if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodFailed && strings.EqualFold(pod.Status.Reason, "Evicted") {
					klog.Infof("Found evicted sandbox pod %s in namespace %s. Deleting pod so controller can recreate or clean up.", pod.Name, rootOpts.Namespace)
					sbName := pod.Labels["sandbox"]
					if sbName == "" {
						sbName = pod.Labels["agents.x-k8s.io/sandbox"]
					}
					if sbName != "" {
						_ = factorysandbox.IncrementSandboxEvictionCount(ctx, kubeClient, rootOpts.Namespace, sbName)
					}
					_ = kubeClient.Clientset.CoreV1().Pods(rootOpts.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
				}
			}
		}()

		reconcileRunningSandboxes(ctx, kubeClient, rootOpts.Namespace)

		if isDoNotProcess(queueDir) {
			runningCount, err := countRunningSandboxTasks(ctx, kubeClient, rootOpts.Namespace)
			if err != nil {
				klog.Errorf("Failed to count running sandbox tasks during drain: %v", err)
			}
			processingFiles, _ := os.ReadDir(processingDir)
			filesInProcessing := 0
			for _, f := range processingFiles {
				if !f.IsDir() && strings.HasPrefix(f.Name(), "task-") && strings.HasSuffix(f.Name(), ".yaml") {
					filesInProcessing++
				}
			}
			klog.Infof("[DO NOT PROCESS] Drain mode active. Active child sandboxes: %d, Tasks in processing: %d. Pausing new scanning and task execution.", runningCount, filesInProcessing)
			return
		}

		now := time.Now()

		runIssueScan := false
		if mode == "all" || mode == "scan" || mode == "scan-issue" {
			if state.lastIssueScan.IsZero() || now.Sub(state.lastIssueScan) >= 30*time.Second {
				runIssueScan = true
			}
		}

		runPRScan := false
		if mode == "all" || mode == "scan" || mode == "scan-pr" {
			if state.lastPRScan.IsZero() || now.Sub(state.lastPRScan) >= 5*time.Minute {
				runPRScan = true
			}
		}

		runRunner := false
		if mode == "all" || mode == "run" {
			if state.lastRunnerRun.IsZero() || now.Sub(state.lastRunnerRun) >= 30*time.Second {
				runRunner = true
			}
		}

		state.mu.Lock()
		refIssues := make(map[int]bool)
		for k, v := range state.referencedIssues {
			refIssues[k] = v
		}
		hasPRs := len(state.openPRs) > 0 || !state.lastPRScan.IsZero()
		state.mu.Unlock()

		// Populate PR cache once on startup if needed by issue scan
		if !hasPRs && runIssueScan {
			klog.Infof("Populating open PRs cache for referenced issues...")
			prs, err := listAllOpenPRs(ctx, ghClient, owner, repo)
			if err == nil {
				state.mu.Lock()
				state.openPRs = prs
				state.referencedIssues = make(map[int]bool)
				for _, pr := range prs {
					for num := range getReferencedIssues(pr) {
						state.referencedIssues[num] = true
						refIssues[num] = true
					}
				}
				state.mu.Unlock()
			} else {
				klog.Errorf("Failed to populate open PRs cache: %v", err)
			}
		}

		// 1. Slow PR Scan Cycle
		if runPRScan {
			scanPRsSlow(ctx, ghClient, kubeClient, cfg, rootOpts, owner, repo, targetAssignee, allBotUsers, githubLogin, triggerLabel, issueMode, choresMode, sandboxEvictionAge, sandboxIdleTimeout, prInactivityTimeout, incomingDir, processingDir, processedDir, queueDir, dryRun, processedIssues, processedPRs, refIssues)
			state.mu.Lock()
			state.lastPRScan = now
			state.mu.Unlock()
		}

		// 2. Fast Issue Scan Cycle
		if runIssueScan {
			fastPRIssues, err := scanIssuesFast(ctx, ghClient, kubeClient, cfg, rootOpts.Namespace, owner, repo, targetAssignee, allBotUsers, githubLogin, scanLimit, issueMode, triggerLabel, processedIssues, refIssues, incomingDir, processingDir, processedDir, queueDir, dryRun)
			if err == nil && len(fastPRIssues) > 0 && prMode != "disabled" {
				klog.Infof("Processing %d assigned PRs in fast cycle...", len(fastPRIssues))
				processPRs(ctx, ghClient, kubeClient, cfg, rootOpts, owner, repo, fastPRIssues, allBotUsers, targetAssignee, githubLogin, triggerLabel, prInactivityTimeout, incomingDir, processingDir, processedDir, queueDir, dryRun, processedPRs)
			}
			state.mu.Lock()
			state.lastIssueScan = now
			state.mu.Unlock()
		}

		// 3. Runner Mode execution
		if runRunner {
			runQueueTasks(ctx, ghClient, kubeClient, cfg, rootOpts, owner, repo, targetAssignee, githubLogin, triggerLabel, maxActions, maxPending, taskTimeout, queueDir, incomingDir, processingDir, processedDir, processingLogDir, processedLogDir, dryRun, &wg)
			state.mu.Lock()
			state.lastRunnerRun = now
			state.mu.Unlock()
		}
	}

	checkRepo()

	if once {
		fmt.Println("Running in once mode. Waiting for active tasks to complete...")
		wg.Wait()
		fmt.Println("All tasks completed. Exiting.")
		return nil
	}

	for {
		fmt.Printf("Sleeping for 10s...\n")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutChan:
			fmt.Printf("\nWatch timeout of %s expired. Shutting down gracefully...\n", watchTimeout)
			state.mu.Lock()
			state.shuttingDown = true
			state.mu.Unlock()

			fmt.Println("Waiting for active tasks to complete...")
			doneChan := make(chan struct{})
			go func() {
				wg.Wait()
				close(doneChan)
			}()
			select {
			case <-doneChan:
				fmt.Println("All tasks completed. Exiting.")
			case <-time.After(5 * time.Minute):
				fmt.Println("Timeout waiting for active tasks to complete. Exiting.")
			}
			return nil
		case <-time.After(10 * time.Second):
			checkRepo()
		}
	}
}

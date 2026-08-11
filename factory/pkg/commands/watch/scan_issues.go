package watch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/clients"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

func queueIssueTasks(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, namespace, owner, repo string, issues []*githubv39.Issue, processedIssues map[int]time.Time, refIssues map[int]bool, targetAssignee string, allBotUsers []string, incomingDir, processingDir, processedDir, queueDir string, dryRun bool, triggerLabel string) {
	klog.Infof("queueIssueTasks called with %d issues", len(issues))
	for _, issue := range issues {
		num := issue.GetNumber()
		if cfg != nil && cfg.MinNumber > 0 && num < cfg.MinNumber {
			continue
		}
		if hasStopLabel(issue.Labels, triggerLabel) {
			klog.Infof("Skipping issue #%d because it has the stop label ('overseer/stop' or '%s/stop')", num, triggerLabel)
			removePendingTasksForNumber(incomingDir, num)
			continue
		}
		if refIssues[num] {
			klog.Infof("Skipping issue #%d because there is already a PR referencing it.", num)
			continue
		}

		// Check if the issue specifies a workflow path in its description
		workflowPath := findWorkflowPath(issue.GetBody())
		workflowName := ""
		if workflowPath != "" {
			if isWorkflowDefinition(ctx, ghClient, owner, repo, workflowPath) {
				filenameOnly := filepath.Base(workflowPath)
				ext := filepath.Ext(filenameOnly)
				workflowName = strings.TrimSuffix(filenameOnly, ext)
			} else {
				// It was just a standard skill/agent prompt mentioned, not a workflow.
				// Fallback to standard issue-fix
				workflowPath = ""
			}
		}

		filename := fmt.Sprintf("task-issue-%d.yaml", num)
		if workflowName != "" {
			filename = fmt.Sprintf("task-workflow-%s-issue-%d.yaml", Slugify(workflowName), num)
		}

		if taskExists(incomingDir, processingDir, filename) {
			continue
		}

		// Check if the workflow session already completed recently
		processedPath := filepath.Join(processedDir, filename)
		if info, err := os.Stat(processedPath); err == nil {
			lastRunTime := info.ModTime()
			if data, err := os.ReadFile(processedPath); err == nil {
				var t QueueTask
				if err := yaml.Unmarshal(data, &t); err == nil && !t.CompletedAt.IsZero() {
					lastRunTime = t.CompletedAt
				}
			}
			cooldown := getWorkflowCooldown(ctx, ghClient, owner, repo, workflowPath)
			if time.Since(lastRunTime) < cooldown {
				continue
			}
		}

		lastProcessed, ok := processedIssues[num]
		if !ok || issue.GetUpdatedAt().After(lastProcessed) || workflowName != "" {
			// Skip KRM check for workflow triggers since they don't necessarily have linked code PRs
			if workflowName == "" {
				linked, err := hasLinkedPR(ctx, ghClient, owner, repo, num)
				if err != nil {
					klog.Errorf("Failed to check linked PR for issue #%d: %v", num, err)
					continue
				} else if linked {
					klog.Infof("Skipping issue #%d because it has a linked PR according to the Timeline API.", num)
					continue
				}
			}

			sandboxName := fmt.Sprintf("fix-%s-%d", repo, num)
			if workflowName != "" {
				sandboxName = fmt.Sprintf("wf-issue-%d", num)
			}

			running, err := isSandboxTaskRunning(ctx, kubeClient, namespace, sandboxName)
			if err != nil {
				klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
				continue
			} else if running {
				klog.Infof("Skipping issue #%d because there is an in-flight sandbox %s.", num, sandboxName)
				continue
			}

			assignedBot := assignedBotUser(issue, allBotUsers)
			taskAssignee := assignedBot
			if taskAssignee == "" {
				taskAssignee = targetAssignee
			}

			issueURL := issue.GetHTMLURL()
			if issueURL == "" {
				issueURL = fmt.Sprintf("https://github.com/%s/%s/issues/%d", owner, repo, num)
			}

			taskType := "issue-fix"
			if workflowName != "" {
				taskType = "agent-chore"
			}

			task := &QueueTask{
				Type:       taskType,
				URL:        issueURL,
				Number:     num,
				Priority:   getIssuePriority(issue),
				Phase:      3,
				CreatedAt:  issue.GetCreatedAt(),
				EnqueuedAt: time.Now(),
				Assignee:   taskAssignee,
				Status:     "Pending",
			}
			if workflowName != "" {
				task.AgentFile = workflowPath
				task.SessionID = fmt.Sprintf("wf-issue-%d", num)
			}

			if dryRun {
				if workflowName != "" {
					fmt.Printf("[DRYRUN] Would queue workflow task %s for issue #%d: %s\n", workflowName, num, issueURL)
				} else {
					fmt.Printf("[DRYRUN] Would queue fix task for issue #%d: %s\n", num, issueURL)
				}
			} else {
				if workflowName != "" {
					fmt.Printf("Queueing workflow task %s for issue #%d...\n", workflowName, num)
				} else {
					fmt.Printf("Queueing fix task for issue #%d...\n", num)
				}
				processedIssues[num] = time.Now()
				if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
					klog.Errorf("Failed to queue task for issue #%d: %v", num, err)
				} else {
					writeTaskJournalEvent(queueDir, filename, task, "Created", 0)
				}
			}
		}
	}
}

func scanIssuesFast(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, namespace, owner, repo string, targetAssignee string, allBotUsers []string, githubLogin string, scanLimit int, issueMode, triggerLabel string, processedIssues map[int]time.Time, refIssues map[int]bool, incomingDir, processingDir, processedDir, queueDir string, dryRun bool) ([]*githubv39.Issue, error) {
	klog.Infof("Running fast issue scan cycle...")
	var allItems []*githubv39.Issue

	limit := scanLimit
	if limit <= 0 {
		limit = 30
	}

	for _, botUser := range allBotUsers {
		opts1 := &githubv39.IssueListByRepoOptions{
			Assignee:    botUser,
			State:       "open",
			Sort:        "updated",
			Direction:   "desc",
			ListOptions: githubv39.ListOptions{PerPage: limit},
		}
		issues1, _, err := ghClient.Issues.ListByRepo(ctx, owner, repo, opts1)
		if err != nil {
			klog.Errorf("Failed to list issues for assignee %s: %v", botUser, err)
		} else {
			klog.Infof("Fetched %d issues assigned to %s from GitHub API", len(issues1), botUser)
			allItems = append(allItems, issues1...)
		}
	}

	if githubLogin != "" {
		optsCreator := &githubv39.IssueListByRepoOptions{
			Creator:     githubLogin,
			State:       "open",
			Sort:        "updated",
			Direction:   "desc",
			ListOptions: githubv39.ListOptions{PerPage: limit},
		}
		issuesCreator, _, err := ghClient.Issues.ListByRepo(ctx, owner, repo, optsCreator)
		if err != nil {
			klog.Errorf("Failed to list issues created by %s: %v", githubLogin, err)
		} else {
			klog.Infof("Fetched %d issues created by %s from GitHub API", len(issuesCreator), githubLogin)
			for _, issue := range issuesCreator {
				if issue.PullRequestLinks != nil {
					continue
				}
				if hasStopLabel(issue.Labels, triggerLabel) {
					klog.Infof("Skipping auto labeling/assigning issue #%d because it has the stop label ('overseer/stop' or '%s/stop')", issue.GetNumber(), triggerLabel)
					continue
				}

				hasTriggerLabel := false
				for _, l := range issue.Labels {
					if strings.EqualFold(l.GetName(), triggerLabel) {
						hasTriggerLabel = true
						break
					}
				}

				hasAssignee := false
				for _, u := range issue.Assignees {
					for _, bot := range allBotUsers {
						if strings.EqualFold(u.GetLogin(), bot) {
							hasAssignee = true
							break
						}
					}
					if hasAssignee {
						break
					}
				}

				if !hasTriggerLabel || !hasAssignee {
					if dryRun {
						fmt.Printf("[DRYRUN] Would label issue #%d created by %s with '%s' and assign to %s\n", issue.GetNumber(), githubLogin, triggerLabel, targetAssignee)
					} else {
						fmt.Printf("Labelling issue #%d created by %s with '%s' and assigning to %s...\n", issue.GetNumber(), githubLogin, triggerLabel, targetAssignee)
						if !hasTriggerLabel {
							if _, _, err := ghClient.Issues.AddLabelsToIssue(ctx, owner, repo, issue.GetNumber(), []string{triggerLabel}); err != nil {
								klog.Errorf("Failed to add label '%s' to issue #%d: %v", triggerLabel, issue.GetNumber(), err)
							} else {
								issue.Labels = append(issue.Labels, &githubv39.Label{Name: githubv39.String(triggerLabel)})
							}
						}
						if !hasAssignee && targetAssignee != "" {
							if _, _, err := ghClient.Issues.AddAssignees(ctx, owner, repo, issue.GetNumber(), []string{targetAssignee}); err != nil {
								klog.Errorf("Failed to assign %s to issue #%d: %v", targetAssignee, issue.GetNumber(), err)
							} else {
								issue.Assignees = append(issue.Assignees, &githubv39.User{Login: githubv39.String(targetAssignee)})
							}
						}
					}
				}
				allItems = append(allItems, issue)
			}
		}
	}

	uniqueIssues := make(map[int]*githubv39.Issue)
	for _, item := range allItems {
		uniqueIssues[item.GetNumber()] = item
	}

	var issues []*githubv39.Issue
	var fastPRIssues []*githubv39.Issue
	for _, item := range uniqueIssues {
		if item.PullRequestLinks == nil {
			issues = append(issues, item)
		} else {
			fastPRIssues = append(fastPRIssues, item)
		}
	}

	if issueMode != "disabled" {
		queueIssueTasks(ctx, ghClient, kubeClient, cfg, namespace, owner, repo, issues, processedIssues, refIssues, targetAssignee, allBotUsers, incomingDir, processingDir, processedDir, queueDir, dryRun, triggerLabel)
	}

	return fastPRIssues, nil
}

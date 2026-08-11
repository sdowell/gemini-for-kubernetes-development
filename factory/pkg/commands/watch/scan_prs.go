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
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	factorysandbox "github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/sandbox"
	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

func processPRs(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, rootOpts RootOptions, owner, repo string, prIssues []*githubv39.Issue, allBotUsers []string, targetAssignee, githubLogin, triggerLabel string, prInactivityTimeout time.Duration, incomingDir, processingDir, processedDir, queueDir string, dryRun bool, processedPRs map[int]prWatchState) {
	for _, prIssue := range prIssues {
		num := prIssue.GetNumber()
		if cfg != nil && cfg.MinNumber > 0 && num < cfg.MinNumber {
			continue
		}
		if hasStopLabel(prIssue.Labels, triggerLabel) {
			klog.Infof("Skipping PR #%d because it has the stop label ('overseer/stop' or '%s/stop')", num, triggerLabel)
			removePendingTasksForNumber(incomingDir, num)
			continue
		}
		if ghClient == nil {
			continue
		}
		pr, _, err := ghClient.PullRequests.Get(ctx, owner, repo, num)
		if err != nil {
			klog.Errorf("Failed to fetch full PR #%d: %v", num, err)
			continue
		}

		// Verify PR Author: Only process PRs created by any bot in the pool
		author := pr.GetUser().GetLogin()
		isBotPR := false
		for _, bot := range allBotUsers {
			if strings.EqualFold(author, bot) {
				isBotPR = true
				break
			}
		}
		if !isBotPR {
			klog.Infof("Skipping PR #%d because it was created by %s (not in our bot pool). We do not have permission to push to external forks.", num, author)
			continue
		}

		// Sync labels from referenced parent issues to the PR
		syncReferencedIssueLabels(ctx, ghClient, owner, repo, pr, prIssue)
		if hasStopLabel(prIssue.Labels, triggerLabel) {
			klog.Infof("Skipping PR #%d after label sync because it has the stop label ('overseer/stop' or '%s/stop')", num, triggerLabel)
			removePendingTasksForNumber(incomingDir, num)
			continue
		}

		headSHA := pr.GetHead().GetSHA()

		// Fetch PR commits to find the last commit timestamp
		prCommits, err := github.ListAllCommits(ctx, ghClient, owner, repo, num)
		var lastCommitTime time.Time
		if err == nil {
			for _, c := range prCommits {
				if c.GetCommit().GetCommitter().GetDate().After(lastCommitTime) {
					lastCommitTime = c.GetCommit().GetCommitter().GetDate()
				}
			}
		}

		// Fetch all PR comments (handling pagination)
		comments, listCommentsErr := github.ListAllIssueComments(ctx, ghClient, owner, repo, num)

		var reviews []*githubv39.PullRequestReview
		var listReviewsErr error
		if listCommentsErr == nil {
			reviews, listReviewsErr = github.ListAllReviews(ctx, ghClient, owner, repo, num)
		}

		revCommentsMap := make(map[int64][]*githubv39.PullRequestComment)
		if listCommentsErr == nil && listReviewsErr == nil {
			for _, r := range reviews {
				if rc, err := github.ListAllReviewComments(ctx, ghClient, owner, repo, num, r.GetID()); err == nil {
					revCommentsMap[r.GetID()] = rc
				}
			}
		}

		state := processedPRs[num]

		// PR Inactivity check
		if prInactivityTimeout > 0 && listCommentsErr == nil && listReviewsErr == nil {
			var bots []string
			if cfg != nil {
				bots = cfg.AllowlistedBots
			}
			lastActivity := getLastPRActivityTime(pr, comments, reviews, revCommentsMap, githubLogin, bots)
			if time.Since(lastActivity) > prInactivityTimeout {
				stopLabel := getStopLabel(triggerLabel)
				if dryRun {
					fmt.Printf("[DRYRUN] Would pause automated processing on PR #%d and apply label '%s' due to inactivity since %v\n", num, stopLabel, lastActivity)
				} else {
					klog.Infof("Pausing automated processing on PR #%d and applying label '%s' due to inactivity since %v", num, stopLabel, lastActivity)
					addGitHubComment(ctx, ghClient, owner, repo, num, fmt.Sprintf("🤖 AI Factory has paused automated processing on this pull request due to a period of inactivity with no human comments (inactive for %s). I have applied the `%s` label.\n\nTo resume automated processing, please remove the `%s` label from this pull request and add a new comment/review.", prInactivityTimeout, stopLabel, stopLabel))
					if _, _, err := ghClient.Issues.AddLabelsToIssue(ctx, owner, repo, num, []string{stopLabel}); err != nil {
						klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
					}
					removePendingTasksForNumber(incomingDir, num)
				}
				continue
			}
		}

		// Check Phase 1: Rebase/Conflicts
		isConflicting := pr.Mergeable != nil && !*pr.Mergeable

		if isConflicting {
			if state.lastIteratedSHA != "" && state.lastIteratedSHA == headSHA {
				klog.Infof("Skipping PR #%d rebase/conflict resolution because an iterate task was already processed for head SHA %s.", num, headSHA)
				continue
			}

			filename := fmt.Sprintf("task-pr-%d-iterate.yaml", num)
			if !taskExists(incomingDir, processingDir, filename) {
				sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, "pr-iterate", num, owner, repo)
				running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
				if err != nil {
					klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
					continue
				} else if running {
					klog.Infof("Skipping PR #%d rebase because there is an in-flight sandbox %s.", num, sandboxName)
				} else {
					assignedBot := assignedBotUser(prIssue, allBotUsers)

					taskAssignee := assignedBot
					if taskAssignee == "" {
						taskAssignee = author
					}

					prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, num)
					task := &QueueTask{
						Type:       "pr-iterate",
						URL:        prURL,
						Number:     num,
						Priority:   getPRPriority(prIssue),
						Phase:      1,
						CreatedAt:  pr.GetCreatedAt(),
						EnqueuedAt: time.Now(),
						Assignee:   taskAssignee,
						Status:     "Pending",
						CommitSHA:  headSHA,
					}

					if dryRun {
						fmt.Printf("[DRYRUN] Would queue rebase task for PR #%d: %s\n", num, prURL)
					} else {
						fmt.Printf("Queueing rebase task for PR #%d...\n", num)
						state.lastIteratedSHA = headSHA
						state.lastIteratedTime = time.Now()
						processedPRs[num] = state
						if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
							klog.Errorf("Failed to queue rebase task for PR #%d: %v", num, err)
						} else {
							writeTaskJournalEvent(queueDir, filename, task, "Created", 0)
						}
					}
				}
			}
			// If conflicting, we prioritize rebase and skip other PR checks for this PR in this loop
			continue
		}

		// Check CI Check Failures
		hasFailure := false
		checkRuns, err := listAllCheckRuns(ctx, ghClient, owner, repo, headSHA)
		if err == nil {
			for _, run := range checkRuns {
				c := run.GetConclusion()
				if c == "failure" || c == "timed_out" || c == "cancelled" {
					hasFailure = true
					break
				}
			}
		}

		statuses, _, err := ghClient.Repositories.ListStatuses(ctx, owner, repo, headSHA, nil)
		if err == nil {
			for _, status := range statuses {
				if status.GetState() == "failure" || status.GetState() == "error" {
					hasFailure = true
					break
				}
			}
		}

		state = processedPRs[num]

		assignedBot := assignedBotUser(prIssue, allBotUsers)
		isExplicitlyAssigned := assignedBot != "" && state.unassignedSHA != headSHA

		if state.lastSHA != "" && state.lastSHA != headSHA {
			if shouldUnassignStaleBot(state.lastSHA, state.unassignedSHA, headSHA, assignedBot) {
				if dryRun {
					fmt.Printf("[DRYRUN] Would unassign stale bot %s from PR #%d due to new commit %s\n", assignedBot, num, headSHA)
				} else {
					fmt.Printf("Unassigning stale bot %s from PR #%d due to new commit %s...\n", assignedBot, num, headSHA)
					if _, _, err := ghClient.Issues.RemoveAssignees(ctx, owner, repo, num, []string{assignedBot}); err != nil {
						klog.Errorf("Failed to unassign stale bot %s from PR #%d: %v", assignedBot, num, err)
					}
					state.unassignedSHA = headSHA
					processedPRs[num] = state
					isExplicitlyAssigned = false
					assignedBot = ""
				}
			}
			// Remove the giving up label if present
			hasGivingUpLabel := false
			for _, l := range prIssue.Labels {
				if l.GetName() == "overseer/giving-up" {
					hasGivingUpLabel = true
					break
				}
			}
			if hasGivingUpLabel {
				if dryRun {
					fmt.Printf("[DRYRUN] Would remove giving up label from PR #%d due to new commit %s\n", num, headSHA)
				} else {
					fmt.Printf("Removing giving up label from PR #%d due to new commit %s...\n", num, headSHA)
					if _, err := ghClient.Issues.RemoveLabelForIssue(ctx, owner, repo, num, "overseer/giving-up"); err != nil {
						klog.Errorf("Failed to remove giving up label from PR #%d: %v", num, err)
					}
				}
			}
		}

		if hasFailure {
			filename := fmt.Sprintf("task-pr-%d-investigate.yaml", num)
			if !taskExists(incomingDir, processingDir, filename) {
				investigationCount := 0
				if listCommentsErr == nil {
					var bots []string
					if cfg != nil {
						bots = cfg.AllowlistedBots
					}
					investigationCount = getInvestigationCount(comments, lastCommitTime, allBotUsers, githubLogin, bots)
				}

				if investigationCount >= 3 {
					stopLabel := getStopLabel(triggerLabel)
					if !dryRun {
						addGitHubComment(ctx, ghClient, owner, repo, num, fmt.Sprintf("🤖 AI Factory has attempted to investigate/fix CI check failures for this pull request 3 times since the last commit or update without success. To prevent infinite loops, I am pausing automated investigation and attaching the `%s` label.\n\nTo request another attempt or resume automated processing, please remove the `%s` label from this pull request (and/or push a new commit or leave a comment).", stopLabel, stopLabel))
						if _, _, err := ghClient.Issues.AddLabelsToIssue(ctx, owner, repo, num, []string{stopLabel}); err != nil {
							klog.Errorf("Failed to add stop label '%s' to PR #%d: %v", stopLabel, num, err)
						}
					}
					klog.Infof("Skipping PR #%d investigate because it has reached the maximum retry limit (3 attempts since last update) and applying stop label '%s'.", num, stopLabel)
				} else {
					prevFailed := false
					processedPath := filepath.Join(processedDir, filename)
					if data, err := os.ReadFile(processedPath); err == nil {
						var t QueueTask
						if err := yaml.Unmarshal(data, &t); err == nil {
							if t.Status == "Failed" {
								prevFailed = true
							}
						}
					}

					if state.lastSHA != headSHA || prevFailed || isExplicitlyAssigned || time.Since(state.lastInvestigatedTime) > 2*time.Hour {
						sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, "pr-investigate", num, owner, repo)
						running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
						if err != nil {
							klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
						} else if running {
							klog.Infof("Skipping PR #%d investigate because there is an in-flight sandbox %s.", num, sandboxName)
						} else {
							assignedBot := assignedBotUser(prIssue, allBotUsers)

							taskAssignee := assignedBot
							if taskAssignee == "" {
								taskAssignee = author
							}

							prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, num)
							task := &QueueTask{
								Type:       "pr-investigate",
								URL:        prURL,
								Number:     num,
								Priority:   getPRPriority(prIssue),
								Phase:      3,
								CreatedAt:  pr.GetCreatedAt(),
								EnqueuedAt: time.Now(),
								Assignee:   taskAssignee,
								Status:     "Pending",
								CommitSHA:  headSHA,
							}

							if dryRun {
								fmt.Printf("[DRYRUN] Would queue investigate task for PR #%d: %s\n", num, prURL)
							} else {
								fmt.Printf("Queueing investigate task for PR #%d...\n", num)
								state.lastSHA = headSHA
								state.lastInvestigatedTime = time.Now()
								processedPRs[num] = state
								if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
									klog.Errorf("Failed to queue investigate task for PR #%d: %v", num, err)
								} else {
									writeTaskJournalEvent(queueDir, filename, task, "Created", 0)
								}
							}
						}
					}
				}
			}
		} else if state.lastSHA != headSHA {
			state.lastSHA = headSHA
			processedPRs[num] = state
		}

		// Check review comments and approvals
		isApproved := isPRApprovedOrLGTM(pr, prIssue, reviews)

		if listCommentsErr == nil {
			hasNewComments := false

			var bots []string
			if cfg != nil {
				bots = cfg.AllowlistedBots
			}

			// Find the latest timestamp of any reply made by an allowlisted bot user (excluding reviewer bots)
			var latestBotReplyTime time.Time
			for _, c := range comments {
				if !isReviewerBot(c.GetUser(), cfg) && isBotReply(c.GetUser(), githubLogin, bots) && c.GetCreatedAt().After(latestBotReplyTime) {
					latestBotReplyTime = c.GetCreatedAt()
				}
			}
			for _, r := range reviews {
				if !isReviewerBot(r.GetUser(), cfg) && isBotReply(r.GetUser(), githubLogin, bots) && r.GetSubmittedAt().After(latestBotReplyTime) {
					latestBotReplyTime = r.GetSubmittedAt()
				}
			}

			var unackCommentIDs []int64
			var unackPRCommentIDs []int64
			hasNewHumanComments := false
			hasNewBotReviews := false
			for _, c := range comments {
				isReviewer := isReviewerBot(c.GetUser(), cfg)
				if !isReviewer && shouldIgnoreUser(c.GetUser(), githubLogin, bots) {
					continue
				}
				if strings.EqualFold(c.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
					continue
				}
				if c.GetCreatedAt().After(lastCommitTime) && c.GetCreatedAt().After(state.lastCommentAddressedTime) && c.GetCreatedAt().After(latestBotReplyTime) {
					if hasIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), "+1", true, bots, githubLogin) {
						continue
					}
					humanRocket := hasIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), "rocket", false, bots, githubLogin)
					if !humanRocket && hasIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), "eyes", true, bots, githubLogin) {
						continue
					}
					if !humanRocket && hasIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), "confused", true, bots, githubLogin) {
						continue
					}
					if isReviewer {
						hasNewBotReviews = true
					} else {
						hasNewHumanComments = true
					}
					unackCommentIDs = append(unackCommentIDs, c.GetID())
				}
			}

			// Also check inline PR review comments directly
			for _, r := range reviews {
				isReviewer := isReviewerBot(r.GetUser(), cfg)
				if !isReviewer && shouldIgnoreUser(r.GetUser(), githubLogin, bots) {
					if r.GetSubmittedAt().After(latestBotReplyTime) {
						latestBotReplyTime = r.GetSubmittedAt()
					}
					continue
				}
				if strings.EqualFold(r.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
					continue
				}
				if r.GetSubmittedAt().After(lastCommitTime) && r.GetSubmittedAt().After(state.lastCommentAddressedTime) && r.GetSubmittedAt().After(latestBotReplyTime) {
					if isReviewer {
						hasNewBotReviews = true
					} else {
						hasNewHumanComments = true
					}
				}

				revComments := revCommentsMap[r.GetID()]
				for _, rc := range revComments {
					isInlineReviewer := isReviewerBot(rc.GetUser(), cfg)
					if !isInlineReviewer && shouldIgnoreUser(rc.GetUser(), githubLogin, bots) {
						if rc.GetCreatedAt().After(latestBotReplyTime) {
							latestBotReplyTime = rc.GetCreatedAt()
						}
						continue
					}
					if strings.EqualFold(rc.GetUser().GetLogin(), pr.GetUser().GetLogin()) {
						continue
					}
					if rc.GetCreatedAt().After(lastCommitTime) && rc.GetCreatedAt().After(state.lastCommentAddressedTime) && rc.GetCreatedAt().After(latestBotReplyTime) {
						if isInlineReviewer {
							hasNewBotReviews = true
						} else {
							hasNewHumanComments = true
						}
						unackPRCommentIDs = append(unackPRCommentIDs, rc.GetID())
					}
				}
			}

			if hasNewHumanComments {
				hasNewComments = true
			} else if hasNewBotReviews {
				if state.lastCommentAddressedSHA != "" && state.lastCommentAddressedSHA == headSHA {
					klog.Infof("Skipping bot review feedback on PR #%d because an address-comments task already ran against SHA %s without resulting in a commit.", num, headSHA)
				} else {
					hasNewComments = true
				}
			}

			if isApproved {
				klog.V(2).Infof("PR #%d is approved / LGTM'd", num)
			}

			if hasNewComments {
				if os.Getenv("DRY_RUN") == "true" {
					continue
				}
				filename := fmt.Sprintf("task-pr-%d-comments.yaml", num)
				if !taskExists(incomingDir, processingDir, filename) {
					sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, "pr-comments", num, owner, repo)
					running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
					if err != nil {
						klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
						continue
					} else if running {
						klog.Infof("Skipping PR #%d address-comments because there is an in-flight sandbox %s.", num, sandboxName)
					} else {
						taskAssignee := assignedBot
						if taskAssignee == "" {
							taskAssignee = author
						}

						prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, num)
						task := &QueueTask{
							Type:       "pr-comments",
							URL:        prURL,
							Number:     num,
							Priority:   getPRPriority(prIssue),
							Phase:      2,
							CreatedAt:  pr.GetCreatedAt(),
							EnqueuedAt: time.Now(),
							Assignee:   taskAssignee,
							Status:     "Pending",
							CommitSHA:  headSHA,
						}

						if dryRun {
							fmt.Printf("[DRYRUN] Would queue address-comments task for PR #%d: %s\n", num, prURL)
						} else {
							fmt.Printf("Queueing address-comments task for PR #%d...\n", num)
							for _, cid := range unackCommentIDs {
								addIssueCommentReaction(ctx, ghClient, owner, repo, cid, "eyes")
							}
							for _, cid := range unackPRCommentIDs {
								addPullRequestCommentReaction(ctx, ghClient, owner, repo, cid, "eyes")
							}
							state.lastCommentAddressedTime = time.Now()
							state.lastCommentAddressedSHA = headSHA
							processedPRs[num] = state
							if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
								klog.Errorf("Failed to queue address-comments task for PR #%d: %v", num, err)
							} else {
								writeTaskJournalEvent(queueDir, filename, task, "Created", 0)
							}
						}
					}
				}
			} else if !hasFailure && !isApproved && state.lastReviewedSHA != headSHA && shouldAutoReviewPR(ctx, ghClient, owner, repo, pr, prIssue, triggerLabel) {
				hasBotReviewAfterLastCommit := false
				for _, r := range reviews {
					if isBotReply(r.GetUser(), githubLogin, bots) && (r.GetSubmittedAt().After(lastCommitTime) || r.GetCommitID() == headSHA) {
						hasBotReviewAfterLastCommit = true
						break
					}
				}

				if !hasBotReviewAfterLastCommit {
					if os.Getenv("DRY_RUN") == "true" {
						continue
					}
					filename := fmt.Sprintf("task-pr-%d-review.yaml", num)
					if !taskExists(incomingDir, processingDir, filename) {
						sandboxName := resolveSandboxName(ctx, kubeClient, ghClient, rootOpts.Namespace, "pr-review", num, owner, repo)
						running, err := isSandboxTaskRunning(ctx, kubeClient, rootOpts.Namespace, sandboxName)
						if err != nil {
							klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
							continue
						} else if running {
							klog.Infof("Skipping PR #%d review because there is an in-flight sandbox %s.", num, sandboxName)
						} else {
							var bodies []string
							if pr.GetBody() != "" {
								bodies = append(bodies, pr.GetBody())
							}
							for refIssueNum := range getReferencedIssues(pr) {
								refIssue, _, err := ghClient.Issues.Get(ctx, owner, repo, refIssueNum)
								if err == nil && refIssue.GetBody() != "" {
									bodies = append(bodies, refIssue.GetBody())
								}
							}
							instructions := ExtractReviewInstructions(bodies...)

							prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, num)
							task := &QueueTask{
								Type:         "pr-review",
								URL:          prURL,
								Number:       num,
								Priority:     getPRPriority(prIssue),
								Phase:        2,
								CreatedAt:    pr.GetCreatedAt(),
								EnqueuedAt:   time.Now(),
								Status:       "Pending",
								CommitSHA:    headSHA,
								Instructions: instructions,
							}

							if dryRun {
								fmt.Printf("[DRYRUN] Would queue review task for PR #%d: %s\n", num, prURL)
							} else {
								fmt.Printf("Queueing review task for PR #%d (Instructions: %d)...\n", num, len(instructions))
								state.lastReviewedSHA = headSHA
								processedPRs[num] = state
								if err := writeTaskAtomically(incomingDir, filename, task); err != nil {
									klog.Errorf("Failed to queue review task for PR #%d: %v", num, err)
								} else {
									writeTaskJournalEvent(queueDir, filename, task, "Created", 0)
								}
							}
						}
					}
				}
			}
		}
	}
}

func scanPRsSlow(ctx context.Context, ghClient *githubv39.Client, kubeClient *clients.KubernetesClient, cfg *config.FactoryConfig, rootOpts RootOptions, owner, repo string, targetAssignee string, allBotUsers []string, githubLogin, triggerLabel, issueMode, choresMode, sandboxEvictionAge string, sandboxIdleTimeout, prInactivityTimeout time.Duration, incomingDir, processingDir, processedDir, queueDir string, dryRun bool, processedIssues map[int]time.Time, processedPRs map[int]prWatchState, refIssues map[int]bool) {
	if ghClient == nil {
		return
	}
	klog.Infof("Running slow PR scan cycle...")
	prs, err := listAllOpenPRs(ctx, ghClient, owner, repo)
	if err != nil {
		klog.Errorf("Failed to list open PRs: %v", err)
	} else {
		for _, pr := range prs {
			for num := range getReferencedIssues(pr) {
				refIssues[num] = true
			}
		}
	}

	// Scan issues labeled with triggerLabel (handling pagination)
	var slowIssues []*githubv39.Issue
	opts2 := &githubv39.IssueListByRepoOptions{
		Labels:      []string{triggerLabel},
		State:       "open",
		ListOptions: githubv39.ListOptions{PerPage: 100},
	}
	for {
		pageIssues, resp, err := ghClient.Issues.ListByRepo(ctx, owner, repo, opts2)
		if err != nil {
			klog.Errorf("Failed to list issues for label %s: %v", triggerLabel, err)
			break
		}
		for _, item := range pageIssues {
			if item.PullRequestLinks == nil {
				slowIssues = append(slowIssues, item)
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts2.Page = resp.NextPage
	}

	// Process slow issues
	if issueMode != "disabled" {
		queueIssueTasks(ctx, ghClient, kubeClient, cfg, rootOpts.Namespace, owner, repo, slowIssues, processedIssues, refIssues, targetAssignee, allBotUsers, incomingDir, processingDir, processedDir, queueDir, dryRun, triggerLabel)
	}

	// Process Pull Requests (Scanner)
	var prIssues []*githubv39.Issue
	var allPRIssues []*githubv39.Issue
	for _, botUser := range allBotUsers {
		opts1 := &githubv39.IssueListByRepoOptions{
			Assignee:    botUser,
			State:       "open",
			ListOptions: githubv39.ListOptions{PerPage: 100},
		}
		for {
			iss1, resp, err := ghClient.Issues.ListByRepo(ctx, owner, repo, opts1)
			if err != nil {
				klog.Errorf("Failed to list PR issues for assignee %s: %v", botUser, err)
				break
			}
			for _, item := range iss1 {
				if item.PullRequestLinks != nil {
					allPRIssues = append(allPRIssues, item)
				}
			}
			if resp.NextPage == 0 {
				break
			}
			opts1.Page = resp.NextPage
		}
	}
	opts2PR := &githubv39.IssueListByRepoOptions{
		Labels:      []string{triggerLabel},
		State:       "open",
		ListOptions: githubv39.ListOptions{PerPage: 100},
	}
	for {
		iss2, resp, err := ghClient.Issues.ListByRepo(ctx, owner, repo, opts2PR)
		if err != nil {
			klog.Errorf("Failed to list PR issues for label %s: %v", triggerLabel, err)
			break
		}
		for _, item := range iss2 {
			if item.PullRequestLinks != nil {
				allPRIssues = append(allPRIssues, item)
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts2PR.Page = resp.NextPage
	}

	// Deduplicate allPRIssues
	uniquePRIssues := make(map[int]*githubv39.Issue)
	for _, item := range allPRIssues {
		uniquePRIssues[item.GetNumber()] = item
	}
	for _, item := range uniquePRIssues {
		prIssues = append(prIssues, item)
	}

	processPRs(ctx, ghClient, kubeClient, cfg, rootOpts, owner, repo, prIssues, allBotUsers, targetAssignee, githubLogin, triggerLabel, prInactivityTimeout, incomingDir, processingDir, processedDir, queueDir, dryRun, processedPRs)

	// Scan chores
	if choresMode != "disabled" {
		scanChores(ctx, ghClient, owner, repo, incomingDir, processingDir, queueDir, dryRun)
	}

	openPRMap := make(map[int]bool)
	for _, pr := range prIssues {
		openPRMap[pr.GetNumber()] = true
	}

	openIssueMap := make(map[int]bool)
	for _, iss := range slowIssues {
		openIssueMap[iss.GetNumber()] = true
	}
	for issNum := range processedIssues {
		openIssueMap[issNum] = true
	}

	// Clean up sandboxes of merged or closed PRs
	if err := cleanupClosedPRSandboxes(ctx, ghClient, kubeClient, owner, repo, rootOpts.Namespace, openPRMap, dryRun); err != nil {
		klog.Errorf("Failed to clean up closed PR sandboxes: %v", err)
	}

	// Clean up sandboxes of closed issues
	if err := cleanupClosedIssueSandboxes(ctx, ghClient, kubeClient, owner, repo, rootOpts.Namespace, openIssueMap, dryRun); err != nil {
		klog.Errorf("Failed to clean up closed issue sandboxes: %v", err)
	}

	// Clean up stale idle sandboxes older than eviction age (defaults to 1 week)
	if err := cleanupStaleIdleSandboxes(ctx, kubeClient, owner+"/"+repo, rootOpts.Namespace, sandboxEvictionAge, dryRun); err != nil {
		klog.Errorf("Failed to clean up stale idle sandboxes: %v", err)
	}

	if sandboxIdleTimeout > 0 {
		if _, err := factorysandbox.SuspendIdleSandboxes(ctx, kubeClient, rootOpts.Namespace, sandboxIdleTimeout, dryRun); err != nil {
			klog.Errorf("Failed to suspend idle sandboxes: %v", err)
		}
	}
}

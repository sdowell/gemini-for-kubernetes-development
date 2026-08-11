package watch

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	githubv39 "github.com/google/go-github/v39/github"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

func assignedBotUser(issue *githubv39.Issue, botUsers []string) string {
	for _, u := range issue.Assignees {
		for _, bot := range botUsers {
			if strings.EqualFold(u.GetLogin(), bot) {
				return u.GetLogin()
			}
		}
	}
	return ""
}

func referencedIssueList(pr *githubv39.PullRequest) []int {
	if pr == nil {
		return nil
	}
	var out []int
	for n := range getReferencedIssues(pr) {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

func listAllCheckRuns(ctx context.Context, client *githubv39.Client, owner, repo, ref string) ([]*githubv39.CheckRun, error) {
	var allRuns []*githubv39.CheckRun
	opts := &githubv39.ListCheckRunsOptions{
		ListOptions: githubv39.ListOptions{
			PerPage: 200,
		},
	}
	for {
		runs, resp, err := client.Checks.ListCheckRunsForRef(ctx, owner, repo, ref, opts)
		if err != nil {
			return nil, err
		}
		allRuns = append(allRuns, runs.CheckRuns...)
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	latestRuns := make(map[string]*githubv39.CheckRun)
	for _, run := range allRuns {
		name := run.GetName()
		if existing, ok := latestRuns[name]; ok {
			if run.GetID() > existing.GetID() {
				latestRuns[name] = run
			}
		} else {
			latestRuns[name] = run
		}
	}

	deduped := make([]*githubv39.CheckRun, 0, len(latestRuns))
	for _, run := range latestRuns {
		deduped = append(deduped, run)
	}
	return deduped, nil
}

// GetReferencedIssues inspects PR head branch, title, and body for linked issue numbers.
func GetReferencedIssues(pr *githubv39.PullRequest) map[int]bool {
	referenced := make(map[int]bool)

	// Check branch name, ignoring epoch timestamps (num >= 10000000)
	if pr.GetHead().GetRef() != "" {
		re := regexp.MustCompile(`\d+`)
		for _, match := range re.FindAllString(pr.GetHead().GetRef(), -1) {
			if num, err := strconv.Atoi(match); err == nil && num < 10000000 {
				referenced[num] = true
			}
		}
	}

	// Check title and body for #1234 or "Fixes/Closes/Resolves/Issue 1234"
	re := regexp.MustCompile(`(?:#|(?i:\b(?:fixes|closes|resolves|issue)\s+))(\d+)\b`)
	for _, text := range []string{pr.GetTitle(), pr.GetBody()} {
		for _, match := range re.FindAllStringSubmatch(text, -1) {
			if len(match) > 1 {
				if num, err := strconv.Atoi(match[1]); err == nil && num < 10000000 {
					referenced[num] = true
				}
			}
		}
	}

	return referenced
}

func getReferencedIssues(pr *githubv39.PullRequest) map[int]bool {
	return GetReferencedIssues(pr)
}

func listAllOpenPRs(ctx context.Context, ghClient *githubv39.Client, owner, repo string) ([]*githubv39.PullRequest, error) {
	var allPRs []*githubv39.PullRequest
	opts := &githubv39.PullRequestListOptions{
		State:       "open",
		ListOptions: githubv39.ListOptions{PerPage: 100},
	}
	for {
		prs, resp, err := ghClient.PullRequests.List(ctx, owner, repo, opts)
		if err != nil {
			return nil, err
		}
		allPRs = append(allPRs, prs...)
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return allPRs, nil
}

func syncReferencedIssueLabels(ctx context.Context, ghClient *githubv39.Client, owner, repo string, pr *githubv39.PullRequest, prIssue *githubv39.Issue) {
	var refIssues []*githubv39.Issue
	for refIssueNum := range getReferencedIssues(pr) {
		refIssue, _, err := ghClient.Issues.Get(ctx, owner, repo, refIssueNum)
		if err != nil {
			klog.Warningf("Failed to fetch referenced parent issue #%d for PR #%d: %v", refIssueNum, pr.GetNumber(), err)
			continue
		}
		refIssues = append(refIssues, refIssue)
	}

	allMissingLabels := getMissingLabelsForPR(prIssue.Labels, refIssues)

	if len(allMissingLabels) > 0 {
		klog.Infof("Adding inherited labels %v to PR #%d", allMissingLabels, pr.GetNumber())
		if _, _, err := ghClient.Issues.AddLabelsToIssue(ctx, owner, repo, pr.GetNumber(), allMissingLabels); err != nil {
			klog.Errorf("Failed to add labels %v to PR #%d: %v", allMissingLabels, pr.GetNumber(), err)
		}
	}
}

func getMissingLabelsForPR(prLabels []*githubv39.Label, refIssues []*githubv39.Issue) []string {
	prLabelsSet := make(map[string]bool)
	for _, label := range prLabels {
		if label.GetName() != "" {
			prLabelsSet[label.GetName()] = true
		}
	}

	var allMissingLabels []string
	missingLabelsSet := make(map[string]bool)

	for _, refIssue := range refIssues {
		if refIssue == nil {
			continue
		}

		for _, label := range refIssue.Labels {
			labelName := label.GetName()
			if labelName != "" && !prLabelsSet[labelName] && !missingLabelsSet[labelName] {
				missingLabelsSet[labelName] = true
				allMissingLabels = append(allMissingLabels, labelName)
			}
		}
	}

	return allMissingLabels
}

func hasLinkedPR(ctx context.Context, client *githubv39.Client, owner, repo string, issueNum int) (bool, error) {
	// 1. Try timeline check (quick and standard)
	timeline, _, err := client.Issues.ListIssueTimeline(ctx, owner, repo, issueNum, nil)
	if err == nil {
		for _, event := range timeline {
			if event.GetEvent() == "cross-referenced" && event.Source != nil {
				if event.Source.Issue != nil && event.Source.Issue.PullRequestLinks != nil {
					if event.Source.Issue.GetState() == "open" {
						return true, nil
					}
				}
			}
		}
	} else {
		klog.Warningf("Failed to list issue timeline for #%d: %v. Falling back to search API.", issueNum, err)
	}

	// 2. Fallback to Search API: search for open PRs referencing the issue number
	query := fmt.Sprintf("repo:%s/%s type:pr state:open \"%d\"", owner, repo, issueNum)
	opts := &githubv39.SearchOptions{
		ListOptions: githubv39.ListOptions{PerPage: 10},
	}
	result, _, err := client.Search.Issues(ctx, query, opts)
	if err != nil {
		return false, fmt.Errorf("failed to search PRs for issue #%d: %w", issueNum, err)
	}

	if result.GetTotal() > 0 {
		return true, nil
	}

	return false, nil
}

func isPRApprovedOrLGTM(pr *githubv39.PullRequest, prIssue *githubv39.Issue, reviews []*githubv39.PullRequestReview) bool {
	// 1. Check labels
	for _, label := range prIssue.Labels {
		if strings.EqualFold(label.GetName(), "lgtm") || strings.EqualFold(label.GetName(), "approved") {
			return true
		}
	}

	// 2. Check reviews
	hasApproved := false
	hasChangesRequested := false
	latestReviews := make(map[string]string)
	for _, r := range reviews {
		if r.GetUser() != nil && r.GetState() != "" {
			latestReviews[r.GetUser().GetLogin()] = r.GetState()
		}
	}
	for _, state := range latestReviews {
		if state == "APPROVED" {
			hasApproved = true
		} else if state == "CHANGES_REQUESTED" {
			hasChangesRequested = true
		}
	}

	return hasApproved && !hasChangesRequested
}

func isReviewerBot(user *githubv39.User, cfg *config.FactoryConfig) bool {
	if user == nil {
		return false
	}
	login := user.GetLogin()
	if cfg != nil {
		if reviewerRole, ok := cfg.Roles["reviewer"]; ok {
			for _, u := range reviewerRole.Users {
				if strings.EqualFold(login, u) {
					return true
				}
			}
		}
	}
	return strings.Contains(strings.ToLower(login), "reviewbot")
}

func isBotReply(user *githubv39.User, githubLogin string, allowlistedBots []string) bool {
	if user == nil {
		return false
	}
	login := user.GetLogin()
	if strings.EqualFold(login, githubLogin) {
		return true
	}
	for _, b := range allowlistedBots {
		if strings.EqualFold(login, b) {
			return true
		}
	}
	return shouldIgnoreUser(user, githubLogin, nil)
}

func shouldIgnoreUser(user *githubv39.User, githubLogin string, allowlistedBots []string) bool {
	if user == nil {
		return false
	}
	login := user.GetLogin()
	if strings.EqualFold(login, githubLogin) {
		return true // always ignore our own bot
	}

	loginLower := strings.ToLower(login)
	isBotUser := strings.EqualFold(user.GetType(), "Bot") ||
		strings.HasSuffix(loginLower, "[bot]") ||
		strings.HasSuffix(loginLower, "-bot") ||
		strings.HasSuffix(loginLower, "-robot") ||
		strings.Contains(loginLower, "prow")

	if isBotUser {
		// Check if it's in the allowlist
		for _, b := range allowlistedBots {
			if strings.EqualFold(login, b) {
				return false // DO NOT ignore (it is allowlisted)
			}
		}
		return true // ignore since it is not allowlisted
	}

	return false
}

func hasStopLabel(labels []*githubv39.Label, triggerLabel string) bool {
	stopLabels := []string{"overseer/stop"}
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		stopLabels = append(stopLabels, triggerLabel+"/stop")
	}
	for _, label := range labels {
		for _, stop := range stopLabels {
			if strings.EqualFold(label.GetName(), stop) {
				return true
			}
		}
	}
	return false
}

func shouldUnassignStaleBot(lastSHA, unassignedSHA, headSHA, assignedBot string) bool {
	if lastSHA == "" || lastSHA == headSHA {
		return false
	}
	if assignedBot == "" {
		return false
	}
	if unassignedSHA == headSHA {
		return false
	}
	return true
}

type prWatchState struct {
	lastSHA                  string
	lastInvestigatedTime     time.Time
	lastCommentAddressedTime time.Time
	lastCommentAddressedSHA  string
	lastReviewedSHA          string
	lastIteratedSHA          string
	lastIteratedTime         time.Time
	unassignedSHA            string
}

func parseProcessedPRTask(filePath string, name string, fInfo os.FileInfo, state prWatchState) prWatchState {
	isComments := strings.HasSuffix(name, "-comments")
	isInvestigate := strings.HasSuffix(name, "-investigate")
	isReview := strings.HasSuffix(name, "-review")
	isIterate := strings.HasSuffix(name, "-iterate")

	var t QueueTask
	hasTask := false
	if data, err := os.ReadFile(filePath); err == nil {
		if err := yaml.Unmarshal(data, &t); err == nil {
			hasTask = true
			if t.CommitSHA != "" {
				state.lastSHA = t.CommitSHA
			}
		}
	}

	if fInfo != nil {
		tTime := fInfo.ModTime()
		if hasTask && !t.CompletedAt.IsZero() {
			tTime = t.CompletedAt
		}
		if isComments {
			if tTime.After(state.lastCommentAddressedTime) {
				state.lastCommentAddressedTime = tTime
			}
			if hasTask && t.CommitSHA != "" {
				state.lastCommentAddressedSHA = t.CommitSHA
			}
		} else if isInvestigate {
			if tTime.After(state.lastInvestigatedTime) {
				state.lastInvestigatedTime = tTime
			}
		} else if isReview {
			if hasTask && t.CommitSHA != "" {
				state.lastReviewedSHA = t.CommitSHA
			}
		} else if isIterate {
			if tTime.After(state.lastIteratedTime) {
				state.lastIteratedTime = tTime
			}
			if hasTask && t.CommitSHA != "" {
				state.lastIteratedSHA = t.CommitSHA
			}
		}
	}
	return state
}

func getInvestigationCount(comments []*githubv39.IssueComment, lastCommitTime time.Time, allBotUsers []string, githubLogin string, bots []string) int {
	lastResetTime := lastCommitTime
	for _, c := range comments {
		isPoolBot := false
		for _, bot := range allBotUsers {
			if strings.EqualFold(c.GetUser().GetLogin(), bot) {
				isPoolBot = true
				break
			}
		}
		isHuman := !isPoolBot && !shouldIgnoreUser(c.GetUser(), githubLogin, bots)
		if (isHuman || strings.Contains(c.GetBody(), "pausing automated investigation")) && c.GetCreatedAt().After(lastResetTime) {
			lastResetTime = c.GetCreatedAt()
		}
	}

	investigationCount := 0
	for _, c := range comments {
		isPoolBot := false
		for _, bot := range allBotUsers {
			if strings.EqualFold(c.GetUser().GetLogin(), bot) {
				isPoolBot = true
				break
			}
		}
		if isPoolBot &&
			strings.Contains(c.GetBody(), "started investigating CI check failures") &&
			c.GetCreatedAt().After(lastResetTime) {
			investigationCount++
		}
	}
	return investigationCount
}

func getStopLabel(triggerLabel string) string {
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		return triggerLabel + "/stop"
	}
	return "overseer/stop"
}

func hasReviewLabel(labels []*githubv39.Label, triggerLabel string) bool {
	reviewLabels := []string{"overseer/review"}
	if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
		reviewLabels = append(reviewLabels, triggerLabel+"/review")
	}
	for _, label := range labels {
		for _, rev := range reviewLabels {
			if strings.EqualFold(label.GetName(), rev) {
				return true
			}
		}
	}
	return false
}

func shouldAutoReviewPR(ctx context.Context, ghClient *githubv39.Client, owner, repo string, pr *githubv39.PullRequest, prIssue *githubv39.Issue, triggerLabel string) bool {
	if hasReviewLabel(prIssue.Labels, triggerLabel) {
		return true
	}
	for refIssueNum := range getReferencedIssues(pr) {
		refIssue, _, err := ghClient.Issues.Get(ctx, owner, repo, refIssueNum)
		if err == nil && hasReviewLabel(refIssue.Labels, triggerLabel) {
			return true
		}
	}
	return false
}

func hasIssueCommentReaction(ctx context.Context, ghClient *githubv39.Client, owner, repo string, commentID int64, content string, filterBot bool, bots []string, selfLogin string) bool {
	if ghClient == nil {
		return false
	}
	reactions, _, err := ghClient.Reactions.ListIssueCommentReactions(ctx, owner, repo, commentID, nil)
	if err != nil {
		return false
	}
	for _, r := range reactions {
		if r.GetContent() == content {
			isBot := shouldIgnoreUser(r.GetUser(), selfLogin, bots)
			if filterBot && isBot {
				return true
			} else if !filterBot && !isBot {
				return true
			}
		}
	}
	return false
}

func resolvePRCommentReactions(ctx context.Context, ghClient *githubv39.Client, owner, repo string, prNum int, resolutionContent string, bots []string, selfLogin string) {
	if ghClient == nil {
		return
	}
	comments, _, err := ghClient.Issues.ListComments(ctx, owner, repo, prNum, nil)
	if err != nil {
		return
	}
	for _, c := range comments {
		if shouldIgnoreUser(c.GetUser(), selfLogin, bots) {
			continue
		}
		if hasIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), "eyes", true, bots, selfLogin) {
			addIssueCommentReaction(ctx, ghClient, owner, repo, c.GetID(), resolutionContent)
		}
	}
}

func addGitHubComment(ctx context.Context, client *githubv39.Client, owner, repo string, number int, body string) {
	comment := &githubv39.IssueComment{
		Body: githubv39.String(body),
	}
	_, _, err := client.Issues.CreateComment(ctx, owner, repo, number, comment)
	if err != nil {
		klog.Errorf("Failed to create GitHub comment on #%d: %v", number, err)
	}
}

func addIssueCommentReaction(ctx context.Context, ghClient *githubv39.Client, owner, repo string, commentID int64, content string) {
	if ghClient == nil {
		return
	}
	_, _, err := ghClient.Reactions.CreateIssueCommentReaction(ctx, owner, repo, commentID, content)
	if err != nil {
		klog.Warningf("Failed to create reaction '%s' on comment %d: %v", content, commentID, err)
	}
}

func addPullRequestCommentReaction(ctx context.Context, ghClient *githubv39.Client, owner, repo string, commentID int64, content string) {
	if ghClient == nil {
		return
	}
	_, _, err := ghClient.Reactions.CreatePullRequestCommentReaction(ctx, owner, repo, commentID, content)
	if err != nil {
		klog.Warningf("Failed to create reaction '%s' on PR review comment %d: %v", content, commentID, err)
	}
}

func getLastPRActivityTime(pr *githubv39.PullRequest, comments []*githubv39.IssueComment, reviews []*githubv39.PullRequestReview, revComments map[int64][]*githubv39.PullRequestComment, githubLogin string, bots []string) time.Time {
	lastActivity := pr.GetCreatedAt()

	// 1. Check issue comments
	for _, c := range comments {
		isBot := isBotReply(c.GetUser(), githubLogin, bots)
		if !isBot {
			if c.GetCreatedAt().After(lastActivity) {
				lastActivity = c.GetCreatedAt()
			}
		} else {
			if strings.Contains(c.GetBody(), "paused automated processing on this pull request due to a period of inactivity") {
				if c.GetCreatedAt().After(lastActivity) {
					lastActivity = c.GetCreatedAt()
				}
			}
		}
	}

	// 2. Check reviews and review comments
	for _, r := range reviews {
		if !isBotReply(r.GetUser(), githubLogin, bots) {
			if r.GetSubmittedAt().After(lastActivity) {
				lastActivity = r.GetSubmittedAt()
			}
		}

		if rcList, ok := revComments[r.GetID()]; ok {
			for _, rc := range rcList {
				if !isBotReply(rc.GetUser(), githubLogin, bots) {
					if rc.GetCreatedAt().After(lastActivity) {
						lastActivity = rc.GetCreatedAt()
					}
				}
			}
		}
	}

	return lastActivity
}

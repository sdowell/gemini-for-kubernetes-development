package common

import (
	"context"
	"strings"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	githubv39 "github.com/google/go-github/v39/github"
)

// IsReviewerBot checks whether a GitHub user belongs to the configured reviewer bot role
// or has a login containing "reviewbot".
func IsReviewerBot(user *githubv39.User, cfg *config.FactoryConfig) bool {
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

// ShouldIgnoreUser reports whether comments/reviews from the user should be ignored as non-allowlisted bot activity.
func ShouldIgnoreUser(user *githubv39.User, githubLogin string, allowlistedBots []string) bool {
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

// IsBotReply reports whether the user is the watcher bot, an allowlisted bot, or any other bot user.
func IsBotReply(user *githubv39.User, githubLogin string, allowlistedBots []string) bool {
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
	return ShouldIgnoreUser(user, githubLogin, nil)
}

// HasIgnorePrefix checks if any line of a comment body starts with the ignore prefix.
// The prefix is constructed as "/" + triggerLabel + "-ignore".
// If triggerLabel is empty or "overseer", we check for "/overseer-ignore".
// Otherwise, we accept either "/overseer-ignore" or "/" + triggerLabel + "-ignore".
func HasIgnorePrefix(body string, triggerLabel string) bool {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.ToLower(strings.TrimSpace(line))
		if strings.HasPrefix(trimmed, "/overseer-ignore") {
			return true
		}
		if triggerLabel != "" && !strings.EqualFold(triggerLabel, "overseer") {
			prefix := "/" + strings.ToLower(triggerLabel) + "-ignore"
			if strings.HasPrefix(trimmed, prefix) {
				return true
			}
		}
	}
	return false
}

// IsSystemOrInvestigateComment reports whether a comment body is an automated watcher status message
// (starting with "🤖 AI Factory ") or a CI failure investigation report (starting with "### Investigating ").
func IsSystemOrInvestigateComment(body string) bool {
	trimmed := strings.TrimSpace(body)
	return strings.HasPrefix(trimmed, "🤖 AI Factory ") || strings.HasPrefix(trimmed, "### Investigating ")
}

// HasIssueCommentReaction reports whether an issue comment has a reaction matching content from a bot (if filterBot is true)
// or from a human (if filterBot is false).
func HasIssueCommentReaction(ctx context.Context, ghClient *githubv39.Client, owner, repo string, commentID int64, content string, filterBot bool, bots []string, selfLogin string) bool {
	if ghClient == nil {
		return false
	}
	reactions, _, err := ghClient.Reactions.ListIssueCommentReactions(ctx, owner, repo, commentID, nil)
	if err != nil {
		return false
	}
	for _, r := range reactions {
		if r.GetContent() == content {
			isBot := IsBotReply(r.GetUser(), selfLogin, bots)
			if filterBot && isBot {
				return true
			} else if !filterBot && !isBot {
				return true
			}
		}
	}
	return false
}

// HasPullRequestCommentReaction reports whether a pull request review comment has a reaction matching content from a bot (if filterBot is true)
// or from a human (if filterBot is false).
func HasPullRequestCommentReaction(ctx context.Context, ghClient *githubv39.Client, owner, repo string, commentID int64, content string, filterBot bool, bots []string, selfLogin string) bool {
	if ghClient == nil {
		return false
	}
	reactions, _, err := ghClient.Reactions.ListPullRequestCommentReactions(ctx, owner, repo, commentID, nil)
	if err != nil {
		return false
	}
	for _, r := range reactions {
		if r.GetContent() == content {
			isBot := IsBotReply(r.GetUser(), selfLogin, bots)
			if filterBot && isBot {
				return true
			} else if !filterBot && !isBot {
				return true
			}
		}
	}
	return false
}

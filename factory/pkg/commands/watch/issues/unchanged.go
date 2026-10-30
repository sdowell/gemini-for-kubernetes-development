package issues

import (
	"slices"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/taskoutput"
)

// unchangedStands reports whether a fix's word that nothing should change
// stands on an issue, the rules' unchanged fact: a comment open-pr posted
// for a fix that made no commit (carrying taskoutput.UnchangedMarker, by a
// pool bot or the watcher), with no trusted human's comment after it -
// conventions.HumanSpokeAt, what a give-up's "since human" counts from.
func (s *Scanner) unchangedStands(comments []*githubv39.IssueComment) bool {
	var said time.Time
	for _, c := range comments {
		login := c.GetUser().GetLogin()
		bot := strings.EqualFold(login, s.cfg.GitHubLogin) ||
			slices.ContainsFunc(s.cfg.BotUsers, func(b string) bool { return strings.EqualFold(b, login) })
		if bot && taskoutput.IsUnchangedComment(c.GetBody()) && c.GetCreatedAt().After(said) {
			said = c.GetCreatedAt()
		}
	}
	if said.IsZero() {
		return false
	}
	answered := conventions.HumanSpokeAt(comments, s.cfg.BotUsers, s.cfg.GitHubLogin, s.cfg.AllowlistedBots, s.cfg.TrustedLogins, s.cfg.TriggerLabel)
	return !answered.After(said)
}

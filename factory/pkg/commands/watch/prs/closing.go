package prs

import (
	"regexp"
	"strconv"
	"strings"

	githubv39 "github.com/google/go-github/v39/github"
)

var (
	// closingRefRe matches a GitHub closing keyword followed by exactly one
	// issue reference: #N, owner/repo#N, or an issue URL. GitHub applies a
	// keyword to the reference right after it and to nothing further along,
	// which is why there is no scope for later references to fall into.
	closingRefRe = regexp.MustCompile(`(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+` +
		`(?:#(\d+)|https?://github\.com/([\w.-]+)/([\w.-]+)/issues/(\d+)|([\w.-]+)/([\w.-]+)#(\d+))\b`)

	// closingBranchRe matches the branch names the watcher pushes fixes to,
	// such as issue-123 or issue-123-1700000000 (see fix_issue.txt).
	closingBranchRe = regexp.MustCompile(`\b(?:issue|factory-issue)[-_](\d+)\b`)
)

// maxIssueNumber excludes the epoch timestamps that follow the issue number in
// the watcher's branch names, and anything else too large to be an issue.
const maxIssueNumber = 10000000

// strictClosingIssues returns the numbers of the issues in owner/repo that a
// pull request closes, judged the way GitHub judges it: a closing keyword
// immediately followed by a reference, in the body only. The watcher's own
// issue-N branch naming also counts, since that is how its fixes record which
// issue they are for.
//
// It is deliberately stricter than github.GetClosingIssues, which takes every
// reference within a sentence of a keyword, reads the title, and ignores which
// repository a reference points at. That looseness is harmless for spotting
// that an issue already has a fix in flight, but not for deciding who to
// assign: "Fixes #7, related to #9" must not make #9's creator an owner, and
// "Fixes other/repo#12" says nothing about this repository's #12. A qualified
// reference only counts when it names owner/repo, so with either unknown, only
// bare #N references do.
func strictClosingIssues(pr *githubv39.PullRequest, owner, repo string) map[int]bool {
	closing := make(map[int]bool)
	add := func(s string) {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < maxIssueNumber {
			closing[n] = true
		}
	}
	sameRepo := func(o, r string) bool {
		return owner != "" && repo != "" && strings.EqualFold(o, owner) && strings.EqualFold(r, repo)
	}

	for _, m := range closingBranchRe.FindAllStringSubmatch(pr.GetHead().GetRef(), -1) {
		add(m[1])
	}
	for _, m := range closingRefRe.FindAllStringSubmatch(pr.GetBody(), -1) {
		switch {
		case m[1] != "":
			add(m[1])
		case m[4] != "" && sameRepo(m[2], m[3]):
			add(m[4])
		case m[7] != "" && sameRepo(m[5], m[6]):
			add(m[7])
		}
	}
	return closing
}

package prs

import (
	"sort"
	"testing"

	githubv39 "github.com/google/go-github/v39/github"
)

func TestStrictClosingIssues(t *testing.T) {
	tests := []struct {
		name   string
		title  string
		body   string
		branch string
		owner  string
		repo   string
		want   []int
	}{
		{name: "bare reference", body: "Fixes #7", want: []int{7}},
		{name: "every keyword form", body: "close #1\ncloses #2\nclosed #3\nfix #4\nfixes #5\nfixed #6\nresolve #7\nresolves #8\nresolved #9", want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{name: "case and colon", body: "FIXES: #7", want: []int{7}},
		{name: "mid-sentence keyword", body: "This change fixes #7 by retrying.", want: []int{7}},
		{name: "later reference on the same line is a mention", body: "Fixes #7, related to #9", want: []int{7}},
		{name: "comma list only closes the first", body: "Fixes #7, #8", want: []int{7}},
		{name: "each keyword closes its own reference", body: "Fixes #7, fixes #8", want: []int{7, 8}},
		{name: "keyword not directly before the reference", body: "Fixes the flake in #7", want: nil},
		{name: "plain mention", body: "Related to #7", want: nil},
		{name: "keyword inside a word", body: "prefixes #7 and suffixed #8", want: nil},
		{name: "title is ignored", title: "fix: resolve #7", body: "Some change", want: nil},
		{name: "same-repo qualified reference", body: "Fixes Test-Owner/test-repo#7", owner: "test-owner", repo: "test-repo", want: []int{7}},
		{name: "other-repo qualified reference", body: "Fixes other/repo#12", owner: "test-owner", repo: "test-repo", want: nil},
		{name: "qualified reference with unknown repo", body: "Fixes test-owner/test-repo#7", want: nil},
		{name: "same-repo issue URL", body: "Closes https://github.com/test-owner/test-repo/issues/7", owner: "test-owner", repo: "test-repo", want: []int{7}},
		{name: "other-repo issue URL", body: "Closes https://github.com/other/repo/issues/7", owner: "test-owner", repo: "test-repo", want: nil},
		{name: "watcher branch with epoch", branch: "issue-123-1700000000", want: []int{123}},
		{name: "factory branch", branch: "factory-issue_45", want: []int{45}},
		{name: "unrelated branch digits", branch: "fix-flake-2", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pr := &githubv39.PullRequest{
				Title: stringPtr(tc.title),
				Body:  stringPtr(tc.body),
				Head:  &githubv39.PullRequestBranch{Ref: stringPtr(tc.branch)},
			}
			var got []int
			for n := range strictClosingIssues(pr, tc.owner, tc.repo) {
				got = append(got, n)
			}
			sort.Ints(got)
			if len(got) != len(tc.want) {
				t.Fatalf("strictClosingIssues() = %v; want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("strictClosingIssues() = %v; want %v", got, tc.want)
				}
			}
		})
	}
}

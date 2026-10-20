package github

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ListSubIssueRefs returns an issue's sub-issues as IssueRefs, following
// pagination. It is ListSubIssues with what each sub-issue says, which the
// sub-issues endpoint returns anyway: the body (where a pipeline child's
// marker is), its state and state reason, labels and assignees.
func (c *Client) ListSubIssueRefs(ctx context.Context, number int) ([]IssueRef, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	var out []IssueRef
	for page := 1; ; page++ {
		req, err := c.gh.NewRequest("GET", fmt.Sprintf("repos/%s/%s/issues/%d/sub_issues?per_page=100&page=%d", c.owner, c.repo, number, page), nil)
		if err != nil {
			return nil, err
		}
		var subs []rawIssue
		resp, err := c.gh.Do(ctx, req, &subs)
		if err != nil {
			return nil, fmt.Errorf("listing the sub-issues of #%d: %w", number, err)
		}
		for _, s := range subs {
			out = append(out, s.ref())
		}
		if resp == nil || resp.NextPage == 0 {
			return out, nil
		}
	}
}

// IssueRefListOptions selects the issues ListIssueRefs returns. Empty fields
// are not sent, so GitHub's defaults apply.
type IssueRefListOptions struct {
	// Creator is the login of the account that opened them.
	Creator string
	// Labels are labels every one of them carries.
	Labels []string
	// State is "open", "closed" or "all".
	State string
	// Sort is "created", "updated" or "comments".
	Sort string
	// Direction is "asc" or "desc".
	Direction string
	// Since keeps only those updated at or after it (ISO 8601).
	Since string
	// PerPage is the page size, at most 100.
	PerPage int
	// Page is the page to read, from 1.
	Page int
}

// ListIssueRefs returns a single page of the repository's issues as
// IssueRefs, pull requests included (test IsPR). It reads the repository's
// issue listing, not the search index, so an issue appears in it as soon as
// it is created.
func (c *Client) ListIssueRefs(ctx context.Context, opts IssueRefListOptions) ([]IssueRef, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	q := url.Values{}
	if opts.Creator != "" {
		q.Set("creator", opts.Creator)
	}
	if len(opts.Labels) > 0 {
		q.Set("labels", strings.Join(opts.Labels, ","))
	}
	if opts.State != "" {
		q.Set("state", opts.State)
	}
	if opts.Sort != "" {
		q.Set("sort", opts.Sort)
	}
	if opts.Direction != "" {
		q.Set("direction", opts.Direction)
	}
	if opts.Since != "" {
		q.Set("since", opts.Since)
	}
	if opts.PerPage > 0 {
		q.Set("per_page", strconv.Itoa(opts.PerPage))
	}
	if opts.Page > 0 {
		q.Set("page", strconv.Itoa(opts.Page))
	}
	u := fmt.Sprintf("repos/%s/%s/issues", c.owner, c.repo)
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	req, err := c.gh.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	var issues []rawIssue
	if _, err := c.gh.Do(ctx, req, &issues); err != nil {
		return nil, fmt.Errorf("listing issues: %w", err)
	}
	out := make([]IssueRef, 0, len(issues))
	for _, is := range issues {
		out = append(out, is.ref())
	}
	return out, nil
}

// SetIssueState opens or closes an issue. state is "open" or "closed";
// reason, which may be empty, is the state reason GitHub records:
// "completed", "not_planned" or "duplicate" when closing, "reopened" when
// opening.
func (c *Client) SetIssueState(ctx context.Context, number int, state, reason string) error {
	if !c.Ready() {
		return errNoClient
	}

	body := map[string]string{"state": state}
	if reason != "" {
		body["state_reason"] = reason
	}
	req, err := c.gh.NewRequest("PATCH", fmt.Sprintf("repos/%s/%s/issues/%d", c.owner, c.repo, number), body)
	if err != nil {
		return err
	}
	if _, err := c.gh.Do(ctx, req, nil); err != nil {
		if reason != "" {
			return fmt.Errorf("setting #%d %s (%s): %w", number, state, reason, err)
		}
		return fmt.Errorf("setting #%d %s: %w", number, state, err)
	}
	return nil
}

// CloseEvent is the last time an issue was closed.
type CloseEvent struct {
	// Actor is the login of the account that closed it.
	Actor string
	// CommitID is set when a commit closed it: one pushed with "Fixes #N",
	// or the merge of a pull request that says so.
	CommitID string
}

// maxEventPages bounds how many pages of an issue's events LastCloseEvent
// reads.
const maxEventPages = 10

// LastCloseEvent returns the last "closed" event on an issue's event list,
// and false when it has none (it was never closed, or not within the pages
// read).
func (c *Client) LastCloseEvent(ctx context.Context, number int) (CloseEvent, bool, error) {
	if !c.Ready() {
		return CloseEvent{}, false, errNoClient
	}

	var last CloseEvent
	found := false
	for page := 1; page <= maxEventPages; page++ {
		req, err := c.gh.NewRequest("GET", fmt.Sprintf("repos/%s/%s/issues/%d/events?per_page=100&page=%d", c.owner, c.repo, number, page), nil)
		if err != nil {
			return CloseEvent{}, false, err
		}
		var events []struct {
			Event    string `json:"event"`
			CommitID string `json:"commit_id"`
			Actor    *struct {
				Login string `json:"login"`
			} `json:"actor"`
		}
		resp, err := c.gh.Do(ctx, req, &events)
		if err != nil {
			return CloseEvent{}, false, fmt.Errorf("listing the events of #%d: %w", number, err)
		}
		for _, ev := range events {
			if ev.Event != "closed" {
				continue
			}
			last = CloseEvent{CommitID: ev.CommitID}
			if ev.Actor != nil {
				last.Actor = ev.Actor.Login
			}
			found = true
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
	}
	return last, found, nil
}

// RepoPermissions returns what the authenticated account may do in the
// repository, as GitHub reports it: "admin", "maintain", "push", "triage"
// and "pull", each true or false.
func (c *Client) RepoPermissions(ctx context.Context) (map[string]bool, error) {
	if !c.Ready() {
		return nil, errNoClient
	}

	repo, _, err := c.gh.Repositories.Get(ctx, c.owner, c.repo)
	if err != nil {
		return nil, fmt.Errorf("fetching %s/%s: %w", c.owner, c.repo, err)
	}
	return repo.Permissions, nil
}

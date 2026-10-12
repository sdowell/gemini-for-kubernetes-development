package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// GraphQLEndpoint is the target URL for GitHub's GraphQL API.
var GraphQLEndpoint = "https://api.github.com/graphql"

// defaultGraphQLEndpoint is the production value of GraphQLEndpoint. Tests that
// do not stand up a fake GraphQL server leave GraphQLEndpoint at this value.
const defaultGraphQLEndpoint = "https://api.github.com/graphql"

// skipGraphQL reports whether a GraphQL call must be skipped because it would
// reach the real GitHub API from a test that did not point GraphQLEndpoint at
// a fake server.
func skipGraphQL(owner string) bool {
	return GraphQLEndpoint == defaultGraphQLEndpoint && owner == "test-owner"
}

// graphQLError is one entry of the errors array of a GraphQL response.
type graphQLError struct {
	Message string `json:"message"`
}

// doGraphQL posts a GraphQL query and decodes its data field into out.
func doGraphQL(ctx context.Context, query string, variables map[string]interface{}, out interface{}) error {
	token, err := GetGithubToken(ctx)
	if err != nil {
		return fmt.Errorf("getting github token: %w", err)
	}

	queryBytes, err := json.Marshal(map[string]interface{}{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return fmt.Errorf("marshaling graphql query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", GraphQLEndpoint, bytes.NewBuffer(queryBytes))
	if err != nil {
		return fmt.Errorf("creating http request: %w", err)
	}

	req.Header.Set("Authorization", "bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	// Use default HTTP client so it automatically respects system proxies and SSL settings
	client := http.DefaultClient

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("sending graphql request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("graphql request failed with status %s", resp.Status)
	}

	var graphqlResp struct {
		Data   json.RawMessage `json:"data"`
		Errors []graphQLError  `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&graphqlResp); err != nil {
		return fmt.Errorf("decoding graphql response: %w", err)
	}
	if len(graphqlResp.Errors) > 0 {
		return fmt.Errorf("graphql error: %s", graphqlResp.Errors[0].Message)
	}
	if len(graphqlResp.Data) == 0 || string(graphqlResp.Data) == "null" {
		return nil
	}
	if err := json.Unmarshal(graphqlResp.Data, out); err != nil {
		return fmt.Errorf("decoding graphql data: %w", err)
	}
	return nil
}

// IsPRInMergeQueue checks if a pull request is currently in a merge queue using the GitHub GraphQL API.
func IsPRInMergeQueue(ctx context.Context, owner, repo string, prNum int) (bool, error) {
	if skipGraphQL(owner) {
		return false, nil
	}

	var data struct {
		Repository struct {
			PullRequest struct {
				MergeQueueEntry *struct {
					State string `json:"state"`
				} `json:"mergeQueueEntry"`
			} `json:"pullRequest"`
		} `json:"repository"`
	}
	err := doGraphQL(ctx, `query($owner: String!, $repo: String!, $prNumber: Int!) {
			repository(owner: $owner, name: $repo) {
				pullRequest(number: $prNumber) {
					mergeQueueEntry {
						state
					}
				}
			}
		}`, map[string]interface{}{
		"owner":    owner,
		"repo":     repo,
		"prNumber": prNum,
	}, &data)
	if err != nil {
		return false, err
	}

	return data.Repository.PullRequest.MergeQueueEntry != nil, nil
}

// MergeQueueEviction describes the most recent removal of a pull request from
// the repository's merge queue.
type MergeQueueEviction struct {
	// At is when the pull request was removed from the merge queue.
	At time.Time
	// Reason is GitHub's free-form reason for the removal, such as
	// "failed_checks".
	Reason string
	// MergeGroupSHA is the temporary merge-group commit the queue tested: the
	// pull request on top of the base branch (and of anything queued ahead of
	// it). The checks that evicted the pull request ran against this commit,
	// not against the pull request's head.
	MergeGroupSHA string
}

// IsFailure reports whether the eviction was caused by the change itself
// failing in the queue - its checks failed, or it no longer merged - rather
// than by someone dequeuing it by hand. Only the former is work for the
// watcher: a manual removal is a person's decision, not a defect to fix.
func (e *MergeQueueEviction) IsFailure() bool {
	if e == nil {
		return false
	}
	r := strings.ToLower(e.Reason)
	return strings.Contains(r, "fail") || strings.Contains(r, "conflict")
}

// LatestMergeQueueEviction returns the most recent removal of a pull request
// from the merge queue, or nil if it has never been removed from one.
//
// The removal is invisible everywhere else the watcher looks: the merge queue
// runs its checks on a temporary merge-group commit, so the pull request's own
// head stays green and mergeable even when the change no longer builds on top
// of the base branch. This timeline event is the only record of that failure
// attached to the pull request itself.
func LatestMergeQueueEviction(ctx context.Context, owner, repo string, prNum int) (*MergeQueueEviction, error) {
	if skipGraphQL(owner) {
		return nil, nil
	}

	var data struct {
		Repository struct {
			PullRequest struct {
				TimelineItems struct {
					Nodes []struct {
						CreatedAt    time.Time `json:"createdAt"`
						Reason       string    `json:"reason"`
						BeforeCommit *struct {
							OID string `json:"oid"`
						} `json:"beforeCommit"`
					} `json:"nodes"`
				} `json:"timelineItems"`
			} `json:"pullRequest"`
		} `json:"repository"`
	}
	err := doGraphQL(ctx, `query($owner: String!, $repo: String!, $prNumber: Int!) {
			repository(owner: $owner, name: $repo) {
				pullRequest(number: $prNumber) {
					timelineItems(last: 1, itemTypes: [REMOVED_FROM_MERGE_QUEUE_EVENT]) {
						nodes {
							... on RemovedFromMergeQueueEvent {
								createdAt
								reason
								beforeCommit {
									oid
								}
							}
						}
					}
				}
			}
		}`, map[string]interface{}{
		"owner":    owner,
		"repo":     repo,
		"prNumber": prNum,
	}, &data)
	if err != nil {
		return nil, err
	}

	nodes := data.Repository.PullRequest.TimelineItems.Nodes
	if len(nodes) == 0 {
		return nil, nil
	}
	node := nodes[len(nodes)-1]
	if node.CreatedAt.IsZero() {
		return nil, nil
	}
	eviction := &MergeQueueEviction{
		At:     node.CreatedAt,
		Reason: node.Reason,
	}
	if node.BeforeCommit != nil {
		eviction.MergeGroupSHA = node.BeforeCommit.OID
	}
	return eviction, nil
}

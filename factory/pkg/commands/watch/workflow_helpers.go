package watch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"
)

func parseGitHubURL(urlStr string) (owner, repo, branch, path string, ok bool) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", "", "", false
	}
	if u.Host != "github.com" {
		return "", "", "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) < 4 || (parts[2] != "blob" && parts[2] != "raw") {
		return "", "", "", "", false
	}
	owner = parts[0]
	repo = parts[1]
	branch = parts[3]
	path = strings.Join(parts[4:], "/")
	return owner, repo, branch, path, true
}

// FetchWorkflowContent retrieves the raw content of a workflow URL.
func FetchWorkflowContent(ctx context.Context, ghClient *githubv39.Client, urlStr string) ([]byte, error) {
	urlStr = SanitizeWorkflowPath(urlStr)
	if owner, repo, branch, path, ok := parseGitHubURL(urlStr); ok {
		klog.Infof("Fetching agent from GitHub repository %s/%s at branch/ref %s, path %s", owner, repo, branch, path)
		fileContent, _, _, err := ghClient.Repositories.GetContents(ctx, owner, repo, path, &githubv39.RepositoryContentGetOptions{Ref: branch})
		if err != nil {
			return nil, fmt.Errorf("fetching content from GitHub repo: %w", err)
		}
		if fileContent == nil {
			return nil, fmt.Errorf("content is nil (possibly a directory or submodule)")
		}
		contentStr, err := fileContent.GetContent()
		if err != nil {
			return nil, fmt.Errorf("decoding GitHub content: %w", err)
		}
		return []byte(contentStr), nil
	}

	klog.Infof("Fetching agent from HTTP URL %s", urlStr)
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status fetching agent from URL %s: %s", urlStr, resp.Status)
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("reading agent body from URL %s: %w", urlStr, err)
	}

	return buf.Bytes(), nil
}

func fetchWorkflowContent(ctx context.Context, ghClient *githubv39.Client, urlStr string) ([]byte, error) {
	return FetchWorkflowContent(ctx, ghClient, urlStr)
}

var workflowURLRegex = regexp.MustCompile(`(?:\s|^)(https?://[^\s\)"'` + "`" + `]+(?:\.(?:md|txt|yaml)|/(?:workflows|agents)/)[^\s\)"'` + "`" + `]*)`)

var workflowFileRegex = regexp.MustCompile(`(?:\s|^)(\.?\.?/?(?:\.?agents?|\.gemini)/[a-zA-Z0-9_\-\./]+)\b`)

// SanitizeWorkflowPath cleans up trailing escapes and newlines from matched paths.
func SanitizeWorkflowPath(path string) string {
	path = strings.TrimSpace(path)
	for strings.HasSuffix(path, `\n`) || strings.HasSuffix(path, `\r`) {
		path = strings.TrimSuffix(strings.TrimSuffix(path, `\n`), `\r`)
		path = strings.TrimSpace(path)
	}
	return path
}

func sanitizeWorkflowPath(path string) string {
	return SanitizeWorkflowPath(path)
}

// FindWorkflowPath extracts workflow URLs or relative agent paths from text.
func FindWorkflowPath(body string) string {
	urlMatch := workflowURLRegex.FindStringSubmatch(body)
	if len(urlMatch) > 1 {
		return SanitizeWorkflowPath(urlMatch[1])
	}

	matches := workflowFileRegex.FindStringSubmatch(body)
	if len(matches) > 1 {
		return SanitizeWorkflowPath(matches[1])
	}
	return ""
}

func findWorkflowPath(body string) string {
	return FindWorkflowPath(body)
}

// IsWorkflowDefinition returns true if the referenced path is a valid workflow.
func IsWorkflowDefinition(ctx context.Context, ghClient *githubv39.Client, owner, repo, path string) bool {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		// 1. Path/URL convention check
		if strings.Contains(path, "/workflows/") || strings.Contains(path, "/agents/") {
			return true
		}

		// 2. Download and verify headers
		content, err := FetchWorkflowContent(ctx, ghClient, path)
		if err != nil {
			klog.V(4).Infof("Failed to fetch content from workflow URL %s: %v", path, err)
			return false
		}

		limit := 2000
		if len(content) < limit {
			limit = len(content)
		}
		header := string(content[:limit])
		if strings.Contains(header, "mode: workflow") || strings.Contains(header, "mode: \"workflow\"") || strings.Contains(header, "AGENT_MODE=workflow") {
			return true
		}
		return false
	}

	// 1. Directory convention: any path containing "/workflows/" is treated as a workflow
	if strings.Contains(path, "/workflows/") {
		return true
	}

	// Clean up leading dot slashes from path to match GitHub API format
	cleanPath := strings.TrimPrefix(path, "./")
	cleanPath = strings.TrimPrefix(cleanPath, "/")

	// 2. Fetch remote content from GitHub and search for keywords/metadata
	fileContent, _, _, err := ghClient.Repositories.GetContents(ctx, owner, repo, cleanPath, &githubv39.RepositoryContentGetOptions{})
	if err != nil {
		klog.V(4).Infof("Failed to get content for %s: %v", cleanPath, err)
		return false
	}
	if fileContent == nil {
		klog.V(4).Infof("Content is nil for %s (possibly a directory or submodule)", cleanPath)
		return false
	}
	content, err := fileContent.GetContent()
	if err != nil {
		return false
	}

	limit := 2000
	if len(content) < limit {
		limit = len(content)
	}
	header := content[:limit]

	// Look for mode: workflow metadata in header or front-matter
	if strings.Contains(header, "mode: workflow") || strings.Contains(header, "mode: \"workflow\"") || strings.Contains(header, "AGENT_MODE=workflow") {
		return true
	}

	return false
}

func isWorkflowDefinition(ctx context.Context, ghClient *githubv39.Client, owner, repo, path string) bool {
	return IsWorkflowDefinition(ctx, ghClient, owner, repo, path)
}

func getWorkflowCooldown(ctx context.Context, ghClient *githubv39.Client, owner, repo, path string) time.Duration {
	defaultCooldown := 10 * time.Minute
	if path == "" {
		return defaultCooldown
	}

	var content []byte
	var err error
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		content, err = fetchWorkflowContent(ctx, ghClient, path)
	} else {
		cleanPath := strings.TrimPrefix(path, "./")
		cleanPath = strings.TrimPrefix(cleanPath, "/")
		var fileContent *githubv39.RepositoryContent
		fileContent, _, _, err = ghClient.Repositories.GetContents(ctx, owner, repo, cleanPath, &githubv39.RepositoryContentGetOptions{})
		if err == nil {
			var contentStr string
			contentStr, err = fileContent.GetContent()
			content = []byte(contentStr)
		}
	}
	if err != nil {
		return defaultCooldown
	}

	agentDef, err := ParseAgent(content)
	if err != nil || agentDef.Cooldown == "" {
		return defaultCooldown
	}

	d, err := time.ParseDuration(agentDef.Cooldown)
	if err != nil {
		klog.Warningf("Failed to parse workflow cooldown duration %q: %v", agentDef.Cooldown, err)
		return defaultCooldown
	}
	return d
}

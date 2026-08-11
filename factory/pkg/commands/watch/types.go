package watch

import (
	"fmt"
	"strings"
	"time"

	factorysandbox "github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/sandbox"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// RootOptions holds resolved k8s and sandbox execution options passed from the root command.
type RootOptions struct {
	Namespace        string
	Image            string
	DiskSize         string
	SecretName       string
	EphemeralStorage string
	CPURequest       string
	CPULimit         string
	MemoryRequest    string
	MemoryLimit      string
	ResolvedSecrets  []factorysandbox.SecretMount
}

// ResolveRootOptionsFunc defines a callback to resolve root-level CLI flags and configuration.
type ResolveRootOptionsFunc func(cmd *cobra.Command) (*RootOptions, error)

// WatchFlags contains CLI flags for the watch command.
type WatchFlags struct {
	Repo                string
	PollInterval        time.Duration
	Assignee            string
	Labels              []string
	DryRun              bool
	WatchTimeout        time.Duration
	MaxActions          int
	MaxPending          int
	Mode                string
	QueueDir            string
	Once                bool
	IssueMode           string
	PRMode              string
	ChoresMode          string
	ScanLimit           int
	TaskTimeout         time.Duration
	SandboxEvictionAge  string
	SandboxIdleTimeout  time.Duration
	PRInactivityTimeout time.Duration
}

// QueueTask represents a discrete unit of work in the queue.
type QueueTask struct {
	Type         string    `yaml:"type"` // "issue-fix", "pr-investigate", "pr-comments", "pr-iterate", "pr-review", "agent-chore"
	URL          string    `yaml:"url"`
	Number       int       `yaml:"number"`
	Priority     string    `yaml:"priority"` // "critical", "urgent", "important", "high", "medium", "low"
	Phase        int       `yaml:"phase"`    // 1: Rebase/iterate, 2: Comments, 3: Investigate/Fix, 4: Chores
	CreatedAt    time.Time `yaml:"createdAt"`
	EnqueuedAt   time.Time `yaml:"enqueuedAt,omitempty"`
	Assignee     string    `yaml:"assignee,omitempty"`
	Status       string    `yaml:"status"` // "Pending", "Running", "Completed", "Failed"
	Error        string    `yaml:"error,omitempty"`
	AgentFile    string    `yaml:"agentFile,omitempty"` // For chore tasks
	SessionID    string    `yaml:"sessionId,omitempty"` // For workflow sessions
	CommitSHA    string    `yaml:"commitSHA,omitempty"`
	Instructions []string  `yaml:"instructions,omitempty"`
	Recovered    bool      `yaml:"recovered,omitempty"`
	CompletedAt  time.Time `yaml:"completedAt,omitempty"`
}

// ChoreRunState records the last execution time of an agent chore.
type ChoreRunState struct {
	LastRun time.Time `json:"lastRun"`
}

// AgentDefinition represents YAML frontmatter for an agent or workflow chore definition.
type AgentDefinition struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Schedule    string `yaml:"schedule"`
	SkipPR      bool   `yaml:"skipPR,omitempty"`
	Mode        string `yaml:"mode,omitempty"`
	Cooldown    string `yaml:"cooldown,omitempty"`
	Prompt      string `yaml:"-"`
}

// ParseAgent parses markdown/YAML frontmatter of an agent definition file.
func ParseAgent(content []byte) (*AgentDefinition, error) {
	parts := strings.SplitN(string(content), "---", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid agent definition format: missing frontmatter")
	}

	var def AgentDefinition
	if err := yaml.Unmarshal([]byte(parts[1]), &def); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontmatter: %w", err)
	}

	def.Prompt = strings.TrimSpace(parts[2])
	return &def, nil
}

// JournalEvent logs task lifecycle transitions in a JSONL journal.
type JournalEvent struct {
	Timestamp      time.Time `json:"timestamp"`
	TaskID         string    `json:"taskId"`
	Event          string    `json:"event"`
	Type           string    `json:"type"`
	URL            string    `json:"url"`
	Priority       string    `json:"priority"`
	Error          string    `json:"error,omitempty"`
	DurationSecond float64   `json:"durationSeconds,omitempty"`
}

// QueueTaskItem represents a formatted queue task returned by the API.
type QueueTaskItem struct {
	FileName   string `json:"fileName"`
	QueueState string `json:"queueState"`
	Type       string `json:"type"`
	URL        string `json:"url"`
	Number     int    `json:"number"`
	Priority   string `json:"priority"`
	Phase      int    `json:"phase"`
	CreatedAt  string `json:"createdAt"`
	EnqueuedAt string `json:"enqueuedAt,omitempty"`
	Assignee   string `json:"assignee"`
	Status     string `json:"status"`
	CommitSHA  string `json:"commitSHA"`
	Rank       int    `json:"rank,omitempty"`
}

// QueueSummary summarizes current queue counts.
type QueueSummary struct {
	TotalPending    int            `json:"totalPending"`
	TotalProcessing int            `json:"totalProcessing"`
	TotalCompleted  int            `json:"totalCompleted"`
	ByPriority      map[string]int `json:"byPriority"`
	ByType          map[string]int `json:"byType"`
}

// QueueResponse holds queue API status and items.
type QueueResponse struct {
	Summary    QueueSummary    `json:"summary"`
	Incoming   []QueueTaskItem `json:"incoming"`
	Processing []QueueTaskItem `json:"processing"`
	Processed  []QueueTaskItem `json:"processed"`
}

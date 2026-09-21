// Package issues owns the issue side of the watch daemon: finding the GitHub
// issues that need an agent and queueing the task that works on them.
//
// The Scanner runs as an autonomous goroutine so that a newly created or newly
// assigned issue is queued within one interval, instead of waiting behind the
// pull request evaluation that used to share its cycle. It reaches the rest of
// the daemon through narrow collaborators - a Queue for the tasks it creates,
// an Entities cache for what other subcontrollers have observed, a Sandboxes
// probe and a UserSelector - and never calls into another subcontroller.
//
// Pull requests are deliberately out of scope. GitHub's issue endpoints return
// pull requests alongside issues, and the scanner drops them: the pull request
// scanner lists them itself, on its own cadence.
package issues

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/common"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/ratelimit"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

const (
	// DefaultInterval is how often the issues assigned to the bot pool or
	// created by the operator are scanned. It is what sets pickup latency for a
	// new issue, and the queries behind it are bounded to a single page.
	DefaultInterval = 30 * time.Second
	// DefaultSweepInterval is how often the full trigger-labelled sweep runs.
	// That sweep paginates over every labelled issue in the repository, so it
	// stays on the slow cadence it has always had; the fast cycle above is what
	// picks up the issues a person just filed or assigned.
	DefaultSweepInterval = 5 * time.Minute
	// defaultScanLimit bounds the fast queries when no limit is configured.
	defaultScanLimit = 30
	// DefaultRateLimitBackoff is how long the scanner holds off after GitHub
	// refuses a cycle for rate limiting. Each consecutive refused cycle doubles
	// it, up to DefaultMaxRateLimitBackoff.
	DefaultRateLimitBackoff = ratelimit.DefaultBase
	// DefaultMaxRateLimitBackoff caps that doubling.
	DefaultMaxRateLimitBackoff = ratelimit.DefaultMax
)

// Queue is the subset of the task queue the Scanner needs: it adds issue tasks,
// withdraws the pending ones for an issue that has since been stopped, and
// reports what has already finished.
//
// The finished work is read through here rather than off the filesystem
// because the queue owns the task files. A scanner that opened the processed
// directory itself would be a second reader of state it does not control, and
// would miss everything that finished after it first looked.
type Queue interface {
	// TaskExists reports whether a task with the given file name is queued or running.
	TaskExists(filename string) bool
	// Enqueue adds a task to the queue under the given file name.
	Enqueue(filename string, task *api.QueueTask) error
	// RemovePendingTasksForNumber drops the not-yet-started tasks targeting an issue.
	RemovePendingTasksForNumber(number int) error
	// GetProcessedTask returns the finished task recorded under the given file
	// name, or nil when nothing by that name has finished.
	GetProcessedTask(filename string) *api.QueueTask
	// ListProcessedTasks returns every finished task, keyed by task file name.
	ListProcessedTasks() map[string]*api.QueueTask
}

// Entities is the shared view of what the scanners have observed. The Scanner
// reads the referenced-issue map to skip issues that already have a fix in
// flight, and publishes the issues it knows to be open for the sandbox
// reconciler to garbage collect against.
type Entities interface {
	// HasOpenPRs reports whether a pull request scan has published to the cache.
	HasOpenPRs() bool
	// UpdateOpenPRs replaces the cached open pull requests.
	UpdateOpenPRs(prs []*githubv39.PullRequest)
	// GetReferencedIssuesMap returns the issues referenced by an open pull request.
	GetReferencedIssuesMap() map[int]bool
	// SetOpenIssueNumbers replaces the set of issues known to be open.
	SetOpenIssueNumbers(nums []int)
}

// Sandboxes reports whether a task is already executing in a sandbox, which is
// the last check before queueing: a sandbox mid-run holds the workspace that a
// second task would fight over.
type Sandboxes interface {
	IsTaskRunning(ctx context.Context, name string) (bool, error)
}

// UserSelector picks the bot account a task should run as. Role configuration
// and sandbox pinning live with the watcher, so the Scanner asks rather than
// deciding.
type UserSelector interface {
	SelectUser(ctx context.Context, taskType api.TaskType, number int) (string, error)
}

// Config holds the tuning knobs of a Scanner.
type Config struct {
	// Interval is the delay between fast scan cycles. Defaults to DefaultInterval.
	Interval time.Duration
	// SweepInterval is the delay between full trigger-labelled sweeps.
	// Defaults to DefaultSweepInterval.
	SweepInterval time.Duration
	// TriggerLabel is the label marking an issue as the watcher's to work on.
	TriggerLabel string
	// TargetAssignee is the bot account the watcher runs as, used as the
	// fallback assignee and as the account auto-assigned to operator issues.
	TargetAssignee string
	// GitHubLogin is the login of the operator whose new issues are adopted
	// automatically. An empty login disables that query.
	GitHubLogin string
	// BotUsers is the pool of bot accounts whose assigned issues are scanned.
	BotUsers []string
	// ScanLimit caps the page size of the fast queries. Defaults to defaultScanLimit.
	ScanLimit int
	// MinNumber skips issues numbered below it, which is how a deployment
	// ignores a repository's history. Zero scans everything.
	MinNumber int
	// PrimeOpenPRs makes this scanner populate the open pull request half of
	// the entity cache itself, which it must do when no pull request scanner is
	// running to do it. See primeOpenPRs.
	PrimeOpenPRs bool
	// RateLimitBackoff is the first delay applied after GitHub refuses a cycle
	// for rate limiting. Defaults to DefaultRateLimitBackoff.
	RateLimitBackoff time.Duration
	// MaxRateLimitBackoff caps the growth of that delay. Defaults to
	// DefaultMaxRateLimitBackoff.
	MaxRateLimitBackoff time.Duration
	// DryRun reports what would be queued without touching the queue or GitHub.
	DryRun bool
}

// Deps holds the collaborators of a Scanner.
type Deps struct {
	// GitHub is the repository-bound client used for every query and write.
	// It carries the owner and repo, which is why the Config does not.
	GitHub *github.Client
	// Queue receives the issue tasks the scan creates.
	Queue Queue
	// Entities is the shared open PR / open issue cache.
	Entities Entities
	// Sandboxes probes whether a sandbox is already running the work.
	Sandboxes Sandboxes
	// Users resolves the bot account a queued task runs as.
	Users UserSelector
	// Paused reports whether scanning must be held off, which is how the
	// watcher propagates drain mode. It is deliberately a plain read-only
	// signal: the scanner can observe that the queue is draining but has no way
	// to mutate it. A nil Paused never pauses.
	//
	// Shutdown does not arrive through here. Cancelling the context passed to
	// Run is what stops the scanner, and unlike this signal it also aborts a
	// cycle that has already started.
	Paused func() bool
}

// Scanner queues the agent tasks for issues that need one.
//
// It is single-goroutine by construction: the record of when each issue was
// last worked on is a plain map owned by whoever calls ScanOnce, which is the
// Run loop in daemon mode and the caller itself in --once mode. Nothing else
// may call into it concurrently.
type Scanner struct {
	cfg       Config
	gh        *github.Client
	queue     Queue
	entities  Entities
	sandboxes Sandboxes
	users     UserSelector
	paused    func() bool

	// processed records when each issue was last queued or completed, read from
	// the processed queue directory on first use.
	processed map[int]time.Time
	// lastSweep is when the trigger-labelled sweep last completed successfully.
	lastSweep time.Time
	// rateLimit holds the scanner off while GitHub is refusing it. Every GitHub
	// error a cycle sees is reported to it, and it is consulted between issues
	// as well as at the top of a cycle, so a refusal partway through costs the
	// rest of the cycle rather than a doomed request per issue.
	rateLimit *ratelimit.Backoff
}

// New constructs a Scanner from its configuration and dependencies.
func New(cfg Config, deps Deps) *Scanner {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.ScanLimit <= 0 {
		cfg.ScanLimit = defaultScanLimit
	}
	return &Scanner{
		cfg:       cfg,
		gh:        deps.GitHub,
		queue:     deps.Queue,
		entities:  deps.Entities,
		sandboxes: deps.Sandboxes,
		users:     deps.Users,
		paused:    deps.Paused,
		rateLimit: ratelimit.New("issue scanner", cfg.RateLimitBackoff, cfg.MaxRateLimitBackoff),
	}
}

// Run scans for issues until ctx is cancelled, returning nil once it has stopped.
//
// A cycle runs immediately so that a restart picks up the issues filed while
// the daemon was down, instead of waiting out a full interval.
//
// The delay between cycles is decided after each one rather than fixed by a
// ticker, because a rate limited scanner has to wait out the refusal and a
// ticker would keep waking it on the interval to rediscover that. A timer per
// iteration also means a cycle that overran cannot be immediately followed by a
// queued tick.
func (s *Scanner) Run(ctx context.Context) error {
	s.cycle(ctx)

	for {
		timer := time.NewTimer(s.nextDelay())
		select {
		case <-ctx.Done():
			// Cancellation is how this subcontroller is asked to stop, so it is
			// not an error worth propagating to the caller.
			timer.Stop()
			return nil
		case <-timer.C:
			s.cycle(ctx)
		}
	}
}

// cycle runs one scan and reports its outcome to the backoff.
//
// This is the only place the backoff is told anything, and it is the reason
// every GitHub call beneath it returns its error rather than logging it and
// carrying on. A refusal surfaces here having already ended the cycle, so one
// report is both necessary and sufficient to decide how long to wait.
//
// A failed cycle is not fatal to the daemon. The next one re-lists from scratch
// and covers whatever this one did not reach.
func (s *Scanner) cycle(ctx context.Context) {
	err := s.ScanOnce(ctx)
	s.rateLimit.Observe(err)
	if err != nil {
		klog.Errorf("Issue scan cycle failed: %v", err)
	}
}

// nextDelay is how long to wait before the next cycle: the scan interval
// normally, and the remainder of a rate limit wait when GitHub has refused us
// for longer than that.
//
// Running the cycle anyway would cost requests that deepen the refusal and
// return listings that cannot be told apart from a repository with nothing to
// do, so the cadence gives way to the wait rather than racing it.
func (s *Scanner) nextDelay() time.Duration {
	wait, blocked := s.rateLimit.Blocked()
	if !blocked || wait <= s.cfg.Interval {
		return s.cfg.Interval
	}
	klog.V(2).Infof("Holding the next issue scan for %s: GitHub is rate limiting us.", wait.Round(time.Second))
	return wait
}

// ScanOnce runs one scan cycle, queueing work for the issues that need it. The
// trigger-labelled sweep runs inside it whenever it has come due.
//
// The error returned is the first GitHub failure the cycle met, and meeting one
// is what ended the cycle. Callers that scan on a schedule hand it to the
// backoff; the one-shot caller reports it and exits non-zero.
//
// A rate limit is not consulted here: this runs a cycle when asked, and it is
// Run that decides when to ask. An operator invoking a one-shot scan wants the
// scan, not a report that the daemon would have waited.
func (s *Scanner) ScanOnce(ctx context.Context) error {
	if s.paused != nil && s.paused() {
		klog.V(2).Infof("Skipping issue scan because the watcher is draining.")
		return nil
	}

	if err := s.primeOpenPRs(ctx); err != nil {
		return err
	}
	if !s.entities.HasOpenPRs() {
		// The open PR cache is the primary duplicate-suppression signal for
		// issue scans. An empty cache is indistinguishable from "no open PR
		// references this issue", so scanning without one re-triggers fixes for
		// issues that already have an open PR. This happens in practice when
		// the process restarts into a GitHub rate limit window and every
		// attempt to list open PRs fails. Fail closed and wait for a successful
		// scan instead.
		klog.Warningf("Skipping issue scan: the open PR cache is not populated, so issues with an open fix PR cannot be identified.")
		return nil
	}
	refIssues := s.entities.GetReferencedIssuesMap()

	if s.sweepDue() {
		klog.Infof("Running trigger-labelled issue sweep...")
		labelled, err := s.scanLabelled(ctx)
		if err != nil {
			return err
		}
		if err := s.queueTasks(ctx, labelled, refIssues); err != nil {
			return err
		}

		// The sweep is only recorded as done once it has been queued from, so a
		// cycle that ended partway through sweeps again rather than waiting out
		// the sweep interval over an issue set it never finished with.
		//
		// Publishing the issues this sweep observed lets the sandbox reconciler
		// garbage collect the closed ones without re-querying GitHub, and it
		// happens here for the same reason: a set published by a cycle that did
		// not complete is one the reconciler would act on.
		s.lastSweep = time.Now()
		s.publishOpenIssues(labelled)
	}

	klog.Infof("Running fast issue scan cycle...")
	assigned, err := s.scanAssigned(ctx)
	if err != nil {
		return err
	}
	return s.queueTasks(ctx, assigned, refIssues)
}

// sweepDue reports whether the trigger-labelled sweep has come due. It runs on
// the first cycle, so a restart does not wait out a full sweep interval.
func (s *Scanner) sweepDue() bool {
	return s.lastSweep.IsZero() || time.Since(s.lastSweep) >= s.cfg.SweepInterval
}

// primeOpenPRs fills the open PR half of the entity cache on behalf of a pull
// request scanner that is not running.
//
// That half belongs to the pull request scanner, which republishes it every
// sweep, and this scan only reads it. But the two are gated independently: a
// deployment can scan issues with pull request scanning switched off entirely,
// and there the cache would never be published to at all. Since the scan fails
// closed on an unpublished cache, that is not a slow issue scan but no issue
// scan ever, so the scanner pays for the one listing itself.
//
// It is deliberately not a fallback for "the pull request scanner has not got
// to it yet". Priming whenever the cache happened to be cold would have both
// scanners issue the same paginated listing on every start, and would leave the
// cache with two writers.
func (s *Scanner) primeOpenPRs(ctx context.Context) error {
	if !s.cfg.PrimeOpenPRs || s.entities.HasOpenPRs() || !s.gh.Ready() {
		return nil
	}
	klog.Infof("Populating open PRs cache for referenced issues...")
	prs, err := s.gh.ListOpenPRs(ctx)
	if err != nil {
		return fmt.Errorf("populating open PRs cache: %w", err)
	}
	s.entities.UpdateOpenPRs(prs)
	return nil
}

// publishOpenIssues records which issues are known to be open in the shared
// entity cache. The sandbox reconciler uses it to skip sandboxes belonging to
// live work instead of confirming every one of them against GitHub.
//
// Issues that have already been processed are treated as open: their sandbox
// may still hold a workspace whose result has not been pushed yet, and the
// reconciler confirms the state with GitHub before deleting anything anyway.
func (s *Scanner) publishOpenIssues(openIssues []*githubv39.Issue) {
	processed := s.processedIssues()
	nums := make([]int, 0, len(openIssues)+len(processed))
	for _, iss := range openIssues {
		if num := iss.GetNumber(); num > 0 {
			nums = append(nums, num)
		}
	}
	for num := range processed {
		nums = append(nums, num)
	}
	s.entities.SetOpenIssueNumbers(nums)
}

// queueTasks queues a fix - or the workflow the issue asks for - for every
// issue that still needs one.
//
// An issue that fails for its own reasons - a malformed workflow reference, an
// issue closed between the listing and the read - is logged and skipped, so one
// bad issue cannot hold up the rest of the queue indefinitely.
//
// A rate limit refusal does end the pass. Each issue costs a timeline listing
// and often a search, so the ones still to come would spend that on refusals
// and arrive at the same answer; they are picked up by the cycle after the wait.
func (s *Scanner) queueTasks(ctx context.Context, issues []*githubv39.Issue, refIssues map[int]bool) error {
	klog.Infof("queueTasks called with %d issues", len(issues))
	for _, issue := range issues {
		if err := s.queueTask(ctx, issue, refIssues); err != nil {
			if github.IsRateLimited(err) {
				return err
			}
			klog.Errorf("Failed to queue task for issue #%d: %v", issue.GetNumber(), err)
		}
	}
	return nil
}

// queueTask evaluates one issue and queues its task if it needs one.
func (s *Scanner) queueTask(ctx context.Context, issue *githubv39.Issue, refIssues map[int]bool) error {
	num := issue.GetNumber()
	if s.cfg.MinNumber > 0 && num < s.cfg.MinNumber {
		return nil
	}
	if conventions.HasStopLabel(issue.Labels, s.cfg.TriggerLabel) {
		klog.Infof("Skipping issue #%d because it has the stop label ('overseer/stop' or '%s/stop')", num, s.cfg.TriggerLabel)
		_ = s.queue.RemovePendingTasksForNumber(num)
		return nil
	}
	if refIssues[num] {
		klog.Infof("Skipping issue #%d because there is already a PR referencing it.", num)
		return nil
	}

	// An issue may name a workflow in its description, in which case the task
	// runs that definition instead of the standard fix.
	workflowPath := common.FindWorkflowPath(issue.GetBody())
	workflowName := ""
	if workflowPath != "" {
		if common.IsWorkflowDefinition(ctx, s.gh, workflowPath) {
			filenameOnly := filepath.Base(workflowPath)
			ext := filepath.Ext(filenameOnly)
			workflowName = strings.TrimSuffix(filenameOnly, ext)
		} else {
			// It was just a standard skill/agent prompt mentioned, not a
			// workflow. Fall back to the standard issue fix.
			workflowPath = ""
		}
	}

	filename := fmt.Sprintf("task-issue-%d.yaml", num)
	if workflowName != "" {
		filename = fmt.Sprintf("task-workflow-%s-issue-%d.yaml", common.Slugify(workflowName), num)
	}

	if s.queue.TaskExists(filename) {
		return nil
	}

	if s.inCooldown(ctx, filename, workflowPath) {
		return nil
	}

	processed := s.processedIssues()
	lastProcessed, ok := processed[num]
	if ok && !issue.GetUpdatedAt().After(lastProcessed) && workflowName == "" {
		return nil
	}

	var timeline []*githubv39.Timeline
	timelineComplete := false
	if s.gh.Ready() {
		tl, complete, err := s.gh.ListIssueTimeline(ctx, num)
		if err != nil {
			return fmt.Errorf("listing timeline for issue #%d: %w", num, err)
		}
		timeline = tl
		timelineComplete = complete
	}

	// Skip the linked-PR check for workflow triggers, which do not necessarily
	// have a linked code PR.
	if workflowName == "" {
		// Only let the linked-PR check trust the timeline we already fetched if
		// it is complete. A missing or truncated timeline cannot prove the
		// absence of a linked PR, so pass nil and let HasLinkedPRWithTimeline
		// fall back to the Search API.
		var verifiedTimeline []*githubv39.Timeline
		if timelineComplete {
			verifiedTimeline = timeline
		}
		linked, err := s.gh.HasLinkedPRWithTimeline(ctx, num, verifiedTimeline)
		if err != nil {
			return fmt.Errorf("checking linked PR for issue #%d: %w", num, err)
		}
		if linked {
			klog.Infof("Skipping issue #%d because it has a linked PR according to the Timeline API.", num)
			return nil
		}
	}

	sandboxName := fmt.Sprintf("fix-%s-%d", s.gh.Repo(), num)
	if workflowName != "" {
		sandboxName = fmt.Sprintf("wf-issue-%d", num)
	}

	// A sandbox lookup talks to the cluster rather than to GitHub, so a failure
	// says nothing about the quota and is this issue's problem alone.
	running, err := s.sandboxes.IsTaskRunning(ctx, sandboxName)
	if err != nil {
		klog.Errorf("Failed to check if sandbox %s is running: %v", sandboxName, err)
		return nil
	} else if running {
		klog.Infof("Skipping issue #%d because there is an in-flight sandbox %s.", num, sandboxName)
		return nil
	}

	wasAutoLabeled := false
	if !conventions.HasTriggerLabel(issue.Labels, s.cfg.TriggerLabel) {
		wasAutoLabeled = true
		if err := s.applyTriggerLabel(ctx, num); err != nil {
			return err
		}
	}

	triggerEventTime, triggerReason, triggerNotes := triggerInfo(issue, timeline, s.cfg.TriggerLabel, wasAutoLabeled)

	taskType := api.TypeIssueFix
	if workflowName != "" {
		taskType = api.TypeAgentChore
	}

	taskAssignee := s.selectUser(ctx, taskType, num)

	var task *api.QueueTask
	if workflowName != "" {
		task = s.newTask(taskOptions{
			Type:             api.TypeAgentChore,
			Issue:            issue,
			Phase:            api.PhaseChores,
			Assignee:         taskAssignee,
			TriggerEventTime: triggerEventTime,
			TriggerReason:    triggerReason,
			TriggerNotes:     triggerNotes,
			AgentFile:        workflowPath,
			SessionID:        fmt.Sprintf("issue-%d", num),
		})
	} else {
		task = s.newTask(taskOptions{
			Type:             api.TypeIssueFix,
			Issue:            issue,
			Phase:            api.PhaseInvestigate,
			Assignee:         taskAssignee,
			TriggerEventTime: triggerEventTime,
			TriggerReason:    triggerReason,
			TriggerNotes:     triggerNotes,
		})
	}

	if s.cfg.DryRun {
		if workflowName != "" {
			fmt.Printf("[DRYRUN] Would queue workflow task %s for issue #%d: %s\n", workflowName, num, task.URL)
		} else {
			fmt.Printf("[DRYRUN] Would queue fix task for issue #%d: %s\n", num, task.URL)
		}
		return nil
	}

	if workflowName != "" {
		fmt.Printf("Queueing workflow task %s for issue #%d...\n", workflowName, num)
	} else {
		fmt.Printf("Queueing fix task for issue #%d...\n", num)
	}
	// The issue is stamped as processed before the enqueue rather than after,
	// so that a failure to write the task file does not leave the next cycle
	// queueing the same work again.
	processed[num] = time.Now()
	if err := s.queue.Enqueue(filename, task); err != nil {
		klog.Errorf("Failed to queue task for issue #%d: %v", num, err)
	}
	return nil
}

// inCooldown reports whether the same task completed recently enough that it
// must not be queued again yet. The cooldown is declared by the workflow
// definition; a standard fix uses the default.
func (s *Scanner) inCooldown(ctx context.Context, filename, workflowPath string) bool {
	last := s.queue.GetProcessedTask(filename)
	if last == nil {
		return false
	}
	// The queue dates every finished task, falling back to the task file's own
	// timestamp for one that never recorded a completion time, so a zero here
	// means only that there is nothing to measure the cooldown against.
	if last.CompletedAt.IsZero() {
		return false
	}
	return time.Since(last.CompletedAt) < common.GetWorkflowCooldown(ctx, s.gh, workflowPath)
}

// applyTriggerLabel adopts an issue that was picked up by assignment rather
// than by label, so that the label always records what the watcher is acting on.
//
// A failed write is returned rather than logged, because the caller goes on to
// describe the task as auto-labelled: queueing it anyway would record a trigger
// reason that nothing on the issue supports.
func (s *Scanner) applyTriggerLabel(ctx context.Context, num int) error {
	if s.cfg.DryRun {
		fmt.Printf("[DRYRUN] Would add label '%s' to issue #%d\n", s.cfg.TriggerLabel, num)
		return nil
	}
	klog.Infof("Adding '%s' label to issue #%d", s.cfg.TriggerLabel, num)
	if err := s.gh.AddLabels(ctx, num, []string{s.cfg.TriggerLabel}); err != nil {
		return fmt.Errorf("adding label %q to issue #%d: %w", s.cfg.TriggerLabel, num, err)
	}
	return nil
}

// selectUser resolves the bot account the task runs as, falling back to the
// watcher's own account when role selection cannot answer.
func (s *Scanner) selectUser(ctx context.Context, taskType api.TaskType, num int) string {
	if s.users == nil {
		return s.cfg.TargetAssignee
	}
	assignee, err := s.users.SelectUser(ctx, taskType, num)
	if err != nil {
		klog.Errorf("Failed to select user for issue #%d: %v", num, err)
		return s.cfg.TargetAssignee
	}
	if assignee == "" {
		return s.cfg.TargetAssignee
	}
	return assignee
}

// processedIssues returns when each issue was last worked on, recovered from
// the queue's finished tasks on first use.
//
// The snapshot is taken once and then kept: queueTask stamps an issue here as
// it queues it, which is what stops a second cycle from queueing the same work
// before the first has finished.
func (s *Scanner) processedIssues() map[int]time.Time {
	if s.processed == nil {
		s.processed = processedIssueTimes(s.queue.ListProcessedTasks())
	}
	return s.processed
}

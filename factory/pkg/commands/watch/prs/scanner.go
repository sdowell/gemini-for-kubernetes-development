// Package prs owns the pull request side of the watch daemon: evaluating the
// open pull requests the watcher is responsible for and queueing the work they
// need - rebasing a conflicted branch, investigating failed CI, addressing
// review feedback, or reviewing the change.
//
// Evaluating one pull request costs the better part of a dozen GitHub requests
// (commits, comments, reviews, inline review comments, check runs, commit
// statuses, merge queue state), which is what made this the slowest part of the
// daemon and the reason it now runs as its own goroutine: nothing else has to
// wait behind it. Within a cycle the pull requests are evaluated on a bounded
// worker pool, so one slow pull request no longer holds up the rest.
package prs

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/api"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/ratelimit"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

const (
	// DefaultInterval is how often the pull requests assigned to the bot pool
	// are evaluated. Those are the ones with work in flight, and the listing
	// behind them is a single page per bot account.
	DefaultInterval = 1 * time.Minute
	// DefaultSweepInterval is how often every pull request the watcher is
	// responsible for is evaluated, and the open PR cache refreshed.
	//
	// Evaluation cost scales with the number of open pull requests, at roughly
	// a dozen requests each, so this interval is what keeps a busy repository
	// inside its hourly GitHub rate limit. Shortening it is not a free latency
	// win: the fast pass above already covers the pull requests that have work
	// in flight.
	DefaultSweepInterval = 5 * time.Minute
	// DefaultWorkers is how many pull requests are evaluated concurrently.
	// The pool bounds the burst of GitHub requests a cycle can produce while
	// still keeping one slow pull request from delaying the others.
	DefaultWorkers = 2
	// defaultScanLimit bounds the fast query when no limit is configured.
	defaultScanLimit = 30
	// DefaultRateLimitBackoff is how long the scanner holds off after GitHub
	// refuses a cycle for rate limiting. Each consecutive refused cycle doubles
	// it, up to DefaultMaxRateLimitBackoff.
	DefaultRateLimitBackoff = ratelimit.DefaultBase
	// DefaultMaxRateLimitBackoff caps that doubling.
	DefaultMaxRateLimitBackoff = ratelimit.DefaultMax
)

// Queue is the subset of the task queue the Scanner needs. It adds pull request
// tasks, withdraws the pending ones for a pull request that has been stopped,
// reports whether a pull request has work in flight - which is what keeps the
// ready-for-human label from flapping while a task is running - and reports
// what has already finished.
//
// The finished work is read through here rather than off the filesystem
// because the queue owns the task files. A scanner that opened the processed
// directory itself would be a second reader of state it does not control, and
// would miss everything that finished after it first looked.
type Queue interface {
	// TaskExists reports whether a task with the given file name is queued or running.
	TaskExists(filename string) bool
	// HasActivePRTask reports whether any task for the pull request is queued or running.
	HasActivePRTask(prNumber int) bool
	// Enqueue adds a task to the queue under the given file name.
	Enqueue(filename string, task *api.QueueTask) error
	// RemovePendingTasksForNumber drops the not-yet-started tasks targeting a pull request.
	RemovePendingTasksForNumber(number int) error
	// GetProcessedTask returns the finished task recorded under the given file
	// name, or nil when nothing by that name has finished.
	GetProcessedTask(filename string) *api.QueueTask
	// ListProcessedTasks returns every finished task, keyed by task file name.
	ListProcessedTasks() map[string]*api.QueueTask
}

// Entities is the shared view of what the scanners have observed. The Scanner
// is the owner of its pull request half: it publishes the open pull requests,
// from which the issue scanner learns which issues already have a fix in
// flight, and the sandbox reconciler learns which sandboxes are still live.
type Entities interface {
	// UpdateOpenPRs replaces the cached open pull requests.
	UpdateOpenPRs(prs []*githubv39.PullRequest)
}

// Sandboxes resolves sandbox names and reports whether one is mid-run, which is
// the last check before queueing: a sandbox already executing holds the
// workspace a second task would fight over.
type Sandboxes interface {
	ResolveName(ctx context.Context, taskType api.TaskType, num int) string
	IsTaskRunning(ctx context.Context, name string) (bool, error)
}

// Config holds the tuning knobs of a Scanner.
type Config struct {
	// Interval is the delay between fast cycles over assigned pull requests.
	// Defaults to DefaultInterval.
	Interval time.Duration
	// SweepInterval is the delay between full sweeps. Defaults to DefaultSweepInterval.
	SweepInterval time.Duration
	// Workers is the size of the evaluation pool. Defaults to DefaultWorkers.
	Workers int
	// TriggerLabel is the label marking a pull request as the watcher's to work on.
	TriggerLabel string
	// GitHubLogin is the watcher's own account, whose comments are never
	// treated as feedback to act on.
	GitHubLogin string
	// BotUsers is the pool of bot accounts whose pull requests are evaluated.
	// A pull request authored outside the pool is skipped: the watcher cannot
	// push to a fork it does not own.
	BotUsers []string
	// ReviewerLogins are the accounts whose reviews count as automated review
	// feedback rather than as noise.
	ReviewerLogins []string
	// AllowlistedBots are the automated accounts whose comments are acted on.
	AllowlistedBots []string
	// ScanLimit caps the page size of the fast query. Defaults to defaultScanLimit.
	ScanLimit int
	// MinNumber skips pull requests numbered below it. Zero evaluates everything.
	MinNumber int
	// InactivityTimeout pauses a pull request that has seen no human activity
	// for this long. Zero disables the check.
	InactivityTimeout time.Duration
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
	// Queue receives the pull request tasks the scan creates.
	Queue Queue
	// Entities is the shared open PR / open issue cache.
	Entities Entities
	// Sandboxes probes whether a sandbox is already running the work.
	Sandboxes Sandboxes
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

// Scanner evaluates open pull requests and queues the work they need.
type Scanner struct {
	cfg       Config
	gh        *github.Client
	queue     Queue
	entities  Entities
	sandboxes Sandboxes
	paused    func() bool

	// reactions reads the acknowledgement state the watcher records on
	// comments. It is bound once to this scanner's identity and bot allowlist,
	// so no call site has to restate whose marks count as the watcher's.
	reactions *conventions.ReactionInterpreter

	// state records what has already been done for each pull request. Unlike
	// the other subcontrollers' bookkeeping it is mutex-guarded, because the
	// worker pool evaluates several pull requests at once.
	state *stateStore
	// lastSweep is when the full sweep last ran.
	lastSweep time.Time
	// rateLimit holds the scanner off while GitHub is refusing it. Every
	// GitHub error the cycle sees is reported to it, and it is consulted
	// between units of work as well as at the top of a cycle, so that a cycle
	// refused partway through stops rather than running the remaining pull
	// requests at a dozen doomed requests each.
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
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
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
		paused:    deps.Paused,
		reactions: conventions.NewReactionInterpreter(deps.GitHub, cfg.GitHubLogin, cfg.AllowlistedBots),
		state:     newStateStore(deps.Queue),
		rateLimit: ratelimit.New("pull request scanner", cfg.RateLimitBackoff, cfg.MaxRateLimitBackoff),
	}
}

// Run evaluates pull requests until ctx is cancelled, returning nil once it has
// stopped.
//
// A cycle runs immediately so that a restart picks up the pull requests that
// changed while the daemon was down, instead of waiting out a full interval.
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
// report is both necessary and sufficient to decide how long to wait; the
// alternative - a report at each of the forty-odd call sites - made the same
// refusal look like forty and drove the wait to its ceiling.
//
// A failed cycle is not fatal to the daemon. The next one re-lists from scratch
// and covers whatever this one did not reach.
func (s *Scanner) cycle(ctx context.Context) {
	err := s.ScanOnce(ctx)
	s.rateLimit.Observe(err)
	if err != nil {
		klog.Errorf("Pull request scan cycle failed: %v", err)
	}
}

// nextDelay is how long to wait before the next cycle: the scan interval
// normally, and the remainder of a rate limit wait when GitHub has refused us
// for longer than that.
//
// Running the cycle anyway would cost requests that deepen the refusal and
// return listings that cannot be told apart from an empty repository, so the
// cadence gives way to the wait rather than racing it.
func (s *Scanner) nextDelay() time.Duration {
	wait, blocked := s.rateLimit.Blocked()
	if !blocked || wait <= s.cfg.Interval {
		return s.cfg.Interval
	}
	klog.V(2).Infof("Holding the next pull request scan for %s: GitHub is rate limiting us.", wait.Round(time.Second))
	return wait
}

// ScanOnce runs one cycle: a full sweep when one has come due, and otherwise a
// fast pass over the pull requests assigned to the bot pool.
//
// The two are alternatives rather than additions. The sweep's candidate set is
// a superset of the fast pass's, so running both in one cycle would evaluate
// the assigned pull requests twice for nothing.
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
		klog.V(2).Infof("Skipping pull request scan because the watcher is draining.")
		return nil
	}

	if s.lastSweep.IsZero() || time.Since(s.lastSweep) >= s.cfg.SweepInterval {
		return s.sweep(ctx)
	}
	return s.fastPass(ctx)
}

// sweep refreshes the open pull request cache and evaluates every pull request
// the watcher is responsible for.
func (s *Scanner) sweep(ctx context.Context) (err error) {
	klog.Infof("Running full PR scan cycle...")
	previousSweep := s.lastSweep
	s.lastSweep = time.Now()

	// A sweep that ended early did not cover the repository, whatever it
	// managed to list first. Leaving it recorded as the last sweep would spend
	// the next five minutes on fast passes over a candidate set the failure
	// truncated, so the claim is withdrawn and the cycle that follows sweeps
	// again.
	defer func() {
		if err != nil {
			s.lastSweep = previousSweep
		}
	}()

	// Publish the open pull requests first: the issue scanner reads the
	// referenced-issue map derived from them to tell which issues already have
	// a fix in flight, and it should see this cycle's picture, not the last
	// one's. A failed listing leaves the previous copy in place rather than
	// publishing a partial one as though it were complete.
	prs, err := s.gh.ListOpenPRs(ctx)
	if err != nil {
		return fmt.Errorf("listing open PRs: %w", err)
	}
	s.entities.UpdateOpenPRs(prs)

	candidates, err := s.scanCandidates(ctx)
	if err != nil {
		return err
	}
	return s.evaluateAll(ctx, candidates)
}

// fastPass evaluates the pull requests currently assigned to the bot pool,
// which are the ones with work in flight and therefore the ones whose state
// changes between sweeps.
func (s *Scanner) fastPass(ctx context.Context) error {
	candidates, err := s.scanAssigned(ctx)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	klog.Infof("Evaluating %d assigned PRs...", len(candidates))
	return s.evaluateAll(ctx, candidates)
}

// evaluateAll evaluates the candidates on a bounded worker pool. It returns an
// error only when GitHub has started refusing us, and nil once every candidate
// has been given its turn.
//
// The pool is what keeps a pull request whose comment history takes ten seconds
// to page from delaying every pull request behind it. Each candidate appears at
// most once per cycle, so no two workers ever touch the same pull request's
// state.
//
// A candidate that fails for its own reasons - a pull request deleted between
// the listing and the read, a branch whose checks API answers 422 - is logged
// and skipped. Letting one such pull request end the pass would park every
// candidate behind it until someone noticed, and it would keep doing so every
// cycle, because nothing about the next cycle makes that pull request healthier.
//
// A rate limit refusal is different, and does end the pass. Evaluating a pull
// request costs the better part of a dozen requests, so once GitHub has started
// refusing us the candidates still queued would spend that on refusals and
// arrive at the same answer; they are picked up by the cycle after the wait.
func (s *Scanner) evaluateAll(ctx context.Context, candidates []*githubv39.Issue) error {
	if len(candidates) == 0 {
		return nil
	}

	work := make(chan *githubv39.Issue)

	// refused is closed by the first worker GitHub turns away, which is how the
	// feeder learns to stop handing out candidates. Cancelling the context would
	// be the obvious way to say this, but it would also abort the label writes
	// and comments the other workers have in flight, turning one refusal into
	// several half-applied ones.
	refused := make(chan struct{})
	var refuseOnce sync.Once

	// Two workers can be refused at the same moment, so the first refusal needs
	// a lock to record.
	var mu sync.Mutex
	var refusal error

	var wg sync.WaitGroup
	for i := 0; i < s.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for prIssue := range work {
				err := s.evaluate(ctx, prIssue)
				if err == nil {
					continue
				}
				if !github.IsRateLimited(err) {
					klog.Errorf("Failed to evaluate PR #%d: %v", prIssue.GetNumber(), err)
					continue
				}
				mu.Lock()
				if refusal == nil {
					refusal = err
				}
				mu.Unlock()
				refuseOnce.Do(func() { close(refused) })
				return
			}
		}()
	}

feed:
	for _, prIssue := range candidates {
		select {
		case <-ctx.Done():
			// Stop feeding the pool on shutdown; the workers drain what they
			// already took and the next run picks the rest up.
			break feed
		case <-refused:
			break feed
		case work <- prIssue:
		}
	}
	close(work)
	wg.Wait()

	// Every worker has returned, so refusal is settled and needs no lock.
	return refusal
}

// evaluate decides what a single pull request needs and queues it.
//
// The phases are ordered rather than independent: unaddressed feedback wins
// over everything, because a human asking for a change makes the conflict or
// the CI failure the agent would otherwise chase irrelevant. A conflicted
// branch is next, since nothing can be verified until it merges; then CI
// failures; and only a green, unreviewed pull request is reviewed.
//
// Any GitHub failure ends the evaluation there. Every phase below reads the
// repository to decide what the pull request needs, and a phase that could not
// read it cannot tell "nothing to do" from "could not look" - so carrying on
// would queue a decision made on a picture the scanner does not have.
func (s *Scanner) evaluate(ctx context.Context, prIssue *githubv39.Issue) error {
	num := prIssue.GetNumber()
	if s.cfg.MinNumber > 0 && num < s.cfg.MinNumber {
		return nil
	}
	if conventions.HasStopLabel(prIssue.Labels, s.cfg.TriggerLabel) {
		klog.Infof("Skipping PR #%d because it has the stop label ('overseer/stop' or '%s/stop')", num, s.cfg.TriggerLabel)
		if err := s.reconcileReadyForHumanLabel(ctx, num, prIssue, false, ""); err != nil {
			return err
		}
		_ = s.queue.RemovePendingTasksForNumber(num)
		return nil
	}
	pr, err := s.gh.GetPullRequest(ctx, num)
	if err != nil {
		return fmt.Errorf("fetching full PR #%d: %w", num, err)
	}

	// A pull request in the merge queue is out of the watcher's hands: pushing
	// to it now would only knock it back out.
	inMergeQueue, err := s.gh.IsInMergeQueue(ctx, num)
	if err != nil {
		return fmt.Errorf("checking whether PR #%d is in the merge queue: %w", num, err)
	}
	if inMergeQueue {
		klog.Infof("Skipping PR #%d because it is in the merge queue", num)
		_ = s.queue.RemovePendingTasksForNumber(num)
		return nil
	}

	// Only pull requests created by a bot in the pool can be worked on: we do
	// not have permission to push to an external fork.
	author := pr.GetUser().GetLogin()
	isBotPR := false
	for _, bot := range s.cfg.BotUsers {
		if strings.EqualFold(author, bot) {
			isBotPR = true
			break
		}
	}
	if !isBotPR {
		klog.Infof("Skipping PR #%d because it was created by %s (not in our bot pool). We do not have permission to push to external forks.", num, author)
		return nil
	}

	// Sync labels from referenced parent issues to the PR, then re-check: the
	// stop label may have been inherited by the sync we just performed.
	if err := s.syncReferencedIssueLabels(ctx, pr, prIssue); err != nil {
		return err
	}
	if conventions.HasStopLabel(prIssue.Labels, s.cfg.TriggerLabel) {
		klog.Infof("Skipping PR #%d after label sync because it has the stop label ('overseer/stop' or '%s/stop')", num, s.cfg.TriggerLabel)
		if err := s.reconcileReadyForHumanLabel(ctx, num, prIssue, false, ""); err != nil {
			return err
		}
		_ = s.queue.RemovePendingTasksForNumber(num)
		return nil
	}

	headSHA := pr.GetHead().GetSHA()

	history, err := s.fetchHistory(ctx, num)
	if err != nil {
		return fmt.Errorf("fetching history for PR #%d: %w", num, err)
	}

	state := s.state.get(num)

	paused, err := s.pauseIfInactive(ctx, pr, prIssue, history, headSHA)
	if err != nil {
		return err
	}
	if paused {
		return nil
	}

	// Check Phase 1: Rebase/Conflicts
	isConflicting := pr.Mergeable != nil && !*pr.Mergeable

	var checkAnalysis prCheckAnalysis
	var canReview bool

	commentAnalysis, err := s.evaluateComments(ctx, num, pr, history, history.lastCommitTime, state.lastCommentAddressedTime, state.lastCommentAddressedSHA, headSHA)
	if err != nil {
		return err
	}

	if !isConflicting {
		checkAnalysis, err = s.evaluateChecks(ctx, headSHA)
		if err != nil {
			return err
		}
		isApproved := isPRApprovedOrLGTM(pr, prIssue, history.reviews)
		if isApproved {
			klog.V(2).Infof("PR #%d is approved / LGTM'd", num)
		}
		if !checkAnalysis.hasFailure && !checkAnalysis.hasPending && !isApproved && state.lastReviewedSHA != headSHA {
			autoReview, err := s.shouldAutoReviewPR(ctx, pr, prIssue)
			if err != nil {
				return err
			}
			if autoReview {
				canReview = !hasBotReviewAfterLastCommit(history.reviews, history.lastCommitTime, headSHA, s.cfg.GitHubLogin, s.cfg.AllowlistedBots)
			}
		}
	}

	assignedBot := conventions.AssignedBotUser(prIssue, s.cfg.BotUsers)
	isExplicitlyAssigned := assignedBot != ""

	taskAssignee := assignedBot
	if taskAssignee == "" {
		taskAssignee = author
	}

	canInvestigate := !isConflicting && checkAnalysis.hasFailure && s.canInvestigatePR(num, headSHA, isExplicitlyAssigned, state, history.comments, history.lastCommitTime)

	shortSHA := headSHA
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	pc := &prContext{
		pr:                   pr,
		prIssue:              prIssue,
		headSHA:              headSHA,
		shortSHA:             shortSHA,
		lastCommitTime:       history.lastCommitTime,
		taskAssignee:         taskAssignee,
		isExplicitlyAssigned: isExplicitlyAssigned,
		prURL:                fmt.Sprintf("https://github.com/%s/%s/pull/%d", s.gh.Owner(), s.gh.Repo(), num),
	}

	// Top level case statement for handling each type of PR task
	switch {
	case commentAnalysis.hasNewComments:
		if err := s.handlePRComments(ctx, pc, commentAnalysis); err != nil {
			return err
		}

	case isConflicting:
		return s.handlePRIterate(ctx, pc)

	case canInvestigate:
		if err := s.handlePRInvestigate(ctx, pc, checkAnalysis, history.comments); err != nil {
			return err
		}

	case canReview:
		if err := s.handlePRReview(ctx, pc, checkAnalysis.checkRuns); err != nil {
			return err
		}
	}

	return s.reconcileReadiness(ctx, pc, checkAnalysis, commentAnalysis, history, isConflicting, assignedBot)
}

// reconcileReadiness decides whether a pull request is ready for a human and
// applies the consequences: the label, and unassigning the bot that was working
// on it.
//
// Every gate has to hold, including that no task is queued or running for the
// pull request. Reading that from the in-memory queue rather than from disk is
// what stopped the label from flapping while a task file was being renamed
// between directories.
func (s *Scanner) reconcileReadiness(
	ctx context.Context,
	pc *prContext,
	checkAnalysis prCheckAnalysis,
	commentAnalysis prCommentAnalysis,
	history *prHistory,
	isConflicting bool,
	assignedBot string,
) error {
	num := pc.prIssue.GetNumber()

	isReviewRequired, err := s.shouldAutoReviewPR(ctx, pc.pr, pc.prIssue)
	if err != nil {
		return err
	}
	hasBotReviewOnHead := s.hasCompletedBotReviewOnHead(history.reviews, pc.headSHA, history.lastCommitTime)
	reviewSatisfied := !isReviewRequired || hasBotReviewOnHead

	isReadyForHuman := !isConflicting &&
		!checkAnalysis.hasFailure &&
		!checkAnalysis.hasPending &&
		!commentAnalysis.hasNewComments &&
		!s.queue.HasActivePRTask(num) &&
		reviewSatisfied &&
		!conventions.HasStopLabel(pc.prIssue.Labels, s.cfg.TriggerLabel) &&
		!pc.pr.GetDraft() &&
		pc.pr.GetState() == "open"

	if err := s.reconcileReadyForHumanLabel(ctx, num, pc.prIssue, isReadyForHuman, pc.headSHA); err != nil {
		return err
	}

	if isReadyForHuman && assignedBot != "" {
		if s.cfg.DryRun {
			fmt.Printf("[DRYRUN] Would unassign bot %s from PR #%d (ready for human review)\n", assignedBot, num)
		} else {
			fmt.Printf("Unassigning bot %s from PR #%d (ready for human review)...\n", assignedBot, num)
			if err := s.gh.RemoveAssignees(ctx, num, []string{assignedBot}); err != nil {
				return fmt.Errorf("unassigning bot %s from PR #%d: %w", assignedBot, num, err)
			}
		}
	}
	return nil
}

// comment posts a comment on a pull request.
//
// A failure is returned rather than logged. Nothing else the watcher does says
// out loud what it has decided, so a cycle that could not comment has not done
// the half of its job the humans in the thread can see.
func (s *Scanner) comment(ctx context.Context, num int, body string) error {
	if err := s.gh.AddComment(ctx, num, body); err != nil {
		return fmt.Errorf("creating GitHub comment on #%d: %w", num, err)
	}
	return nil
}

// react records a reaction on a conversation comment.
//
// Reactions are how the watcher remembers what it has picked up - they are the
// only record that survives a restart - so a reaction that did not land would
// have the next cycle pick the same comment up again.
func (s *Scanner) react(ctx context.Context, commentID int64, content conventions.Reaction) error {
	if err := s.gh.AddIssueCommentReaction(ctx, commentID, string(content)); err != nil {
		return fmt.Errorf("creating reaction %q on comment %d: %w", content, commentID, err)
	}
	return nil
}

// reactToReviewComment records a reaction on an inline review comment.
func (s *Scanner) reactToReviewComment(ctx context.Context, commentID int64, content conventions.Reaction) error {
	if err := s.gh.AddPullRequestCommentReaction(ctx, commentID, string(content)); err != nil {
		return fmt.Errorf("creating reaction %q on PR review comment %d: %w", content, commentID, err)
	}
	return nil
}

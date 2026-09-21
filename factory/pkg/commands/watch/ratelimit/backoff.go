// Package ratelimit paces the watch subcontrollers against GitHub's rate
// limits.
//
// GitHub refuses a client that has spent its quota, and keeps refusing it for
// as long as the window has left to run - up to an hour for the primary limit.
// A scanner that keeps to its normal cadence through that window spends every
// cycle collecting the same refusal, and each refused cycle makes the next one
// likelier to be refused too, because the requests it did get through are
// counted against the quota either way.
//
// Worse than the waste is what a refusal looks like downstream. A listing that
// failed is empty, and an empty listing is indistinguishable from a repository
// with nothing to do, so a scanner that carries on through a rate limit
// reasons about a blank picture of state it could not read. The Backoff here is
// what makes a refusal cost one cycle instead of all of them.
package ratelimit

import (
	"math/rand/v2"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
)

const (
	// DefaultBase is the wait earned by the first refused cycle.
	DefaultBase = 1 * time.Minute
	// DefaultMax caps the doubling. GitHub's primary window is an hour, and a
	// refusal that carries its own reset time is honoured beyond this cap
	// anyway, so the cap only bounds the blind guessing: half an hour is long
	// enough to stop a scanner hammering a closed door, and short enough that a
	// quota which recovered early is not left unused.
	DefaultMax = 30 * time.Minute

	// jitterFraction is how much of a wait is randomised. Every subcontroller
	// shares one quota and is therefore refused at the same moment, so without
	// it they would all resume in lockstep and race each other into the next
	// refusal.
	jitterFraction = 10
)

// Backoff decides whether a scan cycle may run, and grows the wait
// exponentially for each consecutive cycle GitHub refuses.
//
// It is safe for concurrent use: the pull request scanner evaluates on a worker
// pool, so several goroutines report refusals from one cycle.
type Backoff struct {
	// name identifies the holder in the log line a new wait produces.
	name string
	// base is the first wait, doubled for each consecutive refused cycle.
	base time.Duration
	// max caps that doubling.
	max time.Duration

	mu sync.Mutex
	// attempts counts consecutive refused cycles, which is what the doubling
	// is indexed by. A refusal arriving while a wait is already running does
	// not add to it: a cycle makes hundreds of requests, and counting each one
	// would jump straight to the cap on the first refusal.
	attempts int
	// until is when the current wait expires. The zero time means none is.
	until time.Time

	// now and jitter are fields rather than direct calls so that the tests can
	// pin both and assert on exact waits.
	now    func() time.Time
	jitter func(time.Duration) time.Duration
}

// New returns a Backoff that starts at base and doubles up to maxDelay. A
// non-positive bound is replaced by its default, and a maxDelay below base is
// raised to it, so that a misconfiguration cannot produce a backoff that never
// grows.
//
// name identifies the holder in logs; it reads best as the subject of "GitHub
// is rate limiting the ...", e.g. "pull request scanner".
func New(name string, base, maxDelay time.Duration) *Backoff {
	if base <= 0 {
		base = DefaultBase
	}
	if maxDelay <= 0 {
		maxDelay = DefaultMax
	}
	if maxDelay < base {
		maxDelay = base
	}
	return &Backoff{
		name:   name,
		base:   base,
		max:    maxDelay,
		now:    time.Now,
		jitter: defaultJitter,
	}
}

// Blocked reports whether the caller must hold off, and how much of the wait is
// left. A nil Backoff never blocks, so a component constructed without one
// simply runs at its normal cadence.
func (b *Backoff) Blocked() (time.Duration, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	remaining := b.until.Sub(b.now())
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// Observe records the outcome of a GitHub request. A rate limit refusal starts
// or extends the wait; anything else - including success, which callers report
// by passing a nil error - ends it.
//
// Ending the wait is deliberately conditional on the wait having elapsed. A
// cycle refused partway through keeps making requests until its workers notice,
// and some of those succeed, because the quotas GitHub meters are per-endpoint
// rather than one number. Letting any of them cancel the wait would defeat it
// in the cycle that earned it.
func (b *Backoff) Observe(err error) {
	if b == nil {
		return
	}
	retryAfter, limited := github.RetryAfter(err)

	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()

	if !limited {
		if !now.Before(b.until) {
			b.attempts = 0
			b.until = time.Time{}
		}
		return
	}

	if now.Before(b.until) {
		// Already waiting on this refusal. A longer wait than the one we chose
		// is still worth honouring - it came from GitHub - but this is not a
		// second consecutive refusal and must not be counted as one.
		if until := now.Add(retryAfter); until.After(b.until) {
			b.until = until
		}
		return
	}

	b.attempts++
	wait := b.wait(b.attempts, retryAfter)
	b.until = now.Add(wait)
	klog.Warningf("GitHub is rate limiting the %s (%d consecutive refusals); holding off for %s.", b.name, b.attempts, wait.Round(time.Second))
}

// wait is the delay a fresh refusal earns: the exponential step for the number
// of consecutive refusals, never shorter than what GitHub itself asked for.
//
// retryAfter is deliberately not capped by max. The cap exists to stop blind
// doubling from parking a scanner for hours; a wait GitHub named is not a
// guess, and coming back before it has passed earns nothing but another
// refusal.
func (b *Backoff) wait(attempts int, retryAfter time.Duration) time.Duration {
	step := b.max
	// The shift is bounded before it is taken: base << 63 is not a long wait,
	// it is a negative one.
	if shift := attempts - 1; shift >= 0 && shift < 62 {
		if doubled := b.base << shift; doubled > 0 && doubled < b.max {
			step = doubled
		}
	}
	return max(step, retryAfter) + b.jitter(step)
}

// defaultJitter spreads resumption over a tenth of the wait.
func defaultJitter(d time.Duration) time.Duration {
	return rand.N(d/jitterFraction + 1)
}

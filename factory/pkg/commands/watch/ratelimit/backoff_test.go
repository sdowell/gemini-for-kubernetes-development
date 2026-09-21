package ratelimit

import (
	"errors"
	"net/http"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

// fakeClock stands in for time.Now so that a test can assert on exact waits
// instead of on tolerances around a real one.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestBackoff returns a Backoff on a pinned clock and with jitter disabled,
// which is what makes the expected waits exact.
func newTestBackoff(t *testing.T, base, maxDelay time.Duration) (*Backoff, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := New("test scanner", base, maxDelay)
	b.now = clock.now
	b.jitter = func(time.Duration) time.Duration { return 0 }
	return b, clock
}

// refusal is a rate limit refusal that names no wait of its own, leaving the
// backoff to choose one.
func refusal() error {
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r/issues", nil)
	return &githubv39.AbuseRateLimitError{
		Response: &http.Response{StatusCode: http.StatusForbidden, Request: req},
		Message:  "You have exceeded a secondary rate limit",
	}
}

// refusalRetryAfter is a refusal that names the wait GitHub wants.
func refusalRetryAfter(d time.Duration) error {
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r/issues", nil)
	return &githubv39.AbuseRateLimitError{
		Response:   &http.Response{StatusCode: http.StatusForbidden, Request: req},
		Message:    "You have exceeded a secondary rate limit",
		RetryAfter: &d,
	}
}

// blockedFor returns the remaining wait, or -1 when nothing is blocking, so
// that "not blocked" cannot be confused with "blocked for zero".
func blockedFor(b *Backoff) time.Duration {
	remaining, blocked := b.Blocked()
	if !blocked {
		return -1
	}
	return remaining
}

func TestBackoff_DoublesPerConsecutiveRefusal(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, time.Hour)

	if got := blockedFor(b); got != -1 {
		t.Fatalf("a fresh backoff blocked for %v, want not blocked", got)
	}

	// Each refusal is a cycle of its own: the wait is served out before the
	// next one arrives, which is what makes them consecutive.
	for i, want := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute} {
		b.Observe(refusal())
		if got := blockedFor(b); got != want {
			t.Errorf("wait after refusal %d = %v, want %v", i+1, got, want)
		}
		clock.advance(want)
	}
}

func TestBackoff_CapsTheDoubling(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, 4*time.Minute)

	for i := 0; i < 6; i++ {
		b.Observe(refusal())
		clock.advance(blockedFor(b))
	}

	b.Observe(refusal())
	if got := blockedFor(b); got != 4*time.Minute {
		t.Errorf("wait after seven refusals = %v, want the 4m cap", got)
	}
}

// TestBackoff_HonoursRetryAfterBeyondTheCap pins the one case the cap gives way
// to: a wait GitHub named is not a guess, and coming back inside it earns
// nothing but another refusal.
func TestBackoff_HonoursRetryAfterBeyondTheCap(t *testing.T) {
	b, _ := newTestBackoff(t, time.Minute, 5*time.Minute)

	b.Observe(refusalRetryAfter(45 * time.Minute))

	if got := blockedFor(b); got != 45*time.Minute {
		t.Errorf("wait = %v, want the 45m GitHub asked for despite the 5m cap", got)
	}
}

func TestBackoff_ClearsOnceTheWaitHasElapsed(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, time.Hour)

	b.Observe(refusal())
	clock.advance(time.Minute)
	if got := blockedFor(b); got != -1 {
		t.Fatalf("blocked for %v after the wait elapsed, want not blocked", got)
	}

	// The cycle that follows succeeds, so the next refusal starts again from
	// the base rather than continuing the doubling.
	b.Observe(nil)
	b.Observe(refusal())
	if got := blockedFor(b); got != time.Minute {
		t.Errorf("wait after a successful cycle then a refusal = %v, want the base %v", got, time.Minute)
	}
}

// TestBackoff_SuccessDuringTheWaitDoesNotClearIt covers the ordering a cycle
// actually produces: the workers that were already in flight when the refusal
// landed keep returning, and some of them succeed against a quota that is
// metered separately. None of that means the refusal is over.
func TestBackoff_SuccessDuringTheWaitDoesNotClearIt(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, time.Hour)

	b.Observe(refusal())
	clock.advance(10 * time.Second)
	b.Observe(nil)
	b.Observe(errors.New("not found"))

	if got := blockedFor(b); got != 50*time.Second {
		t.Errorf("remaining wait = %v, want 50s: a success mid-wait must not cancel it", got)
	}
}

// TestBackoff_RefusalsWithinOneWaitAreOneRefusal covers what a cycle does to a
// naive counter: the pull request scanner evaluates on a worker pool, and a
// rate limited cycle can report the same refusal dozens of times. Counting each
// would reach the cap on what is really the first failure.
func TestBackoff_RefusalsWithinOneWaitAreOneRefusal(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, time.Hour)

	for i := 0; i < 20; i++ {
		b.Observe(refusal())
	}
	if got := blockedFor(b); got != time.Minute {
		t.Fatalf("wait after 20 refusals in one cycle = %v, want the base %v", got, time.Minute)
	}

	// The next cycle is the second consecutive one to be refused, so now it
	// doubles.
	clock.advance(time.Minute)
	b.Observe(refusal())
	if got := blockedFor(b); got != 2*time.Minute {
		t.Errorf("wait after the second refused cycle = %v, want %v", got, 2*time.Minute)
	}
}

// TestBackoff_ExtendsToALongerRetryAfterMidWait checks that a refusal arriving
// under an existing wait can still lengthen it, which is how a secondary limit
// discovered late in a cycle is honoured.
func TestBackoff_ExtendsToALongerRetryAfterMidWait(t *testing.T) {
	b, clock := newTestBackoff(t, time.Minute, time.Hour)

	b.Observe(refusal())
	clock.advance(10 * time.Second)
	b.Observe(refusalRetryAfter(5 * time.Minute))

	if got := blockedFor(b); got != 5*time.Minute {
		t.Errorf("remaining wait = %v, want 5m", got)
	}
}

// TestBackoff_NilIsInert keeps a component constructed without a backoff - a
// test double, or a caller that has not adopted one - running at its normal
// cadence rather than panicking.
func TestBackoff_NilIsInert(t *testing.T) {
	var b *Backoff

	b.Observe(refusal())

	if _, blocked := b.Blocked(); blocked {
		t.Error("a nil Backoff reported itself blocked, want never blocked")
	}
}

// TestNew_RepairsImpossibleBounds checks that a misconfiguration cannot produce
// a backoff that never grows or never waits.
func TestNew_RepairsImpossibleBounds(t *testing.T) {
	b := New("test scanner", 0, 0)
	if b.base != DefaultBase || b.max != DefaultMax {
		t.Errorf("New(0, 0) = base %v, max %v; want the defaults %v and %v", b.base, b.max, DefaultBase, DefaultMax)
	}

	b = New("test scanner", 10*time.Minute, time.Minute)
	if b.max != b.base {
		t.Errorf("New() with a cap below the base = max %v, want it raised to the base %v", b.max, b.base)
	}
}

package github

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

const (
	// headerRateRemaining carries how many requests are left in the current
	// quota window. go-github does not export it, and a 403 that reports zero
	// here is what distinguishes an exhausted quota from a denied permission.
	headerRateRemaining = "X-RateLimit-Remaining"
	// headerRateReset carries the instant the quota window refills, as Unix
	// seconds.
	headerRateReset = "X-RateLimit-Reset"
	// headerRetryAfter carries how long GitHub wants the client to wait. It
	// accompanies secondary rate limits, which have no window to reset.
	headerRetryAfter = "Retry-After"
)

// secondaryRateLimitPhrases are how GitHub names a secondary rate limit in the
// body of a refusal. They are matched only on the statuses a rate limit can
// arrive as, so an unrelated message quoting one of them cannot be mistaken for
// a refusal.
var secondaryRateLimitPhrases = []string{
	"secondary rate limit",
	"abuse detection mechanism",
}

// IsRateLimited reports whether err is GitHub refusing a request because a rate
// limit is exhausted, rather than because the request itself was unacceptable.
//
// This is a question about the response status and the quota headers, never
// about the text of the error, which is what makes it safe to act on.
func IsRateLimited(err error) bool {
	_, limited := RetryAfter(err)
	return limited
}

// RetryAfter reports whether err is a rate limit refusal, and how long GitHub
// asked the caller to wait before trying again.
//
// A zero duration alongside limited=true means GitHub refused the request
// without saying for how long - which is the common case for a secondary limit
// - so callers must apply a wait of their own rather than read it as "retry
// immediately". When the duration is non-zero it is a floor: GitHub documents
// that a client which retries inside the window it was given simply earns
// another refusal, and having its requests counted against it.
//
// The three shapes exist because a rate limit reaches this process by three
// routes. The pinned go-github types the primary hourly quota and the
// secondary limit it still calls "abuse", but it predates GitHub answering
// secondary limits with 429, which therefore arrives as an untyped response.
func RetryAfter(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}

	// The primary hourly quota. go-github raises this both from a refusal and,
	// once it has seen one, from its own pre-flight check without spending a
	// request - so by the time we look the reset may already have passed.
	var rateErr *githubv39.RateLimitError
	if errors.As(err, &rateErr) {
		return nonNegative(time.Until(rateErr.Rate.Reset.Time)), true
	}

	// A secondary limit, under the name go-github gave it. GitHub supplies a
	// Retry-After with some of them and nothing at all with others.
	var abuseErr *githubv39.AbuseRateLimitError
	if errors.As(err, &abuseErr) {
		if abuseErr.RetryAfter != nil {
			return nonNegative(*abuseErr.RetryAfter), true
		}
		return 0, true
	}

	// Everything else arrives as a plain error response, which is both how a
	// 429 reaches us and how a 403 does when go-github did not recognise it.
	var respErr *githubv39.ErrorResponse
	if errors.As(err, &respErr) && respErr.Response != nil {
		if !isRateLimitResponse(respErr) {
			return 0, false
		}
		return retryAfterHeaders(respErr.Response.Header), true
	}

	return 0, false
}

// isRateLimitResponse reports whether an untyped error response is a rate limit
// refusal.
func isRateLimitResponse(respErr *githubv39.ErrorResponse) bool {
	switch respErr.Response.StatusCode {
	case http.StatusTooManyRequests:
		// GitHub only answers 429 when it is throttling.
		return true
	case http.StatusForbidden:
		// A 403 is also how GitHub refuses a request for want of permission,
		// which must not park the caller for half an hour. An exhausted quota
		// says so in the headers; a secondary limit says so in the body.
		if respErr.Response.Header.Get(headerRateRemaining) == "0" {
			return true
		}
		return mentionsSecondaryRateLimit(respErr.Message)
	default:
		return false
	}
}

// mentionsSecondaryRateLimit reports whether a refusal names a secondary limit.
func mentionsSecondaryRateLimit(message string) bool {
	lowered := strings.ToLower(message)
	for _, phrase := range secondaryRateLimitPhrases {
		if strings.Contains(lowered, phrase) {
			return true
		}
	}
	return false
}

// retryAfterHeaders reads the wait GitHub asked for, preferring the explicit
// Retry-After over the quota reset: a secondary limit carries the former and
// clears long before the hourly window the latter describes.
//
// An unparseable or absent value is reported as no wait at all rather than
// guessed at, leaving the caller's own backoff to decide.
func retryAfterHeaders(header http.Header) time.Duration {
	if v := header.Get(headerRetryAfter); v != "" {
		if seconds, err := strconv.ParseInt(v, 10, 64); err == nil {
			return nonNegative(time.Duration(seconds) * time.Second)
		}
	}
	if v := header.Get(headerRateReset); v != "" {
		if epoch, err := strconv.ParseInt(v, 10, 64); err == nil {
			return nonNegative(time.Until(time.Unix(epoch, 0)))
		}
	}
	return 0
}

// nonNegative clamps a wait that has already elapsed to zero, so that callers
// can add it to the current time without moving it backwards.
func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

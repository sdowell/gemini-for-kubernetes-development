package github

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	githubv39 "github.com/google/go-github/v39/github"
)

// headers builds a canonicalised header set from key/value pairs. Set is used
// rather than a literal map because Header.Get canonicalises the key it looks
// up, and the spellings GitHub documents are not in canonical form.
func headers(pairs ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set(pairs[i], pairs[i+1])
	}
	return h
}

// ghResponse builds the HTTP response a GitHub error carries. The request is
// populated because the go-github error types format it in Error().
func ghResponse(status int, header http.Header) *http.Response {
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r/issues", nil)
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Request: req}
}

// TestRetryAfter covers the three shapes a rate limit refusal reaches this
// process as, and - the point of the exercise - the refusals it must not be
// confused with. A 403 is how GitHub denies permission as well as how it
// reports an exhausted quota, and treating the former as the latter would park
// a scanner for half an hour over a token that will never gain the scope.
func TestRetryAfter(t *testing.T) {
	retryAfter := 30 * time.Second

	testCases := []struct {
		name string
		err  error
		// wantLimited is whether the error is a rate limit refusal.
		wantLimited bool
		// wantWait is the wait expected, within a second of tolerance for the
		// cases computed against the clock.
		wantWait time.Duration
	}{
		{
			name:        "no error",
			err:         nil,
			wantLimited: false,
		},
		{
			name:        "unrelated error",
			err:         errors.New("connection reset"),
			wantLimited: false,
		},
		{
			name: "primary quota reports its reset",
			err: &githubv39.RateLimitError{
				Rate:     githubv39.Rate{Reset: githubv39.Timestamp{Time: time.Now().Add(10 * time.Minute)}},
				Response: ghResponse(http.StatusForbidden, nil),
			},
			wantLimited: true,
			wantWait:    10 * time.Minute,
		},
		{
			name: "primary quota whose reset has passed asks for no wait",
			err: &githubv39.RateLimitError{
				Rate:     githubv39.Rate{Reset: githubv39.Timestamp{Time: time.Now().Add(-time.Minute)}},
				Response: ghResponse(http.StatusForbidden, nil),
			},
			wantLimited: true,
			wantWait:    0,
		},
		{
			name: "secondary limit with a Retry-After",
			err: &githubv39.AbuseRateLimitError{
				Response:   ghResponse(http.StatusForbidden, nil),
				RetryAfter: &retryAfter,
			},
			wantLimited: true,
			wantWait:    30 * time.Second,
		},
		{
			name: "secondary limit without one",
			err: &githubv39.AbuseRateLimitError{
				Response: ghResponse(http.StatusForbidden, nil),
			},
			wantLimited: true,
			wantWait:    0,
		},
		{
			name: "429 with a Retry-After",
			err: &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusTooManyRequests, headers(headerRetryAfter, "60")),
				Message:  "too many requests",
			},
			wantLimited: true,
			wantWait:    60 * time.Second,
		},
		{
			name: "403 with the quota exhausted falls back to the reset header",
			err: &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusForbidden, headers(
					headerRateRemaining, "0",
					headerRateReset, fmt.Sprint(time.Now().Add(5*time.Minute).Unix()),
				)),
				Message: "API rate limit exceeded",
			},
			wantLimited: true,
			wantWait:    5 * time.Minute,
		},
		{
			name: "403 naming a secondary limit in the body",
			err: &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusForbidden, nil),
				Message:  "You have exceeded a secondary rate limit. Please wait a few minutes before you try again.",
			},
			wantLimited: true,
			wantWait:    0,
		},
		{
			name: "403 denying permission is not a rate limit",
			err: &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusForbidden, headers(headerRateRemaining, "4821")),
				Message:  "Resource not accessible by integration",
			},
			wantLimited: false,
		},
		{
			name: "404 is not a rate limit",
			err: &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusNotFound, nil),
				Message:  "Not Found",
			},
			wantLimited: false,
		},
		{
			// Every helper on Client wraps what it returns, so detection that
			// only worked on the bare error would never fire in production.
			name: "a wrapped refusal is still a refusal",
			err: fmt.Errorf("listing issue comments: %w", &githubv39.ErrorResponse{
				Response: ghResponse(http.StatusTooManyRequests, nil),
				Message:  "too many requests",
			}),
			wantLimited: true,
			wantWait:    0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wait, limited := RetryAfter(tc.err)

			if limited != tc.wantLimited {
				t.Fatalf("RetryAfter() limited = %v, want %v", limited, tc.wantLimited)
			}
			if limited != IsRateLimited(tc.err) {
				t.Errorf("IsRateLimited() = %v, want %v (it must agree with RetryAfter)", IsRateLimited(tc.err), limited)
			}
			if diff := wait - tc.wantWait; diff > time.Second || diff < -time.Second {
				t.Errorf("RetryAfter() wait = %v, want %v (within a second)", wait, tc.wantWait)
			}
		})
	}
}

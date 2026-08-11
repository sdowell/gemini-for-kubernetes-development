package watch

import (
	"reflect"
	"testing"
)

func TestExtractReviewInstructions(t *testing.T) {
	tests := []struct {
		name     string
		bodies   []string
		expected []string
	}{
		{
			name: "single body with h2 instructions",
			bodies: []string{
				`## Summary
Some PR summary here.

## Review Instructions
- Focus on concurrency in queue.go
- Ensure memory leaks are avoided
* Check test coverage

## Next Steps
All good.`,
			},
			expected: []string{
				"Focus on concurrency in queue.go",
				"Ensure memory leaks are avoided",
				"Check test coverage",
			},
		},
		{
			name: "fallback to secondary body",
			bodies: []string{
				"No instructions here",
				`# Review Instructions
1. First step
2. Second step`,
			},
			expected: []string{
				"First step",
				"Second step",
			},
		},
		{
			name: "empty or missing instructions",
			bodies: []string{
				"Just regular text",
				"",
			},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractReviewInstructions(tc.bodies...)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("ExtractReviewInstructions() = %v, want %v", got, tc.expected)
			}
		})
	}
}

package watch

import (
	"regexp"
	"strings"
)

var (
	reviewHeaderRe = regexp.MustCompile(`(?i)^(#{1,6})\s+Review\s+Instructions\s*$`)
	listPrefixRe   = regexp.MustCompile(`^(?:[-*]|\d+\.)\s+`)
)

// ExtractReviewInstructions parses markdown bodies (e.g. PR description, parent Issue body)
// and returns all lines under a "#/## Review Instructions" section.
func ExtractReviewInstructions(bodies ...string) []string {
	for _, body := range bodies {
		instructions := parseReviewInstructionsSection(body)
		if len(instructions) > 0 {
			return instructions
		}
	}
	return nil
}

func parseReviewInstructionsSection(body string) []string {
	if strings.TrimSpace(body) == "" {
		return nil
	}

	lines := strings.Split(body, "\n")
	var instructions []string
	inSection := false
	sectionLevel := 0

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)

		if !inSection {
			if m := reviewHeaderRe.FindStringSubmatch(line); m != nil {
				inSection = true
				sectionLevel = len(m[1])
			}
			continue
		}

		if strings.HasPrefix(line, "#") {
			hashes := 0
			for _, ch := range line {
				if ch == '#' {
					hashes++
				} else {
					break
				}
			}
			if hashes <= sectionLevel && len(line) > hashes && (line[hashes] == ' ' || line[hashes] == '\t') {
				break
			}
		}

		if line == "" {
			continue
		}

		cleanLine := listPrefixRe.ReplaceAllString(line, "")
		cleanLine = strings.TrimSpace(cleanLine)
		if cleanLine != "" {
			instructions = append(instructions, cleanLine)
		}
	}

	return instructions
}

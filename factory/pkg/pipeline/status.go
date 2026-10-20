package pipeline

import (
	"fmt"
	"strings"
	"time"
)

// RenderStatus renders the parent's status comment (design §8) from the
// input and the result alone, with no timestamps of its own, so it changes
// only when something real does. extra are warnings the executor adds, such
// as writes that did not take.
func RenderStatus(in Input, r Result, extra []string) string {
	var b strings.Builder
	b.WriteString(StatusMarker + "\n")
	if r.Complete {
		b.WriteString(CompleteMarker + "\n")
	}

	name := "?"
	if in.Request != nil {
		name = in.Request.Pipeline
	}
	fmt.Fprintf(&b, "### Pipeline `%s`", name)
	if in.Definition != nil && in.Request != nil && len(in.Definition.Spec.Params) > 0 {
		if v := in.Request.Params[in.Definition.Spec.Params[0].Name]; v != "" {
			fmt.Fprintf(&b, " — %s", cell(v))
		}
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "**Phase:** %s", r.Phase)
	if cur := currentStep(r.Steps); cur >= 0 && (r.Phase == PhaseRunning || r.Phase == PhasePaused || r.Phase == PhaseBlocked) {
		fmt.Fprintf(&b, " · **Current step:** %d/%d `%s`", cur+1, len(r.Steps), r.Steps[cur].ID)
	}
	b.WriteString("\n")
	if r.Reason != "" {
		fmt.Fprintf(&b, "\n%s: %s\n", r.Phase, oneLine(r.Reason))
	}

	if len(r.Steps) > 0 {
		b.WriteString("\n| # | Step | Issue | State | Started | Completed |\n|---|------|-------|-------|---------|-----------|\n")
		for i, s := range r.Steps {
			issue := "—"
			if s.Issue > 0 {
				issue = fmt.Sprintf("#%d", s.Issue)
			}
			b.WriteString(row(fmt.Sprint(i+1), s.ID, issue, stateCell(s.State), date(s.Started), date(s.Completed)))
		}
	}

	warnings := append(append([]string{}, r.Warnings...), extra...)
	if len(warnings) > 0 {
		b.WriteString("\n")
		for _, w := range warnings {
			fmt.Fprintf(&b, "> ⚠️ %s\n", oneLine(w))
		}
	}

	if in.DefinitionRef != "" {
		fmt.Fprintf(&b, "\n<sub>definition: %s</sub>\n", in.DefinitionRef)
	}
	return b.String()
}

// currentStep is the index of the first active step, else of the first
// pending one, else -1.
func currentStep(steps []StepStatus) int {
	for i, s := range steps {
		if s.State == StepActive {
			return i
		}
	}
	for i, s := range steps {
		if s.State == StepPending {
			return i
		}
	}
	return -1
}

func stateCell(s StepState) string {
	switch s {
	case StepSucceeded:
		return "✅ Succeeded"
	case StepActive:
		return "🔄 Active"
	case StepSkipped:
		return "⏭️ Skipped"
	}
	return "⏳ Pending"
}

// row renders a table row; an empty cell is "| |".
func row(cells ...string) string {
	var b strings.Builder
	b.WriteString("|")
	for _, c := range cells {
		if c == "" {
			b.WriteString(" |")
		} else {
			b.WriteString(" " + c + " |")
		}
	}
	b.WriteString("\n")
	return b.String()
}

func date(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02")
}

// cell keeps a value from breaking the table or the heading.
func cell(s string) string { return strings.ReplaceAll(oneLine(s), "|", `\|`) }

// oneLine joins a multi-line message, such as joined validation errors.
func oneLine(s string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(s, "\n", "; ")), " ")
}

// SameText reports whether a comment already says body. GitHub hands bodies
// back with CRLFs and trailing whitespace not reliably kept, so both are
// compared normalised.
func SameText(a, b string) bool {
	norm := func(s string) string { return strings.TrimSpace(strings.ReplaceAll(s, "\r\n", "\n")) }
	return norm(a) == norm(b)
}

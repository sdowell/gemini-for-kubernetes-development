package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const threeSteps = `apiVersion: factory.gemini.google.com/v1alpha1
kind: IssuePipeline
metadata:
  name: demo
spec:
  params:
    - name: kind
      pattern: '^[A-Z][A-Za-z]+$'
      example: Foo
    - name: version
      required: false
      enum: [v1alpha1, v1beta1]
      default: v1alpha1
  defaults:
    labels: [greenfield]
  steps:
    - id: one
      title: 'One {{ .Params.kind }}'
      body: 'Do one at {{ .Params.version }}.'
      labels: [step/one]
      review: 'Review one.'
    - id: two
      title: 'Two {{ .Params.kind }}'
      body: 'After #{{ (step "one").Issue }}.'
    - id: three
      title: 'Three {{ .Params.kind }}'
      assignees: [someone]
      completion:
        closeOnLinkedPRMerge: false
`

var t0 = time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)

const parentN = 100

type fixture struct {
	in Input
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	req := &Request{Pipeline: "demo", Params: map[string]string{"kind": "Foo"}}
	return &fixture{in: Input{
		Repo:           "owner/repo",
		Parent:         Issue{Number: parentN, Open: true, Labels: []string{"factory", "factory/pipeline"}},
		Request:        req,
		Definition:     mustParse(t, threeSteps),
		DefinitionRef:  ".agents/pipelines/demo.yaml@abc1234",
		TriggerLabel:   "factory",
		TargetAssignee: "bot",
	}}
}

// child adds a linked child for step, in the given state ("open",
// "completed", "not_planned", "duplicate").
func (f *fixture) child(n int, step, state string, mods ...func(*Child)) *fixture {
	c := Child{
		Issue: Issue{
			Number:    n,
			Open:      state == "open",
			Labels:    []string{"greenfield", "factory"},
			Assignees: []string{"bot"},
			CreatedAt: t0.Add(time.Duration(n) * time.Hour),
		},
		Marker: Marker{Parent: parentN, Pipeline: "demo", Step: step, Params: ParamsHash(f.in.Request.Params)},
		Linked: true,
	}
	if state != "open" {
		c.StateReason = state
		c.ClosedAt = t0.Add(time.Duration(n)*time.Hour + 30*time.Minute)
	}
	if step == "one" {
		c.Labels = append(c.Labels, "step/one", "factory/review")
	}
	for _, m := range mods {
		m(&c)
	}
	f.in.Children = append(f.in.Children, c)
	return f
}

func kinds(r Result) string {
	var ks []string
	for _, a := range r.Actions {
		ks = append(ks, fmt.Sprintf("%s#%d", a.Kind, a.Issue))
	}
	return strings.Join(ks, " ")
}

func states(r Result) string {
	var ss []string
	for _, s := range r.Steps {
		ss = append(ss, fmt.Sprintf("%s=%s", s.ID, s.State))
	}
	return strings.Join(ss, " ")
}

func TestReconcile(t *testing.T) {
	for _, tc := range []struct {
		name        string
		setup       func(f *fixture)
		phase       Phase
		states      string
		actions     string
		check       func(t *testing.T, r Result)
		wantWarning string
	}{
		{
			name:    "fresh parent creates the first step",
			setup:   func(f *fixture) {},
			phase:   PhaseRunning,
			states:  "one=Active two=Pending three=Pending",
			actions: "create-child#100",
			check: func(t *testing.T, r Result) {
				a := r.Actions[0]
				if a.Step != "one" || a.Title != "One Foo" {
					t.Errorf("created %+v", a)
				}
				if !strings.Contains(a.Body, "Do one at v1alpha1.") || !strings.Contains(a.Body, "## Review Instructions\n\nReview one.") {
					t.Errorf("body:\n%s", a.Body)
				}
				if strings.Contains(a.Body, "#100") {
					t.Errorf("child body mentions the parent:\n%s", a.Body)
				}
				m, ok := ParseMarker(a.Body)
				if !ok || m.Parent != parentN || m.Step != "one" || m.Pipeline != "demo" {
					t.Errorf("marker %+v %v", m, ok)
				}
				if got := strings.Join(a.Labels, ","); got != "greenfield,step/one,factory,factory/review" {
					t.Errorf("labels %s", got)
				}
				if got := strings.Join(a.Assignees, ","); got != "bot" {
					t.Errorf("assignees %s", got)
				}
			},
		},
		{
			name:    "active step waits",
			setup:   func(f *fixture) { f.child(1, "one", "open") },
			phase:   PhaseRunning,
			states:  "one=Active two=Pending three=Pending",
			actions: "",
		},
		{
			name:    "closed step opens the next, which references it",
			setup:   func(f *fixture) { f.child(1, "one", "completed") },
			phase:   PhaseRunning,
			states:  "one=Succeeded two=Active three=Pending",
			actions: "create-child#100",
			check: func(t *testing.T, r Result) {
				if !strings.Contains(r.Actions[0].Body, "After #1.") {
					t.Errorf("body %q", r.Actions[0].Body)
				}
				for _, l := range r.Actions[0].Labels {
					if l == "factory/review" {
						t.Error("a step without review is labelled for review")
					}
				}
			},
		},
		{
			name:    "not planned is skipped and the pipeline moves on",
			setup:   func(f *fixture) { f.child(1, "one", "not_planned") },
			phase:   PhaseRunning,
			states:  "one=Skipped two=Active three=Pending",
			actions: "create-child#100",
		},
		{
			name:    "closed as duplicate is skipped",
			setup:   func(f *fixture) { f.child(1, "one", "completed").child(2, "two", "duplicate") },
			phase:   PhaseRunning,
			states:  "one=Succeeded two=Skipped three=Active",
			actions: "create-child#100",
			check: func(t *testing.T, r Result) {
				if got := strings.Join(r.Actions[0].Assignees, ","); got != "someone" {
					t.Errorf("step assignees %s", got)
				}
			},
		},
		{
			name: "every step done closes the parent once",
			setup: func(f *fixture) {
				f.child(1, "one", "completed").child(2, "two", "not_planned").child(3, "three", "completed")
			},
			phase:   PhaseSucceeded,
			states:  "one=Succeeded two=Skipped three=Succeeded",
			actions: "close-parent#100",
			check: func(t *testing.T, r Result) {
				if !r.Complete || !strings.Contains(r.Status, CompleteMarker) {
					t.Error("status does not record completion")
				}
			},
		},
		{
			name:    "completed pipeline is left alone, even reopened",
			setup:   func(f *fixture) { f.in.Completed = true },
			phase:   PhaseSucceeded,
			actions: "",
			check: func(t *testing.T, r Result) {
				if !r.LeaveAlone || r.Status != "" {
					t.Error("a completed pipeline writes")
				}
			},
		},
		{
			name: "reopened child goes back to active and blocks creation",
			setup: func(f *fixture) {
				f.child(1, "one", "open").child(2, "two", "open")
			},
			phase:   PhaseRunning,
			states:  "one=Active two=Active three=Pending",
			actions: "",
		},
		{
			name: "duplicate open children: oldest kept, others closed",
			setup: func(f *fixture) {
				f.child(1, "one", "open").child(2, "one", "open")
			},
			phase:       PhaseRunning,
			states:      "one=Active two=Pending three=Pending",
			actions:     "close-duplicate#2",
			wantWarning: "#2 is a duplicate of #1",
		},
		{
			name: "canonical closed child is the newest closed that is not a duplicate",
			setup: func(f *fixture) {
				f.child(1, "one", "completed").child(2, "one", "duplicate", func(c *Child) { c.ClosedAt = t0 })
			},
			phase:   PhaseRunning,
			states:  "one=Succeeded two=Active three=Pending",
			actions: "create-child#100",
		},
		{
			name:    "unlinked child is linked, not created again",
			setup:   func(f *fixture) { f.child(5, "one", "open", func(c *Child) { c.Linked = false }) },
			phase:   PhaseRunning,
			states:  "one=Active two=Pending three=Pending",
			actions: "link-sub-issue#5",
		},
		{
			name: "missing labels and assignee re-applied on the active child",
			setup: func(f *fixture) {
				f.child(1, "one", "open", func(c *Child) { c.Labels = []string{"greenfield"}; c.Assignees = nil })
			},
			phase:   PhaseRunning,
			states:  "one=Active two=Pending three=Pending",
			actions: "add-labels#1 add-assignees#1",
			check: func(t *testing.T, r Result) {
				if got := strings.Join(r.Actions[0].Labels, ","); got != "step/one,factory,factory/review" {
					t.Errorf("labels %s", got)
				}
			},
		},
		{
			name:    "a child claimed by another account keeps its assignee",
			setup:   func(f *fixture) { f.child(1, "one", "open", func(c *Child) { c.Assignees = []string{"coder-2"} }) },
			phase:   PhaseRunning,
			actions: "",
		},
		{
			name:    "merged bot PR completes the child and opens the next step",
			setup:   func(f *fixture) { f.child(1, "one", "open", func(c *Child) { c.MergedPRs = []int{9, 7} }) },
			phase:   PhaseRunning,
			states:  "one=Succeeded two=Active three=Pending",
			actions: "complete-child#1 create-child#100",
			check: func(t *testing.T, r Result) {
				if r.Actions[0].Ref != 7 {
					t.Errorf("completed by PR #%d, want #7", r.Actions[0].Ref)
				}
			},
		},
		{
			name: "closeOnLinkedPRMerge false waits for the child to close",
			setup: func(f *fixture) {
				f.child(1, "one", "completed").child(2, "two", "completed").
					child(3, "three", "open", func(c *Child) { c.MergedPRs = []int{9}; c.Assignees = []string{"someone"} })
			},
			phase:   PhaseRunning,
			states:  "one=Succeeded two=Succeeded three=Active",
			actions: "",
		},
		{
			name: "step added ahead of a started step runs after the active step",
			setup: func(f *fixture) {
				// "one" was added to the definition after "two" started.
				f.child(2, "two", "open")
			},
			phase:   PhaseRunning,
			states:  "one=Pending two=Active three=Pending",
			actions: "",
		},
		{
			name: "step added ahead runs once the active step finishes",
			setup: func(f *fixture) {
				f.child(2, "two", "completed")
			},
			phase:   PhaseRunning,
			states:  "one=Active two=Succeeded three=Pending",
			actions: "create-child#100",
			check: func(t *testing.T, r Result) {
				if r.Actions[0].Step != "one" {
					t.Errorf("created %q", r.Actions[0].Step)
				}
			},
		},
		{
			name:        "removed step's child is orphaned",
			setup:       func(f *fixture) { f.child(1, "one", "open").child(9, "gone", "completed") },
			phase:       PhaseRunning,
			actions:     "",
			wantWarning: "#9 is for step `gone`, which the definition no longer has (orphaned)",
		},
		{
			name:        "unmanaged sub-issues are reported",
			setup:       func(f *fixture) { f.child(1, "one", "open"); f.in.Unmanaged = []int{77} },
			phase:       PhaseRunning,
			actions:     "",
			wantWarning: "#77 is a sub-issue without this pipeline's marker (unmanaged)",
		},
		{
			name: "params changed after start blocks",
			setup: func(f *fixture) {
				f.child(1, "one", "completed")
				f.in.Request.Params = map[string]string{"kind": "Bar"}
			},
			phase:   PhaseBlocked,
			actions: "",
		},
		{
			name: "an optional param added after start blocks too",
			setup: func(f *fixture) {
				f.child(1, "one", "completed")
				f.in.Request.Params = map[string]string{"kind": "Foo", "version": "v1alpha1"}
			},
			phase:   PhaseBlocked,
			actions: "",
		},
		{
			name: "optional param supplied is used",
			setup: func(f *fixture) {
				f.in.Request.Params = map[string]string{"kind": "Foo", "version": "v1beta1"}
			},
			phase:   PhaseRunning,
			actions: "create-child#100",
			check: func(t *testing.T, r Result) {
				if !strings.Contains(r.Actions[0].Body, "v1beta1") {
					t.Errorf("body %q", r.Actions[0].Body)
				}
			},
		},
		{
			name:    "stop label pauses",
			setup:   func(f *fixture) { f.in.Stopped = true },
			phase:   PhasePaused,
			states:  "one=Pending two=Pending three=Pending",
			actions: "",
		},
		{
			name:    "missing required param is invalid and labelled",
			setup:   func(f *fixture) { f.in.Request.Params = map[string]string{} },
			phase:   PhaseInvalid,
			actions: "label-parent#100",
		},
		{
			name:    "unknown param is invalid",
			setup:   func(f *fixture) { f.in.Request.Params["extra"] = "x" },
			phase:   PhaseInvalid,
			actions: "label-parent#100",
		},
		{
			name: "definition not found is invalid; already labelled writes no label",
			setup: func(f *fixture) {
				f.in.Definition, f.in.DefinitionErr = nil, errors.New("no such file")
				f.in.Parent.Labels = append(f.in.Parent.Labels, "factory/pipeline-invalid")
			},
			phase:   PhaseInvalid,
			actions: "",
		},
		{
			name:    "metadata.name mismatch is invalid",
			setup:   func(f *fixture) { f.in.Request.Pipeline = "other" },
			phase:   PhaseInvalid,
			actions: "label-parent#100",
		},
		{
			name:    "valid again takes the invalid label off",
			setup:   func(f *fixture) { f.in.Parent.Labels = append(f.in.Parent.Labels, "factory/pipeline-invalid") },
			phase:   PhaseRunning,
			actions: "unlabel-parent#100 create-child#100",
		},
		{
			name: "parent closed by a person is cancelled; children left open",
			setup: func(f *fixture) {
				f.child(1, "one", "open")
				f.in.Parent.Open = false
			},
			phase:   PhaseCancelled,
			actions: "",
		},
		{
			name: "parent closed by a commit with steps remaining is reopened",
			setup: func(f *fixture) {
				f.child(1, "one", "completed")
				f.in.Parent.Open = false
				f.in.ParentClosedByCommit = "0123456789abcdef"
			},
			phase:   PhaseRunning,
			actions: "reopen-parent#100 create-child#100",
			check: func(t *testing.T, r Result) {
				if !strings.Contains(r.Actions[0].Comment, "0123456") {
					t.Errorf("comment %q", r.Actions[0].Comment)
				}
			},
		},
		{
			name: "parent closed after every step is done records completion without closing",
			setup: func(f *fixture) {
				f.child(1, "one", "completed").child(2, "two", "completed").child(3, "three", "completed")
				f.in.Parent.Open = false
				f.in.ParentClosedByCommit = "abc"
			},
			phase:   PhaseSucceeded,
			actions: "",
			check: func(t *testing.T, r Result) {
				if !r.Complete {
					t.Error("completion not recorded")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.setup(f)
			r := Reconcile(f.in)
			if r.Phase != tc.phase {
				t.Errorf("phase = %s (%s), want %s", r.Phase, r.Reason, tc.phase)
			}
			if tc.states != "" && states(r) != tc.states {
				t.Errorf("states = %s, want %s", states(r), tc.states)
			}
			if got := kinds(r); got != tc.actions {
				t.Errorf("actions = %q, want %q", got, tc.actions)
			}
			if tc.wantWarning != "" && !strings.Contains(r.Status, tc.wantWarning) {
				t.Errorf("status lacks %q:\n%s", tc.wantWarning, r.Status)
			}
			if tc.check != nil {
				tc.check(t, r)
			}
			// Deterministic: the same input gives the same result.
			if again := Reconcile(f.in); again.Status != r.Status || kinds(again) != kinds(r) {
				t.Error("Reconcile is not deterministic")
			}
		})
	}
}

func TestStatusRendering(t *testing.T) {
	f := newFixture(t)
	f.child(1, "one", "completed").child(2, "two", "open")
	r := Reconcile(f.in)
	want := StatusMarker + "\n" +
		"### Pipeline `demo` — Foo\n" +
		"**Phase:** Running · **Current step:** 2/3 `two`\n" +
		"\n| # | Step | Issue | State | Started | Completed |\n|---|------|-------|-------|---------|-----------|\n" +
		"| 1 | one | #1 | ✅ Succeeded | 2026-09-20 | 2026-09-20 |\n" +
		"| 2 | two | #2 | 🔄 Active | 2026-09-20 | |\n" +
		"| 3 | three | — | ⏳ Pending | | |\n" +
		"\n<sub>definition: .agents/pipelines/demo.yaml@abc1234</sub>\n"
	if r.Status != want {
		t.Errorf("status:\n%s\nwant:\n%s", r.Status, want)
	}
}

package pipeline

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// Issue is an issue as Reconcile sees it.
type Issue struct {
	Number int
	Title  string
	Body   string
	Open   bool
	// StateReason is a closed issue's: "completed", "not_planned" or
	// "duplicate".
	StateReason string
	Labels      []string
	Assignees   []string
	Author      string
	CreatedAt   time.Time
	ClosedAt    time.Time
}

func (i Issue) hasLabel(label string) bool {
	return slices.ContainsFunc(i.Labels, func(l string) bool { return strings.EqualFold(l, label) })
}

// skipped reports whether a closed issue was closed as not planned or as a
// duplicate: its step is Skipped, and the pipeline moves on.
func (i Issue) skipped() bool {
	return !i.Open && (i.StateReason == "not_planned" || i.StateReason == "duplicate")
}

// Child is an issue whose marker names the parent.
type Child struct {
	Issue
	Marker Marker
	// Linked is a child that is a sub-issue of the parent.
	Linked bool
	// MergedPRs are bot-authored, merged pull requests that reference the
	// child. Only read for open children.
	MergedPRs []int
}

// Input is everything Reconcile decides on.
type Input struct {
	// Repo is "owner/repo", for the URLs `step` gives templates.
	Repo   string
	Parent Issue
	// ParentClosedByCommit is the commit that closed a closed parent; empty
	// when a person closed it.
	ParentClosedByCommit string
	// Stopped is a parent with the stop label.
	Stopped bool
	// Request is the parent's factory-pipeline block, or RequestErr why it
	// has none that parses.
	Request    *Request
	RequestErr error
	// Definition is the pipeline the request names, read from the default
	// branch, or DefinitionErr why it could not be read or validated.
	Definition    *Definition
	DefinitionErr error
	// DefinitionRef says where the definition was read: path@commit.
	DefinitionRef string
	// Children are the issues whose marker names the parent: its marked
	// sub-issues, and the reconciler's own recent issues not yet linked.
	Children []Child
	// Unmanaged are sub-issues of the parent without a marker naming it.
	Unmanaged []int
	// Completed is a status comment carrying CompleteMarker.
	Completed bool
	// TriggerLabel is the deployment's: every child gets it.
	TriggerLabel string
	// TargetAssignee is who a child is assigned to when its step names
	// nobody.
	TargetAssignee string
}

// Phase is a pipeline's, derived from its steps and its parent.
type Phase string

const (
	PhaseRunning   Phase = "Running"
	PhaseBlocked   Phase = "Blocked"
	PhasePaused    Phase = "Paused"
	PhaseSucceeded Phase = "Succeeded"
	PhaseCancelled Phase = "Cancelled"
	PhaseInvalid   Phase = "Invalid"
)

// StepState is a step's.
type StepState string

const (
	StepPending   StepState = "Pending"
	StepActive    StepState = "Active"
	StepSucceeded StepState = "Succeeded"
	StepSkipped   StepState = "Skipped"
)

func (s StepState) done() bool { return s == StepSucceeded || s == StepSkipped }

// StepStatus is a step's state and its canonical child.
type StepStatus struct {
	ID    string
	State StepState
	// Issue is the canonical child's number; zero when it has none (yet).
	Issue     int
	Started   time.Time
	Completed time.Time
}

// ActionKind says what an Action does.
type ActionKind string

const (
	ActionReopenParent   ActionKind = "reopen-parent"
	ActionLabelParent    ActionKind = "label-parent"
	ActionUnlabelParent  ActionKind = "unlabel-parent"
	ActionLinkSubIssue   ActionKind = "link-sub-issue"
	ActionCloseDuplicate ActionKind = "close-duplicate"
	ActionAddLabels      ActionKind = "add-labels"
	ActionAddAssignees   ActionKind = "add-assignees"
	ActionCompleteChild  ActionKind = "complete-child"
	ActionCreateChild    ActionKind = "create-child"
	ActionCloseParent    ActionKind = "close-parent"
)

// Action is one write. Every action is safe to repeat.
type Action struct {
	Kind ActionKind
	// Issue is the issue written to: the parent, or a child.
	Issue int
	// Ref is the canonical child a duplicate duplicates, or the merged PR
	// that completes a child.
	Ref int
	// Step is the step a created child is for.
	Step      string
	Title     string
	Body      string
	Labels    []string
	Assignees []string
	// Comment is posted with the action, where it has one.
	Comment string
}

func (a Action) String() string {
	switch a.Kind {
	case ActionReopenParent:
		return fmt.Sprintf("reopen #%d", a.Issue)
	case ActionLabelParent:
		return fmt.Sprintf("label #%d %s", a.Issue, strings.Join(a.Labels, ", "))
	case ActionUnlabelParent:
		return fmt.Sprintf("remove label %s from #%d", strings.Join(a.Labels, ", "), a.Issue)
	case ActionLinkSubIssue:
		return fmt.Sprintf("link #%d as a sub-issue", a.Issue)
	case ActionCloseDuplicate:
		return fmt.Sprintf("close #%d as a duplicate of #%d", a.Issue, a.Ref)
	case ActionAddLabels:
		return fmt.Sprintf("label #%d %s", a.Issue, strings.Join(a.Labels, ", "))
	case ActionAddAssignees:
		return fmt.Sprintf("assign #%d to %s", a.Issue, strings.Join(a.Assignees, ", "))
	case ActionCompleteChild:
		return fmt.Sprintf("close #%d as completed: bot PR #%d that references it merged", a.Issue, a.Ref)
	case ActionCreateChild:
		s := fmt.Sprintf("create step %q: %q labelled %s", a.Step, a.Title, strings.Join(a.Labels, ", "))
		if len(a.Assignees) > 0 {
			s += ", assigned to " + strings.Join(a.Assignees, ", ")
		}
		return s
	case ActionCloseParent:
		return fmt.Sprintf("close #%d as completed", a.Issue)
	}
	return string(a.Kind)
}

// Result is what Reconcile decided.
type Result struct {
	Phase Phase
	// Reason explains a phase other than Running.
	Reason string
	// Steps are the definition's steps, in order; nil when the definition
	// or params are invalid.
	Steps   []StepStatus
	Actions []Action
	// Warnings are shown in the status comment.
	Warnings []string
	// Complete is a status comment that records completion.
	Complete bool
	// LeaveAlone is a pipeline already completed: nothing is written, not
	// even the status.
	LeaveAlone bool
	// Status is the status comment as Reconcile renders it.
	Status string
}

// Reconcile decides what to do about a pipeline parent (design §6.1). It
// does no I/O and reads no clock: the same input always gives the same
// result.
func Reconcile(in Input) Result {
	var r Result
	if in.Completed {
		r.Phase = PhaseSucceeded
		r.Reason = "the pipeline completed; completion is final"
		r.LeaveAlone = true
		return r
	}

	def := in.Definition
	params, invalid := resolve(in)
	var m matched
	if invalid == nil {
		m = match(def, in.Children)
		r.Steps = m.steps
	}
	parent := in.Parent.Number

	// 1. Parent closed.
	if !in.Parent.Open {
		switch {
		case invalid == nil && m.blocked(in.Request) == "" && allDone(r.Steps):
			r.Phase = PhaseSucceeded
			r.Complete = true
		case invalid == nil && in.ParentClosedByCommit != "":
			r.Actions = append(r.Actions, Action{
				Kind:  ActionReopenParent,
				Issue: parent,
				Comment: fmt.Sprintf("Commit %s closed this issue while steps of the `%s` pipeline remain, probably through a `Fixes #%d` meant for one of its steps' issues. Reopening it so the pipeline carries on; to stop it, close this issue yourself or add the `%s` label.",
					shortSHA(in.ParentClosedByCommit), in.Request.Pipeline, parent, conventions.StopLabel(in.TriggerLabel)),
			})
			in.Parent.Open = true
		default:
			r.Phase = PhaseCancelled
			r.Reason = "the parent was closed; open children are left open"
		}
		if !in.Parent.Open {
			r.Status = RenderStatus(in, r, nil)
			return r
		}
	}

	// 2. Stop label.
	if in.Stopped {
		r.Phase = PhasePaused
		r.Reason = fmt.Sprintf("remove `%s` to continue", conventions.StopLabel(in.TriggerLabel))
		r.Status = RenderStatus(in, r, nil)
		return r
	}

	// 3. Invalid definition or params.
	invalidLabel := conventions.PipelineInvalidLabel(in.TriggerLabel)
	if invalid != nil {
		return r.invalid(in, invalid, invalidLabel)
	}
	if in.Parent.hasLabel(invalidLabel) {
		r.Actions = append(r.Actions, Action{Kind: ActionUnlabelParent, Issue: parent, Labels: []string{invalidLabel}})
	}

	// 4. Params changed after start.
	if why := m.blocked(in.Request); why != "" {
		r.Phase = PhaseBlocked
		r.Reason = why
		r.Warnings = append(r.Warnings, m.warnings(in)...)
		r.Status = RenderStatus(in, r, nil)
		return r
	}

	// 5. Repair: link what is not linked, close duplicates.
	for _, c := range m.all {
		if !c.Linked {
			r.Actions = append(r.Actions, Action{Kind: ActionLinkSubIssue, Issue: c.Number})
		}
	}
	for _, d := range m.dups {
		if d.child.Open {
			r.Actions = append(r.Actions, Action{
				Kind: ActionCloseDuplicate, Issue: d.child.Number, Ref: d.of,
				Comment: fmt.Sprintf("Duplicate of #%d: step `%s` of this pipeline already has that issue.", d.of, d.child.Marker.Step),
			})
		}
	}
	r.Warnings = append(r.Warnings, m.warnings(in)...)

	// 6. Active children: keep their labels and assignees, and complete
	// them when a bot PR that references them has merged.
	refs := m.refs(in.Repo)
	for i := range r.Steps {
		st := &r.Steps[i]
		if st.State != StepActive {
			continue
		}
		step := def.Spec.Steps[i]
		c := m.canonical[i]
		if step.CloseOnLinkedPRMerge() && len(c.MergedPRs) > 0 {
			pr := slices.Min(c.MergedPRs)
			r.Actions = append(r.Actions, Action{
				Kind: ActionCompleteChild, Issue: c.Number, Ref: pr,
				Comment: fmt.Sprintf("Bot PR #%d, which references this issue, has merged. Closing this issue as completed so the pipeline moves on.", pr),
			})
			st.State = StepSucceeded
			continue
		}
		review := def.reviewWanted(step, params, refs)
		if missing := missingLabels(c.Issue, wantLabels(def, step, in.TriggerLabel, review)); len(missing) > 0 {
			r.Actions = append(r.Actions, Action{Kind: ActionAddLabels, Issue: c.Number, Labels: missing})
		}
		if want := wantAssignees(step, in.TargetAssignee); len(c.Assignees) == 0 && len(want) > 0 {
			r.Actions = append(r.Actions, Action{Kind: ActionAddAssignees, Issue: c.Number, Assignees: want})
		}
	}

	// 7. Create the first pending step's child, unless a step is active.
	active := slices.ContainsFunc(r.Steps, func(s StepStatus) bool { return s.State == StepActive })
	if !active {
		if i := slices.IndexFunc(r.Steps, func(s StepStatus) bool { return s.State == StepPending }); i >= 0 {
			step := def.Spec.Steps[i]
			rendered, err := def.renderStep(step, params, refs)
			if err != nil {
				return r.invalid(in, err, invalidLabel)
			}
			marker := Marker{Parent: parent, Pipeline: in.Request.Pipeline, Step: step.ID, Params: ParamsHash(in.Request.Params)}
			r.Actions = append(r.Actions, Action{
				Kind:      ActionCreateChild,
				Issue:     parent,
				Step:      step.ID,
				Title:     rendered.Title,
				Body:      childBody(rendered.Body, marker),
				Labels:    wantLabels(def, step, in.TriggerLabel, rendered.Review != ""),
				Assignees: wantAssignees(step, in.TargetAssignee),
			})
			r.Steps[i].State = StepActive
		}
	}

	// 8. Complete.
	if allDone(r.Steps) {
		r.Phase = PhaseSucceeded
		r.Complete = true
		r.Actions = append(r.Actions, Action{Kind: ActionCloseParent, Issue: parent})
	} else {
		r.Phase = PhaseRunning
	}

	// 9. Status.
	r.Status = RenderStatus(in, r, nil)
	return r
}

// invalid is the result for a parent whose definition or params do not
// validate: it is labelled, the status says why, and nothing is created.
func (r Result) invalid(in Input, err error, label string) Result {
	r.Phase = PhaseInvalid
	r.Reason = err.Error()
	r.Steps = nil
	r.Warnings = nil
	actions := []Action{}
	for _, a := range r.Actions {
		if a.Kind == ActionReopenParent {
			actions = append(actions, a)
		}
	}
	if !in.Parent.hasLabel(label) {
		actions = append(actions, Action{Kind: ActionLabelParent, Issue: in.Parent.Number, Labels: []string{label}})
	}
	r.Actions = actions
	r.Status = RenderStatus(in, r, nil)
	return r
}

// resolve checks the request against the definition, and returns the
// params' values or why they cannot be had.
func resolve(in Input) (map[string]string, error) {
	switch {
	case in.RequestErr != nil:
		return nil, in.RequestErr
	case in.Request == nil:
		return nil, errors.New("no factory-pipeline block")
	case in.DefinitionErr != nil:
		return nil, fmt.Errorf("%s: %w", Path(in.Request.Pipeline), in.DefinitionErr)
	case in.Definition == nil:
		return nil, fmt.Errorf("%s: not found", Path(in.Request.Pipeline))
	case in.Definition.Metadata.Name != in.Request.Pipeline:
		return nil, fmt.Errorf("%s: metadata.name is %q, want %q", Path(in.Request.Pipeline), in.Definition.Metadata.Name, in.Request.Pipeline)
	}
	return in.Definition.Resolve(in.Request.Params)
}

// duplicate is a child that is not its step's canonical child.
type duplicate struct {
	child Child
	of    int
}

// matched is the children matched to the definition's steps.
type matched struct {
	steps []StepStatus
	// canonical is each step's canonical child, by step index.
	canonical map[int]Child
	dups      []duplicate
	// orphans are children of steps the definition no longer has.
	orphans []Child
	// all are every child, in number order.
	all []Child
}

// match matches children to steps by the step id in their marker (design
// §5.1). A step's canonical child is its oldest open child, or, when none is
// open, its newest closed one (one not closed as a duplicate first).
func match(def *Definition, children []Child) matched {
	m := matched{canonical: map[int]Child{}}
	m.all = slices.Clone(children)
	slices.SortFunc(m.all, func(a, b Child) int { return cmp.Compare(a.Number, b.Number) })
	byStep := map[string][]Child{}
	for _, c := range m.all {
		byStep[c.Marker.Step] = append(byStep[c.Marker.Step], c)
	}
	for i, s := range def.Spec.Steps {
		st := StepStatus{ID: s.ID, State: StepPending}
		cs := byStep[s.ID]
		delete(byStep, s.ID)
		if c, ok := canonical(cs); ok {
			m.canonical[i] = c
			st.Issue = c.Number
			st.Started = c.CreatedAt
			switch {
			case c.Open:
				st.State = StepActive
			case c.skipped():
				st.State = StepSkipped
				st.Completed = c.ClosedAt
			default:
				st.State = StepSucceeded
				st.Completed = c.ClosedAt
			}
			for _, o := range cs {
				if o.Number != c.Number {
					m.dups = append(m.dups, duplicate{child: o, of: c.Number})
				}
			}
		}
		m.steps = append(m.steps, st)
	}
	for _, c := range m.all {
		if _, ok := byStep[c.Marker.Step]; ok {
			m.orphans = append(m.orphans, c)
		}
	}
	return m
}

// canonical picks a step's canonical child.
func canonical(cs []Child) (Child, bool) {
	var open, closed []Child
	for _, c := range cs {
		if c.Open {
			open = append(open, c)
		} else {
			closed = append(closed, c)
		}
	}
	if len(open) > 0 {
		return slices.MinFunc(open, func(a, b Child) int {
			return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.Number, b.Number))
		}), true
	}
	if len(closed) > 0 {
		return slices.MaxFunc(closed, func(a, b Child) int {
			aDup, bDup := a.StateReason == "duplicate", b.StateReason == "duplicate"
			if aDup != bDup {
				if aDup {
					return -1
				}
				return 1
			}
			return cmp.Or(a.ClosedAt.Compare(b.ClosedAt), cmp.Compare(a.Number, b.Number))
		}), true
	}
	return Child{}, false
}

// blocked says why the parent's params no longer match what its children
// were rendered with, or "" when they do.
func (m matched) blocked(req *Request) string {
	if req == nil {
		return ""
	}
	hash := ParamsHash(req.Params)
	var changed []string
	for _, c := range m.all {
		switch {
		case c.Marker.Pipeline != req.Pipeline:
			changed = append(changed, fmt.Sprintf("#%d was started by pipeline `%s`", c.Number, c.Marker.Pipeline))
		case c.Marker.Params != hash:
			changed = append(changed, fmt.Sprintf("#%d", c.Number))
		}
	}
	if len(changed) == 0 {
		return ""
	}
	return fmt.Sprintf("the parent's factory-pipeline block changed after the pipeline started (%s). Restore it to carry on, or file a new parent to run with different params", strings.Join(changed, ", "))
}

// warnings are the status comment's notes on duplicates, orphaned steps and
// unmanaged sub-issues.
func (m matched) warnings(in Input) []string {
	var w []string
	for _, d := range m.dups {
		w = append(w, fmt.Sprintf("#%d is a duplicate of #%d (step `%s`)", d.child.Number, d.of, d.child.Marker.Step))
	}
	for _, c := range m.orphans {
		w = append(w, fmt.Sprintf("#%d is for step `%s`, which the definition no longer has (orphaned)", c.Number, c.Marker.Step))
	}
	for _, n := range in.Unmanaged {
		w = append(w, fmt.Sprintf("#%d is a sub-issue without this pipeline's marker (unmanaged)", n))
	}
	return w
}

// refs are what `step "<id>"` gives templates: each step's canonical child.
func (m matched) refs(repo string) map[string]StepRef {
	refs := map[string]StepRef{}
	for i, st := range m.steps {
		if c, ok := m.canonical[i]; ok {
			refs[st.ID] = StepRef{Issue: c.Number, URL: fmt.Sprintf("https://github.com/%s/issues/%d", repo, c.Number)}
		}
	}
	return refs
}

func allDone(steps []StepStatus) bool {
	if len(steps) == 0 {
		return false
	}
	for _, s := range steps {
		if !s.State.done() {
			return false
		}
	}
	return true
}

// reviewWanted reports whether a step's child is labelled for review: its
// review instructions render non-empty. Should they not render, having any
// is enough.
func (d *Definition) reviewWanted(s Step, params map[string]string, refs map[string]StepRef) bool {
	if s.Review == "" {
		return false
	}
	r, err := d.renderStep(s, params, refs)
	if err != nil {
		return true
	}
	return r.Review != ""
}

// wantLabels are the labels a step's child carries: the defaults', the
// step's, the trigger label, and the review label when it is reviewed.
func wantLabels(d *Definition, s Step, trigger string, review bool) []string {
	var out []string
	add := func(l string) {
		l = strings.TrimSpace(l)
		if l != "" && !slices.ContainsFunc(out, func(o string) bool { return strings.EqualFold(o, l) }) {
			out = append(out, l)
		}
	}
	for _, l := range d.Spec.Defaults.Labels {
		add(l)
	}
	for _, l := range s.Labels {
		add(l)
	}
	add(trigger)
	if review {
		add(conventions.ReviewLabel(trigger))
	}
	return out
}

func missingLabels(i Issue, want []string) []string {
	var missing []string
	for _, l := range want {
		if !i.hasLabel(l) {
			missing = append(missing, l)
		}
	}
	return missing
}

// wantAssignees are who a step's child is assigned to: the step's
// assignees, else the target assignee. They are only re-applied to a child
// assigned to nobody, so a task claimed by another account, or a person
// reassigning it, is not undone.
func wantAssignees(s Step, target string) []string {
	if len(s.Assignees) > 0 {
		return slices.Clone(s.Assignees)
	}
	if target != "" {
		return []string{target}
	}
	return nil
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

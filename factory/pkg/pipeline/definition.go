// Package pipeline implements deterministic multi-step issue pipelines
// (design/issue-pipelines.md): an IssuePipeline definition, committed to the
// watched repository under .agents/pipelines/, is an ordered list of steps,
// each a templated GitHub issue. A parent issue starts one with a
// factory-pipeline block; the reconciler opens each step as a sub-issue of
// the parent, one at a time, and closes the parent after the last.
//
// The package separates deciding from doing. Reconcile is a pure function of
// the definition, the parent's params and what GitHub shows, and returns a
// status and a list of actions; Apply carries them out; Observe reads what
// Reconcile needs. GitHub is the only state store.
package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// APIVersion is the schema version a definition declares.
	APIVersion = "factory.gemini.google.com/v1alpha1"
	// Kind is the kind a definition declares.
	Kind = "IssuePipeline"
	// Dir is where definitions live in the watched repository.
	Dir = ".agents/pipelines"
)

var (
	// nameRe is a pipeline's name: a bare name, so it can only resolve to
	// a file in Dir.
	nameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	// stepIDRe is a step's id, which a child's marker carries.
	stepIDRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	// paramNameRe is a param's name, usable as {{ .Params.<name> }}.
	paramNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

// Definition is an IssuePipeline: what .agents/pipelines/<name>.yaml holds.
type Definition struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Metadata   Metadata `yaml:"metadata"`
	Spec       Spec     `yaml:"spec"`
}

// Metadata names a pipeline.
type Metadata struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
}

// Spec is a pipeline's params and steps.
type Spec struct {
	// Params are the inputs the parent issue supplies.
	Params []Param `yaml:"params,omitempty"`
	// Defaults apply to every step unless the step overrides them.
	Defaults Defaults `yaml:"defaults,omitempty"`
	// Steps run in order, one at a time.
	Steps []Step `yaml:"steps"`
}

// Param is one input. Every param is constrained by a pattern or an enum, so
// free text never reaches a template.
type Param struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
	// Pattern is an anchored regular expression (^...$) the value matches.
	Pattern string `yaml:"pattern,omitempty"`
	// Enum lists the values allowed.
	Enum []string `yaml:"enum,omitempty"`
	// Required defaults to true.
	Required *bool `yaml:"required,omitempty"`
	// Default is an optional param's value when the parent leaves it out.
	Default *string `yaml:"default,omitempty"`
	// Example is a value validation renders the steps with.
	Example string `yaml:"example,omitempty"`
}

// IsRequired reports whether the parent must supply the param.
func (p Param) IsRequired() bool { return p.Required == nil || *p.Required }

// Defaults apply to every step.
type Defaults struct {
	// Labels are added to every child, besides the step's own and the
	// deployment's trigger label.
	Labels []string `yaml:"labels,omitempty"`
}

// Step is one child issue.
type Step struct {
	// ID is the step's stable identity: children are matched to steps by
	// it, never by position or title.
	ID string `yaml:"id"`
	// Title and Body are templates.
	Title string `yaml:"title"`
	Body  string `yaml:"body,omitempty"`
	// Labels are added to the defaults' labels.
	Labels []string `yaml:"labels,omitempty"`
	// Review, a template, are review instructions. When set, the child is
	// labelled for automated review and the rendered text is appended to
	// its body under ## Review Instructions.
	Review string `yaml:"review,omitempty"`
	// Assignees default to the watch's target assignee.
	Assignees []string `yaml:"assignees,omitempty"`
	// Completion says when the step counts as done besides its child
	// closing.
	Completion *Completion `yaml:"completion,omitempty"`
	// DependsOn is reserved for DAGs; in v1 each step depends on the one
	// before it.
	DependsOn []string `yaml:"dependsOn,omitempty"`
}

// Completion tunes how a step's child completes.
type Completion struct {
	// CloseOnLinkedPRMerge, on by default, closes the open child as
	// completed when a bot pull request that references it merges.
	CloseOnLinkedPRMerge *bool `yaml:"closeOnLinkedPRMerge,omitempty"`
}

// CloseOnLinkedPRMerge reports whether a merged bot PR completes the step.
func (s Step) CloseOnLinkedPRMerge() bool {
	return s.Completion == nil || s.Completion.CloseOnLinkedPRMerge == nil || *s.Completion.CloseOnLinkedPRMerge
}

// Path is where a pipeline named name is defined in the watched repository.
func Path(name string) string { return Dir + "/" + name + ".yaml" }

// ValidName reports whether name is a pipeline name: a bare name, never a
// path, URL or another repository's file.
func ValidName(name string) bool { return nameRe.MatchString(name) }

// Parse reads and validates a definition. Unknown fields are errors.
func Parse(data []byte) (*Definition, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var d Definition
	if err := dec.Decode(&d); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("the definition is empty")
		}
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("the definition has more than one YAML document")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Step returns the step with id, and whether there is one.
func (d *Definition) Step(id string) (Step, int, bool) {
	for i, s := range d.Spec.Steps {
		if s.ID == id {
			return s, i, true
		}
	}
	return Step{}, -1, false
}

// Validate checks a definition statically (design §4.4): the header, unique
// and well-formed names, constrained params with valid defaults, templates
// that parse and reference only earlier steps, and that every step renders
// with each param's example, and again with every optional param omitted.
func (d *Definition) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if d.APIVersion != APIVersion {
		add("apiVersion is %q, want %q", d.APIVersion, APIVersion)
	}
	if d.Kind != Kind {
		add("kind is %q, want %q", d.Kind, Kind)
	}
	if !ValidName(d.Metadata.Name) {
		add("metadata.name %q is not a pipeline name (lowercase letters, digits and dashes)", d.Metadata.Name)
	}

	params := map[string]bool{}
	for i, p := range d.Spec.Params {
		where := fmt.Sprintf("params[%d]", i)
		if p.Name != "" {
			where = fmt.Sprintf("param %q", p.Name)
		}
		if !paramNameRe.MatchString(p.Name) {
			add("%s: name must be letters, digits and underscores, starting with a letter", where)
		}
		if params[p.Name] {
			add("%s is declared twice", where)
		}
		params[p.Name] = true
		if err := p.validate(); err != nil {
			add("%s: %v", where, err)
		}
	}

	if len(d.Spec.Steps) == 0 {
		add("spec.steps is empty")
	}
	for _, l := range d.Spec.Defaults.Labels {
		if strings.TrimSpace(l) == "" {
			add("defaults.labels has an empty label")
		}
	}
	ids := map[string]bool{}
	for i, s := range d.Spec.Steps {
		where := fmt.Sprintf("steps[%d]", i)
		if s.ID != "" {
			where = fmt.Sprintf("step %q", s.ID)
		}
		if !stepIDRe.MatchString(s.ID) {
			add("%s: id must be lowercase letters, digits and dashes", where)
		}
		if ids[s.ID] {
			add("%s: id is used twice", where)
		}
		if strings.TrimSpace(s.Title) == "" {
			add("%s: title is empty", where)
		}
		if len(s.DependsOn) > 0 {
			add("%s: dependsOn is reserved; in v1 each step depends on the one before it", where)
		}
		for _, l := range s.Labels {
			if strings.TrimSpace(l) == "" {
				add("%s: labels has an empty label", where)
			}
		}
		for _, a := range s.Assignees {
			if strings.TrimSpace(a) == "" {
				add("%s: assignees has an empty login", where)
			}
		}
		for _, f := range []struct{ name, text string }{{"title", s.Title}, {"body", s.Body}, {"review", s.Review}} {
			t, err := parseTemplate(f.name, f.text)
			if err != nil {
				add("%s: %s: %v", where, f.name, err)
				continue
			}
			refs, err := stepRefs(t)
			if err != nil {
				add("%s: %s: %v", where, f.name, err)
				continue
			}
			for _, ref := range refs {
				if !ids[ref] {
					add("%s: %s: step %q is not an earlier step", where, f.name, ref)
				}
			}
		}
		ids[s.ID] = true
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return d.checkRenders()
}

// validate checks a param's constraint and default.
func (p Param) validate() error {
	if p.Pattern == "" && len(p.Enum) == 0 {
		return fmt.Errorf("needs a pattern or an enum, so free text never reaches a template")
	}
	if p.Pattern != "" {
		if !strings.HasPrefix(p.Pattern, "^") || !strings.HasSuffix(p.Pattern, "$") {
			return fmt.Errorf("pattern %q must be anchored with ^ and $", p.Pattern)
		}
		if _, err := regexp.Compile(p.Pattern); err != nil {
			return fmt.Errorf("pattern: %v", err)
		}
	}
	if slices.Contains(p.Enum, "") {
		return fmt.Errorf("enum has an empty value")
	}
	if p.Default != nil {
		if p.IsRequired() {
			return fmt.Errorf("a default is only allowed on an optional param (required: false)")
		}
		if err := p.check(*p.Default); err != nil {
			return fmt.Errorf("default: %v", err)
		}
	}
	if p.Example != "" {
		if err := p.check(p.Example); err != nil {
			return fmt.Errorf("example: %v", err)
		}
	}
	return nil
}

// check reports whether value satisfies the param's constraint.
func (p Param) check(value string) error {
	if len(p.Enum) > 0 && !slices.Contains(p.Enum, value) {
		return fmt.Errorf("%q is not one of %s", value, strings.Join(p.Enum, ", "))
	}
	if p.Pattern != "" {
		re, err := regexp.Compile(p.Pattern)
		if err != nil {
			return err
		}
		if !re.MatchString(value) {
			return fmt.Errorf("%q does not match %s", value, p.Pattern)
		}
	}
	return nil
}

// sampleValue is the value validation renders a param with: its example,
// else its default, else its first enum value.
func (p Param) sampleValue() (string, bool) {
	switch {
	case p.Example != "":
		return p.Example, true
	case p.Default != nil:
		return *p.Default, true
	case len(p.Enum) > 0:
		return p.Enum[0], true
	}
	return "", false
}

// checkRenders renders every step with each param's sample value, and again
// with every optional param omitted.
func (d *Definition) checkRenders() error {
	full := map[string]string{}
	minimal := map[string]string{}
	var errs []error
	for _, p := range d.Spec.Params {
		v, ok := p.sampleValue()
		switch {
		case !ok && p.IsRequired():
			errs = append(errs, fmt.Errorf("param %q: needs an example to validate the templates with", p.Name))
			continue
		case !ok:
			// An optional param with nothing to render it with is
			// rendered omitted only.
			continue
		}
		full[p.Name] = v
		if p.IsRequired() {
			minimal[p.Name] = v
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	for _, supplied := range []map[string]string{full, minimal} {
		params, err := d.Resolve(supplied)
		if err != nil {
			return err
		}
		refs := map[string]StepRef{}
		for i, s := range d.Spec.Steps {
			c, err := d.renderStep(s, params, refs)
			if err != nil {
				return err
			}
			if HasRequest(c.Body) {
				return fmt.Errorf("step %q: body renders a factory-pipeline block, which would make the child a pipeline parent", s.ID)
			}
			refs[s.ID] = StepRef{Issue: 1000 + i, URL: fmt.Sprintf("https://github.com/example/example/issues/%d", 1000+i)}
		}
	}
	return nil
}

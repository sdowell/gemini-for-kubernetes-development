package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"text/template/parse"
	"unicode"
)

// StepRef is what a template's `step "<id>"` gives: an earlier step's child.
type StepRef struct {
	Issue int
	URL   string
}

// funcs are the helpers templates may call, besides the built-ins. `step` is
// bound per render; this one only lets a template parse.
var funcs = template.FuncMap{
	"lower":      strings.ToLower,
	"upper":      strings.ToUpper,
	"trim":       strings.TrimSpace,
	"trimPrefix": func(prefix, s string) string { return strings.TrimPrefix(s, prefix) },
	"trimSuffix": func(suffix, s string) string { return strings.TrimSuffix(s, suffix) },
	"replace":    func(old, new, s string) string { return strings.ReplaceAll(s, old, new) },
	"hasPrefix":  func(prefix, s string) bool { return strings.HasPrefix(s, prefix) },
	"hasSuffix":  func(suffix, s string) bool { return strings.HasSuffix(s, suffix) },
	"contains":   func(sub, s string) bool { return strings.Contains(s, sub) },
	"kebab":      func(s string) string { return splitWords(s, "-") },
	"snake":      func(s string) string { return splitWords(s, "_") },
	"step":       func(string) (StepRef, error) { return StepRef{}, fmt.Errorf("step is not bound") },
}

// parseTemplate parses text with the helpers. A field a template reads that
// the data does not have is an error (missingkey=error), so a misspelt param
// fails validation instead of writing "<no value>" into a child.
func parseTemplate(name, text string) (*template.Template, error) {
	return template.New(name).Option("missingkey=error").Funcs(funcs).Parse(text)
}

// render executes text with params, `step` resolving to refs.
func render(name, text string, params map[string]string, refs map[string]StepRef) (string, error) {
	t, err := parseTemplate(name, text)
	if err != nil {
		return "", err
	}
	t.Funcs(template.FuncMap{"step": func(id string) (StepRef, error) {
		ref, ok := refs[id]
		if !ok {
			return StepRef{}, fmt.Errorf("step %q has no issue yet", id)
		}
		return ref, nil
	}})
	var b bytes.Buffer
	if err := t.Execute(&b, map[string]any{"Params": params}); err != nil {
		return "", err
	}
	return b.String(), nil
}

// stepRefs returns the step ids a template passes to `step`, which must be
// literal strings so that validation can check they name earlier steps.
func stepRefs(t *template.Template) ([]string, error) {
	var refs []string
	var walkErr error
	var walk func(n parse.Node)
	walk = func(n parse.Node) {
		if n == nil || walkErr != nil {
			return
		}
		switch n := n.(type) {
		case *parse.ListNode:
			if n == nil {
				return
			}
			for _, c := range n.Nodes {
				walk(c)
			}
		case *parse.ActionNode:
			walk(n.Pipe)
		case *parse.IfNode:
			walk(n.Pipe)
			walk(n.List)
			walk(n.ElseList)
		case *parse.RangeNode:
			walk(n.Pipe)
			walk(n.List)
			walk(n.ElseList)
		case *parse.WithNode:
			walk(n.Pipe)
			walk(n.List)
			walk(n.ElseList)
		case *parse.TemplateNode:
			walk(n.Pipe)
		case *parse.PipeNode:
			if n == nil {
				return
			}
			for _, c := range n.Cmds {
				walk(c)
			}
		case *parse.ChainNode:
			walk(n.Node)
		case *parse.CommandNode:
			for i, a := range n.Args {
				if id, ok := a.(*parse.IdentifierNode); ok && id.Ident == "step" {
					if i != 0 || len(n.Args) != 2 {
						walkErr = fmt.Errorf("step takes one argument, a step id in quotes")
						return
					}
					s, ok := n.Args[1].(*parse.StringNode)
					if !ok {
						walkErr = fmt.Errorf("step takes a step id in quotes, not %s", n.Args[1])
						return
					}
					refs = append(refs, s.Text)
					continue
				}
				walk(a)
			}
		}
	}
	for _, tt := range t.Templates() {
		if tt.Tree != nil {
			walk(tt.Tree.Root)
		}
	}
	return refs, walkErr
}

// Rendered is a step's child as it would be created, without its marker.
type Rendered struct {
	Title string
	// Body is the rendered body, with the review instructions appended
	// under ## Review Instructions when the step has them.
	Body string
	// Review is the rendered review instructions; empty, the PR is not
	// reviewed automatically.
	Review string
}

// renderStep renders a step's title, body and review with params and the
// earlier steps' children.
func (d *Definition) renderStep(s Step, params map[string]string, refs map[string]StepRef) (Rendered, error) {
	var r Rendered
	var err error
	if r.Title, err = render("title", s.Title, params, refs); err != nil {
		return Rendered{}, fmt.Errorf("step %q: title: %w", s.ID, err)
	}
	r.Title = strings.Join(strings.Fields(r.Title), " ")
	if r.Title == "" {
		return Rendered{}, fmt.Errorf("step %q: title renders empty", s.ID)
	}
	body, err := render("body", s.Body, params, refs)
	if err != nil {
		return Rendered{}, fmt.Errorf("step %q: body: %w", s.ID, err)
	}
	r.Body = strings.TrimSpace(body)
	if s.Review != "" {
		review, err := render("review", s.Review, params, refs)
		if err != nil {
			return Rendered{}, fmt.Errorf("step %q: review: %w", s.ID, err)
		}
		r.Review = strings.TrimSpace(review)
	}
	if r.Review != "" {
		if r.Body != "" {
			r.Body += "\n\n"
		}
		r.Body += "## Review Instructions\n\n" + r.Review
	}
	return r, nil
}

// splitWords lower-cases s and joins its words with sep, splitting
// CamelCase as well as spaces, dashes and underscores:
// SpannerInstanceConfig -> spanner-instance-config.
func splitWords(s, sep string) string {
	var words []string
	var cur []rune
	runes := []rune(s)
	flush := func() {
		if len(cur) > 0 {
			words = append(words, strings.ToLower(string(cur)))
			cur = cur[:0]
		}
	}
	for i, r := range runes {
		switch {
		case r == ' ' || r == '-' || r == '_' || r == '.':
			flush()
			continue
		case unicode.IsUpper(r) && len(cur) > 0:
			prevLower := unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1])
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if prevLower || (unicode.IsUpper(runes[i-1]) && nextLower) {
				flush()
			}
		}
		cur = append(cur, r)
	}
	flush()
	return strings.Join(words, sep)
}

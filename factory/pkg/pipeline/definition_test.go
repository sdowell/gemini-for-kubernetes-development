package pipeline

import (
	"strings"
	"testing"
)

const greenfield = `apiVersion: factory.gemini.google.com/v1alpha1
kind: IssuePipeline
metadata:
  name: kcc-greenfield
  description: Drive a KCC greenfield resource to a direct controller.
spec:
  params:
    - name: kind
      description: CamelCase KCC Kind
      pattern: '^[A-Z][A-Za-z0-9]{1,62}$'
      example: SpannerInstanceConfig
    - name: version
      required: false
      enum: [v1alpha1, v1beta1]
      default: v1alpha1
    - name: note
      required: false
      pattern: '^[a-z]+$'
  defaults:
    labels: [greenfield]
  steps:
    - id: gen-types
      title: 'Greenfield: types for {{ .Params.kind }}'
      labels: [step/gen-types]
      review: |
        Check the types for {{ .Params.kind }}.
      body: |
        Generate the types at {{ .Params.version }}.
        {{ if .Params.note }}Note: {{ .Params.note }}{{ end }}
    - id: controller
      title: 'Greenfield: controller for {{ .Params.kind | kebab }}'
      body: |
        The types were added in #{{ (step "gen-types").Issue }}.
`

func mustParse(t *testing.T, src string) *Definition {
	t.Helper()
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return d
}

func TestParseValid(t *testing.T) {
	d := mustParse(t, greenfield)
	if d.Metadata.Name != "kcc-greenfield" || len(d.Spec.Steps) != 2 || len(d.Spec.Params) != 3 {
		t.Fatalf("unexpected definition: %+v", d)
	}
	if !d.Spec.Steps[0].CloseOnLinkedPRMerge() {
		t.Error("closeOnLinkedPRMerge should default on")
	}
}

func TestParseInvalid(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(string) string
		want string
	}{
		"unknown field": {func(s string) string {
			return strings.Replace(s, "  defaults:", "  bogus: 1\n  defaults:", 1)
		}, "bogus"},
		"wrong kind":        {func(s string) string { return strings.Replace(s, "kind: IssuePipeline", "kind: Pipeline", 1) }, "kind"},
		"duplicate step id": {func(s string) string { return strings.Replace(s, "id: controller", "id: gen-types", 1) }, "used twice"},
		"unconstrained param": {func(s string) string {
			return strings.Replace(s, "      pattern: '^[a-z]+$'\n", "", 1)
		}, "needs a pattern or an enum"},
		"unanchored pattern": {func(s string) string {
			return strings.Replace(s, "'^[a-z]+$'", "'[a-z]+'", 1)
		}, "anchored"},
		"default on required": {func(s string) string {
			return strings.Replace(s, "      example: SpannerInstanceConfig\n", "      example: SpannerInstanceConfig\n      default: Foo\n", 1)
		}, "only allowed on an optional param"},
		"default breaks enum": {func(s string) string { return strings.Replace(s, "default: v1alpha1", "default: v2", 1) }, "not one of"},
		"later step ref": {func(s string) string {
			return strings.Replace(s, "Generate the types", `See #{{ (step "controller").Issue }}. Generate the types`, 1)
		}, "not an earlier step"},
		"self step ref": {func(s string) string {
			return strings.Replace(s, `(step "gen-types")`, `(step "controller")`, 1)
		}, "not an earlier step"},
		"non-literal step ref": {func(s string) string {
			return strings.Replace(s, `(step "gen-types")`, `(step .Params.kind)`, 1)
		}, "in quotes"},
		"unknown param":   {func(s string) string { return strings.Replace(s, ".Params.version", ".Params.versoin", 1) }, "versoin"},
		"template syntax": {func(s string) string { return strings.Replace(s, "{{ .Params.kind }}'", "{{ .Params.kind '", 1) }, "title"},
		"dependsOn reserved": {func(s string) string {
			return strings.Replace(s, "    - id: controller\n", "    - id: controller\n      dependsOn: [gen-types]\n", 1)
		}, "reserved"},
		"bad name": {func(s string) string { return strings.Replace(s, "name: kcc-greenfield", "name: ../evil", 1) }, "metadata.name"},
		"nested pipeline block": {func(s string) string {
			return strings.Replace(s, "        Generate the types", "        ```factory-pipeline\n        pipeline: x\n        ```\n        Generate the types", 1)
		}, "factory-pipeline block"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.edit(greenfield)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Parse error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	d := mustParse(t, greenfield)
	got, err := d.Resolve(map[string]string{"kind": "Foo"})
	if err != nil {
		t.Fatal(err)
	}
	if got["kind"] != "Foo" || got["version"] != "v1alpha1" || got["note"] != "" {
		t.Errorf("Resolve = %v", got)
	}
	for name, supplied := range map[string]map[string]string{
		"missing required": {},
		"unknown":          {"kind": "Foo", "extra": "x"},
		"bad pattern":      {"kind": "foo"},
		"bad enum":         {"kind": "Foo", "version": "v2"},
		"newline":          {"kind": "Foo\nBar"},
	} {
		if _, err := d.Resolve(supplied); err == nil {
			t.Errorf("%s: Resolve(%v) succeeded", name, supplied)
		}
	}
}

func TestParamsHashStable(t *testing.T) {
	a := ParamsHash(map[string]string{"a": "1", "b": "2"})
	b := ParamsHash(map[string]string{"b": "2", "a": "1"})
	if a != b || len(a) != 16 {
		t.Errorf("hashes %q %q", a, b)
	}
	if a == ParamsHash(map[string]string{"a": "1", "b": "3"}) {
		t.Error("hash ignores a value")
	}
	if ParamsHash(map[string]string{"a": "1=b"}) == ParamsHash(map[string]string{"a": "1", "b": ""}) {
		t.Error("hash is ambiguous")
	}
}

func TestParseRequest(t *testing.T) {
	body := "Onboard it.\n\n```factory-pipeline\npipeline: kcc-greenfield\nparams:\n  kind: SpannerInstanceConfig\n```\n"
	r, err := ParseRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	if r.Pipeline != "kcc-greenfield" || r.Params["kind"] != "SpannerInstanceConfig" {
		t.Errorf("ParseRequest = %+v", r)
	}
	if !HasRequest(body) {
		t.Error("HasRequest = false")
	}

	for name, body := range map[string]string{
		"none":          "just text",
		"path":          "```factory-pipeline\npipeline: ../x\n```",
		"url":           "```factory-pipeline\npipeline: https://example.com/x.yaml\n```",
		"other repo":    "```factory-pipeline\npipeline: owner/repo/x\n```",
		"unknown field": "```factory-pipeline\npipeline: x\nextra: 1\n```",
		"two blocks":    "```factory-pipeline\npipeline: x\n```\n```factory-pipeline\npipeline: y\n```",
		"unclosed":      "```factory-pipeline\npipeline: x\n",
		"nested params": "```factory-pipeline\npipeline: x\nparams:\n  kind: {a: b}\n```",
	} {
		if _, err := ParseRequest(body); err == nil {
			t.Errorf("%s: ParseRequest succeeded", name)
		}
	}

	quoted := "Example:\n~~~~markdown\n```factory-pipeline\npipeline: x\n```\n~~~~\n"
	if HasRequest(quoted) {
		t.Error("a block quoted inside another fence counts as a request")
	}
}

func TestMarkerRoundTrip(t *testing.T) {
	m := Marker{Parent: 12, Pipeline: "kcc-greenfield", Step: "gen-types", Params: ParamsHash(nil)}
	got, ok := ParseMarker("body\n\n" + m.String() + "\n")
	if !ok || got != m {
		t.Errorf("ParseMarker = %+v, %v; want %+v", got, ok, m)
	}
	if _, ok := ParseMarker("<!-- factory:fanout parent=1 items=a -->"); ok {
		t.Error("a fan-out marker parses as a pipeline marker")
	}
}

func TestRenderStep(t *testing.T) {
	d := mustParse(t, greenfield)
	params, _ := d.Resolve(map[string]string{"kind": "SpannerInstanceConfig"})
	r, err := d.renderStep(d.Spec.Steps[0], params, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Title != "Greenfield: types for SpannerInstanceConfig" {
		t.Errorf("title %q", r.Title)
	}
	if !strings.Contains(r.Body, "## Review Instructions\n\nCheck the types for SpannerInstanceConfig.") {
		t.Errorf("body lacks review instructions:\n%s", r.Body)
	}
	r, err = d.renderStep(d.Spec.Steps[1], params, map[string]StepRef{"gen-types": {Issue: 42}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Title != "Greenfield: controller for spanner-instance-config" || !strings.Contains(r.Body, "#42") {
		t.Errorf("rendered %+v", r)
	}
	if _, err := d.renderStep(d.Spec.Steps[1], params, nil); err == nil {
		t.Error("rendering without the earlier step's child succeeded")
	}
}

func TestSplitWords(t *testing.T) {
	for in, want := range map[string]string{
		"SpannerInstanceConfig": "spanner-instance-config",
		"IAMPolicy":             "iam-policy",
		"already-kebab":         "already-kebab",
		"v1Beta1":               "v1-beta1",
	} {
		if got := splitWords(in, "-"); got != want {
			t.Errorf("splitWords(%q) = %q, want %q", in, got, want)
		}
	}
}

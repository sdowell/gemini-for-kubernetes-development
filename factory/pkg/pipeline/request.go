package pipeline

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// fenceInfo is the info string of the block that starts a pipeline.
const fenceInfo = "factory-pipeline"

// Request is what a parent's factory-pipeline block asks for.
type Request struct {
	// Pipeline is a bare name, resolved to Path(Pipeline) in the watched
	// repository.
	Pipeline string `yaml:"pipeline"`
	// Params are the values the parent supplies.
	Params map[string]string `yaml:"params,omitempty"`
}

// fenceRe matches a line opening or closing a fenced code block.
var fenceRe = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})[ \t]*([^`\\s]*)[ \t]*$")

// HasRequest reports whether body has a factory-pipeline block, valid or
// not: such an issue is a pipeline parent, never worked on as a task.
func HasRequest(body string) bool {
	return len(requestBlocks(body)) > 0
}

// block is one factory-pipeline fenced block.
type block struct {
	content string
	closed  bool
}

// requestBlocks returns the body's factory-pipeline blocks. A fence quoted
// inside another fenced block is that block's content, not a request.
func requestBlocks(body string) []block {
	var blocks []block
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		m := fenceRe.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		fence, info := m[1], m[2]
		// Skip to the end of this block, whatever it is.
		var content []string
		closed := false
		for i++; i < len(lines); i++ {
			if c := fenceRe.FindStringSubmatch(lines[i]); c != nil && c[2] == "" && c[1][0] == fence[0] && len(c[1]) >= len(fence) {
				closed = true
				break
			}
			content = append(content, lines[i])
		}
		if info == fenceInfo {
			blocks = append(blocks, block{content: strings.Join(content, "\n"), closed: closed})
		}
	}
	return blocks
}

// ParseRequest reads the factory-pipeline block from a parent's body. It
// fails if there is none, more than one, or it does not validate.
func ParseRequest(body string) (*Request, error) {
	blocks := requestBlocks(body)
	switch len(blocks) {
	case 0:
		return nil, fmt.Errorf("no %s block", fenceInfo)
	case 1:
	default:
		return nil, fmt.Errorf("%d %s blocks; a parent has exactly one", len(blocks), fenceInfo)
	}
	if !blocks[0].closed {
		return nil, fmt.Errorf("the %s block is not closed", fenceInfo)
	}

	dec := yaml.NewDecoder(strings.NewReader(blocks[0].content))
	dec.KnownFields(true)
	var r Request
	if err := dec.Decode(&r); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("the %s block is empty", fenceInfo)
		}
		return nil, fmt.Errorf("the %s block: %w", fenceInfo, err)
	}
	if r.Pipeline == "" {
		return nil, fmt.Errorf("the %s block names no pipeline", fenceInfo)
	}
	if !ValidName(r.Pipeline) {
		return nil, fmt.Errorf("pipeline %q is not a bare name: paths, URLs and other repositories are not allowed; it resolves to %s/<name>.yaml in this repository", r.Pipeline, Dir)
	}
	return &r, nil
}

// Resolve checks the params a parent supplies against the definition and
// returns every param's value: an omitted optional param takes its default,
// or the empty string. Unknown params, missing required ones and values
// that break their constraint are errors.
func (d *Definition) Resolve(supplied map[string]string) (map[string]string, error) {
	var errs []error
	known := map[string]bool{}
	out := map[string]string{}
	for _, p := range d.Spec.Params {
		known[p.Name] = true
		v, ok := supplied[p.Name]
		switch {
		case ok:
			if err := p.check(v); err != nil {
				errs = append(errs, fmt.Errorf("param %q: %v", p.Name, err))
				continue
			}
			out[p.Name] = v
		case p.IsRequired():
			errs = append(errs, fmt.Errorf("param %q is required", p.Name))
		case p.Default != nil:
			out[p.Name] = *p.Default
		default:
			out[p.Name] = ""
		}
	}
	var unknown []string
	for k := range supplied {
		if !known[k] {
			unknown = append(unknown, k)
		}
	}
	slices.Sort(unknown)
	for _, k := range unknown {
		errs = append(errs, fmt.Errorf("param %q is not one of the pipeline's", k))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

// ParamsHash is a short, stable hash of the params a parent supplied, which
// each child records so that a parent edited after start is noticed.
// Defaults are not part of it: only the supplied params are frozen.
func ParamsHash(supplied map[string]string) string {
	keys := make([]string, 0, len(supplied))
	for k := range supplied {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", strconv.Quote(k), strconv.Quote(supplied[k]))
	}
	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:])[:16]
}

package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
)

const (
	// StatusMarker marks the reconciler's status comment on a parent.
	StatusMarker = "<!-- factory:pipeline-status -->"
	// CompleteMarker, in the status comment, records that the pipeline
	// finished, so completion happens exactly once.
	CompleteMarker = "<!-- factory:pipeline-complete -->"
)

// Marker is the hidden marker in a child's body that ties it to its parent
// and step.
type Marker struct {
	Parent   int
	Pipeline string
	Step     string
	// Params is ParamsHash of the params the parent supplied when the child
	// was rendered.
	Params string
}

// markerRe matches a child's marker:
// <!-- factory:pipeline parent=N pipeline=NAME step=ID params=HASH -->
var markerRe = regexp.MustCompile(`<!--\s*factory:pipeline\s+parent=(\d+)\s+pipeline=([a-z0-9-]+)\s+step=([a-z0-9-]+)\s+params=([0-9a-f]+)\s*-->`)

// String renders the marker as it is written into a child's body.
func (m Marker) String() string {
	return fmt.Sprintf("<!-- factory:pipeline parent=%d pipeline=%s step=%s params=%s -->", m.Parent, m.Pipeline, m.Step, m.Params)
}

// ParseMarker reads a child's marker from its body.
func ParseMarker(body string) (Marker, bool) {
	m := markerRe.FindStringSubmatch(body)
	if m == nil {
		return Marker{}, false
	}
	parent, err := strconv.Atoi(m[1])
	if err != nil || parent <= 0 {
		return Marker{}, false
	}
	return Marker{Parent: parent, Pipeline: m[2], Step: m[3], Params: m[4]}, true
}

// childBody is a rendered body with its marker appended.
func childBody(body string, m Marker) string {
	if body == "" {
		return m.String() + "\n"
	}
	return body + "\n\n" + m.String() + "\n"
}

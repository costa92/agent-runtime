// Package observe is the Runtime's observability contract.
//
// Events here are not log lines. They are the record a consumer reconstructs a
// Run's reasoning from — why this model, why this refusal, why this wait — so
// they are declared, versioned and validated like any other published surface.
// An event nobody declared is an event nobody can depend on, and one whose
// payload changed meaning without a version is worse: every existing consumer
// keeps parsing it and quietly draws the wrong conclusion.
package observe

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Stability says what a consumer may rely on.
type Stability string

const (
	// StableEvent: the name and its declared fields will not change meaning.
	// Fields may be added; anything else needs a new apiVersion.
	StableEvent Stability = "stable"
	// ExperimentalEvent: may change or disappear. Declared anyway, so that
	// "this is not settled yet" is a fact a consumer can read rather than
	// discover.
	ExperimentalEvent Stability = "experimental"
)

// EventSpec declares one emittable event.
type EventSpec struct {
	Name       string
	APIVersion string
	Stability  Stability
	// Fields is the closed set of attribute keys this event may carry. It is
	// closed rather than advisory because that is the mechanism that keeps
	// credentials and business payload out: an attribute nobody declared cannot
	// be emitted, so leaking one requires declaring it in review.
	Fields []string
}

func (s EventSpec) validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return run.NewError("unnamed_event", run.ErrorInvalid, run.RetryNever)
	}
	if !apiVersionPattern(s.APIVersion) {
		return run.NewError("unparseable_api_version", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q: %q", s.Name, s.APIVersion))
	}
	if s.Stability != StableEvent && s.Stability != ExperimentalEvent {
		return run.NewError("undeclared_stability", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q", s.Name))
	}
	seen := map[string]bool{}
	for _, field := range s.Fields {
		if seen[field] {
			return run.NewError("duplicate_event_field", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("event %q declares %q twice", s.Name, field))
		}
		seen[field] = true
	}
	return nil
}

// apiVersionPattern accepts v1, v2, v1alpha1 — the same shape the resource
// family uses, spelled out here rather than imported so that observe does not
// depend on the resource package for one regexp.
func apiVersionPattern(version string) bool {
	if len(version) < 2 || version[0] != 'v' || version[1] < '1' || version[1] > '9' {
		return false
	}
	rest := strings.TrimLeft(version[1:], "0123456789")
	switch {
	case rest == "":
		return true
	case strings.HasPrefix(rest, "alpha"), strings.HasPrefix(rest, "beta"):
		suffix := strings.TrimPrefix(strings.TrimPrefix(rest, "alpha"), "beta")
		return suffix != "" && strings.Trim(suffix, "0123456789") == ""
	default:
		return false
	}
}

// EventSpecRegistry is the frozen set of events a deployment may emit.
//
// It is frozen at construction rather than accumulated at run time: a registry
// that could still grow would make "is this event declared" a question with a
// different answer depending on which code path had run first.
type EventSpecRegistry struct {
	specs map[string]EventSpec
}

// NewEventSpecRegistry freezes a set of specs, refusing duplicates.
func NewEventSpecRegistry(specs ...EventSpec) (*EventSpecRegistry, error) {
	registry := &EventSpecRegistry{specs: make(map[string]EventSpec, len(specs))}
	for _, spec := range specs {
		if err := spec.validate(); err != nil {
			return nil, err
		}
		if _, exists := registry.specs[spec.Name]; exists {
			return nil, run.NewError("duplicate_event_spec", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("event %q declared twice", spec.Name))
		}
		registry.specs[spec.Name] = spec
	}
	return registry, nil
}

// Lookup returns a spec, or an error for an event nobody declared.
func (r *EventSpecRegistry) Lookup(name string) (EventSpec, error) {
	spec, ok := r.specs[name]
	if !ok {
		return EventSpec{}, run.NewError("undeclared_event", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q is not in the frozen registry", name))
	}
	return spec, nil
}

// Names returns every declared event, sorted.
func (r *EventSpecRegistry) Names() []string {
	names := make([]string, 0, len(r.specs))
	for name := range r.specs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// CheckCompatible reports whether a spec change is one existing consumers
// survive.
//
// Adding a field is safe: a consumer that does not know it ignores it. Removing
// or renaming one is not, and neither is changing the event's meaning — both
// require a new apiVersion, which is how a consumer finds out it has to change
// instead of silently misreading the payload.
func CheckCompatible(previous, next EventSpec) error {
	if previous.Name != next.Name {
		return run.NewError("renamed_event", run.ErrorInvalid, run.RetryNever)
	}
	if previous.Stability != StableEvent {
		// Nothing was promised, so nothing is broken.
		return nil
	}
	if previous.APIVersion != next.APIVersion {
		// A new apiVersion is the sanctioned way to make a breaking change.
		return nil
	}

	declared := map[string]bool{}
	for _, field := range next.Fields {
		declared[field] = true
	}
	var dropped []string
	for _, field := range previous.Fields {
		if !declared[field] {
			dropped = append(dropped, field)
		}
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		return run.NewError("breaking_event_change", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q dropped %v without a new apiVersion", previous.Name, dropped))
	}
	return nil
}

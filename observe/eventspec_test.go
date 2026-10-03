package observe_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

func builtin(t *testing.T) *observe.EventSpecRegistry {
	t.Helper()
	registry, err := observe.NewEventSpecRegistry(observe.BuiltinEventSpecs()...)
	if err != nil {
		t.Fatalf("the builtin event set does not validate: %v", err)
	}
	return registry
}

func code(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("no error")
	}
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) {
		t.Fatalf("not a run.Error: %v", err)
	}
	return runtimeError.Code
}

// An event nobody declared has no consumer contract. Emitting one is how a
// payload nothing describes reaches durable storage.
func TestUndeclaredEventsAreRefused(t *testing.T) {
	registry := builtin(t)

	err := registry.Validate(observe.Decision{Name: "runtime.something.improvised", RunID: "run-1"})
	if got := code(t, err); got != "undeclared_event" {
		t.Fatalf("code=%q", got)
	}
}

// The closed field set is the mechanism that keeps secrets out. An attribute
// nobody declared cannot be recorded, so leaking one takes a reviewed change.
func TestUndeclaredAttributesAreRefused(t *testing.T) {
	registry := builtin(t)

	err := registry.Validate(observe.Decision{
		Name:  observe.EventPolicyEvaluated,
		RunID: "run-1",
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrDecision, "deny"),
			observe.Attr("authorization_header", "Bearer sk-live-1"),
		},
	})
	if got := code(t, err); got != "undeclared_event_attribute" {
		t.Fatalf("code=%q", got)
	}
	if !strings.Contains(err.Error(), "authorization_header") {
		t.Errorf("the refusal does not name the offending attribute: %v", err)
	}
}

// No builtin event may carry a credential, a claim, or business payload. This
// reads the declarations rather than the emissions: a field that exists will
// eventually be filled.
func TestNoBuiltinEventDeclaresASensitiveField(t *testing.T) {
	forbidden := []string{
		"token", "secret", "credential", "password", "api_key", "authorization",
		"claims", "prompt", "content", "arguments", "output", "payload", "input",
	}

	for _, spec := range observe.BuiltinEventSpecs() {
		for _, field := range spec.Fields {
			for _, pattern := range forbidden {
				if strings.Contains(strings.ToLower(field), pattern) {
					t.Errorf("event %q declares %q, which matches %q", spec.Name, field, pattern)
				}
			}
		}
	}
}

// Every governance decision the Runtime makes must be an event. A decision that
// was only logged cannot answer "why did this Run refuse" a week later.
func TestEveryGovernanceDecisionIsDeclared(t *testing.T) {
	registry := builtin(t)

	required := []string{
		observe.EventRouterPlanSelected,
		observe.EventModelSelected,
		observe.EventPolicyEvaluated,
		observe.EventQuotaRejected,
		observe.EventQuotaDegraded,
		observe.EventQuotaUnreadable,
		observe.EventBudgetRefused,
		observe.EventApprovalRequested,
		observe.EventApprovalDecided,
		observe.EventEgressHostResolved,
	}
	for _, name := range required {
		if _, err := registry.Lookup(name); err != nil {
			t.Errorf("no spec for %q", name)
		}
	}
}

// The policy event has to identify the rule that decided, not just the outcome.
// "Denied" without a rule name is an answer nobody can act on.
func TestThePolicyEventIdentifiesTheRuleThatDecided(t *testing.T) {
	registry := builtin(t)

	if err := registry.Validate(observe.Decision{
		Name:  observe.EventPolicyEvaluated,
		RunID: "run-1",
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrTool, "publish"),
			observe.Attr(observe.AttrDecision, "deny"),
			observe.Attr(observe.AttrPolicyName, "no-publish"),
			observe.Attr(observe.AttrPolicyDigest, "snap-1"),
			observe.Attr(observe.AttrShadow, "false"),
		},
	}); err != nil {
		t.Fatalf("a fully attributed policy decision was refused: %v", err)
	}
}

func TestAnEventMustNameItsRun(t *testing.T) {
	registry := builtin(t)

	err := registry.Validate(observe.Decision{Name: observe.EventBudgetRefused})
	if got := code(t, err); got != "unattributed_event" {
		t.Fatalf("code=%q", got)
	}
}

func TestDuplicateAttributesAreRefused(t *testing.T) {
	registry := builtin(t)

	err := registry.Validate(observe.Decision{
		Name:  observe.EventApprovalDecided,
		RunID: "run-1",
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrApproved, "true"),
			observe.Attr(observe.AttrApproved, "false"),
		},
	})
	if got := code(t, err); got != "duplicate_event_attribute" {
		t.Fatalf("code=%q; which of the two values would a consumer read?", got)
	}
}

func TestMalformedSpecsAreRefusedAtConstruction(t *testing.T) {
	cases := map[string]observe.EventSpec{
		"no name":         {APIVersion: "v1", Stability: observe.StableEvent},
		"no api version":  {Name: "runtime.x", Stability: observe.StableEvent},
		"bad api version": {Name: "runtime.x", APIVersion: "1.0", Stability: observe.StableEvent},
		"no stability":    {Name: "runtime.x", APIVersion: "v1"},
		"duplicate field": {
			Name: "runtime.x", APIVersion: "v1", Stability: observe.StableEvent,
			Fields: []string{"a", "a"},
		},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := observe.NewEventSpecRegistry(spec); err == nil {
				t.Fatal("a malformed spec was frozen into the registry")
			}
		})
	}
}

func TestDuplicateSpecsAreRefused(t *testing.T) {
	spec := observe.EventSpec{Name: "runtime.x", APIVersion: "v1", Stability: observe.StableEvent}

	_, err := observe.NewEventSpecRegistry(spec, spec)
	if got := code(t, err); got != "duplicate_event_spec" {
		t.Fatalf("code=%q", got)
	}
}

// A consumer that does not know a new field ignores it. A consumer whose field
// disappeared breaks — so that needs a new apiVersion, which is how it finds
// out.
func TestStableEventsMayGainFieldsButNotLoseThem(t *testing.T) {
	previous := observe.EventSpec{
		Name: "runtime.x", APIVersion: "v1", Stability: observe.StableEvent,
		Fields: []string{"a", "b"},
	}

	added := previous
	added.Fields = []string{"a", "b", "c"}
	if err := observe.CheckCompatible(previous, added); err != nil {
		t.Fatalf("adding an optional field was treated as breaking: %v", err)
	}

	dropped := previous
	dropped.Fields = []string{"a"}
	if got := code(t, observe.CheckCompatible(previous, dropped)); got != "breaking_event_change" {
		t.Fatalf("code=%q; dropping a field is breaking", got)
	}

	versioned := dropped
	versioned.APIVersion = "v2"
	if err := observe.CheckCompatible(previous, versioned); err != nil {
		t.Fatalf("a new apiVersion is the sanctioned way to break: %v", err)
	}
}

func TestExperimentalEventsPromiseNothing(t *testing.T) {
	previous := observe.EventSpec{
		Name: "runtime.x", APIVersion: "v1", Stability: observe.ExperimentalEvent,
		Fields: []string{"a", "b"},
	}
	next := previous
	next.Fields = nil

	if err := observe.CheckCompatible(previous, next); err != nil {
		t.Fatalf("an experimental event was held to a stable contract: %v", err)
	}
}

func TestRenamingAnEventIsNeverCompatible(t *testing.T) {
	previous := observe.EventSpec{Name: "runtime.x", APIVersion: "v1", Stability: observe.StableEvent}
	next := previous
	next.Name = "runtime.y"

	if got := code(t, observe.CheckCompatible(previous, next)); got != "renamed_event" {
		t.Fatalf("code=%q", got)
	}
}

// The Recorder is the only path to an Observer, so an invalid decision must not
// reach one — that is the whole reason it sits in the middle.
func TestTheRecorderDropsWhatTheRegistryRefuses(t *testing.T) {
	observer := &capturingObserver{}
	recorder := observe.NewRecorder(builtin(t), observer)

	if err := recorder.Record(t.Context(), observe.Decision{Name: "runtime.improvised", RunID: "run-1"}); err == nil {
		t.Fatal("an undeclared event was accepted")
	}
	if len(observer.decisions) != 0 {
		t.Fatalf("the observer received %d refused decisions", len(observer.decisions))
	}

	valid := observe.Decision{
		Name: observe.EventApprovalDecided, RunID: "run-1",
		Attributes: []observe.Attribute{observe.Attr(observe.AttrApproved, "true")},
	}
	if err := recorder.Record(t.Context(), valid); err != nil {
		t.Fatalf("a declared event was refused: %v", err)
	}
	if len(observer.decisions) != 1 {
		t.Fatalf("decisions=%d want=1", len(observer.decisions))
	}
}

func TestANilObserverIsNotACrash(t *testing.T) {
	recorder := observe.NewRecorder(builtin(t), nil)

	if err := recorder.Record(t.Context(), observe.Decision{
		Name: observe.EventApprovalDecided, RunID: "run-1",
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	recorder.Chunk("run-1", "text")
}

// A resumed Run must keep the trace it started, or a Run that waited becomes
// two unrelated traces.
func TestTheNopTracerStillCarriesTheParentContext(t *testing.T) {
	parent := observe.TraceContext{TraceID: "trace-1", SpanID: "span-1", Sampled: true}

	_, span := observe.NopTracer{}.Start(t.Context(), observe.SpanRequest{
		Kind: observe.SpanResumed, RunID: "run-1", Parent: parent,
	})
	if span.Context() != parent {
		t.Fatalf("context=%+v want=%+v", span.Context(), parent)
	}
	span.End(nil)
	span.End(nil)
}

type capturingObserver struct {
	decisions []observe.Decision
}

func (o *capturingObserver) Decision(_ context.Context, decision observe.Decision) error {
	o.decisions = append(o.decisions, decision)
	return nil
}

func (o *capturingObserver) Chunk(run.ID, string) {}

package tool

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
)

type capturingRecorder struct {
	mu        sync.Mutex
	decisions []DecisionRecord
	results   []ResultRecord
}

func (r *capturingRecorder) Decided(_ context.Context, d DecisionRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.decisions = append(r.decisions, d)
}

func (r *capturingRecorder) Finished(_ context.Context, res ResultRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, res)
}

func (r *capturingRecorder) onlyDecision(t *testing.T) DecisionRecord {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.decisions) != 1 {
		t.Fatalf("recorded %d decisions, want exactly one per call", len(r.decisions))
	}
	return r.decisions[0]
}

type staticAuthorizer struct{ err error }

func (a staticAuthorizer) Authorize(context.Context, authorization.PrincipalRef, Spec) error {
	return a.err
}

type stubHandler struct {
	output []byte
	err    error
}

func (h stubHandler) Invoke(context.Context, Invocation) (Result, error) {
	if h.err != nil {
		return Result{}, h.err
	}
	return Result{Output: h.output}, nil
}

func recordingGateway(t *testing.T, authorizer Authorizer, handler Handler) (*Gateway, *capturingRecorder) {
	t.Helper()
	registry := NewRegistry()
	spec := Spec{
		Name: "list_articles", RiskLevel: policy.RiskLow,
		SideEffect: policy.SideEffectRead, Parameters: json.RawMessage(`{}`),
	}
	if err := registry.Register(spec, handler); err != nil {
		t.Fatal(err)
	}
	recorder := &capturingRecorder{}
	return NewGateway(registry, authorizer, WithRecorder(recorder)), recorder
}

func request() InvocationRequest {
	return InvocationRequest{
		RunID: "run-abc", InvocationID: "inv-1", Tool: "list_articles",
		Arguments: json.RawMessage(`{}`),
		Allowlist: []string{"list_articles"},
	}
}

// A call the chain refused never reaches a handler, so anything watching
// handlers cannot see it. That is the case the recorder exists for: "which
// calls were refused, and where" is most of what an audit is read for, and it
// used to be unavailable to the host at all.
func TestRecorderSeesRefusedCalls(t *testing.T) {
	denied := errors.New("principal lacks article:read")
	gateway, recorder := recordingGateway(t, staticAuthorizer{err: denied}, stubHandler{})

	if _, err := gateway.Prepare(context.Background(), request()); err == nil {
		t.Fatal("Prepare succeeded, want the authorizer's refusal")
	}
	decision := recorder.onlyDecision(t)
	if decision.Allowed() {
		t.Error("the refusal was recorded as allowed")
	}
	if decision.Stage != StageAuthorize {
		t.Errorf("stage = %q, want %q — an audit that cannot say where it stopped cannot say why",
			decision.Stage, StageAuthorize)
	}
	if !errors.Is(decision.Err, denied) {
		t.Errorf("err = %v, want the authorizer's reason", decision.Err)
	}
	if decision.RunID != "run-abc" || decision.InvocationID != "inv-1" {
		t.Errorf("record = run %q invocation %q, want the call's own ids",
			decision.RunID, decision.InvocationID)
	}
	if len(recorder.results) != 0 {
		t.Errorf("a refused call reported a result: %+v", recorder.results)
	}
}

// A tool nobody registered stops at the first stage, before there is a Spec to
// describe it. It is still an attempted call and must not vanish.
func TestRecorderSeesCallsForUnknownTools(t *testing.T) {
	gateway, recorder := recordingGateway(t, staticAuthorizer{}, stubHandler{})
	unknown := request()
	unknown.Tool = "delete_everything"

	if _, err := gateway.Prepare(context.Background(), unknown); err == nil {
		t.Fatal("Prepare accepted an unregistered tool")
	}
	decision := recorder.onlyDecision(t)
	if decision.Stage != StageResolve {
		t.Errorf("stage = %q, want %q", decision.Stage, StageResolve)
	}
	if decision.Tool != "delete_everything" {
		t.Errorf("tool = %q, want the name that was asked for", decision.Tool)
	}
}

func TestRecorderReportsWhatTheEffectDid(t *testing.T) {
	gateway, recorder := recordingGateway(t, staticAuthorizer{}, stubHandler{output: []byte(`{"items":[]}`)})

	prepared, err := gateway.Prepare(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if decision := recorder.onlyDecision(t); !decision.Allowed() {
		t.Fatalf("an allowed call recorded a refusal: %v", decision.Err)
	}
	if _, err := gateway.Execute(context.Background(), CommittedInvocation{Prepared: prepared}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.results) != 1 {
		t.Fatalf("recorded %d results, want one", len(recorder.results))
	}
	result := recorder.results[0]
	if result.Outcome != run.OutcomeApplied {
		t.Errorf("outcome = %q, want applied", result.Outcome)
	}
	if result.ResultBytes != len(`{"items":[]}`) {
		t.Errorf("bytes = %d, want the size that reached the prompt", result.ResultBytes)
	}
	if result.RunID != "run-abc" {
		t.Errorf("run = %q, want the call's Run", result.RunID)
	}
	if result.Duration <= 0 {
		t.Error("duration was not measured")
	}
}

// A handler that failed with an unknown outcome is the one an operator most
// needs to find: the effect may or may not have happened, and no later read can
// settle it. Recording only successes would hide exactly these.
func TestRecorderReportsFailedEffects(t *testing.T) {
	boom := run.NewError("tool.exploded", run.ErrorUnknown, run.RetryNever)
	gateway, recorder := recordingGateway(t, staticAuthorizer{}, stubHandler{err: boom})

	prepared, err := gateway.Prepare(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Execute(context.Background(), CommittedInvocation{Prepared: prepared}); err == nil {
		t.Fatal("Execute hid the handler's failure")
	}
	if len(recorder.results) != 1 {
		t.Fatalf("recorded %d results, want the failure recorded", len(recorder.results))
	}
	if got := recorder.results[0].Outcome; got != run.OutcomeUnknown {
		t.Errorf("outcome = %q, want unknown", got)
	}
	if recorder.results[0].Err == nil {
		t.Error("the failure was recorded without its reason")
	}
}

// A host that wires no recorder is a supported deployment; it must lose the
// audit and nothing else.
func TestGatewayWorksWithoutARecorder(t *testing.T) {
	registry := NewRegistry()
	spec := Spec{Name: "list_articles", RiskLevel: policy.RiskLow, SideEffect: policy.SideEffectRead}
	if err := registry.Register(spec, stubHandler{output: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	gateway := NewGateway(registry, staticAuthorizer{})

	prepared, err := gateway.Prepare(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Execute(context.Background(), CommittedInvocation{Prepared: prepared}); err != nil {
		t.Fatal(err)
	}
}

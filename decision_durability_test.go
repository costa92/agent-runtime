package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/observe"
	"github.com/costa92/agent-runtime/run"
)

type refusingDecisionObserver struct{ refused error }

func (o refusingDecisionObserver) Decision(context.Context, observe.Decision) error { return o.refused }
func (refusingDecisionObserver) Chunk(run.ID, string)                               {}

func TestAuditWriteFailureStopsModelBeforeExternalCall(t *testing.T) {
	refused := errors.New("audit database unavailable")
	calls := 0
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, err := request.Ports.Model(ctx, llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}}})
		return agent.Response{Output: json.RawMessage(`"done"`)}, err
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Observer = refusingDecisionObserver{refused: refused}
		deps.Models = capturingModels{onRequest: func(llm.Request) { calls++ }}
	}))
	started := start(t, h)
	_, err := h.runtime.Advance(t.Context(), started.ID)
	if !errors.Is(err, refused) {
		t.Fatalf("advance error = %v, want audit failure", err)
	}
	if calls != 0 {
		t.Fatalf("model was called %d times after audit failure", calls)
	}
	current, err := h.store.Get(t.Context(), started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != run.StateRunning || current.Nodes["answer"].Status.Terminal() {
		t.Fatalf("audit outage permanently settled node: %+v", current)
	}
}

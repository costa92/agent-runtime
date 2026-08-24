package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

// --- fixtures -------------------------------------------------------------

func TestAToolWithATargetHostRecordsTheResolvedHost(t *testing.T) {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "call an API",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
		TargetHost: "api.example.com",
	}, testkit.ToolSucceeding(`{"status":200}`)); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{}`)); err != nil {
			t.Errorf("tool: %v", err)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	recorded := h.observer.named(observe.EventEgressHostResolved)
	if len(recorded) != 1 {
		t.Fatalf("egress decisions = %d, want 1", len(recorded))
	}
	if got := attributeOf(recorded[0], observe.AttrHost); got != "api.example.com" {
		t.Errorf("host = %q, want api.example.com", got)
	}
}

// A tool that reaches no network records no egress: an event naming an empty
// host would read as a call to nowhere rather than as a local call.

func TestAToolWithoutATargetHostRecordsNoEgress(t *testing.T) {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, testkit.ToolSucceeding(`{"book_id":"b1"}`)); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{}`)); err != nil {
			t.Errorf("tool: %v", err)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got := len(h.observer.named(observe.EventEgressHostResolved)); got != 0 {
		t.Errorf("egress decisions = %d, want 0", got)
	}
}

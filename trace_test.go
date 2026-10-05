package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/internal/testkit"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/observe"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/tool"
)

// The Tracer port was declared with the module and never called (TD-041): a
// host that swapped NopTracer for a real adapter got zero spans and a trace
// backend that claimed to be configured. These tests make the instrumentation
// points an executable fact, so the next review cannot find the same hole.
func TestOneAdvanceOpensOneRunSpanUnderTheRunsPinnedTrace(t *testing.T) {
	tracer := testkit.NewRecordingTracer()
	h := newHarness(
		t,
		scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDeps(func(deps *agentruntime.Dependencies) { deps.Tracer = tracer }),
	)

	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	spans := tracer.OfKind(observe.SpanRun)
	if len(spans) != 1 {
		t.Fatalf("run spans = %d, want exactly one per advance: %+v", len(spans), tracer.Spans())
	}
	span := spans[0]
	if span.Request.RunID != started.ID {
		t.Errorf("span run id = %q, want %q", span.Request.RunID, started.ID)
	}
	if span.Request.Parent != started.Pins.Trace {
		t.Errorf("span parent = %+v, want the Run's pinned trace %+v: two advances are "+
			"separated by the database, so the parent has to come from the Snapshot",
			span.Request.Parent, started.Pins.Trace)
	}
	if !span.Ended || span.Err != nil {
		t.Errorf("span ended=%v err=%v, want ended cleanly", span.Ended, span.Err)
	}
}

func TestModelAndToolCallsOpenEffectSpansUnderTheNodeSpan(t *testing.T) {
	tracer := testkit.NewRecordingTracer()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "retrieve",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, testkit.ToolSucceeding(`"found"`)); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()

	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
				return agent.Response{}, err
			}
			if _, err := request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{}`)); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
			Tools:          []definition.ToolRef{{Key: "search_evidence"}},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Tracer = tracer
			deps.Models = scriptedModels{}
			deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	runSpans := tracer.OfKind(observe.SpanRun)
	if len(runSpans) != 1 {
		t.Fatalf("run spans = %d, want 1", len(runSpans))
	}
	advance := runSpans[0].Context

	nodes := tracer.OfKind(observe.SpanNode)
	if len(nodes) != 1 {
		t.Fatalf("node spans = %d, want 1", len(nodes))
	}
	if nodes[0].Request.Parent != advance {
		t.Errorf("node span parent = %+v, want the advance span %+v", nodes[0].Request.Parent, advance)
	}
	if nodes[0].Request.Name != "agent.node" || nodes[0].Request.NodeID == "" || nodes[0].Request.AgentKey != "answer" {
		t.Errorf("node span = %+v, want agent.node carrying the node id and agent key as attributes", nodes[0].Request)
	}
	node := nodes[0].Context

	models := tracer.OfKind(observe.SpanModelCall)
	if len(models) != 1 {
		t.Fatalf("model spans = %d, want 1", len(models))
	}
	if models[0].Request.Parent != node {
		t.Errorf("model span parent = %+v, want the node span %+v", models[0].Request.Parent, node)
	}
	if models[0].Request.InvocationID == "" {
		t.Error("model span carries no invocation id; a trace that cannot be joined to the Invocation record is not one reconciliation can use")
	}

	tools := tracer.OfKind(observe.SpanToolCall)
	if len(tools) != 1 {
		t.Fatalf("tool spans = %d, want 1", len(tools))
	}
	if tools[0].Request.Name != "tool.search_evidence" {
		t.Errorf("tool span name = %q, want tool.search_evidence", tools[0].Request.Name)
	}
	if tools[0].Request.Parent != node || tools[0].Request.InvocationID == "" || tools[0].Request.IdempotencyKey == "" {
		t.Errorf("tool span = %+v, want parented to the node span with invocation id and idempotency key", tools[0].Request)
	}
	if tools[0].InputBytes != 2 || tools[0].OutputBytes != len(`"found"`) {
		t.Errorf("tool sizes = %d in / %d out", tools[0].InputBytes, tools[0].OutputBytes)
	}
	for _, span := range tracer.Spans() {
		if !span.Ended {
			t.Errorf("span %q never ended", span.Request.Name)
		}
	}
}

func TestAFailedModelCallEndsItsSpanWithTheError(t *testing.T) {
	tracer := testkit.NewRecordingTracer()
	boom := errors.New("provider down")
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			_, err := request.Ports.Model(ctx, llm.Request{})
			return agent.Response{}, err
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Tracer = tracer
			deps.Models = scriptedModels{err: boom}
		}),
	)

	_, _ = h.runtime.Advance(t.Context(), start(t, h).ID)

	models := tracer.OfKind(observe.SpanModelCall)
	if len(models) != 1 {
		t.Fatalf("model spans = %d, want 1", len(models))
	}
	if !models[0].Ended || !errors.Is(models[0].Err, boom) {
		t.Errorf("model span ended=%v err=%v, want ended with the provider error", models[0].Ended, models[0].Err)
	}
}

// A Run claimed in any state but queued is a continuation: after an approval,
// after a takeover, after a crash. Its segment is a resumed span, so that a
// wait spent on a human does not sit inside a latency measurement.
func TestAContinuationOpensAResumedSpan(t *testing.T) {
	tracer := testkit.NewRecordingTracer()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, testkit.ToolSucceeding(`"done"`)); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()

	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{}`)); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast"},
			Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Tracer = tracer
			deps.Governance = fakeGovernance{policies: policy.Snapshot{
				Policies: []policy.Policy{{
					Name:  "writes-need-a-human",
					Scope: policy.ScopeTenant,
					Conditions: []policy.Condition{{
						Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
						Values: []string{string(policy.SideEffectWrite)},
					}},
					Decision: policy.DecisionRequireApproval,
				}},
			}}
			deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		}),
	)

	started := start(t, h)
	first, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if first.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", first.Run.State)
	}
	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: first.Run.PendingApprovalID, Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	h.clock.Advance(time.Minute)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("second advance: %v", err)
	}

	if got := len(tracer.OfKind(observe.SpanRun)); got != 1 {
		t.Errorf("run spans = %d, want 1 for the first segment", got)
	}
	resumed := tracer.OfKind(observe.SpanResumed)
	if len(resumed) != 1 {
		t.Fatalf("resumed spans = %d, want 1 for the continuation", len(resumed))
	}
	if resumed[0].Request.Parent != started.Pins.Trace {
		t.Errorf("resumed span parent = %+v, want the pinned trace", resumed[0].Request.Parent)
	}
}

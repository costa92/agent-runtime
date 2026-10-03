package agentruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

func TestApprovedToolArgumentsCannotBeSubstituted(t *testing.T) {
	handler := testkit.ToolSucceeding(`{"ok":true}`)
	h := approvalTestHarness(t, handler, func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted == nil {
			_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"approved"}`))
			return agent.Response{}, err
		}
		mutated := request.Granted.Arguments
		mutated[bytes.Index(mutated, []byte("approved"))] = 'X'
		_, err := request.Ports.Tool(ctx, request.Granted.Name, mutated)
		if err == nil || run.CodeOf(err) != "approval_arguments_mismatch" {
			t.Errorf("substituted arguments: %v", err)
		}
		return agent.Response{Output: json.RawMessage(`{"refused":true}`)}, nil
	})
	approveAndAdvance(t, h)
	if calls := handler.Calls(); calls != 0 {
		t.Fatalf("substituted write executed %d times", calls)
	}
}

func TestConcurrentCallsConsumeOneApprovalOnce(t *testing.T) {
	handler := testkit.ToolSucceeding(`{"ok":true}`)
	h := approvalTestHarness(t, handler, func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted == nil {
			_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"approved"}`))
			return agent.Response{}, err
		}
		var group sync.WaitGroup
		group.Add(2)
		start := make(chan struct{})
		errors := make(chan error, 2)
		for range 2 {
			go func() {
				defer group.Done()
				<-start
				_, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
				errors <- err
			}()
		}
		close(start)
		group.Wait()
		close(errors)
		successes := 0
		for err := range errors {
			if err == nil {
				successes++
			} else if !tool.IsApprovalRequired(err) {
				t.Errorf("second call returned %v", err)
			}
		}
		if successes != 1 {
			t.Errorf("approved executions=%d want=1", successes)
		}
		return agent.Response{Output: json.RawMessage(`{"done":true}`)}, nil
	})
	approveAndAdvance(t, h)
	if calls := handler.Calls(); calls != 1 {
		t.Fatalf("write executed %d times, want one", calls)
	}
}

func approvalTestHarness(t *testing.T, handler tool.Handler, execute func(context.Context, agent.Request) (agent.Response, error)) *harness {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "create book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	return newHarness(t, scriptedAgent{execute: execute}, withDefinition(definition.Definition{
		Ref:  run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode: definition.ModeSpecialist, Implementation: "answer",
		Prompt: "be brief", Model: definition.ModelPolicy{Profile: "fast"},
		Tools: []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{Policies: []policy.Policy{{
			Name: "writes-need-approval", Scope: policy.ScopeTenant,
			Conditions: []policy.Condition{{Fact: policy.FactToolSideEffect, Operator: policy.OpEquals, Values: []string{string(policy.SideEffectWrite)}}},
			Decision:   policy.DecisionRequireApproval,
		}}}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
}

func approveAndAdvance(t *testing.T, h *harness) {
	t.Helper()
	started := start(t, h)
	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil || parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("initial advance: state=%s err=%v", parked.Run.State, err)
	}
	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	h.clock.Advance(time.Minute)
	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil || result.Run.State != run.StateSucceeded {
		t.Fatalf("resumed advance: state=%s err=%v", result.Run.State, err)
	}
}

// --- fixtures -------------------------------------------------------------

func TestARequiredApprovalIsNamedOnTheParkedRun(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{}, tool.ApprovalRequired
	}})
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateWaitingApproval {
		t.Fatalf("state = %s, want waiting_approval", result.Run.State)
	}
	inspected, err := h.runtime.Inspect(t.Context(), principal(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspected.PendingApprovalID == "" {
		t.Fatal("Inspect has no pending_approval_id; confirm has nothing to POST")
	}

	page, err := h.store.Events(t.Context(), store.EventQuery{RunID: started.ID, Limit: 20})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var parked run.Event
	for _, event := range page.Events {
		if event.To == run.StateWaitingApproval {
			parked = event
			break
		}
	}
	if parked.ApprovalID == "" {
		t.Fatal("the waiting_approval event has no approval_id")
	}
	if parked.ApprovalID != inspected.PendingApprovalID {
		t.Fatalf("event approval %q != snapshot %q", parked.ApprovalID, inspected.PendingApprovalID)
	}
}

// Denying a write must not kill the Run: an assistant whose tool request was
// refused still has to answer. The denied effect is reported back as a plain
// refusal — granted never carries it into execution — so the agent can tell
// the model and let it answer without the tool.

func TestDenyingAWriteLetsTheRunContinueWithoutTheEffect(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	// Round 1 asks for the write and parks. Round 2 (after the denial) asks
	// again, is refused, and answers in text instead — exactly what the
	// assistant agent does with a refused tool.
	rounds := 0
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		rounds++
		if request.Granted != nil {
			t.Fatal("granted was injected for a denied approval; the refused write must never execute")
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		if err == nil {
			t.Fatal("the denied tool executed")
		}
		if tool.IsApprovalRequired(err) {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`{"content":"绘本没有生成，我直接回答你"}`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
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
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: false, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after deny: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; a denied approval failed the whole Run instead of letting the agent answer", result.Run.State)
	}
	if rounds != 2 {
		t.Fatalf("rounds=%d want=2 (ask, then answer after the refusal)", rounds)
	}
	if handler.Calls() != 0 {
		t.Fatalf("write calls=%d; the denied effect ran anyway", handler.Calls())
	}
}

// Confirming a write must perform that write. Re-running the node from
// scratch asks the model again, which parks again, and the user sees no
// follow-up after 确认执行.

func TestConfirmingAWritePerformsTheWrite(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: out}, nil
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
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
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}
	if handler.Calls() != 0 {
		t.Fatal("the write ran before a human approved it")
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// The worker that parked still holds the lease. Production waits it out;
	// the test clock is Store-authoritative.
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; confirming a write did not finish the turn", result.Run.State)
	}
	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d; the approved effect did not run", handler.Calls())
	}
}

func TestMemoryWriteBeforeApprovedToolPreservesGrant(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"id":"created"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "create book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	memoryProvider := &countingMemoryProvider{}
	memoryRegistry := memory.NewRegistry()
	if err := memoryRegistry.Register("notes", memoryProvider); err != nil {
		t.Fatal(err)
	}
	memoryRegistry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			if err := request.Ports.Remember(ctx, "notes", "ref", "text", "approved-memory"); err != nil {
				return agent.Response{}, err
			}
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			return agent.Response{Output: out}, err
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:  run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode: definition.ModeSpecialist, Implementation: "answer",
		Model:    definition.ModelPolicy{Profile: "fast"},
		Tools:    []definition.ToolRef{{Key: "render_picture_book"}},
		Memories: []definition.MemoryRef{{Key: "notes", Namespace: "ns", Writable: true, MaxRecords: 8, MaxTokens: 600}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{Policies: []policy.Policy{{
			Name: "writes-need-a-human", Scope: policy.ScopeTenant,
			Conditions: []policy.Condition{{
				Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
				Values: []string{string(policy.SideEffectWrite)},
			}},
			Decision: policy.DecisionRequireApproval,
		}}}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		deps.Memories = memory.NewGateway(memoryRegistry, testkit.AllowAllMemoryAuthorizer())
	}))
	started := start(t, h)
	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil || parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("parked state=%s err=%v", parked.Run.State, err)
	}
	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(time.Minute)
	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil || result.Run.State != run.StateSucceeded {
		t.Fatalf("resumed state=%s err=%v", result.Run.State, err)
	}
	if memoryProvider.writes != 1 || handler.Calls() != 1 {
		t.Fatalf("memory writes=%d tool calls=%d, want one each", memoryProvider.writes, handler.Calls())
	}
}

// Confirming must not park the resumed Run over a model call it already
// finished. The real assistant shape is model → tool: by the time the write
// parks for approval, the model call has completed. Left as in_flight in the
// snapshot — complete() used to settle the budget without carrying the
// outcome — that call reads to parkUnclassifiedEffects as a worker that died
// mid-effect, and the Run parks itself in waiting_resolution the moment the
// approval is confirmed. The user sees 确认执行 and then 正在核对 forever.

func TestConfirmingAWriteDoesNotParkTheRunOverItsFinishedModelCall(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: out}, nil
		}
		// One finished model call first, then the write that needs a human:
		// the exact shape that left an in_flight model invocation behind when
		// the Run parked for approval.
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{response: llm.Response{
			Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 100}}},
		}}
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
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; the resumed Run parked over the model call it already finished", result.Run.State)
	}
	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d; the approved effect did not run", handler.Calls())
	}
	for id, invocation := range result.Run.Invocations {
		if invocation.Outcome == run.OutcomeInFlight {
			t.Fatalf("invocation %s is still in_flight at the terminal snapshot", id)
		}
	}
}

// The per-profile consumption detail the agent reports must reach the Store
// verbatim: it is the host's only input for pricing v2 spend, and the engine
// is the only path that carries it out of the agent.

package agentruntime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

func ceiling(n int) *int { return &n }

// searchRegistry is a read-only tool, so nothing here parks for approval unless
// a test asks for it.
func searchRegistry(t *testing.T) (*tool.Registry, *testkit.CountingHandler) {
	t.Helper()
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"results":[]}`)
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "search the web",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	return registry, handler
}

func searchingDefinition(maxCalls *int) definition.Definition {
	return definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "search_evidence", MaxCalls: maxCalls}},
	}
}

// The ceiling the author published is the number of calls the Run gets. Without
// this, max_evidence_calls was a field with a database column, a validator, an
// input box and a version hash — and no effect on any Run.
func TestAToolIsRefusedOnceTheDeclaredCallCeilingIsReached(t *testing.T) {
	registry, handler := searchRegistry(t)

	var errs []error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		for range 3 {
			_, err := request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"兔"}`))
			errs = append(errs, err)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(searchingDefinition(ceiling(2))), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("calls within the ceiling were refused: %v, %v", errs[0], errs[1])
	}
	if errs[2] == nil {
		t.Fatal("the third call was allowed against a ceiling of 2")
	}
	if kind := run.KindOf(errs[2]); kind != run.ErrorDenied {
		t.Errorf("kind = %s, want denied", kind)
	}
	if run.RetryOf(errs[2]) != run.RetryNever {
		t.Error("the refusal invites a retry; an agent should answer from what it has instead")
	}
	// Refused before Prepare, so the gateway is never asked and no external
	// call is made. A ceiling enforced after the effect is not a ceiling.
	if handler.Calls() != 2 {
		t.Errorf("tool ran %d times, want 2; the refusal came too late", handler.Calls())
	}
}

// A ceiling of zero is a ceiling of zero, not an absent one. The host validator
// accepts max_evidence_calls=0 and the UI presents it as "off", so reading it
// as "unbounded" would make the one setting an author reaches for to disable
// retrieval the one setting with no effect at all.
func TestACeilingOfZeroRefusesTheFirstCall(t *testing.T) {
	registry, handler := searchRegistry(t)

	var callErr error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, callErr = request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"兔"}`))
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(searchingDefinition(ceiling(0))), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if callErr == nil {
		t.Fatal("a ceiling of 0 allowed a call")
	}
	if handler.Calls() != 0 {
		t.Errorf("tool ran %d times against a ceiling of 0", handler.Calls())
	}
}

// Declaring no ceiling is not declaring a ceiling of zero. Every tool in every
// Definition published before this existed carries a nil MaxCalls, so getting
// this backwards would refuse every tool call in the deployment.
func TestAToolWithNoDeclaredCeilingIsUnbounded(t *testing.T) {
	registry, handler := searchRegistry(t)

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		for range 5 {
			if _, err := request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"兔"}`)); err != nil {
				t.Errorf("tool: %v", err)
			}
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(searchingDefinition(nil)), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if handler.Calls() != 5 {
		t.Errorf("tool ran %d times, want 5; an undeclared ceiling bounded something", handler.Calls())
	}
}

// The ceiling bounds the Run, not the claim that happens to be executing it.
//
// This is the case an in-memory counter cannot serve, and the reason the tool
// key had to become a durable column: parking for approval ends the session
// that observed the first call. A counter held in the session, in governedPorts
// or in a host-side map resets here, and the Run gets a fresh allowance every
// time a human confirms something — which on the assistant path is often.
func TestTheCeilingCountsAcrossAPark(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"results":[]}`)
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "search the web",
		Parameters: json.RawMessage(`{"type":"object"}`),
		// High risk purely to make the policy below park it; the ceiling has
		// nothing to do with approval.
		RiskLevel: policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	var resumedErr error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			// The approved call, then one more. The first consumes the ceiling
			// of 1; the second must be refused by a count that outlived the
			// session which recorded it.
			if _, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments); err != nil {
				return agent.Response{}, err
			}
			_, resumedErr = request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"again"}`))
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}
		_, err := request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(searchingDefinition(ceiling(1))), withDeps(func(deps *agentruntime.Dependencies) {
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
		t.Fatalf("state=%s, want waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	h.clock.Advance(time.Minute)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}

	if handler.Calls() != 1 {
		t.Fatalf("tool ran %d times against a ceiling of 1; "+
			"the count did not survive the park", handler.Calls())
	}
	if resumedErr == nil {
		t.Fatal("the second call was allowed after a park; " +
			"the ceiling is bounding the claim, not the Run")
	}
	if !strings.Contains(resumedErr.Error(), "tool_call_ceiling_exhausted") {
		t.Errorf("refused for the wrong reason: %v", resumedErr)
	}
}

// Every succeeding tool call costs exactly one ToolCall from the Run budget,
// whatever the tool is.
//
// The governance matrix leans on this: it calls Budget.ToolCalls the governed
// per-Run tool ceiling, published with the Definition and adjustable without a
// deploy. That is only true while the reservation is unconditional across
// tools. A "free" tool exempted from it — the thing the tool loop's own comment
// used to claim already existed — would put an unbounded hole in the ceiling.
//
// Succeeding is the operative word, and the limit of the claim: see
// TestAFailedToolCallCostsTheBudgetNothing, which is why the matrix files the
// per-loop request bound as still worth having rather than as redundant.
func TestEverySucceedingToolCallCostsOneToolCall(t *testing.T) {
	registry := tool.NewRegistry()
	cheap := testkit.ToolSucceeding(`{"ok":true}`)
	costly := testkit.ToolSucceeding(`{"ok":true}`)
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "retrieve",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, cheap); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, costly); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	var errs []error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		for i := range 5 {
			name := "search_evidence"
			if i%2 == 1 {
				name = "render_picture_book"
			}
			_, err := request.Ports.Tool(ctx, name, json.RawMessage(`{}`))
			errs = append(errs, err)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "search_evidence"}, {Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		// The default rule set refuses high-risk writes, which would refuse
		// this tool before anything is reserved — and then the test would be
		// comparing one tool against nothing.
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "allow-the-write",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionAllow,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))

	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 3},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	dispatched := cheap.Calls() + costly.Calls()
	if dispatched != 3 {
		t.Fatalf("dispatched %d calls against a budget of 3; "+
			"some tool is not charging one ToolCall", dispatched)
	}
	// Both kinds ran, so the count above is not three of one cheap tool.
	if cheap.Calls() == 0 || costly.Calls() == 0 {
		t.Fatalf("only one kind of tool ran (cheap=%d costly=%d); "+
			"the test no longer compares them", cheap.Calls(), costly.Calls())
	}
	for i, err := range errs[:3] {
		if err != nil {
			t.Errorf("call %d was refused inside the budget: %v", i, err)
		}
	}
	if errs[3] == nil {
		t.Fatal("the fourth call was allowed against a budget of 3")
	}
}

// failingHandler fails without reporting usage, which is what a real handler
// that never reached its backend does.
type failingHandler struct{ calls int }

func (h *failingHandler) Invoke(context.Context, tool.Invocation) (tool.Result, error) {
	h.calls++
	return tool.Result{}, run.NewError("upstream_down", run.ErrorRetryable, run.RetryBackoff)
}

// A failed tool call costs the Run budget nothing, so Budget.ToolCalls does not
// bound a model that hammers a broken tool.
//
// This is the gap the compiled-in per-loop request bound covers, and the reason
// the governance matrix files that bound as an implementation detail rather
// than deleting it as redundant. The reservation is released when the effect
// settles as not-applied with no usage, which is correct — a call that did
// nothing should not spend the envelope — and it is exactly why "the budget is
// the only tool ceiling you need" is false.
func TestAFailedToolCallCostsTheBudgetNothing(t *testing.T) {
	registry := tool.NewRegistry()
	handler := &failingHandler{}
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "search the web",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		for range 3 {
			_, _ = request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{}`))
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(searchingDefinition(nil)), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))

	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 1},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	if handler.calls != 3 {
		t.Fatalf("handler ran %d times against a budget of 1; if this is now 1, "+
			"the budget bounds failures too and the per-loop request bound may be "+
			"redundant — revisit the governance matrix row that assumes it is not",
			handler.calls)
	}
}

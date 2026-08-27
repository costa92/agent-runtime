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

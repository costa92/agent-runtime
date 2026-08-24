package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// --- fixtures -------------------------------------------------------------

func TestQuotaRejectionAtCreationCreatesNoRun(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{quotas: quota.Snapshot{
			Digest: "q", Limits: []quota.Limit{{
				Name: "concurrency", Scope: quota.Scope{Tenant: "acme"},
				Unit: quota.UnitConcurrentRuns, Max: 1,
			}},
		}}
		deps.Meter = fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 1}}
	}))

	_, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
	})
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
	if len(h.observer.named(observe.EventQuotaRejected)) != 1 {
		t.Error("the rejection produced no decision event")
	}
}

// A Run must come back as the shape it started as. A deployment whose published
// graph moved cannot be allowed to resume it as something else.

func TestAgentsReceiveGovernedPortsAndTheRunsBudget(t *testing.T) {
	var seen agent.Request
	h := newHarness(t, scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = request
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if seen.Ports == nil {
		t.Fatal("the agent got no ports; it could only reach effects ungoverned or not at all")
	}
	if seen.Prompt != "be brief" {
		t.Errorf("prompt=%q; the published Definition's prompt did not reach the agent", seen.Prompt)
	}
	if seen.Remaining.Tokens != 10_000 {
		t.Errorf("remaining=%+v; an agent that cannot see the ceiling discovers it by hitting it", seen.Remaining)
	}
	if seen.Principal.Subject != "alice" {
		t.Errorf("principal=%+v", seen.Principal)
	}
}

// A model call must reserve before it is issued and settle after. Reserving
// afterwards means the budget is already spent by the time anything could
// refuse.

func TestAModelCallIsReservedBeforeItIsIssuedAndSettledAfter(t *testing.T) {
	var reservedDuringCall bool
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{onCall: func() { reservedDuringCall = true }}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !reservedDuringCall {
		t.Fatal("the model was never called")
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s", result.Run.State)
	}
	if result.Run.Budget.Used.Tokens == 0 {
		t.Error("the call settled no usage; a ledger that counts nothing bounds nothing")
	}
	if len(h.observer.named(observe.EventModelSelected)) == 0 {
		t.Error("model selection produced no decision event")
	}
}

// Every attempt reports usage, including the failed ones: a provider that
// consumed the prompt and then errored still charged for it.

func TestAFailedModelCallStillSettlesItsUsage(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100})
		if err == nil {
			t.Error("the scripted failure did not surface")
		}
		return agent.Response{Output: json.RawMessage(`"partial"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{
			err: run.NewError("provider_down", run.ErrorRetryable, run.RetryBackoff),
			response: llm.Response{Attempts: []llm.Attempt{
				{Usage: llm.Usage{InputTokens: 40}, Failed: true},
			}},
		}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.Budget.Used.Tokens != 40 {
		t.Fatalf("used=%+v; a failed attempt's spend is still real", result.Run.Budget.Used)
	}
}

// Production assistant envelopes copy MaxLLMCalls/MaxToolCalls and leave
// Tokens at 0. After the first model call settles its real usage, the next
// effect must still be admitted — otherwise a tool-calling turn fails with
// no assistant reply, which is what the user saw as a silent failed run.

func TestAnUncappedTokenEnvelopeStillAdmitsTheNextEffect(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{response: llm.Response{
			Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 2000, OutputTokens: 147}}},
		}}
	}))
	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     run.Limits{LLMCalls: 10, ToolCalls: 20},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; the follow-up model call against an uncapped token component failed", result.Run.State)
	}
	if result.Run.Budget.Used.LLMCalls != 2 {
		t.Fatalf("used llm=%d; the second model call did not settle", result.Run.Budget.Used.LLMCalls)
	}
}

// A budget that cannot pay must refuse before the call, not after.

func TestAnEffectOverTheBudgetIsRefusedBeforeTheProviderIsCalled(t *testing.T) {
	var called bool
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 1_000_000}); err == nil {
			t.Error("an unaffordable call was allowed")
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{onCall: func() { called = true }}
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if called {
		t.Fatal("the provider was reached despite the refusal")
	}
	if len(h.observer.named(observe.EventBudgetRefused)) == 0 {
		t.Error("the budget refusal produced no decision event")
	}
}

// A memory key the Definition never declared is refused even when the provider
// exists: the Definition is what the Run was published to do.

func TestAnUndeclaredMemoryKeyIsRefused(t *testing.T) {
	var refusal error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, refusal = request.Ports.Recall(ctx, "notes", "anything")
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if run.KindOf(refusal) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(refusal))
	}
}

// A worker whose lease lapsed mid-effect must not commit the result: another
// worker may already have taken over.

func TestNodeModelUsageReachesTheStore(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		var modelUsage []run.ModelUsage
		for range 2 {
			response, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100})
			if err != nil {
				return agent.Response{}, err
			}
			modelUsage = run.MergeModelUsage(modelUsage,
				response.Model.Profile, response.Usage.InputTokens, response.Usage.OutputTokens)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`), ModelUsage: modelUsage}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{}
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	usage := h.store.NodeModelUsage(started.ID)
	if len(usage) != 1 {
		t.Fatalf("model usage lines = %+v, want one line for the pinned profile", usage)
	}
	if usage[0].Profile != "fast" {
		t.Fatalf("profile = %q, want the pinned profile %q", usage[0].Profile, "fast")
	}
	if usage[0].InputTokens != 20 || usage[0].OutputTokens != 10 {
		t.Fatalf("usage = %+v, want input 20 output 10 (two scripted calls)", usage[0])
	}
}

// A node that reports no model detail must commit nothing: an empty profile
// line would reach the host as a zero-priced slice and read like free spend.

func TestEmptyModelUsageCommitsNoRows(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if usage := h.store.NodeModelUsage(started.ID); len(usage) != 0 {
		t.Fatalf("usage = %+v, want none", usage)
	}
}

// A grant authorises one write, not a standing permission for that tool.
//
// The hold lives in the checkpoint and nothing removed it once the granted
// call had run, so it stayed in place for the rest of the Run. The damage
// shows on the recovery path — effect performed, result not committed, the
// call parked as unknown, resolved as applied, and the next advance injects
// Granted again and repeats a non-idempotent write (a second picture book was
// generated this way in production) — but the mechanism is simply that the
// hold outlives the call it was granted for, which a second request for the
// same tool exposes directly.

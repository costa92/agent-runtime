package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/observe"
	"github.com/costa92/agent-runtime/quota"
	"github.com/costa92/agent-runtime/run"
)

// brokenMeter cannot answer. This is the ledger being unreadable — a locked
// table, an exhausted pool, a migration in flight — not a limit being reached.
type brokenMeter struct{}

func (brokenMeter) Observe(context.Context, quota.Scope, quota.Unit, time.Duration) (int, error) {
	return 0, errors.New("ledger unavailable")
}

// The enforcer fails closed when its meter cannot answer, and that refusal has
// to be visible.
//
// Treating an unreachable meter as "nothing used" would lift every cap at
// exactly the moment the deployment is already unhealthy, so failing closed is
// right. But the refusal returned an error rather than a decision, and the
// counters only observe decisions — so the one path that can refuse every model
// call, every tool call and every Run creation at once was the one path with no
// instrument at all.
//
// It must not arrive as EventQuotaRejected: operators alert on that as "we are
// at our ceiling", and an availability fault dressed as enforcement working is
// the worst possible way for this to read.
func TestAnUnreadableMeterRefusesWithItsOwnDecision(t *testing.T) {
	// An agent that actually calls the model: the check under test sits on the
	// effect, so an agent that answers from thin air never reaches it.
	var modelErr error
	caller := scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, modelErr = request.Ports.Model(ctx, llm.Request{MaxTokens: 100})
		return agent.Response{Output: json.RawMessage(`"done"`)}, modelErr
	}}

	h := newHarness(t, caller, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{quotas: quota.Snapshot{
			Digest: "q", Limits: []quota.Limit{{
				Name: "llm-calls-per-hour", Scope: quota.Scope{Tenant: "acme"},
				Unit: quota.UnitTokens, Max: 1000, Window: time.Hour,
			}},
		}}
		deps.Meter = brokenMeter{}
		// A registry has to resolve before the quota check is reached; without
		// one the call fails earlier and this test would pass on the wrong
		// refusal.
		deps.Models = scriptedModels{}
	}))

	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
	})
	if err != nil {
		// Start reads a different unit; if it too is refused, the model call
		// below never happens and the test would pass for the wrong reason.
		t.Skipf("start refused before the model call: %v", err)
	}
	if _, err = h.runtime.Advance(t.Context(), started.ID); err == nil && modelErr == nil {
		t.Fatal("the model call went through while the quota meter was unreadable")
	}
	if run.CodeOf(modelErr) != quota.CodeUnreadable {
		t.Fatalf("model refused with %q, want %q", run.CodeOf(modelErr), quota.CodeUnreadable)
	}

	if got := h.observer.named(observe.EventQuotaUnreadable); len(got) != 1 {
		t.Fatalf("recorded %d unreadable decisions, want 1; "+
			"a refusal that is only an error cannot answer why a Run stopped", len(got))
	}
	if got := h.observer.named(observe.EventQuotaRejected); len(got) != 0 {
		t.Fatalf("recorded %d rejections; an unreadable meter is a fault, "+
			"not the deployment sitting at its ceiling", len(got))
	}
}

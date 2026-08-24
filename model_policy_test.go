package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// The published Definition's model policy has to reach the provider.
//
// It did not. The governed Model port filled only Profile, so an Agent built on
// the shared mechanical step — which constructs a bare llm.Request — called
// every model with Temperature and MaxTokens at zero, and the temperature the
// Definition was published with did nothing.
func TestTheDefinitionsModelPolicyReachesTheProvider(t *testing.T) {
	var seen llm.Request
	h := newHarness(t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			// A bare request, the way the shared step builds one.
			if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model: definition.ModelPolicy{
				Profile: "fast", Temperature: 0.2, MaxTokens: 900,
			},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{onRequest: func(request llm.Request) { seen = request }}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if seen.Model.Profile != "fast" {
		t.Errorf("profile=%q, want the Definition's", seen.Model.Profile)
	}
	if seen.Temperature != 0.2 {
		t.Errorf("temperature=%v, want the published 0.2; the Definition's policy was dropped",
			seen.Temperature)
	}
	if seen.MaxTokens != 900 {
		t.Errorf("max tokens=%d, want the published 900", seen.MaxTokens)
	}
}

// MaxTokens is not only a provider argument: it is what the token reservation
// is sized from. While the Definition's value was being dropped, every model
// call reserved zero tokens, so the token half of the budget refused nothing
// and only the call count was holding.
func TestAModelCallReservesTheDefinitionsTokenCeiling(t *testing.T) {
	h := newHarness(t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			// Above the Run's whole token budget, so a reservation that is
			// really sized from this cannot be admitted. A test that only
			// inspected settled usage would pass either way: the provider
			// double reports its own small usage no matter what was reserved.
			Model: definition.ModelPolicy{Profile: "fast", MaxTokens: 1_000_000},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{}
		}),
	)

	result, err := h.runtime.Advance(t.Context(), start(t, h).ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State == run.StateSucceeded {
		t.Fatal("a call ceilinged above the whole budget was admitted; " +
			"the reservation is not sized from the Definition's MaxTokens")
	}
}

// Caller-wins, the same rule Profile follows. An Agent that computed a ceiling
// for one particular call must not have it replaced by the default.
func TestAnAgentsOwnModelSettingsSurviveTheDefinitionsDefaults(t *testing.T) {
	var seen llm.Request
	h := newHarness(t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{
				Temperature: 0.9, MaxTokens: 64,
			}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model: definition.ModelPolicy{
				Profile: "fast", Temperature: 0.2, MaxTokens: 900,
			},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{onRequest: func(request llm.Request) { seen = request }}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if seen.Temperature != 0.9 || seen.MaxTokens != 64 {
		t.Errorf("the Definition overrode the Agent: temperature=%v max tokens=%d",
			seen.Temperature, seen.MaxTokens)
	}
}

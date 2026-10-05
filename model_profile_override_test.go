package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/run"
)

func modelCallingAgent() scriptedAgent {
	return scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}}}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}
}

func profileDefinition(profile string) definition.Definition {
	return definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: profile},
	}
}

// A Definition that pins no engine means "whichever the deployment picks", and
// the caller's per-request choice is part of that pick. It used to be dropped:
// the host validated the chosen provider and then started a Run that could not
// carry it, so an engine picker in the UI changed nothing.
func TestARunModelProfileIsUsedWhenTheDefinitionPinsNone(t *testing.T) {
	var resolved []string
	h := newHarness(t, modelCallingAgent(), withDefinition(profileDefinition("")),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = capturingModels{onRequest: func(request llm.Request) {
				resolved = append(resolved, request.Model.Profile)
			}}
		}))
	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:    principal(),
		Definition:   run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:        json.RawMessage(`{"q":"x"}`),
		Budget:       run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10},
		Restrictions: run.Restrictions{ModelProfile: "mock/mock-chat"},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(resolved) != 1 || resolved[0] != "mock/mock-chat" {
		t.Fatalf("model calls resolved %v, want the Run's profile", resolved)
	}
}

// A pinned engine is what the Definition was published and evaluated under;
// a caller swapping it would bill the Run for a model nobody reviewed.
func TestARunModelProfileIsRefusedWhenTheDefinitionPinsOne(t *testing.T) {
	h := newHarness(t, modelCallingAgent(), withDefinition(profileDefinition("fast")))
	_, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:    principal(),
		Definition:   run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:        json.RawMessage(`{"q":"x"}`),
		Budget:       run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10},
		Restrictions: run.Restrictions{ModelProfile: "mock/mock-chat"},
	})
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("start error = %v; a pinned Definition accepted a different engine", err)
	}
}

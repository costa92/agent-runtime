package agentruntime_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/internal/testkit"
	"github.com/costa92/agent-runtime/memory"
	"github.com/costa92/agent-runtime/run"
)

type capturingMemory struct {
	queries []memory.Query
}

func (c *capturingMemory) Retrieve(_ context.Context, query memory.Query) ([]memory.Record, error) {
	c.queries = append(c.queries, query)
	return nil, nil
}

func (c *capturingMemory) Write(context.Context, memory.Scope, string, string) (memory.Mutation, error) {
	return memory.Mutation{}, memory.ErrReadOnly
}

func TestRecallWritesTheNarrowedNodeTools(t *testing.T) {
	capture := &capturingMemory{}
	registry := memory.NewRegistry()
	if err := registry.Register("notes", capture); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Recall(ctx, "notes", "本轮问题"); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools: []definition.ToolRef{
			{Key: "search_evidence"},
			{Key: "render_picture_book"},
		},
		Memories: []definition.MemoryRef{{
			Key: "notes", Namespace: "ns", MaxRecords: 8, MaxTokens: 600,
		}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Memories = memory.NewGateway(registry, testkit.AllowAllMemoryAuthorizer())
	}))

	snapshot, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:    principal(),
		Definition:   run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:        json.RawMessage(`{"q":"x"}`),
		Budget:       run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10},
		Restrictions: run.Restrictions{ToolNarrowing: []string{"search_evidence", "search_evidence"}},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.runtime.Advance(t.Context(), snapshot.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(capture.queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(capture.queries))
	}
	got := capture.queries[0].Context.Tools
	want := []string{"search_evidence"}
	if !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want the narrowed unique allowlist %v", got, want)
	}
	if capture.queries[0].Text != "本轮问题" {
		t.Fatalf("text = %q, want the recall argument", capture.queries[0].Text)
	}
}

package agentruntime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/internal/testkit"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/tool"
	"github.com/costa92/agent-runtime/workflow"
)

// --- fixtures -------------------------------------------------------------

func TestTheAgentReceivesTheRunsInput(t *testing.T) {
	var seen agent.Request
	h := newHarness(t, scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = request
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if string(seen.Input) != `{"q":"x"}` {
		t.Fatalf("input = %s; the agent executed without what it was asked", seen.Input)
	}
}

// A Run whose effect was left in flight is not simply run again.
//
// A worker that dies between committing the invocation-begin fact and settling
// it leaves an effect nobody can classify: the model call may have been issued
// and charged, the tool may have published. The next worker to claim the Run
// cannot schedule the node again — re-executing is the duplicate side effect the
// whole reserve-before-effect order exists to prevent, and refusing it is what
// the legacy delegation worker did by failing closed on a child that had already
// emitted events.

func TestADownstreamNodeReceivesItsUpstreamsOutputNotItsRef(t *testing.T) {
	var seen []string
	recorder := scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = append(seen, string(request.Input))
		return agent.Response{Output: json.RawMessage(`{"content":"planned"}`)}, nil
	}}

	h := newHarness(t, recorder, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{Name: "writer", Agent: "answer", DependsOn: []string{"planner"}, Inputs: []string{"planner"}},
		}},
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("nodes executed = %d, want 2: %q", len(seen), seen)
	}
	if strings.Contains(seen[1], `"from"`) {
		t.Fatalf("the downstream node was handed a ref it cannot follow: %s", seen[1])
	}
	if !strings.Contains(seen[1], "planned") {
		t.Fatalf("downstream input = %s; it does not carry the upstream's output", seen[1])
	}
}

// Tool authority is granted per node, and what the model is offered follows the
// grant. Offering the Definition's whole tool list to every node made a planner
// spend all three of its turns answering a search tool it had no use for, and
// then fail for want of any prose — while the writer beside it kept an authority
// only the researcher was supposed to have.

func TestOnlyTheNodeThatWasGrantedAToolIsOfferedIt(t *testing.T) {
	offered := map[string][]string{}
	var current string
	models := capturingModels{onRequest: func(request llm.Request) {
		for _, def := range request.Tools {
			offered[current] = append(offered[current], def.Name)
		}
	}}
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, testkit.ToolSucceeding(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	gateway := tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())

	asking := scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		current = string(request.Input)
		if _, err := request.Ports.Model(ctx, llm.Request{
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`{"content":"done"}`)}, nil
	}}

	h := newHarness(t, asking, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{Name: "drawer", Agent: "answer", DependsOn: []string{"planner"}, Inputs: []string{"planner"}, Tools: []string{"render_picture_book"}},
		}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = gateway
		deps.Models = models
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// The planner runs on the Run's own input; the drawer runs on the planner's
	// output, which is what tells the two model calls apart here.
	for input, names := range offered {
		if strings.Contains(input, "done") {
			if len(names) != 1 || names[0] != "render_picture_book" {
				t.Fatalf("the granted node was offered %v, want [render_picture_book]", names)
			}
			continue
		}
		t.Fatalf("a node that was granted no tools was offered %v (input %q)", names, input)
	}
	if len(offered) != 1 {
		t.Fatalf("nodes offered tools = %d, want only the granted one: %v", len(offered), offered)
	}
}

// A node that declares several upstreams receives all of them, keyed by the
// node that produced each. Restricting a step to one upstream is what left the
// writer holding only the research: it read a finished-looking draft with no
// plan beside it, and answered with an editor's review of that draft instead of
// the article it was asked to write.

func TestANodeReadingSeveralUpstreamsReceivesThemKeyedByNode(t *testing.T) {
	var seen []string
	byNode := map[string]json.RawMessage{}
	recorder := scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = append(seen, string(request.Input))
		// Each node answers with something identifiable, so the merged payload
		// can be checked against who produced which half.
		output := `{"content":"plan"}`
		if len(seen) == 2 {
			output = `{"content":"evidence"}`
		}
		return agent.Response{Output: json.RawMessage(output)}, nil
	}}

	h := newHarness(t, recorder, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{Name: "researcher", Agent: "answer", DependsOn: []string{"planner"}, Inputs: []string{"planner"}},
			{
				Name: "writer", Agent: "answer",
				DependsOn: []string{"planner", "researcher"},
				Inputs:    []string{"planner", "researcher"},
			},
		}},
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("nodes executed = %d, want 3: %q", len(seen), seen)
	}
	if err := json.Unmarshal([]byte(seen[2]), &byNode); err != nil {
		t.Fatalf("the writer's input is not an object keyed by node: %s", seen[2])
	}
	if !strings.Contains(string(byNode["planner"]), "plan") {
		t.Fatalf("the writer did not receive the plan: %s", seen[2])
	}
	if !strings.Contains(string(byNode["researcher"]), "evidence") {
		t.Fatalf("the writer did not receive the research: %s", seen[2])
	}
	if _, ok := byNode[workflow.RunInputKey]; ok {
		t.Fatalf("a node that did not ask for the Run input was given it anyway: %s", seen[2])
	}
}

// A node that declares with_run_input receives the Run's own input beside its
// upstreams, under a key no node may take.
//
// Without it a step reading an upstream can never see what the Run was started
// with: the reader profile the host puts on the input reached the writer only as
// far as the planner happened to echo it into the plan, so the personalisation
// was present in the outline and absent from the body.

func TestANodeCanReadTheRunInputBesideItsUpstreams(t *testing.T) {
	var seen []string
	recorder := scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = append(seen, string(request.Input))
		return agent.Response{Output: json.RawMessage(`{"content":"plan"}`)}, nil
	}}

	h := newHarness(t, recorder, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{
				Name: "writer", Agent: "answer",
				DependsOn:    []string{"planner"},
				Inputs:       []string{"planner"},
				WithRunInput: true,
			},
		}},
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("nodes executed = %d, want 2: %q", len(seen), seen)
	}
	byNode := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(seen[1]), &byNode); err != nil {
		// A single upstream plus the flag still delivers the keyed object, so the
		// shape is read off the declaration rather than counted at run time.
		t.Fatalf("the writer's input is not an object keyed by node: %s", seen[1])
	}
	if !strings.Contains(string(byNode["planner"]), "plan") {
		t.Fatalf("the writer did not receive the plan: %s", seen[1])
	}
	if string(byNode[workflow.RunInputKey]) != `{"q":"x"}` {
		t.Fatalf("the writer did not receive the Run input: %s", seen[1])
	}
}

// The outbound host a tool reaches is a governance fact, recorded as a decision
// so "which hosts did this Run touch" survives the allowlist that permitted it.

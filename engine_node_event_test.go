package agentruntime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// nodeEventAgent is shared by both harnesses below. Both nodes register under
// one Agent key and tell themselves apart by their input: the planner runs on
// the Run's own input, and the writer runs on the planner's output.
func nodeEventAgent() agent.Agent {
	return scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if strings.Contains(string(request.Input), "planned") {
			if _, err := request.Ports.Model(ctx, llm.Request{
				Messages: []llm.Message{{Role: llm.RoleUser, Content: "write"}},
			}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{}, run.NewError("writer_failed", run.ErrorInternal, run.RetryNever)
		}
		return agent.Response{Output: json.RawMessage(`{"content":"planned"}`)}, nil
	}}
}

// newTwoNodeHarness builds a Run whose second node (the writer) fails hard
// after making a model call. FailHard is the default failure policy, so the
// writer's own result is what concludes the graph: `workflow.commandFor`
// returns `CommandFail` synchronously inside `ApplyNodeResult`, and that
// terminal transition is reduced and committed in the very same call as the
// writer's own effects (`runNode` → `commitNode(node.ID, ...)`). This is the
// ordinary path for a Run whose last node fails or succeeds — not an edge
// case — so the terminal event is expected to carry the writer's id.
func newTwoNodeHarness(t *testing.T) *harness {
	t.Helper()

	h := newHarness(t, nodeEventAgent(), withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{Name: "writer", Agent: "answer", DependsOn: []string{"planner"}, Inputs: []string{"planner"}},
		}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{}
	}))
	return h
}

// newStarvedThreeNodeHarness builds a Run that reaches terminal through
// finish() rather than through any node's own result.
//
// The writer is Optional (FailSoft), so a soft-failed dependency still leaves
// its dependent reachable: after the writer's own commit, `commandFor` sees
// the reviewer as unaccounted-for-but-reachable and returns no Command, so
// that commit is not terminal. The Run only turns terminal on the next
// Advance iteration — deliberately starved of budget (`LLMCalls: 1`, exactly
// what the writer's one model call spends) so `ReadyNodes` refuses the
// reviewer for want of budget, and the loop's own `finish()` — not a node's
// result — commits the terminal transition, with no node in scope.
func newStarvedThreeNodeHarness(t *testing.T) *harness {
	t.Helper()

	h := newHarness(t, nodeEventAgent(), withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "planner", Agent: "answer"},
			{Name: "writer", Agent: "answer", DependsOn: []string{"planner"}, Inputs: []string{"planner"}, Optional: true},
			{Name: "reviewer", Agent: "answer", DependsOn: []string{"writer"}, Inputs: []string{"writer"}},
		}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{}
	}))
	return h
}

// advanceToTerminal starts a Run under the given budget, drives it to a
// terminal state, and returns its full, ordered event stream.
func advanceToTerminal(t *testing.T, h *harness, budget run.Limits) []run.Event {
	t.Helper()
	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     budget,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !result.Run.State.Terminal() {
		t.Fatalf("run state = %s, want terminal", result.Run.State)
	}

	page, err := h.runtime.ListEvents(t.Context(), principal(), store.EventQuery{RunID: started.ID, Limit: 1000})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return page.Events
}

// TestNodeScopedEventsCarryTheNodeTheyHappenedIn is the whole point of the
// field: an operator looking at a failed Run must be able to say which node
// failed without correlating timestamps against the projection table.
//
// Both directions are asserted. Testing only that node events carry an ID
// would also pass for an implementation that stamps every event with the last
// node it saw — including the run's own start, which has no node by
// construction.
func TestNodeScopedEventsCarryTheNodeTheyHappenedIn(t *testing.T) {
	h := newTwoNodeHarness(t)
	events := advanceToTerminal(t, h, run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10})

	var sawNodeScoped bool
	for _, event := range events {
		switch event.Kind {
		case run.EventBudgetReserved, run.EventInvocationParked:
			if event.NodeID == "" {
				t.Errorf("%s at sequence %d has no node; effects always happen inside one",
					event.Kind, event.Sequence)
			}
			sawNodeScoped = true
		}
	}
	if !sawNodeScoped {
		t.Fatal("harness produced no node-scoped events; the assertion above proved nothing")
	}

	if first := events[0]; first.NodeID != "" {
		t.Errorf("start event carries node %q; the Run had not entered a node yet", first.NodeID)
	}
}

// TestATerminalEventCommittedWithANodesResultCarriesThatNode covers the
// ordinary path: a Run's last node concludes the graph, and its result and
// the Run's terminal transition commit together. The terminal event is the
// first thing an operator looking at a failed Run reads, and it must name the
// node whose result ended the Run — not be silent about it.
func TestATerminalEventCommittedWithANodesResultCarriesThatNode(t *testing.T) {
	h := newTwoNodeHarness(t)
	events := advanceToTerminal(t, h, run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10})

	last := events[len(events)-1]
	if last.NodeID != "writer" {
		t.Errorf("terminal event carries node %q, want %q: the writer's own failure concluded the Run", last.NodeID, "writer")
	}
}

// TestATerminalEventReachedWithNothingSchedulableCarriesNoNode covers the
// exception: a Run that turns terminal because ReadyNodes found nothing to
// schedule (here, budget exhaustion) commits through finish(), which has no
// node in scope. Pinning both this and the node-concluded case is what stops
// either one from later being "fixed" into the other.
func TestATerminalEventReachedWithNothingSchedulableCarriesNoNode(t *testing.T) {
	h := newStarvedThreeNodeHarness(t)
	events := advanceToTerminal(t, h, run.Limits{LLMCalls: 1, Tokens: 10_000, ToolCalls: 10})

	last := events[len(events)-1]
	if last.NodeID != "" {
		t.Errorf("terminal event carries node %q; finish() has no node in scope", last.NodeID)
	}
}

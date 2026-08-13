package workflow_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

func running(nodes map[string]run.NodeState) run.Snapshot {
	return run.Snapshot{ID: "run-1", State: run.StateRunning, Nodes: nodes}
}

func succeeded(ids ...string) map[string]run.NodeState {
	states := map[string]run.NodeState{}
	for _, id := range ids {
		states[id] = run.NodeState{Status: run.StateSucceeded, Attempts: 1}
	}
	return states
}

func readyIDs(graph *workflow.ExecutionGraph, snapshot run.Snapshot) []string {
	var ids []string
	for _, node := range workflow.ReadyNodes(graph, snapshot) {
		ids = append(ids, node.ID)
	}
	return ids
}

func fanOut(t *testing.T) *workflow.ExecutionGraph {
	t.Helper()
	// a → {b, c} → d
	return compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Input: "a"},
		definition.NodeSpec{Name: "c", Agent: "review", DependsOn: []string{"a"}, Input: "a"},
		definition.NodeSpec{Name: "d", Agent: "publish", DependsOn: []string{"b", "c"}},
	))
}

func TestIndependentNodesAreReadyTogether(t *testing.T) {
	graph := fanOut(t)

	if got := readyIDs(graph, running(nil)); len(got) != 1 || got[0] != "a" {
		t.Fatalf("ready=%v want=[a]", got)
	}

	ready := readyIDs(graph, running(succeeded("a")))
	if len(ready) != 2 || ready[0] != "b" || ready[1] != "c" {
		t.Fatalf("ready=%v want=[b c]; both branches unblock at once", ready)
	}
}

func TestADependentNodeStaysBlockedUntilAllDependenciesSucceed(t *testing.T) {
	graph := fanOut(t)

	for _, id := range readyIDs(graph, running(succeeded("a", "b"))) {
		if id == "d" {
			t.Fatal("d became ready with one of its two dependencies outstanding")
		}
	}
	if got := readyIDs(graph, running(succeeded("a", "b", "c"))); len(got) != 1 || got[0] != "d" {
		t.Fatalf("ready=%v want=[d]", got)
	}
}

// A soft failure does not unblock the nodes that read from it. They were
// declared to consume its output, and it produced none.
func TestASoftFailedDependencyDoesNotUnblockItsDependents(t *testing.T) {
	graph := fanOut(t)
	nodes := succeeded("a", "c")
	nodes["b"] = run.NodeState{Status: run.StateFailed, Attempts: 1}

	if got := readyIDs(graph, running(nodes)); len(got) != 0 {
		t.Fatalf("ready=%v want none", got)
	}
}

func TestSchedulingReadsOnlyTheSnapshotAndTheGraph(t *testing.T) {
	graph := fanOut(t)
	snapshot := running(succeeded("a"))

	first := readyIDs(graph, snapshot)
	for range 30 {
		again := readyIDs(graph, snapshot)
		if len(again) != len(first) {
			t.Fatalf("ready=%v want=%v", again, first)
		}
		for i := range again {
			if again[i] != first[i] {
				t.Fatalf("ready order is not stable: %v vs %v", first, again)
			}
		}
	}
}

func TestNothingIsScheduledOutsideRunning(t *testing.T) {
	graph := fanOut(t)
	for _, state := range []run.State{
		run.StateQueued, run.StateWaitingApproval, run.StateWaitingResolution,
		run.StateSucceeded, run.StateCancelled,
	} {
		snapshot := running(nil)
		snapshot.State = state
		if got := readyIDs(graph, snapshot); got != nil {
			t.Errorf("state=%s scheduled %v", state, got)
		}
	}
}

// Handing out a node the envelope cannot pay for only moves the refusal to the
// reservation, after the Runtime has already committed to starting it.
func TestAnExhaustedBudgetSchedulesNothing(t *testing.T) {
	graph := fanOut(t)
	snapshot := running(nil)
	snapshot.Budget = run.Budget{
		Envelope: run.Limits{LLMCalls: 2, Tokens: 100, ToolCalls: 2},
		Used:     run.Limits{LLMCalls: 2, Tokens: 100, ToolCalls: 2},
	}

	if got := readyIDs(graph, snapshot); got != nil {
		t.Fatalf("ready=%v with nothing left to spend", got)
	}

	// A zero envelope is unlimited, not "nothing allowed".
	snapshot.Budget = run.Budget{}
	if got := readyIDs(graph, snapshot); len(got) != 1 {
		t.Fatalf("ready=%v; an unbudgeted Run must still run", got)
	}
}

func TestApplyingEveryNodeSucceedsTheRun(t *testing.T) {
	graph := fanOut(t)
	snapshot := running(succeeded("a", "b", "c"))

	progress, err := workflow.ApplyNodeResult(graph, snapshot, workflow.NodeResult{
		NodeID: "d", OutputRef: "blob-1",
	}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandSucceed {
		t.Fatalf("command=%+v want=succeed", progress.Command)
	}
	if progress.Nodes["d"].OutputRef != "blob-1" {
		t.Error("the node's output reference was not recorded")
	}
}

func TestAnIntermediateResultLeavesTheRunRunning(t *testing.T) {
	graph := fanOut(t)

	progress, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{NodeID: "a"}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command != nil {
		t.Fatalf("command=%+v; the graph advanced but the Run did not", progress.Command)
	}
	if progress.Nodes["a"].Status != run.StateSucceeded {
		t.Fatalf("node a=%+v", progress.Nodes["a"])
	}
}

// A required step failing means the Run cannot produce what it promised.
// Finishing the unrelated branches would report a result the Definition never
// described.
func TestAHardFailureFailsTheRunImmediately(t *testing.T) {
	graph := fanOut(t)

	progress, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{
		NodeID: "a", Failed: true,
	}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandFail {
		t.Fatalf("command=%+v want=fail", progress.Command)
	}
}

func TestASoftFailureEndsThePartialRunAsPartial(t *testing.T) {
	// a → b, with b optional: a succeeds and b does not.
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Optional: true},
	))

	progress, err := workflow.ApplyNodeResult(graph, running(succeeded("a")), workflow.NodeResult{
		NodeID: "b", Failed: true,
	}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandPartial {
		t.Fatalf("command=%+v want=partial", progress.Command)
	}
}

// Nodes stranded behind a soft failure will never run. Counting them as
// outstanding would park the Run forever on work nothing can schedule.
func TestNodesUnreachableBehindASoftFailureDoNotStallTheRun(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Optional: true},
		definition.NodeSpec{Name: "c", Agent: "publish", DependsOn: []string{"b"}},
	))

	progress, err := workflow.ApplyNodeResult(graph, running(succeeded("a")), workflow.NodeResult{
		NodeID: "b", Failed: true,
	}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandPartial {
		t.Fatalf("command=%+v want=partial; c can never run", progress.Command)
	}
}

func TestEverySoftFailureFailsTheRun(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft", Optional: true},
	))

	progress, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{
		NodeID: "a", Failed: true,
	}, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandFail {
		t.Fatalf("command=%+v want=fail; nothing succeeded", progress.Command)
	}
}

// A leaf's output is the Run's output. Checking it here is what stops a
// malformed result from reaching whoever consumes the Run.
func TestOutputIsValidatedBeforeItCanBeConsumed(t *testing.T) {
	d := dag(definition.NodeSpec{Name: "a", Agent: "draft"})
	d.OutputSchema = json.RawMessage(`{"type":"object"}`)
	graph := compile(t, d)

	progress, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{
		NodeID: "a", Output: json.RawMessage(`"a string"`), OutputRef: "blob-1",
	}, valueSchemas{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if progress.Nodes["a"].Status != run.StateFailed {
		t.Fatalf("node=%+v; an output the schema refuses is a node failure", progress.Nodes["a"])
	}
	if progress.Nodes["a"].OutputRef != "" {
		t.Error("a refused output is still referenced downstream")
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandFail {
		t.Fatalf("command=%+v want=fail", progress.Command)
	}
}

// The first answer is the one the graph advanced on. A worker that lost its
// lease and returned late must not overwrite it.
func TestALateResultForASettledNodeIsRefused(t *testing.T) {
	graph := fanOut(t)

	_, err := workflow.ApplyNodeResult(graph, running(succeeded("a")), workflow.NodeResult{NodeID: "a"}, nil)
	if run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("kind=%s want=conflict", run.KindOf(err))
	}
}

func TestAResultForAnUnknownNodeIsRefused(t *testing.T) {
	graph := fanOut(t)

	_, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{NodeID: "ghost"}, nil)
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "unknown_node" {
		t.Fatalf("err=%v", err)
	}
}

// ApplyNodeResult must not mutate the Snapshot it was handed: the caller still
// holds the pre-image and commits its CAS against it.
func TestApplyDoesNotMutateTheSnapshot(t *testing.T) {
	graph := fanOut(t)
	snapshot := running(succeeded("a"))

	if _, err := workflow.ApplyNodeResult(graph, snapshot, workflow.NodeResult{NodeID: "b"}, nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("the caller's snapshot grew to %d nodes", len(snapshot.Nodes))
	}
}

type valueSchemas struct{}

func (valueSchemas) ValidateSchema(json.RawMessage) error { return nil }

func (valueSchemas) ValidateValue(_, value json.RawMessage) error {
	var object map[string]any
	return json.Unmarshal(value, &object)
}

package workflow_test

import (
	"encoding/json"
	"errors"
	"maps"
	"testing"

	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/workflow"
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
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Inputs: []string{"a"}},
		definition.NodeSpec{Name: "c", Agent: "review", DependsOn: []string{"a"}, Inputs: []string{"a"}},
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

// A soft failure unblocks a dependent. The input binder omits the missing
// output, allowing a final assembler to use the remaining successful draft.
func TestASoftFailedDependencyUnblocksItsDependent(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "writer", Agent: "draft"},
		definition.NodeSpec{Name: "illustrator", Agent: "review", DependsOn: []string{"writer"}, Inputs: []string{"writer"}, Optional: true},
		definition.NodeSpec{Name: "assembler", Agent: "publish", DependsOn: []string{"writer", "illustrator"}, Inputs: []string{"writer", "illustrator"}},
	))
	nodes := succeeded("writer")
	nodes["illustrator"] = run.NodeState{Status: run.StateFailed, Attempts: 1}

	if got := readyIDs(graph, running(nodes)); len(got) != 1 || got[0] != "assembler" {
		t.Fatalf("ready=%v want [assembler]", got)
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

// A node behind a soft failure remains runnable; counting it as unreachable
// would prevent the assembler fallback from ever executing.
func TestNodesBehindASoftFailureRemainRunnable(t *testing.T) {
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
	if progress.Command != nil {
		t.Fatalf("command=%+v; c must remain runnable", progress.Command)
	}
	if got := readyIDs(graph, running(progress.Nodes)); len(got) != 1 || got[0] != "c" {
		t.Fatalf("ready=%v want [c]", got)
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
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
	))
	graph.Nodes[0].OutputSchema = json.RawMessage(`{"type":"object"}`)

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
	if ready := readyIDs(graph, running(progress.Nodes)); len(ready) != 0 {
		t.Fatalf("downstream nodes became ready after invalid output: %v", ready)
	}
	if len(progress.Output) != 0 {
		t.Fatalf("invalid output escaped normalization: %s", progress.Output)
	}
}

func TestNormalizedOutputIsReturnedToTheCommitPath(t *testing.T) {
	d := dag(definition.NodeSpec{Name: "a", Agent: "draft"})
	d.OutputSchema = json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer"}}}`)
	graph := compile(t, d)

	progress, err := workflow.ApplyNodeResult(graph, running(nil), workflow.NodeResult{
		NodeID: "a", Output: json.RawMessage(`{"count":1.0}`), OutputRef: "blob-1",
	}, valueSchemas{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(progress.Output) != `{"count":1}` {
		t.Fatalf("progress output = %s, want normalized literal %s", progress.Output, `{"count":1}`)
	}
	if progress.Nodes["a"].Status != run.StateSucceeded || progress.Nodes["a"].OutputRef != "blob-1" {
		t.Fatalf("node = %+v", progress.Nodes["a"])
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

func (valueSchemas) NormalizeValue(_, value json.RawMessage) (json.RawMessage, error) {
	var object map[string]any
	if err := json.Unmarshal(value, &object); err != nil {
		return nil, err
	}
	return json.RawMessage(`{"count":1}`), nil
}

func TestLoopCascadingResetInScheduler(t *testing.T) {
	// a (plan) -> b (write) -> c (review with Loop back to a)
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "draft", DependsOn: []string{"a"}, Inputs: []string{"a"}},
		definition.NodeSpec{
			Name: "c", Agent: "review", DependsOn: []string{"b"}, Inputs: []string{"b"},
			Loop: &definition.LoopSpec{
				MaxIterations: 2,
				Condition:     "node.quality_score < 80",
				TargetNode:    "a",
			},
		},
	))

	// Initial run: a succeeded, b succeeded
	snapshot := running(succeeded("a", "b"))

	// Node c succeeds with quality_score 70 (triggering loop back to a)
	progress, err := workflow.ApplyNodeResult(graph, snapshot, workflow.NodeResult{
		NodeID: "c",
		Output: json.RawMessage(`{"quality_score": 70}`),
	}, nil)
	if err != nil {
		t.Fatalf("ApplyNodeResult error: %v", err)
	}

	// Command should be nil (Run remains running)
	if progress.Command != nil {
		t.Fatalf("expected nil command for loop reset, got %+v", progress.Command)
	}

	// a, b, and c are all reset so a can re-run. Their entries stay, holding
	// the attempt counts the loop ceiling is measured against, but none of
	// them is settled any more.
	for _, id := range []string{"a", "b", "c"} {
		state := progress.Nodes[id]
		if state.Status.Terminal() {
			t.Fatalf("expected node %q to be reset, got %+v", id, state)
		}
		if state.Attempts == 0 {
			t.Fatalf("expected node %q to keep its attempt count, got %+v", id, state)
		}
	}

	// Scheduler should now mark 'a' as ready again
	nextSnapshot := running(progress.Nodes)
	ready := readyIDs(graph, nextSnapshot)
	if len(ready) != 1 || ready[0] != "a" {
		t.Fatalf("expected node 'a' to be ready after loop reset, got %v", ready)
	}

	// Now re-run 'a' and 'b', and 'c' outputs quality_score 90 (no loop)
	snapshot2 := running(succeeded("a", "b"))
	progress2, err := workflow.ApplyNodeResult(graph, snapshot2, workflow.NodeResult{
		NodeID: "c",
		Output: json.RawMessage(`{"quality_score": 90}`),
	}, nil)
	if err != nil {
		t.Fatalf("ApplyNodeResult 2 error: %v", err)
	}
	if progress2.Command == nil || progress2.Command.Kind != run.CommandSucceed {
		t.Fatalf("expected CommandSucceed when quality condition not met, got %+v", progress2.Command)
	}
}

// TestLoopStopsAtMaxIterations pins the number the compiler validates to the
// number the scheduler enforces. The loop node's own attempt count has to
// survive the cascading reset, or the ceiling is never reached and only the
// Run budget ends the loop.
func TestLoopStopsAtMaxIterations(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "draft", DependsOn: []string{"a"}, Inputs: []string{"a"}},
		definition.NodeSpec{
			Name: "c", Agent: "review", DependsOn: []string{"b"}, Inputs: []string{"b"},
			Loop: &definition.LoopSpec{
				MaxIterations: 2,
				Condition:     "node.quality_score < 80",
				TargetNode:    "a",
			},
		},
	))

	nodes := succeeded("a", "b")
	// The condition matches every time, so only MaxIterations can stop this.
	for iteration := 1; iteration <= 2; iteration++ {
		progress, err := workflow.ApplyNodeResult(graph, running(nodes), workflow.NodeResult{
			NodeID: "c",
			Output: json.RawMessage(`{"quality_score": 70}`),
		}, nil)
		if err != nil {
			t.Fatalf("iteration %d: ApplyNodeResult error: %v", iteration, err)
		}
		if progress.Command != nil {
			t.Fatalf("iteration %d: expected the loop to be taken, got command %+v", iteration, progress.Command)
		}
		if got := progress.Nodes["c"].Attempts; got != iteration {
			t.Fatalf("iteration %d: loop node attempts reset to %d", iteration, got)
		}
		nodes = rerun(progress.Nodes, "a", "b")
	}

	progress, err := workflow.ApplyNodeResult(graph, running(nodes), workflow.NodeResult{
		NodeID: "c",
		Output: json.RawMessage(`{"quality_score": 70}`),
	}, nil)
	if err != nil {
		t.Fatalf("ApplyNodeResult error: %v", err)
	}
	if progress.Command == nil || progress.Command.Kind != run.CommandSucceed {
		t.Fatalf("expected the Run to finish once max_iterations is spent, got %+v", progress.Command)
	}
}

// rerun marks the named reset nodes as having succeeded again, which is what
// the engine records once the re-dispatched nodes come back.
func rerun(nodes map[string]run.NodeState, ids ...string) map[string]run.NodeState {
	next := map[string]run.NodeState{}
	maps.Copy(next, nodes)
	for _, id := range ids {
		state := next[id]
		state.Status = run.StateSucceeded
		state.Attempts++
		next[id] = state
	}
	return next
}

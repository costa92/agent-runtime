package workflow

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// stepFloor is the smallest spend an Agent step can incur: one model call.
//
// Offering a node the envelope cannot pay for would only move the refusal from
// the scheduler to the reservation, after the Runtime has already committed to
// starting it. The floor is a lower bound and deliberately not an estimate —
// estimating a step's cost before running it is guessing.
var stepFloor = run.Limits{LLMCalls: 1}

// ReadyNodes returns the nodes that may start now.
//
// It reads only the graph and the Snapshot. No clock, no store, no registry: a
// scheduler that consulted anything else would return different work for the
// same durable state, and a takeover after a crash would not resume the same
// Run the previous worker was executing.
func ReadyNodes(graph *ExecutionGraph, snapshot run.Snapshot) []Node {
	if graph == nil || snapshot.State != run.StateRunning {
		return nil
	}

	// Budget exhaustion stops the scheduler rather than each node discovering
	// it separately: the Run has nothing left to spend, so there is no work to
	// hand out.
	if !snapshot.Budget.Affords(stepFloor) {
		return nil
	}

	var ready []Node
	for _, node := range graph.Nodes {
		if _, started := snapshot.Nodes[node.ID]; started {
			continue
		}
		if !dependenciesMet(graph, node, snapshot) {
			continue
		}
		ready = append(ready, node)
	}
	// Graph order is already canonical, so parallel-ready nodes come back in
	// the same order on every worker and on every replay.
	return ready
}

// dependenciesMet reports whether every node this one waits for has settled in
// a state that permits this node to run. A soft-failed dependency is allowed:
// its output is omitted by the input binder, which is the graph's fail-soft
// contract. A hard failure still blocks the dependent and the Run has already
// been failed by ApplyNodeResult.
func dependenciesMet(graph *ExecutionGraph, node Node, snapshot run.Snapshot) bool {
	for _, dependency := range node.DependsOn {
		state, ok := snapshot.Nodes[dependency]
		if !ok {
			return false
		}
		if state.Status == run.StateSucceeded {
			continue
		}
		if state.Status == run.StateFailed {
			upstream, err := graph.Lookup(dependency)
			if err == nil && upstream.Failure == FailSoft {
				continue
			}
		}
		return false
	}
	return true
}

// NodeResult is what a node produced.
type NodeResult struct {
	NodeID string
	// Failed marks a node that did not produce a usable output. The reason is
	// the caller's to record; what the scheduler needs is whether the graph can
	// go on.
	Failed bool
	Output json.RawMessage
	// OutputRef is where the output was stored. The Snapshot holds the
	// reference and never the value, which is unbounded.
	OutputRef string
}

// Progress is what a node result implies.
//
// Nodes is the node map to commit; Command is the Run-level transition, nil
// when the graph advanced without the Run changing state. Reduce owns Run state
// and knows nothing about nodes, so the two halves are returned separately and
// committed together by the caller — a node map that landed without its
// transition would describe a graph further along than the Run it belongs to.
type Progress struct {
	Nodes   map[string]run.NodeState
	Command *run.Command
}

// ApplyNodeResult folds one node result into the graph's progress.
//
// It performs no effect: it writes no store, calls no provider and consults no
// clock. It validates the result against the node's declared OutputSchema
// before that output can feed anything downstream — a leaf whose output does
// not match what the Definition promised is a failure of that node, not a
// surprise for whoever consumes the Run's result.
func ApplyNodeResult(graph *ExecutionGraph, snapshot run.Snapshot, result NodeResult, schemas definition.SchemaValidator) (Progress, error) {
	if graph == nil {
		return Progress{}, run.NewError("missing_graph", run.ErrorInternal, run.RetryNever)
	}
	node, err := graph.Lookup(result.NodeID)
	if err != nil {
		return Progress{}, err
	}
	if state, ok := snapshot.Nodes[node.ID]; ok && state.Status.Terminal() {
		// A late result from a worker that lost its lease. The first answer is
		// the one the graph already advanced on.
		return Progress{}, run.NewError("node_already_settled", run.ErrorConflict, run.RetryNever)
	}

	failed := result.Failed
	if !failed && len(node.OutputSchema) > 0 && schemas != nil {
		if err := schemas.ValidateValue(node.OutputSchema, result.Output); err != nil {
			failed = true
		}
	}

	nodes := make(map[string]run.NodeState, len(snapshot.Nodes)+1)
	maps.Copy(nodes, snapshot.Nodes)
	state := nodes[node.ID]
	state.Attempts++
	state.OutputRef = result.OutputRef
	state.Status = run.StateSucceeded
	if failed {
		state.Status = run.StateFailed
		state.OutputRef = ""
	}
	nodes[node.ID] = state

	command, err := commandFor(graph, nodes, node, failed)
	if err != nil {
		return Progress{}, err
	}
	return Progress{Nodes: nodes, Command: command}, nil
}

// commandFor decides what the node result means for the Run.
func commandFor(graph *ExecutionGraph, nodes map[string]run.NodeState, settled Node, failed bool) (*run.Command, error) {
	if failed && settled.Failure == FailHard {
		// A required step failed. Nothing downstream of it can run, and
		// finishing the unrelated branches would report a result the
		// Definition did not describe.
		return &run.Command{Kind: run.CommandFail}, nil
	}

	var succeeded, done int
	for _, node := range graph.Nodes {
		state, ok := nodes[node.ID]
		if !ok {
			if reachable(graph, nodes, node) {
				// Still work to do; the Run stays running.
				return nil, nil
			}
			// Unreachable behind a soft failure: it will never run, and that is
			// settled rather than pending.
			done++
			continue
		}
		if !state.Status.Terminal() {
			return nil, nil
		}
		done++
		if state.Status == run.StateSucceeded {
			succeeded++
		}
	}
	if done != len(graph.Nodes) {
		return nil, run.NewError("inconsistent_graph_progress", run.ErrorInternal, run.RetryNever,
			fmt.Errorf("%d of %d nodes accounted for", done, len(graph.Nodes)))
	}

	switch {
	case succeeded == len(graph.Nodes):
		return &run.Command{Kind: run.CommandSucceed}, nil
	case succeeded == 0:
		return &run.Command{Kind: run.CommandFail}, nil
	default:
		// Some nodes produced output and some did not. Partial is the honest
		// answer; rounding it to success would hide a missing branch and
		// rounding it to failure would discard work that was done.
		return &run.Command{Kind: run.CommandPartial}, nil
	}
}

// reachable reports whether an unstarted node can still run, i.e. no dependency
// of it has already failed or become unreachable itself.
func reachable(graph *ExecutionGraph, nodes map[string]run.NodeState, node Node) bool {
	for _, id := range node.DependsOn {
		dependency, err := graph.Lookup(id)
		if err != nil {
			return false
		}
		state, ok := nodes[id]
		if !ok {
			if !reachable(graph, nodes, dependency) {
				return false
			}
			continue
		}
		if state.Status == run.StateFailed {
			if dependencyNode, err := graph.Lookup(id); err == nil && dependencyNode.Failure == FailSoft {
				continue
			}
			return false
		}
	}
	return true
}

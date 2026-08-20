// Package workflow compiles a published Definition into the one execution
// shape the Runtime knows how to run, and schedules it.
//
// A single Agent and a DAG are the same graph here — the single Agent is a
// one-node graph. That is the whole point of the package: two execution paths
// would be two sets of invariants about budget, failure and ordering, and the
// one used less often is the one that drifts.
//
// Compilation happens on publish, never on Start. A Run pins the compiled
// graph's digest, so recovery reloads a shape rather than re-deriving one from
// a compiler that may have changed under it.
package workflow

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// NodeKind is what a node does. It is assigned by the compiler from the
// declaration, never guessed at execution time.
//
// There is one kind today, and that is a statement rather than an oversight: a
// graph declares steps, and only an Agent is a step. Tools and memories are
// capabilities a step uses, reached through their own gateways and governed
// there — modelling them as nodes would put the same call under two different
// sets of checks depending on how it was reached.
type NodeKind string

const NodeAgent NodeKind = "agent"

// FailurePolicy is what a node's failure means for the Run.
type FailurePolicy string

const (
	// FailHard: the Run fails. The default, because a step nobody marked
	// optional is a step the Run needs.
	FailHard FailurePolicy = "hard"
	// FailSoft: the Run continues and ends partial if anything else succeeded.
	FailSoft FailurePolicy = "soft"
)

// BindingSource says where a node's input comes from.
type BindingSource string

const (
	// SourceRunInput is the Run's own input, validated once at the boundary.
	SourceRunInput BindingSource = "run_input"
	// SourceNode is another node's output, delivered verbatim.
	SourceNode BindingSource = "node"
	// SourceNodes is several nodes' outputs, delivered as one object keyed by
	// producing node name.
	//
	// It is a distinct source rather than "SourceNode with more than one From"
	// so that the payload shape is read off the declaration instead of counted
	// at run time: a node whose upstream list grows from one to two would
	// otherwise silently change what its consumer receives.
	SourceNodes BindingSource = "nodes"
)

// Binding is a node's input wiring.
type Binding struct {
	Source BindingSource `json:"source"`
	// From is the producing nodes, set for SourceNode and SourceNodes.
	From []string `json:"from,omitempty"`
	// WithRunInput adds the Run's own input to what an upstream-reading node
	// receives, under RunInputKey in the keyed object.
	//
	// A node that reads an upstream could otherwise never see the Run input,
	// and what the host puts there is not always something an upstream carries
	// forward: a writer working from a plan still needs the reader profile the
	// Run was started with, and it reached the writer only to the extent the
	// planner happened to echo it into the plan.
	//
	// It is a flag rather than a reserved name inside From, because From holds
	// node names and a reserved name there would compete with a real node for
	// the same namespace. Setting it forces the keyed object even for a single
	// upstream, so the payload shape stays readable off the declaration.
	WithRunInput bool `json:"with_run_input,omitempty"`
}

// RunInputKey is where the Run's own input appears in a keyed input object.
//
// No node may be named this, so the key cannot be shadowed by an upstream; the
// compiler refuses such a graph rather than letting the collision decide.
const RunInputKey = "run_input"

// Node is one compiled step.
//
// It carries no per-node budget. Nothing declares one, and a field that is
// always zero reads as a ceiling that is being enforced when it is not; spend is
// bounded by the Run envelope, which is the only budget that exists.
type Node struct {
	ID   string   `json:"id"`
	Kind NodeKind `json:"kind"`
	// Implementation is the resolved registry key. The compiler has already
	// proven it is registered, so execution cannot fail on an unknown key.
	Implementation string   `json:"implementation"`
	DependsOn      []string `json:"depends_on,omitempty"`
	// Tools is the resolved tool allowlist for this node, and it is the whole
	// allowlist: empty grants nothing. The compiler has proven every key is
	// both registered and declared by the Definition, so a node cannot reach a
	// tool the Definition never asked for, nor one a sibling node was granted.
	Tools []string `json:"tools,omitempty"`
	Input Binding  `json:"input"`
	// OutputSchema is set only on a leaf: a node with no dependents produces
	// the Run's output, and that is the one output the Definition describes.
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`
	Failure      FailurePolicy   `json:"failure"`
}

// ExecutionGraph is an immutable compiled Definition.
//
// Callers must not mutate Nodes. The compiler is the only constructor, and the
// digest in Ref covers the nodes exactly as compiled — a mutated graph would
// still claim the digest of the one that was published.
type ExecutionGraph struct {
	Ref   run.ExecutionGraphRef `json:"ref"`
	Nodes []Node                `json:"nodes"`
}

// Lookup returns a node by ID.
func (g *ExecutionGraph) Lookup(id string) (Node, error) {
	for _, node := range g.Nodes {
		if node.ID == id {
			return node, nil
		}
	}
	return Node{}, run.NewError("unknown_node", run.ErrorInvalid, run.RetryNever)
}

// Verify refuses a graph that is not the one a Run pinned.
//
// Recovery loads by digest for this reason: a Definition republished under the
// same ref with a different shape, or a deployment whose compiler changed, would
// otherwise bring a Run back as something other than what it started as.
func (g *ExecutionGraph) Verify(pinned run.ExecutionGraphRef) error {
	if g.Ref == pinned {
		return nil
	}
	return run.NewError("graph_mismatch", run.ErrorConflict, run.RetryNever)
}
